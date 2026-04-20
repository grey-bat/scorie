from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


LEGACY_SCORE_COLUMNS = ["fo_persona", "ft_persona", "allocator", "access"]
NEW_SCORE_COLUMNS = [*LEGACY_SCORE_COLUMNS, "company_fit"]
DIRECT_SCORE_COLUMNS = [
    "company_fit",
    "family_office_relevance",
    "fintech_relevance",
    "allocator_power",
    "access",
    "role_fit",
]

# The legacy four-score blend is kept for historical comparison.
DEFAULT_LEGACY_WEIGHTS = {
    "fo_persona": 0.42,
    "ft_persona": 0.25,
    "allocator": 0.15,
    "access": 0.18,
}

# Company fit gets the dominant weight in the new model because current company
# plus current role is the strongest qualitative signal in the eval loop.
DEFAULT_WEIGHTS = {
    "fo_persona": 0.18,
    "ft_persona": 0.12,
    "allocator": 0.15,
    "access": 0.15,
    "company_fit": 0.40,
}

DEFAULT_SCORE_BANDS = {
    "qualified": {"min": 75, "max": 100},
    "nearly_qualified": {"min": 50, "max": 74},
    "little_qualified": {"min": 25, "max": 49},
    "totally_unqualified": {"min": 0, "max": 24},
}

DEFAULT_DIRECT_POINT_MAPS = {
    # FT score dimensions (sum of caps = 100)
    "company_fit": [7, 14, 21, 28, 35],
    "fintech_relevance": [6, 12, 18, 24, 30],
    "allocator_power": [4, 8, 12, 16, 18],
    "access": [2, 5, 8, 10, 12],
    "role_fit": [1, 2, 3, 4, 5],
    # FO is scored separately (not in FT 100-point budget)
    "family_office_relevance": [3, 6, 9, 12, 15],
}


@dataclass(frozen=True)
class CompositeConfig:
    weights: dict[str, float]
    legacy_weights: dict[str, float]
    score_bands: dict[str, dict[str, int]]
    direct_point_maps: dict[str, list[int]]


def _score_value(value) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def normalize_weights(weights: Mapping[str, float], columns: list[str]) -> dict[str, float]:
    normalized = {name: float(weights.get(name, 0.0)) for name in columns}
    total = sum(normalized.values())
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    return {name: value / total for name, value in normalized.items()}


def weighted_score(scores: Mapping[str, float], weights: Mapping[str, float]) -> float:
    total = 0.0
    for name, weight in weights.items():
        total += _score_value(scores.get(name, 0.0)) * float(weight)
    return round(total * 20.0, 2)


FT_DIRECT_DIMENSIONS = {
    "company_fit",
    "fintech_relevance",
    "allocator_power",
    "access",
    "role_fit",
}


def direct_score(scores: Mapping[str, float], point_maps: Mapping[str, list[int]] | None = None) -> int:
    """FT score on a 0-100 scale.

    Only the five FT dimensions (company_fit, fintech_relevance, allocator_power,
    access, role_fit) contribute. family_office_relevance is kept out entirely so
    its cap can be anything and does not consume FT budget.
    """
    point_maps = point_maps or DEFAULT_DIRECT_POINT_MAPS
    total = 0.0
    max_total = 0.0
    for name, allowed_points in point_maps.items():
        if name not in FT_DIRECT_DIMENSIONS:
            continue
        value = _score_value(scores.get(name, 0.0))
        if not allowed_points:
            continue
        if int(round(value)) not in {int(v) for v in allowed_points}:
            raise ValueError(f"invalid direct score for {name}: {value}")
        total += value
        max_total += max(allowed_points)
    if max_total <= 0:
        raise ValueError("FT direct point maps must have a positive max total")
    if int(round(max_total)) != 100:
        raise ValueError(f"FT direct point maps must sum to 100, got {max_total}")
    return int(round(total))


def fintech_total(scores: Mapping[str, float]) -> int:
    return int(round(
        _score_value(scores.get("company_fit", 0.0))
        + _score_value(scores.get("fintech_relevance", 0.0))
        + _score_value(scores.get("allocator_power", 0.0))
        + _score_value(scores.get("access", 0.0))
        + _score_value(scores.get("role_fit", 0.0))
    ))


def family_office_total(scores: Mapping[str, float]) -> int:
    return int(round(
        _score_value(scores.get("company_fit", 0.0))
        + _score_value(scores.get("family_office_relevance", 0.0))
        + _score_value(scores.get("allocator_power", 0.0))
        + _score_value(scores.get("access", 0.0))
        + _score_value(scores.get("role_fit", 0.0))
    ))


def legacy_weighted_score(scores: Mapping[str, float], weights: Mapping[str, float] | None = None) -> float:
    return weighted_score(scores, weights or DEFAULT_LEGACY_WEIGHTS)


def score_band(score: float, bands: Mapping[str, Mapping[str, int]] | None = None) -> str:
    bands = bands or DEFAULT_SCORE_BANDS
    value = int(round(float(score)))
    for band_name, bounds in bands.items():
        if bounds["min"] <= value <= bounds["max"]:
            return band_name
    return "totally_unqualified"


def build_notion_formula(weights: Mapping[str, float], score_props: Mapping[str, str] | None = None) -> str:
    score_props = score_props or {
        "fo_persona": "fo_persona",
        "ft_persona": "ft_persona",
        "allocator": "allocator",
        "access": "access",
        "company_fit": "company_fit",
    }
    terms = []
    for key in NEW_SCORE_COLUMNS:
        weight = float(weights.get(key, 0.0))
        if weight == 0:
            continue
        prop = score_props[key]
        terms.append(f'prop("{prop}") * {weight}')
    if not terms:
        raise ValueError("cannot build formula without weights")
    return f"round(({ ' + '.join(terms) }) * 20)"


def _parse_section(text: str, header: str) -> str:
    pattern = rf"(?ms)^## {re.escape(header)}\s*$\n(.*?)(?=\n## |\Z)"
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def _parse_weights(section: str) -> dict[str, float]:
    weights = {}
    for line in section.splitlines():
        line = line.strip().lstrip("-* ")
        if not line:
            continue
        match = re.match(r"([a-z_]+)\s*[:=]\s*([0-9.]+)", line)
        if match:
            weights[match.group(1)] = float(match.group(2))
    return weights


def _parse_direct_point_maps(section: str) -> dict[str, list[int]]:
    point_maps: dict[str, list[int]] = {}
    for line in section.splitlines():
        # Strip inline HTML (e.g., <span data-proof=...> wrappers that the
        # rubric-editing UI sometimes leaves around point-map lines) and
        # markdown escape backslashes before matching.
        line = re.sub(r"<[^>]+>", "", line)
        line = line.replace("\\_", "_")
        line = line.strip().lstrip("-* ")
        if not line:
            continue
        match = re.match(r"([a-z_]+)\s*[:=]\s*([0-9,\s]+)", line)
        if not match:
            continue
        values = [int(v.strip()) for v in match.group(2).split(",") if v.strip()]
        if values:
            point_maps[match.group(1)] = values
    return point_maps


def _parse_bands(section: str) -> dict[str, dict[str, int]]:
    bands = {}
    for line in section.splitlines():
        line = line.strip().lstrip("-* ")
        if not line:
            continue
        match = re.match(r"([a-z_]+)\s*[:=]\s*([0-9]+)\s*-\s*([0-9]+)", line)
        if match:
            bands[match.group(1)] = {"min": int(match.group(2)), "max": int(match.group(3))}
            continue
        match = re.match(r"([a-z_]+)\s*[:=]\s*([0-9]+)\+?", line)
        if match:
            bands[match.group(1)] = {"min": int(match.group(2)), "max": 100}
    return bands


def load_composite_config(rubric_path: str | Path = "scoring_rubric.md") -> CompositeConfig:
    path = Path(rubric_path)
    if path.exists():
        text = path.read_text(encoding="utf-8")
        weights = _parse_weights(_parse_section(text, "Weights"))
        bands = _parse_bands(_parse_section(text, "Score Bands"))
        direct_point_maps = _parse_direct_point_maps(_parse_section(text, "Direct Point Maps"))
        if bands:
            return CompositeConfig(
                weights=weights or dict(DEFAULT_WEIGHTS),
                legacy_weights=dict(DEFAULT_LEGACY_WEIGHTS),
                score_bands=bands,
                direct_point_maps=direct_point_maps or {k: list(v) for k, v in DEFAULT_DIRECT_POINT_MAPS.items()},
            )
    return composite_config()


def composite_config(
    weights: Mapping[str, float] | None = None,
    legacy_weights: Mapping[str, float] | None = None,
    score_bands: Mapping[str, Mapping[str, int]] | None = None,
    direct_point_maps: Mapping[str, list[int]] | None = None,
) -> CompositeConfig:
    return CompositeConfig(
        weights=dict(weights or DEFAULT_WEIGHTS),
        legacy_weights=dict(legacy_weights or DEFAULT_LEGACY_WEIGHTS),
        score_bands={k: dict(v) for k, v in (score_bands or DEFAULT_SCORE_BANDS).items()},
        direct_point_maps={k: list(v) for k, v in (direct_point_maps or DEFAULT_DIRECT_POINT_MAPS).items()},
    )
