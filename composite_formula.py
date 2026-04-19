from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


LEGACY_SCORE_COLUMNS = ["fo_persona", "ft_persona", "allocator", "access"]
NEW_SCORE_COLUMNS = [*LEGACY_SCORE_COLUMNS, "company_fit"]

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


@dataclass(frozen=True)
class CompositeConfig:
    weights: dict[str, float]
    legacy_weights: dict[str, float]
    score_bands: dict[str, dict[str, int]]


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
        if weights and bands:
            return CompositeConfig(weights=weights, legacy_weights=dict(DEFAULT_LEGACY_WEIGHTS), score_bands=bands)
    return composite_config()


def composite_config(
    weights: Mapping[str, float] | None = None,
    legacy_weights: Mapping[str, float] | None = None,
    score_bands: Mapping[str, Mapping[str, int]] | None = None,
) -> CompositeConfig:
    return CompositeConfig(
        weights=dict(weights or DEFAULT_WEIGHTS),
        legacy_weights=dict(legacy_weights or DEFAULT_LEGACY_WEIGHTS),
        score_bands={k: dict(v) for k, v in (score_bands or DEFAULT_SCORE_BANDS).items()},
    )
