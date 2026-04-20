from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import requests
from dotenv import load_dotenv

from composite_formula import DEFAULT_SCORE_BANDS, DEFAULT_WEIGHTS, CompositeConfig

DEFAULT_RUBRIC_PAGE_URL = os.getenv(
    "NOTION_RUBRIC_PAGE_URL",
    "https://www.notion.so/inpt/Current-Rubric-April-15-2026-344ec57f140d806c9370e7b6e28240dc",
)
DEFAULT_RUBRIC_PAGE_ID = os.getenv("NOTION_RUBRIC_PAGE_ID", "344ec57f140d806c9370e7b6e28240dc")
DEFAULT_2AXIS_RUBRIC_PAGE_URL = os.getenv(
    "NOTION_2AXIS_RUBRIC_PAGE_URL",
    "https://www.notion.so/inpt/Scoring-Rubric-2-Axis-d31a47ec5fee40a49f09765f5d13a5c0?source=copy_link",
)
DEFAULT_2AXIS_RUBRIC_PAGE_ID = os.getenv("NOTION_2AXIS_RUBRIC_PAGE_ID", "d31a47ec5fee40a49f09765f5d13a5c0")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2026-03-11")


@dataclass(frozen=True)
class RubricSnapshot:
    page_id: str
    rubric_text: str
    config: CompositeConfig
    source_url: str


def _page_id_from_url(url: str) -> str:
    candidates = re.findall(r"[0-9a-f]{32}", url, re.I)
    if candidates:
        return candidates[-1]
    match = re.search(r"([0-9a-f-]{36})$", url, re.I)
    if match:
        return match.group(1).replace("-", "")
    if "Scoring-Rubric-2-Axis" in url or "d31a47ec5fee40a49f09765f5d13a5c0" in url:
        return DEFAULT_2AXIS_RUBRIC_PAGE_ID
    return DEFAULT_RUBRIC_PAGE_ID


def _extract_plain_text(obj: dict) -> str:
    if not isinstance(obj, dict):
        return ""
    t = obj.get("type")
    if t in {"paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item", "numbered_list_item", "quote", "callout"}:
        parts = obj.get(t, {}).get("rich_text", [])
        return "".join(part.get("plain_text", "") for part in parts).strip()
    if t == "code":
        return obj.get("code", {}).get("rich_text", [{}])[0].get("plain_text", "").strip()
    if t == "toggle":
        parts = obj.get("toggle", {}).get("rich_text", [])
        return "".join(part.get("plain_text", "") for part in parts).strip()
    return ""


def _fetch_children(page_id: str, api_key: str) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": NOTION_VERSION,
    }
    children: list[dict] = []
    cursor = None
    with requests.Session() as session:
        while True:
            params = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            resp = session.get(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            children.extend(data.get("results", []))
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
    return children


def fetch_notion_page_text(page_url: str = DEFAULT_RUBRIC_PAGE_URL, api_key: str | None = None) -> tuple[str, str]:
    api_key = api_key or os.getenv("NOTION_API_KEY")
    if not api_key:
        raise RuntimeError("NOTION_API_KEY is required to sync rubric from Notion")
    page_id = _page_id_from_url(page_url)
    blocks = _fetch_children(page_id, api_key)
    lines: list[str] = []
    for block in blocks:
        text = _extract_plain_text(block)
        if not text:
            continue
        btype = block.get("type", "")
        if btype.startswith("heading_"):
            lines.append("")
            lines.append("#" * int(btype.split("_")[-1]) + f" {text}")
        elif btype in {"bulleted_list_item", "numbered_list_item", "quote", "callout"}:
            prefix = "- " if btype == "bulleted_list_item" else "1. " if btype == "numbered_list_item" else "> "
            lines.append(f"{prefix}{text}")
        else:
            lines.append(text)
    return page_id, "\n".join(lines).strip()


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
        m = re.match(r"([a-z_]+)\s*[:=]\s*([0-9.]+)", line)
        if m:
            weights[m.group(1)] = float(m.group(2))
    return weights


def _parse_bands(section: str) -> dict[str, dict[str, int]]:
    bands = {}
    for line in section.splitlines():
        line = line.strip().lstrip("-* ")
        if not line:
            continue
        m = re.match(r"([a-z_]+)\s*[:=]\s*([0-9]+)\s*-\s*([0-9]+)", line)
        if m:
            bands[m.group(1)] = {"min": int(m.group(2)), "max": int(m.group(3))}
            continue
        m = re.match(r"([a-z_]+)\s*[:=]\s*([0-9]+)\+?", line)
        if m:
            bands[m.group(1)] = {"min": int(m.group(2)), "max": 100}
    return bands


def build_snapshot_text(rubric_text: str, weights: Mapping[str, float], bands: Mapping[str, Mapping[str, int]]) -> str:
    body = rubric_text.strip()
    appendix = [
        "## Weights",
        *[f"- {name} = {float(value):g}" for name, value in weights.items()],
        "",
        "## Score Bands",
        *[f"- {name} = {bounds['min']}-{bounds['max']}" for name, bounds in bands.items()],
    ]
    return f"{body}\n\n" + "\n".join(appendix).strip() + "\n"


def sync_rubric_snapshot(
    page_url: str = DEFAULT_RUBRIC_PAGE_URL,
    *,
    out_path: str | Path = "scoring_rubric.md",
    api_key: str | None = None,
) -> RubricSnapshot:
    page_id, page_text = fetch_notion_page_text(page_url, api_key=api_key)
    weights_section = _parse_section(page_text, "Weights")
    bands_section = _parse_section(page_text, "Score Bands")
    if not weights_section:
        raise RuntimeError("Notion rubric page is missing a ## Weights section")
    if not bands_section:
        raise RuntimeError("Notion rubric page is missing a ## Score Bands section")
    weights = _parse_weights(weights_section)
    bands = _parse_bands(bands_section)
    expected_weights = set(DEFAULT_WEIGHTS)
    expected_bands = set(DEFAULT_SCORE_BANDS)
    if set(weights) != expected_weights:
        missing = sorted(expected_weights - set(weights))
        extra = sorted(set(weights) - expected_weights)
        raise RuntimeError(f"Notion rubric weights must define {sorted(expected_weights)}; missing={missing} extra={extra}")
    if set(bands) != expected_bands:
        missing = sorted(expected_bands - set(bands))
        extra = sorted(set(bands) - expected_bands)
        raise RuntimeError(f"Notion score bands must define {sorted(expected_bands)}; missing={missing} extra={extra}")
    rubric_text = page_text
    snapshot_text = build_snapshot_text(rubric_text, weights, bands)
    out_path = Path(out_path)
    out_path.write_text(snapshot_text, encoding="utf-8")
    return RubricSnapshot(
        page_id=page_id,
        rubric_text=snapshot_text,
        config=CompositeConfig(weights=weights, legacy_weights={}, score_bands=bands, direct_point_maps={}),
        source_url=page_url,
    )


def sync_2axis_rubric_latest(
    page_url: str = DEFAULT_2AXIS_RUBRIC_PAGE_URL,
    *,
    out_path: str | Path = "rubric_latest.md",
    api_key: str | None = None,
) -> RubricSnapshot:
    page_id, page_text = fetch_notion_page_text(page_url, api_key=api_key)
    rubric_text = page_text.strip() + "\n"
    out_path = Path(out_path)
    out_path.write_text(rubric_text, encoding="utf-8")
    return RubricSnapshot(
        page_id=page_id,
        rubric_text=rubric_text,
        config=CompositeConfig(weights={}, legacy_weights={}, score_bands={}, direct_point_maps={}),
        source_url=page_url,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["legacy_snapshot", "2axis_latest"], default="2axis_latest")
    ap.add_argument("--page-url", default=None)
    ap.add_argument("--out", default=None)
    return ap


def main() -> None:
    load_dotenv()
    args = build_arg_parser().parse_args()
    if args.mode == "legacy_snapshot":
        snapshot = sync_rubric_snapshot(
            page_url=args.page_url or DEFAULT_RUBRIC_PAGE_URL,
            out_path=args.out or "scoring_rubric.md",
        )
    else:
        snapshot = sync_2axis_rubric_latest(
            page_url=args.page_url or DEFAULT_2AXIS_RUBRIC_PAGE_URL,
            out_path=args.out or "rubric_latest.md",
        )
    print(snapshot.page_id)
    print(snapshot.source_url)


if __name__ == "__main__":
    main()
