from __future__ import annotations

from pathlib import Path

import pandas as pd

from composite_formula import (
    direct_score,
    family_office_total,
    fintech_total,
    legacy_weighted_score,
    load_composite_config,
    score_band,
    weighted_score,
)
from reason_catalog import reason_suggestions
from utils import spreadsheet_text


def build_review_queue(scored: pd.DataFrame, *, include_legacy: bool = True) -> pd.DataFrame:
    df = scored.copy()
    config = load_composite_config()
    is_direct = "direct_score" in df.columns or df.get("score_track", pd.Series(dtype=str)).eq("autopilot_direct_100").any()
    if is_direct:
        if "fo_total" not in df.columns:
            df["fo_total"] = df.apply(family_office_total, axis=1)
        if "ft_total" not in df.columns:
            df["ft_total"] = df.apply(fintech_total, axis=1)
        if "direct_score" not in df.columns:
            df["direct_score"] = df["ft_total"]
        if "overall_direct_score" not in df.columns:
            if "direct_score" in df.columns:
                df["overall_direct_score"] = df["direct_score"]
            else:
                df["overall_direct_score"] = df.apply(lambda row: direct_score(row, config.direct_point_maps), axis=1)
        df["score_band"] = df["direct_score"].map(lambda v: score_band(v, config.score_bands))
    else:
        if "company_fit" not in df.columns:
            df["company_fit"] = 0
        df["weighted_score"] = df.apply(lambda row: weighted_score(row, config.weights), axis=1)
        if include_legacy:
            df["legacy_weighted_score"] = df.apply(lambda row: legacy_weighted_score(row, config.legacy_weights), axis=1)
        df["score_band"] = df["weighted_score"].map(lambda v: score_band(v, config.score_bands))
    if "Status" not in df.columns:
        df["Status"] = ""
    if "Reason" not in df.columns:
        df["Reason"] = ""
    df["Reason Suggestions"] = df["Reason"].map(lambda value: " | ".join(reason_suggestions(value)))
    order_cols = [c for c in (["direct_score", "ft_total", "company_fit", "fintech_relevance", "allocator_power", "role_fit", "access"] if is_direct else ["weighted_score", "company_fit", "ft_persona", "fo_persona", "allocator", "access"]) if c in df.columns]
    if order_cols:
        df = df.sort_values(order_cols, ascending=[False] * len(order_cols), kind="stable")
    preferred = [
        "Status",
        "Reason",
        "Reason Suggestions",
        "Full Name",
        "Current Company",
        "Current Title",
        "direct_score",
        "ft_total",
        "fo_total",
        "overall_direct_score",
        "weighted_score",
        "legacy_weighted_score",
        "score_band",
        "score_track",
    ]
    ordered = [c for c in preferred if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    df = df[ordered]
    return df


def write_review_queue(scored: pd.DataFrame, out_path: str | Path) -> pd.DataFrame:
    queue = build_review_queue(scored)
    if "Raw ID" in queue.columns:
        queue["Raw ID"] = queue["Raw ID"].map(spreadsheet_text)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(out_path, index=False)
    return queue
