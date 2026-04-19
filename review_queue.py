from __future__ import annotations

from pathlib import Path

import pandas as pd

from composite_formula import legacy_weighted_score, load_composite_config, score_band, weighted_score
from reason_catalog import reason_suggestions


def build_review_queue(scored: pd.DataFrame, *, include_legacy: bool = True) -> pd.DataFrame:
    df = scored.copy()
    if "company_fit" not in df.columns:
        df["company_fit"] = 0
    config = load_composite_config()
    df["weighted_score"] = df.apply(lambda row: weighted_score(row, config.weights), axis=1)
    if include_legacy:
        df["legacy_weighted_score"] = df.apply(lambda row: legacy_weighted_score(row, config.legacy_weights), axis=1)
    df["score_band"] = df["weighted_score"].map(lambda v: score_band(v, config.score_bands))
    if "Status" not in df.columns:
        df["Status"] = ""
    if "Reason" not in df.columns:
        df["Reason"] = ""
    df["Reason Suggestions"] = df["Reason"].map(lambda value: " | ".join(reason_suggestions(value)))
    order_cols = [c for c in ["weighted_score", "company_fit", "ft_persona", "fo_persona", "allocator", "access"] if c in df.columns]
    if order_cols:
        df = df.sort_values(order_cols, ascending=[False] * len(order_cols), kind="stable")
    preferred = [
        "Status",
        "Reason",
        "Reason Suggestions",
        "Full Name",
        "Current Company",
        "Current Title",
        "weighted_score",
        "legacy_weighted_score",
        "score_band",
    ]
    ordered = [c for c in preferred if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    df = df[ordered]
    return df


def write_review_queue(scored: pd.DataFrame, out_path: str | Path) -> pd.DataFrame:
    queue = build_review_queue(scored)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(out_path, index=False)
    return queue
