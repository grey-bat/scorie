from __future__ import annotations

from pathlib import Path

import pandas as pd

from composite_formula import load_composite_config, score_band, weighted_score


def build_regression_report(candidate: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    if candidate.empty or history.empty:
        return pd.DataFrame()
    config = load_composite_config()
    cand = candidate.copy()
    if "weighted_score" not in cand.columns:
        cand["weighted_score"] = cand.apply(lambda row: weighted_score(row, config.weights), axis=1)
    cand["band"] = cand["weighted_score"].map(lambda v: score_band(v, config.score_bands))

    hist = history.copy()
    if "weighted_score" not in hist.columns:
        hist["weighted_score"] = hist.apply(lambda row: weighted_score(row, config.weights), axis=1)
    hist["band"] = hist["weighted_score"].map(lambda v: score_band(v, config.score_bands))

    merged = cand.merge(
        hist[["Match Key", "Status", "Reason", "weighted_score", "band"]],
        on="Match Key",
        how="left",
        suffixes=("", "_history"),
    )
    merged["regression"] = merged.apply(
        lambda row: (
            row.get("band_history", "") != row.get("band", "")
            or row.get("Status", "") == "Skip" and row.get("band", "") == "qualified"
        ),
        axis=1,
    )
    return merged


def write_regression_report(candidate: pd.DataFrame, history: pd.DataFrame, out_path: str | Path) -> pd.DataFrame:
    report = build_regression_report(candidate, history)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(out_path, index=False)
    return report
