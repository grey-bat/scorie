from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def append_eval_history(reviewed: pd.DataFrame, out_dir: str | Path, score_track: str = "legacy_raw_weighted") -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    history_path = out_dir / f"eval_history_{score_track}.csv"
    snapshot_path = out_dir / f"eval_history_{score_track}_{_timestamp()}.csv"
    reviewed = reviewed.copy()
    if "score_track" not in reviewed.columns:
        reviewed["score_track"] = score_track
    reviewed.to_csv(snapshot_path, index=False)
    if history_path.exists():
        existing = pd.read_csv(history_path, dtype=str, low_memory=False)
        combined = pd.concat([existing, reviewed.astype(str)], ignore_index=True, sort=False)
    else:
        combined = reviewed.astype(str)
    combined.to_csv(history_path, index=False)
    return snapshot_path


def load_eval_history(out_dir: str | Path, score_track: str = "legacy_raw_weighted") -> pd.DataFrame:
    path = Path(out_dir) / f"eval_history_{score_track}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, low_memory=False)
