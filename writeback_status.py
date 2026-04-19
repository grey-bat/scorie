import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


STATUS_SCHEMA_VERSION = 1


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "calculating"
    total = max(0, int(round(seconds)))
    if total == 0:
        return "0s"
    parts = []
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def estimate_eta_seconds(processed_rows: int, total_rows: int, elapsed_seconds: float) -> Optional[float]:
    if processed_rows < 3:
        return None
    if total_rows <= 0 or elapsed_seconds <= 0:
        return None
    remaining = max(total_rows - processed_rows, 0)
    if remaining <= 0:
        return 0.0
    rate = processed_rows / elapsed_seconds
    if rate <= 0:
        return None
    return remaining / rate


def build_writeback_status(
    *,
    phase: str,
    total_rows: int,
    processed_rows: int,
    updated_rows: int,
    noop_rows: int,
    unmatched_rows: int,
    ambiguous_rows: int,
    retries: int,
    elapsed_seconds: float,
    started_at: str,
    mode: str,
    current_row_index: Optional[int] = None,
    current_match_key: Optional[str] = None,
    current_page_id: Optional[str] = None,
    loaded_source_pages: Optional[int] = None,
    loaded_source_rows: Optional[int] = None,
    queued_write_rows: Optional[int] = None,
    duplicate_report_path: Optional[str] = None,
    duplicate_lookup_preview: Optional[list[dict[str, Any]]] = None,
    last_error: Optional[str] = None,
    last_success_match_key: Optional[str] = None,
    last_success_page_id: Optional[str] = None,
    finished_at: Optional[str] = None,
) -> dict[str, Any]:
    remaining_rows = max(total_rows - processed_rows, 0)
    if finished_at is not None or remaining_rows == 0:
        eta_seconds = 0.0
    else:
        eta_seconds = estimate_eta_seconds(processed_rows, total_rows, elapsed_seconds)
    rows_per_second = None
    if processed_rows > 0 and elapsed_seconds > 0:
        rows_per_second = processed_rows / elapsed_seconds

    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "phase": phase,
        "mode": mode,
        "started_at": started_at,
        "finished_at": finished_at,
        "total_rows": total_rows,
        "processed_rows": processed_rows,
        "remaining_rows": remaining_rows,
        "updated_rows": updated_rows,
        "noop_rows": noop_rows,
        "unmatched_rows": unmatched_rows,
        "ambiguous_rows": ambiguous_rows,
        "retries": retries,
        "current_row_index": current_row_index,
        "current_match_key": current_match_key,
        "current_page_id": current_page_id,
        "loaded_source_pages": loaded_source_pages,
        "loaded_source_rows": loaded_source_rows,
        "queued_write_rows": queued_write_rows,
        "duplicate_report_path": duplicate_report_path,
        "duplicate_lookup_preview": duplicate_lookup_preview,
        "last_error": last_error,
        "last_success_match_key": last_success_match_key,
        "last_success_page_id": last_success_page_id,
        "elapsed_seconds": elapsed_seconds,
        "eta_seconds": eta_seconds,
        "rows_per_second": rows_per_second,
    }


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass


def read_json_status(path: Path) -> Optional[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
