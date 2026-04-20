from __future__ import annotations

import csv
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


MANIFEST_COLUMNS = [
    "version_id",
    "parent_version_id",
    "created_at",
    "iteration",
    "scoring_track",
    "stop_mode",
    "fp_rate",
    "fn_rate",
    "promotion_status",
    "change_summary",
    "path",
]


@dataclass(frozen=True)
class RubricVersion:
    version_id: str
    path: Path
    manifest_path: Path


def timestamp_slug(now: datetime | None = None) -> str:
    now = now or datetime.now()
    return now.strftime("%Y-%m-%dT%H-%M-%S")


def ensure_rubric_store(base_dir: str | Path) -> tuple[Path, Path]:
    base = Path(base_dir)
    rubrics_dir = base / "rubrics"
    rubrics_dir.mkdir(parents=True, exist_ok=True)
    manifest = rubrics_dir / "manifest.csv"
    if not manifest.exists():
        with open(manifest, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
            writer.writeheader()
    return rubrics_dir, manifest


def next_version_number(rubrics_dir: str | Path) -> int:
    rubrics_dir = Path(rubrics_dir)
    highest = 0
    for path in rubrics_dir.glob("rubric_v*.md"):
        match = re.match(r"rubric_v(\d+)_", path.name)
        if match:
            highest = max(highest, int(match.group(1)))
    return highest + 1


def create_version_filename(rubrics_dir: str | Path, version_number: int, created_at: datetime | None = None) -> Path:
    stamp = timestamp_slug(created_at)
    return Path(rubrics_dir) / f"rubric_v{version_number:03d}_{stamp}.md"


def append_manifest_row(manifest_path: str | Path, row: dict[str, Any]) -> None:
    manifest_path = Path(manifest_path)
    with open(manifest_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        writer.writerow({key: row.get(key, "") for key in MANIFEST_COLUMNS})


def update_manifest_row(manifest_path: str | Path, version_id: str, **updates: Any) -> None:
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        return
    with open(manifest_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    changed = False
    for row in rows:
        if row.get("version_id") == version_id:
            for key, value in updates.items():
                if key in MANIFEST_COLUMNS:
                    row[key] = value
                    changed = True
    if not changed:
        return
    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def create_rubric_version(
    *,
    base_dir: str | Path,
    text: str,
    parent_version_id: str = "",
    iteration: int = 0,
    scoring_track: str = "autopilot_direct_100",
    stop_mode: str = "",
    fp_rate: float | None = None,
    fn_rate: float | None = None,
    promotion_status: str = "candidate",
    change_summary: str = "",
) -> RubricVersion:
    rubrics_dir, manifest_path = ensure_rubric_store(base_dir)
    version_number = next_version_number(rubrics_dir)
    created_at = datetime.now()
    path = create_version_filename(rubrics_dir, version_number, created_at)
    path.write_text(text, encoding="utf-8")
    version_id = path.stem
    append_manifest_row(
        manifest_path,
        {
            "version_id": version_id,
            "parent_version_id": parent_version_id,
            "created_at": created_at.isoformat(timespec="seconds"),
            "iteration": iteration,
            "scoring_track": scoring_track,
            "stop_mode": stop_mode,
            "fp_rate": "" if fp_rate is None else f"{fp_rate:.4f}",
            "fn_rate": "" if fn_rate is None else f"{fn_rate:.4f}",
            "promotion_status": promotion_status,
            "change_summary": change_summary,
            "path": str(path),
        },
    )
    return RubricVersion(version_id=version_id, path=path, manifest_path=manifest_path)


def promote_rubric_version(version_path: str | Path, active_path: str | Path = "scoring_rubric.md") -> None:
    shutil.copyfile(version_path, active_path)
