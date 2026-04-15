import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


def count_data_rows(csv_path: Path) -> int:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return max(0, sum(1 for _ in reader) - 1)


def snapshot_csv_prefix(src: Path, dst: Path) -> int:
    """
    Copy a stable CSV prefix so incremental sync can read a file that is still
    being appended to without choking on a partially written trailing row.
    """
    data = src.read_bytes()
    if data and not data.endswith(b"\n"):
        last_newline = data.rfind(b"\n")
        if last_newline >= 0:
            data = data[: last_newline + 1]
        else:
            data = b""
    dst.write_bytes(data)
    return count_data_rows(dst)


def copy_new_rows(src: Path, dst: Path, start_after: int) -> int:
    with open(src, newline="", encoding="utf-8") as fin, open(dst, "w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            return 0
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        written = 0
        for idx, row in enumerate(reader, start=1):
            if idx <= start_after:
                continue
            writer.writerow(row)
            written += 1
    return written


def run(cmd):
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="out")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--notion-limit", type=int, default=None)
    args = ap.parse_args()

    wd = Path(args.workdir)
    prep = wd / "01_prepare"
    score = wd / "02_score"
    delta_dir = wd / "03_delta_sync"
    notion_out = wd / "04_notion_sync"
    state_path = wd / ".incremental_sync_state.json"
    delta_dir.mkdir(parents=True, exist_ok=True)
    notion_out.mkdir(parents=True, exist_ok=True)

    scores_path = score / "scores_raw.csv"
    if not scores_path.exists():
        raise SystemExit(f"Missing scores file: {scores_path}")

    snapshot_path = delta_dir / "scores_raw.snapshot.csv"
    current_rows = snapshot_csv_prefix(scores_path, snapshot_path)
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        state = {"last_synced_rows": 0}
    last_synced_rows = int(state.get("last_synced_rows", 0) or 0)
    if current_rows <= last_synced_rows:
        print(f"No new scored rows since last sync: {current_rows} rows total, {last_synced_rows} already synced.")
        return

    new_scores = delta_dir / "scores_new_rows.csv"
    extracted = copy_new_rows(snapshot_path, new_scores, last_synced_rows)
    if extracted == 0:
        print("No new complete rows available to sync yet.")
        return

    print(f"Syncing {extracted} new scored rows from {last_synced_rows + 1} to {last_synced_rows + extracted}.")
    run([
        sys.executable,
        "build_delta.py",
        "--full", str(prep / "full_deduped_for_scoring.csv"),
        "--prepared", str(prep / "prepared_scoring_input.csv"),
        "--scores", str(new_scores),
        "--out", str(delta_dir),
    ])

    notion_cmd = [
        sys.executable,
        "update_notion.py",
        "--delta", str(delta_dir / "delta_updates.csv"),
        "--out", str(notion_out),
    ]
    if args.notion_limit:
        notion_cmd += ["--limit", str(args.notion_limit)]
    if args.dry_run:
        notion_cmd += ["--dry-run"]
    run(notion_cmd)

    if not args.dry_run:
        state["last_synced_rows"] = current_rows
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(f"Updated sync state: {state_path} -> last_synced_rows={current_rows}")


if __name__ == "__main__":
    main()
