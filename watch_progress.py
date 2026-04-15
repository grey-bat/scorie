import argparse
import csv
import json
import time
from collections import deque
from pathlib import Path


def read_csv_summary(path: Path):
    if not path.exists():
        return None
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows


def read_kv_csv(path: Path):
    rows = read_csv_summary(path)
    if not rows:
        return {}
    out = {}
    for row in rows:
        key = row.get("metric") or row.get("Metric") or row.get("name")
        val = row.get("value") or row.get("Value") or row.get("count")
        if key:
            out[str(key)] = str(val) if val is not None else ""
    return out


def tail_lines(path: Path, count: int):
    if not path.exists():
        return []
    dq = deque(maxlen=count)
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            dq.append(line.rstrip("\n"))
    return list(dq)


def safe_count_csv_rows(path: Path):
    if not path.exists():
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return max(0, sum(1 for _ in csv.reader(f)) - 1)


def count_lines(path: Path):
    if not path.exists():
        return None
    with open(path, encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f)


def fmt_mtime(path: Path):
    if not path.exists():
        return "n/a"
    return time.strftime("%H:%M:%S", time.localtime(path.stat().st_mtime))


def latest_jsonl_row(path: Path):
    if not path.exists():
        return None
    last = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line:
                last = line
    if not last:
        return None
    try:
        return json.loads(last)
    except Exception:
        return {"raw": last}


def status_line(label, value):
    return f"{label:<20} {value}"


def render(workdir: Path, tail_n: int):
    prep = workdir / "01_prepare"
    score = workdir / "02_score"
    delta_sync = workdir / "03_delta_sync"
    notion_sync = workdir / "04_notion_sync"
    sync_state = workdir / ".incremental_sync_state.json"

    print("\033[2J\033[H", end="")
    print(f"Score4 progress viewer  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"workdir: {workdir}")
    print()

    score_rows = safe_count_csv_rows(score / "scores_raw.csv")
    progress_tail = latest_jsonl_row(score / "scores_progress.jsonl")
    failed_rows = count_lines(score / "failed_batches.jsonl")
    delta_summary = read_kv_csv(delta_sync / "delta_summary.csv")
    notion_summary = read_kv_csv(notion_sync / "notion_writeback_summary.csv")
    notion_log_tail = tail_lines(notion_sync / "notion_writeback_log.csv", tail_n)
    score_tail = tail_lines(score / "scores_progress.jsonl", tail_n)

    print("Scoring")
    print(status_line("scores_raw rows", score_rows if score_rows is not None else "n/a"))
    print(status_line("latest progress", json.dumps(progress_tail, ensure_ascii=False) if progress_tail else "n/a"))
    print(status_line("failed batches", failed_rows if failed_rows is not None else "n/a"))
    print()

    print("Incremental sync")
    if sync_state.exists():
        try:
            state = json.loads(sync_state.read_text(encoding="utf-8"))
        except Exception as e:
            state = {"error": repr(e)}
    else:
        state = None
    print(status_line("sync state", json.dumps(state, ensure_ascii=False) if state is not None else "n/a"))
    print(status_line("delta summary", json.dumps(delta_summary, ensure_ascii=False) if delta_summary else "n/a"))
    print(status_line("notion summary", json.dumps(notion_summary, ensure_ascii=False) if notion_summary else "n/a"))
    print(status_line("delta mtime", fmt_mtime(delta_sync / "delta_updates.csv")))
    print(status_line("notion mtime", fmt_mtime(notion_sync / "notion_writeback_summary.csv")))
    print(status_line("sync state mtime", fmt_mtime(sync_state)))
    print()

    print("Recent scoring rows")
    for line in score_tail[-tail_n:]:
        print(line)
    print()

    print("Recent Notion writeback")
    for line in notion_log_tail[-tail_n:]:
        print(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="out")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--tail", type=int, default=8)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    workdir = Path(args.workdir)
    if args.once:
        render(workdir, args.tail)
        return

    try:
        while True:
            render(workdir, args.tail)
            time.sleep(max(0.5, args.interval))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
