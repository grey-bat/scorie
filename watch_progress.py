import argparse
import csv
import json
import time
from collections import deque
from pathlib import Path

from writeback_status import format_duration, read_json_status


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


def resolve_notion_dir(workdir: Path):
    for name in ("04_notion_sync", "04_notion", "04_notion_test"):
        candidate = workdir / name
        if (candidate / "notion_writeback_status.json").exists() or (candidate / "notion_writeback_summary.csv").exists():
            return candidate
    return workdir / "04_notion_sync"


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


def compact_notion_status(notion_status, notion_summary):
    if notion_status:
        phase = notion_status.get("phase", "n/a")
        processed = notion_status.get("processed_rows", "n/a")
        total = notion_status.get("total_rows", "n/a")
        remaining = notion_status.get("remaining_rows", "n/a")
        updated = notion_status.get("updated_rows", "n/a")
        noop = notion_status.get("noop_rows", "n/a")
        loaded_source_rows = notion_status.get("loaded_source_rows")
        loaded_source_pages = notion_status.get("loaded_source_pages")
        queued_write_rows = notion_status.get("queued_write_rows")
        eta_value = notion_status.get("eta_seconds")
        eta = format_duration(eta_value) if eta_value is not None else "calculating"
        if phase == "loading_candidates" and loaded_source_rows is not None:
            source_bits = [f"{loaded_source_rows} source rows"]
            if loaded_source_pages is not None:
                source_bits.append(f"{loaded_source_pages} pages")
            return f"{' | '.join(source_bits)} | {phase} | ETA {eta}"
        if queued_write_rows is not None and phase == "writing":
            return f"{processed}/{total} processed | {updated} updated | {noop} noop | {queued_write_rows} queued | {remaining} remaining | {phase} | ETA {eta}"
        return f"{processed}/{total} processed | {updated} updated | {noop} noop | {remaining} remaining | {phase} | ETA {eta}"
    if notion_summary:
        delta_rows = notion_summary.get("delta_rows", "n/a")
        updated = notion_summary.get("updated", "n/a")
        noop = notion_summary.get("noop", "n/a")
        unmatched = notion_summary.get("unmatched", "n/a")
        ambiguous = notion_summary.get("ambiguous", "n/a")
        write_rows = notion_summary.get("write_rows", "n/a")
        dup_raw = notion_summary.get("duplicate_raw_ids")
        dup_email = notion_summary.get("duplicate_best_emails")
        duplicate_bits = []
        if dup_raw is not None:
            duplicate_bits.append(f"{dup_raw} raw dup")
        if dup_email is not None:
            duplicate_bits.append(f"{dup_email} email dup")
        duplicate_suffix = f" | {' | '.join(duplicate_bits)}" if duplicate_bits else ""
        return f"{updated}/{write_rows} written | {noop} noop | {unmatched} unmatched | {ambiguous} ambiguous{duplicate_suffix}"
    return "n/a"


def render(workdir: Path, tail_n: int, compact_only: bool = False):
    prep = workdir / "01_prepare"
    score = workdir / "02_score"
    delta_sync = workdir / "03_delta_sync"
    notion_sync = resolve_notion_dir(workdir)
    sync_state = workdir / ".incremental_sync_state.json"

    print("\033[2J\033[H", end="")
    print(f"Score4 progress viewer  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"workdir: {workdir}")
    print()

    notion_summary = read_kv_csv(notion_sync / "notion_writeback_summary.csv")
    notion_status = read_json_status(notion_sync / "notion_writeback_status.json")
    notion_log_tail = tail_lines(notion_sync / "notion_writeback_log.csv", tail_n) if not compact_only else []
    if not compact_only:
        score_rows = safe_count_csv_rows(score / "scores_raw.csv")
        progress_tail = latest_jsonl_row(score / "scores_progress.jsonl")
        failed_rows = count_lines(score / "failed_batches.jsonl")
        delta_summary = read_kv_csv(delta_sync / "delta_summary.csv")
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

    print("Notion writeback")
    print(status_line("compact", compact_notion_status(notion_status, notion_summary)))
    if notion_status:
        print(status_line("phase", notion_status.get("phase", "n/a")))
        print(status_line("mode", notion_status.get("mode", "n/a")))
        print(status_line("need updates", notion_status.get("total_rows", "n/a")))
        print(status_line("processed", notion_status.get("processed_rows", "n/a")))
        print(status_line("remaining", notion_status.get("remaining_rows", "n/a")))
        print(status_line("updated", notion_status.get("updated_rows", "n/a")))
        print(status_line("noop", notion_status.get("noop_rows", "n/a")))
        if notion_status.get("loaded_source_rows") is not None:
            print(status_line("source rows", notion_status.get("loaded_source_rows")))
        if notion_status.get("loaded_source_pages") is not None:
            print(status_line("source pages", notion_status.get("loaded_source_pages")))
        if notion_status.get("queued_write_rows") is not None:
            print(status_line("queued writes", notion_status.get("queued_write_rows")))
        print(status_line("unmatched", notion_status.get("unmatched_rows", "n/a")))
        print(status_line("ambiguous", notion_status.get("ambiguous_rows", "n/a")))
        print(status_line("retries", notion_status.get("retries", "n/a")))
        print(status_line("elapsed", format_duration(notion_status.get("elapsed_seconds"))))
        eta_value = notion_status.get("eta_seconds")
        print(status_line("eta", format_duration(eta_value) if eta_value is not None else "calculating"))
        if notion_status.get("last_error"):
            print(status_line("last error", notion_status.get("last_error")))
        if notion_status.get("duplicate_report_path"):
            print(status_line("duplicate report", notion_status.get("duplicate_report_path")))
        preview = notion_status.get("duplicate_lookup_preview") or []
        if preview:
            print(status_line("duplicate preview", json.dumps(preview[:3], ensure_ascii=False)))
        print()
    elif notion_summary:
        print(status_line("notion summary", json.dumps(notion_summary, ensure_ascii=False)))
        print(status_line("write rows", notion_summary.get("write_rows", "n/a")))
        print(status_line("duplicate raw ids", notion_summary.get("duplicate_raw_ids", "n/a")))
        print(status_line("duplicate best emails", notion_summary.get("duplicate_best_emails", "n/a")))
        print(status_line("live status", "missing; showing final summary only"))
        print()

    if compact_only:
        return

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
    ap.add_argument("--compact-only", action="store_true")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    workdir = Path(args.workdir)
    if args.once:
        render(workdir, args.tail, compact_only=args.compact_only)
        return

    try:
        while True:
            render(workdir, args.tail, compact_only=args.compact_only)
            time.sleep(max(0.5, args.interval))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
