import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from autopilot_calibrate import autopilot_calibrate
from company_backfill import select_company_backfill_candidates
from review_queue import write_review_queue
from rubric_sync import sync_rubric_snapshot


def run(cmd):
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def write_live_status(workdir: Path, payload: dict) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    status_path = workdir / "autopilot_status.json"
    live_md_path = workdir / "live_status.md"
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Autopilot Status",
        "",
        f"- phase: {payload.get('phase', 'n/a')}",
        f"- iteration: {payload.get('iteration', 'n/a')}",
        f"- rubric_version: {payload.get('rubric_version', 'n/a')}",
        f"- best_version: {payload.get('best_version', 'n/a')}",
        f"- processed_rows: {payload.get('processed_rows', 'n/a')}",
        f"- total_rows: {payload.get('total_rows', 'n/a')}",
    ]
    if payload.get("note"):
        lines.append(f"- note: {payload['note']}")
    live_md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", default="data/full.csv")
    ap.add_argument("--distance-csv", default="data/everything.csv")
    ap.add_argument("--workdir", default="run_output")
    ap.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "z-ai/glm-5.1"))
    ap.add_argument("--rubric-model", default=os.getenv("OPENROUTER_RUBRIC_MODEL", "z-ai/glm-5.1"))
    ap.add_argument("--company-backfill-dir", default=os.getenv("COMPANY_BACKFILL_DIR", "."))
    speed_group = ap.add_mutually_exclusive_group()
    speed_group.add_argument("--safe", action="store_true")
    speed_group.add_argument("--fast", action="store_true")
    speed_group.add_argument("--aggressive", action="store_true")
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "32")))
    ap.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "12")))
    ap.add_argument("--batch-retries", type=int, default=int(os.getenv("OPENROUTER_BATCH_RETRIES", "2")))
    ap.add_argument("--recovery-delay", type=int, default=int(os.getenv("OPENROUTER_RECOVERY_DELAY", "300")))
    ap.add_argument("--timeout-total", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_TOTAL", "420")))
    ap.add_argument("--timeout-connect", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_CONNECT", "20")))
    ap.add_argument("--timeout-sock-connect", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_SOCK_CONNECT", "20")))
    ap.add_argument("--timeout-sock-read", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_SOCK_READ", "300")))
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--start-row", type=int, default=1)
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--sync-notion", action="store_true")
    ap.add_argument("--sync-notion-every-waves", type=int, default=1)
    ap.add_argument("--writeback-notion", action="store_true")
    ap.add_argument("--dry-run-notion", action="store_true")
    ap.add_argument("--notion-limit", type=int, default=None)
    ap.add_argument("--scoring-mode", choices=["legacy_raw_weighted", "autopilot_direct_100"], default="legacy_raw_weighted")
    ap.add_argument("--autopilot", action="store_true")
    ap.add_argument("--manual-labels-csv", default=None)
    ap.add_argument("--iterations", type=int, default=None)
    ap.add_argument("--target", type=float, default=None)
    ap.add_argument("--target-fp", type=float, default=None)
    ap.add_argument("--target-fn", type=float, default=None)
    ap.add_argument("--max-iterations", type=int, default=8)
    args = ap.parse_args()
    if args.scoring_mode == "autopilot_direct_100" and (args.writeback_notion or args.dry_run_notion or args.sync_notion):
        raise SystemExit("autopilot_direct_100 scoring does not support Notion sync/writeback yet")
    if args.autopilot and (args.writeback_notion or args.dry_run_notion or args.sync_notion):
        raise SystemExit("--autopilot does not support Notion sync/writeback yet")

    wd = Path(args.workdir)
    prep = wd / "01_prepare"
    score = wd / "02_score"
    delta = wd / "03_delta"
    review = wd / "04_review"
    backfill = wd / "05_backfill"
    notion = wd / "04_notion"

    if os.getenv("NOTION_API_KEY"):
        try:
            sync_rubric_snapshot(out_path="scoring_rubric.md")
        except Exception as exc:
            print(
                f"Notion rubric sync failed ({exc}); using checked-in scoring_rubric.md snapshot instead.",
                flush=True,
            )
    else:
        print("NOTION_API_KEY is not set; using the checked-in scoring_rubric.md snapshot.", flush=True)

    if args.autopilot:
        write_live_status(
            wd,
            {
                "phase": "preparing_input",
                "iteration": 0,
                "rubric_version": "pending",
                "best_version": "pending",
                "processed_rows": 0,
                "total_rows": "preparing",
                "note": "Running prepare_input.py before autopilot scoring starts",
                "scoring_model": args.model,
                "rubric_model": args.rubric_model,
            },
        )

    run([
        sys.executable,
        "prepare_input.py",
        "--full",
        args.full,
        "--distance-csv",
        args.distance_csv,
        "--out",
        str(prep),
        "--company-backfill-dir",
        args.company_backfill_dir,
    ])

    if args.autopilot:
        if not args.manual_labels_csv:
            raise SystemExit("--manual-labels-csv is required when --autopilot is used")
        prepared_csv = prep / "prepared_scoring_input.csv"
        prepared_rows = 0
        if prepared_csv.exists():
            prepared_rows = max(0, sum(1 for _ in open(prepared_csv, encoding="utf-8")) - 1)
        write_live_status(
            wd,
            {
                "phase": "prepared_input",
                "iteration": 0,
                "rubric_version": "pending",
                "best_version": "pending",
                "processed_rows": 0,
                "total_rows": prepared_rows,
                "note": "Input prep complete; autopilot calibration starting",
                "scoring_model": args.model,
                "rubric_model": args.rubric_model,
            },
        )
        autopilot_args = argparse.Namespace(
            workdir=str(wd),
            manual_labels_csv=args.manual_labels_csv,
            rubric_path="scoring_rubric.md",
            model=args.model,
            scoring_model=args.model,
            rubric_model=args.rubric_model,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            batch_retries=args.batch_retries,
            recovery_delay=args.recovery_delay,
            timeout_total=args.timeout_total,
            timeout_connect=args.timeout_connect,
            timeout_sock_connect=args.timeout_sock_connect,
            timeout_sock_read=args.timeout_sock_read,
            iterations=args.iterations,
            target=args.target,
            target_fp=args.target_fp,
            target_fn=args.target_fn,
            max_iterations=args.max_iterations,
            max_records=args.max_records,
            start_row=args.start_row,
            mock=args.mock,
        )
        best_rubric_path = autopilot_calibrate(autopilot_args)
        print(f"Promoted rubric: {best_rubric_path}")
        return

    score_cmd = [
        sys.executable, "score_openrouter.py",
        "--input", str(prep / "prepared_scoring_input.csv"),
        "--out", str(score),
        "--model", args.model,
        "--start-row", str(args.start_row),
        "--scoring-mode", args.scoring_mode,
        "--rubric-path", "scoring_rubric.md",
    ]
    if args.safe:
        score_cmd += ["--speed", "safe"]
    elif args.fast:
        score_cmd += ["--speed", "fast"]
    elif args.aggressive:
        score_cmd += ["--speed", "aggressive"]
    else:
        score_cmd += ["--batch-size", str(args.batch_size), "--concurrency", str(args.concurrency)]
    score_cmd += [
        "--batch-retries", str(args.batch_retries),
        "--recovery-delay", str(args.recovery_delay),
        "--timeout-total", str(args.timeout_total),
        "--timeout-connect", str(args.timeout_connect),
        "--timeout-sock-connect", str(args.timeout_sock_connect),
        "--timeout-sock-read", str(args.timeout_sock_read),
    ]
    if args.max_records:
        score_cmd += ["--max-records", str(args.max_records)]
    if args.mock:
        score_cmd += ["--mock"]
    if args.sync_notion:
        score_cmd += ["--sync-notion", "--sync-notion-every-waves", str(args.sync_notion_every_waves)]
    run(score_cmd)

    run([
        sys.executable, "build_delta.py",
        "--full", str(prep / "full_deduped_for_scoring.csv"),
        "--prepared", str(prep / "prepared_scoring_input.csv"),
        "--scores", str(score / "scores_raw.csv"),
        "--out", str(delta),
    ])

    review.mkdir(parents=True, exist_ok=True)
    delta_df = pd.read_csv(delta / "delta_updates.csv", low_memory=False)
    backfill.mkdir(parents=True, exist_ok=True)
    candidates = select_company_backfill_candidates(delta_df, min_weighted_score=50)
    if not candidates.empty:
        candidates.to_csv(backfill / "company_backfill_candidates.csv", index=False)
        print(f"Wrote {backfill / 'company_backfill_candidates.csv'}")
    if not delta_df.empty:
        write_review_queue(delta_df, review / "review_queue.csv")
        print(f"Wrote {review / 'review_queue.csv'}")

    if args.writeback_notion or args.dry_run_notion:
        notion_cmd = [sys.executable, "update_notion.py", "--delta", str(delta / "delta_updates.csv"), "--out", str(notion)]
        if args.notion_limit:
            notion_cmd += ["--limit", str(args.notion_limit)]
        if args.dry_run_notion:
            notion_cmd += ["--dry-run"]
        run(notion_cmd)


if __name__ == "__main__":
    main()
