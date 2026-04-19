import argparse
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from company_backfill import select_company_backfill_candidates
from review_queue import write_review_queue
from rubric_sync import sync_rubric_snapshot


def run(cmd):
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", default="data/full.csv")
    ap.add_argument("--distance-csv", default="data/everything.csv")
    ap.add_argument("--workdir", default="run_output")
    ap.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "minimax/minimax-m2.7"))
    ap.add_argument("--company-backfill-dir", default=os.getenv("COMPANY_BACKFILL_DIR", "."))
    speed_group = ap.add_mutually_exclusive_group()
    speed_group.add_argument("--safe", action="store_true")
    speed_group.add_argument("--fast", action="store_true")
    speed_group.add_argument("--aggressive", action="store_true")
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "8")))
    ap.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "12")))
    ap.add_argument("--batch-retries", type=int, default=int(os.getenv("OPENROUTER_BATCH_RETRIES", "2")))
    ap.add_argument("--recovery-delay", type=int, default=int(os.getenv("OPENROUTER_RECOVERY_DELAY", "300")))
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--start-row", type=int, default=1)
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--sync-notion", action="store_true")
    ap.add_argument("--sync-notion-every-waves", type=int, default=1)
    ap.add_argument("--writeback-notion", action="store_true")
    ap.add_argument("--dry-run-notion", action="store_true")
    ap.add_argument("--notion-limit", type=int, default=None)
    args = ap.parse_args()

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

    score_cmd = [
        sys.executable, "score_openrouter.py",
        "--input", str(prep / "prepared_scoring_input.csv"),
        "--out", str(score),
        "--model", args.model,
        "--start-row", str(args.start_row),
    ]
    if args.safe:
        score_cmd += ["--speed", "safe"]
    elif args.fast:
        score_cmd += ["--speed", "fast"]
    elif args.aggressive:
        score_cmd += ["--speed", "aggressive"]
    else:
        score_cmd += ["--batch-size", str(args.batch_size), "--concurrency", str(args.concurrency)]
    score_cmd += ["--batch-retries", str(args.batch_retries), "--recovery-delay", str(args.recovery_delay)]
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
