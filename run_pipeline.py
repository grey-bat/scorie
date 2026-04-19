import argparse
import os
import subprocess
import sys
from pathlib import Path


def run(cmd):
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", default="data/full.csv")
    ap.add_argument("--distance-csv", default="data/everything.csv")
    ap.add_argument("--workdir", default="run_output")
    ap.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "minimax/minimax-m2.7"))
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
    notion = wd / "04_notion"

    run([sys.executable, "prepare_input.py", "--full", args.full, "--distance-csv", args.distance_csv, "--out", str(prep)])

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

    if args.writeback_notion or args.dry_run_notion:
        notion_cmd = [sys.executable, "update_notion.py", "--delta", str(delta / "delta_updates.csv"), "--out", str(notion)]
        if args.notion_limit:
            notion_cmd += ["--limit", str(args.notion_limit)]
        if args.dry_run_notion:
            notion_cmd += ["--dry-run"]
        run(notion_cmd)


if __name__ == "__main__":
    main()
