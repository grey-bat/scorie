import argparse
import subprocess
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="run_output")
    ap.add_argument("--limit", type=int, default=25)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    delta = Path(args.workdir) / "03_delta" / "delta_updates.csv"
    notion_out = Path(args.workdir) / "04_notion_test"
    cmd = [sys.executable, "update_notion.py", "--delta", str(delta), "--out", str(notion_out), "--limit", str(args.limit)]
    if not args.write:
        cmd.append("--dry-run")
    print("$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
