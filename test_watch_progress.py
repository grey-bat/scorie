import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from watch_progress import render


class WatchProgressTests(unittest.TestCase):
    def test_render_uses_live_status_when_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            notion_dir = workdir / "04_notion"
            notion_dir.mkdir(parents=True)
            (workdir / "01_prepare").mkdir()
            (workdir / "02_score").mkdir()
            (workdir / "03_delta_sync").mkdir()

            (notion_dir / "notion_writeback_status.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "phase": "writing",
                        "mode": "write",
                        "total_rows": 10,
                        "processed_rows": 4,
                        "remaining_rows": 6,
                        "updated_rows": 3,
                        "noop_rows": 0,
                        "queued_write_rows": 2,
                        "duplicate_report_path": "out/04_notion_sync/notion_writeback_duplicates.csv",
                        "duplicate_lookup_preview": [
                            {"lookup_type": "Raw ID", "lookup_key": "abc", "match_count": 2, "page_ids": "p1;p2"},
                        ],
                        "unmatched_rows": 1,
                        "ambiguous_rows": 0,
                        "retries": 2,
                        "elapsed_seconds": 20.0,
                        "eta_seconds": 30.0,
                        "loaded_source_pages": 4,
                        "loaded_source_rows": 400,
                    }
                ),
                encoding="utf-8",
            )
            with open(notion_dir / "notion_writeback_summary.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["metric", "value"])
                writer.writeheader()
                writer.writerow({"metric": "delta_rows", "value": 10})

            buf = io.StringIO()
            with redirect_stdout(buf):
                render(workdir, tail_n=3)

            output = buf.getvalue()
            self.assertIn("Notion writeback", output)
            self.assertIn("compact", output)
            self.assertIn("writing", output)
            self.assertIn("need updates", output)
            self.assertIn("eta", output)
            self.assertIn("6", output)
            self.assertIn("queued writes", output)
            self.assertIn("duplicate report", output)
            self.assertIn("duplicate preview", output)

    def test_render_falls_back_to_summary_when_live_status_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            notion_dir = workdir / "04_notion_sync"
            notion_dir.mkdir(parents=True)
            (workdir / "01_prepare").mkdir()
            (workdir / "02_score").mkdir()
            (workdir / "03_delta_sync").mkdir()

            with open(notion_dir / "notion_writeback_summary.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["metric", "value"])
                writer.writeheader()
                writer.writerow({"metric": "delta_rows", "value": 12})
                writer.writerow({"metric": "updated", "value": 4})

            buf = io.StringIO()
            with redirect_stdout(buf):
                render(workdir, tail_n=3)

            output = buf.getvalue()
            self.assertIn("notion summary", output)
            self.assertIn("delta_rows", output)
            self.assertIn("live status", output)
            self.assertIn("write rows", output)


if __name__ == "__main__":
    unittest.main()
