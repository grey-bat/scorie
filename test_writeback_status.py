import os
import tempfile
import unittest
from pathlib import Path

from writeback_status import build_writeback_status, estimate_eta_seconds, format_duration, read_json_status, write_json_atomic


class WritebackStatusTests(unittest.TestCase):
    def test_format_duration_renders_human_friendly_values(self):
        self.assertEqual(format_duration(None), "calculating")
        self.assertEqual(format_duration(0), "0s")
        self.assertEqual(format_duration(59), "59s")
        self.assertEqual(format_duration(61), "1m 1s")
        self.assertEqual(format_duration(3661), "1h 1m 1s")

    def test_build_status_reports_remaining_and_eta(self):
        status = build_writeback_status(
            phase="writing",
            total_rows=10,
            processed_rows=4,
            updated_rows=3,
            noop_rows=1,
            unmatched_rows=1,
            ambiguous_rows=0,
            retries=2,
            elapsed_seconds=20.0,
            started_at="2026-04-15T16:00:00+00:00",
            mode="write",
            current_row_index=5,
            current_match_key="raw:abc",
            current_page_id="page-1",
            loaded_source_pages=3,
            loaded_source_rows=300,
            queued_write_rows=6,
            duplicate_report_path="out/notion_writeback_duplicates.csv",
            duplicate_lookup_preview=[{"lookup_type": "Raw ID", "lookup_key": "abc", "match_count": 2, "page_ids": "p1;p2"}],
            last_error="rate_limited",
        )

        self.assertEqual(status["remaining_rows"], 6)
        self.assertEqual(status["updated_rows"], 3)
        self.assertEqual(status["noop_rows"], 1)
        self.assertEqual(status["loaded_source_pages"], 3)
        self.assertEqual(status["loaded_source_rows"], 300)
        self.assertEqual(status["queued_write_rows"], 6)
        self.assertEqual(status["duplicate_report_path"], "out/notion_writeback_duplicates.csv")
        self.assertEqual(status["duplicate_lookup_preview"][0]["lookup_key"], "abc")
        self.assertEqual(status["retries"], 2)
        self.assertEqual(status["current_row_index"], 5)
        self.assertIsNotNone(status["eta_seconds"])

    def test_eta_is_calculating_until_enough_rows_have_completed(self):
        self.assertIsNone(estimate_eta_seconds(0, 10, 5.0))
        self.assertIsNone(estimate_eta_seconds(2, 10, 5.0))
        self.assertEqual(estimate_eta_seconds(10, 10, 5.0), 0.0)

    def test_atomic_write_round_trips_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "status.json"
            status = build_writeback_status(
                phase="done",
                total_rows=1,
                processed_rows=1,
                updated_rows=1,
                noop_rows=0,
                unmatched_rows=0,
                ambiguous_rows=0,
                retries=0,
                elapsed_seconds=1.0,
                started_at="2026-04-15T16:00:00+00:00",
                mode="write",
                queued_write_rows=0,
                duplicate_report_path=None,
                finished_at="2026-04-15T16:00:01+00:00",
            )

            write_json_atomic(path, status)

            loaded = read_json_status(path)
            self.assertEqual(loaded["phase"], "done")
            self.assertEqual(loaded["total_rows"], 1)
            self.assertEqual(loaded["updated_rows"], 1)

    def test_read_json_status_returns_none_for_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "does_not_exist.json"
            self.assertIsNone(read_json_status(path))

    def test_read_json_status_returns_none_for_malformed_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "malformed.json"
            path.write_text("{this is not valid json:", encoding="utf-8")
            self.assertIsNone(read_json_status(path))


if __name__ == "__main__":
    unittest.main()
