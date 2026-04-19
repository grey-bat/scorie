import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import notion_dedupe_cleanup


class FakeClient:
    def __init__(self):
        self.patches = []

    def get(self, path):
        if path.startswith("/data_sources/"):
            return {
                "properties": {
                    "Raw ID": {"type": "rich_text"},
                    "is_dupe": {"type": "checkbox"},
                }
            }
        raise AssertionError(path)

    def patch(self, path, payload):
        self.patches.append((path, payload))
        return {}


def make_page(page_id, raw_id, stage="", best_email="", company="", headline="", created_time="2026-04-15T00:00:00Z"):
    return {
        "id": page_id,
        "created_time": created_time,
        "properties": {
            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": raw_id, "text": {"content": raw_id}}]},
            "Best Email": {"type": "email", "email": best_email},
            "Full Name": {"type": "title", "title": [{"type": "text", "plain_text": raw_id, "text": {"content": raw_id}}]},
            "Current Company": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": company, "text": {"content": company}}]},
            "Current Title": {"type": "rich_text", "rich_text": []},
            "Headline": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": headline, "text": {"content": headline}}]},
            "Summary": {"type": "rich_text", "rich_text": []},
            "Industry": {"type": "rich_text", "rich_text": []},
            "Stage": {"type": "select", "select": {"name": stage} if stage else None},
            "Created": {"type": "date", "date": {"start": created_time}},
            "Last Touch Date": {"type": "date", "date": None},
            "Last Sent At": {"type": "date", "date": None},
            "Last Received At": {"type": "date", "date": None},
            "Connected At": {"type": "date", "date": None},
            "Position 1 Description": {"type": "rich_text", "rich_text": []},
            "Position 2 Description": {"type": "rich_text", "rich_text": []},
            "Position 3 Description": {"type": "rich_text", "rich_text": []},
            "Organization 1": {"type": "rich_text", "rich_text": []},
            "Organization 2": {"type": "rich_text", "rich_text": []},
            "Organization 3": {"type": "rich_text", "rich_text": []},
            "Organization 1 Title": {"type": "rich_text", "rich_text": []},
            "Organization 2 Title": {"type": "rich_text", "rich_text": []},
            "Organization 3 Title": {"type": "rich_text", "rich_text": []},
            "Organization 1 Description": {"type": "rich_text", "rich_text": []},
            "Organization 2 Description": {"type": "rich_text", "rich_text": []},
            "Organization 3 Description": {"type": "rich_text", "rich_text": []},
            "Mutual Count": {"type": "number", "number": None},
            "Berkeley Signal": {"type": "rich_text", "rich_text": []},
            "Columbia Signal": {"type": "rich_text", "rich_text": []},
        },
    }


class NotionDedupeCleanupTests(unittest.TestCase):
    def test_live_mode_writes_only_current_duplicates_and_flags_losers(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out_path = tmp_path / "plan.csv"
            fake_client = FakeClient()

            pages = [
                make_page("page-keep", "abc", stage="Booked", company="Keep Co", headline="keeper"),
                make_page("page-flag", "abc", stage="", company="", headline="loser"),
                make_page("page-unique", "xyz", stage="New", company="Solo Co", headline="solo"),
            ]

            argv = [
                "notion_dedupe_cleanup.py",
                "--out",
                str(out_path),
                "--source",
                "live",
                "--apply",
            ]

            with patch("notion_dedupe_cleanup.NotionClient", return_value=fake_client), \
                 patch("notion_dedupe_cleanup.resolve_data_source_id", return_value="ds-1"), \
                 patch("notion_dedupe_cleanup.query_all_pages", return_value=pages), \
                 patch("sys.argv", argv), \
                 patch.dict(notion_dedupe_cleanup.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                notion_dedupe_cleanup.main()

            with open(out_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["Raw ID"], "abc")
            self.assertEqual({row["status"] for row in rows}, {"keep", "flag"})
            self.assertEqual(len(fake_client.patches), 1)
            self.assertEqual(fake_client.patches[0][0], "/pages/page-flag")
            self.assertEqual(fake_client.patches[0][1], {"properties": {"is_dupe": {"checkbox": True}}})

    def test_live_mode_handles_zero_duplicates_without_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out_path = tmp_path / "plan.csv"
            fake_client = FakeClient()

            argv = [
                "notion_dedupe_cleanup.py",
                "--out",
                str(out_path),
                "--source",
                "live",
            ]

            with patch("notion_dedupe_cleanup.NotionClient", return_value=fake_client), \
                 patch("notion_dedupe_cleanup.resolve_data_source_id", return_value="ds-1"), \
                 patch("notion_dedupe_cleanup.query_all_pages", return_value=[make_page("page-1", "abc")]), \
                 patch("sys.argv", argv), \
                 patch.dict(notion_dedupe_cleanup.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                notion_dedupe_cleanup.main()

            with open(out_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows, [])
            self.assertEqual(fake_client.patches, [])


if __name__ == "__main__":
    unittest.main()
