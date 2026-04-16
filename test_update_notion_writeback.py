import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import update_notion


class FakeClient:
    def __init__(self, *args, fail_on_patch_after=None, **kwargs):
        self.patches = []
        self.fail_on_patch_after = fail_on_patch_after

    def get(self, path):
        if path.startswith("/data_sources/"):
            return {
                "properties": {
                    "Raw ID": {"type": "rich_text"},
                    "Best Email": {"type": "email"},
                    "Persona Signal - Family Office": {"type": "number"},
                    "Persona Signal - Fintech": {"type": "number"},
                    "Allocator Score": {"type": "number"},
                    "Access Score": {"type": "number"},
                    "Degree": {"type": "number"},
                    "Alumni Signal": {"type": "rich_text"},
                }
            }
        raise AssertionError(path)

    def patch(self, path, payload):
        if self.fail_on_patch_after is not None and len(self.patches) >= self.fail_on_patch_after:
            raise RuntimeError("Notion error 400: boom")
        self.patches.append((path, payload))
        return {}


class UpdateNotionWritebackTests(unittest.TestCase):
    def test_main_writes_live_status_and_final_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            delta_path = tmp_path / "delta.csv"
            out_path = tmp_path / "out"
            with open(delta_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Match Key", "Raw ID", "Best Email", "Persona Signal - Family Office", "Persona Signal - Fintech", "Allocator Score", "Access Score", "Degree", "Alumni Signal"])
                writer.writeheader()
                writer.writerow({
                    "Match Key": "raw:abc",
                    "Raw ID": "abc",
                    "Best Email": "",
                    "Persona Signal - Family Office": 1,
                    "Persona Signal - Fintech": 2,
                    "Allocator Score": 3,
                    "Access Score": 4,
                    "Degree": 5,
                    "Alumni Signal": "Cal",
                })
                writer.writerow({
                    "Match Key": "raw:missing",
                    "Raw ID": "missing",
                    "Best Email": "",
                    "Persona Signal - Family Office": 9,
                    "Persona Signal - Fintech": 8,
                    "Allocator Score": 7,
                    "Access Score": 6,
                    "Degree": 5,
                    "Alumni Signal": "CBS",
                })

            fake_client = FakeClient()

            def fake_query_all_pages(client, data_source_id, on_page=None):
                pages = [
                    {
                        "id": "page-1",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "abc", "text": {"content": "abc"}}]},
                            "Best Email": {"type": "email", "email": ""},
                            "Persona Signal - Family Office": {"type": "number", "number": 0},
                            "Persona Signal - Fintech": {"type": "number", "number": 0},
                            "Allocator Score": {"type": "number", "number": 0},
                            "Access Score": {"type": "number", "number": 0},
                            "Degree": {"type": "number", "number": 0},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "", "text": {"content": ""}}]},
                        },
                    }
                ]
                if on_page is not None:
                    on_page(1, 1)
                return pages

            status_snapshots = []

            original_write = update_notion.write_json_atomic

            def capture_status(path, data):
                status_snapshots.append(data.copy())
                return original_write(path, data)

            argv = [
                "update_notion.py",
                "--delta",
                str(delta_path),
                "--out",
                str(out_path),
            ]

            with patch("update_notion.NotionClient", return_value=fake_client), \
                 patch("update_notion.resolve_data_source_id", return_value="ds-1"), \
                 patch("update_notion.query_all_pages", side_effect=fake_query_all_pages), \
                 patch("update_notion.write_json_atomic", side_effect=capture_status), \
                 patch("sys.argv", argv), \
                 patch.dict(update_notion.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                update_notion.main()

            self.assertGreaterEqual(len(status_snapshots), 3)
            self.assertEqual(status_snapshots[0]["phase"], "loading_candidates")
            self.assertEqual(status_snapshots[0]["total_rows"], 2)
            self.assertTrue(any(snapshot.get("loaded_source_pages") == 1 for snapshot in status_snapshots))
            self.assertTrue(any(snapshot.get("loaded_source_rows") == 1 for snapshot in status_snapshots))
            self.assertEqual(status_snapshots[-1]["phase"], "done")
            self.assertEqual(status_snapshots[-1]["processed_rows"], 2)
            self.assertEqual(status_snapshots[-1]["updated_rows"], 1)
            self.assertEqual(status_snapshots[-1]["unmatched_rows"], 1)
            self.assertEqual(status_snapshots[-1]["noop_rows"], 0)
            self.assertTrue((out_path / "notion_writeback_summary.csv").exists())
            self.assertTrue((out_path / "notion_writeback_log.csv").exists())
            self.assertTrue((out_path / "notion_writeback_status.json").exists())

    def test_main_persists_partial_artifacts_on_fatal_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            delta_path = tmp_path / "delta.csv"
            out_path = tmp_path / "out"
            with open(delta_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Match Key", "Raw ID", "Best Email", "Persona Signal - Family Office", "Persona Signal - Fintech", "Allocator Score", "Access Score", "Degree", "Alumni Signal"])
                writer.writeheader()
                writer.writerow({
                    "Match Key": "raw:abc",
                    "Raw ID": "abc",
                    "Best Email": "",
                    "Persona Signal - Family Office": 1,
                    "Persona Signal - Fintech": 2,
                    "Allocator Score": 3,
                    "Access Score": 4,
                    "Degree": 5,
                    "Alumni Signal": "Cal",
                })
                writer.writerow({
                    "Match Key": "raw:def",
                    "Raw ID": "def",
                    "Best Email": "",
                    "Persona Signal - Family Office": 9,
                    "Persona Signal - Fintech": 8,
                    "Allocator Score": 7,
                    "Access Score": 6,
                    "Degree": 5,
                    "Alumni Signal": "CBS",
                })

            fake_client = FakeClient(fail_on_patch_after=1)

            def fake_query_all_pages(client, data_source_id, on_page=None):
                return [
                    {
                        "id": "page-1",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "abc", "text": {"content": "abc"}}]},
                            "Best Email": {"type": "email", "email": ""},
                            "Persona Signal - Family Office": {"type": "number", "number": 0},
                            "Persona Signal - Fintech": {"type": "number", "number": 0},
                            "Allocator Score": {"type": "number", "number": 0},
                            "Access Score": {"type": "number", "number": 0},
                            "Degree": {"type": "number", "number": 0},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "", "text": {"content": ""}}]},
                        },
                    },
                    {
                        "id": "page-2",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "def", "text": {"content": "def"}}]},
                            "Best Email": {"type": "email", "email": ""},
                            "Persona Signal - Family Office": {"type": "number", "number": 0},
                            "Persona Signal - Fintech": {"type": "number", "number": 0},
                            "Allocator Score": {"type": "number", "number": 0},
                            "Access Score": {"type": "number", "number": 0},
                            "Degree": {"type": "number", "number": 0},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "", "text": {"content": ""}}]},
                        },
                    },
                ]

            argv = [
                "update_notion.py",
                "--delta",
                str(delta_path),
                "--out",
                str(out_path),
            ]

            with patch("update_notion.NotionClient", return_value=fake_client), \
                 patch("update_notion.resolve_data_source_id", return_value="ds-1"), \
                 patch("update_notion.query_all_pages", side_effect=fake_query_all_pages), \
                 patch("sys.argv", argv), \
                 patch.dict(update_notion.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                with self.assertRaisesRegex(RuntimeError, "Notion error 400: boom"):
                    update_notion.main()

            self.assertTrue((out_path / "notion_writeback_summary.csv").exists())
            self.assertTrue((out_path / "notion_writeback_log.csv").exists())
            self.assertTrue((out_path / "notion_writeback_status.json").exists())

    def test_main_skips_noop_rows_without_patching(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            delta_path = tmp_path / "delta.csv"
            out_path = tmp_path / "out"
            with open(delta_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Match Key", "Raw ID", "Best Email", "Persona Signal - Family Office", "Persona Signal - Fintech", "Allocator Score", "Access Score", "Degree", "Alumni Signal"])
                writer.writeheader()
                writer.writerow({
                    "Match Key": "raw:abc",
                    "Raw ID": "abc",
                    "Best Email": "",
                    "Persona Signal - Family Office": 1,
                    "Persona Signal - Fintech": 2,
                    "Allocator Score": 3,
                    "Access Score": 4,
                    "Degree": 5,
                    "Alumni Signal": "Cal",
                })

            fake_client = FakeClient()

            def fake_query_all_pages(client, data_source_id, on_page=None):
                pages = [
                    {
                        "id": "page-1",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "abc", "text": {"content": "abc"}}]},
                            "Best Email": {"type": "email", "email": ""},
                            "Persona Signal - Family Office": {"type": "number", "number": 1},
                            "Persona Signal - Fintech": {"type": "number", "number": 2},
                            "Allocator Score": {"type": "number", "number": 3},
                            "Access Score": {"type": "number", "number": 4},
                            "Degree": {"type": "number", "number": 5},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "Cal", "text": {"content": "Cal"}}]},
                        },
                    }
                ]
                if on_page is not None:
                    on_page(1, 1)
                return pages

            argv = [
                "update_notion.py",
                "--delta",
                str(delta_path),
                "--out",
                str(out_path),
            ]

            with patch("update_notion.NotionClient", return_value=fake_client), \
                 patch("update_notion.resolve_data_source_id", return_value="ds-1"), \
                 patch("update_notion.query_all_pages", side_effect=fake_query_all_pages), \
                 patch("sys.argv", argv), \
                 patch.dict(update_notion.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                update_notion.main()

            self.assertEqual(fake_client.patches, [])
            summary = (out_path / "notion_writeback_summary.csv").read_text(encoding="utf-8")
            self.assertIn("noop", summary)
            status = json.loads((out_path / "notion_writeback_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["noop_rows"], 1)
            self.assertEqual(status["updated_rows"], 0)

    def test_main_prefers_raw_id_over_ambiguous_email(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            delta_path = tmp_path / "delta.csv"
            out_path = tmp_path / "out"
            with open(delta_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Match Key", "Raw ID", "Best Email", "Persona Signal - Family Office", "Persona Signal - Fintech", "Allocator Score", "Access Score", "Degree", "Alumni Signal"])
                writer.writeheader()
                writer.writerow({
                    "Match Key": "raw:abc",
                    "Raw ID": "abc",
                    "Best Email": "shared@example.com",
                    "Persona Signal - Family Office": 7,
                    "Persona Signal - Fintech": 8,
                    "Allocator Score": 9,
                    "Access Score": 1,
                    "Degree": 2,
                    "Alumni Signal": "Cal",
                })

            fake_client = FakeClient()

            def fake_query_all_pages(client, data_source_id, on_page=None):
                pages = [
                    {
                        "id": "page-abc",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "abc", "text": {"content": "abc"}}]},
                            "Best Email": {"type": "email", "email": "shared@example.com"},
                            "Persona Signal - Family Office": {"type": "number", "number": 0},
                            "Persona Signal - Fintech": {"type": "number", "number": 0},
                            "Allocator Score": {"type": "number", "number": 0},
                            "Access Score": {"type": "number", "number": 0},
                            "Degree": {"type": "number", "number": 0},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "", "text": {"content": ""}}]},
                        },
                    },
                    {
                        "id": "page-email-1",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "other-1", "text": {"content": "other-1"}}]},
                            "Best Email": {"type": "email", "email": "shared@example.com"},
                            "Persona Signal - Family Office": {"type": "number", "number": 0},
                            "Persona Signal - Fintech": {"type": "number", "number": 0},
                            "Allocator Score": {"type": "number", "number": 0},
                            "Access Score": {"type": "number", "number": 0},
                            "Degree": {"type": "number", "number": 0},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "", "text": {"content": ""}}]},
                        },
                    },
                    {
                        "id": "page-email-2",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "other-2", "text": {"content": "other-2"}}]},
                            "Best Email": {"type": "email", "email": "shared@example.com"},
                            "Persona Signal - Family Office": {"type": "number", "number": 0},
                            "Persona Signal - Fintech": {"type": "number", "number": 0},
                            "Allocator Score": {"type": "number", "number": 0},
                            "Access Score": {"type": "number", "number": 0},
                            "Degree": {"type": "number", "number": 0},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "", "text": {"content": ""}}]},
                        },
                    },
                ]
                if on_page is not None:
                    on_page(1, 3)
                return pages

            argv = [
                "update_notion.py",
                "--delta",
                str(delta_path),
                "--out",
                str(out_path),
            ]

            with patch("update_notion.NotionClient", return_value=fake_client), \
                 patch("update_notion.resolve_data_source_id", return_value="ds-1"), \
                 patch("update_notion.query_all_pages", side_effect=fake_query_all_pages), \
                 patch("sys.argv", argv), \
                 patch.dict(update_notion.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                update_notion.main()

            self.assertEqual(len(fake_client.patches), 1)
            self.assertEqual(fake_client.patches[0][0], "/pages/page-abc")
            status = json.loads((out_path / "notion_writeback_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["updated_rows"], 1)
            self.assertEqual(status["ambiguous_rows"], 0)

    def test_main_skips_duplicate_raw_ids_and_finishes(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            delta_path = tmp_path / "delta.csv"
            out_path = tmp_path / "out"
            with open(delta_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Match Key", "Raw ID", "Best Email", "Persona Signal - Family Office", "Persona Signal - Fintech", "Allocator Score", "Access Score", "Degree", "Alumni Signal"])
                writer.writeheader()
                writer.writerow({
                    "Match Key": "raw:abc",
                    "Raw ID": "abc",
                    "Best Email": "",
                    "Persona Signal - Family Office": 1,
                    "Persona Signal - Fintech": 2,
                    "Allocator Score": 3,
                    "Access Score": 4,
                    "Degree": 5,
                    "Alumni Signal": "Cal",
                })

            fake_client = FakeClient()

            def fake_query_all_pages(client, data_source_id, on_page=None):
                pages = [
                    {
                        "id": "page-1",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "abc", "text": {"content": "abc"}}]},
                            "Best Email": {"type": "email", "email": ""},
                            "Persona Signal - Family Office": {"type": "number", "number": 1},
                            "Persona Signal - Fintech": {"type": "number", "number": 2},
                            "Allocator Score": {"type": "number", "number": 3},
                            "Access Score": {"type": "number", "number": 4},
                            "Degree": {"type": "number", "number": 5},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "Cal", "text": {"content": "Cal"}}]},
                        },
                    },
                    {
                        "id": "page-2",
                        "properties": {
                            "Raw ID": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "abc", "text": {"content": "abc"}}]},
                            "Best Email": {"type": "email", "email": ""},
                            "Persona Signal - Family Office": {"type": "number", "number": 9},
                            "Persona Signal - Fintech": {"type": "number", "number": 8},
                            "Allocator Score": {"type": "number", "number": 7},
                            "Access Score": {"type": "number", "number": 6},
                            "Degree": {"type": "number", "number": 5},
                            "Alumni Signal": {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": "CBS", "text": {"content": "CBS"}}]},
                        },
                    },
                ]
                if on_page is not None:
                    on_page(1, 2)
                return pages

            argv = [
                "update_notion.py",
                "--delta",
                str(delta_path),
                "--out",
                str(out_path),
            ]

            with patch("update_notion.NotionClient", return_value=fake_client), \
                 patch("update_notion.resolve_data_source_id", return_value="ds-1"), \
                 patch("update_notion.query_all_pages", side_effect=fake_query_all_pages), \
                 patch("sys.argv", argv), \
                 patch.dict(update_notion.os.environ, {"NOTION_API_KEY": "test-token"}, clear=False):
                update_notion.main()

            self.assertEqual(fake_client.patches, [])
            duplicates = (out_path / "notion_writeback_duplicates.csv").read_text(encoding="utf-8")
            self.assertIn("abc", duplicates)
            status = json.loads((out_path / "notion_writeback_status.json").read_text(encoding="utf-8"))
            self.assertTrue(status["duplicate_lookup_preview"])
            self.assertEqual(status["phase"], "done")
            self.assertEqual(status["ambiguous_rows"], 1)
            self.assertTrue((out_path / "notion_writeback_summary.csv").exists())
            self.assertTrue((out_path / "notion_writeback_log.csv").exists())


if __name__ == "__main__":
    unittest.main()
