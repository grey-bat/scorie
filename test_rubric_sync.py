import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rubric_sync import sync_rubric_snapshot


class RubricSyncTests(unittest.TestCase):
    def test_sync_rubric_snapshot_parses_weights_and_bands(self):
        page_text = """
## Weights
- fo_persona = 0.18
- ft_persona = 0.12
- allocator = 0.15
- access = 0.15
- company_fit = 0.40

## Score Bands
- qualified = 75-100
- nearly_qualified = 50-74
- little_qualified = 25-49
- totally_unqualified = 0-24
""".strip()

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "scoring_rubric.md"
            with patch("rubric_sync.fetch_notion_page_text", return_value=("page-1", page_text)):
                snapshot = sync_rubric_snapshot(out_path=out_path)

            self.assertEqual(snapshot.page_id, "page-1")
            self.assertEqual(snapshot.config.weights["company_fit"], 0.40)
            self.assertEqual(snapshot.config.score_bands["qualified"]["min"], 75)
            self.assertTrue(out_path.exists())
            self.assertIn("## Weights", out_path.read_text(encoding="utf-8"))

    def test_sync_rubric_snapshot_rejects_missing_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "scoring_rubric.md"
            with patch("rubric_sync.fetch_notion_page_text", return_value=("page-1", "## Weights\n- fo_persona = 0.18")):
                with self.assertRaisesRegex(RuntimeError, "Score Bands"):
                    sync_rubric_snapshot(out_path=out_path)


if __name__ == "__main__":
    unittest.main()
