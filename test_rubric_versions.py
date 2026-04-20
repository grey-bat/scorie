import tempfile
import unittest
from pathlib import Path

from rubric_versions import create_rubric_version, ensure_rubric_store, promote_rubric_version


class RubricVersionTests(unittest.TestCase):
    def test_create_version_writes_timestamped_file_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            version = create_rubric_version(
                base_dir=tmp,
                text="# Rubric\n",
                iteration=1,
                stop_mode="iterations",
            )
            rubrics_dir, manifest = ensure_rubric_store(tmp)
            self.assertTrue(version.path.exists())
            self.assertTrue(manifest.exists())
            self.assertTrue(version.path.parent == rubrics_dir)
            self.assertIn("rubric_v", version.path.name)

    def test_promote_copies_version_to_active_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            version = create_rubric_version(base_dir=tmp, text="# Active\n")
            active = Path(tmp) / "scoring_rubric.md"
            promote_rubric_version(version.path, active)
            self.assertEqual(active.read_text(encoding="utf-8"), "# Active\n")


if __name__ == "__main__":
    unittest.main()
