import tempfile
import unittest
from pathlib import Path

import pandas as pd

from eval_history import append_eval_history, load_eval_history
from regression_report import build_regression_report


class EvalHistoryTests(unittest.TestCase):
    def test_append_and_load_history(self):
        reviewed = pd.DataFrame([
            {
                "Match Key": "raw:abc",
                "Status": "Skip",
                "Reason": "company mismatch",
                "weighted_score": 95,
            }
        ])

        with tempfile.TemporaryDirectory() as tmp:
            snapshot = append_eval_history(reviewed, tmp)
            self.assertTrue(snapshot.exists())
            loaded = load_eval_history(tmp)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded.iloc[0]["Status"], "Skip")

    def test_regression_report_flags_skip_qualified_rows(self):
        candidate = pd.DataFrame([
            {
                "Match Key": "raw:abc",
                "fo_persona": 5,
                "ft_persona": 5,
                "allocator": 5,
                "access": 5,
                "company_fit": 5,
                "Status": "Skip",
                "Reason": "company mismatch",
            }
        ])
        history = pd.DataFrame([
            {
                "Match Key": "raw:abc",
                "Status": "Skip",
                "Reason": "company mismatch",
                "weighted_score": 95,
            }
        ])

        report = build_regression_report(candidate, history)

        self.assertFalse(report.empty)
        self.assertTrue(bool(report.iloc[0]["regression"]))


if __name__ == "__main__":
    unittest.main()
