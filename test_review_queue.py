import unittest

import pandas as pd

from review_queue import build_review_queue


class ReviewQueueTests(unittest.TestCase):
    def test_build_review_queue_orders_by_company_fit_and_keeps_reason(self):
        scored = pd.DataFrame([
            {
                "Full Name": "Low CoFit",
                "Current Company": "A",
                "Current Title": "Title",
                "fo_persona": 1,
                "ft_persona": 1,
                "allocator": 1,
                "access": 1,
                "company_fit": 1,
                "Reason": "custom",
            },
            {
                "Full Name": "High CoFit",
                "Current Company": "B",
                "Current Title": "Title",
                "fo_persona": 1,
                "ft_persona": 1,
                "allocator": 1,
                "access": 1,
                "company_fit": 5,
                "Reason": "",
            },
        ])

        queue = build_review_queue(scored)

        self.assertEqual(queue.iloc[0]["Full Name"], "High CoFit")
        self.assertEqual(queue.iloc[0]["Status"], "")
        self.assertIn("Reason Suggestions", queue.columns)
        self.assertIn("custom", queue.iloc[1]["Reason Suggestions"])


if __name__ == "__main__":
    unittest.main()
