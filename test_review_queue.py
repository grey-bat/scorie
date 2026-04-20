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
                "Reason": "friend",
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
        self.assertIn("friend", queue.iloc[1]["Reason Suggestions"])

    def test_build_review_queue_supports_direct_score_track(self):
        scored = pd.DataFrame([
            {
                "Full Name": "Low Direct",
                "Current Company": "A",
                "Current Title": "Title",
                "company_fit": 14,
                "family_office_relevance": 6,
                "fintech_relevance": 12,
                "allocator_power": 8,
                "access": 5,
                "role_fit": 2,
                "direct_score": 41,
                "score_track": "autopilot_direct_100",
            },
            {
                "Full Name": "High Direct",
                "Current Company": "B",
                "Current Title": "Title",
                "company_fit": 28,
                "family_office_relevance": 15,
                "fintech_relevance": 24,
                "allocator_power": 12,
                "access": 8,
                "role_fit": 3,
                "direct_score": 75,
                "score_track": "autopilot_direct_100",
            },
        ])

        queue = build_review_queue(scored)

        self.assertEqual(queue.iloc[0]["Full Name"], "High Direct")
        self.assertEqual(queue.iloc[0]["direct_score"], 75)
        self.assertEqual(queue.iloc[0]["score_band"], "qualified")


if __name__ == "__main__":
    unittest.main()
