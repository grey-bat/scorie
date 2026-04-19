import unittest

import pandas as pd

from score_openrouter import compact_record, deterministic_mock


class ScoreOpenRouterCompanyFitTests(unittest.TestCase):
    def test_compact_record_trims_company_context(self):
        row = pd.Series({
            "Match Key": "raw:abc",
            "Raw ID": "abc",
            "Best Email": "",
            "Full Name": "Alice Example",
            "Current Company": "Example Co",
            "Current Title": "Partner",
            "Headline": "Headline",
            "Industry": "Finance",
            "Mutual Count": 3,
            "Degree": 2,
            "Summary": "Summary",
            "Alumni Signal": "Cal",
            "Company Context Source": "visit",
            "Company Backfill Needed": "yes",
            "Company Backfill Reason": "missing domain",
            "Company Context Score": 9,
            "Organization 1": "Example Co",
            "Organization 1 Title": "Partner",
            "Organization 1 Description": "x" * 900,
            "Organization 1 Website": "https://example.com",
            "Organization 1 Domain": "example.com",
            "Position 1 Description": "",
            "Position 2 Description": "",
            "Position 3 Description": "",
        })

        record = compact_record(row)

        self.assertEqual(record["company_context_source"], "visit")
        self.assertEqual(len(record["company_context"]), 1)
        self.assertLessEqual(len(record["company_context"][0]["description"]), 700)

    def test_deterministic_mock_returns_company_fit(self):
        records = [{
            "id": "raw:abc",
            "raw_id": "abc",
            "best_email": "",
            "full_name": "Alice Example",
            "current_company": "Family Office Partners",
            "current_title": "Partner",
            "headline": "Headline",
            "industry": "Finance",
            "mutual_count": 2,
            "degree": 3,
            "summary": "Summary",
            "alumni_signal": "Cal",
            "company_context": [],
            "position_1_description": "",
            "position_2_description": "",
            "position_3_description": "",
        }]

        out = deterministic_mock(records)

        self.assertEqual(len(out), 1)
        self.assertIn("company_fit", out[0])
        self.assertGreaterEqual(out[0]["company_fit"], 1)


if __name__ == "__main__":
    unittest.main()
