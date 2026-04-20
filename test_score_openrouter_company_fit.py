import unittest

import pandas as pd

from score_openrouter import compact_record, deterministic_mock, normalize_direct_value
from utils import derive_alumni_signal_from_education


class ScoreOpenRouterCompanyFitTests(unittest.TestCase):
    def test_compact_record_keeps_current_company_context_without_truncation(self):
        row = pd.Series({
            "Match Key": "raw:abc",
            "URN": "12345",
            "Raw ID": "abc",
            "Best Email": "",
            "Full Name": "Alice Example",
            "Location": "Sao Paulo",
            "Current Company": "Example Co",
            "Current Title": "Partner",
            "Headline": "Headline",
            "Industry": "Finance",
            "Mutual Count": 3,
            "Followers": 91,
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

        self.assertEqual(record["member_id"], "12345")
        self.assertNotIn("best_email", record)
        self.assertEqual(record["current_company"], "Example Co")
        self.assertEqual(record["current_position"], "Partner")
        # current_company_description is the Organization 1 Description, kept in full.
        self.assertEqual(len(record["current_company_description"]), 900)
        # No secondary role in this row -> additional_role is None.
        self.assertIsNone(record["additional_role"])
        self.assertNotIn("company_context", record)
        self.assertNotIn("position_2_description", record)
        self.assertNotIn("position_3_description", record)

    def test_deterministic_mock_returns_company_fit(self):
        records = [{
            "member_id": "12345",
            "_match_key": "raw:abc",
            "_raw_id": "abc",
            "full_name": "Alice Example",
            "current_company": "Family Office Partners",
            "current_position": "Partner",
            "location": "Sao Paulo",
            "headline": "Headline",
            "current_industry": "Finance",
            "mutual_count": 2,
            "degree": 3,
            "summary": "Summary",
            "alumni_signal": "Cal",
            "additional_role": None,
            "current_company_description": "",
            "current_position_description": "",
        }]

        out = deterministic_mock(records)

        self.assertEqual(len(out), 1)
        self.assertIn("company_fit", out[0])
        self.assertGreaterEqual(out[0]["company_fit"], 1)

    def test_deterministic_mock_supports_direct_mode(self):
        records = [{
            "member_id": "12345",
            "_match_key": "raw:abc",
            "_raw_id": "abc",
            "full_name": "Alice Example",
            "current_company": "Family Office Partners",
            "current_position": "Partner",
            "location": "Sao Paulo",
            "headline": "Partner",
            "current_industry": "Finance",
            "mutual_count": 2,
            "degree": 3,
            "summary": "Summary",
            "alumni_signal": "Cal",
            "additional_role": None,
            "current_company_description": "",
            "current_position_description": "",
        }]

        out = deterministic_mock(records, scoring_mode="autopilot_direct_100")

        self.assertEqual(len(out), 1)
        self.assertIn("family_office_relevance", out[0])
        self.assertIn(out[0]["company_fit"], {7, 14, 21, 28, 35})

    def test_normalize_direct_value_maps_bucket_ordinals(self):
        self.assertEqual(normalize_direct_value("company_fit", 1), 7)
        self.assertEqual(normalize_direct_value("company_fit", 5), 35)
        self.assertEqual(normalize_direct_value("fintech_relevance", 3), 18)
        self.assertEqual(normalize_direct_value("allocator_power", 0), 4)
        self.assertEqual(normalize_direct_value("access", 15), 12)
        # 24 is in fintech_relevance[3] under the new maps -> fallback picks role_fit[3] = 4.
        self.assertEqual(normalize_direct_value("role_fit", 24), 4)

    def test_normalize_direct_value_infers_missing_role_fit(self):
        record = {"current_position": "Partner", "headline": "Partner at Fund"}
        self.assertEqual(normalize_direct_value("role_fit", None, record), 5)

    def test_derive_alumni_signal_detects_cbs_synonym(self):
        signal = derive_alumni_signal_from_education(
            {
                "Education 1 School": "CBS",
                "Education 2 School": "Haas School of Business",
            }
        )
        self.assertEqual(signal, "Cal+CBS")


if __name__ == "__main__":
    unittest.main()
