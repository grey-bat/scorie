import unittest
from pathlib import Path

import pandas as pd

from company_backfill import build_company_source_index, enrich_company_context, load_company_sources, select_company_backfill_candidates


class CompanyBackfillTests(unittest.TestCase):
    def test_load_company_sources_finds_sample_modes(self):
        repo_root = Path(__file__).resolve().parent
        sources = load_company_sources(repo_root)
        self.assertFalse(sources.empty)
        self.assertTrue({"nodata", "visit", "credits"}.intersection(set(sources["backfill_mode"].dropna().unique())))

    def test_enrich_company_context_uses_sample_backfill(self):
        repo_root = Path(__file__).resolve().parent
        sources = build_company_source_index(repo_root)
        self.assertFalse(sources.empty)
        source_row = sources.iloc[0]
        base = pd.DataFrame([
            {
                "Raw ID": source_row.get("Raw ID", source_row.get("id", "")),
                "Best Email": "",
                "LinkedIn URL": source_row.get("LinkedIn URL", source_row.get("profile_url", "")),
                "Full Name": source_row.get("full_name", ""),
                "Current Company": "",
                "Current Title": "",
                "Industry": "",
                "Organization 1 Description": "",
                "Organization 1 Website": "",
                "Organization 1 Domain": "",
            }
        ])

        enriched, report = enrich_company_context(base, repo_root)

        self.assertEqual(len(enriched), 1)
        self.assertIn(enriched.iloc[0]["Company Context Source"], {"visit", "credits", "nodata", "native"})
        self.assertNotEqual(enriched.iloc[0]["Company Context Source"], "native")
        self.assertNotEqual(enriched.iloc[0]["Current Company"], "")
        self.assertFalse(report.empty)

    def test_select_company_backfill_candidates_filters_to_close_rows(self):
        scored = pd.DataFrame([
            {"Match Key": "raw:1", "weighted_score": 80, "Company Backfill Needed": "yes"},
            {"Match Key": "raw:2", "weighted_score": 40, "Company Backfill Needed": "yes"},
            {"Match Key": "raw:3", "weighted_score": 90, "Company Backfill Needed": "no"},
        ])

        candidates = select_company_backfill_candidates(scored, min_weighted_score=50)

        self.assertEqual(list(candidates["Match Key"]), ["raw:1"])


if __name__ == "__main__":
    unittest.main()
