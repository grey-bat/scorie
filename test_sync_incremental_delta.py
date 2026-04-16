import unittest
from unittest.mock import patch

import pandas as pd

from build_delta import build_scoring_frames
from sync_incremental_delta import filter_rows_needing_live_update


def make_full_row(match_key, raw_id, best_email, company, title, headline, stage="Booked", alumni_signal="Cal"):
    return {
        "Raw ID": raw_id,
        "Best Email": best_email,
        "Full Name": raw_id,
        "Current Company": company,
        "Current Title": title,
        "Headline": headline,
        "Industry": "Finance",
        "Mutual Count": 3,
        "Summary": "summary",
        "Berkeley Signal": "Cal",
        "Columbia Signal": "",
        "Alumni Signal": alumni_signal,
        "Persona Signal - Family Office": 1,
        "Persona Signal - Fintech": 2,
        "Allocator Score": 3,
        "Access Score": 4,
        "Stage": stage,
        "Position 1 Description": "",
        "Position 2 Description": "",
        "Position 3 Description": "",
        "Organization 1": "",
        "Organization 2": "",
        "Organization 3": "",
        "Organization 1 Title": "",
        "Organization 2 Title": "",
        "Organization 3 Title": "",
        "Organization 1 Description": "",
        "Organization 2 Description": "",
        "Organization 3 Description": "",
        "Distance": 3,
        "Match Key": match_key,
    }


def make_prepared_row(match_key, raw_id, best_email, degree, alumni_signal):
    return {
        "Match Key": match_key,
        "Raw ID": raw_id,
        "Best Email": best_email,
        "Full Name": raw_id,
        "Current Company": "Company",
        "Current Title": "Title",
        "Headline": "Headline",
        "Industry": "Finance",
        "Mutual Count": 3,
        "Summary": "summary",
        "Alumni Signal": alumni_signal,
        "Position 1 Description": "",
        "Position 2 Description": "",
        "Position 3 Description": "",
        "Organization 1": "",
        "Organization 2": "",
        "Organization 3": "",
        "Organization 1 Title": "",
        "Organization 2 Title": "",
        "Organization 3 Title": "",
        "Organization 1 Description": "",
        "Organization 2 Description": "",
        "Organization 3 Description": "",
        "Degree": degree,
    }


def make_scores_row(match_key, raw_id, best_email, fo=1, ft=2, allocator=3, access=4):
    return {
        "Match Key": match_key,
        "Raw ID": raw_id,
        "Best Email": best_email,
        "fo_persona": fo,
        "ft_persona": ft,
        "allocator": allocator,
        "access": access,
    }


def make_notion_page(page_id, raw_id, best_email="", fo=None, ft=None, allocator=None, access=None, degree=None, alumni_signal=""):
    def rich_text(value):
        return {"type": "rich_text", "rich_text": [{"type": "text", "plain_text": value, "text": {"content": value}}] if value else []}

    def number(value):
        return {"type": "number", "number": value}

    return {
        "id": page_id,
        "properties": {
            "Raw ID": rich_text(raw_id),
            "Best Email": {"type": "email", "email": best_email},
            "Persona Signal - Family Office": number(fo),
            "Persona Signal - Fintech": number(ft),
            "Allocator Score": number(allocator),
            "Access Score": number(access),
            "Degree": number(degree),
            "Alumni Signal": rich_text(alumni_signal),
        },
    }


class SyncIncrementalDeltaTests(unittest.TestCase):
    def test_build_scoring_frames_include_all_keeps_every_scored_row(self):
        full = pd.DataFrame([
            make_full_row("raw:abc", "abc", "", "Keep Co", "Title", "Headline"),
            make_full_row("raw:def", "def", "", "Other Co", "Title", "Headline", alumni_signal=""),
        ])
        prepared = pd.DataFrame([
            make_prepared_row("raw:abc", "abc", "", 3, "Cal"),
            make_prepared_row("raw:def", "def", "", 3, ""),
        ])
        scores = pd.DataFrame([
            make_scores_row("raw:abc", "abc", ""),
            make_scores_row("raw:def", "def", ""),
        ])

        output, merged, delta, summary, changed_mask = build_scoring_frames(full, prepared, scores, include_all=True)

        self.assertEqual(len(output), 2)
        self.assertEqual(len(merged), 2)
        self.assertEqual(len(delta), 2)
        self.assertEqual(int(summary.set_index("metric").loc["changed_rows", "value"]), 0)
        self.assertEqual(list(changed_mask), [False, False])

    def test_filter_rows_needing_live_update_only_returns_incomplete_rows(self):
        delta = pd.DataFrame([
            {
                "Match Key": "raw:abc",
                "Raw ID": "abc",
                "Best Email": "",
                "Current Company": "Keep Co",
                "Current Title": "Title",
                "Headline": "Headline",
                "Degree": 3,
                "Alumni Signal": "Cal",
                "Persona Signal - Family Office": 1,
                "Persona Signal - Fintech": 2,
                "Allocator Score": 3,
                "Access Score": 4,
            },
            {
                "Match Key": "raw:def",
                "Raw ID": "def",
                "Best Email": "",
                "Current Company": "Other Co",
                "Current Title": "Title",
                "Headline": "Headline",
                "Degree": 3,
                "Alumni Signal": "CBS",
                "Persona Signal - Family Office": 1,
                "Persona Signal - Fintech": 2,
                "Allocator Score": 3,
                "Access Score": 4,
            },
        ])
        schema = {
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
        pages = [
            make_notion_page("page-abc", "abc", fo=1, ft=2, allocator=3, access=4, degree=3, alumni_signal="Cal"),
            make_notion_page("page-def", "def", fo=None, ft=None, allocator=None, access=None, degree=None, alumni_signal=""),
        ]

        with patch("sync_incremental_delta.query_all_pages", return_value=pages):
            filtered, report = filter_rows_needing_live_update(delta, object(), "ds-1", schema)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["Match Key"], "raw:def")
        statuses = report.set_index("Match Key")["status"].to_dict()
        self.assertEqual(statuses["raw:abc"], "already_complete")
        self.assertEqual(statuses["raw:def"], "needs_update")


if __name__ == "__main__":
    unittest.main()
