import unittest
from collections import Counter

from reason_catalog import (
    STANDARD_REASON_OPTIONS,
    categorize_reason,
    normalize_reason,
    reason_counter,
    reason_suggestions,
)


class TestReasonCatalog(unittest.TestCase):
    def test_normalize_reason_empty_inputs(self):
        self.assertEqual(normalize_reason(""), "")
        self.assertEqual(normalize_reason(None), "")
        self.assertEqual(normalize_reason("   "), "")

    def test_normalize_reason_exact_mapping(self):
        self.assertEqual(normalize_reason("friends"), "friend")
        self.assertEqual(normalize_reason("FO"), "family office")
        self.assertEqual(normalize_reason("Fam Office"), "family office")
        self.assertEqual(normalize_reason("service provider to fintechs"), "service provider")

    def test_normalize_reason_contains_mapping(self):
        self.assertEqual(normalize_reason("some random defi project"), "competitor")
        self.assertEqual(normalize_reason("working at paypal right now"), "too broad/too big")
        self.assertEqual(normalize_reason("too old for us"), "not qualified")
        self.assertEqual(normalize_reason("credit infra startup"), "better channel than buyer")

    def test_normalize_reason_no_mapping(self):
        self.assertEqual(normalize_reason("completely unknown reason"), "completely unknown reason")
        self.assertEqual(normalize_reason("Another Unknown"), "Another Unknown")

    def test_normalize_reason_stripping_and_lowercase(self):
        self.assertEqual(normalize_reason("  Friends  "), "friend")
        self.assertEqual(normalize_reason("DEFI"), "competitor")
        self.assertEqual(normalize_reason("  TOO BIG  "), "too broad/too big")

    def test_categorize_reason_known_categories(self):
        self.assertEqual(categorize_reason("friend"), "relationship_override")
        self.assertEqual(categorize_reason("FO"), "buyer_fit_positive")  # Testing it normalizes first
        self.assertEqual(categorize_reason("service provider"), "service_provider")
        self.assertEqual(categorize_reason("defi"), "company_mismatch")
        self.assertEqual(categorize_reason("pe"), "allocator_mismatch")

    def test_categorize_reason_unknown_category(self):
        self.assertEqual(categorize_reason("some weird reason"), "other")
        self.assertEqual(categorize_reason(""), "other")
        self.assertEqual(categorize_reason(None), "other")

    def test_reason_suggestions_empty_or_none(self):
        self.assertEqual(reason_suggestions(None), STANDARD_REASON_OPTIONS)
        self.assertEqual(reason_suggestions(""), STANDARD_REASON_OPTIONS)
        self.assertEqual(reason_suggestions("   "), STANDARD_REASON_OPTIONS)

    def test_reason_suggestions_known_reason(self):
        # Push to front when it's a known standard option
        suggestions = reason_suggestions("family office")
        self.assertEqual(suggestions[0], "family office")
        self.assertEqual(len(suggestions), len(STANDARD_REASON_OPTIONS))

        # Test that normalization triggers the push to front
        suggestions = reason_suggestions("FO")
        self.assertEqual(suggestions[0], "family office")
        self.assertEqual(len(suggestions), len(STANDARD_REASON_OPTIONS))

    def test_reason_suggestions_unknown_reason(self):
        # Unknown reason just returns the standard options list without modifying it
        suggestions = reason_suggestions("completely unknown")
        self.assertEqual(suggestions, STANDARD_REASON_OPTIONS)

    def test_reason_counter(self):
        values = [
            "friends",
            "FO",
            "family office",
            "completely unknown",
            "",
            None,
            "defi",
            "   "
        ]
        counter = reason_counter(values)

        # 'friends' -> 'friend'
        self.assertEqual(counter["friend"], 1)
        # 'FO' and 'family office' -> 'family office'
        self.assertEqual(counter["family office"], 2)
        # 'completely unknown' -> 'completely unknown'
        self.assertEqual(counter["completely unknown"], 1)
        # 'defi' -> 'competitor'
        self.assertEqual(counter["competitor"], 1)
        # Empty string, None, and whitespace are ignored
        self.assertNotIn("", counter)
        self.assertNotIn(None, counter)

if __name__ == "__main__":
    unittest.main()
