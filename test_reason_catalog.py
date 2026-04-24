import unittest
from reason_catalog import normalize_reason

class TestReasonCatalog(unittest.TestCase):
    def test_normalize_reason_exact_mapping(self):
        cases = [
            ("friends", "friend"),
            ("fo", "family office"),
            ("fam office", "family office"),
            ("service provider to fintechs", "service provider"),
            ("crypto competitor", "crypto"),
            ("investor", "investor only"),
            ("pe", "investor only"),
            ("too big", "too broad/too big"),
            ("better channel than allocator", "better channel than buyer"),
            ("broken link", "broken link rewritten"),
            ("company fit weak", "missing company context"),
        ]
        for input_str, expected in cases:
            with self.subTest(input_str=input_str):
                self.assertEqual(normalize_reason(input_str), expected)

    def test_normalize_reason_contains_mapping(self):
        cases = [
            ("is a friend of mine", "friend"),
            ("our family office department", "family office"),
            ("the flip script here", "service provider"),
            ("we are a competitor", "competitor"),
            ("uses paypal for payments", "too broad/too big"),
            ("this is too old", "not qualified"),
            ("not fintech at all", "company mismatch"),
            ("works in real estate", "company mismatch"),
            ("deals with credit infra", "better channel than buyer"),
            ("uses fireblocks", "better channel than buyer"),
        ]
        for input_str, expected in cases:
            with self.subTest(input_str=input_str):
                self.assertEqual(normalize_reason(input_str), expected)

    def test_normalize_reason_whitespace_and_case(self):
        cases = [
            ("FRIENDS", "friend"),
            ("Family Office", "family office"),
            ("FO", "family office"),
            ("  friend  ", "friend"),
            ("too   big", "too broad/too big"),
            ("\nfriend\t", "friend"),
            ("  ", ""),
        ]
        for input_str, expected in cases:
            with self.subTest(input_str=input_str):
                self.assertEqual(normalize_reason(input_str), expected)

    def test_normalize_reason_none_and_empty(self):
        cases = [
            (None, ""),
            ("", ""),
        ]
        for input_str, expected in cases:
            with self.subTest(input_str=input_str):
                self.assertEqual(normalize_reason(input_str), expected)

    def test_normalize_reason_no_match(self):
        cases = [
            ("some random reason", "some random reason"),
            ("Unknown Reason", "Unknown Reason"),
        ]
        for input_str, expected in cases:
            with self.subTest(input_str=input_str):
                self.assertEqual(normalize_reason(input_str), expected)

if __name__ == "__main__":
    unittest.main()
