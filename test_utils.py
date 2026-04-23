import unittest
from datetime import datetime, timezone
from utils import parse_ts

class TestUtils(unittest.TestCase):

    def test_parse_ts_invalid(self):
        """Test that parse_ts returns None for invalid date strings."""
        invalid_inputs = [
            "not a date",
            "2023-13-45",  # Invalid month and day
            "2023-01-01 25:00:00",  # Invalid time
            "random text",
            "",
            None,
            12345,  # Invalid type (though normalize_text might cast it, still shouldn't parse as date)
            "2023/1/1", # not in our custom formats
            "23-01-01",
        ]

        for invalid_input in invalid_inputs:
            with self.subTest(invalid_input=invalid_input):
                self.assertIsNone(parse_ts(invalid_input))

    def test_parse_ts_valid(self):
        """Test that parse_ts correctly parses valid date strings."""
        valid_cases = [
            # (input, expected_datetime)
            ("2023-05-15T10:30:00Z", datetime(2023, 5, 15, 10, 30, tzinfo=timezone.utc)),
            ("2023-05-15T10:30:00+00:00", datetime(2023, 5, 15, 10, 30, tzinfo=timezone.utc)),
            ("2023-05-15", datetime(2023, 5, 15, 0, 0, tzinfo=timezone.utc)),
            ("2023-05-15 10:30:00", datetime(2023, 5, 15, 10, 30, tzinfo=timezone.utc)),
            ("05/15/2023 10:30", datetime(2023, 5, 15, 10, 30, tzinfo=timezone.utc)),
            ("05/15/2023", datetime(2023, 5, 15, 0, 0, tzinfo=timezone.utc)),
        ]

        for input_str, expected_dt in valid_cases:
            with self.subTest(input_str=input_str):
                self.assertEqual(parse_ts(input_str), expected_dt)

if __name__ == '__main__':
from utils import canonicalize_identifier


class TestUtils(unittest.TestCase):
    def test_canonicalize_identifier(self):
        test_cases = [
            # Basic string
            ("John Doe", "johndoe"),
            ("CamelCase", "camelcase"),
            ("UPPERCASE", "uppercase"),

            # Empty / None / NaN
            (None, ""),
            ("", ""),
            ("   ", ""),
            (float("nan"), ""),

            # URL Encoded
            ("John%20Doe", "johndoe"),
            ("a%3Db", "ab"),  # %3D is '='
            ("caf%C3%A9", "cafe"), # URL-encoded utf-8 'é'

            # Unicode & Accents
            ("José", "jose"),
            ("naïve", "naive"),
            ("François", "francois"),
            ("München", "munchen"),
            ("ąśćżźń", "asczzn"), # Checking multiple diacritics

            # Punctuation & Special Characters
            ("a.b-c_d!e?f", "abcdef"),
            ("user@example.com", "userexamplecom"),
            ("hello #world!", "helloworld"),

            # Non-string inputs
            (12345, "12345"),
            (True, "true"),
            (12.34, "1234")
        ]

        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val, expected=expected):
                self.assertEqual(canonicalize_identifier(input_val), expected)

if __name__ == "__main__":
    unittest.main()
