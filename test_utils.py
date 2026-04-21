import unittest
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
