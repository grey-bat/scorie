import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from composite_formula import DEFAULT_DIRECT_POINT_MAPS, direct_score, load_composite_config


class CompositeFormulaTests(unittest.TestCase):
    def test_direct_score_sums_to_100(self):
        row = {name: values[-1] for name, values in DEFAULT_DIRECT_POINT_MAPS.items()}
        self.assertEqual(direct_score(row), 100)

    def test_load_composite_config_includes_direct_point_maps(self):
        config = load_composite_config()
        self.assertIn("company_fit", config.direct_point_maps)
        self.assertEqual(config.direct_point_maps["role_fit"], [1, 2, 3, 4, 5])

    def test_load_composite_config_accepts_direct_rubric_without_weights(self):
        rubric = """## Direct Point Maps
- company_fit = 6, 12, 18, 24, 30
- family_office_relevance = 3, 6, 9, 12, 15
- fintech_relevance = 5, 10, 15, 20, 25
- allocator_power = 3, 6, 9, 12, 15
- access = 2, 4, 6, 8, 10
- role_fit = 1, 2, 3, 4, 5

## Score Bands
- qualified = 75-100
- nearly_qualified = 50-74
- little_qualified = 25-49
- totally_unqualified = 0-24
"""
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "rubric.md"
            path.write_text(rubric, encoding="utf-8")
            config = load_composite_config(path)
        self.assertEqual(config.direct_point_maps["company_fit"][-1], 30)
        self.assertEqual(config.score_bands["qualified"]["max"], 100)


if __name__ == "__main__":
    unittest.main()
