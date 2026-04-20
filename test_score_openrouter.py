import time
import unittest

from score_openrouter import extract_assistant_content, maybe_recover_capacity, remap_batch_results
from utils import canonicalize_identifier


class ScoreOpenRouterTests(unittest.TestCase):
    def test_canonicalize_identifier_handles_accents_and_encoding(self):
        self.assertEqual(canonicalize_identifier("germano-cs-araújo"), "germanocsaraujo")
        self.assertEqual(canonicalize_identifier("germano-cs-ara%C3%BAjo"), "germanocsaraujo")
        self.assertEqual(canonicalize_identifier("raphael-gonçalves-simcsik-147b323b"), "raphaelgoncalvessimcsik147b323b")
        self.assertEqual(canonicalize_identifier("renato aristeu"), "renatoaristeu")

    def test_remap_batch_results_accepts_canonical_equivalents(self):
        records = [
            {"member_id": "1001", "_match_key": "raw:germano-cs-araújo", "_raw_id": "germano-cs-araújo"},
            {"member_id": "1002", "_match_key": "raw:raphael-gonçalves-simcsik-147b323b", "_raw_id": "raphael-gonçalves-simcsik-147b323b"},
            {"member_id": "1003", "_match_key": "raw:renatoaristeu", "_raw_id": "renatoaristeu"},
        ]
        out = [
            {"URN": "1001", "fo_persona": 1, "ft_persona": 2, "allocator": 3, "access": 4},
            {"URN": "1003", "fo_persona": 5, "ft_persona": 4, "allocator": 3, "access": 2},
            {"URN": "1002", "fo_persona": 0, "ft_persona": 1, "allocator": 2, "access": 3},
        ]

        remapped, missing = remap_batch_results(records, out)

        self.assertEqual(missing, [])
        self.assertEqual([item["Match Key"] for item in remapped], [record["_match_key"] for record in records])
        self.assertEqual([item["fo_persona"] for item in remapped], [1, 0, 5])

    def test_remap_batch_results_returns_missing_for_partial_output(self):
        records = [
            {"member_id": "1001", "_match_key": "raw:a", "_raw_id": "a"},
            {"member_id": "1002", "_match_key": "raw:b", "_raw_id": "b"},
            {"member_id": "1003", "_match_key": "raw:c", "_raw_id": "c"},
        ]
        # Model only returned 2 of 3 records.
        out = [
            {"URN": "1001", "fo_persona": 1, "ft_persona": 2, "allocator": 3, "access": 4},
            {"URN": "1003", "fo_persona": 5, "ft_persona": 4, "allocator": 3, "access": 2},
        ]

        remapped, missing = remap_batch_results(records, out)

        self.assertEqual(len(remapped), 2)
        self.assertEqual([item["Match Key"] for item in remapped], ["raw:a", "raw:c"])
        self.assertEqual([rec["member_id"] for rec in missing], ["1002"])

    def test_maybe_recover_capacity_steps_back_up_after_cooldown(self):
        last_adjustment_at = time.monotonic() - 1000

        new_c, new_b, recovered = maybe_recover_capacity(
            current_concurrency=1,
            current_batch_size=4,
            initial_concurrency=3,
            initial_batch_size=8,
            last_adjustment_at=last_adjustment_at,
            recovery_delay=300,
        )

        self.assertTrue(recovered)
        self.assertEqual((new_c, new_b), (2, 5))

        new_c, new_b, recovered = maybe_recover_capacity(
            current_concurrency=3,
            current_batch_size=4,
            initial_concurrency=3,
            initial_batch_size=8,
            last_adjustment_at=last_adjustment_at,
            recovery_delay=300,
        )

        self.assertTrue(recovered)
        self.assertEqual((new_c, new_b), (3, 5))

    def test_extract_assistant_content_rejects_empty_or_missing_content(self):
        with self.assertRaisesRegex(RuntimeError, "missing assistant content"):
            extract_assistant_content({})

        with self.assertRaisesRegex(RuntimeError, "empty assistant content"):
            extract_assistant_content({"choices": [{"message": {"content": None}}]})


if __name__ == "__main__":
    unittest.main()
