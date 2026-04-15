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
            {"id": "raw:germano-cs-araújo"},
            {"id": "raw:raphael-gonçalves-simcsik-147b323b"},
            {"id": "raw:renatoaristeu"},
        ]
        out = [
            {"Match Key": "raw:germano-cs-ara%C3%BAjo", "fo_persona": 1, "ft_persona": 2, "allocator": 3, "access": 4},
            {"Match Key": "raw:renato aristeu", "fo_persona": 5, "ft_persona": 4, "allocator": 3, "access": 2},
            {"Match Key": "raw:raphael-goncalves-simcsik-147b323b", "fo_persona": 0, "ft_persona": 1, "allocator": 2, "access": 3},
        ]

        remapped = remap_batch_results(records, out)

        self.assertEqual([item["Match Key"] for item in remapped], [record["id"] for record in records])
        self.assertEqual([item["fo_persona"] for item in remapped], [1, 0, 5])

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
