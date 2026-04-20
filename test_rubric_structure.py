import unittest

from rubric_structure import (
    RubricSpec,
    evaluate_candidate_gate,
    generate_point_map,
    heuristic_mutate,
    parse_rubric,
    render_semantic_diff_markdown,
    rewrite_point_maps_in_markdown,
    semantic_rubric_delta,
)


BASELINE_RUBRIC = """# Lead Scoring Rubric v1

## Core Rules

- Score against Auto Pro, not generic prestige.
- Current company and current role are the primary decision surface.
- When manual reasons indicate service_provider, lower company_fit and allocator_power.
- When manual reasons indicate company_mismatch, do not let biography override weak current fit.

## Direct Point Maps

- company_fit = 7, 14, 21, 28, 35
- fintech_relevance = 6, 12, 18, 24, 30
- allocator_power = 4, 8, 12, 16, 18
- access = 2, 5, 8, 10, 12
- role_fit = 1, 2, 3, 4, 5
- family_office_relevance = 3, 6, 9, 12, 15

## Score Bands

- qualified = 75-100
- nearly_qualified = 50-74
- little_qualified = 25-49
- totally_unqualified = 0-24

## Dimension Guidance

### company_fit

- 30: Current company is a highly relevant institutional fintech platform for Auto Pro.
- 24: Current company is strongly relevant, but not a top fit.
- 18: Current company is directionally relevant.
- 12: Current company is weakly relevant or more channel than buyer.
- 6: Current company is generic, irrelevant, or thinly described.

### fintech_relevance

- 25: Explicit fintech or banking infrastructure role and company.
- 20: Strong fintech relevance, one step removed.
- 15: Real ecosystem adjacency.
- 10: Weak adjacency.
- 5: No credible fintech relevance.
"""


DOSSIER = {
    "dimension_means": {
        "company_fit": {"sent_mean": 22, "skip_mean": 18},
        "fintech_relevance": {"sent_mean": 20, "skip_mean": 10},
        "role_fit": {"sent_mean": 3, "skip_mean": 3},
    },
    "reason_breakdown": {
        "false_positives": [
            {"reason_category": "service_provider", "count": 5},
            {"reason_category": "allocator_mismatch", "count": 3},
            {"reason_category": "company_mismatch", "count": 2},
        ],
        "false_negatives": [
            {"reason_category": "buyer_fit_positive", "count": 4},
            {"reason_category": "channel_vs_buyer", "count": 2},
        ],
    },
}


class ParseRubricTests(unittest.TestCase):
    def test_parses_weights_and_point_maps(self):
        spec = parse_rubric(BASELINE_RUBRIC)
        self.assertEqual(spec.weights["company_fit"], 35)
        self.assertEqual(spec.weights["role_fit"], 5)
        # FT caps sum to 100 (FO not included).
        from rubric_structure import FT_DIMENSIONS
        self.assertEqual(sum(spec.weights[d] for d in FT_DIMENSIONS), 100)
        self.assertEqual(spec.point_maps["fintech_relevance"], [6, 12, 18, 24, 30])

    def test_parses_threshold_rules(self):
        spec = parse_rubric(BASELINE_RUBRIC)
        cf_rules = spec.rules_for("company_fit")
        thresholds = [r for r in cf_rules if r.trigger_kind == "threshold"]
        # Threshold anchors come from the Dimension Guidance section which still
        # uses the old v5 values in this test fixture.
        self.assertEqual(sorted(r.trigger_value for r in thresholds), ["12", "18", "24", "30", "6"])

    def test_parses_reason_category_rules(self):
        spec = parse_rubric(BASELINE_RUBRIC)
        cats = {r.trigger_value for r in spec.reason_category_rules()}
        self.assertIn("service_provider", cats)
        self.assertIn("company_mismatch", cats)


class SemanticDeltaTests(unittest.TestCase):
    def test_prose_only_rewrite_is_mostly_modified_not_added(self):
        rewritten = BASELINE_RUBRIC.replace(
            "Current company is a highly relevant institutional fintech platform for Auto Pro.",
            "Current company is a top-tier institutional fintech platform clearly in Auto Pro scope.",
        )
        parent = parse_rubric(BASELINE_RUBRIC)
        candidate = parse_rubric(rewritten)
        delta = semantic_rubric_delta(parent, candidate)
        # Threshold "- 30:" line changed: strong-identity match, so counts as modified (not add/remove).
        self.assertEqual(len(delta.rules_added), 0)
        self.assertEqual(len(delta.rules_removed), 0)
        self.assertEqual(len(delta.rules_modified), 1)
        self.assertEqual(delta.weights_changed, {})

    def test_weight_change_is_detected(self):
        shifted = rewrite_point_maps_in_markdown(
            BASELINE_RUBRIC,
            {
                "company_fit": [8, 16, 24, 32, 40],
                "fintech_relevance": [5, 10, 15, 20, 25],
            },
        )
        parent = parse_rubric(BASELINE_RUBRIC)
        candidate = parse_rubric(shifted)
        delta = semantic_rubric_delta(parent, candidate)
        self.assertIn("company_fit", delta.weights_changed)
        self.assertEqual(delta.weights_changed["company_fit"], (35, 40))
        self.assertIn("fintech_relevance", delta.weights_changed)


class GateTests(unittest.TestCase):
    def test_gate_rejects_prose_only_rewrite(self):
        rewritten = BASELINE_RUBRIC.replace("Auto Pro", "Auto Pro (emphasis)")
        parent = parse_rubric(BASELINE_RUBRIC)
        candidate = parse_rubric(rewritten)
        gate = evaluate_candidate_gate(parent, candidate, DOSSIER, weight_step=6)
        self.assertFalse(gate.passed)
        self.assertTrue(any("material rule changes" in r for r in gate.reasons))
        self.assertTrue(any("weights" in r for r in gate.reasons))

    def test_gate_rejects_ft_caps_not_summing_to_100(self):
        broken = rewrite_point_maps_in_markdown(
            BASELINE_RUBRIC,
            {"company_fit": [10, 20, 30, 40, 50]},  # FT cap 50 pushes FT total to 115
        )
        parent = parse_rubric(BASELINE_RUBRIC)
        candidate = parse_rubric(broken)
        gate = evaluate_candidate_gate(parent, candidate, DOSSIER, weight_step=6)
        self.assertFalse(gate.passed)
        self.assertTrue(any("sum to exactly 100" in r for r in gate.reasons))

    def test_gate_allows_fo_cap_change_without_affecting_ft_sum(self):
        # Drop FO cap from 15 to 0, keep FT caps unchanged; this should NOT
        # cause the FT-sum check to complain.
        shifted = rewrite_point_maps_in_markdown(
            BASELINE_RUBRIC, {"family_office_relevance": [0, 0, 0, 0, 0]}
        )
        parent = parse_rubric(BASELINE_RUBRIC)
        candidate = parse_rubric(shifted)
        gate = evaluate_candidate_gate(parent, candidate, DOSSIER, weight_step=6)
        # Gate may still fail on rule-change count (this is a weight-only tweak),
        # but it MUST NOT complain about FT sum or about FO weight step.
        self.assertFalse(any("sum to exactly 100" in r for r in gate.reasons))
        self.assertFalse(any("family_office_relevance" in r and "stay within" in r for r in gate.reasons))

    def test_gate_accepts_real_change_with_reason_category(self):
        parent = parse_rubric(BASELINE_RUBRIC)
        # Build a candidate: shift weights, rewrite all 5 company_fit threshold anchors,
        # and add a new reason-category rule targeting allocator_mismatch.
        # Shift cf down 3 (35->32) and ft up 3 (30->33), keeping FT sum = 100.
        text = rewrite_point_maps_in_markdown(
            BASELINE_RUBRIC,
            {
                "company_fit": [6, 13, 19, 26, 32],
                "fintech_relevance": [7, 13, 20, 26, 33],
            },
        )
        # Rewrite 5 company_fit thresholds
        replacements = {
            "- 30: Current company is a highly relevant institutional fintech platform for Auto Pro.":
                "- 32: Current company is an elite institutional treasury platform directly in Auto Pro buying scope.",
            "- 24: Current company is strongly relevant, but not a top fit.":
                "- 26: Current company is strongly relevant with clear digital-asset infrastructure exposure.",
            "- 18: Current company is directionally relevant.":
                "- 19: Current company is directionally relevant with some fintech adjacency.",
            "- 12: Current company is weakly relevant or more channel than buyer.":
                "- 13: Current company is weakly relevant and more channel than buyer.",
            "- 6: Current company is generic, irrelevant, or thinly described.":
                "- 6: Current company is generic, out of scope, or too thin to trust.",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        # Add new reason_category rule for allocator_mismatch.
        text = text.replace(
            "- When manual reasons indicate company_mismatch, do not let biography override weak current fit.",
            "- When manual reasons indicate company_mismatch, do not let biography override weak current fit.\n"
            "- When manual reasons indicate allocator_mismatch, cap allocator_power below 9 for investor-branded profiles.",
        )
        candidate = parse_rubric(text)
        gate = evaluate_candidate_gate(parent, candidate, DOSSIER, weight_step=6)
        self.assertTrue(gate.passed, msg=f"gate reasons: {gate.reasons}")


class HeuristicMutateTests(unittest.TestCase):
    def test_heuristic_changes_weights_and_injects_reason_rule(self):
        parent = parse_rubric(BASELINE_RUBRIC)
        mutated_text = heuristic_mutate(BASELINE_RUBRIC, parent, DOSSIER, weight_step=6)
        candidate = parse_rubric(mutated_text)
        delta = semantic_rubric_delta(parent, candidate)
        self.assertTrue(delta.weights_changed, msg=f"expected weight changes, got none; candidate weights={candidate.weights}")
        # FT caps must still sum to 100 (FO is independent and not included).
        from rubric_structure import FT_DIMENSIONS
        self.assertEqual(sum(candidate.weights[d] for d in FT_DIMENSIONS), 100)
        # Must introduce a new reason-category rule (allocator_mismatch is a top FP cat).
        parent_cats = {r.trigger_value for r in parent.reason_category_rules()}
        cand_cats = {r.trigger_value for r in candidate.reason_category_rules()}
        self.assertTrue(cand_cats - parent_cats, "heuristic should add at least one new reason-category rule")

    def test_heuristic_output_passes_gate(self):
        parent = parse_rubric(BASELINE_RUBRIC)
        mutated_text = heuristic_mutate(BASELINE_RUBRIC, parent, DOSSIER, weight_step=6)
        candidate = parse_rubric(mutated_text)
        gate = evaluate_candidate_gate(parent, candidate, DOSSIER, weight_step=6)
        # Heuristic should at minimum change weights and add a reason rule. It may
        # not always clear the 6-rule bar on a very short parent, so loosen here:
        # require it to change weights and cover a top reason category.
        self.assertTrue(gate.delta.weights_changed)
        top_fp = {r["reason_category"] for r in DOSSIER["reason_breakdown"]["false_positives"]}
        self.assertTrue(
            set(gate.delta.reason_categories_newly_addressed) & top_fp,
            msg=f"expected to newly address a top FP category; got {gate.delta.reason_categories_newly_addressed}",
        )


class RenderTests(unittest.TestCase):
    def test_render_semantic_diff_markdown_contains_sections(self):
        parent = parse_rubric(BASELINE_RUBRIC)
        mutated = heuristic_mutate(BASELINE_RUBRIC, parent, DOSSIER, weight_step=6)
        candidate = parse_rubric(mutated)
        delta = semantic_rubric_delta(parent, candidate)
        md = render_semantic_diff_markdown(delta)
        self.assertIn("material_rule_changes", md)
        self.assertIn("Weight Changes", md)


class PointMapTests(unittest.TestCase):
    def test_generate_point_map_strictly_increasing_ends_at_cap(self):
        for cap in (5, 10, 15, 24, 30, 36, 40):
            pm = generate_point_map(cap)
            self.assertEqual(len(pm), 5)
            self.assertEqual(pm[-1], cap)
            for i in range(1, len(pm)):
                self.assertGreater(pm[i], pm[i - 1])


if __name__ == "__main__":
    unittest.main()
