import tempfile
import unittest
from pathlib import Path

import pandas as pd

from autopilot_calibrate import (
    DEFAULT_WEIGHT_STEP,
    build_error_dossier,
    evaluate_predictions,
    load_manual_labels,
    propose_rubric_with_gate,
    render_status_markdown,
    semantic_rule_change_count,
    should_stop,
    write_rubric_diff,
    write_semantic_rubric_diff,
)


class AutopilotCalibrateTests(unittest.TestCase):
    def test_load_manual_labels_excludes_cust(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.csv"
            pd.DataFrame([
                {"Full Name": "Alice One", "Status": "Sent", "Reason": "friend"},
                {"Full Name": "Bob Two", "Status": "Skip", "Reason": "pure crypto"},
                {"Full Name": "Carl Three", "Status": "Cust", "Reason": "friend"},
            ]).to_csv(path, index=False)
            labels = load_manual_labels(path)
            self.assertEqual(sorted(labels["Status"].unique().tolist()), ["Sent", "Skip"])
            self.assertIn("Reason Category", labels.columns)
            self.assertIn("Full Name Key", labels.columns)

    def test_evaluate_predictions_uses_sent_skip_threshold(self):
        scored = pd.DataFrame([
            {"Full Name": "Alice One", "direct_score": 82},
            {"Full Name": "Bob Two", "direct_score": 20},
        ])
        labels = pd.DataFrame([
            {"Full Name Key": "alice one", "Status": "Sent", "Reason": "", "Reason Category": "other"},
            {"Full Name Key": "bob two", "Status": "Skip", "Reason": "", "Reason Category": "other"},
        ])
        metrics, _merged = evaluate_predictions(scored, labels, threshold=75)
        self.assertEqual(metrics["false_positives"], 0)
        self.assertEqual(metrics["false_negatives"], 0)
        self.assertEqual(metrics["matches"], 2)
        self.assertEqual(metrics["match_rate"], 1.0)

    def test_should_stop_supports_thresholds(self):
        metrics = {"fp_rate": 0.09, "fn_rate": 0.08}
        self.assertTrue(should_stop(metrics, iteration=2, iterations=None, target_fp=0.1, target_fn=0.1, max_iterations=8))

    def test_write_rubric_diff_summarizes_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "diff.md"
            summary = write_rubric_diff("a\nb\n", "a\nc\n", out)
            self.assertTrue(out.exists())
            self.assertIn("+1", summary)
            self.assertIn("-1", summary)

    def test_write_semantic_rubric_diff_summarizes_rule_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "semantic.md"
            summary = write_semantic_rubric_diff("- one\n- two\n", "- one\n- three\n", out)
            self.assertTrue(out.exists())
            self.assertIn("1 added", summary)
            self.assertIn("1 removed", summary)

    def test_build_error_dossier_has_dimension_means(self):
        merged = pd.DataFrame([
            {"Status": "Sent", "direct_score": 80, "company_fit": 20, "family_office_relevance": 15, "fintech_relevance": 8, "allocator_power": 8, "access": 3, "role_fit": 4, "Current Company": "GoodCo", "Current Title": "CEO", "Reason Category": "relationship_override", "Fintech Score": "78"},
            {"Status": "Skip", "direct_score": 82, "company_fit": 20, "family_office_relevance": 5, "fintech_relevance": 4, "allocator_power": 2, "access": 2, "role_fit": 2, "Current Company": "BadCo", "Current Title": "Advisor", "Reason Category": "company_mismatch", "Fintech Score": "32"},
        ])
        dossier = build_error_dossier(merged)
        self.assertIn("dimension_means", dossier)
        self.assertIn("company_fit", dossier["dimension_means"])
        self.assertIn("reason_breakdown", dossier)
        self.assertIn("prior_score_summary", dossier)

    def test_semantic_rule_change_count_detects_material_change(self):
        self.assertEqual(semantic_rule_change_count("- a\n- b\n", "- a\n- c\n- d\n"), 3)

    def test_render_status_markdown_includes_metrics(self):
        text = render_status_markdown(
            {
                "phase": "evaluated",
                "iteration": 2,
                "rubric_version": "rubric_v002",
                "best_version": "rubric_v002",
                "processed_rows": 10,
                "total_rows": 12,
                "current_fp_rate": 0.2,
                "current_fn_rate": 0.1,
                "current_match_rate": 0.7,
                "rubric_diff_summary": "+5 / -3 changed lines",
                "scoring_model": "openrouter/elephant-alpha",
                "rubric_model": "z-ai/glm-5.1",
                "semantic_diff_summary": "3 added semantic rules / 1 removed",
            }
        )
        self.assertIn("current_fp_rate", text)
        self.assertIn("FP:", text)
        self.assertIn("FN:", text)
        self.assertIn("rubric_diff_summary", text)
        self.assertIn("scoring_model", text)
        self.assertIn("rubric_model", text)


BASELINE_RUBRIC = """# Lead Scoring Rubric v1

## Core Rules

- Current company and current role are the primary signals.
- When manual reasons indicate service_provider, lower company_fit and allocator_power.
- When manual reasons indicate company_mismatch, do not let biography override current fit.

## Direct Point Maps

- company_fit = 7, 14, 21, 28, 35
- fintech_relevance = 6, 12, 18, 24, 30
- allocator_power = 4, 8, 12, 16, 18
- access = 2, 5, 8, 10, 12
- role_fit = 1, 2, 3, 4, 5
- family_office_relevance = 3, 6, 9, 12, 15

## Score Bands

- qualified = 75-100

## Dimension Guidance

### company_fit

- 35: Current company is a highly relevant institutional fintech platform.
- 28: Current company is strongly relevant but not ideal.
- 21: Current company is directionally relevant.
- 14: Current company is weakly relevant.
- 7: Current company is generic or irrelevant.

### fintech_relevance

- 30: Explicit fintech or banking infrastructure role and company.
- 24: Strong fintech relevance.
- 18: Real ecosystem adjacency.
- 12: Weak adjacency.
- 6: No credible fintech relevance.
"""


DOSSIER = {
    "dimension_means": {
        "company_fit": {"sent_mean": 22, "skip_mean": 18},
        "fintech_relevance": {"sent_mean": 20, "skip_mean": 8},
        "role_fit": {"sent_mean": 3, "skip_mean": 3},
    },
    "reason_breakdown": {
        "false_positives": [
            {"reason_category": "service_provider", "count": 5},
            {"reason_category": "allocator_mismatch", "count": 3},
        ],
        "false_negatives": [
            {"reason_category": "buyer_fit_positive", "count": 4},
        ],
    },
}


class ProposeRubricGateTests(unittest.TestCase):
    def test_gate_rejects_prose_only_rewrite_and_falls_back_to_heuristic(self):
        # LLM mock that only tweaks a word: not a material change.
        calls = []

        def fake_candidate(*, base_text, examples, error_dossier, iteration, model,
                          use_openrouter, weight_step, temperature, previous_failure_feedback,
                          prior_attempts=None, target_fp_share=None, target_fn_share=None):
            calls.append({"temperature": temperature, "feedback": previous_failure_feedback})
            return base_text.replace("highly relevant", "very relevant")

        text, gate, meta = propose_rubric_with_gate(
            parent_text=BASELINE_RUBRIC,
            examples={"false_positives": [], "false_negatives": []},
            error_dossier=DOSSIER,
            iteration=1,
            model="mock",
            use_openrouter=True,
            weight_step=DEFAULT_WEIGHT_STEP,
            max_retries=2,
            candidate_fn=fake_candidate,
        )
        # 3 LLM attempts + 1 heuristic attempt = 4 entries.
        self.assertEqual(len(meta["attempts"]), 4)
        self.assertTrue(meta["used_heuristic_fallback"])
        self.assertEqual(meta["source"], "heuristic")
        # Retry must have received non-empty feedback (not the first call).
        self.assertEqual(calls[0]["feedback"], "")
        self.assertNotEqual(calls[1]["feedback"], "")
        # Temperatures increase on retry.
        self.assertGreater(calls[1]["temperature"], calls[0]["temperature"])
        # Heuristic output must materially change weights.
        self.assertTrue(gate.delta.weights_changed)

    def test_gate_accepts_real_structural_change_without_fallback(self):
        # LLM mock that returns a properly-mutated rubric.
        from rubric_structure import (
            heuristic_mutate,
            parse_rubric,
        )

        real = heuristic_mutate(
            BASELINE_RUBRIC, parse_rubric(BASELINE_RUBRIC), DOSSIER, weight_step=DEFAULT_WEIGHT_STEP
        )

        def fake_candidate(**kwargs):
            return real

        text, gate, meta = propose_rubric_with_gate(
            parent_text=BASELINE_RUBRIC,
            examples={"false_positives": [], "false_negatives": []},
            error_dossier=DOSSIER,
            iteration=1,
            model="mock",
            use_openrouter=True,
            max_retries=2,
            candidate_fn=fake_candidate,
        )
        self.assertFalse(meta["used_heuristic_fallback"])
        self.assertEqual(meta["source"], "llm")
        self.assertTrue(gate.delta.weights_changed)


if __name__ == "__main__":
    unittest.main()
