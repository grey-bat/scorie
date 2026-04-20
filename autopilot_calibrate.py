from __future__ import annotations

import argparse
import difflib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from composite_formula import DIRECT_SCORE_COLUMNS, load_composite_config, score_band
from reason_catalog import categorize_reason, normalize_reason
from rubric_structure import (
    DIMENSIONS,
    GateResult,
    RubricSpec,
    SemanticDelta,
    evaluate_candidate_gate,
    generate_point_map,
    heuristic_mutate,
    parse_rubric,
    render_semantic_diff_markdown,
    rewrite_point_maps_in_markdown,
    semantic_rubric_delta,
)
from rubric_versions import create_rubric_version, promote_rubric_version, update_manifest_row
from utils import canonical_match_key, normalize_email, normalize_key, normalize_text


DEFAULT_RUBRIC_MODEL = "google/gemini-3.1-pro-preview"
DEFAULT_WEIGHT_STEP = 6
RUBRIC_RETRY_TEMPERATURES = (0.0, 0.4, 0.7)


DIRECT_SCORE_PROPS = [
    "company_fit",
    "family_office_relevance",
    "fintech_relevance",
    "allocator_power",
    "access",
    "role_fit",
]

STATUS_PATH_NAME = "autopilot_status.json"
STATUS_MD_NAME = "live_status.md"


def write_status(workdir: Path, payload: dict) -> None:
    (workdir / STATUS_PATH_NAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (workdir / STATUS_MD_NAME).write_text(render_status_markdown(payload), encoding="utf-8")


def render_status_markdown(payload: dict) -> str:
    lines = [
        "# Autopilot Status",
        "",
        f"- phase: {payload.get('phase', 'n/a')}",
        f"- iteration: {payload.get('iteration', 'n/a')}",
        f"- rubric_version: {payload.get('rubric_version', 'n/a')}",
        f"- best_version: {payload.get('best_version', 'n/a')}",
        f"- processed_rows: {payload.get('processed_rows', 'n/a')}",
        f"- total_rows: {payload.get('total_rows', 'n/a')}",
    ]
    if payload.get("current_scores_csv"):
        lines.append(f"- current_scores_csv: {payload['current_scores_csv']}")
    if payload.get("current_progress_jsonl"):
        lines.append(f"- current_progress_jsonl: {payload['current_progress_jsonl']}")
    if payload.get("scoring_model"):
        lines.append(f"- scoring_model: {payload['scoring_model']}")
    if payload.get("rubric_model"):
        lines.append(f"- rubric_model: {payload['rubric_model']}")
    if payload.get("current_fp_rate") is not None:
        lines.append(f"- current_fp_rate: {payload['current_fp_rate']:.4f}")
        lines.append(f"- FP: {payload['current_fp_rate']:.4%}")
    if payload.get("current_fn_rate") is not None:
        lines.append(f"- current_fn_rate: {payload['current_fn_rate']:.4f}")
        lines.append(f"- FN: {payload['current_fn_rate']:.4%}")
    if payload.get("current_match_rate") is not None:
        lines.append(f"- current_match_rate: {payload['current_match_rate']:.4f}")
        lines.append(f"- Match: {payload['current_match_rate']:.4%}")
    if payload.get("best_fp_rate") is not None:
        lines.append(f"- best_fp_rate: {payload['best_fp_rate']:.4f}")
    if payload.get("best_fn_rate") is not None:
        lines.append(f"- best_fn_rate: {payload['best_fn_rate']:.4f}")
    if payload.get("best_match_rate") is not None:
        lines.append(f"- best_match_rate: {payload['best_match_rate']:.4f}")
    if payload.get("rubric_diff_file"):
        lines.append(f"- rubric_diff_file: {payload['rubric_diff_file']}")
    if payload.get("rubric_diff_summary"):
        lines.append(f"- rubric_diff_summary: {payload['rubric_diff_summary']}")
    if payload.get("semantic_diff_summary"):
        lines.append(f"- semantic_diff_summary: {payload['semantic_diff_summary']}")
    return "\n".join(lines) + "\n"


def count_scored_rows(scores_csv: Path) -> int:
    if not scores_csv.exists():
        return 0
    try:
        with open(scores_csv, encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)
    except OSError:
        return 0


def _normalize_full_name(value) -> str:
    """Casefolded, whitespace-collapsed full name for robust joining."""
    return " ".join(str(value or "").split()).strip().casefold()


def load_manual_labels(labels_csv: str | Path) -> pd.DataFrame:
    """Load the manual-eval CSV using ONLY Status, Reason, and Full Name.

    Per user directive: manual evaluation must be keyed on Full Name and must
    use only the Status and Reason columns. All other columns in the file are
    intentionally ignored to keep the evaluation surface small and auditable.
    Only rows with Status in {Sent, Skip} are kept (GOOD vs SKIP); Cust rows
    are not a valid outcome and are dropped here.
    """
    raw = pd.read_csv(labels_csv, dtype=str, low_memory=False).fillna("")
    for required in ("Status", "Reason", "Full Name"):
        if required not in raw.columns:
            raise ValueError(f"manual labels csv must include column: {required}")
    labels = raw[["Status", "Reason", "Full Name"]].copy()
    labels = labels[labels["Status"].isin(["Sent", "Skip"])].copy()
    if labels.empty:
        raise ValueError("manual labels csv has no Sent/Skip rows")
    labels["Full Name"] = labels["Full Name"].map(normalize_text)
    labels["Full Name Key"] = labels["Full Name"].map(_normalize_full_name)
    labels["Reason"] = labels["Reason"].map(normalize_text)
    labels["Reason Normalized"] = labels["Reason"].map(normalize_reason)
    labels["Reason Category"] = labels["Reason"].map(categorize_reason)
    # Deduplicate on full-name key so ambiguous rows collapse predictably.
    labels = labels.drop_duplicates(subset=["Full Name Key"], keep="first").reset_index(drop=True)
    return labels


def build_eval_prepared_subset(workdir: Path, labels: pd.DataFrame) -> tuple[Path, pd.DataFrame]:
    """Select the prepared-scoring rows whose Full Name matches a manual label.

    Only the Full Name column is used for matching (per user directive). The
    prepared CSV must carry a Full Name column; if not, we synthesize it from
    common first/last name columns.
    """
    prepared_csv = workdir / "01_prepare" / "prepared_scoring_input.csv"
    prepared = pd.read_csv(prepared_csv, dtype=str, low_memory=False).fillna("")
    if "Full Name" not in prepared.columns:
        first = prepared.get("First Name", pd.Series([""] * len(prepared)))
        last = prepared.get("Last Name", pd.Series([""] * len(prepared)))
        prepared["Full Name"] = (first.astype(str) + " " + last.astype(str)).str.strip()
    prepared["Full Name Key"] = prepared["Full Name"].map(_normalize_full_name)
    label_name_keys = {k for k in labels["Full Name Key"].tolist() if k}
    subset = prepared[prepared["Full Name Key"].isin(label_name_keys)].copy()
    if subset.empty:
        raise ValueError("no Sent/Skip rows matched prepared scoring input by Full Name")
    # Attach the manual Status as a "Manual" column ("GOOD" or "SKIP") so the
    # live scoring table can display it alongside model output.
    status_map = dict(zip(labels["Full Name Key"], labels["Status"]))
    manual_map = {"Sent": "GOOD", "Skip": "SKIP"}
    subset["Manual"] = subset["Full Name Key"].map(status_map).map(manual_map).fillna("")
    # Drop the helper join column before persisting so scoring input stays clean.
    subset = subset.drop(columns=["Full Name Key"])
    subset_path = workdir / "01_prepare" / "prepared_scoring_input_eval_only.csv"
    subset.to_csv(subset_path, index=False)
    return subset_path, labels


def load_scored_direct(scores_csv: str | Path, prepared_csv: str | Path | None = None) -> pd.DataFrame:
    scored = pd.read_csv(scores_csv, dtype=str, low_memory=False).fillna("")
    for column in DIRECT_SCORE_PROPS + ["direct_score", "fo_total", "ft_total", "overall_direct_score"]:
        if column in scored.columns:
            scored[column] = scored[column].map(lambda v: float(v or 0))
    for col in ("Raw ID", "Best Email", "Match Key", "LinkedIn URL"):
        if col not in scored.columns:
            scored[col] = ""
    scored["Raw ID"] = scored["Raw ID"].map(normalize_key)
    scored["Best Email"] = scored["Best Email"].map(normalize_email)
    scored["Match Key"] = scored["Match Key"].map(normalize_key)
    scored["LinkedIn URL"] = scored["LinkedIn URL"].map(lambda v: normalize_text(v).lower().rstrip("/"))
    # The scoring CSV doesn't include Full Name (payload is slim), but manual
    # eval requires Full Name as the sole join key. Merge it from the prepared
    # CSV on Match Key so join_labels can do its job.
    if prepared_csv is not None:
        prepared_path = Path(prepared_csv)
        if prepared_path.exists():
            prep = pd.read_csv(prepared_path, dtype=str, low_memory=False).fillna("")
            if "Match Key" in prep.columns:
                prep["Match Key"] = prep["Match Key"].map(normalize_key)
            if "Full Name" not in prep.columns:
                first = prep.get("First Name", pd.Series([""] * len(prep)))
                last = prep.get("Last Name", pd.Series([""] * len(prep)))
                prep["Full Name"] = (first.astype(str) + " " + last.astype(str)).str.strip()
            keep_cols = [c for c in ("Match Key", "Full Name", "Current Company", "Current Title", "Headline") if c in prep.columns]
            scored = scored.merge(prep[keep_cols].drop_duplicates("Match Key"), on="Match Key", how="left")
    if "Full Name" not in scored.columns:
        scored["Full Name"] = ""
    return scored


def join_labels(scored: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """Join scored rows to manual labels by Full Name only.

    Per user directive the only manual-eval surface is Full Name + Status +
    Reason. We match scored rows to label rows on a normalized Full Name key
    and drop any scored rows that have no manual label.
    """
    scored = scored.copy()
    if "Full Name" not in scored.columns:
        first = scored.get("First Name", pd.Series([""] * len(scored)))
        last = scored.get("Last Name", pd.Series([""] * len(scored)))
        scored["Full Name"] = (first.astype(str) + " " + last.astype(str)).str.strip()
    scored["Full Name Key"] = scored["Full Name"].map(_normalize_full_name)
    label_columns = ["Full Name Key", "Status", "Reason", "Reason Normalized", "Reason Category"]
    label_columns = [c for c in label_columns if c in labels.columns]
    merged = scored.merge(labels[label_columns], on="Full Name Key", how="left")
    merged = merged[merged["Status"].isin(["Sent", "Skip"])].copy()
    if merged.empty:
        raise ValueError("no scored rows matched Sent/Skip labels by Full Name")
    return merged


def evaluate_predictions(scored: pd.DataFrame, labels: pd.DataFrame, threshold: float = 75.0) -> tuple[dict, pd.DataFrame]:
    merged = join_labels(scored, labels)
    merged["predicted_positive"] = merged["direct_score"].astype(float) >= threshold
    merged["actual_positive"] = merged["Status"].eq("Sent")
    actual_pos = int(merged["actual_positive"].sum())
    actual_neg = int((~merged["actual_positive"]).sum())
    true_pos = int((merged["predicted_positive"] & merged["actual_positive"]).sum())
    true_neg = int((~merged["predicted_positive"] & ~merged["actual_positive"]).sum())
    false_pos = int((merged["predicted_positive"] & ~merged["actual_positive"]).sum())
    false_neg = int((~merged["predicted_positive"] & merged["actual_positive"]).sum())
    sent_mean = float(merged.loc[merged["actual_positive"], "direct_score"].mean())
    skip_mean = float(merged.loc[~merged["actual_positive"], "direct_score"].mean())
    n = len(merged)
    metrics = {
        "evaluated_rows": int(n),
        "threshold": threshold,
        "true_positives": true_pos,
        "true_negatives": true_neg,
        "false_positives": false_pos,
        "false_negatives": false_neg,
        "matches": true_pos + true_neg,
        "match_rate": ((true_pos + true_neg) / n) if n else 0.0,
        # Share-of-total versions (these sum with match_rate to 1.0).
        # match_rate + fp_share + fn_share == 1.0 by construction.
        "fp_share": (false_pos / n) if n else 0.0,
        "fn_share": (false_neg / n) if n else 0.0,
        # Class-conditional error rates (denominators differ; used for targets).
        "fp_rate": (false_pos / actual_neg) if actual_neg else 0.0,
        "fn_rate": (false_neg / actual_pos) if actual_pos else 0.0,
        "combined_error": ((false_pos / actual_neg) if actual_neg else 0.0) + ((false_neg / actual_pos) if actual_pos else 0.0),
        "sent_mean": sent_mean,
        "skip_mean": skip_mean,
        "separation": sent_mean - skip_mean,
    }
    return metrics, merged


def _top_examples(merged: pd.DataFrame) -> dict[str, list[dict]]:
    fp = merged[(merged["Status"] == "Skip") & (merged["direct_score"] >= 75)].sort_values("direct_score", ascending=False)
    fn = merged[(merged["Status"] == "Sent") & (merged["direct_score"] < 75)].sort_values("direct_score", ascending=True)
    keep = []
    for column in ["Full Name", "Current Company", "Current Title", "Headline", "Status", "direct_score"]:
        if column in merged.columns:
            keep.append(column)
    for column in ["Reason", "Reason Normalized", "Reason Category", "Fintech Score"]:
        if column in merged.columns and column not in keep:
            keep.append(column)
    if "Status" not in keep:
        keep.append("Status")
    if "direct_score" not in keep:
        keep.append("direct_score")
    return {
        "false_positives": fp[keep].head(8).to_dict(orient="records"),
        "false_negatives": fn[keep].head(8).to_dict(orient="records"),
    }


def write_rubric_diff(parent_text: str, candidate_text: str, out_path: Path) -> str:
    diff_lines = list(
        difflib.unified_diff(
            parent_text.splitlines(),
            candidate_text.splitlines(),
            fromfile="parent_rubric.md",
            tofile="candidate_rubric.md",
            lineterm="",
        )
    )
    out_path.write_text("\n".join(diff_lines) + ("\n" if diff_lines else ""), encoding="utf-8")
    additions = sum(1 for line in diff_lines if line.startswith("+") and not line.startswith("+++"))
    deletions = sum(1 for line in diff_lines if line.startswith("-") and not line.startswith("---"))
    return f"+{additions} / -{deletions} changed lines"


def write_semantic_rubric_diff(parent_text: str, candidate_text: str, out_path: Path) -> str:
    parent_lines = {line.strip() for line in parent_text.splitlines() if line.strip().startswith("- ")}
    candidate_lines = {line.strip() for line in candidate_text.splitlines() if line.strip().startswith("- ")}
    added = sorted(candidate_lines - parent_lines)
    removed = sorted(parent_lines - candidate_lines)
    lines = ["# Semantic Rubric Diff", ""]
    lines.append(f"- added_rules: {len(added)}")
    lines.append(f"- removed_rules: {len(removed)}")
    lines.append("")
    lines.append("## Added")
    lines.extend(added or ["- none"])
    lines.append("")
    lines.append("## Removed")
    lines.extend(removed or ["- none"])
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return f"{len(added)} added semantic rules / {len(removed)} removed"


def write_iteration_report(
    *,
    out_path: Path,
    metrics: dict,
    baseline_metrics: dict,
    best_metrics: dict,
    version_id: str,
    best_version_id: str,
    rubric_diff_summary: str = "",
    semantic_diff_summary: str = "",
) -> None:
    lines = [
        f"# Iteration Report: {version_id}",
        "",
        f"- best_version: {best_version_id}",
        f"- matches: {metrics['matches']} / {metrics['evaluated_rows']} ({metrics['match_rate']:.2%})",
        f"- false_positive_rate: {metrics['fp_rate']:.2%}",
        f"- false_negative_rate: {metrics['fn_rate']:.2%}",
        f"- separation: {metrics['separation']:.2f}",
        f"- baseline_match_rate: {baseline_metrics['match_rate']:.2%}",
        f"- baseline_fp_rate: {baseline_metrics['fp_rate']:.2%}",
        f"- baseline_fn_rate: {baseline_metrics['fn_rate']:.2%}",
        f"- best_match_rate: {best_metrics['match_rate']:.2%}",
        f"- best_fp_rate: {best_metrics['fp_rate']:.2%}",
        f"- best_fn_rate: {best_metrics['fn_rate']:.2%}",
    ]
    if rubric_diff_summary:
        lines.append(f"- rubric_diff: {rubric_diff_summary}")
    if semantic_diff_summary:
        lines.append(f"- semantic_diff: {semantic_diff_summary}")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def status_payload(
    *,
    phase: str,
    iteration: int,
    rubric_version: str,
    best_version: str,
    total_rows: int,
    current_scores_csv: Path,
    current_progress_jsonl: Path,
    baseline_metrics: dict | None = None,
    current_metrics: dict | None = None,
    best_metrics: dict | None = None,
    target_fp: float | None = None,
    target_fn: float | None = None,
    rubric_diff_file: Path | None = None,
    rubric_diff_summary: str = "",
    semantic_diff_file: Path | None = None,
    semantic_diff_summary: str = "",
    scoring_model: str = "",
    rubric_model: str = "",
) -> dict:
    payload = {
        "phase": phase,
        "iteration": iteration,
        "rubric_version": rubric_version,
        "best_version": best_version,
        "processed_rows": count_scored_rows(current_scores_csv),
        "total_rows": total_rows,
        "current_scores_csv": str(current_scores_csv),
        "current_progress_jsonl": str(current_progress_jsonl),
        "target_fp": target_fp,
        "target_fn": target_fn,
        "rubric_diff_file": "" if rubric_diff_file is None else str(rubric_diff_file),
        "rubric_diff_summary": rubric_diff_summary,
        "semantic_diff_file": "" if semantic_diff_file is None else str(semantic_diff_file),
        "semantic_diff_summary": semantic_diff_summary,
        "scoring_model": scoring_model,
        "rubric_model": rubric_model,
    }
    if baseline_metrics:
        payload.update(
            {
                "baseline_fp_rate": baseline_metrics["fp_rate"],
                "baseline_fn_rate": baseline_metrics["fn_rate"],
                "baseline_match_rate": baseline_metrics["match_rate"],
                "baseline_fp_share": baseline_metrics.get("fp_share", 0.0),
                "baseline_fn_share": baseline_metrics.get("fn_share", 0.0),
            }
        )
    if current_metrics:
        payload.update(
            {
                "current_fp_rate": current_metrics["fp_rate"],
                "current_fn_rate": current_metrics["fn_rate"],
                "current_match_rate": current_metrics["match_rate"],
                "current_fp_share": current_metrics.get("fp_share", 0.0),
                "current_fn_share": current_metrics.get("fn_share", 0.0),
            }
        )
    if best_metrics:
        payload.update(
            {
                "best_fp_rate": best_metrics["fp_rate"],
                "best_fn_rate": best_metrics["fn_rate"],
                "best_match_rate": best_metrics["match_rate"],
                "best_fp_share": best_metrics.get("fp_share", 0.0),
                "best_fn_share": best_metrics.get("fn_share", 0.0),
            }
        )
    return payload


def default_direct_rubric(base_text: str, examples: dict[str, list[dict]], iteration: int) -> str:
    fp_lines = "\n".join(
        f"- FP: {row.get('Full Name', '')} | {row.get('Current Company', '')} | {row.get('Current Title', '')} | {int(float(row.get('direct_score', 0)))}"
        for row in examples["false_positives"]
    ) or "- none"
    fn_lines = "\n".join(
        f"- FN: {row.get('Full Name', '')} | {row.get('Current Company', '')} | {row.get('Current Title', '')} | {int(float(row.get('direct_score', 0)))}"
        for row in examples["false_negatives"]
    ) or "- none"
    fp_reason_lines = "\n".join(
        f"- {row.get('Reason Category', 'other')}: {row.get('Full Name', '')} | {row.get('Current Company', '')} | {row.get('Current Title', '')}"
        for row in examples["false_positives"][:8]
    ) or "- none"
    fn_reason_lines = "\n".join(
        f"- {row.get('Reason Category', 'other')}: {row.get('Full Name', '')} | {row.get('Current Company', '')} | {row.get('Current Title', '')}"
        for row in examples["false_negatives"][:8]
    ) or "- none"
    return f"""# Lead Scoring Rubric Autopilot

## Output

Return one JSON object with a `results` array.
Each result must include:

- `urn`
- `company_fit`
- `family_office_relevance`
- `fintech_relevance`
- `allocator_power`
- `access`
- `role_fit`

Use only the allowed direct point values from `Direct Point Maps`.
Return no prose and no derived commentary.

## Core Rules

- Current company and current role are the primary signals.
- Use current company context before historical biography.
- A great company with a weak role should not get a perfect role score.
- A strong title at an irrelevant company should not get a high company score.
- Optimize for distinguishing `Sent` from `Skip`, not for preserving older weighted-score shapes.
- Be conservative on companies that are generic, ambiguous, or thinly described.
- Service providers can be relevant, but should not be scored like direct buyers unless company and role clearly justify it.
- When manual reasons indicate `service_provider`, lower company_fit and allocator_power unless the role is clearly embedded in adoption or distribution.
- When manual reasons indicate `allocator_mismatch`, keep allocator_power and role_fit moderate even if the person is senior or investor-branded.
- When manual reasons indicate `company_mismatch`, do not let biography prestige override weak current-company fit.
- When manual reasons indicate `channel_vs_buyer`, treat the lead as adjacent distribution/channel rather than direct buyer unless the current mandate is clearly commercial and relevant.
- When manual reasons indicate `relationship_override`, do not let that reason inflate the generic rubric; relationship exceptions are not general scoring rules.

## Direct Point Maps

FT score point maps (sum of caps = 100; family_office_relevance is a separate FO score and NOT part of the FT 100):

- company_fit = 7, 14, 21, 28, 35
- fintech_relevance = 6, 12, 18, 24, 30
- allocator_power = 4, 8, 12, 16, 18
- access = 2, 5, 8, 10, 12
- role_fit = 1, 2, 3, 4, 5

FO score point map (independent):

- family_office_relevance = 3, 6, 9, 12, 15

## Score Bands

- qualified = 75-100
- nearly_qualified = 50-74
- little_qualified = 25-49
- totally_unqualified = 0-24

## Dimension Guidance

### company_fit

- 30: Current company is a highly relevant buyer, allocator platform, family office, wealth platform, or fintech institution clearly in-scope.
- 24: Current company is strongly relevant but not ideal.
- 18: Current company is directionally relevant.
- 12: Current company is weakly relevant or more channel than buyer.
- 6: Current company is mostly out of scope, generic, or too thin to trust.

### family_office_relevance

- 15: Explicit family office or deeply family-capital-centric current lane.
- 12: Strong UHNW, private wealth, trust, estate, allocator, or private-capital adjacency.
- 9: Real but moderate family office lane relevance.
- 6: Weak adjacency.
- 3: No credible family office relevance.

### fintech_relevance

- 25: Explicit fintech, payments, banking infrastructure, treasury infrastructure, or institutional financial transformation role/company.
- 20: Strong fintech relevance but not ideal.
- 15: Real ecosystem or bank adjacency.
- 10: Weak adjacency.
- 5: No credible fintech relevance.

### allocator_power

- 15: Direct decision maker, capital allocator, budget owner, or sponsor.
- 12: Strong influencing executive or functional owner.
- 9: Meaningful recommender or evaluator.
- 6: Partial influence only.
- 3: Minimal or no decision influence.

### access

- 10: Warm path or unusually strong reachable connection.
- 8: Strong reachable path.
- 6: Moderate path.
- 4: Weak path.
- 2: Cold path.

### role_fit

- 5: Current role is directly aligned with buying, allocation, partnerships, strategy, treasury, product, or principal decision-making.
- 4: Strong role relevance.
- 3: Moderate role relevance.
- 2: Weak role relevance.
- 1: Role is mostly irrelevant.

## Calibration Notes

Autopilot iteration: {iteration}

Largest false positives from prior pass:
{fp_lines}

Largest false negatives from prior pass:
{fn_lines}

False positive reason categories from prior pass:
{fp_reason_lines}

False negative reason categories from prior pass:
{fn_reason_lines}

Base rubric snapshot follows for reference:

```md
{base_text[:3500]}
```
"""


def build_error_dossier(merged: pd.DataFrame) -> dict:
    sent = merged[merged["Status"] == "Sent"].copy()
    skip = merged[merged["Status"] == "Skip"].copy()
    fp = merged[(merged["Status"] == "Skip") & (merged["direct_score"] >= 75)].copy()
    fn = merged[(merged["Status"] == "Sent") & (merged["direct_score"] < 75)].copy()

    def score_summary(frame: pd.DataFrame, status_name: str) -> dict:
        if frame.empty:
            return {"status": status_name, "count": 0}
        return {
            "status": status_name,
            "count": int(len(frame)),
            "mean_score": round(float(frame["direct_score"].mean()), 2),
            "median_score": round(float(frame["direct_score"].median()), 2),
            "qualified_count": int((frame["direct_score"] >= 75).sum()),
        }

    dimension_means = {}
    for column in DIRECT_SCORE_PROPS:
        if column in merged.columns:
            dimension_means[column] = {
                "sent_mean": round(float(sent[column].mean()), 2) if not sent.empty else 0.0,
                "skip_mean": round(float(skip[column].mean()), 2) if not skip.empty else 0.0,
            }

    pattern_fields = ["Current Company", "Current Title"]
    patterns = {}
    for field in pattern_fields:
        if field not in merged.columns:
            continue
        fp_counts = fp[field].fillna("").replace("", pd.NA).dropna().value_counts().head(5)
        fn_counts = fn[field].fillna("").replace("", pd.NA).dropna().value_counts().head(5)
        patterns[field] = {
            "false_positive_top": [{"value": str(idx), "count": int(val)} for idx, val in fp_counts.items()],
            "false_negative_top": [{"value": str(idx), "count": int(val)} for idx, val in fn_counts.items()],
        }

    reason_breakdown = {}
    if "Reason Category" in merged.columns:
        for frame_name, frame in [("all", merged), ("sent", sent), ("skip", skip), ("false_positives", fp), ("false_negatives", fn)]:
            counts = (
                frame["Reason Category"]
                .fillna("")
                .replace("", pd.NA)
                .dropna()
                .value_counts()
                .head(10)
            )
            reason_breakdown[frame_name] = [{"reason_category": str(idx), "count": int(val)} for idx, val in counts.items()]

    prior_score_summary = {}
    if "Fintech Score" in merged.columns:
        numeric = pd.to_numeric(merged["Fintech Score"], errors="coerce")
        merged_with_prior = merged.assign(_prior_score=numeric)
        for frame_name, frame in [
            ("sent", merged_with_prior[merged_with_prior["Status"] == "Sent"]),
            ("skip", merged_with_prior[merged_with_prior["Status"] == "Skip"]),
        ]:
            if not frame["_prior_score"].dropna().empty:
                prior_score_summary[frame_name] = {
                    "mean_prior_score": round(float(frame["_prior_score"].mean()), 2),
                    "median_prior_score": round(float(frame["_prior_score"].median()), 2),
                }

    return {
        "status_summaries": [score_summary(sent, "Sent"), score_summary(skip, "Skip")],
        "dimension_means": dimension_means,
        "patterns": patterns,
        "reason_breakdown": reason_breakdown,
        "prior_score_summary": prior_score_summary,
        "top_false_positives": _top_examples(merged)["false_positives"],
        "top_false_negatives": _top_examples(merged)["false_negatives"],
    }


def _reason_verbatim_samples(examples: dict[str, list[dict]]) -> dict[str, list[dict]]:
    keep = ("Full Name", "Current Company", "Current Title", "Reason",
            "Reason Normalized", "Reason Category", "direct_score")
    def pick(rows):
        return [{k: row.get(k, "") for k in keep} for row in rows[:8]]
    return {
        "false_positives": pick(examples.get("false_positives", [])),
        "false_negatives": pick(examples.get("false_negatives", [])),
    }


def _top_fp_fn_categories(error_dossier: dict) -> dict[str, list[str]]:
    rb = (error_dossier or {}).get("reason_breakdown", {}) or {}
    def top(name: str, k: int = 3) -> list[str]:
        out = []
        for row in (rb.get(name) or [])[:k]:
            cat = str(row.get("reason_category") or "").strip()
            if cat and cat != "other":
                out.append(cat)
        return out
    return {"false_positives": top("false_positives"), "false_negatives": top("false_negatives")}


def generate_rubric_candidate(
    *,
    base_text: str,
    examples: dict[str, list[dict]],
    error_dossier: dict,
    iteration: int,
    model: str,
    use_openrouter: bool,
    weight_step: int = DEFAULT_WEIGHT_STEP,
    temperature: float = 0.0,
    previous_failure_feedback: str = "",
    prior_attempts: list[dict] | None = None,
    target_fp_share: float | None = None,
    target_fn_share: float | None = None,
) -> str:
    if not use_openrouter:
        return default_direct_rubric(base_text, examples, iteration)
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return default_direct_rubric(base_text, examples, iteration)
    parent_spec = parse_rubric(base_text)
    top_cats = _top_fp_fn_categories(error_dossier)
    system_prompt = (
        "You rewrite scoring rubrics for a lead-qualification pipeline. "
        "The OPTIMIZATION TARGET is the FT score. There are only two scores: FT and FO. "
        "The FT score is the sum of exactly five dimensions: company_fit, fintech_relevance, "
        "allocator_power, access, role_fit. Their caps MUST sum to exactly 100. "
        "family_office_relevance is a SEPARATE score (FO); it is NOT part of the FT 100-point "
        "budget. Its cap is independent and may be any value -- it will not affect the FT metric. "
        "Return markdown only. Keep the required sections Output, Core Rules, Direct Point Maps, "
        "Score Bands, Dimension Guidance, and Calibration Notes. "
        "You MUST change the decision rules and the FT dimension weights -- not just the prose. "
        "A candidate that only rephrases the parent rubric will be rejected and you will be asked again. "
        "You MAY shift per-FT-dimension point-map caps by up to +-{weight_step} points per iteration. "
        "FT caps MUST still sum to exactly 100 after any shifts. Render point maps in the exact form "
        "'company_fit = 8, 16, 24, 32, 40' (strictly increasing ints, last value = the cap). "
        "The FT score bands 75/50/25 are fixed. "
        "The manual 'Reason' column from Greg is the highest-signal input: it is his actual "
        "explanation for why he Sent or Skipped a profile. For each of the top-3 FP and top-3 FN "
        "reason categories, you MUST either add a new Core Rule bullet that references that reason "
        "category by name, or rewrite the existing one so the rule actually changes behavior. "
        "Do not emit 0-5 raw-score language or legacy weighted-score language. "
        "HARD CONSTRAINT – NO PROPER NOUNS: The rubric MUST NOT contain any specific "
        "company brand names or individual people's names. Describe categories only "
        "('mega-cap diversified tech platform', 'retail crypto exchange', 'global card network', "
        "'BaaS platform', 'VP of partnerships at a payments processor', etc.). Any rewrite "
        "that introduces identifiable brand names (Meta, Google, Amazon, Stripe, PayPal, Visa, "
        "Mastercard, Ripple, Circle, any named bank, any named fintech, any named individual, etc.) "
        "will be rejected and you will be asked again. Use generic descriptors, always. "
        "Optimize for separating Sent from Skip on the FT score. "
        "If your rewrite does not also change per-dimension threshold anchors (the '- 28:' / '- 35:' "
        "style lines inside Dimension Guidance), it is not a real behavioral change."
    ).format(weight_step=weight_step)
    user_payload: dict = {
        "iteration": iteration,
        "current_point_maps": parent_spec.point_maps or {
            "company_fit": [7, 14, 21, 28, 35],
            "fintech_relevance": [6, 12, 18, 24, 30],
            "allocator_power": [4, 8, 12, 16, 18],
            "access": [2, 5, 8, 10, 12],
            "role_fit": [1, 2, 3, 4, 5],
            "family_office_relevance": [3, 6, 9, 12, 15],
        },
        "weight_step_allowed": weight_step,
        "ft_dimensions": ["company_fit", "fintech_relevance", "allocator_power", "access", "role_fit"],
        "ft_caps_must_sum_to": 100,
        "fo_dimension": "family_office_relevance",
        "fo_is_independent": True,
        "score_bands_fixed": {
            "qualified": "75-100",
            "nearly_qualified": "50-74",
            "little_qualified": "25-49",
            "totally_unqualified": "0-24",
        },
        "base_rubric": base_text,
        "error_dossier": error_dossier,
        "top_fp_reason_categories": top_cats["false_positives"],
        "top_fn_reason_categories": top_cats["false_negatives"],
        "reason_verbatim_samples": _reason_verbatim_samples(examples),
        "rewrite_requirements": [
            "Make at least 6 material rule changes (not paraphrases) across Core Rules and Dimension Guidance.",
            "Change at least one FT-dimension point-map cap (within +-{w}) and compensate another FT dim so FT caps still sum to exactly 100.".format(w=weight_step),
            "family_office_relevance is NOT part of the FT 100-point budget; do not move points in or out of FO to balance FT. Treat FO as a separate optional score.",
            "For each top FP reason category, add or rewrite a Core Rule bullet that lowers similar future profiles.",
            "For each top FN reason category, add or rewrite a Core Rule bullet that rescues similar future profiles.",
            "Rewrite the numeric anchor bullets inside Dimension Guidance (the '- 35:', '- 28:', '- 21:' style lines) so the threshold descriptions actually change.",
            "Use current company + current role as the primary decision surface.",
            "Separate allocator authority from general seniority.",
            "Treat Greg's Reason column as ground-truth intent, not flavor text.",
        ],
    }
    if previous_failure_feedback:
        user_payload["previous_failure_feedback"] = previous_failure_feedback
    if prior_attempts:
        # Give the rewriter a compact trail of earlier attempts so it does
        # not repeat moves that already failed. Order oldest -> newest.
        user_payload["prior_attempts"] = sorted(prior_attempts, key=lambda a: a.get("iteration", 0))
        user_payload["learning_instructions"] = [
            "prior_attempts lists candidate rubrics already scored this run with their metrics (match_rate, fp_share, fn_share, combined_error, separation).",
            "Do NOT repeat a move (cap shift, threshold anchor, or reason-category rule) that a prior attempt already tried without improvement.",
            "If a prior attempt reduced FN but blew up FP (or vice versa), propose a move that targets the *other* side of the trade without undoing the gain.",
            "Treat the highest match_rate in prior_attempts as the new floor: your candidate must do something meaningfully different, not converge to a previously-tried rubric.",
            "Summaries under semantic_diff_markdown show exactly which rules and caps each prior attempt changed. Avoid identical edits.",
            "Reason about the DIRECTION: if fn_share > target, profiles labeled Sent are scoring too low -> raise caps / lower thresholds / add rescue rules for FN reason categories. If fp_share > target, profiles labeled Skip are scoring too high -> lower caps / raise thresholds / add harder constraints for FP reason categories.",
        ]
    if target_fp_share is not None or target_fn_share is not None:
        user_payload["acceptance_targets"] = {
            "fp_share_max": target_fp_share,
            "fn_share_max": target_fn_share,
            "note": "fp_share and fn_share are share-of-total (summing with match_rate to 1.0). Your candidate must push BOTH below these caps.",
        }
    user_prompt = json.dumps(user_payload, ensure_ascii=False)
    # Cap max_tokens so OpenRouter does not refuse the request with 402
    # when account credit balance cannot fund the model's default ceiling
    # (pro-preview defaults to 65536 which is ~15x what a rubric rewrite
    # actually needs). 16k is ample for a full rubric candidate + JSON meta.
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": 16000,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    # Transient retry on 402/429/5xx with short backoff so a momentary
    # credit blip or rate limit does not nuke a 10-iteration autopilot run.
    last_err: Exception | None = None
    for attempt in range(4):
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=180,
            )
            if response.status_code in (402, 429) or response.status_code >= 500:
                wait = 10 * (attempt + 1)
                print(
                    f"rubric-model HTTP {response.status_code} ({response.text[:200]!r}); "
                    f"retrying in {wait}s (attempt {attempt + 1}/4)",
                    flush=True,
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            return str(content).strip()
        except requests.exceptions.RequestException as e:
            last_err = e
            wait = 10 * (attempt + 1)
            print(f"rubric-model request error: {e!r}; retrying in {wait}s", flush=True)
            time.sleep(wait)
    # Out of retries -> fall back to heuristic mutation by returning the
    # default deterministic rubric. propose_rubric_with_gate will still gate
    # it and, if needed, apply heuristic_mutate as the final fallback.
    print(
        f"rubric-model exhausted retries; falling back to default_direct_rubric. last_err={last_err!r}",
        flush=True,
    )
    return default_direct_rubric(base_text, examples, iteration)


def semantic_rule_change_count(parent_text: str, candidate_text: str) -> int:
    """Backwards-compatible line-set diff (kept for legacy tests).

    Use `rubric_structure.semantic_rubric_delta` for the structural gate.
    """
    parent_lines = {line.strip() for line in parent_text.splitlines() if line.strip().startswith("- ")}
    candidate_lines = {line.strip() for line in candidate_text.splitlines() if line.strip().startswith("- ")}
    return len(candidate_lines - parent_lines) + len(parent_lines - candidate_lines)


def propose_rubric_with_gate(
    *,
    parent_text: str,
    examples: dict[str, list[dict]],
    error_dossier: dict,
    iteration: int,
    model: str,
    use_openrouter: bool,
    weight_step: int = DEFAULT_WEIGHT_STEP,
    max_retries: int = 2,
    candidate_fn=None,
    prior_attempts: list[dict] | None = None,
    target_fp_share: float | None = None,
    target_fn_share: float | None = None,
) -> tuple[str, GateResult, dict]:
    """Call the rubric model, gate the candidate, retry with feedback, heuristic fallback.

    `candidate_fn` is the LLM callable. Defaulting to generate_rubric_candidate keeps
    tests able to inject a fake.
    """
    if candidate_fn is None:
        candidate_fn = generate_rubric_candidate
    parent_spec = parse_rubric(parent_text)
    attempts: list[dict] = []
    feedback = ""
    candidate_text = ""
    candidate_spec: RubricSpec | None = None
    gate: GateResult | None = None
    for attempt_idx in range(max_retries + 1):
        temperature = RUBRIC_RETRY_TEMPERATURES[min(attempt_idx, len(RUBRIC_RETRY_TEMPERATURES) - 1)]
        candidate_text = candidate_fn(
            base_text=parent_text,
            examples=examples,
            error_dossier=error_dossier,
            iteration=iteration,
            model=model,
            use_openrouter=use_openrouter,
            weight_step=weight_step,
            temperature=temperature,
            previous_failure_feedback=feedback,
            prior_attempts=prior_attempts,
            target_fp_share=target_fp_share,
            target_fn_share=target_fn_share,
        )
        candidate_spec = parse_rubric(candidate_text)
        gate = evaluate_candidate_gate(
            parent_spec, candidate_spec, error_dossier, weight_step=weight_step
        )
        attempts.append({
            "attempt": attempt_idx + 1,
            "temperature": temperature,
            "passed": gate.passed,
            "reasons": list(gate.reasons),
            "rule_changes": gate.delta.material_rule_change_count,
            "weights_changed": list(gate.delta.weights_changed.keys()),
        })
        if gate.passed:
            return candidate_text, gate, {"attempts": attempts, "used_heuristic_fallback": False, "source": "llm"}
        feedback = gate.feedback_for_retry

    # All LLM attempts failed the gate -> deterministic heuristic fallback.
    candidate_text = heuristic_mutate(
        parent_text, parent_spec, error_dossier, weight_step=weight_step
    )
    candidate_spec = parse_rubric(candidate_text)
    gate = evaluate_candidate_gate(
        parent_spec, candidate_spec, error_dossier, weight_step=weight_step
    )
    attempts.append({
        "attempt": len(attempts) + 1,
        "temperature": None,
        "passed": gate.passed,
        "reasons": list(gate.reasons),
        "rule_changes": gate.delta.material_rule_change_count,
        "weights_changed": list(gate.delta.weights_changed.keys()),
        "heuristic": True,
    })
    return candidate_text, gate, {"attempts": attempts, "used_heuristic_fallback": True, "source": "heuristic"}


def run_score_iteration(
    *,
    prepared_csv: Path,
    out_dir: Path,
    rubric_path: Path,
    model: str,
    batch_size: int | None,
    concurrency: int | None,
    batch_retries: int | None,
    recovery_delay: int | None,
    timeout_total: int | None,
    timeout_connect: int | None,
    timeout_sock_connect: int | None,
    timeout_sock_read: int | None,
    max_records: int | None,
    start_row: int,
    mock: bool,
) -> Path:
    cmd = [
        sys.executable,
        "score_openrouter.py",
        "--input",
        str(prepared_csv),
        "--out",
        str(out_dir),
        "--model",
        model,
        "--start-row",
        str(start_row),
        "--scoring-mode",
        "autopilot_direct_100",
        "--rubric-path",
        str(rubric_path),
    ]
    if batch_size is not None:
        cmd += ["--batch-size", str(batch_size)]
    if concurrency is not None:
        cmd += ["--concurrency", str(concurrency)]
    if batch_retries is not None:
        cmd += ["--batch-retries", str(batch_retries)]
    if recovery_delay is not None:
        cmd += ["--recovery-delay", str(recovery_delay)]
    if timeout_total is not None:
        cmd += ["--timeout-total", str(timeout_total)]
    if timeout_connect is not None:
        cmd += ["--timeout-connect", str(timeout_connect)]
    if timeout_sock_connect is not None:
        cmd += ["--timeout-sock-connect", str(timeout_sock_connect)]
    if timeout_sock_read is not None:
        cmd += ["--timeout-sock-read", str(timeout_sock_read)]
    if max_records:
        cmd += ["--max-records", str(max_records)]
    if mock:
        cmd += ["--mock"]
    subprocess.run(cmd, check=True)
    return out_dir / "scores_raw.csv"


def should_stop(metrics: dict, *, iteration: int, iterations: int | None, target_fp: float | None, target_fn: float | None, max_iterations: int) -> bool:
    if iteration >= max_iterations:
        return True
    if iterations is not None and iteration >= iterations:
        return True
    if target_fp is not None and target_fn is not None:
        # Targets are share-of-total (so FP + FN + match == 100%). Compare
        # against fp_share / fn_share; fall back to class-conditional rates
        # for back-compat with older metrics dicts that predate fp_share.
        fp = metrics.get("fp_share", metrics.get("fp_rate", 1.0))
        fn = metrics.get("fn_share", metrics.get("fn_rate", 1.0))
        return fp <= target_fp and fn <= target_fn
    return False


def autopilot_calibrate(args) -> Path:
    workdir = Path(args.workdir)
    prepared_csv = workdir / "01_prepare" / "prepared_scoring_input.csv"
    if not prepared_csv.exists():
        raise FileNotFoundError(f"prepared scoring input not found: {prepared_csv}")
    labels = load_manual_labels(args.manual_labels_csv)
    prepared_csv, labels = build_eval_prepared_subset(workdir, labels)
    active_rubric = Path(args.rubric_path)
    base_text = active_rubric.read_text(encoding="utf-8")
    total_rows = len(pd.read_csv(prepared_csv, dtype=str, low_memory=False))
    scoring_model = getattr(args, "scoring_model", args.model)
    rubric_model = getattr(args, "rubric_model", args.model)
    iteration = 0
    target_fp = args.target_fp if args.target_fp is not None else args.target
    target_fn = args.target_fn if args.target_fn is not None else args.target
    stop_mode = "iterations" if args.iterations is not None else "threshold"
    baseline_version = create_rubric_version(
        base_dir=workdir,
        text=base_text,
        parent_version_id="",
        iteration=0,
        stop_mode=stop_mode,
        promotion_status="baseline",
        change_summary="baseline promoted rubric",
    )
    baseline_out = workdir / "autopilot_iter_00"
    baseline_scores_csv = baseline_out / "scores_raw.csv"
    baseline_progress_jsonl = baseline_out / "scores_progress.jsonl"
    # Compounded-learning resume: if baseline already scored + evaluated in a
    # prior run, skip re-scoring it. The baseline is the initial reference
    # point; subsequent launches should just continue iterating on top of the
    # best rubric found so far without burning credits on a repeat baseline.
    baseline_already_run = (baseline_out / "autopilot_metrics.csv").exists() and baseline_scores_csv.exists()
    if baseline_already_run:
        print(
            f"Resume: reusing existing baseline scores at {baseline_scores_csv} "
            "(skipping baseline re-run)",
            flush=True,
        )
        scored = load_scored_direct(baseline_scores_csv, prepared_csv=prepared_csv)
        best_metrics, joined = evaluate_predictions(scored, labels)
    else:
        write_status(
            workdir,
            status_payload(
                phase="baseline_scoring",
                iteration=0,
                rubric_version=baseline_version.version_id,
                best_version=baseline_version.version_id,
                total_rows=total_rows,
                current_scores_csv=baseline_scores_csv,
                current_progress_jsonl=baseline_progress_jsonl,
                target_fp=target_fp,
                target_fn=target_fn,
                scoring_model=scoring_model,
                rubric_model=rubric_model,
            ),
        )
        scores_csv = run_score_iteration(
            prepared_csv=prepared_csv,
            out_dir=baseline_out,
            rubric_path=baseline_version.path,
            model=scoring_model,
            batch_size=getattr(args, "batch_size", None),
            concurrency=getattr(args, "concurrency", None),
            batch_retries=getattr(args, "batch_retries", None),
            recovery_delay=getattr(args, "recovery_delay", None),
            timeout_total=getattr(args, "timeout_total", None),
            timeout_connect=getattr(args, "timeout_connect", None),
            timeout_sock_connect=getattr(args, "timeout_sock_connect", None),
            timeout_sock_read=getattr(args, "timeout_sock_read", None),
            max_records=args.max_records,
            start_row=args.start_row,
            mock=args.mock,
        )
        scored = load_scored_direct(scores_csv, prepared_csv=prepared_csv)
        best_metrics, joined = evaluate_predictions(scored, labels)
        joined.to_csv(baseline_out / "autopilot_eval.csv", index=False)
        pd.DataFrame([best_metrics]).to_csv(baseline_out / "autopilot_metrics.csv", index=False)
    write_iteration_report(
        out_path=baseline_out / "iteration_report.md",
        metrics=best_metrics,
        baseline_metrics=best_metrics,
        best_metrics=best_metrics,
        version_id=baseline_version.version_id,
        best_version_id=baseline_version.version_id,
    )
    update_manifest_row(
        baseline_version.manifest_path,
        baseline_version.version_id,
        fp_rate=f"{best_metrics['fp_rate']:.4f}",
        fn_rate=f"{best_metrics['fn_rate']:.4f}",
        promotion_status="baseline",
    )
    write_status(
        workdir,
        status_payload(
            phase="baseline_evaluated",
            iteration=0,
            rubric_version=baseline_version.version_id,
            best_version=baseline_version.version_id,
            total_rows=total_rows,
            current_scores_csv=baseline_scores_csv,
            current_progress_jsonl=baseline_progress_jsonl,
            baseline_metrics=best_metrics,
            current_metrics=best_metrics,
            best_metrics=best_metrics,
            target_fp=target_fp,
            target_fn=target_fn,
            scoring_model=scoring_model,
            rubric_model=rubric_model,
        ),
    )
    best_version = baseline_version
    baseline_metrics = dict(best_metrics)
    parent_version_id = baseline_version.version_id

    # Compound-learning resume: scan existing iteration dirs. If one beat
    # baseline, promote it to best_version so the next rewrite iterates from
    # there. Also collect a trail of prior attempts (metrics + rubric path)
    # to feed the rewrite prompt so the LLM stops repeating past failures.
    prior_attempts: list[dict] = []
    if baseline_already_run:
        existing_iter_dirs = sorted(workdir.glob("autopilot_iter_[0-9][0-9]"))
        for it_dir in existing_iter_dirs:
            it_metrics_csv = it_dir / "autopilot_metrics.csv"
            if not it_metrics_csv.exists() or it_dir == baseline_out:
                continue
            try:
                it_metrics = pd.read_csv(it_metrics_csv).iloc[0].to_dict()
            except Exception:
                continue
            # Parse iteration number from dir name (autopilot_iter_NN).
            try:
                it_num = int(it_dir.name.rsplit("_", 1)[-1])
            except ValueError:
                continue
            iteration = max(iteration, it_num)
            # Summarise its diff vs parent if available.
            diff_path = it_dir / "rubric_semantic_diff.md"
            diff_summary = ""
            if diff_path.exists():
                diff_summary = diff_path.read_text(encoding="utf-8")[:2500]
            prior_attempts.append({
                "iteration": it_num,
                "match_rate": float(it_metrics.get("match_rate", 0.0)),
                "fp_share": float(it_metrics.get("fp_share", 0.0)),
                "fn_share": float(it_metrics.get("fn_share", 0.0)),
                "fp_rate": float(it_metrics.get("fp_rate", 0.0)),
                "fn_rate": float(it_metrics.get("fn_rate", 0.0)),
                "combined_error": float(it_metrics.get("combined_error", 0.0)),
                "separation": float(it_metrics.get("separation", 0.0)),
                "semantic_diff_markdown": diff_summary,
            })
            # Promote to best if it strictly improves on current best.
            if (
                float(it_metrics.get("combined_error", 1e9)) < best_metrics["combined_error"]
                or (
                    float(it_metrics.get("combined_error", 1e9)) == best_metrics["combined_error"]
                    and float(it_metrics.get("separation", 0.0)) > best_metrics["separation"]
                )
            ):
                best_metrics = {k: (float(v) if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace(".", "", 1).replace("-", "", 1).isdigit()) else v) for k, v in it_metrics.items()}
                # Find the rubric file referenced by this iteration (the
                # rubric_versions/*_NN_* file whose iteration matches).
                rubric_candidates = sorted((workdir / "rubrics").glob(f"rubric_v{it_num:03d}_*.md"))
                if rubric_candidates:
                    from types import SimpleNamespace
                    best_version = SimpleNamespace(
                        version_id=rubric_candidates[-1].stem,
                        path=rubric_candidates[-1],
                        manifest_path=baseline_version.manifest_path,
                    )
                    parent_version_id = best_version.version_id
                # Rebuild joined so the next error-dossier / examples reflect
                # best-so-far, not the original baseline.
                try:
                    it_scores_csv = it_dir / "scores_raw.csv"
                    if it_scores_csv.exists():
                        best_scored = load_scored_direct(it_scores_csv, prepared_csv=prepared_csv)
                        _, joined = evaluate_predictions(best_scored, labels)
                except Exception as e:
                    print(f"Warning: could not rebuild joined from {it_dir}: {e!r}", flush=True)
        if prior_attempts:
            print(
                f"Resume: found {len(prior_attempts)} prior iteration(s); "
                f"best match so far = {best_metrics['match_rate']:.4f} "
                f"(combined_error = {best_metrics['combined_error']:.4f}); "
                f"continuing from iteration {iteration + 1}.",
                flush=True,
            )

    while True:
        iteration += 1
        baseline_examples = _top_examples(joined)
        error_dossier = build_error_dossier(joined)
        parent_text = best_version.path.read_text(encoding="utf-8")
        iteration_out = workdir / f"autopilot_iter_{iteration:02d}"
        iteration_out.mkdir(parents=True, exist_ok=True)
        scores_csv_path = iteration_out / "scores_raw.csv"
        progress_jsonl_path = iteration_out / "scores_progress.jsonl"
        rubric_diff_path = iteration_out / "rubric_diff_from_parent.md"
        semantic_diff_path = iteration_out / "rubric_semantic_diff.md"
        error_dossier_path = iteration_out / "iteration_error_dossier.json"
        error_dossier_path.write_text(json.dumps(error_dossier, ensure_ascii=False, indent=2), encoding="utf-8")
        write_status(
            workdir,
            status_payload(
                phase="regenerating_rubric",
                iteration=iteration,
                rubric_version=f"candidate_for_{best_version.version_id}",
                best_version="" if best_version is None else best_version.version_id,
                total_rows=total_rows,
                current_scores_csv=scores_csv_path,
                current_progress_jsonl=progress_jsonl_path,
                baseline_metrics=baseline_metrics,
                best_metrics=best_metrics,
                target_fp=target_fp,
                target_fn=target_fn,
                scoring_model=scoring_model,
                rubric_model=rubric_model,
            ),
        )
        candidate_text, gate, gate_meta = propose_rubric_with_gate(
            parent_text=parent_text,
            examples=baseline_examples,
            error_dossier=error_dossier,
            iteration=iteration,
            model=rubric_model,
            use_openrouter=not args.mock,
            weight_step=getattr(args, "weight_step", DEFAULT_WEIGHT_STEP),
            max_retries=getattr(args, "rubric_max_retries", 2),
            prior_attempts=prior_attempts,
            target_fp_share=target_fp,
            target_fn_share=target_fn,
        )
        change_summary = (
            f"autopilot iteration {iteration} | source={gate_meta['source']} | "
            f"rule_changes={gate.delta.material_rule_change_count} | "
            f"weights_changed={','.join(gate.delta.weights_changed.keys()) or 'none'} | "
            f"reason_cats_addressed={','.join(gate.delta.reason_categories_newly_addressed) or 'none'}"
        )
        (iteration_out / "rubric_gate.json").write_text(
            json.dumps(
                {
                    "passed": gate.passed,
                    "reasons": gate.reasons,
                    "attempts": gate_meta["attempts"],
                    "source": gate_meta["source"],
                    "used_heuristic_fallback": gate_meta["used_heuristic_fallback"],
                    "material_rule_changes": gate.delta.material_rule_change_count,
                    "weights_changed": gate.delta.weights_changed,
                    "reason_categories_newly_addressed": gate.delta.reason_categories_newly_addressed,
                    "reason_categories_dropped": gate.delta.reason_categories_dropped,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        version = create_rubric_version(
            base_dir=workdir,
            text=candidate_text,
            parent_version_id=parent_version_id,
            iteration=iteration,
            stop_mode=stop_mode,
            change_summary=change_summary,
        )
        rubric_diff_summary = write_rubric_diff(parent_text, candidate_text, rubric_diff_path)
        semantic_diff_path.write_text(render_semantic_diff_markdown(gate.delta), encoding="utf-8")
        semantic_diff_summary = gate.delta.summary()
        write_status(
            workdir,
            status_payload(
                phase="scoring",
                iteration=iteration,
                rubric_version=version.version_id,
                best_version="" if best_version is None else best_version.version_id,
                total_rows=total_rows,
                current_scores_csv=scores_csv_path,
                current_progress_jsonl=progress_jsonl_path,
                baseline_metrics=baseline_metrics,
                best_metrics=best_metrics,
                target_fp=target_fp,
                target_fn=target_fn,
                rubric_diff_file=rubric_diff_path,
                rubric_diff_summary=rubric_diff_summary,
                semantic_diff_file=semantic_diff_path,
                semantic_diff_summary=semantic_diff_summary,
                scoring_model=scoring_model,
                rubric_model=rubric_model,
            ),
        )
        scores_csv = run_score_iteration(
            prepared_csv=prepared_csv,
            out_dir=iteration_out,
            rubric_path=version.path,
            model=scoring_model,
            batch_size=getattr(args, "batch_size", None),
            concurrency=getattr(args, "concurrency", None),
            batch_retries=getattr(args, "batch_retries", None),
            recovery_delay=getattr(args, "recovery_delay", None),
            timeout_total=getattr(args, "timeout_total", None),
            timeout_connect=getattr(args, "timeout_connect", None),
            timeout_sock_connect=getattr(args, "timeout_sock_connect", None),
            timeout_sock_read=getattr(args, "timeout_sock_read", None),
            max_records=args.max_records,
            start_row=args.start_row,
            mock=args.mock,
        )
        scored = load_scored_direct(scores_csv, prepared_csv=prepared_csv)
        metrics, joined_candidate = evaluate_predictions(scored, labels)
        joined_candidate.to_csv(iteration_out / "autopilot_eval.csv", index=False)
        pd.DataFrame([metrics]).to_csv(iteration_out / "autopilot_metrics.csv", index=False)
        # Feed this iteration's outcome into prior_attempts so the next
        # rewrite can see what was just tried and how it did.
        prior_attempts.append({
            "iteration": iteration,
            "match_rate": float(metrics.get("match_rate", 0.0)),
            "fp_share": float(metrics.get("fp_share", 0.0)),
            "fn_share": float(metrics.get("fn_share", 0.0)),
            "fp_rate": float(metrics.get("fp_rate", 0.0)),
            "fn_rate": float(metrics.get("fn_rate", 0.0)),
            "combined_error": float(metrics.get("combined_error", 0.0)),
            "separation": float(metrics.get("separation", 0.0)),
            "semantic_diff_markdown": semantic_diff_path.read_text(encoding="utf-8")[:2500] if semantic_diff_path.exists() else "",
        })
        update_manifest_row(
            version.manifest_path,
            version.version_id,
            fp_rate=f"{metrics['fp_rate']:.4f}",
            fn_rate=f"{metrics['fn_rate']:.4f}",
            promotion_status="candidate",
        )
        if best_metrics is None or (
            metrics["combined_error"] < best_metrics["combined_error"]
            or (
                metrics["combined_error"] == best_metrics["combined_error"]
                and metrics["separation"] > best_metrics["separation"]
            )
        ):
            best_metrics = metrics
            best_version = version
            parent_version_id = version.version_id
            joined = joined_candidate
        write_iteration_report(
            out_path=iteration_out / "iteration_report.md",
            metrics=metrics,
            baseline_metrics=baseline_metrics,
            best_metrics=best_metrics,
            version_id=version.version_id,
            best_version_id=best_version.version_id if best_version else "",
            rubric_diff_summary=rubric_diff_summary,
            semantic_diff_summary=semantic_diff_summary,
        )
        write_status(
            workdir,
            status_payload(
                phase="evaluated",
                iteration=iteration,
                rubric_version=version.version_id,
                best_version=best_version.version_id if best_version else "",
                total_rows=total_rows,
                current_scores_csv=scores_csv_path,
                current_progress_jsonl=progress_jsonl_path,
                baseline_metrics=baseline_metrics,
                current_metrics=metrics,
                best_metrics=best_metrics,
                target_fp=target_fp,
                target_fn=target_fn,
                rubric_diff_file=rubric_diff_path,
                rubric_diff_summary=rubric_diff_summary,
                semantic_diff_file=semantic_diff_path,
                semantic_diff_summary=semantic_diff_summary,
                scoring_model=scoring_model,
                rubric_model=rubric_model,
            ),
        )
        if should_stop(
            metrics,
            iteration=iteration,
            iterations=args.iterations,
            target_fp=target_fp,
            target_fn=target_fn,
            max_iterations=args.max_iterations,
        ):
            break

    if best_version is None:
        raise RuntimeError("autopilot did not generate a rubric version")
    promote_rubric_version(best_version.path, active_rubric)
    update_manifest_row(best_version.manifest_path, best_version.version_id, promotion_status="promoted")
    write_status(
        workdir,
        status_payload(
            phase="complete",
            iteration=iteration,
            rubric_version=best_version.version_id,
            best_version=best_version.version_id,
            total_rows=total_rows,
            current_scores_csv=workdir / f"autopilot_iter_{iteration:02d}" / "scores_raw.csv",
            current_progress_jsonl=workdir / f"autopilot_iter_{iteration:02d}" / "scores_progress.jsonl",
            baseline_metrics=baseline_metrics,
            current_metrics=best_metrics,
            best_metrics=best_metrics,
            target_fp=target_fp,
            target_fn=target_fn,
            scoring_model=scoring_model,
            rubric_model=rubric_model,
        ),
    )
    return best_version.path


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--manual-labels-csv", required=True)
    ap.add_argument("--rubric-path", default="rubric_latest.md")
    ap.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "minimax/minimax-m2.7"))
    ap.add_argument("--scoring-model", default=os.getenv("OPENROUTER_SCORING_MODEL", "google/gemini-3.1-flash-lite-preview"))
    ap.add_argument("--rubric-model", default=os.getenv("OPENROUTER_RUBRIC_MODEL", DEFAULT_RUBRIC_MODEL))
    ap.add_argument("--weight-step", type=int, default=DEFAULT_WEIGHT_STEP,
                    help="Max +/- shift per dimension cap per iteration (default 6). Sum of caps must stay 100.")
    ap.add_argument("--rubric-max-retries", type=int, default=2,
                    help="Max LLM retries before falling back to heuristic mutation (default 2).")
    ap.add_argument("--iterations", type=int, default=None)
    ap.add_argument("--target", type=float, default=None)
    ap.add_argument("--target-fp", type=float, default=None)
    ap.add_argument("--target-fn", type=float, default=None)
    ap.add_argument("--max-iterations", type=int, default=8)
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--start-row", type=int, default=1)
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--concurrency", type=int, default=12)
    ap.add_argument("--mock", action="store_true")
    return ap


def main() -> None:
    load_dotenv()
    args = build_arg_parser().parse_args()
    autopilot_calibrate(args)


if __name__ == "__main__":
    main()
