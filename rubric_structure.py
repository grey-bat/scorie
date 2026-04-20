"""Structured rubric parsing and semantic-diff utilities.

Used by autopilot_calibrate to gate rubric regeneration on material rule +
weight changes, not prose-level edits. Everything here is LLM-free.
"""

from __future__ import annotations

import copy
import math
import re
from dataclasses import dataclass, field
from typing import Iterable


DIMENSIONS = [
    "company_fit",
    "family_office_relevance",
    "fintech_relevance",
    "allocator_power",
    "access",
    "role_fit",
]

# Dimensions that contribute to the FT score (the metric being optimized).
# family_office_relevance is EXCLUDED from ft_total so its cap should not
# consume optimization budget.
FT_DIMENSIONS = [
    "company_fit",
    "fintech_relevance",
    "allocator_power",
    "access",
    "role_fit",
]


REASON_CATEGORIES = {
    "service_provider",
    "allocator_mismatch",
    "company_mismatch",
    "channel_vs_buyer",
    "relationship_override",
    "buyer_fit_negative",
    "buyer_fit_positive",
    "data_quality",
    "missing_context",
    "other",
}


_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "into", "from", "have", "has",
    "are", "was", "were", "but", "not", "any", "all", "can", "should", "must",
    "when", "their", "them", "they", "its", "than", "then", "more", "less",
    "also", "just", "only", "over", "under", "out", "off", "still", "even",
    "very", "some", "other", "such", "like", "onto", "upon", "whether",
    "while", "about", "after", "before", "because", "above", "below",
    "score", "scored", "scoring", "rule", "rules",
}


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def _content_bag(text: str) -> frozenset[str]:
    return frozenset(
        t for t in _tokens(text)
        if len(t) > 3 and t not in _STOPWORDS and not t.isdigit()
    )


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


@dataclass(frozen=True)
class Rule:
    dimension: str              # "core" or one of DIMENSIONS
    trigger_kind: str           # "threshold" | "reason_category" | "keyword"
    trigger_value: str          # e.g. "30", "service_provider", "" (for keyword)
    raw: str                    # verbatim bullet text (without leading "- ")
    polarity: str               # "up", "down", "neutral"

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.dimension, self.trigger_kind, self.trigger_value)

    @property
    def fingerprint(self) -> frozenset[str]:
        return _content_bag(self.raw)


@dataclass
class RubricSpec:
    weights: dict[str, int] = field(default_factory=dict)           # dim -> max cap (last value of point map)
    point_maps: dict[str, list[int]] = field(default_factory=dict)  # dim -> full map
    score_bands: dict[str, tuple[int, int]] = field(default_factory=dict)
    rules: list[Rule] = field(default_factory=list)

    def rules_for(self, dimension: str) -> list[Rule]:
        return [r for r in self.rules if r.dimension == dimension]

    def reason_category_rules(self) -> list[Rule]:
        return [r for r in self.rules if r.trigger_kind == "reason_category"]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


_HEADER_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
_BULLET_RE = re.compile(r"^\s*[-*]\s+(.*?)\s*$")
_THRESHOLD_RE = re.compile(r"^\s*(\d+)\s*[:\-]\s*(.+)$")
_POINT_MAP_RE = re.compile(r"^([a-z_]+)\s*[:=]\s*([0-9,\s]+)")


_POLARITY_UP = {
    "rescue", "raise", "increase", "up", "reward", "bump", "inflate",
    "strong", "top", "high", "boost",
}
_POLARITY_DOWN = {
    "lower", "reduce", "down", "push", "cap", "below", "avoid",
    "penalize", "discount", "conservative", "minimal", "overcorrect",
    "weak",
}


def _infer_polarity(text: str) -> str:
    toks = set(_tokens(text))
    up = bool(toks & _POLARITY_UP)
    down = bool(toks & _POLARITY_DOWN)
    if up and not down:
        return "up"
    if down and not up:
        return "down"
    return "neutral"


def _detect_reason_category(text: str) -> str | None:
    low = text.lower()
    for cat in REASON_CATEGORIES:
        if cat == "other":
            continue
        if cat in low:
            return cat
    return None


def _classify_rule(dimension: str, body: str) -> Rule:
    # Threshold anchor line inside a dimension subsection, e.g. "30: Current ..."
    th = _THRESHOLD_RE.match(body)
    if th and dimension != "core":
        return Rule(
            dimension=dimension,
            trigger_kind="threshold",
            trigger_value=th.group(1),
            raw=body,
            polarity="neutral",
        )
    cat = _detect_reason_category(body)
    if cat:
        return Rule(
            dimension=dimension,
            trigger_kind="reason_category",
            trigger_value=cat,
            raw=body,
            polarity=_infer_polarity(body),
        )
    return Rule(
        dimension=dimension,
        trigger_kind="keyword",
        trigger_value="",
        raw=body,
        polarity=_infer_polarity(body),
    )


def _parse_point_maps(lines: list[str]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for line in lines:
        # Strip inline HTML wrappers (e.g., <span data-proof=...>) and
        # markdown-escaped underscores so edited rubrics still parse.
        line = re.sub(r"<[^>]+>", "", line).replace("\\_", "_")
        s = line.strip().lstrip("-* ").strip()
        m = _POINT_MAP_RE.match(s)
        if not m:
            continue
        vals = [int(v.strip()) for v in m.group(2).split(",") if v.strip()]
        if vals:
            out[m.group(1)] = vals
    return out


def _parse_bands(lines: list[str]) -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for line in lines:
        s = line.strip().lstrip("-* ").strip()
        m = re.match(r"([a-z_]+)\s*[:=]\s*(\d+)\s*-\s*(\d+)", s)
        if m:
            out[m.group(1)] = (int(m.group(2)), int(m.group(3)))
    return out


def parse_rubric(text: str) -> RubricSpec:
    lines = text.splitlines()
    spec = RubricSpec()

    section_stack: list[tuple[int, str]] = []  # (level, normalized name)
    section_buffers: dict[str, list[str]] = {}

    current_top = ""
    current_sub = ""

    for raw_line in lines:
        m = _HEADER_RE.match(raw_line)
        if m:
            level = len(m.group(1))
            name = m.group(2).strip().lower()
            if level <= 2:
                current_top = name
                current_sub = ""
            elif level == 3:
                current_sub = name
            else:
                current_sub = name
            continue

        bullet = _BULLET_RE.match(raw_line)
        if not bullet:
            continue
        body = bullet.group(1).strip()
        if not body:
            continue

        key_top = current_top
        key_sub = current_sub

        if "direct point map" in key_top:
            section_buffers.setdefault("point_maps", []).append(body)
            continue
        if "score band" in key_top:
            section_buffers.setdefault("bands", []).append(body)
            continue
        if "core rule" in key_top:
            spec.rules.append(_classify_rule("core", body))
            continue
        if "dimension guidance" in key_top:
            dim = key_sub if key_sub in DIMENSIONS else "core"
            spec.rules.append(_classify_rule(dim, body))
            continue
        # Also accept top-level dimension sections like "## company_fit"
        if key_top in DIMENSIONS:
            spec.rules.append(_classify_rule(key_top, body))
            continue

    pm = _parse_point_maps(section_buffers.get("point_maps", []))
    spec.point_maps = pm
    spec.weights = {dim: vals[-1] for dim, vals in pm.items() if vals}
    spec.score_bands = _parse_bands(section_buffers.get("bands", []))
    return spec


# ---------------------------------------------------------------------------
# Semantic diff
# ---------------------------------------------------------------------------


def _match_rules(parent: list[Rule], candidate: list[Rule], jaccard_threshold: float = 0.6) -> dict:
    """Return counts of added / removed / modified / unchanged rules.

    Rules with identical strong identity (dimension, trigger_kind, trigger_value) are
    matched first. Remaining rules are matched fuzzily by content-bag Jaccard within
    the same dimension. Fuzzy-matched pairs with <0.95 Jaccard are counted as modified.
    """
    added: list[Rule] = []
    removed: list[Rule] = []
    modified: list[tuple[Rule, Rule]] = []
    unchanged: list[Rule] = []

    # Strong-identity matching: only consume pairs that match on BOTH sides.
    # Non-keyword rules with no strong-identity counterpart remain available for
    # fuzzy matching below (e.g. a '- 30:' anchor whose cap moved to '- 32:').
    consumed_parent_idx: set[int] = set()
    consumed_cand_idx: set[int] = set()
    p_idx_by_id: dict[tuple, list[int]] = {}
    for i, r in enumerate(parent):
        p_idx_by_id.setdefault(r.identity, []).append(i)
    c_idx_by_id: dict[tuple, list[int]] = {}
    for j, r in enumerate(candidate):
        c_idx_by_id.setdefault(r.identity, []).append(j)
    for identity, p_idxs in p_idx_by_id.items():
        if identity[1] == "keyword":
            continue
        c_idxs = c_idx_by_id.get(identity, [])
        pairs = min(len(p_idxs), len(c_idxs))
        for k in range(pairs):
            pi, cj = p_idxs[k], c_idxs[k]
            pr, cr = parent[pi], candidate[cj]
            if pr.fingerprint == cr.fingerprint and pr.raw.strip() == cr.raw.strip():
                unchanged.append(pr)
            else:
                modified.append((pr, cr))
            consumed_parent_idx.add(pi)
            consumed_cand_idx.add(cj)

    # Fuzzy matching for all remaining rules (keyword + unmatched thresholds/reason rules).
    p_remaining: list[Rule] = [parent[i] for i in range(len(parent)) if i not in consumed_parent_idx]
    c_remaining: list[Rule] = [candidate[j] for j in range(len(candidate)) if j not in consumed_cand_idx]

    p_used = [False] * len(p_remaining)
    c_used = [False] * len(c_remaining)

    # Greedy best-pair matching.
    pairs_scored: list[tuple[float, int, int]] = []
    for i, pr in enumerate(p_remaining):
        for j, cr in enumerate(c_remaining):
            if pr.dimension != cr.dimension:
                continue
            s = _jaccard(pr.fingerprint, cr.fingerprint)
            if s >= jaccard_threshold:
                pairs_scored.append((s, i, j))
    pairs_scored.sort(reverse=True)
    for s, i, j in pairs_scored:
        if p_used[i] or c_used[j]:
            continue
        p_used[i] = True
        c_used[j] = True
        pr, cr = p_remaining[i], c_remaining[j]
        if s >= 0.95 and pr.raw.strip() == cr.raw.strip():
            unchanged.append(pr)
        else:
            modified.append((pr, cr))
    removed.extend(pr for i, pr in enumerate(p_remaining) if not p_used[i])
    added.extend(cr for j, cr in enumerate(c_remaining) if not c_used[j])

    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "unchanged": unchanged,
    }


@dataclass
class SemanticDelta:
    rules_added: list[Rule]
    rules_removed: list[Rule]
    rules_modified: list[tuple[Rule, Rule]]
    rules_unchanged: list[Rule]
    weights_changed: dict[str, tuple[int, int]]
    reason_categories_newly_addressed: list[str]
    reason_categories_dropped: list[str]

    @property
    def material_rule_change_count(self) -> int:
        return (
            len(self.rules_added)
            + len(self.rules_removed)
            + len(self.rules_modified)
        )

    def summary(self) -> str:
        return (
            f"{len(self.rules_added)} added / "
            f"{len(self.rules_removed)} removed / "
            f"{len(self.rules_modified)} modified rules; "
            f"{len(self.weights_changed)} weight(s) changed; "
            f"new reason categories addressed: "
            f"{','.join(self.reason_categories_newly_addressed) or 'none'}"
        )


def semantic_rubric_delta(parent: RubricSpec, candidate: RubricSpec) -> SemanticDelta:
    matched = _match_rules(parent.rules, candidate.rules)
    weights_changed: dict[str, tuple[int, int]] = {}
    for dim, cap in candidate.weights.items():
        old = parent.weights.get(dim)
        if old is not None and old != cap:
            weights_changed[dim] = (int(old), int(cap))
    parent_cats = {r.trigger_value for r in parent.reason_category_rules()}
    cand_cats = {r.trigger_value for r in candidate.reason_category_rules()}
    return SemanticDelta(
        rules_added=matched["added"],
        rules_removed=matched["removed"],
        rules_modified=matched["modified"],
        rules_unchanged=matched["unchanged"],
        weights_changed=weights_changed,
        reason_categories_newly_addressed=sorted(cand_cats - parent_cats),
        reason_categories_dropped=sorted(parent_cats - cand_cats),
    )


def render_semantic_diff_markdown(delta: SemanticDelta) -> str:
    lines = ["# Semantic Rubric Diff", ""]
    lines.append(f"- material_rule_changes: {delta.material_rule_change_count}")
    lines.append(f"- rules_added: {len(delta.rules_added)}")
    lines.append(f"- rules_removed: {len(delta.rules_removed)}")
    lines.append(f"- rules_modified: {len(delta.rules_modified)}")
    lines.append(f"- weights_changed: {len(delta.weights_changed)}")
    lines.append(f"- reason_categories_newly_addressed: {','.join(delta.reason_categories_newly_addressed) or 'none'}")
    lines.append(f"- reason_categories_dropped: {','.join(delta.reason_categories_dropped) or 'none'}")
    lines.append("")
    if delta.weights_changed:
        lines.append("## Weight Changes")
        for dim, (old, new) in sorted(delta.weights_changed.items()):
            lines.append(f"- {dim}: {old} -> {new}")
        lines.append("")
    if delta.rules_added:
        lines.append("## Added")
        for r in delta.rules_added:
            lines.append(f"- [{r.dimension}/{r.trigger_kind}:{r.trigger_value}] {r.raw}")
        lines.append("")
    if delta.rules_removed:
        lines.append("## Removed")
        for r in delta.rules_removed:
            lines.append(f"- [{r.dimension}/{r.trigger_kind}:{r.trigger_value}] {r.raw}")
        lines.append("")
    if delta.rules_modified:
        lines.append("## Modified")
        for pr, cr in delta.rules_modified:
            lines.append(f"- [{cr.dimension}/{cr.trigger_kind}:{cr.trigger_value}]")
            lines.append(f"  - before: {pr.raw}")
            lines.append(f"  - after:  {cr.raw}")
        lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


@dataclass
class GateResult:
    passed: bool
    reasons: list[str]
    delta: SemanticDelta
    feedback_for_retry: str


def _top_reason_categories(error_dossier: dict, bucket: str, k: int = 3) -> list[str]:
    rb = (error_dossier or {}).get("reason_breakdown", {}) or {}
    entries = rb.get(bucket) or []
    out = []
    for row in entries[:k]:
        cat = str(row.get("reason_category") or "").strip()
        if cat and cat != "other":
            out.append(cat)
    return out


def evaluate_candidate_gate(
    parent: RubricSpec,
    candidate: RubricSpec,
    error_dossier: dict,
    *,
    min_rule_changes: int = 6,
    require_weight_change: bool = True,
    weight_step: int = 6,
    require_reason_coverage: bool = True,
) -> GateResult:
    delta = semantic_rubric_delta(parent, candidate)
    problems: list[str] = []

    # Rule-change count
    if delta.material_rule_change_count < min_rule_changes:
        problems.append(
            f"only {delta.material_rule_change_count} material rule changes; "
            f"need >= {min_rule_changes}. Change threshold anchors inside dimension "
            f"guidance and add/remove reason-category rules, not just prose."
        )

    # Weight check: FT caps (5 FT dims) must sum to 100. FO is independent.
    cand_ft_sum = sum(candidate.weights.get(d, parent.weights.get(d, 0)) for d in FT_DIMENSIONS)
    if cand_ft_sum != 100:
        problems.append(
            f"FT point_maps caps sum to {cand_ft_sum}, must sum to exactly 100. "
            f"Rebalance so the five FT dimensions (company_fit, fintech_relevance, "
            f"allocator_power, access, role_fit) add to 100. "
            f"family_office_relevance is independent and does NOT contribute to the 100."
        )
    if require_weight_change:
        ft_changes = {d: v for d, v in delta.weights_changed.items() if d in FT_DIMENSIONS}
        if not ft_changes:
            problems.append(
                "no FT-dimension weights changed; shift at least one of "
                f"{FT_DIMENSIONS} by up to +-{weight_step}. "
                "family_office_relevance is excluded from the FT metric so "
                "changing only FO does not count."
            )
        for dim, (old, new) in ft_changes.items():
            if abs(new - old) > weight_step:
                problems.append(
                    f"weight for {dim} moved by {abs(new - old)} (>{weight_step}); "
                    f"stay within +-{weight_step} per FT dimension per iteration."
                )
        # FO cap may move freely (it can go to 0 if LLM wants to reallocate
        # all 100 points into FT dims), so no step check for FO.

    # Reason-category coverage
    if require_reason_coverage:
        top_fp = _top_reason_categories(error_dossier, "false_positives")
        top_fn = _top_reason_categories(error_dossier, "false_negatives")
        top_any = set(top_fp) | set(top_fn)
        cand_cats = {r.trigger_value for r in candidate.reason_category_rules()}
        parent_cats = {r.trigger_value for r in parent.reason_category_rules()}
        # Need either a *new* reason category rule addressing one of the top
        # categories, OR a *modified* reason category rule whose text changed
        # for one of them.
        newly_addressed = (cand_cats - parent_cats) & top_any
        modified_cats = {
            cr.trigger_value
            for pr, cr in delta.rules_modified
            if cr.trigger_kind == "reason_category"
        } & top_any
        if top_any and not (newly_addressed or modified_cats):
            problems.append(
                "no rubric change targets the top reason categories from the "
                f"error dossier (top FP={top_fp}, top FN={top_fn}). Add or "
                "rewrite a reason-category bullet for at least one of these."
            )

    feedback = ""
    if problems:
        feedback = (
            "Your previous candidate was rejected because:\n- "
            + "\n- ".join(problems)
            + "\nProduce a new rubric that fixes all of the above. Do not just "
            "rephrase; change the actual decision rules and the point caps."
        )

    return GateResult(
        passed=not problems,
        reasons=problems,
        delta=delta,
        feedback_for_retry=feedback,
    )


# ---------------------------------------------------------------------------
# Rendering weights back into a rubric
# ---------------------------------------------------------------------------


def generate_point_map(cap: int, steps: int = 5) -> list[int]:
    """Five increasing integer points from floor(cap/steps) up to cap.

    Cap must be >= steps so all five values are distinct positive ints. For
    caps below steps, returns [1,2,...,steps-1,cap] clamped to strictly
    increasing. For cap == 0, returns [0]*steps (downstream direct_score
    accepts any value from the allowed set and the dim contributes 0).
    """
    if steps <= 0:
        return [cap]
    if cap <= 0:
        return [0] * steps
    step = cap / steps
    vals = [max(1, int(round(step * i))) for i in range(1, steps + 1)]
    # Enforce strictly increasing where possible.
    for i in range(1, len(vals)):
        if vals[i] <= vals[i - 1]:
            vals[i] = vals[i - 1] + 1
    vals[-1] = cap
    # If forced increments pushed below-cap values above cap, re-clamp.
    for i in range(len(vals) - 2, -1, -1):
        if vals[i] >= vals[i + 1]:
            vals[i] = max(0, vals[i + 1] - 1)
    return vals


_POINT_MAP_LINE_RE = re.compile(r"^(\s*[-*]\s*)([a-z_]+)(\s*[:=]\s*)([0-9,\s]+)\s*$")


def rewrite_point_maps_in_markdown(text: str, point_maps: dict[str, list[int]]) -> str:
    out_lines = []
    for line in text.splitlines():
        m = _POINT_MAP_LINE_RE.match(line)
        if m and m.group(2) in point_maps:
            vals = point_maps[m.group(2)]
            out_lines.append(f"{m.group(1)}{m.group(2)}{m.group(3)}{', '.join(str(v) for v in vals)}")
        else:
            out_lines.append(line)
    return "\n".join(out_lines) + ("\n" if text.endswith("\n") else "")


# ---------------------------------------------------------------------------
# Heuristic fallback mutation
# ---------------------------------------------------------------------------


_REASON_CATEGORY_TEMPLATES: dict[str, str] = {
    "service_provider": (
        "When manual reason category is service_provider (consultancy, advisory, "
        "recruiting, legal, accounting), cap company_fit at 12 and allocator_power "
        "at 6 unless current role clearly owns treasury, product, partnerships, or "
        "distribution."
    ),
    "company_mismatch": (
        "When manual reason category is company_mismatch, downweight biography "
        "prestige: company_fit and fintech_relevance must be driven by the current "
        "company, not former employers."
    ),
    "allocator_mismatch": (
        "When manual reason category is allocator_mismatch, do not grant "
        "allocator_power above 9 to investor-branded or generally senior profiles "
        "without direct evidence of budget, treasury, or partnership ownership."
    ),
    "channel_vs_buyer": (
        "When manual reason category is channel_vs_buyer, keep company_fit <= 18 "
        "and role_fit <= 3 unless the current mandate is explicitly commercial and "
        "revenue-tied to Auto Pro."
    ),
    "buyer_fit_negative": (
        "When manual reason category is buyer_fit_negative (too broad, too big, "
        "not qualified), do not let title seniority rescue company_fit; keep it "
        "at 12 or below."
    ),
    "buyer_fit_positive": (
        "When manual reason category is buyer_fit_positive (e.g. direct family "
        "office allocator), rescue family_office_relevance to >= 12 and "
        "allocator_power to >= 9 if the current company confirms the mandate."
    ),
    "missing_context": (
        "When manual reason category is missing_context, default company_fit and "
        "fintech_relevance to the middle band (18 and 15 respectively) rather "
        "than the top."
    ),
}


def _separation(error_dossier: dict, dim: str) -> float:
    dm = (error_dossier or {}).get("dimension_means", {}) or {}
    d = dm.get(dim) or {}
    return float(d.get("sent_mean", 0.0)) - float(d.get("skip_mean", 0.0))


def _shift_weights(
    weights: dict[str, int], error_dossier: dict, step: int
) -> dict[str, int]:
    """Shift budget among FT dimensions only. FO cap is left unchanged.

    Keeps FT caps summing to 100 (assumes they already do).
    """
    if not weights:
        return dict(weights)
    ft_present = [d for d in FT_DIMENSIONS if d in weights]
    if len(ft_present) < 2:
        return dict(weights)
    sep = {dim: _separation(error_dossier, dim) for dim in ft_present}
    best = max(sep, key=lambda d: sep[d])
    worst = min(sep, key=lambda d: sep[d])
    if best == worst:
        best, worst = "fintech_relevance", "role_fit"
        if best not in weights or worst not in weights:
            best, worst = ft_present[0], ft_present[-1]
    new = dict(weights)
    new[best] = new.get(best, 0) + step
    new[worst] = max(1, new.get(worst, 0) - step)
    # Ensure FT caps still sum to 100.
    ft_sum = sum(new[d] for d in ft_present)
    if ft_sum != 100:
        diff = 100 - ft_sum
        new[best] += diff
    return new


def _insert_core_rules(text: str, new_rules: Iterable[str]) -> str:
    # Insert bullets at end of ## Core Rules section. If absent, append a section.
    lines = text.splitlines()
    new_rules = [r for r in new_rules if r]
    if not new_rules:
        return text
    # Find "## Core Rules" header.
    header_idx = None
    for i, line in enumerate(lines):
        if re.match(r"^##\s+Core Rules\s*$", line, re.IGNORECASE):
            header_idx = i
            break
    if header_idx is None:
        addition = ["", "## Core Rules", ""] + [f"- {r}" for r in new_rules] + [""]
        return text.rstrip() + "\n" + "\n".join(addition) + "\n"
    # Find end of section = next header or EOF.
    end_idx = len(lines)
    for j in range(header_idx + 1, len(lines)):
        if re.match(r"^#{1,6}\s+", lines[j]):
            end_idx = j
            break
    # Insert before end_idx, keeping a blank line.
    insertion = [f"- {r}" for r in new_rules]
    before = lines[:end_idx]
    # Trim trailing blank lines in the section
    while before and before[-1].strip() == "":
        before.pop()
    new_lines = before + [""] + insertion + [""] + lines[end_idx:]
    return "\n".join(new_lines) + ("\n" if text.endswith("\n") else "")


def _rewrite_threshold_anchors(text: str, dim: str, old_map: list[int], new_map: list[int]) -> str:
    """Rewrite '- N: ...' threshold lines inside '### <dim>' so their leading point
    value matches the new point map. Lines are paired by position (high->high).

    Only rewrites inside the matching '### <dim>' subsection, bounded by the
    next header. Returns text unchanged if the subsection isn't found.
    """
    lines = text.splitlines()
    header_re = re.compile(rf"^###\s+{re.escape(dim)}\s*$", re.IGNORECASE)
    start = None
    for i, line in enumerate(lines):
        if header_re.match(line):
            start = i + 1
            break
    if start is None:
        return text
    end = len(lines)
    for j in range(start, len(lines)):
        if re.match(r"^#{1,6}\s+", lines[j]):
            end = j
            break
    # Collect threshold lines in descending-number order to align with point maps.
    # Build old->new map: same rank (descending) in both lists.
    old_sorted = sorted(old_map, reverse=True)
    new_sorted = sorted(new_map, reverse=True)
    replacement = dict(zip(old_sorted, new_sorted))
    out = list(lines)
    for j in range(start, end):
        m = re.match(r"^(\s*[-*]\s+)(\d+)(\s*:\s*)(.*)$", out[j])
        if not m:
            continue
        n = int(m.group(2))
        if n in replacement and replacement[n] != n:
            out[j] = f"{m.group(1)}{replacement[n]}{m.group(3)}{m.group(4)}"
    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


def heuristic_mutate(
    parent_text: str,
    parent_spec: RubricSpec,
    error_dossier: dict,
    *,
    weight_step: int = 6,
    max_new_rules: int = 3,
) -> str:
    """Deterministic fallback: shift weights, rewrite threshold anchors, inject
    reason-category rules. Used when the LLM fails the structural gate after retries.
    """
    # 1. Shift weights + rebuild point maps.
    new_weights = _shift_weights(parent_spec.weights, error_dossier, weight_step)
    new_point_maps = {dim: generate_point_map(cap) for dim, cap in new_weights.items()}
    text = rewrite_point_maps_in_markdown(parent_text, new_point_maps)

    # 2. Rewrite threshold anchors in each dimension whose cap changed so that
    #    leading numbers stay consistent with the new point map (and gate sees
    #    "modified" rules rather than a silent mismatch).
    for dim, new_map in new_point_maps.items():
        old_map = parent_spec.point_maps.get(dim)
        if not old_map or old_map == new_map:
            continue
        text = _rewrite_threshold_anchors(text, dim, old_map, new_map)

    # 2. Inject reason-category rules for top FP/FN categories not yet covered.
    covered = {r.trigger_value for r in parent_spec.reason_category_rules()}
    top_fp = _top_reason_categories(error_dossier, "false_positives")
    top_fn = _top_reason_categories(error_dossier, "false_negatives")
    candidates: list[str] = []
    for cat in top_fp + top_fn:
        if cat in covered:
            continue
        template = _REASON_CATEGORY_TEMPLATES.get(cat)
        if template:
            candidates.append(template)
            covered.add(cat)
        if len(candidates) >= max_new_rules:
            break
    # If nothing matched (because categories are already covered), force a
    # generic change so the gate passes.
    if not candidates:
        candidates.append(
            "Use Greg's manual reason category as the deciding signal whenever "
            "present; reason category overrides title prestige for allocator_power "
            "and company_fit."
        )
    text = _insert_core_rules(text, candidates)

    return text
