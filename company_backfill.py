from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from utils import canonical_match_key, canonicalize_identifier, normalize_key, normalize_text


BACKFILL_PRIORITY = {"visit": 3, "credits": 2, "nodata": 1, "native": 0}

ORG_FIELDS = [
    "Current Company",
    "Current Title",
    "Industry",
    "Organization 1",
    "Organization 1 Title",
    "Organization 1 Description",
    "Organization 1 Website",
    "Organization 1 Domain",
    "Organization 2",
    "Organization 2 Title",
    "Organization 2 Description",
    "Organization 2 Website",
    "Organization 2 Domain",
    "Organization 3",
    "Organization 3 Title",
    "Organization 3 Description",
    "Organization 3 Website",
    "Organization 3 Domain",
]

FILL_PRIORITY = [
    "current_company_industry",
    "organization_1_description",
    "organization_1_domain",
    "organization_1_website",
    "organization_2_description",
    "organization_2_domain",
    "organization_2_website",
    "organization_3_description",
    "organization_3_domain",
    "organization_3_website",
]

AMBIGUOUS_COMPANY_PATTERNS = [
    r"^self[- ]?employed$",
    r"^freelance$",
    r"^independent$",
    r"^consultant$",
    r"^founder$",
    r"^owner$",
    r"^principal$",
]


@dataclass(frozen=True)
class BackfillDecision:
    needed: bool
    reason: str
    source: str
    context_score: int


def _mode_from_name(name: str) -> str:
    lower = name.lower()
    if "visit" in lower:
        return "visit"
    if "credit" in lower:
        return "credits"
    return "nodata"


def _key_for_row(row: pd.Series | dict) -> str:
    raw_id = normalize_text(row.get("Raw ID", row.get("id", "")))
    if raw_id:
        return canonicalize_identifier(raw_id)
    url = normalize_text(row.get("LinkedIn URL", row.get("profile_url", "")))
    if url:
        return canonicalize_identifier(url)
    return ""


def _fill_value(value: str, source_value: str) -> str:
    if normalize_text(value):
        return normalize_text(value)
    return normalize_text(source_value)


def company_context_score(row: pd.Series | dict) -> int:
    score = 0
    checks = [
        "Current Company",
        "Current Title",
        "Industry",
        "Organization 1",
        "Organization 1 Title",
        "Organization 1 Description",
        "Organization 1 Website",
        "Organization 1 Domain",
        "Organization 2",
        "Organization 2 Title",
        "Organization 2 Description",
        "Organization 2 Website",
        "Organization 2 Domain",
        "Organization 3",
        "Organization 3 Title",
        "Organization 3 Description",
        "Organization 3 Website",
        "Organization 3 Domain",
    ]
    for col in checks:
        if normalize_text(row.get(col, "")):
            score += 1
    return score


def company_backfill_reason(row: pd.Series | dict) -> str:
    company = normalize_text(row.get("Current Company", ""))
    title = normalize_text(row.get("Current Title", ""))
    industry = normalize_text(row.get("Industry", ""))
    org_desc = normalize_text(row.get("Organization 1 Description", ""))
    org_web = normalize_text(row.get("Organization 1 Website", ""))
    org_dom = normalize_text(row.get("Organization 1 Domain", ""))

    if not company:
        return "missing company"
    if not title:
        return "missing title"
    if not industry and not org_desc:
        return "missing company context"
    if not org_web and not org_dom:
        return "missing domain"
    if any(re.match(pattern, company.lower()) for pattern in AMBIGUOUS_COMPANY_PATTERNS):
        return "ambiguous company"
    return ""


def needs_company_backfill(row: pd.Series | dict, company_fit_score: int | None = None) -> bool:
    if company_context_score(row) < 6:
        return True
    if not normalize_text(row.get("Current Company", "")):
        return True
    if not normalize_text(row.get("Current Title", "")):
        return True
    if not normalize_text(row.get("Industry", "")) and not normalize_text(row.get("Organization 1 Description", "")):
        return True
    if not normalize_text(row.get("Organization 1 Website", "")) and not normalize_text(row.get("Organization 1 Domain", "")):
        return True
    if company_fit_score is not None and company_fit_score <= 2:
        return True
    return False


def backfill_source_for_row(row: pd.Series | dict, company_fit_score: int | None = None) -> BackfillDecision:
    reason = company_backfill_reason(row)
    score = company_context_score(row)
    needed = needs_company_backfill(row, company_fit_score=company_fit_score)
    if not needed:
        return BackfillDecision(False, "", "native", score)
    if reason in {"missing company", "missing title", "missing company context", "ambiguous company"}:
        return BackfillDecision(True, reason or "low confidence company_fit", "visit", score)
    if score <= 8 or company_fit_score is not None and company_fit_score <= 2:
        return BackfillDecision(True, reason or "low confidence company_fit", "credits", score)
    return BackfillDecision(True, reason or "low confidence company_fit", "visit", score)


def _canonical_source_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "Raw ID" not in frame.columns and "id" in frame.columns:
        frame["Raw ID"] = frame["id"]
    if "LinkedIn URL" not in frame.columns and "profile_url" in frame.columns:
        frame["LinkedIn URL"] = frame["profile_url"]
    frame["backfill_key"] = frame.apply(_key_for_row, axis=1)
    frame["backfill_mode"] = frame["__source_name"].map(_mode_from_name)
    frame["backfill_priority"] = frame["backfill_mode"].map(BACKFILL_PRIORITY).fillna(0).astype(int)
    richness_cols = [c for c in ORG_FIELDS if c in frame.columns]
    if richness_cols:
        richness_frame = frame[richness_cols].fillna("").astype(str).applymap(normalize_text)
        frame["backfill_richness"] = richness_frame.ne("").sum(axis=1)
    else:
        frame["backfill_richness"] = 0
    return frame


def load_company_sources(source_dir: str | Path) -> pd.DataFrame:
    source_dir = Path(source_dir)
    frames = []
    for path in sorted(source_dir.glob("*.csv")):
        if path.name.lower() == "full.csv":
            continue
        if not any(token in path.name.lower() for token in ("nodata", "visit", "credit")):
            continue
        try:
            df = pd.read_csv(path, dtype=str, low_memory=False)
        except Exception:
            continue
        if df.empty:
            continue
        df["__source_name"] = path.stem
        frames.append(_canonical_source_frame(df))
    if not frames:
        return pd.DataFrame(columns=["backfill_key"])
    combined = pd.concat(frames, ignore_index=True, sort=False)
    return combined


def build_company_source_index(source_dir: str | Path) -> pd.DataFrame:
    sources = load_company_sources(source_dir)
    if sources.empty:
        return sources
    sort_cols = ["backfill_priority", "backfill_richness"]
    sources = sources.sort_values(sort_cols, ascending=[False, False], kind="stable")
    sources = sources.drop_duplicates(subset=["backfill_key"], keep="first")
    return sources


def _fill_from_source(row: pd.Series, source_row: pd.Series) -> pd.Series:
    out = row.copy()
    mapping = {
        "Current Company": source_row.get("current_company", source_row.get("Current Company", "")),
        "Current Title": source_row.get("current_company_position", source_row.get("Current Title", "")),
        "Industry": source_row.get("current_company_industry", source_row.get("Industry", "")),
        "Organization 1": source_row.get("organization_1", source_row.get("Organization 1", "")),
        "Organization 1 Title": source_row.get("organization_title_1", source_row.get("Organization 1 Title", "")),
        "Organization 1 Description": source_row.get("organization_description_1", source_row.get("Organization 1 Description", "")),
        "Organization 1 Website": source_row.get("organization_website_1", source_row.get("Organization 1 Website", "")),
        "Organization 1 Domain": source_row.get("organization_domain_1", source_row.get("Organization 1 Domain", "")),
        "Organization 2": source_row.get("organization_2", source_row.get("Organization 2", "")),
        "Organization 2 Title": source_row.get("organization_title_2", source_row.get("Organization 2 Title", "")),
        "Organization 2 Description": source_row.get("organization_description_2", source_row.get("Organization 2 Description", "")),
        "Organization 2 Website": source_row.get("organization_website_2", source_row.get("Organization 2 Website", "")),
        "Organization 2 Domain": source_row.get("organization_domain_2", source_row.get("Organization 2 Domain", "")),
        "Organization 3": source_row.get("organization_3", source_row.get("Organization 3", "")),
        "Organization 3 Title": source_row.get("organization_title_3", source_row.get("Organization 3 Title", "")),
        "Organization 3 Description": source_row.get("organization_description_3", source_row.get("Organization 3 Description", "")),
        "Organization 3 Website": source_row.get("organization_website_3", source_row.get("Organization 3 Website", "")),
        "Organization 3 Domain": source_row.get("organization_domain_3", source_row.get("Organization 3 Domain", "")),
    }
    for target_col, source_value in mapping.items():
        out[target_col] = _fill_value(out.get(target_col, ""), source_value)
    out["Company Context Source"] = source_row.get("backfill_mode", out.get("Company Context Source", "native") or "native")
    out["Company Backfill Needed"] = "yes" if needs_company_backfill(out) else "no"
    out["Company Backfill Reason"] = company_backfill_reason(out)
    out["Company Context Score"] = company_context_score(out)
    return out


def enrich_company_context(base: pd.DataFrame, source_dir: str | Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    enriched = base.copy()
    enriched["backfill_key"] = enriched.apply(_key_for_row, axis=1)
    enriched["Company Context Source"] = "native"
    enriched["Company Backfill Needed"] = "no"
    enriched["Company Backfill Reason"] = ""
    enriched["Company Context Score"] = enriched.apply(company_context_score, axis=1)

    if source_dir is None:
        enriched["Company Backfill Needed"] = enriched.apply(lambda row: "yes" if needs_company_backfill(row) else "no", axis=1)
        enriched["Company Backfill Reason"] = enriched.apply(company_backfill_reason, axis=1)
        return enriched, pd.DataFrame()

    source_index = build_company_source_index(source_dir)
    if source_index.empty:
        enriched["Company Backfill Needed"] = enriched.apply(lambda row: "yes" if needs_company_backfill(row) else "no", axis=1)
        enriched["Company Backfill Reason"] = enriched.apply(company_backfill_reason, axis=1)
        return enriched, pd.DataFrame()

    source_map = source_index.set_index("backfill_key")
    report_rows = []
    for idx, row in enriched.iterrows():
        decision = backfill_source_for_row(row)
        source_row = source_map.loc[row["backfill_key"]] if row["backfill_key"] in source_map.index else None
        if decision.needed and source_row is not None:
            row = _fill_from_source(row, source_row)
            report_rows.append({
                "Match Key": canonical_match_key(row.get("Raw ID", ""), row.get("Best Email", "")),
                "backfill_key": row["backfill_key"],
                "source_mode": row.get("Company Context Source", "native"),
                "decision_source": decision.source,
                "company_context_score": decision.context_score,
                "reason": decision.reason,
            })
        else:
            row["Company Backfill Needed"] = "yes" if decision.needed else "no"
            row["Company Backfill Reason"] = decision.reason
            row["Company Context Source"] = "native"
        row["Company Context Score"] = company_context_score(row)
        row["Company Backfill Needed"] = "yes" if needs_company_backfill(row) else "no"
        row["Company Backfill Reason"] = company_backfill_reason(row)
        for col, value in row.items():
            enriched.at[idx, col] = value
    report = pd.DataFrame(report_rows)
    if not report.empty:
        report = report.drop_duplicates(subset=["backfill_key"], keep="first")
    return enriched, report


def select_company_backfill_candidates(scored: pd.DataFrame, min_weighted_score: float = 50.0) -> pd.DataFrame:
    df = scored.copy()
    if "weighted_score" not in df.columns:
        return df.iloc[0:0].copy()
    needed_col = df.get("Company Backfill Needed", "no")
    if isinstance(needed_col, pd.Series):
        needed_mask = needed_col.astype(str).str.lower().eq("yes")
    else:
        needed_mask = pd.Series([False] * len(df), index=df.index)
    score_mask = pd.to_numeric(df["weighted_score"], errors="coerce").fillna(-1) >= float(min_weighted_score)
    candidates = df[needed_mask & score_mask].copy()
    if candidates.empty:
        return candidates
    candidate_cols = [
        c for c in [
            "Match Key",
            "Raw ID",
            "Best Email",
            "Full Name",
            "LinkedIn URL",
            "Current Company",
            "Current Title",
            "weighted_score",
            "legacy_weighted_score",
            "score_band",
            "Company Context Source",
            "Company Backfill Needed",
            "Company Backfill Reason",
            "Company Context Score",
            "Organization 1",
            "Organization 1 Title",
            "Organization 1 Description",
            "Organization 1 Website",
            "Organization 1 Domain",
            "Organization 2",
            "Organization 2 Title",
            "Organization 2 Description",
            "Organization 2 Website",
            "Organization 2 Domain",
            "Organization 3",
            "Organization 3 Title",
            "Organization 3 Description",
            "Organization 3 Website",
            "Organization 3 Domain",
        ]
        if c in candidates.columns
    ]
    ordered = [c for c in ["weighted_score", "Company Context Score"] if c in candidates.columns]
    return candidates[candidate_cols].sort_values(ordered, ascending=[False] * len(ordered), kind="stable") if ordered else candidates[candidate_cols]
