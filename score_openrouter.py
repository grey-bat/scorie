import argparse
import asyncio
import csv
import json
import os
import subprocess
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from utils import (
    canonicalize_identifier,
    ensure_dir,
    make_autopilot_header_lines,
    make_autopilot_row_line,
    make_header_lines,
    make_row_line,
    normalize_text,
    parse_json_from_content,
    to_int_score,
)
from composite_formula import (
    DIRECT_SCORE_COLUMNS,
    direct_score,
    family_office_total,
    fintech_total,
    legacy_weighted_score,
    load_composite_config,
    score_band,
    weighted_score,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RUBRIC_PATH = SCRIPT_DIR / "scoring_rubric.md"


class TransientBatchError(RuntimeError):
    pass


SPEED_PRESETS = {
    "safe": {"batch_size": 2, "concurrency": 1},
    "fast": {"batch_size": 4, "concurrency": 4},
    "aggressive": {"batch_size": 5, "concurrency": 6},
}

_FO_GENERAL_KEYWORDS = ["family office", "private office", "wealth", "investment", "capital", "gestora", "asset"]
_FO_SPECIFIC_KEYWORDS = ["family office", "private office"]
_FO_SENIOR_KEYWORDS = ["partner", "principal", "cio", "portfolio manager", "investor", "investment committee"]
_FO_TOP_KEYWORDS = ["founder", "partner", "principal", "cio", "investor", "investing"]
_FT_GENERAL_KEYWORDS = ["fintech", "payments", "neobank", "embedded finance", "wallet", "stablecoin", "blockchain", "web3", "digital bank"]
_FT_SENIOR_KEYWORDS = ["head of digital", "innovation", "treasury", "banking infrastructure", "partnerships", "product"]
_ALLOC_TOP_KEYWORDS = ["partner", "principal", "cio", "cfo", "founder", "president", "managing director", "chief investment officer"]
_ALLOC_DECISION_KEYWORDS = ["investment committee", "portfolio allocation", "capital allocation", "budget owner"]
_ALLOC_MID_KEYWORDS = ["director", "head", "vp", "vice president", "manager"]
from composite_formula import DEFAULT_DIRECT_POINT_MAPS as _DEFAULT_DPM

# Active point maps used for output validation; overridden at runtime by the
# loaded rubric in main() via set_direct_point_help().
DIRECT_POINT_HELP = {k: list(v) for k, v in _DEFAULT_DPM.items()}
DIRECT_POINT_SETS = [values for values in DIRECT_POINT_HELP.values()]


def set_direct_point_help(point_maps: dict) -> None:
    """Replace the module-level point help from a loaded rubric's point maps."""
    global DIRECT_POINT_HELP, DIRECT_POINT_SETS
    DIRECT_POINT_HELP = {k: list(v) for k, v in point_maps.items() if v}
    DIRECT_POINT_SETS = [values for values in DIRECT_POINT_HELP.values()]


def load_rubric_text(rubric_path: str | Path) -> str:
    return Path(rubric_path).read_text(encoding="utf-8")


def _format_allowed_values_block() -> str:
    """Emit the active allowed-value set per dimension, to be injected into the
    system prompt so the model cannot return null / empty / out-of-set values
    even if the rubric prose is ambiguous."""
    lines = ["Allowed values per dimension (use EXACTLY one of these integers, never null, never empty, never absent):"]
    for name, values in DIRECT_POINT_HELP.items():
        lines.append(f"- {name}: one of {values}")
    return "\n".join(lines)


def build_system_prompt(scoring_mode: str, rubric_text: str) -> str:
    common = (
        "You are a scoring engine for B2B lead qualification. "
        "Read every provided field carefully. Count synonyms, translations, equivalent phrases, and near-synonyms. "
        "Use current_company + current_position + current_company_description + current_position_description as the primary decision surface. "
        "additional_role is supporting evidence, not a replacement. "
        "Return ONLY valid JSON. No markdown. No prose. "
        "Return a single JSON object with a top-level key named results. "
        "results must be an array of objects, one per input record, in the same order. "
        "member_id must exactly equal the input member_id string. "
        "Every score field MUST be a non-null integer drawn from the allowed-values list; "
        "never return null, never return an empty string, never omit a key. "
        "If you are uncertain, pick the LOWEST bucket rather than returning null. "
    )
    if scoring_mode == "autopilot_direct_100":
        # Data-driven: dims list comes from the active rubric's point maps
        # so 2-axis rubrics tell the LLM to return only company_fit + role_fit.
        dims = list(DIRECT_POINT_HELP.keys())
        keys_list = ", ".join(["member_id", *dims])
        return (
            common
            + f"Each object must contain exactly these keys: {keys_list}. "
            + "Use only allowed direct point values from the rubric and do not calculate extra formulas.\n\n"
            + _format_allowed_values_block()
            + "\n\n"
            + rubric_text
        )
    return (
        common
        + "Each object must contain exactly these keys: member_id, fo_persona, ft_persona, allocator, access, company_fit. "
        + "All five scores must be integers from 0 to 5. "
        + "Do not calculate derived formulas.\n\n"
        + rubric_text
    )


def infer_role_fit_points(record: dict) -> int:
    title_text = f"{normalize_text(record.get('current_position', ''))} {normalize_text(record.get('headline', ''))}".lower()
    if any(k in title_text for k in _ALLOC_TOP_KEYWORDS):
        return 5
    if any(k in title_text for k in _ALLOC_MID_KEYWORDS):
        return 4
    if any(k in title_text for k in ["analyst", "associate", "manager"]):
        return 3
    return 2 if title_text else 1


def normalize_direct_value(name: str, value, record: dict | None = None) -> int:
    allowed = DIRECT_POINT_HELP[name]
    if value is None or value == "":
        if name == "role_fit" and record is not None:
            inferred = infer_role_fit_points(record)
            return allowed[max(0, min(4, inferred - 1))]
        raise ValueError(f"missing direct score for {name}")
    parsed = int(round(float(value)))
    if parsed <= 0:
        return allowed[0]
    if 1 <= parsed <= 5 and allowed != [1, 2, 3, 4, 5]:
        return allowed[parsed - 1]
    if parsed in allowed:
        return parsed
    for point_set in DIRECT_POINT_SETS:
        if parsed in point_set:
            ordinal = point_set.index(parsed)
            if ordinal < len(allowed):
                return allowed[ordinal]
    if 1 <= parsed <= 5:
        return allowed[parsed - 1]
    if name == "role_fit" and record is not None:
        inferred = infer_role_fit_points(record)
        return allowed[max(0, min(4, inferred - 1))]
    # Last resort: the model returned a value that isn't in this dimension's
    # bucket and isn't in any other dimension's bucket either (e.g. a leftover
    # cap from an older rubric like 20 or 25). Snap to the nearest allowed
    # bucket rather than hard-failing the whole batch.
    if parsed > max(allowed):
        return allowed[-1]
    if parsed < min(allowed):
        return allowed[0]
    nearest = min(allowed, key=lambda a: (abs(a - parsed), a))
    return nearest


def compact_record(row: pd.Series) -> dict:
    # Member ID is the payload primary key. Fall back to URN column, Raw ID,
    # then Match Key so we always have something non-empty to tie back to Notion.
    member_id = (
        normalize_text(row.get("Member ID", ""))
        or normalize_text(row.get("member_id", ""))
        or normalize_text(row.get("URN", ""))
        or normalize_text(row.get("Raw ID", ""))
        or normalize_text(row.get("Match Key", ""))
    )
    add_org = normalize_text(row.get("Organization 2", ""))
    add_title = normalize_text(row.get("Organization 2 Title", ""))
    add_desc = normalize_text(row.get("Organization 2 Description", ""))
    add_pos_desc = normalize_text(row.get("Position 2 Description", ""))
    add_website = normalize_text(row.get("Organization 2 Website", ""))
    additional_role = None
    if any([add_org, add_title, add_desc, add_pos_desc, add_website]):
        additional_role = {
            "organization": add_org,
            "title": add_title,
            "description": add_desc,
            "position_description": add_pos_desc,
            "website": add_website,
        }
    payload = {
        "_match_key": normalize_text(row.get("Match Key", "")),
        "_raw_id": normalize_text(row.get("Raw ID", "")),
        "member_id": member_id,
        "full_name": normalize_text(row.get("Full Name", "")),
        "location": normalize_text(row.get("Location", "")),
        "current_company": normalize_text(row.get("Current Company", "")) or normalize_text(row.get("Organization 1", "")),
        "current_position": normalize_text(row.get("Current Title", "")) or normalize_text(row.get("Organization 1 Title", "")),
        "current_industry": normalize_text(row.get("Industry", "")),
        "current_company_description": normalize_text(row.get("Organization 1 Description", "")),
        "current_position_description": normalize_text(row.get("Position 1 Description", "")),
        "additional_role": additional_role,
        "headline": normalize_text(row.get("Headline", "")),
        "summary": normalize_text(row.get("Summary", "")),
        "mutual_count": int(row.get("Mutual Count", 0) or 0),
        "degree": int(row.get("Degree", 3) or 3),
        "alumni_signal": normalize_text(row.get("Alumni Signal", "")),
    }
    return payload


def build_user_prompt(records: list[dict]) -> str:
    payload_records = [{k: v for k, v in record.items() if not k.startswith("_")} for record in records]
    return json.dumps({"records": payload_records}, ensure_ascii=False)


def build_canonical_id_map(ids: list[str]) -> dict[str, str]:
    canonical_map: dict[str, str] = {}
    seen: dict[str, str] = {}
    for rid in ids:
        key = canonicalize_identifier(rid)
        if not key:
            raise RuntimeError(f"Empty canonical id for {rid!r}")
        if key in seen and seen[key] != rid:
            raise RuntimeError(f"Ambiguous canonical ids in batch: {seen[key]!r} and {rid!r}")
        seen[key] = rid
        canonical_map[key] = rid
    return canonical_map


def _extract_member_id(item: dict) -> str:
    for k in ("member_id", "URN", "urn"):
        if k in item and str(item[k]).strip():
            return str(item[k])
    raise RuntimeError(f"Output item missing member_id: {str(item)[:200]!r}")


def remap_batch_results(records: list[dict], out: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (matched_items_in_input_order, missing_records).

    Partial outputs are supported: if the model returns only a subset of
    records, we return the matched subset in input order plus the list of
    input records that had no corresponding output. The caller can re-queue
    the missing records rather than tossing the whole batch.
    """
    in_ids = [r["member_id"] for r in records]
    in_map = build_canonical_id_map(in_ids)
    out_map: dict[str, dict] = {}
    remap_pairs = []
    unknown_out: list[str] = []
    for item in out:
        try:
            item_member_id = _extract_member_id(item)
        except RuntimeError as e:
            print(f"Dropping malformed output item: {e}", flush=True)
            continue
        canonical_id = canonicalize_identifier(item_member_id)
        if canonical_id not in in_map:
            unknown_out.append(item_member_id)
            continue
        original_id = in_map[canonical_id]
        record = next(rec for rec in records if rec["member_id"] == original_id)
        if item_member_id != original_id:
            remap_pairs.append((item_member_id, original_id))
        normalized_item = dict(item)
        normalized_item["member_id"] = original_id
        # Also emit URN for downstream CSV compatibility (same value).
        normalized_item["URN"] = original_id
        normalized_item["Match Key"] = record["_match_key"]
        normalized_item["Raw ID"] = record["_raw_id"]
        out_map[original_id] = normalized_item
    if remap_pairs:
        preview = ", ".join(f"{src} -> {dst}" for src, dst in remap_pairs[:5])
        if len(remap_pairs) > 5:
            preview += f", ... (+{len(remap_pairs) - 5} more)"
        print(f"Canonical ID remap: {preview}", flush=True)
    if unknown_out:
        print(f"Dropping {len(unknown_out)} unknown output member_ids (e.g. {unknown_out[:3]})", flush=True)
    missing = [rec for rec in records if rec["member_id"] not in out_map]
    if missing:
        print(
            f"Partial batch result: got {len(out_map)}/{len(records)}, "
            f"re-queueing {len(missing)} missing record(s).",
            flush=True,
        )
    matched = [out_map[rid] for rid in in_ids if rid in out_map]
    return matched, missing


def maybe_recover_capacity(
    current_concurrency: int,
    current_batch_size: int,
    initial_concurrency: int,
    initial_batch_size: int,
    last_adjustment_at: float | None,
    recovery_delay: int,
) -> tuple[int, int, bool]:
    if last_adjustment_at is None or recovery_delay <= 0:
        return current_concurrency, current_batch_size, False
    if time.monotonic() - last_adjustment_at < recovery_delay:
        return current_concurrency, current_batch_size, False
    new_concurrency = min(initial_concurrency, current_concurrency + 1)
    new_batch_size = min(initial_batch_size, current_batch_size + 1)
    if new_concurrency == current_concurrency and new_batch_size == current_batch_size:
        return current_concurrency, current_batch_size, False
    return new_concurrency, new_batch_size, True


def extract_assistant_content(data: dict) -> str:
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"OpenRouter response missing assistant content: {e!r}") from e
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("OpenRouter response had empty assistant content")
    return content


async def call_openrouter(
    session: aiohttp.ClientSession,
    model: str,
    records: list[dict],
    system_prompt: str,
    scoring_mode: str,
    retries: int = 5,
):
    record_lookup = {normalize_text(record["member_id"]): record for record in records}
    # Reasoning models (Gemini 2.5/3.x Pro, GPT-5, Claude thinking, etc.)
    # spend most of max_tokens on internal reasoning BEFORE emitting JSON, so
    # "500 tokens per record" truncates the answer (finish_reason=length) and
    # items come back with only member_id populated. Give ~3k per record for
    # reasoning headroom plus a healthy floor; cap at 32k to stay under the
    # per-key credit ceiling observed on OpenRouter.
    max_out_tokens = min(32000, max(8000, 3000 * len(records)))
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": max_out_tokens,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": build_user_prompt(records)},
        ],
    }
    wait = 2.0
    last_error = None
    for _attempt in range(retries):
        attempt = _attempt + 1
        try:
            async with session.post("https://openrouter.ai/api/v1/chat/completions", json=payload) as resp:
                text = await resp.text()
                if resp.status in (408, 409, 425, 429, 500, 502, 503, 504):
                    last_error = f"transient_http_{resp.status}: {text[:300]}"
                    print(f"Retrying OpenRouter batch after HTTP {resp.status} ({attempt}/{retries})", flush=True)
                    await asyncio.sleep(wait + random.random())
                    wait = min(wait * 1.8, 45)
                    continue
                if resp.status >= 400:
                    raise RuntimeError(f"OpenRouter error {resp.status}: {text[:1200]}")
                data = json.loads(text)
                try:
                    content = extract_assistant_content(data)
                except RuntimeError as e:
                    last_error = str(e)
                    print(
                        f"Retrying OpenRouter batch after empty assistant content ({attempt}/{retries})",
                        flush=True,
                    )
                    await asyncio.sleep(wait + random.random())
                    wait = min(wait * 1.8, 45)
                    continue
                parsed = parse_json_from_content(content)
                if not isinstance(parsed, dict) or "results" not in parsed:
                    raise RuntimeError(f"Model returned invalid JSON wrapper: {str(parsed)[:800]}")
                results = parsed["results"]
                if not isinstance(results, list):
                    raise RuntimeError(f"results is not a list: {str(results)[:800]}")
                out = []
                dropped_malformed = 0
                # Data-driven: the rubric's point maps (loaded into
                # DIRECT_POINT_HELP at startup) dictate which dimensions the
                # LLM must return. This makes 2-axis rubrics (company+role
                # only) and 5-axis rubrics share the same code path.
                active_direct_dims = list(DIRECT_POINT_HELP.keys())
                required_keys = {"member_id", *active_direct_dims} if scoring_mode == "autopilot_direct_100" else {
                    "member_id", "fo_persona", "ft_persona", "allocator", "access", "company_fit"
                }
                for item in results:
                    # Accept legacy 'urn' key if the model still uses it.
                    if "member_id" not in item and "urn" in item:
                        item["member_id"] = item["urn"]
                    missing = required_keys - set(item)
                    # role_fit is the only dim we will infer from title text
                    # if the model forgets it, so tolerate that single-key gap.
                    if scoring_mode == "autopilot_direct_100" and missing == {"role_fit"}:
                        missing = set()
                    if missing:
                        # Skip this one item; remap_batch_results will flag
                        # its record as 'missing' and the caller will retry
                        # just that subset instead of killing the whole batch.
                        dropped_malformed += 1
                        continue
                    try:
                        if scoring_mode == "autopilot_direct_100":
                            record = record_lookup.get(normalize_text(item["member_id"]), {})
                            row_out = {
                                "member_id": normalize_text(item["member_id"]),
                                "URN": normalize_text(item["member_id"]),
                            }
                            for dim in active_direct_dims:
                                row_out[dim] = normalize_direct_value(dim, item.get(dim), record)
                            out.append(row_out)
                        else:
                            out.append({
                                "member_id": normalize_text(item["member_id"]),
                                "URN": normalize_text(item["member_id"]),
                                "fo_persona": to_int_score(item["fo_persona"]),
                                "ft_persona": to_int_score(item["ft_persona"]),
                                "allocator": to_int_score(item["allocator"]),
                                "access": to_int_score(item["access"]),
                                "company_fit": to_int_score(item["company_fit"]),
                            })
                    except (ValueError, TypeError, KeyError) as item_err:
                        # A single item had an invalid/missing per-field value
                        # (e.g., access=null). Treat as malformed-per-record so
                        # the good items in this batch still make it through
                        # and only this one gets re-queued.
                        dropped_malformed += 1
                        print(
                            f"Dropping malformed item for member_id="
                            f"{item.get('member_id')!r}: {item_err!r}",
                            flush=True,
                        )
                        continue
                matched, missing = remap_batch_results(records, out)
                if dropped_malformed:
                    finish_reason = ""
                    try:
                        finish_reason = str(data.get("choices", [{}])[0].get("finish_reason", ""))
                    except Exception:
                        pass
                    usage = data.get("usage") or {}
                    print(
                        f"Partial batch: kept {len(matched)} / {len(records)} "
                        f"(dropped {dropped_malformed}; re-queueing {len(missing)}) "
                        f"finish_reason={finish_reason!r} "
                        f"completion_tokens={usage.get('completion_tokens')} "
                        f"max_tokens={max_out_tokens}",
                        flush=True,
                    )
                return matched, missing
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            last_error = repr(e)
            print(f"Retrying OpenRouter batch after network timeout/error ({attempt}/{retries}): {last_error}", flush=True)
            await asyncio.sleep(wait + random.random())
            wait = min(wait * 1.8, 45)
            continue
    raise TransientBatchError(last_error or "OpenRouter request failed after retries")


def deterministic_mock(records: list[dict], scoring_mode: str = "legacy_raw_weighted") -> list[dict]:
    out = []
    for rec in records:
        text = " ".join(str(rec.get(k, "")) for k in rec if k != "company_context").lower()
        company_context = " ".join(
            " ".join(
                str(part.get(k, "")) for k in ("organization", "title", "description", "website", "domain")
            )
            for part in rec.get("company_context", [])
        ).lower()
        text = f"{text} {company_context}"
        fo = 0
        ft = 0
        alloc = 1
        access = 0
        company_fit = 0
        if any(k in text for k in _FO_GENERAL_KEYWORDS):
            fo = 3
        if any(k in text for k in _FO_SPECIFIC_KEYWORDS):
            fo = 4
        if any(k in text for k in _FO_SENIOR_KEYWORDS):
            fo = max(fo, 4)
        if any(k in text for k in _FO_SPECIFIC_KEYWORDS) and any(k in text for k in _FO_TOP_KEYWORDS):
            fo = 5
        if any(k in text for k in _FT_GENERAL_KEYWORDS):
            ft = 3
        if any(k in text for k in _FT_SENIOR_KEYWORDS):
            ft = max(ft, 4)
        if any(k in text for k in _ALLOC_TOP_KEYWORDS):
            alloc = 4
        if any(k in text for k in _ALLOC_DECISION_KEYWORDS):
            alloc = 5
        elif any(k in text for k in _ALLOC_MID_KEYWORDS):
            alloc = max(alloc, 3)
        alumni = rec.get("alumni_signal", "")
        mc_num = int(rec.get("mutual_count", 0) or 0)
        access = 4 if alumni == "Cal+CBS" else 3 if alumni == "CBS" else 2 if alumni == "Cal" else 0
        if mc_num >= 1:
            access = max(access, 2)
        if mc_num >= 10:
            access = max(access, 3)
        if mc_num >= 50:
            access = max(access, 4)
        if rec.get("company_backfill_needed") == "yes":
            company_fit = 1
        company_keywords = [
            "family office", "wealth", "private bank", "asset management", "investment", "capital", "fintech",
            "payments", "bank", "treasury", "portfolio", "vc", "venture", "credit", "lending", "neobank",
        ]
        company_hits = sum(1 for k in company_keywords if k in text)
        if company_hits >= 1:
            company_fit = max(company_fit, 3)
        if company_hits >= 3:
            company_fit = max(company_fit, 4)
        if any(k in text for k in ["family office", "wealth management", "private bank", "investment committee"]):
            company_fit = 5
        if not rec.get("current_company"):
            company_fit = min(company_fit, 1)
        if scoring_mode == "autopilot_direct_100":
            title_text = f"{normalize_text(rec.get('current_title', ''))} {normalize_text(rec.get('headline', ''))}".lower()
            role_band = 1
            if any(k in title_text for k in _ALLOC_TOP_KEYWORDS):
                role_band = 5
            elif any(k in title_text for k in _ALLOC_MID_KEYWORDS):
                role_band = 4
            elif any(k in title_text for k in ["analyst", "associate", "manager"]):
                role_band = 3
            out.append({
                "URN": rec.get("member_id") or rec.get("urn", ""),
                "member_id": rec.get("member_id") or rec.get("urn", ""),
                "company_fit": DIRECT_POINT_HELP["company_fit"][max(0, min(4, company_fit - 1))],
                "family_office_relevance": DIRECT_POINT_HELP["family_office_relevance"][max(0, min(4, fo - 1))],
                "fintech_relevance": DIRECT_POINT_HELP["fintech_relevance"][max(0, min(4, ft - 1))],
                "allocator_power": DIRECT_POINT_HELP["allocator_power"][max(0, min(4, alloc - 1))],
                "access": DIRECT_POINT_HELP["access"][max(0, min(4, access - 1))],
                "role_fit": DIRECT_POINT_HELP["role_fit"][max(0, min(4, role_band - 1))],
            })
            continue
        out.append({
            "URN": rec.get("member_id") or rec.get("urn", ""),
            "member_id": rec.get("member_id") or rec.get("urn", ""),
            "fo_persona": min(5, fo),
            "ft_persona": min(5, ft),
            "allocator": min(5, alloc),
            "access": min(5, access),
            "company_fit": min(5, company_fit),
        })
    return out


def score_fieldnames(scoring_mode: str) -> list[str]:
    if scoring_mode == "autopilot_direct_100":
        # Use the active dims loaded from the rubric's point maps so 2-axis
        # rubrics produce a 2-dim CSV and 5-axis rubrics produce a 5-dim CSV.
        active_dims = list(DIRECT_POINT_HELP.keys()) or list(DIRECT_SCORE_COLUMNS)
        return [
            "Match Key",
            "URN",
            "Raw ID",
            "Best Email",
            *active_dims,
            "fo_total",
            "ft_total",
            "direct_score",
            "overall_direct_score",
            "score_band",
            "score_track",
        ]
    return [
        "Match Key",
        "URN",
        "Raw ID",
        "Best Email",
        "fo_persona",
        "ft_persona",
        "allocator",
        "access",
        "company_fit",
        "weighted_score",
        "legacy_weighted_score",
        "score_band",
        "score_track",
    ]


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "minimax/minimax-m2.7"))
    ap.add_argument("--speed", choices=sorted(SPEED_PRESETS.keys()))
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "32")))
    ap.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "12")))
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--start-row", type=int, default=1)
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--max-failures-per-record", type=int, default=3)
    ap.add_argument("--batch-retries", type=int, default=int(os.getenv("OPENROUTER_BATCH_RETRIES", "2")))
    ap.add_argument("--recovery-delay", type=int, default=int(os.getenv("OPENROUTER_RECOVERY_DELAY", "30")))
    ap.add_argument("--timeout-total", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_TOTAL", "420")))
    ap.add_argument("--timeout-connect", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_CONNECT", "20")))
    ap.add_argument("--timeout-sock-connect", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_SOCK_CONNECT", "20")))
    ap.add_argument("--timeout-sock-read", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_SOCK_READ", "300")))
    ap.add_argument("--sync-notion", action="store_true")
    ap.add_argument("--sync-notion-every-waves", type=int, default=1)
    ap.add_argument("--scoring-mode", choices=["legacy_raw_weighted", "autopilot_direct_100"], default="legacy_raw_weighted")
    ap.add_argument("--rubric-path", default=str(DEFAULT_RUBRIC_PATH))
    return ap


def load_todo(args) -> tuple[pd.DataFrame, set, Path, Path, Path]:
    ensure_dir(args.out)
    df = pd.read_csv(args.input, dtype={"Match Key": str, "URN": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    df = df.iloc[max(0, args.start_row - 1):].copy()
    if args.max_records:
        df = df.iloc[: args.max_records]
    results_csv = Path(args.out) / "scores_raw.csv"
    progress_jsonl = Path(args.out) / "scores_progress.jsonl"
    failed_jsonl = Path(args.out) / "failed_batches.jsonl"
    done_ids: set = set()
    if results_csv.exists():
        prev = pd.read_csv(results_csv, dtype={"Match Key": str})
        if "Match Key" in prev.columns:
            done_ids = set(prev["Match Key"].map(normalize_text).tolist())
    todo = df[~df["Match Key"].map(normalize_text).isin(done_ids)].copy()
    return todo, done_ids, results_csv, progress_jsonl, failed_jsonl


def open_api_session(args) -> "aiohttp.ClientSession | None":
    if args.mock:
        return None
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required unless --mock is used.")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(
        total=args.timeout_total,
        connect=args.timeout_connect,
        sock_connect=args.timeout_sock_connect,
        sock_read=args.timeout_sock_read,
    )
    return aiohttp.ClientSession(headers=headers, timeout=timeout)


def _flush_batch_sync(
    batch_out: list,
    *,
    meta_by_index: dict,
    results_csv: Path,
    progress_jsonl: Path,
    counter_ref: list,
    scoring_mode: str,
    composite_config,
) -> None:
    with open(results_csv, "a", newline="", encoding="utf-8") as fcsv, \
            open(progress_jsonl, "a", encoding="utf-8") as fj:
        writer = csv.DictWriter(fcsv, fieldnames=score_fieldnames(scoring_mode))
        for item in batch_out:
            meta = meta_by_index[item["_seq"]]
            row = {
                "Match Key": item["Match Key"],
                "URN": normalize_text(meta.get("URN", "")),
                "Raw ID": normalize_text(meta.get("Raw ID", "")),
                "Best Email": normalize_text(meta.get("Best Email", "")),
                "score_track": scoring_mode,
            }
            if scoring_mode == "autopilot_direct_100":
                # Only write the dims the active rubric actually produces.
                for field in DIRECT_POINT_HELP:
                    row[field] = item.get(field, 0)
                row["fo_total"] = family_office_total(row)
                row["ft_total"] = fintech_total(row)
                row["direct_score"] = row["ft_total"]
                row["overall_direct_score"] = direct_score(row, composite_config.direct_point_maps)
                row["score_band"] = score_band(row["direct_score"], composite_config.score_bands)
            else:
                row.update({
                    "fo_persona": item["fo_persona"],
                    "ft_persona": item["ft_persona"],
                    "allocator": item["allocator"],
                    "access": item["access"],
                    "company_fit": item["company_fit"],
                })
                row["weighted_score"] = weighted_score(row, composite_config.weights)
                row["legacy_weighted_score"] = legacy_weighted_score(row, composite_config.legacy_weights)
                row["score_band"] = score_band(row["weighted_score"], composite_config.score_bands)
            writer.writerow(row)
            counter_ref[0] += 1
            display_row = {
                "done": counter_ref[0],
                "Full Name": meta.get("Full Name", ""),
                "Current Company": meta.get("Current Company", ""),
                "Manual": normalize_text(meta.get("Manual", "")),
                "Degree": meta.get("Degree", ""),
                "Headline": meta.get("Headline", ""),
                "Summary": meta.get("Summary", ""),
            }
            if scoring_mode == "autopilot_direct_100":
                # Role/access/fintech/allocator may be absent from 2-axis
                # rubrics; default to blank so the live table still renders.
                role_fit = item.get("role_fit", "")
                fo_total = row["fo_total"]
                ft_total = row["ft_total"]
                display_row.update({
                    "fo_total": fo_total,
                    "ft_total": ft_total,
                    "score_band": row["score_band"],
                    "allocator": item.get("allocator_power", ""),
                    "company_fit": item.get("company_fit", ""),
                    "ft_relevance": item.get("fintech_relevance", ""),
                    "access": item.get("access", ""),
                    "role_fit": role_fit,
                })
            else:
                display_row.update({
                    "fo_persona": item["fo_persona"],
                    "ft_persona": item["ft_persona"],
                    "allocator": item["allocator"],
                    "company_fit": item["company_fit"],
                    "access": item["access"],
                })
            progress_row = dict(row)
            progress_row["Full Name"] = normalize_text(meta.get("Full Name", ""))
            progress_row["Current Company"] = normalize_text(meta.get("Current Company", ""))
            progress_row["Current Title"] = normalize_text(meta.get("Current Title", ""))
            progress_row["lead_score"] = row.get("direct_score", row.get("weighted_score", ""))
            if scoring_mode == "autopilot_direct_100":
                progress_row["fo_total"] = fo_total
                progress_row["ft_total"] = ft_total
                progress_row["overall_direct_score"] = row["overall_direct_score"]
                progress_row["ft_relevance"] = item.get("fintech_relevance", "")
            fj.write(json.dumps(progress_row, ensure_ascii=False) + "\n")
            row_line = make_autopilot_row_line(display_row) if scoring_mode == "autopilot_direct_100" else make_row_line(display_row)
            print(row_line, flush=True)


async def _flush_batch(
    batch_out: list,
    *,
    meta_by_index: dict,
    results_csv: Path,
    progress_jsonl: Path,
    io_lock: asyncio.Lock,
    counter_ref: list,
    scoring_mode: str,
    composite_config,
) -> None:
    async with io_lock:
        await asyncio.to_thread(
            _flush_batch_sync,
            batch_out,
            meta_by_index=meta_by_index,
            results_csv=results_csv,
            progress_jsonl=progress_jsonl,
            counter_ref=counter_ref,
            scoring_mode=scoring_mode,
            composite_config=composite_config,
        )


async def _process_batch(
    batch: list,
    *,
    args,
    session: "aiohttp.ClientSession | None",
    batch_retry_count: int,
    meta_by_index: dict,
    results_csv: Path,
    progress_jsonl: Path,
    io_lock: asyncio.Lock,
    counter_ref: list,
    scoring_mode: str,
    composite_config,
    system_prompt: str,
) -> None:
    for attempt in range(batch_retry_count + 1):
        try:
            if args.mock:
                batch_out = deterministic_mock(batch, scoring_mode=scoring_mode)
            else:
                matched, missing = await call_openrouter(
                    session,
                    args.model,
                    batch,
                    system_prompt,
                    scoring_mode,
                )
                batch_out = matched
            # Tag each result with the _seq of the input record it corresponds to.
            rec_by_member = {rec["member_id"]: rec for rec in batch}
            for item in batch_out:
                rec = rec_by_member.get(item.get("member_id"))
                if rec is not None:
                    item["_seq"] = rec["_seq"]
            if batch_out:
                await _flush_batch(
                    batch_out,
                    meta_by_index=meta_by_index,
                    results_csv=results_csv,
                    progress_jsonl=progress_jsonl,
                    io_lock=io_lock,
                    counter_ref=counter_ref,
                    scoring_mode=scoring_mode,
                    composite_config=composite_config,
                )
            # If the LLM skipped some records, retry just the missing subset
            # instead of the whole batch (so successes are not lost).
            if missing and not args.mock:
                batch = missing
                print(
                    f"Retrying {len(missing)} missing record(s) from partial batch "
                    f"(attempt {attempt + 1}/{batch_retry_count + 1})",
                    flush=True,
                )
                # Count this as a retry step, but continue outer loop so we
                # re-enter call_openrouter for the reduced batch.
                await asyncio.sleep(min(2 ** attempt, 4) + random.random())
                continue
            return
        except Exception as e:
            if attempt < batch_retry_count:
                print(
                    "Retrying batch before backoff: "
                    f"attempt={attempt + 1}/{batch_retry_count} | "
                    f"first_member_id={batch[0].get('member_id') or batch[0].get('urn', '')} | "
                    f"error={repr(e)}",
                    flush=True,
                )
                await asyncio.sleep(min(2 ** attempt, 8) + random.random())
                continue
            raise


def _spawn_sync_notion(args) -> "asyncio.Task | None":
    if not args.sync_notion or args.mock:
        return None
    sync_workdir = Path(args.out).parent
    cmd = [sys.executable, str(SCRIPT_DIR / "sync_incremental_delta.py"), "--workdir", str(sync_workdir)]
    print(f"Partial Notion sync: workdir={sync_workdir}", flush=True)
    return asyncio.create_task(asyncio.to_thread(subprocess.run, cmd, check=True))


async def run_scoring_session(
    records: list,
    meta_by_index: dict,
    *,
    args,
    session: "aiohttp.ClientSession | None",
    results_csv: Path,
    progress_jsonl: Path,
    failed_jsonl: Path,
    counter_ref: list,
    scoring_mode: str,
    composite_config,
    system_prompt: str,
) -> None:
    io_lock = asyncio.Lock()
    failure_counts: dict = {}
    wave_count = 0
    current_batch_size = max(1, args.batch_size)
    current_concurrency = max(1, args.concurrency)
    initial_batch_size = current_batch_size
    initial_concurrency = current_concurrency
    last_adjustment_at = None
    batch_retry_count = max(0, args.batch_retries)
    pending = records[:]
    sync_task = None

    flush_kwargs = dict(
        meta_by_index=meta_by_index,
        results_csv=results_csv,
        progress_jsonl=progress_jsonl,
        io_lock=io_lock,
        counter_ref=counter_ref,
        scoring_mode=scoring_mode,
        composite_config=composite_config,
    )
    process_kwargs = dict(
        args=args,
        session=session,
        batch_retry_count=batch_retry_count,
        system_prompt=system_prompt,
        **flush_kwargs,
    )

    try:
        while pending:
            active = []
            for _ in range(current_concurrency):
                if not pending:
                    break
                batch = pending[:current_batch_size]
                pending = pending[current_batch_size:]
                active.append(batch)
            results = await asyncio.gather(
                *(_process_batch(batch, **process_kwargs) for batch in active),
                return_exceptions=True,
            )
            wave_count += 1
            failures = [(batch, res) for batch, res in zip(active, results) if isinstance(res, Exception)]
            if failures:
                old_c, old_b = current_concurrency, current_batch_size
                if current_concurrency > 1:
                    current_concurrency = max(1, current_concurrency // 2)
                elif current_batch_size > 1:
                    current_batch_size = max(1, current_batch_size // 2)
                print(
                    "Backoff: "
                    f"failures={len(failures)} | "
                    f"concurrency {old_c}->{current_concurrency} | "
                    f"batch_size {old_b}->{current_batch_size}",
                    flush=True,
                )
                last_adjustment_at = time.monotonic()
                async with io_lock:
                    with open(failed_jsonl, "a", encoding="utf-8") as ff:
                        for batch, err in failures:
                            print(
                                "Failed batch: "
                                f"size={len(batch)} | "
                                f"first_member_id={batch[0].get('member_id') or batch[0].get('urn', '')} | "
                                f"error={repr(err)}",
                                flush=True,
                            )
                            ff.write(json.dumps({
                                "event": "batch_failure",
                                "member_ids": [r.get("member_id") or r.get("urn", "") for r in batch],
                                "error": repr(err),
                                "new_concurrency": current_concurrency,
                                "new_batch_size": current_batch_size,
                            }, ensure_ascii=False) + "\n")
                for batch, err in reversed(failures):
                    if old_c == 1 and old_b == 1 and len(batch) == 1:
                        mk = batch[0].get("member_id") or batch[0].get("urn", "")
                        failure_counts[mk] = failure_counts.get(mk, 0) + 1
                        if failure_counts[mk] >= args.max_failures_per_record:
                            continue
                    pending = batch + pending
            else:
                new_c, new_b, recovered = maybe_recover_capacity(
                    current_concurrency, current_batch_size,
                    initial_concurrency, initial_batch_size,
                    last_adjustment_at, args.recovery_delay,
                )
                if recovered and (new_c != current_concurrency or new_b != current_batch_size):
                    print(
                        "Recovery: "
                        f"concurrency {current_concurrency}->{new_c} | "
                        f"batch_size {current_batch_size}->{new_b}",
                        flush=True,
                    )
                    current_concurrency, current_batch_size = new_c, new_b
                    last_adjustment_at = time.monotonic()

            if args.sync_notion and (wave_count % max(1, args.sync_notion_every_waves) == 0):
                if sync_task is None or sync_task.done():
                    if sync_task is not None:
                        try:
                            sync_task.result()
                        except Exception as e:
                            print(f"Partial Notion sync failed: {e!r}", flush=True)
                    sync_task = _spawn_sync_notion(args)
                else:
                    print("Partial Notion sync already in flight; skipping this wave.", flush=True)
    finally:
        if session is not None:
            await session.close()
        if sync_task is not None and not sync_task.done():
            try:
                await sync_task
            except Exception as e:
                print(f"Final Partial Notion sync failed: {e!r}", flush=True)


async def main():
    load_dotenv()
    args = build_arg_parser().parse_args()
    rubric_text = load_rubric_text(args.rubric_path)
    composite_config = load_composite_config(args.rubric_path)
    # Align the score-validation point maps with whatever the active rubric uses.
    set_direct_point_help(composite_config.direct_point_maps)
    system_prompt = build_system_prompt(args.scoring_mode, rubric_text)
    if args.speed:
        preset = SPEED_PRESETS[args.speed]
        args.batch_size = preset["batch_size"]
        args.concurrency = preset["concurrency"]

    todo, done_ids, results_csv, progress_jsonl, failed_jsonl = load_todo(args)
    if todo.empty:
        print("Nothing to do.")
        return

    print(f"!!! STARTING NEW SESSION AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} !!!")
    print(
        "Run config: "
        f"speed={args.speed or 'custom'} | batch_size={args.batch_size} | "
        f"concurrency={args.concurrency} | sync_notion={bool(args.sync_notion)} | "
        f"sync_every_waves={args.sync_notion_every_waves} | batch_retries={args.batch_retries} | "
        f"recovery_delay={args.recovery_delay}s | timeout_total={args.timeout_total}s | "
        f"timeout_sock_read={args.timeout_sock_read}s | timeout_connect={args.timeout_connect}s | "
        f"remaining_records={len(todo)} | already_done={len(done_ids)} | scoring_mode={args.scoring_mode} | model={args.model}",
        flush=True,
    )
    header = make_autopilot_header_lines() if args.scoring_mode == "autopilot_direct_100" else make_header_lines()
    print(header, flush=True)

    if not results_csv.exists():
        with open(results_csv, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=score_fieldnames(args.scoring_mode)).writeheader()

    ordered_rows = list(todo.iterrows())
    meta_by_index = {seq: row for seq, (_idx, row) in enumerate(ordered_rows)}
    records = []
    for seq, (_idx, row) in enumerate(ordered_rows):
        rec = compact_record(row)
        rec["_seq"] = seq
        records.append(rec)

    session = open_api_session(args)
    await run_scoring_session(
        records, meta_by_index,
        args=args,
        session=session,
        results_csv=results_csv,
        progress_jsonl=progress_jsonl,
        failed_jsonl=failed_jsonl,
        counter_ref=[len(done_ids)],
        scoring_mode=args.scoring_mode,
        composite_config=composite_config,
        system_prompt=system_prompt,
    )


if __name__ == "__main__":
    asyncio.run(main())
