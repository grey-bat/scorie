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
    make_header_lines,
    make_row_line,
    normalize_text,
    parse_json_from_content,
    to_int_score,
)

SCRIPT_DIR = Path(__file__).resolve().parent
RUBRIC_TEXT = (SCRIPT_DIR / "scoring_rubric.md").read_text(encoding="utf-8")
SYSTEM_PROMPT = (
    "You are a scoring engine for B2B lead qualification. "
    "Read every provided field carefully. Count synonyms, translations, equivalent phrases, and near-synonyms. "
    "Use the full record, including current role, historical roles, organization names, organization titles, organization descriptions, headline, summary, alumni signal, mutual count, and degree. "
    "Return ONLY valid JSON. No markdown. No prose. "
    "Return a single JSON object with a top-level key named results. "
    "results must be an array of objects. "
    "Each object must contain exactly these keys: id, fo_persona, ft_persona, allocator, access. "
    "id must exactly equal the input id string. "
    "All four scores must be integers from 0 to 5. "
    "Do not calculate derived formulas.\n\n" + RUBRIC_TEXT
)


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


def compact_record(row: pd.Series) -> dict:
    return {
        "id": normalize_text(row["Match Key"]),
        "raw_id": normalize_text(row.get("Raw ID", "")),
        "best_email": normalize_text(row.get("Best Email", "")),
        "full_name": normalize_text(row.get("Full Name", "")),
        "current_company": normalize_text(row.get("Current Company", "")),
        "current_title": normalize_text(row.get("Current Title", "")),
        "headline": normalize_text(row.get("Headline", "")),
        "industry": normalize_text(row.get("Industry", "")),
        "mutual_count": int(row.get("Mutual Count", 0) or 0),
        "degree": int(row.get("Degree", 3) or 3),
        "summary": normalize_text(row.get("Summary", "")),
        "alumni_signal": normalize_text(row.get("Alumni Signal", "")),
        "position_1_description": normalize_text(row.get("Position 1 Description", "")),
        "position_2_description": normalize_text(row.get("Position 2 Description", "")),
        "position_3_description": normalize_text(row.get("Position 3 Description", "")),
        "organization_1": normalize_text(row.get("Organization 1", "")),
        "organization_2": normalize_text(row.get("Organization 2", "")),
        "organization_3": normalize_text(row.get("Organization 3", "")),
        "organization_1_title": normalize_text(row.get("Organization 1 Title", "")),
        "organization_2_title": normalize_text(row.get("Organization 2 Title", "")),
        "organization_3_title": normalize_text(row.get("Organization 3 Title", "")),
        "organization_1_description": normalize_text(row.get("Organization 1 Description", "")),
        "organization_2_description": normalize_text(row.get("Organization 2 Description", "")),
        "organization_3_description": normalize_text(row.get("Organization 3 Description", "")),
    }


def build_user_prompt(records: list[dict]) -> str:
    return json.dumps({"records": records}, ensure_ascii=False)


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


def remap_batch_results(records: list[dict], out: list[dict]) -> list[dict]:
    in_ids = [r["id"] for r in records]
    out_ids = [r["Match Key"] for r in out]
    in_map = build_canonical_id_map(in_ids)
    out_map: dict[str, dict] = {}
    remap_pairs = []
    for item in out:
        canonical_id = canonicalize_identifier(item["Match Key"])
        if canonical_id not in in_map:
            raise RuntimeError(f"Unknown output id: {item['Match Key']!r}")
        original_id = in_map[canonical_id]
        if item["Match Key"] != original_id:
            remap_pairs.append((item["Match Key"], original_id))
        normalized_item = dict(item)
        normalized_item["Match Key"] = original_id
        out_map[original_id] = normalized_item
    if set(in_map) != {canonicalize_identifier(rid) for rid in out_ids}:
        raise RuntimeError(f"ID mismatch: sent {in_ids[:5]} got {out_ids[:5]}")
    if len(out_map) != len(records):
        raise RuntimeError(f"Batch size mismatch: sent {len(records)} got {len(out)}")
    if remap_pairs:
        preview = ", ".join(f"{src} -> {dst}" for src, dst in remap_pairs[:5])
        if len(remap_pairs) > 5:
            preview += f", ... (+{len(remap_pairs) - 5} more)"
        print(f"Canonical ID remap: {preview}", flush=True)
    return [out_map[rid] for rid in in_ids]


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


async def call_openrouter(session: aiohttp.ClientSession, model: str, records: list[dict], retries: int = 5):
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
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
                for item in results:
                    out.append({
                        "Match Key": normalize_text(item["id"]),
                        "fo_persona": to_int_score(item["fo_persona"]),
                        "ft_persona": to_int_score(item["ft_persona"]),
                        "allocator": to_int_score(item["allocator"]),
                        "access": to_int_score(item["access"]),
                    })
                return remap_batch_results(records, out)
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            last_error = repr(e)
            print(f"Retrying OpenRouter batch after network timeout/error ({attempt}/{retries}): {last_error}", flush=True)
            await asyncio.sleep(wait + random.random())
            wait = min(wait * 1.8, 45)
            continue
    raise TransientBatchError(last_error or "OpenRouter request failed after retries")


def deterministic_mock(records: list[dict]) -> list[dict]:
    out = []
    for rec in records:
        text = " ".join(str(rec.get(k, "")) for k in rec).lower()
        fo = 0
        ft = 0
        alloc = 1
        access = 0
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
        out.append({"Match Key": rec["id"], "fo_persona": min(5, fo), "ft_persona": min(5, ft), "allocator": min(5, alloc), "access": min(5, access)})
    return out


async def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "minimax/minimax-m2.7"))
    ap.add_argument("--speed", choices=sorted(SPEED_PRESETS.keys()))
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "8")))
    ap.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "12")))
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--start-row", type=int, default=1)
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--max-failures-per-record", type=int, default=3)
    ap.add_argument("--batch-retries", type=int, default=int(os.getenv("OPENROUTER_BATCH_RETRIES", "2")))
    ap.add_argument("--recovery-delay", type=int, default=int(os.getenv("OPENROUTER_RECOVERY_DELAY", "300")))
    ap.add_argument("--timeout-total", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_TOTAL", "180")))
    ap.add_argument("--timeout-connect", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_CONNECT", "15")))
    ap.add_argument("--timeout-sock-connect", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_SOCK_CONNECT", "15")))
    ap.add_argument("--timeout-sock-read", type=int, default=int(os.getenv("OPENROUTER_TIMEOUT_SOCK_READ", "120")))
    ap.add_argument("--sync-notion", action="store_true")
    ap.add_argument("--sync-notion-every-waves", type=int, default=1)
    args = ap.parse_args()

    if args.speed:
        preset = SPEED_PRESETS[args.speed]
        args.batch_size = preset["batch_size"]
        args.concurrency = preset["concurrency"]

    ensure_dir(args.out)
    df = pd.read_csv(args.input, dtype={"Match Key": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    df = df.iloc[max(0, args.start_row - 1):].copy()
    if args.max_records:
        df = df.iloc[: args.max_records]

    results_csv = Path(args.out) / "scores_raw.csv"
    progress_jsonl = Path(args.out) / "scores_progress.jsonl"
    failed_jsonl = Path(args.out) / "failed_batches.jsonl"
    done_ids = set()
    if results_csv.exists():
        prev = pd.read_csv(results_csv, dtype={"Match Key": str})
        if "Match Key" in prev.columns:
            done_ids = set(prev["Match Key"].map(normalize_text).tolist())

    todo = df[~df["Match Key"].map(normalize_text).isin(done_ids)].copy()
    if todo.empty:
        print("Nothing to do.")
        return

    print(f"!!! STARTING NEW SESSION AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} !!!")
    print(
        "Run config: "
        f"speed={args.speed or 'custom'} | "
        f"batch_size={args.batch_size} | "
        f"concurrency={args.concurrency} | "
        f"sync_notion={bool(args.sync_notion)} | "
        f"sync_every_waves={args.sync_notion_every_waves} | "
        f"batch_retries={args.batch_retries} | "
        f"recovery_delay={args.recovery_delay}s | "
        f"timeout_total={args.timeout_total}s | "
        f"timeout_sock_read={args.timeout_sock_read}s | "
        f"timeout_connect={args.timeout_connect}s | "
        f"remaining_records={len(todo)} | "
        f"already_done={len(done_ids)}",
        flush=True,
    )
    print(make_header_lines(), flush=True)

    if not results_csv.exists():
        with open(results_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Match Key", "Raw ID", "Best Email", "fo_persona", "ft_persona", "allocator", "access"])
            writer.writeheader()

    ordered_rows = list(todo.iterrows())
    meta_by_index = {seq: row for seq, (_idx, row) in enumerate(ordered_rows)}
    records = []
    for seq, (_idx, row) in enumerate(ordered_rows):
        rec = compact_record(row)
        rec["_seq"] = seq
        records.append(rec)
    counter = len(done_ids)
    io_lock = asyncio.Lock()
    failure_counts = {}
    wave_count = 0
    current_batch_size = max(1, args.batch_size)
    current_concurrency = max(1, args.concurrency)
    initial_batch_size = current_batch_size
    initial_concurrency = current_concurrency
    last_adjustment_at = None
    batch_retry_count = max(0, args.batch_retries)
    api_key = os.getenv("OPENROUTER_API_KEY")
    session = None
    if not args.mock:
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is required unless --mock is used.")
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        timeout = aiohttp.ClientTimeout(
            total=args.timeout_total,
            connect=args.timeout_connect,
            sock_connect=args.timeout_sock_connect,
            sock_read=args.timeout_sock_read,
        )
        session = aiohttp.ClientSession(headers=headers, timeout=timeout)

    async def flush_batch(batch_out):
        nonlocal counter
        async with io_lock:
            with open(results_csv, "a", newline="", encoding="utf-8") as fcsv, open(progress_jsonl, "a", encoding="utf-8") as fj:
                writer = csv.DictWriter(fcsv, fieldnames=["Match Key", "Raw ID", "Best Email", "fo_persona", "ft_persona", "allocator", "access"])
                for item in batch_out:
                    meta = meta_by_index[item["_seq"]]
                    row = {
                        "Match Key": item["Match Key"],
                        "Raw ID": normalize_text(meta.get("Raw ID", "")),
                        "Best Email": normalize_text(meta.get("Best Email", "")),
                        "fo_persona": item["fo_persona"],
                        "ft_persona": item["ft_persona"],
                        "allocator": item["allocator"],
                        "access": item["access"],
                    }
                    writer.writerow(row)
                    fj.write(json.dumps(row, ensure_ascii=False) + "\n")
                    counter += 1
                    print(make_row_line({
                        "done": counter,
                        "Full Name": meta.get("Full Name", ""),
                        "Current Company": meta.get("Current Company", ""),
                        "fo_persona": item["fo_persona"],
                        "ft_persona": item["ft_persona"],
                        "allocator": item["allocator"],
                        "Degree": meta.get("Degree", ""),
                        "access": item["access"],
                        "Headline": meta.get("Headline", ""),
                        "Summary": meta.get("Summary", ""),
                    }), flush=True)

    def spawn_sync_incremental_notion():
        if not args.sync_notion or args.mock:
            return None
        sync_workdir = Path(args.out).parent
        cmd = [
            sys.executable,
            str(SCRIPT_DIR / "sync_incremental_delta.py"),
            "--workdir",
            str(sync_workdir),
        ]
        print(f"Partial Notion sync: workdir={sync_workdir}", flush=True)
        return asyncio.create_task(asyncio.to_thread(subprocess.run, cmd, check=True))

    async def process_batch(batch):
        for attempt in range(batch_retry_count + 1):
            try:
                if args.mock:
                    batch_out = deterministic_mock(batch)
                else:
                    batch_out = await call_openrouter(session, args.model, batch)
                for item, rec in zip(batch_out, batch):
                    item["_seq"] = rec["_seq"]
                await flush_batch(batch_out)
                return
            except Exception as e:
                if attempt < batch_retry_count:
                    print(
                        "Retrying batch before backoff: "
                        f"attempt={attempt + 1}/{batch_retry_count} | "
                        f"first_match_key={batch[0]['id']} | "
                        f"error={repr(e)}",
                        flush=True,
                    )
                    await asyncio.sleep(min(2 ** attempt, 8) + random.random())
                    continue
                raise

    pending = records[:]
    sync_task = None
    try:
        while pending:
            active = []
            for _ in range(current_concurrency):
                if not pending:
                    break
                batch = pending[:current_batch_size]
                pending = pending[current_batch_size:]
                active.append(batch)
            results = await asyncio.gather(*(process_batch(batch) for batch in active), return_exceptions=True)
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
                                f"first_match_key={batch[0]['id']} | "
                                f"error={repr(err)}",
                                flush=True,
                            )
                            ff.write(json.dumps({
                                "event": "batch_failure",
                                "match_keys": [r["id"] for r in batch],
                                "error": repr(err),
                                "new_concurrency": current_concurrency,
                                "new_batch_size": current_batch_size,
                            }, ensure_ascii=False) + "\n")
                for batch, err in reversed(failures):
                    if old_c == 1 and old_b == 1 and len(batch) == 1:
                        mk = batch[0]["id"]
                        failure_counts[mk] = failure_counts.get(mk, 0) + 1
                        if failure_counts[mk] >= args.max_failures_per_record:
                            continue
                    pending = batch + pending
            else:
                new_c, new_b, recovered = maybe_recover_capacity(
                    current_concurrency,
                    current_batch_size,
                    initial_concurrency,
                    initial_batch_size,
                    last_adjustment_at,
                    args.recovery_delay,
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
                    sync_task = spawn_sync_incremental_notion()
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


if __name__ == "__main__":
    asyncio.run(main())
