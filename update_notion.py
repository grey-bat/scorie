import argparse
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import threading
from typing import Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from utils import RAW_SCORE_COLUMNS, ensure_dir, notion_plain_text, notion_set_payload, normalize_key, normalize_email, normalize_text
from writeback_status import build_writeback_status, now_iso, write_json_atomic

NOTION_VERSION = os.getenv("NOTION_VERSION", "2026-03-11")


class NotionClient:
    def __init__(
        self,
        api_key: str,
        min_interval: float = 0.45,
        connect_timeout: float = 15.0,
        read_timeout: float = 45.0,
        retries: int = 5,
        rate_limiter=None,
    ):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        })
        self.min_interval = min_interval
        self.last_call = 0.0
        self.timeout = (connect_timeout, read_timeout)
        self.retries = retries
        self.rate_limiter = rate_limiter

    def _pace(self):
        if self.rate_limiter is not None:
            self.rate_limiter.wait()
            return
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()

    def get(self, path: str):
        return self._request("get", path)

    def post(self, path: str, payload: dict):
        return self._request("post", path, payload)

    def patch(self, path: str, payload: dict):
        return self._request("patch", path, payload)

    def _request(self, method: str, path: str, payload: Optional[dict] = None):
        url = f"https://api.notion.com/v1{path}"
        wait = 2.0
        last_error = None
        for attempt in range(1, self.retries + 1):
            self._pace()
            try:
                response = self.s.request(method, url, json=payload, timeout=self.timeout)
                return self._handle(response)
            except RuntimeError as e:
                if not retryable_notion_error(e):
                    raise
                last_error = e
                if attempt == self.retries:
                    break
                print(f"Retrying Notion {method.upper()} {path} after {e} ({attempt}/{self.retries})", flush=True)
                time.sleep(wait)
                wait = min(wait * 1.8, 20.0)
            except requests.Timeout as e:
                last_error = e
                if attempt == self.retries:
                    break
                print(f"Retrying Notion {method.upper()} {path} after timeout ({attempt}/{self.retries})", flush=True)
                time.sleep(wait)
                wait = min(wait * 1.8, 20.0)
            except requests.RequestException as e:
                last_error = e
                if attempt == self.retries:
                    break
                print(f"Retrying Notion {method.upper()} {path} after request error ({attempt}/{self.retries}): {e}", flush=True)
                time.sleep(wait)
                wait = min(wait * 1.8, 20.0)
        raise RuntimeError(f"request_failed_{method}_{path}: {last_error}")

    def _handle(self, r):
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "2"))
            time.sleep(retry)
            raise RuntimeError("rate_limited")
        if r.status_code in (500, 502, 503, 504, 409):
            raise RuntimeError(f"retryable_{r.status_code}")
        if r.status_code >= 400:
            raise RuntimeError(f"Notion error {r.status_code}: {r.text[:1000]}")
        return r.json()


class SharedRateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.next_allowed_at = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            target = max(now, self.next_allowed_at)
            self.next_allowed_at = target + self.min_interval
        sleep_for = target - now
        if sleep_for > 0:
            time.sleep(sleep_for)


def resolve_data_source_id(client: NotionClient, explicit_data_source_id: Optional[str], database_id: Optional[str]) -> str:
    if explicit_data_source_id:
        return explicit_data_source_id
    if not database_id:
        raise SystemExit("Provide NOTION_DATA_SOURCE_ID or NOTION_DATABASE_ID.")
    db = client.get(f"/databases/{database_id}")
    data_sources = db.get("data_sources") or []
    if len(data_sources) != 1:
        raise SystemExit(f"Database returned {len(data_sources)} data sources; set NOTION_DATA_SOURCE_ID explicitly.")
    return data_sources[0]["id"]


def query_all_pages(client: NotionClient, data_source_id: str, on_page=None):
    pages = []
    cursor = None
    loaded_pages = 0
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        while True:
            try:
                data = client.post(f"/data_sources/{data_source_id}/query", payload)
                break
            except RuntimeError as e:
                if str(e) in ("rate_limited", "retryable_500", "retryable_502", "retryable_503", "retryable_504", "retryable_409"):
                    time.sleep(2)
                    continue
                raise
        pages.extend(data.get("results", []))
        loaded_pages += 1
        if on_page is not None:
            on_page(loaded_pages, len(pages))
        cursor = data.get("next_cursor")
        if not data.get("has_more"):
            break
    return pages


def query_pages_by_filter(client: NotionClient, data_source_id: str, filter_obj: dict, page_size: int = 100):
    results = []
    cursor = None
    while True:
        payload = {"page_size": page_size, "filter": filter_obj}
        if cursor:
            payload["start_cursor"] = cursor
        while True:
            try:
                data = client.post(f"/data_sources/{data_source_id}/query", payload)
                break
            except RuntimeError as e:
                if str(e) in ("rate_limited", "retryable_500", "retryable_502", "retryable_503", "retryable_504", "retryable_409"):
                    time.sleep(2)
                    continue
                raise
        results.extend(data.get("results", []))
        cursor = data.get("next_cursor")
        if not data.get("has_more"):
            break
    return results


def batch_values(values, size: int = 25):
    values = list(values)
    for i in range(0, len(values), size):
        yield values[i:i + size]


def lookup_pages_by_property_values(client: NotionClient, data_source_id: str, property_name: str, property_type: str, values, batch_size: int = 25):
    cache = defaultdict(list)
    for group in batch_values(values, batch_size):
        if not group:
            continue
        if len(group) == 1:
            filter_obj = {
                "property": property_name,
                property_type: {"equals": group[0]},
            }
        else:
            filter_obj = {
                "or": [
                    {
                        "property": property_name,
                        property_type: {"equals": value},
                    }
                    for value in group
                ]
            }
        for page in query_pages_by_filter(client, data_source_id, filter_obj):
            props = page.get("properties", {})
            plain = notion_plain_text(props.get(property_name, {}))
            key = normalize_email(plain) if property_type == "email" else normalize_key(plain)
            if key:
                cache[key].append(page)
    return cache


def _payload_rich_text_content(payload: dict) -> str:
    return "".join([part.get("text", {}).get("content", "") for part in payload.get("rich_text", [])])


def page_matches_payload(page: dict, payload: dict, target_types: Dict[str, str]) -> bool:
    properties = page.get("properties", {})
    desired_properties = payload.get("properties", {})
    for name, desired in desired_properties.items():
        prop_type = target_types[name]
        current = properties.get(name, {})
        if prop_type == "number":
            if current.get("number") != desired.get("number"):
                return False
            continue
        if prop_type == "rich_text":
            if normalize_text(notion_plain_text(current)) != normalize_text(_payload_rich_text_content(desired)):
                return False
            continue
        if prop_type == "select":
            current_select = current.get("select") or {}
            desired_select = desired.get("select") or {}
            if normalize_text(current_select.get("name")) != normalize_text(desired_select.get("name")):
                return False
            continue
        if prop_type == "status":
            current_status = current.get("status") or {}
            desired_status = desired.get("status") or {}
            if normalize_text(current_status.get("name")) != normalize_text(desired_status.get("name")):
                return False
            continue
        raise ValueError(f"Unsupported target property type for comparison: {prop_type}")
    return True


def retryable_notion_error(error: RuntimeError) -> bool:
    return str(error).startswith("retryable_") or str(error) == "rate_limited"


def apply_write_job(client: NotionClient, job: dict) -> dict:
    page_id = job["page_id"]
    payload = job["payload"]
    retries_used = 0
    local_backoff = 0.5
    while True:
        try:
            client.patch(f"/pages/{page_id}", payload)
            return {"status": "updated", "retries_used": retries_used}
        except RuntimeError as e:
            if retryable_notion_error(e):
                retries_used += 1
                time.sleep(local_backoff)
                local_backoff = min(local_backoff * 1.8, 20.0)
                continue
            return {"status": "failed", "error": str(e), "retries_used": retries_used}


def duplicate_lookup_rows(cache: Dict[str, list], lookup_type: str) -> list[dict]:
    rows = []
    for key in sorted(cache):
        pages = cache[key]
        if len(pages) <= 1:
            continue
        rows.append({
            "lookup_type": lookup_type,
            "lookup_key": key,
            "match_count": len(pages),
            "page_ids": ";".join(page.get("id", "") for page in pages if page.get("id")),
        })
    return rows


def build_match_caches(pages: list[dict], use_raw_id: bool, use_best_email: bool):
    raw_cache = defaultdict(list)
    email_cache = defaultdict(list)
    for page in pages:
        props = page.get("properties", {})
        if use_raw_id:
            raw_key = normalize_key(notion_plain_text(props.get("Raw ID", {})))
            if raw_key:
                raw_cache[raw_key].append(page)
        if use_best_email:
            email_key = normalize_email(notion_plain_text(props.get("Best Email", {})))
            if email_key:
                email_cache[email_key].append(page)
    return raw_cache, email_cache


def load_delta(args) -> pd.DataFrame:
    delta = pd.read_csv(args.delta, dtype={"Match Key": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    delta["Raw ID"] = delta["Raw ID"].map(normalize_key)
    delta["Best Email"] = delta["Best Email"].map(normalize_email)
    if args.limit:
        delta = delta.iloc[: args.limit]
    return delta


def resolve_schema(client: NotionClient, data_source_id: str) -> tuple[Dict[str, str], Optional[str], Optional[str], list[str]]:
    schema = client.get(f"/data_sources/{data_source_id}")
    props = schema.get("properties", {})
    required_targets = [
        RAW_SCORE_COLUMNS["fo_persona"],
        RAW_SCORE_COLUMNS["ft_persona"],
        RAW_SCORE_COLUMNS["allocator"],
        RAW_SCORE_COLUMNS["access"],
        "Degree",
        "Alumni Signal",
    ]
    target_types: Dict[str, str] = {}
    for name in required_targets:
        if name not in props:
            raise SystemExit(f"Target property missing in Notion data source: {name}")
        ptype = props[name]["type"]
        if ptype == "formula":
            raise SystemExit(f"Target property is formula and cannot be updated via API: {name}")
        target_types[name] = ptype
    optional_targets: list[str] = []
    company_fit_prop = RAW_SCORE_COLUMNS.get("company_fit")
    if company_fit_prop and company_fit_prop in props and props[company_fit_prop]["type"] != "formula":
        optional_targets.append(company_fit_prop)
        target_types[company_fit_prop] = props[company_fit_prop]["type"]
    if "Raw ID" not in props and "Best Email" not in props:
        raise SystemExit("Notion data source must contain Raw ID and/or Best Email for writeback matching.")
    raw_prop_type = props["Raw ID"]["type"] if "Raw ID" in props else None
    email_prop_type = props["Best Email"]["type"] if "Best Email" in props else None
    return target_types, raw_prop_type, email_prop_type, optional_targets


def build_write_jobs(
    delta: pd.DataFrame,
    all_pages: list,
    raw_prop_type: Optional[str],
    email_prop_type: Optional[str],
    required_targets: list,
    optional_targets: list,
    target_types: Dict[str, str],
    out: str,
    dry_run: bool,
    publish,
) -> tuple[list, list, dict, list, list]:
    raw_cache, email_cache = build_match_caches(all_pages, raw_prop_type is not None, email_prop_type is not None)
    raw_duplicate_rows = duplicate_lookup_rows(raw_cache, "Raw ID")
    email_duplicate_rows = duplicate_lookup_rows(email_cache, "Best Email")
    duplicate_rows = raw_duplicate_rows + email_duplicate_rows
    logs: list = []
    counters: dict = {"updated": 0, "noop": 0, "unmatched": 0, "ambiguous": 0, "processed_rows": 0,
                      "duplicate_report_path": None, "duplicate_lookup_preview": None}

    if duplicate_rows:
        counters["duplicate_report_path"] = str(Path(out) / "notion_writeback_duplicates.csv")
        pd.DataFrame(duplicate_rows).to_csv(counters["duplicate_report_path"], index=False)
        counters["duplicate_lookup_preview"] = duplicate_rows[:5]
        print(
            f"Warning: Duplicate lookup keys detected: {len(raw_duplicate_rows)} raw id keys, "
            f"{len(email_duplicate_rows)} best email keys. See {counters['duplicate_report_path']}",
            flush=True,
        )
        logs.extend({
            "Match Key": row["lookup_key"],
            "status": f"duplicate_{row['lookup_type'].lower().replace(' ', '_')}",
            "page_id": row["page_ids"],
        } for row in duplicate_rows)

    publish("matching")
    write_jobs: list = []
    for row_number, (_, row) in enumerate(delta.iterrows(), start=1):
        mk = row.get("Match Key", "")
        rid, email = row["Raw ID"], row["Best Email"]
        page = None
        if rid:
            matches = raw_cache.get(rid, [])
            if len(matches) > 1:
                logs.append({"Match Key": mk, "status": "ambiguous_raw_id", "page_id": ""})
                counters["ambiguous"] += 1
                counters["processed_rows"] += 1
                publish("matching")
                continue
            if matches:
                page = matches[0]
        if page is None and email:
            matches = email_cache.get(email, [])
            if len(matches) > 1:
                logs.append({"Match Key": mk, "status": "ambiguous_best_email", "page_id": ""})
                counters["ambiguous"] += 1
                counters["processed_rows"] += 1
                publish("matching")
                continue
            if matches:
                page = matches[0]
        if page is None:
            logs.append({"Match Key": mk, "status": "not_found", "page_id": ""})
            counters["unmatched"] += 1
            counters["processed_rows"] += 1
            publish("matching")
            continue
        page_id = page["id"]
        payload_cols = required_targets + optional_targets
        payload = {"properties": {col: notion_set_payload(target_types[col], row[col]) for col in payload_cols if col in row.index}}
        if page_matches_payload(page, payload, target_types):
            counters["noop"] += 1
            logs.append({"Match Key": mk, "status": "noop", "page_id": page_id})
            counters["processed_rows"] += 1
            publish("matching")
            continue
        if dry_run:
            logs.append({"Match Key": mk, "status": "dry_run", "page_id": page_id})
            counters["processed_rows"] += 1
            publish("matching")
            continue
        write_jobs.append({"row_number": row_number, "match_key": mk, "page_id": page_id, "payload": payload})
        publish("writing")

    return write_jobs, logs, counters, raw_duplicate_rows, email_duplicate_rows


def execute_write_jobs(
    write_jobs: list,
    client: NotionClient,
    api_key: str,
    args,
    logs: list,
    counters: dict,
    publish,
    terminal_phase_ref: list,
) -> None:
    write_workers = args.write_workers
    if write_workers is None:
        write_workers = int(os.getenv("NOTION_WRITE_WORKERS", "0") or 0)
    if write_workers <= 0:
        write_workers = 4 if len(write_jobs) >= 200 else 1
    write_workers = max(1, write_workers)

    def record_result(job: dict, result: dict) -> None:
        counters["retries"] = counters.get("retries", 0) + result["retries_used"]
        if result["status"] == "updated":
            counters["updated"] += 1
            counters["processed_rows"] += 1
            logs.append({"Match Key": job["match_key"], "status": "updated", "page_id": job["page_id"]})
            counters["last_success_match_key"] = job["match_key"]
            counters["last_success_page_id"] = job["page_id"]
            counters["last_error"] = None
            publish("writing")
            return
        counters["last_error"] = result["error"]
        terminal_phase_ref[0] = "failed"
        publish("failed", finished_at=now_iso())
        raise RuntimeError(result["error"])

    if write_workers <= 1:
        for job in write_jobs:
            record_result(job, apply_write_job(client, job))
        return

    shared_limiter = SharedRateLimiter(min_interval=0.35)
    worker_local = threading.local()

    def get_worker_client() -> NotionClient:
        wc = getattr(worker_local, "client", None)
        if wc is None:
            wc = NotionClient(api_key, min_interval=0.0, connect_timeout=15.0, read_timeout=45.0,
                              retries=5, rate_limiter=shared_limiter)
            worker_local.client = wc
        return wc

    with ThreadPoolExecutor(max_workers=write_workers) as executor:
        for batch in batch_values(write_jobs, args.apply_batch_size):
            future_map = {executor.submit(apply_write_job, get_worker_client(), job): job for job in batch}
            for future in as_completed(future_map):
                record_result(future_map[future], future.result())


def write_artifacts(out: str, logs: list, write_jobs: list, counters: dict,
                    raw_cache: dict, email_cache: dict,
                    raw_duplicate_rows: list, email_duplicate_rows: list) -> None:
    try:
        pd.DataFrame(logs).to_csv(f"{out}/notion_writeback_log.csv", index=False)
        pd.DataFrame([
            {"metric": "delta_rows", "value": counters.get("delta_rows", 0)},
            {"metric": "write_rows", "value": len(write_jobs)},
            {"metric": "updated", "value": counters["updated"]},
            {"metric": "noop", "value": counters["noop"]},
            {"metric": "unmatched", "value": counters["unmatched"]},
            {"metric": "ambiguous", "value": counters["ambiguous"]},
            {"metric": "duplicate_raw_ids", "value": len(raw_duplicate_rows)},
            {"metric": "duplicate_best_emails", "value": len(email_duplicate_rows)},
            {"metric": "raw_id_lookups", "value": len(raw_cache)},
            {"metric": "best_email_lookups", "value": len(email_cache)},
        ]).to_csv(f"{out}/notion_writeback_summary.csv", index=False)
    except Exception as artifact_error:
        print(f"Warning: failed to write Notion writeback artifacts: {artifact_error}", flush=True)


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--delta", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--data-source-id", default=os.getenv("NOTION_DATA_SOURCE_ID"))
    ap.add_argument("--database-id", default=os.getenv("NOTION_DATABASE_ID"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply-batch-size", type=int, default=100)
    ap.add_argument("--write-workers", type=int, default=None)
    args = ap.parse_args()

    ensure_dir(args.out)
    api_key = os.getenv("NOTION_API_KEY")
    if not api_key:
        raise SystemExit("NOTION_API_KEY is required.")

    client = NotionClient(api_key)
    data_source_id = resolve_data_source_id(client, args.data_source_id, args.database_id)
    target_types, raw_prop_type, email_prop_type, optional_targets = resolve_schema(client, data_source_id)
    required_targets = [
        RAW_SCORE_COLUMNS["fo_persona"],
        RAW_SCORE_COLUMNS["ft_persona"],
        RAW_SCORE_COLUMNS["allocator"],
        RAW_SCORE_COLUMNS["access"],
        "Degree",
        "Alumni Signal",
    ]

    delta = load_delta(args)
    total_rows = len(delta)
    print(f"Notion writeback rows needing update: {total_rows}", flush=True)

    status_path = Path(args.out) / "notion_writeback_status.json"
    started_at = now_iso()
    started_monotonic = time.monotonic()
    counters: dict = {"updated": 0, "noop": 0, "unmatched": 0, "ambiguous": 0,
                      "processed_rows": 0, "retries": 0, "delta_rows": total_rows,
                      "last_error": None, "last_success_match_key": None, "last_success_page_id": None,
                      "loaded_source_pages": 0, "loaded_source_rows": 0,
                      "queued_write_rows": 0, "duplicate_report_path": None, "duplicate_lookup_preview": None}
    terminal_phase_ref = ["done"]

    def publish(phase: str, finished_at: Optional[str] = None) -> None:
        write_json_atomic(status_path, build_writeback_status(
            phase=phase, total_rows=total_rows,
            processed_rows=counters["processed_rows"],
            updated_rows=counters["updated"], noop_rows=counters["noop"],
            unmatched_rows=counters["unmatched"], ambiguous_rows=counters["ambiguous"],
            retries=counters.get("retries", 0),
            elapsed_seconds=time.monotonic() - started_monotonic,
            started_at=started_at, mode="dry_run" if args.dry_run else "write",
            current_row_index=None, current_match_key=counters.get("last_success_match_key"),
            current_page_id=counters.get("last_success_page_id"),
            loaded_source_pages=counters["loaded_source_pages"],
            loaded_source_rows=counters["loaded_source_rows"],
            queued_write_rows=counters["queued_write_rows"],
            duplicate_report_path=counters["duplicate_report_path"],
            duplicate_lookup_preview=counters["duplicate_lookup_preview"],
            last_error=counters.get("last_error"),
            last_success_match_key=counters.get("last_success_match_key"),
            last_success_page_id=counters.get("last_success_page_id"),
            finished_at=finished_at,
        ))

    publish("loading_candidates")
    write_jobs: list = []
    raw_duplicate_rows: list = []
    email_duplicate_rows: list = []
    raw_cache: dict = {}
    email_cache: dict = {}
    logs: list = []

    try:
        def on_page_loaded(pages_loaded: int, rows_loaded: int) -> None:
            counters["loaded_source_pages"] = pages_loaded
            counters["loaded_source_rows"] = rows_loaded
            publish("loading_candidates")

        all_pages = query_all_pages(client, data_source_id, on_page=on_page_loaded)
        raw_cache, email_cache = build_match_caches(all_pages, raw_prop_type is not None, email_prop_type is not None)
        write_jobs, logs, job_counters, raw_duplicate_rows, email_duplicate_rows = build_write_jobs(
            delta, all_pages, raw_prop_type, email_prop_type,
            required_targets, optional_targets, target_types, args.out, args.dry_run, publish,
        )
        counters.update(job_counters)
        if write_jobs:
            execute_write_jobs(write_jobs, client, api_key, args, logs, counters, publish, terminal_phase_ref)
        publish("done", finished_at=now_iso())
    except Exception:
        terminal_phase_ref[0] = "failed"
        raise
    finally:
        write_artifacts(args.out, logs, write_jobs, counters, raw_cache, email_cache,
                        raw_duplicate_rows, email_duplicate_rows)
        if terminal_phase_ref[0] == "done":
            print(f"Wrote {args.out}/notion_writeback_log.csv")
            print(f"Updated: {counters['updated']}, noop: {counters['noop']}, "
                  f"unmatched: {counters['unmatched']}, ambiguous: {counters['ambiguous']}")
        elif terminal_phase_ref[0] == "failed" and counters.get("last_error"):
            print(f"Notion writeback stopped: {counters['last_error']}")


if __name__ == "__main__":
    main()
