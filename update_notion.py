import argparse
import os
import time
from collections import defaultdict
from typing import Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from utils import RAW_SCORE_COLUMNS, ensure_dir, notion_plain_text, notion_set_payload, normalize_key, normalize_email

NOTION_VERSION = os.getenv("NOTION_VERSION", "2026-03-11")


class NotionClient:
    def __init__(
        self,
        api_key: str,
        min_interval: float = 0.45,
        connect_timeout: float = 15.0,
        read_timeout: float = 45.0,
        retries: int = 5,
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

    def _pace(self):
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


def query_all_pages(client: NotionClient, data_source_id: str):
    pages = []
    cursor = None
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
    args = ap.parse_args()

    ensure_dir(args.out)
    api_key = os.getenv("NOTION_API_KEY")
    if not api_key:
        raise SystemExit("NOTION_API_KEY is required.")
    client = NotionClient(api_key)
    data_source_id = resolve_data_source_id(client, args.data_source_id, args.database_id)
    schema = client.get(f"/data_sources/{data_source_id}")
    props = schema.get("properties", {})
    required_targets = list(RAW_SCORE_COLUMNS.values()) + ["Degree", "Alumni Signal"]
    target_types = {}
    for name in required_targets:
        if name not in props:
            raise SystemExit(f"Target property missing in Notion data source: {name}")
        ptype = props[name]["type"]
        if ptype == "formula":
            raise SystemExit(f"Target property is formula and cannot be updated via API: {name}")
        target_types[name] = ptype

    if "Raw ID" not in props and "Best Email" not in props:
        raise SystemExit("Notion data source must contain Raw ID and/or Best Email for writeback matching.")
    raw_prop_type = props["Raw ID"]["type"] if "Raw ID" in props else None
    email_prop_type = props["Best Email"]["type"] if "Best Email" in props else None

    delta = pd.read_csv(args.delta, dtype={"Match Key": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    delta["Raw ID"] = delta["Raw ID"].map(normalize_key)
    delta["Best Email"] = delta["Best Email"].map(normalize_email)
    if args.limit:
        delta = delta.iloc[: args.limit]

    logs = []
    updated = 0
    unmatched = 0
    ambiguous = 0
    raw_cache: Dict[str, list] = {}
    email_cache: Dict[str, list] = {}
    raw_ids = sorted({rid for rid in delta["Raw ID"].tolist() if rid})
    emails = sorted({email for email in delta["Best Email"].tolist() if email})
    if raw_prop_type and raw_ids:
        raw_cache = dict(lookup_pages_by_property_values(client, data_source_id, "Raw ID", raw_prop_type, raw_ids))
    if email_prop_type and emails:
        email_cache = dict(lookup_pages_by_property_values(client, data_source_id, "Best Email", email_prop_type, emails))
    for _, row in delta.iterrows():
        rid = row["Raw ID"]
        email = row["Best Email"]
        page = None
        if rid:
            matches = raw_cache.get(rid, [])
            if len(matches) > 1:
                logs.append({"Match Key": row.get("Match Key", ""), "status": "ambiguous_raw_id", "page_id": ""})
                ambiguous += 1
                continue
            if matches:
                page = matches[0]
        if page is None and email:
            matches = email_cache.get(email, [])
            if len(matches) > 1:
                logs.append({"Match Key": row.get("Match Key", ""), "status": "ambiguous_best_email", "page_id": ""})
                ambiguous += 1
                continue
            if matches:
                page = matches[0]
        if page is None:
            logs.append({"Match Key": row.get("Match Key", ""), "status": "not_found", "page_id": ""})
            unmatched += 1
            continue
        page_id = page["id"]
        payload = {"properties": {}}
        for col in required_targets:
            payload["properties"][col] = notion_set_payload(target_types[col], row[col])
        if args.dry_run:
            logs.append({"Match Key": row.get("Match Key", ""), "status": "dry_run", "page_id": page_id})
            continue
        while True:
            try:
                client.patch(f"/pages/{page_id}", payload)
                updated += 1
                logs.append({"Match Key": row.get("Match Key", ""), "status": "updated", "page_id": page_id})
                break
            except RuntimeError as e:
                if str(e).startswith("retryable_") or str(e) == "rate_limited":
                    time.sleep(2)
                    continue
                raise

    pd.DataFrame(logs).to_csv(f"{args.out}/notion_writeback_log.csv", index=False)
    pd.DataFrame([
        {"metric": "delta_rows", "value": len(delta)},
        {"metric": "updated", "value": updated},
        {"metric": "unmatched", "value": unmatched},
        {"metric": "ambiguous", "value": ambiguous},
        {"metric": "raw_id_lookups", "value": len(raw_cache)},
        {"metric": "best_email_lookups", "value": len(email_cache)},
    ]).to_csv(f"{args.out}/notion_writeback_summary.csv", index=False)
    print(f"Wrote {args.out}/notion_writeback_log.csv")
    print(f"Updated: {updated}, unmatched: {unmatched}, ambiguous: {ambiguous}")


if __name__ == "__main__":
    main()
