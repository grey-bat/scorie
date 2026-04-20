import argparse
import os
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from update_notion import (
    NotionClient,
    apply_write_job,
    duplicate_lookup_rows,
    lookup_pages_by_property_values,
    notion_plain_text,
    page_matches_payload,
    resolve_data_source_id,
)
from utils import ensure_dir, normalize_email, normalize_key, notion_set_payload


FT_V2_PROP = "Fintech Score v2"
ROLE_FIT_V2_PROP = "Role Fit v2"
COMPANY_FIT_V2_PROP = "Company Fit v2"
URN_PROP = "LinkedIn Member URN"


def ensure_v2_properties(client: NotionClient, data_source_id: str, props: dict) -> dict:
    missing = [name for name in (FT_V2_PROP, ROLE_FIT_V2_PROP, COMPANY_FIT_V2_PROP) if name not in props]
    if not missing:
        print("V2 properties already exist.", flush=True)
        return props
    print(f"Creating missing Notion properties: {', '.join(missing)}", flush=True)
    payload = {
        "properties": {
            name: {"number": {"format": "number"}}
            for name in missing
        }
    }
    client.patch(f"/data_sources/{data_source_id}", payload)
    refreshed = client.get(f"/data_sources/{data_source_id}")
    return refreshed.get("properties", {})


def build_lookup_cache(client: NotionClient, data_source_id: str, property_name: str, property_type: str, values, normalizer):
    values = list(values)
    print(f"Looking up {len(values)} values via {property_name} ({property_type})", flush=True)
    raw = lookup_pages_by_property_values(client, data_source_id, property_name, property_type, values)
    cache = defaultdict(list)
    for key, pages in raw.items():
        cache[normalizer(key)].extend(pages)
    print(f"Resolved {len(cache)} distinct lookup keys via {property_name}", flush=True)
    return cache


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="out/65_and_higher_v006/scored_65_and_higher_v006.csv")
    ap.add_argument("--out", default="out/65_and_higher_v006/04_notion_v2")
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_dotenv(Path(".env"))
    api_key = os.getenv("NOTION_API_KEY")
    if not api_key:
        raise SystemExit("NOTION_API_KEY is required.")

    ensure_dir(args.out)
    df = pd.read_csv(args.input, dtype={"URN": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    if args.offset or args.limit is not None:
        end = None if args.limit is None else args.offset + args.limit
        df = df.iloc[args.offset:end].copy()
    df["URN"] = df["URN"].map(normalize_key)
    df["Raw ID"] = df["Raw ID"].map(normalize_key)
    df["Best Email"] = df["Best Email"].map(normalize_email)
    print(f"Loaded {len(df)} scored rows from {args.input}", flush=True)

    client = NotionClient(api_key)
    data_source_id = resolve_data_source_id(client, os.getenv("NOTION_DATA_SOURCE_ID"), os.getenv("NOTION_DATABASE_ID"))
    print(f"Using data source {data_source_id}", flush=True)
    schema = client.get(f"/data_sources/{data_source_id}")
    props = ensure_v2_properties(client, data_source_id, schema.get("properties", {}))

    for name in (URN_PROP, "Raw ID", "Best Email", FT_V2_PROP, ROLE_FIT_V2_PROP, COMPANY_FIT_V2_PROP):
        if name not in props:
            raise SystemExit(f"Required Notion property missing: {name}")

    target_types = {
        FT_V2_PROP: props[FT_V2_PROP]["type"],
        ROLE_FIT_V2_PROP: props[ROLE_FIT_V2_PROP]["type"],
        COMPANY_FIT_V2_PROP: props[COMPANY_FIT_V2_PROP]["type"],
    }
    urn_prop_type = props[URN_PROP]["type"]
    raw_prop_type = props["Raw ID"]["type"]
    email_prop_type = props["Best Email"]["type"]

    urn_values = [v for v in df["URN"].tolist() if normalize_key(v)]
    urn_cache = build_lookup_cache(client, data_source_id, URN_PROP, urn_prop_type, urn_values, normalize_key)

    unresolved_after_urn = []
    for _, row in df.iterrows():
        urn = row.get("URN", "")
        if not urn or not urn_cache.get(urn):
            unresolved_after_urn.append(row)
    print(f"Rows unresolved after URN lookup: {len(unresolved_after_urn)}", flush=True)

    raw_cache = defaultdict(list)
    email_cache = defaultdict(list)
    if unresolved_after_urn:
        raw_values = [normalize_key(row.get("Raw ID", "")) for row in unresolved_after_urn if normalize_key(row.get("Raw ID", ""))]
        email_values = [normalize_email(row.get("Best Email", "")) for row in unresolved_after_urn if normalize_email(row.get("Best Email", ""))]
        if raw_values:
            raw_cache = build_lookup_cache(client, data_source_id, "Raw ID", raw_prop_type, raw_values, normalize_key)
        if email_values:
            email_cache = build_lookup_cache(client, data_source_id, "Best Email", email_prop_type, email_values, normalize_email)
    print("Finished live lookup phase.", flush=True)

    duplicate_rows = (
        duplicate_lookup_rows(urn_cache, URN_PROP)
        + duplicate_lookup_rows(raw_cache, "Raw ID")
        + duplicate_lookup_rows(email_cache, "Best Email")
    )
    if duplicate_rows:
        pd.DataFrame(duplicate_rows).to_csv(Path(args.out) / "notion_v2_duplicates.csv", index=False)

    logs = []
    counters = {"updated": 0, "noop": 0, "unmatched": 0, "ambiguous": 0}
    jobs = []

    for _, row in df.iterrows():
        urn = row.get("URN", "")
        rid = row.get("Raw ID", "")
        email = row.get("Best Email", "")
        page = None
        match_source = ""

        if urn:
            matches = urn_cache.get(urn, [])
            if len(matches) > 1:
                counters["ambiguous"] += 1
                logs.append({"URN": urn, "Raw ID": rid, "Best Email": email, "status": "ambiguous_urn"})
                continue
            if matches:
                page = matches[0]
                match_source = "urn"

        if page is None and rid:
            matches = raw_cache.get(rid, [])
            if len(matches) > 1:
                counters["ambiguous"] += 1
                logs.append({"URN": urn, "Raw ID": rid, "Best Email": email, "status": "ambiguous_raw_id"})
                continue
            if matches:
                page = matches[0]
                match_source = "raw_id"

        if page is None and email:
            matches = email_cache.get(email, [])
            if len(matches) > 1:
                counters["ambiguous"] += 1
                logs.append({"URN": urn, "Raw ID": rid, "Best Email": email, "status": "ambiguous_best_email"})
                continue
            if matches:
                page = matches[0]
                match_source = "best_email"

        if page is None:
            counters["unmatched"] += 1
            logs.append({"URN": urn, "Raw ID": rid, "Best Email": email, "status": "not_found"})
            continue

        payload = {
            "properties": {
                FT_V2_PROP: notion_set_payload(target_types[FT_V2_PROP], row["ft_total"]),
                ROLE_FIT_V2_PROP: notion_set_payload(target_types[ROLE_FIT_V2_PROP], row["role_fit"]),
                COMPANY_FIT_V2_PROP: notion_set_payload(target_types[COMPANY_FIT_V2_PROP], row["company_fit"]),
            }
        }
        if page_matches_payload(page, payload, target_types):
            counters["noop"] += 1
            logs.append({"URN": urn, "Raw ID": rid, "Best Email": email, "status": "noop", "match_source": match_source, "page_id": page["id"]})
            continue
        if args.dry_run:
            logs.append({"URN": urn, "Raw ID": rid, "Best Email": email, "status": "dry_run", "match_source": match_source, "page_id": page["id"]})
            continue
        jobs.append({"page_id": page["id"], "payload": payload, "urn": urn, "raw_id": rid, "best_email": email, "match_source": match_source})

    for job in jobs:
        result = apply_write_job(client, {"page_id": job["page_id"], "payload": job["payload"]})
        if result["status"] != "updated":
            raise SystemExit(f"Failed updating page {job['page_id']}: {result}")
        counters["updated"] += 1
        logs.append({
            "URN": job["urn"],
            "Raw ID": job["raw_id"],
            "Best Email": job["best_email"],
            "status": "updated",
            "match_source": job["match_source"],
            "page_id": job["page_id"],
        })
        time.sleep(0.05)

    pd.DataFrame(logs).to_csv(Path(args.out) / "notion_v2_write_log.csv", index=False)
    pd.DataFrame(
        [
            {"metric": "input_rows", "value": len(df)},
            {"metric": "write_rows", "value": len(jobs)},
            {"metric": "updated", "value": counters["updated"]},
            {"metric": "noop", "value": counters["noop"]},
            {"metric": "unmatched", "value": counters["unmatched"]},
            {"metric": "ambiguous", "value": counters["ambiguous"]},
            {"metric": "urn_lookup_keys", "value": len(urn_cache)},
            {"metric": "raw_id_lookup_keys", "value": len(raw_cache)},
            {"metric": "best_email_lookup_keys", "value": len(email_cache)},
        ]
    ).to_csv(Path(args.out) / "notion_v2_write_summary.csv", index=False)

    print(f"Created/verified properties: {FT_V2_PROP}, {ROLE_FIT_V2_PROP}, {COMPANY_FIT_V2_PROP}")
    print(f"Updated: {counters['updated']}, noop: {counters['noop']}, unmatched: {counters['unmatched']}, ambiguous: {counters['ambiguous']}")


if __name__ == "__main__":
    main()
