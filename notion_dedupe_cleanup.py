import argparse
import csv
import os
import time
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

from update_notion import NotionClient, resolve_data_source_id, query_all_pages, lookup_pages_by_property_values
from utils import notion_plain_text, choose_best_duplicate, normalize_key, normalize_email


def page_to_record(page):
    props = page.get("properties", {})
    return {
        "page_id": page["id"],
        "Raw ID": normalize_key(notion_plain_text(props.get("Raw ID", {}))),
        "Best Email": normalize_email(notion_plain_text(props.get("Best Email", {}))),
        "Full Name": notion_plain_text(props.get("Full Name", {})) or notion_plain_text(props.get("Name", {})),
        "Current Company": notion_plain_text(props.get("Current Company", {})),
        "Current Title": notion_plain_text(props.get("Current Title", {})),
        "Headline": notion_plain_text(props.get("Headline", {})),
        "Summary": notion_plain_text(props.get("Summary", {})),
        "Industry": notion_plain_text(props.get("Industry", {})),
        "Stage": notion_plain_text(props.get("Stage", {})),
        "Created": notion_plain_text(props.get("Created", {})) or page.get("created_time", ""),
        "Last Touch Date": notion_plain_text(props.get("Last Touch Date", {})),
        "Last Sent At": notion_plain_text(props.get("Last Sent At", {})),
        "Last Received At": notion_plain_text(props.get("Last Received At", {})),
        "Connected At": notion_plain_text(props.get("Connected At", {})),
        "Position 1 Description": notion_plain_text(props.get("Position 1 Description", {})),
        "Position 2 Description": notion_plain_text(props.get("Position 2 Description", {})),
        "Position 3 Description": notion_plain_text(props.get("Position 3 Description", {})),
        "Organization 1": notion_plain_text(props.get("Organization 1", {})),
        "Organization 2": notion_plain_text(props.get("Organization 2", {})),
        "Organization 3": notion_plain_text(props.get("Organization 3", {})),
        "Organization 1 Title": notion_plain_text(props.get("Organization 1 Title", {})),
        "Organization 2 Title": notion_plain_text(props.get("Organization 2 Title", {})),
        "Organization 3 Title": notion_plain_text(props.get("Organization 3 Title", {})),
        "Organization 1 Description": notion_plain_text(props.get("Organization 1 Description", {})),
        "Organization 2 Description": notion_plain_text(props.get("Organization 2 Description", {})),
        "Organization 3 Description": notion_plain_text(props.get("Organization 3 Description", {})),
        "Mutual Count": notion_plain_text(props.get("Mutual Count", {})),
        "Berkeley Signal": notion_plain_text(props.get("Berkeley Signal", {})),
        "Columbia Signal": notion_plain_text(props.get("Columbia Signal", {})),
    }


def load_duplicate_raw_ids_from_full_csv(full_csv_path):
    full = pd.read_csv(full_csv_path, dtype={"Raw ID": str}, low_memory=False, usecols=["Raw ID"])
    full["Raw ID"] = full["Raw ID"].map(normalize_key)
    dupes = full[full["Raw ID"] != ""].copy()
    counts = dupes["Raw ID"].value_counts()
    return sorted(counts[counts > 1].index.tolist())


def collect_live_duplicate_groups(client, data_source_id):
    duplicate_groups = {}
    pages = query_all_pages(client, data_source_id)
    grouped = {}
    for page in pages:
        record = page_to_record(page)
        raw_id = record["Raw ID"]
        if not raw_id:
            continue
        grouped.setdefault(raw_id, []).append(record)
    for raw_id, group in grouped.items():
        if len(group) > 1:
            duplicate_groups[raw_id] = group
    return duplicate_groups, len(pages)


def batch_values(values, size: int):
    values = list(values)
    for i in range(0, len(values), size):
        yield values[i:i + size]


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--full")
    ap.add_argument("--out", required=True)
    ap.add_argument("--data-source-id", default=os.getenv("NOTION_DATA_SOURCE_ID"))
    ap.add_argument("--database-id", default=os.getenv("NOTION_DATABASE_ID"))
    ap.add_argument("--source", choices=["live", "export"], default="live")
    ap.add_argument("--flag-property", default=os.getenv("NOTION_DUPE_FLAG_PROPERTY", "is_dupe"))
    ap.add_argument("--lookup-batch-size", type=int, default=100)
    ap.add_argument("--apply-batch-size", type=int, default=100)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    api_key = os.getenv("NOTION_API_KEY")
    if not api_key:
        raise SystemExit("NOTION_API_KEY is required.")
    client = NotionClient(api_key)
    data_source_id = resolve_data_source_id(client, args.data_source_id, args.database_id)
    schema = client.get(f"/data_sources/{data_source_id}")
    props = schema.get("properties", {})
    if args.flag_property not in props:
        raise SystemExit(f"Flag property missing in Notion data source: {args.flag_property}")
    if props[args.flag_property].get("type") != "checkbox":
        raise SystemExit(f"Flag property must be a checkbox: {args.flag_property}")
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["Raw ID", "page_id", "status", "Best Email", "Stage", "Current Company", "Headline"]
    total_losers = 0
    lookup_batch_size = max(1, args.lookup_batch_size)
    apply_batch_size = max(1, args.apply_batch_size)
    duplicate_groups = {}
    duplicate_source = ""
    source_page_count = None
    raw_ids = []

    if args.source == "live":
        print("Scanning live Notion pages for duplicate Raw IDs", flush=True)
        duplicate_groups, source_page_count = collect_live_duplicate_groups(client, data_source_id)
        raw_ids = sorted(duplicate_groups.keys())
        duplicate_source = "live Notion"
        print(f"Loaded {source_page_count} live Notion pages", flush=True)
    else:
        if not args.full:
            raise SystemExit("--full is required when --source export is used.")
        raw_ids = load_duplicate_raw_ids_from_full_csv(args.full)
        duplicate_source = "full.csv"
        print(f"Duplicate Raw IDs found in full.csv: {len(raw_ids)}", flush=True)

    if args.source == "live" and not raw_ids:
        print("No duplicate Raw ID values found in live Notion", flush=True)
    elif args.source == "export" and not raw_ids:
        print("No duplicate Raw ID values found in full.csv", flush=True)
    else:
        print(f"Duplicate Raw IDs found in {duplicate_source}: {len(raw_ids)}", flush=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if args.source == "export" and raw_ids:
            duplicate_groups = {}
            for batch_index, raw_batch in enumerate(batch_values(raw_ids, lookup_batch_size), start=1):
                batch_start = (batch_index - 1) * lookup_batch_size + 1
                batch_end = batch_start + len(raw_batch) - 1
                print(f"Processing raw-id batch {batch_start}-{batch_end} of {len(raw_ids)}", flush=True)
                batch_cache = dict(lookup_pages_by_property_values(client, data_source_id, "Raw ID", props["Raw ID"]["type"], raw_batch, batch_size=25))
                for raw_id in raw_batch:
                    group = [page_to_record(page) for page in batch_cache.get(raw_id, [])]
                    if len(group) > 1:
                        duplicate_groups[raw_id] = group

        ordered_raw_ids = sorted(duplicate_groups.keys())
        if not ordered_raw_ids:
            print("No duplicate groups to write.", flush=True)
        for batch_index, raw_batch in enumerate(batch_values(ordered_raw_ids, lookup_batch_size), start=1):
            batch_start = (batch_index - 1) * lookup_batch_size + 1
            batch_end = batch_start + len(raw_batch) - 1
            print(f"Processing raw-id batch {batch_start}-{batch_end} of {len(ordered_raw_ids)}", flush=True)
            batch_rows = []
            batch_losers = []
            for raw_id in raw_batch:
                group = duplicate_groups.get(raw_id, [])
                if len(group) < 2:
                    continue
                winner = choose_best_duplicate(group)
                for r in group:
                    status = "keep" if r["page_id"] == winner["page_id"] else "flag"
                    row = {
                        "Raw ID": raw_id,
                        "page_id": r["page_id"],
                        "status": status,
                        "Best Email": r.get("Best Email", ""),
                        "Stage": r.get("Stage", ""),
                        "Current Company": r.get("Current Company", ""),
                        "Headline": r.get("Headline", ""),
                    }
                    batch_rows.append(row)
                    if status == "flag":
                        batch_losers.append(r["page_id"])
            for row in batch_rows:
                writer.writerow(row)
            total_losers += len(batch_losers)
            print(f"Batch {batch_index}: wrote {len(batch_rows)} rows, flagged {len(batch_losers)} losers", flush=True)
            if args.apply and batch_losers:
                for start in range(0, len(batch_losers), apply_batch_size):
                    apply_batch = batch_losers[start:start + apply_batch_size]
                    print(f"Flagging rows {start + 1}-{start + len(apply_batch)} of {len(batch_losers)} in this batch", flush=True)
                    for pid in apply_batch:
                        while True:
                            try:
                                client.patch(f"/pages/{pid}", {"properties": {args.flag_property: {"checkbox": True}}})
                                break
                            except RuntimeError as e:
                                if str(e).startswith("retryable_") or str(e) == "rate_limited":
                                    time.sleep(2)
                                    continue
                                raise
    print(f"Wrote {args.out}")
    print(f"Duplicate pages to flag: {total_losers}")


if __name__ == "__main__":
    main()
