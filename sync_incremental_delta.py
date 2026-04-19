import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from build_delta import build_scoring_frames, load_scoring_frames
from update_notion import NotionClient, build_match_caches, page_matches_payload, query_all_pages, resolve_data_source_id
from utils import RAW_SCORE_COLUMNS, ensure_dir, notion_set_payload
from writeback_status import build_writeback_status, now_iso, write_json_atomic


def count_data_rows(csv_path: Path) -> int:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return max(0, sum(1 for _ in reader) - 1)


def snapshot_csv_prefix(src: Path, dst: Path) -> int:
    """
    Copy a stable CSV prefix so incremental sync can read a file that is still
    being appended to without choking on a partially written trailing row.
    """
    data = src.read_bytes()
    if data and not data.endswith(b"\n"):
        last_newline = data.rfind(b"\n")
        if last_newline >= 0:
            data = data[: last_newline + 1]
        else:
            data = b""
    dst.write_bytes(data)
    return count_data_rows(dst)


def copy_new_rows(src: Path, dst: Path, start_after: int) -> int:
    with open(src, newline="", encoding="utf-8") as fin, open(dst, "w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            return 0
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        written = 0
        for idx, row in enumerate(reader, start=1):
            if idx <= start_after:
                continue
            writer.writerow(row)
            written += 1
    return written


def run(cmd):
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def target_types_for_writeback(schema: dict) -> dict[str, str]:
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
    return target_types


def build_payload_for_row(row: pd.Series, target_types: dict[str, str]) -> dict:
    payload = {"properties": {}}
    for name in list(RAW_SCORE_COLUMNS.values()) + ["Degree", "Alumni Signal"]:
        payload["properties"][name] = notion_set_payload(target_types[name], row[name])
    return payload


def filter_rows_needing_live_update(
    delta: pd.DataFrame,
    client: NotionClient,
    data_source_id: str,
    schema: dict,
    *,
    status_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_types = target_types_for_writeback(schema)
    started_at = now_iso()
    started_monotonic = time.monotonic()
    processed_rows = 0
    already_complete = 0
    needs_update = 0
    unmatched_rows = 0
    ambiguous_rows = 0
    loaded_source_pages = 0
    loaded_source_rows = 0

    def publish(
        phase: str,
        current_row_index: int | None = None,
        current_match_key: str | None = None,
        current_page_id: str | None = None,
    ):
        if status_path is None:
            return
        snapshot = build_writeback_status(
            phase=phase,
            total_rows=len(delta),
            processed_rows=processed_rows,
            updated_rows=needs_update,
            noop_rows=already_complete,
            unmatched_rows=unmatched_rows,
            ambiguous_rows=ambiguous_rows,
            retries=0,
            elapsed_seconds=time.monotonic() - started_monotonic,
            started_at=started_at,
            mode="reconcile_missing",
            current_row_index=current_row_index,
            current_match_key=current_match_key,
            current_page_id=current_page_id,
            loaded_source_pages=loaded_source_pages,
            loaded_source_rows=loaded_source_rows,
            queued_write_rows=needs_update,
            last_error=None,
        )
        write_json_atomic(status_path, snapshot)

    def on_page_loaded(pages_loaded: int, rows_loaded: int):
        nonlocal loaded_source_pages, loaded_source_rows
        loaded_source_pages = pages_loaded
        loaded_source_rows = rows_loaded
        publish("loading_candidates")

    all_pages = query_all_pages(client, data_source_id, on_page=on_page_loaded)
    raw_cache, email_cache = build_match_caches(all_pages, True, True)
    rows = []
    report = []

    publish("matching")
    for _, row in delta.iterrows():
        processed_rows += 1
        rid = row.get("Raw ID", "")
        email = row.get("Best Email", "")
        page = None
        status = "not_found"
        if rid:
            matches = raw_cache.get(rid, [])
            if len(matches) == 1:
                page = matches[0]
            elif len(matches) > 1:
                status = "ambiguous_raw_id"
                ambiguous_rows += 1
                report.append({
                    "Match Key": row.get("Match Key", ""),
                    "Raw ID": rid,
                    "Best Email": email,
                    "status": status,
                    "page_id": ";".join(page.get("id", "") for page in matches if page.get("id")),
                })
                continue
        if page is None and email:
            matches = email_cache.get(email, [])
            if len(matches) == 1:
                page = matches[0]
                status = "matched_best_email"
            elif len(matches) > 1:
                status = "ambiguous_best_email"
                ambiguous_rows += 1
        if page is None:
            unmatched_rows += 1
            report.append({
                "Match Key": row.get("Match Key", ""),
                "Raw ID": rid,
                "Best Email": email,
                "status": status,
                "page_id": "",
            })
            continue
        payload = build_payload_for_row(row, target_types)
        if page_matches_payload(page, payload, target_types):
            already_complete += 1
            report.append({
                "Match Key": row.get("Match Key", ""),
                "Raw ID": rid,
                "Best Email": email,
                "status": "already_complete",
                "page_id": page["id"],
            })
            continue
        needs_update += 1
        report.append({
            "Match Key": row.get("Match Key", ""),
            "Raw ID": rid,
            "Best Email": email,
            "status": "needs_update",
            "page_id": page["id"],
        })
        rows.append(row)
        publish("matching", current_match_key=row.get("Match Key", ""), current_page_id=page["id"])
    publish("done")
    return pd.DataFrame(rows), pd.DataFrame(report)


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="out")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--notion-limit", type=int, default=None)
    ap.add_argument("--catch-up", action="store_true")
    ap.add_argument("--reconcile-missing", action="store_true")
    args = ap.parse_args()

    wd = Path(args.workdir)
    prep = wd / "01_prepare"
    score = wd / "02_score"
    delta_dir = wd / "03_delta_sync"
    notion_out = wd / "04_notion_sync"
    state_path = wd / ".incremental_sync_state.json"
    reconcile_dir = delta_dir
    delta_dir.mkdir(parents=True, exist_ok=True)
    notion_out.mkdir(parents=True, exist_ok=True)

    scores_path = score / "scores_raw.csv"
    if not scores_path.exists():
        raise SystemExit(f"Missing scores file: {scores_path}")

    snapshot_path = delta_dir / "scores_raw.snapshot.csv"
    current_rows = snapshot_csv_prefix(scores_path, snapshot_path)
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        state = {"last_synced_rows": 0}
    last_synced_rows = int(state.get("last_synced_rows", 0) or 0)
    full, prepared, scores = load_scoring_frames(str(prep / "full_deduped_for_scoring.csv"), str(prep / "prepared_scoring_input.csv"), str(scores_path))

    if args.catch_up:
        print(f"Running full catch-up sync for {len(scores)} scored rows.")
        _output, _merged, delta, _summary, _changed_mask = build_scoring_frames(full, prepared, scores, include_all=True)
    elif args.reconcile_missing:
        print(f"Running live reconciliation scan for {len(scores)} scored rows.")
        if not os.getenv("NOTION_API_KEY"):
            raise SystemExit("NOTION_API_KEY is required.")
        client = NotionClient(os.getenv("NOTION_API_KEY", ""))
        data_source_id = resolve_data_source_id(client, os.getenv("NOTION_DATA_SOURCE_ID"), os.getenv("NOTION_DATABASE_ID"))
        schema = client.get(f"/data_sources/{data_source_id}")
        _output, _merged, delta, _summary, _changed_mask = build_scoring_frames(full, prepared, scores, include_all=True)
        reconcile_status_path = notion_out / "notion_writeback_status.json"
        delta, reconcile_report = filter_rows_needing_live_update(
            delta,
            client,
            data_source_id,
            schema,
            status_path=reconcile_status_path,
        )
        reconcile_report_path = reconcile_dir / "notion_reconcile_report.csv"
        reconcile_report.to_csv(reconcile_report_path, index=False)
        print(f"Wrote {reconcile_report_path}")
        print(f"Rows needing update in live Notion: {len(delta)}")
        if delta.empty:
            return
    else:
        if current_rows <= last_synced_rows:
            print(f"No new scored rows since last sync: {current_rows} rows total, {last_synced_rows} already synced.")
            return

        new_scores = delta_dir / "scores_new_rows.csv"
        extracted = copy_new_rows(snapshot_path, new_scores, last_synced_rows)
        if extracted == 0:
            print("No new complete rows available to sync yet.")
            return

        print(f"Syncing {extracted} new scored rows from {last_synced_rows + 1} to {last_synced_rows + extracted}.")
        full, prepared, scores = load_scoring_frames(str(prep / "full_deduped_for_scoring.csv"), str(prep / "prepared_scoring_input.csv"), str(new_scores))
        _output, _merged, delta, _summary, _changed_mask = build_scoring_frames(full, prepared, scores, include_all=False)

    delta_path = delta_dir / "delta_updates.csv"
    delta.to_csv(delta_path, index=False)
    print(f"Wrote {delta_path}")

    notion_cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "update_notion.py"),
        "--delta", str(delta_path),
        "--out", str(notion_out),
    ]
    if args.notion_limit:
        notion_cmd += ["--limit", str(args.notion_limit)]
    if args.dry_run:
        notion_cmd += ["--dry-run"]
    run(notion_cmd)

    if not args.dry_run and not args.reconcile_missing:
        state["last_synced_rows"] = current_rows
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(f"Updated sync state: {state_path} -> last_synced_rows={current_rows}")


if __name__ == "__main__":
    main()
