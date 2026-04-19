import argparse
from pathlib import Path

import pandas as pd
from utils import (
    PREPARED_OUTPUT_COLUMNS,
    MODEL_CONTEXT_COLUMNS,
    STAGE_RANK,
    RICHNESS_FIELDS,
    ensure_dir,
    map_distance_label,
    normalize_key,
    normalize_email,
    normalize_mutual_count,
    recompute_alumni_signal,
    canonical_match_key,
)


def load_distance_map(distance_csv: str) -> pd.DataFrame:
    dist = pd.read_csv(distance_csv, usecols=["id", "member_distance"], dtype={"id": str}, low_memory=False)
    dist["Raw ID"] = dist["id"].map(normalize_key)
    dist = dist[dist["Raw ID"] != ""].copy()
    dist = dist[["Raw ID", "member_distance"]].drop_duplicates(subset=["Raw ID"], keep="first")
    dist["Degree"] = dist["member_distance"].map(map_distance_label)
    return dist[["Raw ID", "Degree"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--distance-csv", default="data/everything.csv")
    args = ap.parse_args()

    full_path = Path(args.full)
    distance_path = Path(args.distance_csv)
    if not full_path.exists():
        raise SystemExit(f"Missing input CSV: {full_path}")
    if not distance_path.exists():
        raise SystemExit(f"Missing distance CSV: {distance_path}")

    ensure_dir(args.out)
    full = pd.read_csv(full_path, dtype={"Raw ID": str, "Best Email": str}, low_memory=False)

    required = {
        "Raw ID", "Best Email", "Full Name", "Current Company", "Current Title", "Headline", "Industry", "Mutual Count", "Summary",
        "Berkeley Signal", "Columbia Signal", "Stage",
        "Position 1 Description", "Position 2 Description", "Position 3 Description",
        "Organization 1", "Organization 2", "Organization 3",
        "Organization 1 Title", "Organization 2 Title", "Organization 3 Title",
        "Organization 1 Description", "Organization 2 Description", "Organization 3 Description",
    }
    missing = [c for c in required if c not in full.columns]
    if missing:
        raise SystemExit(f"Missing required columns in full.csv: {missing}")

    full["Raw ID"] = full["Raw ID"].map(normalize_key)
    full["Best Email"] = full["Best Email"].map(normalize_email)
    full["Match Key"] = [canonical_match_key(r, e) for r, e in zip(full["Raw ID"], full["Best Email"])]
    full["_source_order"] = range(len(full))

    rows_without_any_key = int((full["Match Key"] == "").sum())
    blank_raw_id_rows = int((full["Raw ID"] == "").sum())
    fallback_email_rows = int(((full["Raw ID"] == "") & (full["Best Email"] != "")).sum())

    keyed = full[full["Match Key"] != ""].copy()
    duplicate_input_rows = int(keyed["Match Key"].duplicated().sum())

    # Vectorized dedupe for speed: prefer Stage present, then later Stage, then more recent timestamp, then richer row.
    keyed["_stage_norm"] = keyed["Stage"].fillna("").astype(str).str.strip().str.lower().str.replace("-", " ", regex=False)
    keyed["_stage_norm"] = keyed["_stage_norm"].replace({"ondeck": "on deck", "follow up": "followup", "replied": "responded"})
    keyed["_has_stage"] = (keyed["_stage_norm"] != "").astype(int)
    keyed["_stage_rank"] = keyed["_stage_norm"].map(STAGE_RANK).fillna(0).astype(int)
    ts_cols = [c for c in ["Last Touch Date", "Last Sent At", "Last Received At", "Connected At", "Created"] if c in keyed.columns]
    for c in ts_cols:
        keyed[c + "__ts"] = pd.to_datetime(keyed[c], errors="coerce", utc=True)
    if ts_cols:
        ts_frame = keyed[[c + "__ts" for c in ts_cols]]
        keyed["_best_ts"] = ts_frame.max(axis=1)
    else:
        keyed["_best_ts"] = pd.Timestamp("1970-01-01", tz="UTC")
    _richness_cols = [c for c in RICHNESS_FIELDS if c in keyed.columns]
    keyed["_richness"] = keyed[_richness_cols].notna().sum(axis=1)
    keyed = keyed.sort_values(["Match Key", "_has_stage", "_stage_rank", "_best_ts", "_richness"], ascending=[True, False, False, False, False], kind="stable")
    deduped = keyed.drop_duplicates(subset=["Match Key"], keep="first").copy()
    deduped = deduped.sort_values(["_source_order"], kind="stable").copy()

    prepared = pd.DataFrame({
        "Match Key": deduped["Match Key"],
        "Raw ID": deduped["Raw ID"],
        "Best Email": deduped["Best Email"],
        "Full Name": deduped["Full Name"],
        "Current Company": deduped["Current Company"],
        "Current Title": deduped["Current Title"],
        "Headline": deduped["Headline"],
        "Industry": deduped["Industry"],
        "Mutual Count": deduped["Mutual Count"].map(normalize_mutual_count),
        "Summary": deduped["Summary"],
        "Alumni Signal": [
            recompute_alumni_signal(b, c)
            for b, c in zip(deduped["Berkeley Signal"], deduped["Columbia Signal"])
        ],
        "Position 1 Description": deduped["Position 1 Description"],
        "Position 2 Description": deduped["Position 2 Description"],
        "Position 3 Description": deduped["Position 3 Description"],
        "Organization 1": deduped["Organization 1"],
        "Organization 2": deduped["Organization 2"],
        "Organization 3": deduped["Organization 3"],
        "Organization 1 Title": deduped["Organization 1 Title"],
        "Organization 2 Title": deduped["Organization 2 Title"],
        "Organization 3 Title": deduped["Organization 3 Title"],
        "Organization 1 Description": deduped["Organization 1 Description"],
        "Organization 2 Description": deduped["Organization 2 Description"],
        "Organization 3 Description": deduped["Organization 3 Description"],
    })

    dist = load_distance_map(str(distance_path))
    prepared = prepared.merge(dist, on="Raw ID", how="left")
    prepared["Degree"] = prepared["Degree"].fillna(3).astype(int)

    prepared = prepared[PREPARED_OUTPUT_COLUMNS]
    trimmed_generated = prepared[["Raw ID", *MODEL_CONTEXT_COLUMNS]].copy()

    prepared.to_csv(f"{args.out}/prepared_scoring_input.csv", index=False)
    trimmed_generated.to_csv(f"{args.out}/trimmed_generated.csv", index=False)
    deduped.to_csv(f"{args.out}/full_deduped_for_scoring.csv", index=False)

    pd.DataFrame([
        {"metric": "source_rows", "value": len(full)},
        {"metric": "rows_without_any_key_dropped", "value": rows_without_any_key},
        {"metric": "blank_raw_id_rows", "value": blank_raw_id_rows},
        {"metric": "fallback_best_email_rows", "value": fallback_email_rows},
        {"metric": "duplicate_match_key_rows_deduped", "value": duplicate_input_rows},
        {"metric": "deduped_rows", "value": len(deduped)},
        {"metric": "degree_matches", "value": int(prepared["Degree"].isin([1, 2]).sum())},
        {"metric": "degree_defaulted_to_3", "value": int((prepared["Degree"] == 3).sum())},
        {"metric": "alumni_cal_cbs", "value": int((prepared["Alumni Signal"] == "Cal+CBS").sum())},
        {"metric": "alumni_cal", "value": int((prepared["Alumni Signal"] == "Cal").sum())},
        {"metric": "alumni_cbs", "value": int((prepared["Alumni Signal"] == "CBS").sum())},
        {"metric": "alumni_blank", "value": int((prepared["Alumni Signal"] == "").sum())},
    ]).to_csv(f"{args.out}/prepare_report.csv", index=False)

    print(f"Wrote {args.out}/trimmed_generated.csv")
    print(f"Wrote {args.out}/prepared_scoring_input.csv")
    print(f"Wrote {args.out}/full_deduped_for_scoring.csv")
    print(f"Wrote {args.out}/prepare_report.csv")


if __name__ == "__main__":
    main()
