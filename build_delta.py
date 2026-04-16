import argparse
import pandas as pd
from utils import RAW_SCORE_COLUMNS, ensure_dir, normalize_key, normalize_email, canonical_match_key


def same_score(a, b) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    try:
        return int(float(a)) == int(float(b))
    except Exception:
        return str(a).strip() == str(b).strip()


def load_scoring_frames(full_path: str, prepared_path: str, scores_path: str):
    full = pd.read_csv(full_path, dtype={"Raw ID": str, "Best Email": str}, low_memory=False)
    prepared = pd.read_csv(prepared_path, dtype={"Match Key": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    scores = pd.read_csv(scores_path, dtype={"Match Key": str, "Raw ID": str, "Best Email": str}, low_memory=False)
    if scores.empty:
        raise SystemExit("No scored rows found to build delta.")

    full["Raw ID"] = full["Raw ID"].map(normalize_key)
    full["Best Email"] = full["Best Email"].map(normalize_email)
    full["Match Key"] = [canonical_match_key(r, e) for r, e in zip(full["Raw ID"], full["Best Email"])]
    full = full[full["Match Key"] != ""].copy().drop_duplicates(subset=["Match Key"], keep="first")

    prepared["Match Key"] = prepared["Match Key"].map(normalize_key)
    prepared = prepared[prepared["Match Key"] != ""].copy().drop_duplicates(subset=["Match Key"], keep="last")

    scores["Match Key"] = scores["Match Key"].map(normalize_key)
    scores = scores[scores["Match Key"] != ""].copy().drop_duplicates(subset=["Match Key"], keep="last")
    return full, prepared, scores


def build_scoring_frames(full, prepared, scores, include_all: bool = False):
    merged = full.merge(prepared, on="Match Key", how="inner", suffixes=("", "_prepared"))
    merged = merged.merge(scores, on="Match Key", how="inner", suffixes=("", "_score"))
    if merged.empty:
        raise SystemExit("No matching keys found between deduped full.csv, prepared input, and scores_raw.csv")

    output = full.copy()
    score_map = scores.set_index("Match Key")
    for score_col, target_col in RAW_SCORE_COLUMNS.items():
        mapped = output["Match Key"].map(score_map[score_col])
        mask = mapped.notna()
        output.loc[mask, target_col] = mapped[mask].values

    prepared_map = prepared.set_index("Match Key")
    degree_map = output["Match Key"].map(prepared_map["Degree"])
    degree_mask = degree_map.notna()
    output.loc[degree_mask, "Degree"] = degree_map[degree_mask].values
    alumni_map = output["Match Key"].map(prepared_map["Alumni Signal"])
    alumni_mask = alumni_map.notna()
    output.loc[alumni_mask, "Alumni Signal"] = alumni_map[alumni_mask].values

    changed_mask = []
    for _, row in merged.iterrows():
        changed = False
        changed |= not same_score(row.get("Distance"), row["Degree"])
        changed |= not same_score(row.get("Alumni Signal"), row.get("Alumni Signal_prepared"))
        changed |= not same_score(row.get(RAW_SCORE_COLUMNS["fo_persona"]), row["fo_persona"])
        changed |= not same_score(row.get(RAW_SCORE_COLUMNS["ft_persona"]), row["ft_persona"])
        changed |= not same_score(row.get(RAW_SCORE_COLUMNS["allocator"]), row["allocator"])
        changed |= not same_score(row.get(RAW_SCORE_COLUMNS["access"]), row["access"])
        changed_mask.append(changed)
    changed_mask = pd.Series(changed_mask, dtype=bool)

    delta = pd.DataFrame({
        "Match Key": merged["Match Key"],
        "Raw ID": merged["Raw ID"],
        "Best Email": merged["Best Email"],
        "Current Company": merged.get("Current Company", ""),
        "Current Title": merged.get("Current Title", ""),
        "Headline": merged.get("Headline", ""),
        "Degree": merged["Degree"],
        "Alumni Signal": merged["Alumni Signal_prepared"],
        RAW_SCORE_COLUMNS["fo_persona"]: merged["fo_persona"],
        RAW_SCORE_COLUMNS["ft_persona"]: merged["ft_persona"],
        RAW_SCORE_COLUMNS["allocator"]: merged["allocator"],
        RAW_SCORE_COLUMNS["access"]: merged["access"],
    })
    if not include_all:
        delta = delta[changed_mask]

    summary = pd.DataFrame({
        "metric": ["deduped_full_rows", "scored_rows", "changed_rows"],
        "value": [len(output), len(merged), int(changed_mask.sum())],
    })
    return output, merged, delta, summary, changed_mask


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", required=True)
    ap.add_argument("--prepared", required=True)
    ap.add_argument("--scores", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--include-all", action="store_true")
    args = ap.parse_args()

    ensure_dir(args.out)
    full, prepared, scores = load_scoring_frames(args.full, args.prepared, args.scores)
    output, merged, delta, summary, changed_mask = build_scoring_frames(full, prepared, scores, include_all=args.include_all)

    delta.to_csv(f"{args.out}/delta_updates.csv", index=False)
    output.to_csv(f"{args.out}/full_with_new_raw_scores.csv", index=False)
    summary.to_csv(f"{args.out}/delta_summary.csv", index=False)

    print(f"Wrote {args.out}/delta_updates.csv")
    print(f"Wrote {args.out}/full_with_new_raw_scores.csv")
    print(f"Scored rows: {len(merged)}")
    print(f"Changed rows: {int(changed_mask.sum())}")


if __name__ == "__main__":
    main()
