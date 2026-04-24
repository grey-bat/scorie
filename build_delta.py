import argparse
import pandas as pd
import numpy as np
from utils import DIRECT_SCORE_COLUMNS, RAW_SCORE_COLUMNS, ensure_dir, normalize_key, normalize_email, canonical_match_key


def same_score(a, b) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    try:
        return int(float(a)) == int(float(b))
    except Exception:
        return str(a).strip() == str(b).strip()


def same_score_vectorized(s1: pd.Series, s2: pd.Series) -> pd.Series:
    """Vectorized version of same_score."""
    # 1. Handle NaN cases (pd.isna handles both np.nan and None)
    both_nan = s1.isna() & s2.isna()

    # 2. Try numeric comparison: int(float(a)) == int(float(b))
    # Coerce to numeric; those that fail become NaN.
    s1_f = pd.to_numeric(s1, errors='coerce')
    s2_f = pd.to_numeric(s2, errors='coerce')

    # same_score's try block succeeds if both can be converted to float AND then to int.
    # int(float(inf)) raises OverflowError, so we only use numeric path for finite values.
    both_finite = np.isfinite(s1_f) & np.isfinite(s2_f)

    # 3. Default to stripped string comparison
    s1_s = s1.astype(str).str.strip()
    s2_s = s2.astype(str).str.strip()
    res = (s1_s == s2_s)

    # 4. Override with numeric comparison (int truncation) where both are finite
    if both_finite.any():
        res[both_finite] = (s1_f[both_finite].astype(int) == s2_f[both_finite].astype(int))

    # 5. Override with True where both were NaN
    res[both_nan] = True

    return res


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
    prepared_map = prepared.set_index("Match Key")
    score_map = scores.set_index("Match Key")
    direct_mode = "direct_score" in score_map.columns
    if direct_mode:
        for score_col, target_col in DIRECT_SCORE_COLUMNS.items():
            if score_col not in score_map.columns:
                if target_col not in output.columns:
                    output[target_col] = 0
                continue
            mapped = output["Match Key"].map(score_map[score_col])
            mask = mapped.notna()
            output.loc[mask, target_col] = mapped[mask].values
    else:
        for score_col, target_col in RAW_SCORE_COLUMNS.items():
            if score_col not in score_map.columns:
                if target_col not in output.columns:
                    output[target_col] = 0
                continue
            mapped = output["Match Key"].map(score_map[score_col])
            mask = mapped.notna()
            output.loc[mask, target_col] = mapped[mask].values
    for col in prepared.columns:
        if col in {"Match Key"}:
            continue
        if col in output.columns:
            continue
        output[col] = output["Match Key"].map(prepared_map[col])

    degree_map = output["Match Key"].map(prepared_map["Degree"])
    degree_mask = degree_map.notna()
    output.loc[degree_mask, "Degree"] = degree_map[degree_mask].values
    alumni_map = output["Match Key"].map(prepared_map["Alumni Signal"])
    alumni_mask = alumni_map.notna()
    output.loc[alumni_mask, "Alumni Signal"] = alumni_map[alumni_mask].values

    prepared_degree_col = "Degree_prepared" if "Degree_prepared" in merged.columns else "Degree"
    prepared_alumni_col = "Alumni Signal_prepared" if "Alumni Signal_prepared" in merged.columns else "Alumni Signal"

    if "company_fit" not in merged.columns:
        merged["company_fit"] = 0
    if "weighted_score" not in merged.columns:
        merged["weighted_score"] = 0
    if "legacy_weighted_score" not in merged.columns:
        merged["legacy_weighted_score"] = 0
    if "direct_score" not in merged.columns:
        merged["direct_score"] = 0
    if "score_band" not in merged.columns:
        merged["score_band"] = ""
    if "score_track" not in merged.columns:
        merged["score_track"] = "legacy_raw_weighted"

    company_fit_supported = "company_fit" in score_map.columns

    # Vectorized change detection
    # Initialize changed_mask as all False
    changed_mask = pd.Series(False, index=merged.index)

    # Core columns
    changed_mask |= ~same_score_vectorized(merged["Degree"], merged[prepared_degree_col])
    changed_mask |= ~same_score_vectorized(merged["Alumni Signal"], merged[prepared_alumni_col])

    if direct_mode:
        for score_col, target_col in DIRECT_SCORE_COLUMNS.items():
            # If column is missing from merged, it defaults to empty string/0
            s1 = merged[target_col] if target_col in merged.columns else pd.Series(0, index=merged.index)
            s2 = merged[score_col] if score_col in merged.columns else pd.Series("", index=merged.index)
            changed_mask |= ~same_score_vectorized(s1, s2)
    else:
        changed_mask |= ~same_score_vectorized(merged[RAW_SCORE_COLUMNS["fo_persona"]], merged["fo_persona"])
        changed_mask |= ~same_score_vectorized(merged[RAW_SCORE_COLUMNS["ft_persona"]], merged["ft_persona"])
        changed_mask |= ~same_score_vectorized(merged[RAW_SCORE_COLUMNS["allocator"]], merged["allocator"])
        changed_mask |= ~same_score_vectorized(merged[RAW_SCORE_COLUMNS["access"]], merged["access"])
        if company_fit_supported:
            s1 = merged["Company Fit Score"] if "Company Fit Score" in merged.columns else pd.Series("", index=merged.index)
            s2 = merged["company_fit"]
            changed_mask |= ~same_score_vectorized(s1, s2)

    zero_series = pd.Series([0] * len(merged), index=merged.index)
    delta = pd.DataFrame({
        "Match Key": merged["Match Key"],
        "Raw ID": merged["Raw ID"],
        "Best Email": merged["Best Email"],
        "Current Company": merged.get("Current Company", ""),
        "Current Title": merged.get("Current Title", ""),
        "Headline": merged.get("Headline", ""),
        "Degree": merged["Degree"],
        "Alumni Signal": merged[prepared_alumni_col] if prepared_alumni_col in merged.columns else merged["Alumni Signal"],
        "weighted_score": merged["weighted_score"] if "weighted_score" in merged.columns else zero_series,
        "legacy_weighted_score": merged["legacy_weighted_score"] if "legacy_weighted_score" in merged.columns else zero_series,
        "direct_score": merged["direct_score"] if "direct_score" in merged.columns else zero_series,
        "score_band": merged["score_band"] if "score_band" in merged.columns else pd.Series([""] * len(merged), index=merged.index),
        "score_track": merged["score_track"] if "score_track" in merged.columns else pd.Series(["legacy_raw_weighted"] * len(merged), index=merged.index),
    })
    if direct_mode:
        for score_col, target_col in DIRECT_SCORE_COLUMNS.items():
            delta[target_col] = merged[score_col]
    else:
        delta[RAW_SCORE_COLUMNS["fo_persona"]] = merged["fo_persona"]
        delta[RAW_SCORE_COLUMNS["ft_persona"]] = merged["ft_persona"]
        delta[RAW_SCORE_COLUMNS["allocator"]] = merged["allocator"]
        delta[RAW_SCORE_COLUMNS["access"]] = merged["access"]
        delta[RAW_SCORE_COLUMNS["company_fit"]] = merged["company_fit"]
    for extra_col in ["Company Context Source", "Company Backfill Needed", "Company Backfill Reason", "Company Context Score"]:
        if extra_col in merged.columns:
            delta[extra_col] = merged[extra_col]
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
