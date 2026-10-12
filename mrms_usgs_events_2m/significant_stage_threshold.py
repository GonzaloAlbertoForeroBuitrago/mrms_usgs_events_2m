"""Prepare traceable, unvalidated stage thresholds for later MRMS event screening.

Reads the national consolidation (one row per USGS station). Does not recompute
HydroEventDetector results, change source files, or download rainfall.

Run from ~/Documents/usgs_mrms_2m:
  PYTHONPATH="$PWD/mrms_usgs_events_2m" python -m mrms_usgs_events_2m.significant_stage_threshold --project-root "$PWD"

Outputs: national_significant_stage_thresholds.{parquet,csv} and
national_significant_stage_thresholds_manifest.json alongside the consolidation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

METHOD = "median_baseflow_densest_1ft_window_top_200_peaks"
EXPECTED_TOP_N = 200
EXPECTED_WINDOW_FT = 1.0
EXPECTED_ALPHA = 0.987
EXCLUDED_ACTION_REFERENCE = "01429000"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def git_info(repo: Path) -> dict:
    def git(*args: str) -> str | None:
        try:
            return subprocess.check_output(
                ["git", "-C", str(repo), *args], text=True,
                stderr=subprocess.DEVNULL, timeout=10,
            ).strip()
        except (OSError, subprocess.SubprocessError):
            return None
    return {"commit": git("rev-parse", "HEAD"),
            "working_tree_dirty": bool(git("status", "--porcelain"))
            if git("status", "--porcelain") is not None else None}


def build(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "site_id", "state", "selected_events", "detected_events",
        "median_baseflow_ft", "significant_stage_threshold_candidate_ft",
        "fraction_in_window", "events_in_window", "threshold_method",
        "threshold_mrms_validated", "validation_status",
        "action_comparison_eligible", "action_reference_exception",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Consolidation missing required columns: {missing}")
    out = df.copy()
    out["site_id"] = out["site_id"].astype("string").str.strip()
    if out["site_id"].isna().any() or out["site_id"].duplicated().any():
        raise ValueError("site_id must be unique and nonmissing")
    numeric = ["selected_events", "detected_events", "median_baseflow_ft",
               "significant_stage_threshold_candidate_ft", "fraction_in_window",
               "events_in_window"]
    for c in numeric:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    candidate = out["significant_stage_threshold_candidate_ft"].notna()
    if not (out.loc[candidate, "median_baseflow_ft"] -
            out.loc[candidate, "significant_stage_threshold_candidate_ft"]).abs().le(1e-9).all():
        raise ValueError("Candidate thresholds differ from saved median_baseflow_ft")
    if (out.loc[candidate, "threshold_method"] != METHOD).any():
        raise ValueError("Unexpected candidate threshold method")
    if out["threshold_mrms_validated"].fillna(False).astype(bool).any():
        raise ValueError("Input claims MRMS validation; this preparatory module cannot certify it")
    if ((out["selected_events"] > EXPECTED_TOP_N) |
        (out["selected_events"] < 0)).fillna(False).any():
        raise ValueError("selected_events outside expected range")
    if ((out["fraction_in_window"] < 0) |
        (out["fraction_in_window"] > 1)).fillna(False).any():
        raise ValueError("fraction_in_window outside [0, 1]")
    if ((out["events_in_window"] > out["selected_events"]) & candidate).any():
        raise ValueError("events_in_window exceeds selected_events")

    def event_group(n):
        if pd.isna(n): return "missing_peak_analysis"
        if n < 100: return "limited_event_sample"
        if n < EXPECTED_TOP_N: return "partial_event_sample"
        return "full_event_sample"

    out["threshold_event_sample_class"] = out["selected_events"].map(event_group)
    out["threshold_low_concentration"] = (out["fraction_in_window"] < 0.5).fillna(False)
    out["threshold_quality_flags"] = out.apply(
        lambda r: ";".join(
            (["missing_peak_analysis"] if pd.isna(r["selected_events"]) else [])
            + (["limited_event_sample"] if pd.notna(r["selected_events"]) and r["selected_events"] < 100 else [])
            + (["partial_event_sample"] if pd.notna(r["selected_events"]) and 100 <= r["selected_events"] < 200 else [])
            + (["low_concentration"] if r["threshold_low_concentration"] else [])
        ), axis=1,
    )
    out["threshold_ready_for_mrms_test"] = candidate & out["selected_events"].gt(0)
    out["threshold_ready_for_mrms_test"] = out["threshold_ready_for_mrms_test"].fillna(False)
    out["threshold_mrms_validated"] = False
    out["threshold_validation_source"] = "statistical_candidate_not_mrms_validated"
    out.loc[~candidate, "threshold_validation_source"] = "no_peak_analysis"
    out["threshold_for_peak_filter_ft"] = out["significant_stage_threshold_candidate_ft"]
    out["threshold_peak_filter_rule"] = "observed_peak_stage_ft >= threshold_for_peak_filter_ft"
    out["threshold_status_detail"] = "candidate_for_testing_only"
    out.loc[~candidate, "threshold_status_detail"] = "missing_peak_analysis"
    # NOAA Action Stage is an independent comparator, not a prerequisite for this threshold.
    if "action_comparison_eligible" in out:
        out["action_comparison_eligible"] = out["action_comparison_eligible"].fillna(False).astype(bool)
    out.loc[out["site_id"] == EXCLUDED_ACTION_REFERENCE, "action_comparison_eligible"] = False
    if "action_reference_exception" in out:
        out.loc[out["site_id"] == EXCLUDED_ACTION_REFERENCE, "action_reference_exception"] = "action_stage_reference_unresolved"
    return out


def atomic_json(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--stage-root", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    project = args.project_root.expanduser().resolve()
    stage = (args.stage_root or Path.home() / ".tethys/Tethys_flood_2m/media/usgs_mrms_2m/app/stage_data").expanduser().resolve()
    source = stage / "_action_stage_peaks_run/national_stage_peaks_consolidated.parquet"
    dest = (args.output_dir or source.parent).expanduser().resolve()
    if not source.is_file():
        parser.error(f"Consolidated input not found: {source}")
    df = pd.read_parquet(source)
    result = build(df)
    if len(result) != len(df):
        raise RuntimeError("Unexpected row count change")
    dest.mkdir(parents=True, exist_ok=True)
    parquet = dest / "national_significant_stage_thresholds.parquet"
    csv = dest / "national_significant_stage_thresholds.csv"
    result.to_parquet(parquet, index=False)
    result.to_csv(csv, index=False)
    counts = {str(k): int(v) for k, v in result["threshold_event_sample_class"].value_counts().items()}
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(source), "source_sha256": sha256_file(source),
        "source_rows": len(df), "output_rows": len(result),
        "unique_sites": int(result["site_id"].nunique()),
        "candidate_thresholds": int(result["threshold_for_peak_filter_ft"].notna().sum()),
        "missing_candidates": int(result["threshold_for_peak_filter_ft"].isna().sum()),
        "ready_for_mrms_test": int(result["threshold_ready_for_mrms_test"].sum()),
        "mrms_validated": 0,
        "event_sample_classes": counts,
        "low_concentration_count": int(result["threshold_low_concentration"].sum()),
        "method": METHOD,
        "parameters": {"top_n": EXPECTED_TOP_N, "baseflow_alpha": EXPECTED_ALPHA,
                       "window_width_ft": EXPECTED_WINDOW_FT},
        "interpretation": "A candidate stage screen; not an official Action Stage, bankfull stage, or validated rainfall predictor.",
        "excluded_action_reference": {EXCLUDED_ACTION_REFERENCE: "action_stage_reference_unresolved"},
        "code": {"module": "mrms_usgs_events_2m.significant_stage_threshold",
                 "sha256": sha256_file(Path(__file__)),
                 "git": git_info(project / "mrms_usgs_events_2m")},
        "outputs": [str(parquet), str(csv)],
    }
    atomic_json(dest / "national_significant_stage_thresholds_manifest.json", manifest)
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
