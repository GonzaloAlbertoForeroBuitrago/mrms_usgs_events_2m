"""Consolidate national USGS stage peaks and NOAA Action Stage audits.

Read-only inputs; outputs are written exclusively to _action_stage_peaks_run.
Run from the project root:
  PYTHONPATH="$PWD/mrms_usgs_events_2m" python -m mrms_usgs_events_2m.national_stage_consolidation --project-root "$PWD"

This does NOT validate MRMS rainfall or establish a physical bankfull stage.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

EXCLUDED_ACTION_REFERENCE = {"01429000": "action_stage_reference_unresolved"}
ACCEPTED_DATUM_STATUS = {"reference_compatible", "historically_supported"}
TOP_N = 200
ALPHA = 0.987
WINDOW_FT = 1.0
RANKING = "flow_peak"


def read_table(path: Path, sep: str = ",") -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Missing required input: {path}")
    df = pd.read_csv(path, sep=sep, dtype={"site_id": "string"}, low_memory=False)
    if "site_id" not in df:
        raise ValueError(f"site_id column missing: {path}")
    df["site_id"] = df["site_id"].astype("string").str.strip().str.zfill(8)
    if df["site_id"].isna().any() or df["site_id"].duplicated().any():
        raise ValueError(f"Missing or duplicate site_id: {path}")
    return df


def station_metadata(stage_root: Path, inventory: pd.DataFrame) -> pd.DataFrame:
    wanted = ["station_name", "state_code", "state_name", "county_name",
              "hydrologic_unit_code", "latitude", "longitude", "altitude",
              "vertical_datum", "vertical_datum_name", "drainage_area",
              "contributing_drainage_area", "time_zone", "source"]
    rows = []
    for r in inventory[["site_id", "state"]].itertuples(index=False):
        p = stage_root / str(r.state) / str(r.site_id) / "downloaded_data" / "station_metadata.parquet"
        row = {"site_id": str(r.site_id), "station_metadata_found": p.is_file()}
        if p.is_file():
            try:
                md = pd.read_parquet(p)
                if len(md) != 1:
                    row["station_metadata_error"] = f"expected_one_row_found_{len(md)}"
                elif "site_id" in md and str(md.iloc[0]["site_id"]).zfill(8) != str(r.site_id):
                    row["station_metadata_error"] = "site_id_mismatch"
                else:
                    for col in wanted:
                        row[col] = md.iloc[0][col] if col in md else None
            except (OSError, ValueError, KeyError) as exc:
                row["station_metadata_error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        rows.append(row)
    return pd.DataFrame(rows)



def selected_source_metadata(stage_root: Path, inventory: pd.DataFrame) -> pd.DataFrame:
    """Read existing source selection and metadata; never infer sensor hardware."""
    rows = []
    fields = {
        "time_series_id": "selected_time_series_id",
        "method_category": "sensor_method_category",
        "method_id": "sensor_method_id",
        "method_description": "sensor_method_description",
        "sublocation_identifier": "sensor_sublocation",
        "series_primary": "sensor_is_primary",
        "source": "selected_source_provider",
        "vertical_datum": "sensor_vertical_datum",
        "vertical_datum_name": "sensor_vertical_datum_name",
    }
    for r in inventory[["site_id", "state"]].itertuples(index=False):
        folder = stage_root / str(r.state) / str(r.site_id) / "downloaded_data"
        row = {"site_id": str(r.site_id), "source_selection_status": "missing",
               "selected_stage_source_id": None, "sensor_identification_status": "missing_source_selection"}
        selection_path = folder / "stage_source_selection.json"
        if not selection_path.is_file():
            rows.append(row)
            continue
        try:
            selection = json.loads(selection_path.read_text(encoding="utf-8"))
            row["source_selection_status"] = selection.get("status")
            row["source_selection_reason"] = selection.get("reason")
            row["source_candidate_count"] = selection.get("candidate_count")
            row["source_selected_rows"] = selection.get("selected_rows")
            row["source_excluded_rows"] = selection.get("excluded_rows")
            key = selection.get("selected_source_key")
            row["selected_stage_source_id"] = key
            if selection.get("status") != "selected" or not key:
                row["sensor_identification_status"] = "source_not_selected"
                rows.append(row)
                continue
            metadata_path = folder / "stage_source_metadata.parquet"
            if not metadata_path.is_file():
                row["sensor_identification_status"] = "missing_source_metadata"
                rows.append(row)
                continue
            md = pd.read_parquet(metadata_path)
            if "stage_source_id" not in md.columns:
                row["sensor_identification_status"] = "missing_source_id_column"
                rows.append(row)
                continue
            matches = md.loc[md["stage_source_id"].astype("string") == str(key)]
            if len(matches) != 1:
                row["sensor_identification_status"] = f"source_metadata_matches_{len(matches)}"
                rows.append(row)
                continue
            for src, dst in fields.items():
                row[dst] = matches.iloc[0][src] if src in matches else None
            method = row.get("sensor_method_description")
            row["sensor_identification_status"] = (
                "method_documented" if pd.notna(method) and str(method).strip() else
                "source_identified_method_unspecified"
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            row["sensor_identification_status"] = "metadata_read_error"
            row["sensor_metadata_error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        rows.append(row)
    return pd.DataFrame(rows)


def build(project_root: Path, stage_root: Path, output_dir: Path) -> tuple[pd.DataFrame, dict]:
    inventory_path = project_root / "us_basins_action_stage_nwps.txt"
    correction_path = project_root / "us_basins_action_stage_corrected.csv"
    peaks_path = stage_root / "_action_stage_peaks_run" / "national_action_stage_peaks.csv"
    quality_path = stage_root / "_action_stage_peaks_run" / "national_action_stage_quality_audit.csv"
    inventory = read_table(inventory_path, "\t")
    correction = read_table(correction_path)
    peaks = read_table(peaks_path)
    quality = read_table(quality_path)
    # The inventory is the authority for station/state identity and original NOAA values.
    result = inventory.rename(columns={"Action_Stage_ft_exact_USGS_ID": "inventory_action_stage_ft",
                                       "supplied_NWS_LID": "nws_lid"}).copy()
    result["state"] = result["state"].astype("string")
    correction_cols = ["site_id", "action_stage_original_ft", "action_stage_corrected_ft",
                       "noaa_minus_usgs_ft", "validation_status", "reason", "historical_matches",
                       "historical_total", "historical_consistency_fraction",
                       "historical_offset_range_ft", "noaa_observed_utc", "usgs_observed_utc",
                       "noaa_datum", "noaa_datum_value_ft", "checked_at_utc",
                       "reference_compatible", "correction_applied", "action_stage_candidate_ft"]
    result = result.merge(correction[[c for c in correction_cols if c in correction]],
                          on="site_id", how="left", validate="one_to_one")
    peak_cols = ["site_id", "detected_events", "selected_events", "analysis_status",
                 "median_baseflow_ft", "median_all_baseflow_ft", "window_min_ft",
                 "window_max_ft", "events_in_window", "fraction_in_window"]
    result = result.merge(peaks[[c for c in peak_cols if c in peaks]],
                          on="site_id", how="left", validate="one_to_one")
    quality_cols = ["site_id", "historical_min_ft", "historical_median_ft",
                    "historical_p99_ft", "historical_max_ft", "quality_flags"]
    result = result.merge(quality[[c for c in quality_cols if c in quality]],
                          on="site_id", how="left", validate="one_to_one")
    result = result.merge(station_metadata(stage_root, inventory), on="site_id",
                          how="left", validate="one_to_one")
    result = result.merge(selected_source_metadata(stage_root, inventory), on="site_id",
                          how="left", validate="one_to_one")
    for c in ["action_stage_original_ft", "action_stage_corrected_ft", "noaa_minus_usgs_ft",
              "median_baseflow_ft", "median_all_baseflow_ft", "fraction_in_window",
              "selected_events", "detected_events", "events_in_window"]:
        if c in result:
            result[c] = pd.to_numeric(result[c], errors="coerce")
    result["action_reference_exception"] = result["site_id"].map(EXCLUDED_ACTION_REFERENCE)
    result["action_comparison_eligible"] = (
        result["validation_status"].isin(ACCEPTED_DATUM_STATUS)
        & result["action_stage_corrected_ft"].notna()
        & result["median_baseflow_ft"].notna()
        & result["action_reference_exception"].isna()
    )
    result["action_minus_baseflow_ft"] = (
        result["action_stage_corrected_ft"] - result["median_baseflow_ft"]
    ).where(result["action_comparison_eligible"])
    # Candidate only: rainfall-discrimination ability has not been established.
    result["significant_stage_threshold_candidate_ft"] = result["median_baseflow_ft"]
    result["threshold_method"] = "median_baseflow_densest_1ft_window_top_200_peaks"
    result.loc[result["median_baseflow_ft"].isna(), "threshold_method"] = pd.NA
    result["threshold_mrms_validated"] = False
    result["threshold_status"] = result["median_baseflow_ft"].notna().map(
        {True: "candidate_unvalidated_against_mrms", False: "missing_peak_analysis"})
    result["top_n_configured"] = TOP_N
    result["baseflow_alpha_configured"] = ALPHA
    result["window_width_ft_configured"] = WINDOW_FT
    result["ranking_method_configured"] = RANKING
    if len(result) != len(inventory) or result["site_id"].nunique() != len(inventory):
        raise AssertionError("Consolidated inventory lost or duplicated stations")
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "national_stage_peaks_consolidated.csv"
    parquet_path = output_dir / "national_stage_peaks_consolidated.parquet"
    summary_path = output_dir / "national_stage_analysis_summary.json"
    result.to_csv(csv_path, index=False)
    result.to_parquet(parquet_path, index=False)
    statuses = result["validation_status"].fillna("missing").value_counts().to_dict()
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "inventory_count": len(result),
        "stations_with_peak_analysis": int(result["median_baseflow_ft"].notna().sum()),
        "stations_with_action_comparison": int(result["action_comparison_eligible"].sum()),
        "stations_with_candidate_threshold": int(result["significant_stage_threshold_candidate_ft"].notna().sum()),
        "stations_with_selected_source": int(result["selected_stage_source_id"].notna().sum()),
        "sensor_identification_status_counts": {
            str(k): int(v) for k, v in result["sensor_identification_status"].fillna("missing").value_counts().items()
        },
        "excluded_action_reference": EXCLUDED_ACTION_REFERENCE,
        "validation_status_counts": {str(k): int(v) for k, v in statuses.items()},
        "source_files": [str(p) for p in [inventory_path, correction_path, peaks_path, quality_path]],
        "settings": {"top_n": TOP_N, "alpha": ALPHA, "ranking": RANKING, "window_width_ft": WINDOW_FT},
        "notes": ["NOAA Action Stage is an operational threshold, not bankfull stage.",
                  "Candidate significant stage thresholds have NOT been validated against MRMS rainfall.",
                  "Action Stage conversion is evaluated separately from stage-event detection.",
                  "The state column denotes the USGS station location, not all states intersected by its watershed."],
        "outputs": [str(csv_path), str(parquet_path)],
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return result, summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--stage-root", type=Path, default=Path.home() / ".tethys/Tethys_flood_2m/media/usgs_mrms_2m/app/stage_data")
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    stage_root = args.stage_root.expanduser().resolve()
    out = args.output_dir.expanduser().resolve() if args.output_dir else stage_root / "_action_stage_peaks_run"
    _, summary = build(args.project_root.expanduser().resolve(), stage_root, out)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
