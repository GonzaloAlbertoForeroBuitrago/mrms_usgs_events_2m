"""Process USGS stations without NOAA Action Stage, reusing HydroEventDetector.

Run from project root with PYTHONPATH pointing at mrms_usgs_events_2m.
This module reads existing downloaded_data and never downloads or changes it.
It writes processed_data_without_action (separate from existing processed_data).

The densest 1-ft baseflow window uses the shared
analyze_stage_baseflow_window function, without any NOAA Action Stage.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .stage_event_detection import detect_stage_events, analyze_stage_baseflow_window

TOP_N = 200
ALPHA = 0.987
RANKING = "flow_peak"
WINDOW_FT = 1.0
EXPECTED_PENDING = 4145
OUTPUT_FILES = ("stage_baseflow.parquet", "stage_events_detected.parquet",
                "stage_events_filtered.parquet", "stage_events_selected.parquet",
                "event_detection_manifest.json", "stage_significant_threshold.json")


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def load_pending(path, master):
    d = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    m = pd.read_csv(master, sep=r"\s+", dtype=str, keep_default_na=False)
    if not {"site_id", "state"}.issubset(d) or not {"site_id", "state"}.issubset(m):
        raise ValueError("Inventories require site_id and state columns")
    if len(m) != 8616 or m.site_id.nunique() != 8616:
        raise ValueError("Master inventory is not the expected 8,616 unique stations")
    if d.empty:
        raise ValueError("Pending inventory is empty")
    if d.site_id.duplicated().any():
        raise ValueError("Pending inventory contains duplicate stations")
    if len(d) > EXPECTED_PENDING:
        raise ValueError("Pending inventory exceeds 4,145 stations")
    known = set(zip(m.site_id, m.state))
    if any(pair not in known for pair in zip(d.site_id, d.state)):
        raise ValueError("Pending inventory has IDs/states absent from master")
    if d.site_id.str.strip().ne(d.site_id).any() or d.state.str.strip().ne(d.state).any():
        raise ValueError("Unexpected whitespace in pending inventory")
    return list(zip(d.state, d.site_id))


def completed(path, site):
    if not all((path / name).is_file() for name in OUTPUT_FILES):
        return False
    try:
        a = json.loads((path / "stage_significant_threshold.json").read_text())
        m = json.loads((path / "event_detection_manifest.json").read_text())
        return (a.get("site_id") == site and a.get("status") == "ok"
                and m.get("site_id") == site and a.get("method") == "median_baseflow_densest_1ft_window_top_200_peaks")
    except (OSError, ValueError):
        return False


def process_one(state, site, root):
    folder = root / state / site
    downloaded = folder / "downloaded_data"
    source = downloaded / "stage_source_selection.json"
    inp = downloaded / "stage_hydroeventdetector.parquet"
    if not source.is_file() or not inp.is_file():
        return "missing_download", "missing source selection or stage input", None
    try:
        selection = json.loads(source.read_text())
    except (OSError, ValueError) as exc:
        return "invalid_selection", str(exc), None
    if selection.get("status") != "selected":
        return "source_review_required", str(selection.get("reason", "source not selected")), None
    out = folder / "processed_data_without_action"
    if completed(out, site):
        return "already_complete", "", None
    staging = folder / "processed_data_without_action.in_progress"
    if staging.exists():
        shutil.rmtree(staging)
    try:
        result = detect_stage_events(
            input_path=inp, output_directory=staging, site_id=site,
            baseflow_alpha=ALPHA, top_n_events=TOP_N, ranking_method=RANKING,
            event_filter_percentile=None, action_stage_ft=None,
            action_window_width_ft=WINDOW_FT,
        )
        selected = pd.read_parquet(staging / "stage_events_selected.parquet")
        # Reuse the shared moving-window algorithm; no NOAA Action Stage is needed.
        analysis = analyze_stage_baseflow_window(selected, window_width_ft=WINDOW_FT)
        if analysis.get("status") != "ok":
            raise ValueError("No finite selected-event baseflow values")
        analysis.update(site_id=site, state=state,
                        method="median_baseflow_densest_1ft_window_top_200_peaks",
                        significant_stage_threshold_candidate_ft=analysis["median_window_baseflow_ft"],
                        threshold_mrms_validated=False,
                        threshold_status="candidate_unvalidated_against_mrms",
                        input_sha256=sha256(inp), source_selection=selection.get("selected_source_key"),
                        processed_at_utc=now())
        write_json(staging / "stage_significant_threshold.json", analysis)
        manifest_file = staging / "event_detection_manifest.json"
        manifest = json.loads(manifest_file.read_text())
        manifest["output_directory"] = str(out)
        manifest["significant_threshold_file"] = "stage_significant_threshold.json"
        write_json(manifest_file, manifest)
        if out.exists():
            # Do not silently overwrite a preexisting output directory.
            raise FileExistsError(f"Existing incomplete output requires manual review: {out}")
        staging.rename(out)
        if not completed(out, site):
            raise RuntimeError("Post-write output validation failed")
        return "ok", "", {"detected": result.number_of_detected_events,
                           "selected": result.number_of_selected_events,
                           "threshold_ft": analysis["median_window_baseflow_ft"],
                           "fraction_in_window": analysis["fraction_in_window"]}
    except Exception:
        # Keep staging directory for diagnosis; no original data is touched.
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project-root", type=Path, default=Path.cwd())
    p.add_argument("--stage-root", type=Path, default=Path.home() / ".tethys/Tethys_flood_2m/media/usgs_mrms_2m/app/stage_data")
    p.add_argument("--inventory", type=Path, default=None)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    project = args.project_root.expanduser().resolve()
    root = args.stage_root.expanduser().resolve()
    inv = args.inventory or project / "national_pending_stations/stations_without_action_stage_master.txt"
    rows = load_pending(inv, project / "us_basins_water_stage.txt")
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be positive")
    pending = [(state, site) for state, site in rows
               if not completed(root / state / site / "processed_data_without_action", site)]
    scheduled = pending[:args.limit] if args.limit else pending
    print(f"Inventory={len(rows)} complete={len(rows)-len(pending)} scheduled={len(scheduled)} dry_run={args.dry_run}", flush=True)
    if args.dry_run:
        for state, site in scheduled[:20]:
            print("[WOULD CHECK]", state, site)
        return
    logdir = root / "_stage_peaks_without_action_run"
    logdir.mkdir(parents=True, exist_ok=True)
    logpath = logdir / "processing_status.csv"
    fields = ["timestamp_utc", "state", "site_id", "status", "detected", "selected", "threshold_ft", "fraction_in_window", "detail"]
    for i, (state, site) in enumerate(scheduled, 1):
        try:
            status, detail, metrics = process_one(state, site, root)
        except Exception as exc:
            status, detail, metrics = "error", f"{type(exc).__name__}: {exc}", None
        row = dict(timestamp_utc=now(), state=state, site_id=site, status=status, detail=detail)
        if metrics:
            row.update(metrics)
        with logpath.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if f.tell() == 0:
                writer.writeheader()
            writer.writerow(row)
        print(f"[{i}/{len(scheduled)}] {state} {site} {status} {detail}", flush=True)
    print("FINISHED", logpath, flush=True)


if __name__ == "__main__":
    main()
