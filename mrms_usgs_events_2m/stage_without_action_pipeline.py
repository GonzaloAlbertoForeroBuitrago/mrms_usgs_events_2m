"""Reproducible USGS stage download + event pipeline without NOAA Action Stage.

Uses the existing stage_download and stage_event_detection modules. Never
silently overwrites downloaded_data or processed_data. Default inventory is the
4,145-site complement of the Action Stage inventory within the 8,616-site master.

Example (isolated test):
  python -m mrms_usgs_events_2m.stage_without_action_pipeline \
    --project-root ~/Documents/usgs_mrms_2m \
    --stage-root ~/Documents/usgs_mrms_2m/stage_validation_test \
    --inventory ~/Documents/usgs_mrms_2m/stage_validation_test/test_inventory.txt \
    --allow-subset --dry-run
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from .stage_download import build_session, download_stage_data
from .stage_event_detection import detect_stage_events, analyze_stage_baseflow_window

START = "2019-01-01"
END = "2026-10-05"  # exclusive, UTC
ALPHA = 0.987
TOP_N = 200
RANKING = "flow_peak"
WINDOW_FT = 1.0
DOWNLOAD_FILES = (
    "stage_observations.parquet", "stage_hydroeventdetector.parquet",
    "station_metadata.parquet", "time_series_metadata.parquet",
    "stage_source_metadata.parquet", "stage_source_selection.json",
    "excluded_observations.parquet", "download_manifest.json",
)
PROCESS_FILES = (
    "stage_baseflow.parquet", "stage_events_detected.parquet",
    "stage_events_filtered.parquet", "stage_events_selected.parquet",
    "event_detection_manifest.json", "stage_baseflow_window_analysis.json",
    "stage_significant_threshold.json",
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def digest(path: Path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def json_read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def json_write(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    temp.replace(path)


def inventory(project: Path, path: Path, allow_subset: bool):
    master = pd.read_csv(project / "us_basins_water_stage.txt", sep=r"\s+", dtype=str, keep_default_na=False)
    action = pd.read_csv(project / "us_basins_action_stage_nwps.txt", sep="\t", dtype=str, keep_default_na=False)
    chosen = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    for name, d in (("master", master), ("action", action), ("chosen", chosen)):
        if not {"site_id", "state"}.issubset(d.columns):
            raise ValueError(f"{name}: expected site_id and state")
        if d.site_id.duplicated().any() or d.site_id.eq("").any():
            raise ValueError(f"{name}: duplicate or empty site_id")
        if any(d[c].ne(d[c].str.strip()).any() for c in ("site_id", "state")):
            raise ValueError(f"{name}: unexpected whitespace")
    if len(master) != 8616 or len(action) != 4471:
        raise ValueError("Unexpected master or Action Stage inventory size")
    master_map = dict(zip(master.site_id, master.state))
    if any(master_map.get(s) != st for s, st in zip(action.site_id, action.state)):
        raise ValueError("Action Stage inventory does not match master state naming")
    pending = set(master.site_id) - set(action.site_id)
    if len(pending) != 4145:
        raise ValueError("Expected 4,145 pending sites")
    if any(s not in pending or master_map.get(s) != st for s, st in zip(chosen.site_id, chosen.state)):
        raise ValueError("Chosen inventory includes an Action Stage site, unknown site, or wrong state")
    if chosen.empty:
        raise ValueError("Empty inventory")
    if not allow_subset and (len(chosen) != 4145 or set(chosen.site_id) != pending):
        raise ValueError("National run requires all 4,145 sites; use --allow-subset for testing")
    return list(zip(chosen.state, chosen.site_id))


def download_status(folder: Path):
    if not folder.exists():
        return "absent", None
    if not folder.is_dir() or any(not (folder / f).is_file() for f in DOWNLOAD_FILES):
        return "incomplete", None
    try:
        manifest = json_read(folder / "download_manifest.json")
        selection = json_read(folder / "stage_source_selection.json")
        if manifest.get("requested_start") != START or manifest.get("requested_end") != END:
            return "different_dates", selection
        if manifest.get("time_window_semantics") != "[start, end)":
            return "different_time_contract", selection
        if selection.get("status") != "selected":
            return "unselected", selection
        if not len(pd.read_parquet(folder / "stage_hydroeventdetector.parquet", columns=["Stage_ft"])):
            return "empty", selection
        return "ready", selection
    except (OSError, ValueError, KeyError, ImportError) as exc:
        return "invalid", {"detail": str(exc)}


def process_status(folder: Path, site: str, input_file: Path):
    if not folder.exists():
        return "absent"
    if not folder.is_dir() or any(not (folder / f).is_file() for f in PROCESS_FILES):
        return "incomplete"
    try:
        threshold = json_read(folder / "stage_significant_threshold.json")
        manifest = json_read(folder / "event_detection_manifest.json")
        if (threshold.get("site_id") == site and threshold.get("status") == "ok"
            and threshold.get("input_sha256") == digest(input_file)
            and threshold.get("method") == "median_baseflow_densest_1ft_window_top_200_peaks"
            and manifest.get("site_id") == site
            and manifest.get("baseflow_alpha") == ALPHA
            and manifest.get("top_n_events") == TOP_N
            and manifest.get("ranking_method") == RANKING):
            return "complete"
        return "stale_or_mismatched"
    except (OSError, ValueError, KeyError):
        return "invalid"


def download_one(root: Path, state: str, site: str):
    station = root / state / site
    final = station / "downloaded_data"
    status, _ = download_status(final)
    if status != "absent":
        return status
    # The downloader creates downloaded_data under its output_root.
    staging_root = station / ".download_staging"
    if staging_root.exists():
        return "staging_exists"
    staging_root.mkdir(parents=True)
    try:
        session = build_session()
        try:
            result = download_stage_data(
                state, site, START, END, output_root=staging_root,
                session=session, save_raw_metadata=True,
                save_raw_observations=False,
            )
        finally:
            session.close()
        candidate = staging_root / state / site / "downloaded_data"
        check, _ = download_status(candidate)
        if check not in ("ready", "unselected"):
            return "download_validation_" + check
        if final.exists():
            return "destination_appeared"
        candidate.rename(final)
        return "downloaded_" + check
    finally:
        # Preserve failed/partial staging for manual review, never delete it.
        if staging_root.exists() and not any(staging_root.rglob("*")):
            staging_root.rmdir()
        elif staging_root.exists() and not (staging_root / state / site / "downloaded_data").exists():
            # Successful promotion: remove only now-empty staging folders.
            shutil.rmtree(staging_root)


def process_one(root: Path, state: str, site: str):
    station = root / state / site
    inp = station / "downloaded_data" / "stage_hydroeventdetector.parquet"
    final = station / "processed_data"
    status = process_status(final, site, inp)
    if status != "absent":
        return status, None
    staging = station / "processed_data.in_progress"
    if staging.exists():
        return "processing_staging_exists", None
    result = detect_stage_events(
        input_path=inp, output_directory=staging, site_id=site,
        baseflow_alpha=ALPHA, top_n_events=TOP_N,
        ranking_method=RANKING, event_filter_percentile=None,
        action_stage_ft=None, action_window_width_ft=WINDOW_FT,
    )
    selected = pd.read_parquet(staging / "stage_events_selected.parquet")
    analysis = analyze_stage_baseflow_window(selected, window_width_ft=WINDOW_FT)
    if analysis.get("status") != "ok":
        raise ValueError("No usable baseflow window; staging preserved")
    selection = json_read(station / "downloaded_data" / "stage_source_selection.json")
    analysis.update(
        site_id=site, state=state,
        method="median_baseflow_densest_1ft_window_top_200_peaks",
        significant_stage_threshold_candidate_ft=analysis["median_window_baseflow_ft"],
        threshold_mrms_validated=False,
        threshold_status="candidate_unvalidated_against_mrms",
        input_sha256=digest(inp),
        source_selection=selection.get("selected_source_key"),
        processed_at_utc=utc_now(),
    )
    json_write(staging / "stage_significant_threshold.json", analysis)
    manifest_path = staging / "event_detection_manifest.json"
    manifest = json_read(manifest_path)
    manifest["output_directory"] = str(final)
    manifest["significant_threshold_file"] = "stage_significant_threshold.json"
    json_write(manifest_path, manifest)
    if final.exists():
        return "destination_appeared", None
    staging.rename(final)
    if process_status(final, site, inp) != "complete":
        raise RuntimeError("Post-promotion validation failed")
    return "processed", {
        "detected_events": result.number_of_detected_events,
        "selected_events": result.number_of_selected_events,
        "threshold_ft": analysis["median_window_baseflow_ft"],
        "fraction_in_window": analysis["fraction_in_window"],
    }


def append_csv(path: Path, record: dict):
    cols = ("timestamp_utc", "state", "site_id", "download_status", "process_status",
            "detected_events", "selected_events", "threshold_ft", "fraction_in_window", "error")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if f.tell() == 0:
            w.writeheader()
        w.writerow(record)
        f.flush()
        os.fsync(f.fileno())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project-root", type=Path, default=Path.cwd())
    p.add_argument("--stage-root", type=Path, default=Path.home() / ".tethys/Tethys_flood_2m/media/usgs_mrms_2m/app/stage_data")
    p.add_argument("--inventory", type=Path, default=None)
    p.add_argument("--allow-subset", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sleep", type=float, default=2.0, help="Seconds between stations")
    args = p.parse_args()
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be positive")
    if args.sleep < 0:
        p.error("--sleep must be nonnegative")
    project = args.project_root.expanduser().resolve()
    root = args.stage_root.expanduser().resolve()
    inv = (args.inventory or project / "national_pending_stations/stations_without_action_stage_master.txt").expanduser().resolve()
    rows = inventory(project, inv, args.allow_subset)
    pending = []
    for state, site in rows:
        dstatus, _ = download_status(root / state / site / "downloaded_data")
        pstatus = process_status(root / state / site / "processed_data", site,
                                 root / state / site / "downloaded_data/stage_hydroeventdetector.parquet") if dstatus == "ready" else "unknown"
        if dstatus == "ready" and pstatus == "complete":
            continue
        pending.append((state, site, dstatus, pstatus))
    scheduled = pending[:args.limit] if args.limit is not None else pending
    print(f"Inventory={len(rows)} complete={len(rows)-len(pending)} scheduled={len(scheduled)} dry_run={args.dry_run}", flush=True)
    if args.dry_run:
        for state, site, ds, ps in scheduled[:20]:
            print(f"[WOULD CHECK] {state} {site} download={ds} process={ps}")
        return
    run_dir = root / "_stage_without_action_pipeline_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    json_write(run_dir / "run_config.json", {
        "created_at_utc": utc_now(), "inventory": str(inv), "inventory_sha256": digest(inv),
        "master_sha256": digest(project / "us_basins_water_stage.txt"),
        "action_inventory_sha256": digest(project / "us_basins_action_stage_nwps.txt"),
        "module_sha256": digest(Path(__file__)), "requested_start": START,
        "requested_end_exclusive": END, "time_standard": "UTC", "top_n": TOP_N,
        "baseflow_alpha": ALPHA, "ranking_method": RANKING,
        "window_width_ft": WINDOW_FT, "scheduled": len(scheduled),
    })
    log = run_dir / "processing_status.csv"
    for i, (state, site, _, _) in enumerate(scheduled, 1):
        rec = {"timestamp_utc": utc_now(), "state": state, "site_id": site,
               "download_status": "", "process_status": "", "error": ""}
        try:
            dstatus, _ = download_status(root / state / site / "downloaded_data")
            if dstatus == "absent":
                dstatus = download_one(root, state, site)
            rec["download_status"] = dstatus
            verified, selection = download_status(root / state / site / "downloaded_data")
            if verified == "ready":
                pstatus, metrics = process_one(root, state, site)
                rec["process_status"] = pstatus
                if metrics:
                    rec.update(metrics)
            else:
                rec["process_status"] = "skipped_" + verified
        except Exception as exc:
            rec["error"] = f"{type(exc).__name__}: {str(exc)[:400]}"
            rec["process_status"] = "error"
        append_csv(log, rec)
        print(f"[{i}/{len(scheduled)}] {state} {site} download={rec['download_status']} process={rec['process_status']} {rec['error']}", flush=True)
        if args.sleep:
            time.sleep(args.sleep)
    print("FINISHED", log, flush=True)


if __name__ == "__main__":
    main()
