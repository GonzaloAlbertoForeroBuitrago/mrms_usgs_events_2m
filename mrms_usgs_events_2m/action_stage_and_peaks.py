"""National HydroEventDetector + Action Stage processing (USGS MRMS 2m).

Reads the Action Stage inventory and previously downloaded USGS stage data.
Never changes downloaded_data. Outputs per-station processed_data plus national logs.

Example (from ~/Documents/usgs_mrms_2m):
    PYTHONPATH="$PWD/mrms_usgs_events_2m" python -m mrms_usgs_events_2m.action_stage_and_peaks --dry-run
    PYTHONPATH="$PWD/mrms_usgs_events_2m" python -m mrms_usgs_events_2m.action_stage_and_peaks --limit 5
    PYTHONPATH="$PWD/mrms_usgs_events_2m" python -m mrms_usgs_events_2m.action_stage_and_peaks
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .stage_event_detection import detect_stage_events

TOP_N = 200
WINDOW_WIDTH_FT = 1.0
ALPHA = 0.987
RANKING = "flow_peak"
EXPECTED_STATIONS = 4471
OUTPUT_FILES = (
    "stage_baseflow.parquet",
    "stage_events_detected.parquet",
    "stage_events_filtered.parquet",
    "stage_events_selected.parquet",
    "stage_action_analysis.json",
    "event_detection_manifest.json",
)
STATUS_FIELDS = (
    "timestamp_utc", "state", "site_id", "status", "detected_events",
    "selected_events", "median_window_baseflow_ft", "action_minus_median_window_ft",
    "elapsed_seconds", "detail",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def paths(project_root: Path, stage_root: Path | None) -> tuple[Path, Path, Path]:
    inventory = project_root / "us_basins_action_stage.txt"
    data = stage_root or (Path.home() / ".tethys" / "Tethys_flood_2m" /
                          "media" / "usgs_mrms_2m" / "app" / "stage_data")
    return inventory, data, data / "_action_stage_peaks_run"


def inventory_rows(path: Path) -> list[tuple[str, str, float]]:
    if not path.is_file():
        raise FileNotFoundError(f"Missing inventory: {path}")
    df = pd.read_csv(path, sep=r"\s+", dtype=str, keep_default_na=False)
    required = {"site_id", "state", "Action_Stage_ft_exact_USGS_ID"}
    if not required.issubset(df.columns):
        raise ValueError(f"Inventory missing: {sorted(required - set(df.columns))}")
    if len(df) != EXPECTED_STATIONS:
        raise ValueError(f"Expected {EXPECTED_STATIONS} stations; found {len(df)}")
    df["site_id"] = df["site_id"].str.strip()
    df["state"] = df["state"].str.strip()
    if df.site_id.duplicated().any() or df.site_id.eq("").any() or df.state.eq("").any():
        raise ValueError("Inventory contains duplicate/blank station identifiers or states")
    stages = pd.to_numeric(df["Action_Stage_ft_exact_USGS_ID"], errors="coerce")
    if not stages.map(math.isfinite).all():
        raise ValueError("Inventory contains nonfinite Action Stage values")
    return list(zip(df.site_id, df.state, stages.astype(float)))


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def valid_completed(out: Path, site: str, action: float) -> bool:
    if not all((out / name).is_file() for name in OUTPUT_FILES):
        return False
    m = read_json(out / "event_detection_manifest.json")
    a = read_json(out / "stage_action_analysis.json")
    try:
        return (
            str(m.get("site_id")) == site
            and m.get("top_n_events") == TOP_N
            and m.get("ranking_method") == RANKING
            and m.get("event_filter_percentile") is None
            and math.isclose(float(m.get("baseflow_alpha")), ALPHA, abs_tol=1e-12)
            and math.isclose(float(a.get("action_stage_ft")), action, abs_tol=1e-9)
            and math.isclose(float(a.get("window_width_ft")), WINDOW_WIDTH_FT, abs_tol=1e-12)
            and a.get("status") in {"ok", "no_valid_baseflow"}
        )
    except (TypeError, ValueError):
        return False


def append_status(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.is_file() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=STATUS_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({key: record.get(key) for key in STATUS_FIELDS})


def run(project_root: Path, stage_root: Path | None, limit: int | None, dry_run: bool) -> None:
    inventory_path, data_root, run_root = paths(project_root, stage_root)
    stations = inventory_rows(inventory_path)
    if limit is not None and limit < 1:
        raise ValueError("--limit must be positive")
    todo: list[tuple[str, str, float]] = []
    complete = ambiguous = missing = 0
    for site, state, action in stations:
        folder = data_root / state / site
        selection = read_json(folder / "downloaded_data" / "stage_source_selection.json")
        if selection.get("status") != "selected":
            ambiguous += 1
            continue
        inp = folder / "downloaded_data" / "stage_hydroeventdetector.parquet"
        if not inp.is_file():
            missing += 1
            continue
        if valid_completed(folder / "processed_data", site, action):
            complete += 1
            continue
        todo.append((site, state, action))

    scheduled = todo[:limit] if limit is not None else todo
    print("=" * 72, flush=True)
    print("ACTION STAGE AND PEAKS — NATIONAL PROCESSING", flush=True)
    print(f"Inventory: {inventory_path}", flush=True)
    print(f"Data root: {data_root}", flush=True)
    print(f"Stations: {len(stations)} | completed: {complete} | excluded: {ambiguous} | missing input: {missing}", flush=True)
    print(f"Pending: {len(todo)} | scheduled: {len(scheduled)} | dry-run: {dry_run}", flush=True)
    print(f"Top N: {TOP_N} | alpha: {ALPHA} | ranking: {RANKING} | window: {WINDOW_WIDTH_FT} ft", flush=True)
    if dry_run:
        for site, state, _ in scheduled[:20]:
            print(f"[WOULD PROCESS] {state} {site}", flush=True)
        return

    run_root.mkdir(parents=True, exist_ok=True)
    status_csv = run_root / "processing_status.csv"
    errors_csv = run_root / "errors.csv"
    success = failed = 0
    for i, (site, state, action) in enumerate(scheduled, 1):
        t0 = time.monotonic()
        station_dir = data_root / state / site
        input_path = station_dir / "downloaded_data" / "stage_hydroeventdetector.parquet"
        output_dir = station_dir / "processed_data"
        # A staging folder keeps interrupted/failed runs from being mistaken for complete.
        staging = station_dir / "processed_data.in_progress"
        record: dict[str, Any] = {"timestamp_utc": utc_now(), "state": state, "site_id": site}
        print(f"[START {i}/{len(scheduled)}] {state} {site}", flush=True)
        try:
            if staging.exists():
                shutil.rmtree(staging)
            result = detect_stage_events(
                input_path=input_path,
                output_directory=staging,
                site_id=site,
                baseflow_alpha=ALPHA,
                top_n_events=TOP_N,
                ranking_method=RANKING,
                event_filter_percentile=None,
                action_stage_ft=action,
                action_window_width_ft=WINDOW_WIDTH_FT,
            )
            analysis = read_json(staging / "stage_action_analysis.json")
            if not analysis or not all((staging / n).is_file() for n in OUTPUT_FILES):
                raise RuntimeError("Missing expected processing outputs")
            # The manifest's output path is recorded by the detector; update it
            # to the permanent location before promoting the staging directory.
            manifest_path = staging / "event_detection_manifest.json"
            manifest = read_json(manifest_path)
            manifest["output_directory"] = str(output_dir)
            manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
            if output_dir.exists():
                # Remove only previous derived outputs, never downloaded_data.
                shutil.rmtree(output_dir)
            staging.rename(output_dir)
            if not valid_completed(output_dir, site, action):
                raise RuntimeError("Post-write validation failed")
            record.update(status="OK", detected_events=result.number_of_detected_events,
                          selected_events=result.number_of_selected_events,
                          median_window_baseflow_ft=analysis.get("median_window_baseflow_ft"),
                          action_minus_median_window_ft=analysis.get("action_minus_median_window_ft"),
                          detail="")
            success += 1
            print(f"[DONE] {state} {site} | detected={result.number_of_detected_events} "
                  f"selected={result.number_of_selected_events} | {time.monotonic()-t0:.1f}s", flush=True)
        except Exception as exc:
            failed += 1
            record.update(status="ERROR", detail=f"{type(exc).__name__}: {exc}")
            print(f"[ERROR] {state} {site}: {record['detail']}", flush=True)
            append_status(errors_csv, {**record, "elapsed_seconds": round(time.monotonic()-t0, 2)})
        record["elapsed_seconds"] = round(time.monotonic() - t0, 2)
        append_status(status_csv, record)
        (run_root / "run_summary.json").write_text(json.dumps({
            "updated_at_utc": utc_now(), "inventory": str(inventory_path),
            "scheduled": len(scheduled), "successful_this_run": success,
            "errors_this_run": failed, "already_complete_at_start": complete,
            "ambiguous_or_unselected": ambiguous, "missing_input": missing,
            "top_n_events": TOP_N, "window_width_ft": WINDOW_WIDTH_FT,
        }, indent=2), encoding="utf-8")
    print(f"FINISHED | successful={success} | errors={failed} | status={status_csv}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--stage-root", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.project_root.expanduser().resolve(),
        args.stage_root.expanduser().resolve() if args.stage_root else None,
        args.limit, args.dry_run)


if __name__ == "__main__":
    main()
