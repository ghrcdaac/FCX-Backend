#!/usr/bin/env python3

import argparse
import json
import math
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


# ------------------------------------------------------------
# efmlib import helper
# ------------------------------------------------------------

def add_efmlib_to_path(efmlib_path):
    efmlib_path = Path(efmlib_path).resolve()
    print("Using efmlib path:", efmlib_path)

    if not efmlib_path.exists():
        raise FileNotFoundError(f"efmlib path does not exist: {efmlib_path}")

    sys.path.insert(0, str(efmlib_path))


# ------------------------------------------------------------
# Filename helpers
# ------------------------------------------------------------

def parse_start_time(path):
    """
    Parse launch time from filename:
      EFM_20221118_2258_IOP2_WestSmithville_RUST
    """
    match = re.search(r"EFM_(\d{8})_(\d{4})", path.name)

    if not match:
        raise ValueError(f"Cannot parse start time from filename: {path.name}")

    return datetime.strptime(
        match.group(1) + match.group(2),
        "%Y%m%d%H%M",
    ).replace(tzinfo=timezone.utc)


def clean_name(path):
    name = path.name.replace("(1)", "")
    name = re.sub(r"[^A-Za-z0-9_\-]+", "_", name)
    return name.strip("_")


def short_flight_label(flight_name):
    """
    Convert:
      EFM_20221119_0252_IOP2_WestSmithville_GRAUPEL
    into:
      GRAUPEL 02:52
    """
    parts = flight_name.split("_")

    try:
        time_part = parts[2]
        hh = time_part[:2]
        mm = time_part[2:]
        instrument = parts[-1]
        return f"{instrument} {hh}:{mm}"
    except Exception:
        return flight_name


def make_clean_point_label(label, flight_name, alt):
    instrument = flight_name.split("_")[-1]

    if label == "Max Altitude":
        return f"{instrument} Max {alt:.0f} m"

    if label == "Launch":
        return f"{instrument} Launch"

    if label == "End":
        return f"{instrument} End"

    return f"{instrument} {label}"


# ------------------------------------------------------------
# Cleaning / sampling
# ------------------------------------------------------------

def clean_gps(df_gps):
    gps = df_gps.copy()

    required_cols = ["millis", "latitude", "longitude", "altitude"]
    missing = [c for c in required_cols if c not in gps.columns]

    if missing:
        raise ValueError(f"GPS dataframe is missing columns: {missing}")

    gps = gps[required_cols].copy()
    gps = gps.replace([np.inf, -np.inf], np.nan)
    gps = gps.dropna()

    gps = gps[
        gps["latitude"].between(-90, 90)
        & gps["longitude"].between(-180, 180)
        & gps["altitude"].between(-500, 30000)
    ]

    gps = gps.sort_values("millis").drop_duplicates("millis")

    gps["dt_s"] = gps["millis"].diff() / 1000.0
    gps["dlat"] = gps["latitude"].diff().abs()
    gps["dlon"] = gps["longitude"].diff().abs()
    gps["dalt"] = gps["altitude"].diff().abs()

    gps = gps[
        gps["dt_s"].isna()
        | (
            (gps["dt_s"] > 0)
            & (gps["dt_s"] < 60)
            & (gps["dlat"] < 0.1)
            & (gps["dlon"] < 0.1)
            & (gps["dalt"] < 3000)
        )
    ]

    gps = gps.drop(columns=["dt_s", "dlat", "dlon", "dalt"])
    gps = gps.reset_index(drop=True)

    return gps


def add_time_columns(gps, launch_time):
    gps = gps.copy()

    t0 = float(gps["millis"].iloc[0])
    gps["elapsed_s"] = (gps["millis"] - t0) / 1000.0

    gps["time_utc"] = gps["elapsed_s"].apply(
        lambda s: (launch_time + timedelta(seconds=float(s)))
        .isoformat()
        .replace("+00:00", "Z")
    )

    return gps


def trim_to_active_flight(
    gps,
    launch_altitude_buffer_m=40,
    pad_seconds=120,
    min_active_duration_s=60,
):
    """
    Remove long pre-launch and post-landing/stationary GPS records.

    Raw EFM files can include:
      - pre-launch instrument-on GPS records
      - actual balloon flight
      - long post-landing GPS records

    This trims to the useful active-flight time window.

    Logic:
      1. Estimate launch altitude from first 30 GPS rows.
      2. Identify rows where altitude is above launch altitude + buffer.
      3. Keep from first active row to last active row.
      4. Add small padding before/after.
    """
    if gps.empty or len(gps) < 20:
        return gps

    gps = gps.sort_values("elapsed_s").reset_index(drop=True).copy()

    launch_alt = float(gps["altitude"].head(30).median())
    threshold_alt = launch_alt + launch_altitude_buffer_m

    active = gps[gps["altitude"] > threshold_alt]

    if active.empty:
        print("No active altitude window found. Keeping full GPS.")
        return gps

    start_s = max(0.0, float(active["elapsed_s"].iloc[0]) - pad_seconds)
    end_s = float(active["elapsed_s"].iloc[-1]) + pad_seconds

    if end_s - start_s < min_active_duration_s:
        print("Active window too short. Keeping full GPS.")
        return gps

    trimmed = gps[
        (gps["elapsed_s"] >= start_s)
        & (gps["elapsed_s"] <= end_s)
    ].copy()

    trimmed = trimmed.reset_index(drop=True)

    print(
        f"Trim active flight: {len(gps)} rows -> {len(trimmed)} rows | "
        f"launch_alt={launch_alt:.1f} m | "
        f"threshold={threshold_alt:.1f} m | "
        f"window={start_s:.1f}s to {end_s:.1f}s"
    )

    return trimmed


def downsample_gps(gps, sample_seconds):
    gps = gps.copy()

    gps["sample_bin"] = (gps["elapsed_s"] // sample_seconds).astype(int)
    samples = gps.groupby("sample_bin", as_index=False).first()
    samples = samples.drop(columns=["sample_bin"])

    return samples.reset_index(drop=True)


def clean_fiber(df_fiber, max_millis):
    """
    Keep useful quick-look fields from df_fiber.

    adc_volts is raw quick-look voltage.
    It is NOT final science-grade electric field.
    """
    if df_fiber is None or df_fiber.empty:
        return pd.DataFrame()

    fiber = df_fiber.copy()
    fiber = fiber.replace([np.inf, -np.inf], np.nan)

    if "adc_ready_millis" not in fiber.columns:
        print("Warning: df_fiber does not contain adc_ready_millis. Skipping sensor merge.")
        return pd.DataFrame()

    keep_cols = [
        "adc_ready_millis",
        "adc_reading",
        "adc_volts",
        "adc_volts_withlag",
        "temperature",
        "relative_humidity",
        "pressure",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "magnetometer_x",
        "magnetometer_y",
        "magnetometer_z",
        "gyroscope_x",
        "gyroscope_y",
        "gyroscope_z",
    ]

    cols = [c for c in keep_cols if c in fiber.columns]
    fiber = fiber[cols].copy()

    fiber = fiber.dropna(subset=["adc_ready_millis"])

    fiber = fiber[
        (fiber["adc_ready_millis"] >= 0)
        & (fiber["adc_ready_millis"] <= max_millis)
    ]

    if "temperature" in fiber.columns:
        fiber.loc[~fiber["temperature"].between(-100, 80), "temperature"] = np.nan

    if "relative_humidity" in fiber.columns:
        fiber.loc[~fiber["relative_humidity"].between(0, 100), "relative_humidity"] = np.nan

    if "pressure" in fiber.columns:
        fiber.loc[~fiber["pressure"].between(50, 1100), "pressure"] = np.nan

    fiber = fiber.sort_values("adc_ready_millis").reset_index(drop=True)

    return fiber


def merge_fiber_to_samples(samples, fiber, tolerance_ms=1500):
    """
    Merge closest sensor packet onto each GPS sample.
    """
    if samples.empty or fiber.empty:
        return samples

    left = samples.sort_values("millis").copy()
    right = fiber.sort_values("adc_ready_millis").copy()

    merged = pd.merge_asof(
        left,
        right,
        left_on="millis",
        right_on="adc_ready_millis",
        direction="nearest",
        tolerance=tolerance_ms,
    )

    return merged.reset_index(drop=True)


# ------------------------------------------------------------
# Color helpers
# ------------------------------------------------------------

def safe_float(value):
    try:
        if value is None:
            return None

        if pd.isna(value):
            return None

        value = float(value)

        if not math.isfinite(value):
            return None

        return value
    except Exception:
        return None


def value_range(df, field):
    if field not in df.columns:
        return None, None

    values = pd.to_numeric(df[field], errors="coerce").dropna()

    if len(values) < 2:
        return None, None

    vmin = float(values.quantile(0.05))
    vmax = float(values.quantile(0.95))

    if not math.isfinite(vmin) or not math.isfinite(vmax) or vmax <= vmin:
        vmin = float(values.min())
        vmax = float(values.max())

    if vmax <= vmin:
        return None, None

    return vmin, vmax


def color_ramp(value, vmin, vmax):
    """
    Blue -> cyan -> yellow -> red.
    """
    value = safe_float(value)

    if value is None or vmin is None or vmax is None or vmax <= vmin:
        return [180, 180, 180, 180]

    x = (value - vmin) / (vmax - vmin)
    x = max(0.0, min(1.0, x))

    if x < 0.33:
        t = x / 0.33
        r = 40
        g = int(100 + 140 * t)
        b = 255
    elif x < 0.66:
        t = (x - 0.33) / 0.33
        r = int(40 + 215 * t)
        g = 240
        b = int(255 - 215 * t)
    else:
        t = (x - 0.66) / 0.34
        r = 255
        g = int(240 - 190 * t)
        b = 40

    return [r, g, b, 230]


# ------------------------------------------------------------
# CZML helpers
# ------------------------------------------------------------

def html_value(value, digits=3):
    value = safe_float(value)

    if value is None:
        return ""

    return f"{value:.{digits}f}"


def make_description(flight_name, row, color_by):
    lat = safe_float(row.get("latitude"))
    lon = safe_float(row.get("longitude"))
    alt = safe_float(row.get("altitude"))

    rows = [
        ("Flight", flight_name),
        ("Time UTC", row.get("time_utc")),
        ("Latitude", f"{lat:.6f}" if lat is not None else ""),
        ("Longitude", f"{lon:.6f}" if lon is not None else ""),
        ("Altitude m", f"{alt:.1f}" if alt is not None else ""),
        ("Color by", color_by),
        ("Raw ADC volts", html_value(row.get("adc_volts"), 6)),
        ("Temperature C", html_value(row.get("temperature"), 2)),
        ("Relative humidity %", html_value(row.get("relative_humidity"), 2)),
        ("Pressure hPa", html_value(row.get("pressure"), 2)),
        ("Accel X", html_value(row.get("acceleration_x"), 3)),
        ("Accel Y", html_value(row.get("acceleration_y"), 3)),
        ("Accel Z", html_value(row.get("acceleration_z"), 3)),
        ("Gyro X", html_value(row.get("gyroscope_x"), 3)),
        ("Gyro Y", html_value(row.get("gyroscope_y"), 3)),
        ("Gyro Z", html_value(row.get("gyroscope_z"), 3)),
    ]

    html = f"<h3>{short_flight_label(flight_name)}</h3>"
    html += "<table style='border-collapse: collapse;'>"

    for key, value in rows:
        if value is None or value == "":
            continue

        html += (
            "<tr>"
            f"<th style='text-align:left;padding:3px 8px;border-bottom:1px solid #555;'>{key}</th>"
            f"<td style='padding:3px 8px;border-bottom:1px solid #555;'>{value}</td>"
            "</tr>"
        )

    html += "</table>"
    html += (
        "<p><b>Note:</b> adc_volts is quick-look raw sensor voltage, "
        "not final QC electric-field.</p>"
    )

    return html


def make_positions_property(df):
    start_time = df["time_utc"].iloc[0]
    first_elapsed = float(df["elapsed_s"].iloc[0])

    positions = []

    for _, row in df.iterrows():
        seconds = float(row["elapsed_s"] - first_elapsed)
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        alt = float(row["altitude"])

        positions.extend([
            round(seconds, 3),
            lon,
            lat,
            alt,
        ])

    return {
        "epoch": start_time,
        "interpolationAlgorithm": "LINEAR",
        "interpolationDegree": 1,
        "cartographicDegrees": positions,
    }


def make_static_path_positions(df, max_path_points=900):
    """
    Static full-track polyline positions.

    Uses fewer path points to avoid dense zig-zag visual clutter.
    """
    positions = []

    df = df.sort_values("elapsed_s").reset_index(drop=True)

    step = max(1, math.ceil(len(df) / max_path_points))
    path_df = df.iloc[::step].copy()

    if len(path_df) > 0 and path_df.index[-1] != df.index[-1]:
        path_df = pd.concat([path_df, df.iloc[[-1]]])

    for _, row in path_df.iterrows():
        positions.extend([
            float(row["longitude"]),
            float(row["latitude"]),
            float(row["altitude"]),
        ])

    return positions


def make_czml(flight_name, df, color_by="altitude", max_point_entities=7000):
    """
    Enhanced EFM Cesium CZML.

    Important:
      - Full line is a separate static polyline entity.
      - Moving balloon path is disabled to avoid disappearing/flickering.
      - GPS is already trimmed before this function.
    """
    if df.empty:
        return []

    start_time = df["time_utc"].iloc[0]
    end_time = df["time_utc"].iloc[-1]
    interval = f"{start_time}/{end_time}"

    if color_by not in df.columns:
        print(f"Warning: color field '{color_by}' not available. Falling back to altitude.")
        color_by = "altitude"

    color_min, color_max = value_range(df, color_by)

    alt_min = float(df["altitude"].min())
    alt_max = float(df["altitude"].max())

    positions_property = make_positions_property(df)
    static_path_positions = make_static_path_positions(df)

    czml = [
        {
            "id": "document",
            "name": f"{short_flight_label(flight_name)} Enhanced EFM Balloon Track",
            "version": "1.0",
            "clock": {
                "interval": interval,
                "currentTime": start_time,
                "multiplier": 60,
                "range": "LOOP_STOP",
                "step": "SYSTEM_CLOCK_MULTIPLIER",
            },
        },
        {
            "id": f"{flight_name}_static_path",
            "name": f"{short_flight_label(flight_name)} Full Track Line",
            "polyline": {
                "positions": {
                    "cartographicDegrees": static_path_positions,
                },
                "width": 4,
                "material": {
                    "polylineGlow": {
                        "color": {
                            "rgba": [255, 210, 60, 255],
                        },
                        "glowPower": 0.22,
                    },
                },
                "clampToGround": False,
            },
            "description": (
                f"<h3>{short_flight_label(flight_name)} Full Track</h3>"
                "<p>Static full balloon trajectory line.</p>"
            ),
        },
        {
            "id": f"{flight_name}_moving_balloon",
            "name": f"{short_flight_label(flight_name)} Moving Balloon",
            "availability": interval,
            "position": positions_property,
            "point": {
                "pixelSize": 16,
                "color": {
                    "rgba": [255, 255, 255, 255],
                },
                "outlineColor": {
                    "rgba": [0, 0, 0, 255],
                },
                "outlineWidth": 3,
                "heightReference": "NONE",
            },
            "path": {
                "show": False,
                "width": 3,
                "leadTime": 0,
                "trailTime": 1800,
                "resolution": 5,
                "material": {
                    "polylineGlow": {
                        "color": {
                            "rgba": [255, 255, 255, 180],
                        },
                        "glowPower": 0.15,
                    },
                },
            },
            "label": {
                "text": short_flight_label(flight_name),
                "font": "12pt sans-serif",
                "style": "FILL_AND_OUTLINE",
                "fillColor": {
                    "rgba": [255, 255, 255, 230],
                },
                "outlineColor": {
                    "rgba": [0, 0, 0, 255],
                },
                "outlineWidth": 2,
                "pixelOffset": {
                    "cartesian2": [0, -26],
                },
                "scale": 0.65,
                "show": False,
            },
            "description": (
                f"<h3>{short_flight_label(flight_name)}</h3>"
                f"<p><b>Altitude range:</b> {alt_min:.1f} m to {alt_max:.1f} m</p>"
                f"<p><b>Colored by:</b> {color_by}</p>"
                "<p>This is a time-dynamic EFM balloon marker.</p>"
                "<p><b>Note:</b> adc_volts is raw quick-look voltage, not final QC electric field.</p>"
            ),
        },
    ]

    point_step = max(1, math.ceil(len(df) / max_point_entities))

    for i, row in df.iloc[::point_step].iterrows():
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        alt = float(row["altitude"])

        color_value = row.get(color_by)
        color = color_ramp(color_value, color_min, color_max)

        pixel_size = 6
        if alt_max > alt_min:
            alt_norm = (alt - alt_min) / (alt_max - alt_min)
            pixel_size = int(5 + 5 * alt_norm)

        czml.append(
            {
                "id": f"{flight_name}_point_{i:05d}",
                "name": f"{short_flight_label(flight_name)} sample",
                "availability": interval,
                "position": {
                    "cartographicDegrees": [lon, lat, alt],
                },
                "point": {
                    "pixelSize": pixel_size,
                    "color": {
                        "rgba": color,
                    },
                    "outlineColor": {
                        "rgba": [0, 0, 0, 180],
                    },
                    "outlineWidth": 1,
                    "heightReference": "NONE",
                },
                "description": make_description(flight_name, row, color_by),
            }
        )

    first = df.iloc[0]
    last = df.iloc[-1]
    max_row = df.loc[df["altitude"].idxmax()]

    label_points = [
        ("Launch", first, [80, 255, 120, 255]),
        ("End", last, [255, 120, 120, 255]),
        ("Max Altitude", max_row, [255, 255, 80, 255]),
    ]

    for label, row, color in label_points:
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        alt = float(row["altitude"])

        label_show = label == "Max Altitude"

        czml.append(
            {
                "id": f"{flight_name}_{label.replace(' ', '_')}",
                "name": f"{short_flight_label(flight_name)} {label}",
                "position": {
                    "cartographicDegrees": [lon, lat, alt],
                },
                "point": {
                    "pixelSize": 15,
                    "color": {
                        "rgba": color,
                    },
                    "outlineColor": {
                        "rgba": [0, 0, 0, 255],
                    },
                    "outlineWidth": 2,
                    "heightReference": "NONE",
                },
                "label": {
                    "text": make_clean_point_label(label, flight_name, alt),
                    "font": "11pt sans-serif",
                    "style": "FILL_AND_OUTLINE",
                    "fillColor": {
                        "rgba": [255, 255, 255, 230],
                    },
                    "outlineColor": {
                        "rgba": [0, 0, 0, 255],
                    },
                    "outlineWidth": 2,
                    "pixelOffset": {
                        "cartesian2": [0, -30],
                    },
                    "scale": 0.65,
                    "show": label_show,
                    "distanceDisplayCondition": {
                        "distanceDisplayCondition": [0, 180000],
                    },
                },
                "description": make_description(flight_name, row, color_by),
            }
        )

    # Sparse vertical drop lines.
    drop_count_target = 20
    drop_step = max(1, math.ceil(len(df) / drop_count_target))

    for i, row in df.iloc[::drop_step].iterrows():
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        alt = float(row["altitude"])
        color = color_ramp(row.get(color_by), color_min, color_max)

        czml.append(
            {
                "id": f"{flight_name}_drop_line_{i:05d}",
                "name": f"{short_flight_label(flight_name)} altitude drop line",
                "polyline": {
                    "positions": {
                        "cartographicDegrees": [
                            lon, lat, 0,
                            lon, lat, alt,
                        ],
                    },
                    "width": 1,
                    "material": {
                        "solidColor": {
                            "color": {
                                "rgba": [color[0], color[1], color[2], 80],
                            },
                        },
                    },
                    "distanceDisplayCondition": {
                        "distanceDisplayCondition": [0, 220000],
                    },
                },
            }
        )

    return czml


# ------------------------------------------------------------
# Processing
# ------------------------------------------------------------

def process_file(
    raw_path,
    out_dir,
    sample_seconds,
    color_by,
    efmlib,
    trim_flight=True,
    launch_altitude_buffer_m=40,
    trim_pad_seconds=120,
):
    raw_path = Path(raw_path)

    print("\nProcessing:", raw_path)
    print("Exists:", raw_path.exists())

    if not raw_path.exists():
        raise FileNotFoundError(raw_path)

    flight_name = clean_name(raw_path)
    launch_time = parse_start_time(raw_path)

    df_gps, df_fiber = efmlib.io.read_efm_raw([str(raw_path)])

    print("GPS rows raw:", len(df_gps))
    print("GPS columns:", list(df_gps.columns))
    print("Fiber rows raw:", len(df_fiber))
    print("Fiber columns:", list(df_fiber.columns)[:30])

    if df_gps.empty:
        raise RuntimeError(f"No GPS rows found for {raw_path}")

    gps = clean_gps(df_gps)
    gps = add_time_columns(gps, launch_time)

    if trim_flight:
        gps = trim_to_active_flight(
            gps,
            launch_altitude_buffer_m=launch_altitude_buffer_m,
            pad_seconds=trim_pad_seconds,
        )

    max_millis = int(gps["millis"].max() + 10000)

    fiber = clean_fiber(df_fiber, max_millis=max_millis)

    samples = downsample_gps(gps, sample_seconds=sample_seconds)
    samples = merge_fiber_to_samples(samples, fiber, tolerance_ms=1500)

    out_dir.mkdir(parents=True, exist_ok=True)

    gps_csv = out_dir / f"{flight_name}_gps_clean.csv"
    sample_csv = out_dir / f"{flight_name}_cesium_samples.csv"
    czml_file = out_dir / f"{flight_name}_track.czml"

    gps.to_csv(gps_csv, index=False)
    samples.to_csv(sample_csv, index=False)

    czml = make_czml(
        flight_name=flight_name,
        df=samples,
        color_by=color_by,
    )

    czml_file.write_text(json.dumps(czml, indent=2), encoding="utf-8")

    print("Wrote:", gps_csv)
    print("Wrote:", sample_csv)
    print("Wrote:", czml_file)

    return {
        "flight_name": flight_name,
        "input": str(raw_path),
        "gps_rows": int(len(gps)),
        "fiber_rows": int(len(fiber)),
        "cesium_sample_rows": int(len(samples)),
        "color_by": color_by,
        "trim_flight": trim_flight,
        "lat_min": float(gps["latitude"].min()),
        "lat_max": float(gps["latitude"].max()),
        "lon_min": float(gps["longitude"].min()),
        "lon_max": float(gps["longitude"].max()),
        "alt_min_m": float(gps["altitude"].min()),
        "alt_max_m": float(gps["altitude"].max()),
        "time_start": str(gps["time_utc"].iloc[0]),
        "time_end": str(gps["time_utc"].iloc[-1]),
        "outputs": {
            "gps_clean_csv": str(gps_csv),
            "cesium_samples_csv": str(sample_csv),
            "czml": str(czml_file),
        },
        "note": "adc_volts is raw quick-look sensor voltage, not final QC electric-field.",
    }


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract raw NSSL EFM files and create enhanced Cesium-ready CZML."
    )

    parser.add_argument(
        "inputs",
        nargs="+",
        help="Raw EFM flight files",
    )

    parser.add_argument(
        "--efmlib-path",
        required=True,
        help="Path to efmlib-main folder",
    )

    parser.add_argument(
        "--out-dir",
        default="./public/efm_cesium_outputs",
        help="Output directory. Put under public/ for Vite/Cesium.",
    )

    parser.add_argument(
        "--sample-seconds",
        type=int,
        default=10,
        help="Sample spacing in seconds for Cesium output.",
    )

    parser.add_argument(
        "--color-by",
        default="altitude",
        choices=[
            "altitude",
            "adc_volts",
            "adc_volts_withlag",
            "temperature",
            "relative_humidity",
            "pressure",
            "acceleration_x",
            "acceleration_y",
            "acceleration_z",
            "gyroscope_x",
            "gyroscope_y",
            "gyroscope_z",
        ],
        help="Variable used to color Cesium points.",
    )

    parser.add_argument(
        "--no-trim-flight",
        action="store_true",
        help="Disable trimming of pre-launch/post-landing stationary GPS records.",
    )

    parser.add_argument(
        "--launch-altitude-buffer-m",
        type=float,
        default=40,
        help="Altitude above launch altitude used to detect active flight.",
    )

    parser.add_argument(
        "--trim-pad-seconds",
        type=float,
        default=120,
        help="Seconds to keep before/after detected active flight.",
    )

    args = parser.parse_args()

    print("INPUTS:", args.inputs)
    print("OUT DIR:", args.out_dir)
    print("SAMPLE SECONDS:", args.sample_seconds)
    print("COLOR BY:", args.color_by)
    print("TRIM FLIGHT:", not args.no_trim_flight)

    add_efmlib_to_path(args.efmlib_path)

    import efmlib
    import efmlib.io

    print("efmlib loaded from:", efmlib.__file__)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summaries = []

    for input_file in args.inputs:
        summary = process_file(
            raw_path=input_file,
            out_dir=out_dir,
            sample_seconds=args.sample_seconds,
            color_by=args.color_by,
            efmlib=efmlib,
            trim_flight=not args.no_trim_flight,
            launch_altitude_buffer_m=args.launch_altitude_buffer_m,
            trim_pad_seconds=args.trim_pad_seconds,
        )
        summaries.append(summary)

    summary_file = out_dir / "summary.json"
    summary_file.write_text(json.dumps(summaries, indent=2), encoding="utf-8")

    print("\nDone.")
    print("Summary written:", summary_file)


if __name__ == "__main__":
    main()