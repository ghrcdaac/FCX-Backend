#!/usr/bin/env python3
"""
generate_dow_surface_obs_czml.py

Generate animated Cesium CZML for DOW surface observation CSV files.

This follows the same folder pattern as the DOW 3D tiles generator:

  public/DOW/Nov18/data
  public/DOW/Nov18/output

  public/DOW/Nov19/data
  public/DOW/Nov19/output

Input example:
  Mesonet-20221118-IOP02-DOW7-LEE-QC.csv

Output example:
  public/DOW/Nov18/output/dow7_surface_obs.czml

Examples:

Nov18 / IOP02:
python generate_dow_surface_obs_czml.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov18/data/Mesonet-20221118-IOP02-DOW7-LEE-QC.csv \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov18/output \
  --name dow7_surface_obs \
  --title "DOW7 Surface Observations Nov18 IOP02" \
  --start-time 2022-11-18T18:58:00Z \
  --end-time 2022-11-19T06:31:00Z

Nov19:
python generate_dow_surface_obs_czml.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov19/data/Mesonet-20221119-IOP03-DOW7-LEE-QC.csv \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov19/output \
  --name dow7_surface_obs \
  --title "DOW7 Surface Observations Nov19" \
  --start-time 2022-11-19T00:00:00Z \
  --end-time 2022-11-19T23:59:59Z
"""

import argparse
import json
import re
from pathlib import Path

import pandas as pd


NUMERIC_COLS = [
    "Temperature",
    "Relative_Humidity",
    "Pressure",
    "Latitude",
    "Longitude",
    "Altitude",
    "Bld_Wnd_Spd_Corr",
    "Bld_Wnd_Dir_Corr",
    "3-sec_Bld_Wnd_Spd_Corr",
    "3-sec_Bld_Wnd_Dir_Corr",
    "1-min_Bld_Wnd_Spd_Corr",
    "1-min_Bld_Wnd_Dir_Corr",
    "Corr_Pressure",
]


def safe_slug(value):
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower() or "dow7_surface_obs"


def resolve_output_path(out_arg, name):
    out = Path(out_arg)

    if out.suffix.lower() == ".czml":
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    out.mkdir(parents=True, exist_ok=True)
    return out / f"{safe_slug(name)}.czml"


def iso_z(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat().replace("+00:00", "Z")


def safe_fmt(row, col, fmt=".2f", fallback="N/A"):
    if col not in row or pd.isna(row[col]):
        return fallback
    try:
        return format(float(row[col]), fmt)
    except Exception:
        return fallback


def read_surface_obs_csv(input_path, skiprows=5):
    df = pd.read_csv(input_path, skiprows=skiprows)

    # Remove units row: date_timeUTC, deg_C, percent, etc.
    if len(df) > 0:
        df = df.iloc[1:].copy()

    if "Time" not in df.columns:
        raise RuntimeError(
            f"Input CSV does not contain a Time column. Columns: {list(df.columns)}"
        )

    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")

    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    required = ["Time", "Latitude", "Longitude"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Columns: {list(df.columns)}")

    df = df.dropna(subset=["Time", "Latitude", "Longitude"])
    df = df.sort_values("Time")

    return df


def build_description(row, title):
    return f"""
<table>
<tr><td><b>Dataset</b></td><td>{title}</td></tr>
<tr><td><b>Time</b></td><td>{iso_z(row["Time"])}</td></tr>
<tr><td><b>Temperature</b></td><td>{safe_fmt(row, "Temperature")} °C</td></tr>
<tr><td><b>Relative Humidity</b></td><td>{safe_fmt(row, "Relative_Humidity")} %</td></tr>
<tr><td><b>Pressure</b></td><td>{safe_fmt(row, "Corr_Pressure")} mbar</td></tr>
<tr><td><b>Raw Pressure</b></td><td>{safe_fmt(row, "Pressure")} mbar</td></tr>
<tr><td><b>Wind Speed</b></td><td>{safe_fmt(row, "1-min_Bld_Wnd_Spd_Corr")} m/s</td></tr>
<tr><td><b>Wind Direction</b></td><td>{safe_fmt(row, "1-min_Bld_Wnd_Dir_Corr", ".1f")}°</td></tr>
<tr><td><b>3-sec Wind Speed</b></td><td>{safe_fmt(row, "3-sec_Bld_Wnd_Spd_Corr")} m/s</td></tr>
<tr><td><b>3-sec Wind Direction</b></td><td>{safe_fmt(row, "3-sec_Bld_Wnd_Dir_Corr", ".1f")}°</td></tr>
<tr><td><b>Latitude</b></td><td>{safe_fmt(row, "Latitude", ".6f")}</td></tr>
<tr><td><b>Longitude</b></td><td>{safe_fmt(row, "Longitude", ".6f")}</td></tr>
<tr><td><b>Altitude</b></td><td>{safe_fmt(row, "Altitude")} m</td></tr>
</table>
"""


def build_czml(df, name, title, clock_multiplier, default_altitude):
    if df.empty:
        raise RuntimeError("No rows available for CZML generation.")

    start = df["Time"].iloc[0]
    end = df["Time"].iloc[-1]
    epoch = start

    positions = []

    for _, r in df.iterrows():
        sec = (r["Time"] - epoch).total_seconds()

        if "Altitude" in df.columns and pd.notna(r.get("Altitude")):
            alt = float(r["Altitude"])
        else:
            alt = float(default_altitude)

        positions += [
            float(sec),
            float(r["Longitude"]),
            float(r["Latitude"]),
            alt,
        ]

    latest = df.iloc[-1]
    description = build_description(latest, title)

    interval = f"{iso_z(start)}/{iso_z(end)}"

    czml = [
        {
            "id": "document",
            "name": title,
            "version": "1.0",
            "clock": {
                "interval": interval,
                "currentTime": iso_z(start),
                "multiplier": clock_multiplier,
                "range": "LOOP_STOP",
                "step": "SYSTEM_CLOCK_MULTIPLIER",
            },
        },
        {
            "id": safe_slug(name),
            "name": title,
            "availability": interval,
            "description": description,
            "position": {
                "epoch": iso_z(epoch),
                "cartographicDegrees": positions,
                "interpolationAlgorithm": "LINEAR",
                "interpolationDegree": 1,
            },
            "point": {
                "pixelSize": 16,
                "color": {"rgba": [255, 255, 0, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 2,
                "disableDepthTestDistance": 1000000000,
            },
            "label": {
                "text": title,
                "font": "16px sans-serif",
                "fillColor": {"rgba": [255, 255, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 3,
                "style": "FILL_AND_OUTLINE",
                "pixelOffset": {"cartesian2": [0, -35]},
                "disableDepthTestDistance": 1000000000,
            },
            "path": {
                "show": True,
                "width": 4,
                "material": {
                    "solidColor": {
                        "color": {"rgba": [255, 255, 0, 220]}
                    }
                },
            },
        },
    ]

    return czml, start, end


def main():
    parser = argparse.ArgumentParser(
        description="Generate animated CZML for DOW surface observation CSV files."
    )

    parser.add_argument("input", help="Input DOW surface observation CSV file.")
    parser.add_argument(
        "--out",
        required=True,
        help="Output .czml path or output folder. If folder, writes <name>.czml.",
    )
    parser.add_argument(
        "--name",
        default="dow7_surface_obs",
        help="Output/entity name. Default: dow7_surface_obs.",
    )
    parser.add_argument(
        "--title",
        default="DOW7 Surface Observations",
        help="Display title in CZML.",
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="Optional UTC start filter, e.g. 2022-11-18T18:58:00Z.",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="Optional UTC end filter, e.g. 2022-11-19T06:31:00Z.",
    )
    parser.add_argument(
        "--downsample",
        type=int,
        default=10,
        help="Use every Nth row for browser performance. Default: 10.",
    )
    parser.add_argument(
        "--skiprows",
        type=int,
        default=5,
        help="Rows to skip before CSV header. Default: 5.",
    )
    parser.add_argument(
        "--clock-multiplier",
        type=int,
        default=60,
        help="Cesium clock multiplier. Default: 60.",
    )
    parser.add_argument(
        "--default-altitude",
        type=float,
        default=95.0,
        help="Fallback altitude when Altitude column is missing. Default: 95.",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    out_path = resolve_output_path(args.out, args.name)

    df = read_surface_obs_csv(input_path, skiprows=args.skiprows)

    if args.start_time:
        start_filter = pd.Timestamp(args.start_time)
        if start_filter.tzinfo is None:
            start_filter = start_filter.tz_localize("UTC")
        else:
            start_filter = start_filter.tz_convert("UTC")
        df = df[df["Time"] >= start_filter].copy()

    if args.end_time:
        end_filter = pd.Timestamp(args.end_time)
        if end_filter.tzinfo is None:
            end_filter = end_filter.tz_localize("UTC")
        else:
            end_filter = end_filter.tz_convert("UTC")
        df = df[df["Time"] <= end_filter].copy()

    if args.downsample and args.downsample > 1:
        df = df.iloc[::args.downsample].copy()

    if df.empty:
        raise RuntimeError("No rows left after filtering. Check input file/time range.")

    czml, start, end = build_czml(
        df=df,
        name=args.name,
        title=args.title,
        clock_multiplier=args.clock_multiplier,
        default_altitude=args.default_altitude,
    )

    out_path.write_text(json.dumps(czml, indent=2), encoding="utf-8")

    print("Wrote:", out_path)
    print("Input:", input_path)
    print("Rows used:", len(df))
    print("Start:", iso_z(start))
    print("End:", iso_z(end))
    print("Lat range:", float(df["Latitude"].min()), float(df["Latitude"].max()))
    print("Lon range:", float(df["Longitude"].min()), float(df["Longitude"].max()))


if __name__ == "__main__":
    main()
