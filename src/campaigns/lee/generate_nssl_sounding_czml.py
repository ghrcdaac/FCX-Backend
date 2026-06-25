#!/usr/bin/env python3
"""
generate_nssl_sounding_czml.py

Generate animated Cesium CZML for NSSL/MW41 sounding text files.

Input format expected:
Rows with 11 numeric columns:
PRES HGHT TEMP DWPT RELH MIXR DRCT SKNT THTA THTE THTV

Example:
python generate_nssl_sounding_czml.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/sounding/nssl/data/MW41_output_textFormat_20221118_225843.txt \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/sounding/nssl/output/nssl1_sounding_animated.czml \
  --launch-lon -76.026593 \
  --launch-lat 43.990895 \
  --launch-time 2022-11-18T22:58:43Z \
  --name nssl1

If --out is a folder, the script writes:
  <name>_sounding_animated.czml

If --name is not passed, it derives a default name from the input filename.
"""

import argparse
import json
import re
from pathlib import Path

import pandas as pd


COLS = [
    "PRES", "HGHT", "TEMP", "DWPT", "RELH", "MIXR",
    "DRCT", "SKNT", "THTA", "THTE", "THTV"
]


def safe_slug(value):
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower() or "sounding"


def default_name_from_input(input_path):
    name = Path(input_path).stem
    name = name.replace("MW41_output_textFormat_", "")
    return safe_slug(name)


def resolve_output_path(out_arg, name, input_path):
    out = Path(out_arg)

    if out.suffix.lower() == ".czml":
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    final_name = safe_slug(name) if name else default_name_from_input(input_path)
    out.mkdir(parents=True, exist_ok=True)
    return out / f"{final_name}_sounding_animated.czml"


def parse_launch_time(value):
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def read_sounding_text(input_path):
    rows = []

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.split()

            if len(parts) != 11:
                continue

            try:
                rows.append([float(x) for x in parts])
            except ValueError:
                continue

    if not rows:
        raise RuntimeError(f"No valid 11-column sounding rows found in: {input_path}")

    df = pd.DataFrame(rows, columns=COLS)
    df = df.replace([float("inf"), float("-inf")], pd.NA)
    df = df.dropna()
    df = df[df["HGHT"] >= 0].copy()

    if df.empty:
        raise RuntimeError(f"No valid sounding rows after cleaning: {input_path}")

    return df.reset_index(drop=True)


def temp_color(t):
    if t <= -20:
        return [80, 80, 255, 230]
    if t <= -10:
        return [0, 180, 255, 230]
    if t <= 0:
        return [0, 255, 180, 230]
    if t <= 10:
        return [255, 220, 0, 230]
    return [255, 80, 0, 230]


def wind_arrow(drct):
    arrows = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]
    idx = int(((float(drct) + 22.5) % 360) // 45)
    return arrows[idx]


def iso_z(ts):
    return ts.isoformat().replace("+00:00", "Z")


def build_description(row, t0):
    return f"""
    <table>
      <tr><td><b>Time</b></td><td>{iso_z(t0)}</td></tr>
      <tr><td><b>Pressure</b></td><td>{float(row["PRES"]):.1f} hPa</td></tr>
      <tr><td><b>Height</b></td><td>{float(row["HGHT"]):.1f} m</td></tr>
      <tr><td><b>Temperature</b></td><td>{float(row["TEMP"]):.1f} °C</td></tr>
      <tr><td><b>Dewpoint</b></td><td>{float(row["DWPT"]):.1f} °C</td></tr>
      <tr><td><b>RH</b></td><td>{float(row["RELH"]):.0f} %</td></tr>
      <tr><td><b>Wind</b></td><td>{float(row["DRCT"]):.0f}° / {float(row["SKNT"]):.0f} kt</td></tr>
      <tr><td><b>Mixing Ratio</b></td><td>{float(row["MIXR"]):.2f} g/kg</td></tr>
      <tr><td><b>Theta</b></td><td>{float(row["THTA"]):.1f} K</td></tr>
      <tr><td><b>Theta-E</b></td><td>{float(row["THTE"]):.1f} K</td></tr>
      <tr><td><b>Theta-V</b></td><td>{float(row["THTV"]):.1f} K</td></tr>
    </table>
    """


def make_czml(
    df,
    launch_lon,
    launch_lat,
    launch_time,
    name,
    title,
    launch_height_m,
    sample_every,
    point_every,
    wind_every,
    seconds_per_original_sample,
    clock_multiplier,
):
    df = df.iloc[::sample_every].copy().reset_index(drop=True)

    # Original script assumption:
    # one original sample per second. If sample_every=3, output spacing becomes 3 sec.
    df["seconds"] = df.index * sample_every * seconds_per_original_sample
    df["time"] = df["seconds"].apply(
        lambda s: launch_time + pd.Timedelta(seconds=float(s))
    )

    start = df["time"].iloc[0]
    end = df["time"].iloc[-1]
    interval = f"{iso_z(start)}/{iso_z(end)}"

    positions = []
    for _, r in df.iterrows():
        positions += [
            float(r["seconds"]),
            float(launch_lon),
            float(launch_lat),
            float(r["HGHT"]),
        ]

    final_name = safe_slug(name)
    display_title = title or f"{name.upper()} MW41 Sounding"

    czml = [
        {
            "id": "document",
            "name": display_title,
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
            "id": f"{final_name}_radiosonde_balloon",
            "name": "Animated Radiosonde Balloon",
            "availability": interval,
            "position": {
                "epoch": iso_z(start),
                "cartographicDegrees": positions,
                "interpolationAlgorithm": "LINEAR",
                "interpolationDegree": 1,
            },
            "point": {
                "pixelSize": 18,
                "color": {"rgba": [255, 255, 0, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 3,
                "disableDepthTestDistance": 1000000000,
            },
            "label": {
                "text": "Radiosonde",
                "font": "18px sans-serif",
                "fillColor": {"rgba": [255, 255, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 3,
                "style": "FILL_AND_OUTLINE",
                "pixelOffset": {"cartesian2": [0, -40]},
                "disableDepthTestDistance": 1000000000,
            },
            "path": {
                "show": True,
                "leadTime": 0,
                "trailTime": 100000,
                "width": 4,
                "resolution": 1,
                "material": {
                    "solidColor": {
                        "color": {"rgba": [255, 255, 255, 210]}
                    }
                },
            },
        },
        {
            "id": f"{final_name}_launch_site",
            "name": "Sounding Launch Site",
            "position": {
                "cartographicDegrees": [
                    float(launch_lon),
                    float(launch_lat),
                    float(launch_height_m),
                ]
            },
            "point": {
                "pixelSize": 16,
                "color": {"rgba": [255, 180, 0, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 2,
                "disableDepthTestDistance": 1000000000,
            },
            "label": {
                "text": "Launch Site",
                "font": "16px sans-serif",
                "fillColor": {"rgba": [255, 255, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 3,
                "style": "FILL_AND_OUTLINE",
                "pixelOffset": {"cartesian2": [0, -35]},
                "disableDepthTestDistance": 1000000000,
            },
        },
    ]

    # Temperature profile points.
    for idx, r in df.iloc[::point_every].iterrows():
        t0 = r["time"]
        h = float(r["HGHT"])
        temp = float(r["TEMP"])

        czml.append({
            "id": f"{final_name}_profile_temp_point_{idx}",
            "name": f"{h:.0f} m | {temp:.1f} °C",
            "availability": f"{iso_z(t0)}/{iso_z(end)}",
            "description": build_description(r, t0),
            "position": {
                "cartographicDegrees": [float(launch_lon), float(launch_lat), h]
            },
            "point": {
                "pixelSize": 8,
                "color": {"rgba": temp_color(temp)},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 1,
                "disableDepthTestDistance": 1000000000,
            },
        })

    # Wind arrows offset slightly east so they do not overlap profile.
    for idx, r in df.iloc[::wind_every].iterrows():
        t0 = r["time"]
        h = float(r["HGHT"])
        drct = float(r["DRCT"])
        sknt = float(r["SKNT"])

        czml.append({
            "id": f"{final_name}_wind_arrow_{idx}",
            "name": f"Wind {h:.0f} m",
            "availability": f"{iso_z(t0)}/{iso_z(end)}",
            "position": {
                "cartographicDegrees": [float(launch_lon) + 0.015, float(launch_lat), h]
            },
            "label": {
                "text": f"{wind_arrow(drct)} {sknt:.0f} kt",
                "font": "15px sans-serif",
                "fillColor": {"rgba": [120, 220, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 3,
                "style": "FILL_AND_OUTLINE",
                "disableDepthTestDistance": 1000000000,
            },
        })

    return czml, df, start, end


def main():
    parser = argparse.ArgumentParser(
        description="Generate animated Cesium CZML for NSSL/MW41 sounding text files."
    )

    parser.add_argument(
        "input",
        help="Input NSSL/MW41 sounding text file.",
    )

    parser.add_argument(
        "--out",
        required=True,
        help=(
            "Output .czml path or output folder. "
            "If folder, writes <name>_sounding_animated.czml."
        ),
    )

    parser.add_argument(
        "--name",
        default=None,
        help="Output/entity name prefix, e.g. nssl1. If omitted, derived from filename.",
    )

    parser.add_argument(
        "--title",
        default=None,
        help="CZML document display title.",
    )

    parser.add_argument(
        "--launch-lon",
        type=float,
        default=-76.026593,
        help="Launch longitude. Default: -76.026593.",
    )

    parser.add_argument(
        "--launch-lat",
        type=float,
        default=43.990895,
        help="Launch latitude. Default: 43.990895.",
    )

    parser.add_argument(
        "--launch-height-m",
        type=float,
        default=52.0,
        help="Launch site height in meters. Default: 52.",
    )

    parser.add_argument(
        "--launch-time",
        default="2022-11-18T22:58:43Z",
        help="Launch time in UTC, e.g. 2022-11-18T22:58:43Z.",
    )

    parser.add_argument(
        "--sample-every",
        type=int,
        default=3,
        help="Downsample input rows. Default: 3.",
    )

    parser.add_argument(
        "--point-every",
        type=int,
        default=2,
        help="Create temp point every N downsampled rows. Default: 2.",
    )

    parser.add_argument(
        "--wind-every",
        type=int,
        default=20,
        help="Create wind arrow every N downsampled rows. Default: 20.",
    )

    parser.add_argument(
        "--seconds-per-original-sample",
        type=float,
        default=1.0,
        help="Time spacing of original rows in seconds. Default: 1.",
    )

    parser.add_argument(
        "--clock-multiplier",
        type=int,
        default=30,
        help="Cesium clock multiplier. Default: 30.",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    name = args.name or default_name_from_input(input_path)
    out_path = resolve_output_path(args.out, name, input_path)
    launch_time = parse_launch_time(args.launch_time)

    df_raw = read_sounding_text(input_path)

    czml, df_used, start, end = make_czml(
        df=df_raw,
        launch_lon=args.launch_lon,
        launch_lat=args.launch_lat,
        launch_time=launch_time,
        name=name,
        title=args.title,
        launch_height_m=args.launch_height_m,
        sample_every=args.sample_every,
        point_every=args.point_every,
        wind_every=args.wind_every,
        seconds_per_original_sample=args.seconds_per_original_sample,
        clock_multiplier=args.clock_multiplier,
    )

    out_path.write_text(json.dumps(czml, indent=2), encoding="utf-8")

    print("Wrote:", out_path)
    print("Input:", input_path)
    print("Rows raw:", len(df_raw))
    print("Rows used:", len(df_used))
    print("Start:", iso_z(start))
    print("End:", iso_z(end))
    print("Height range:", float(df_used["HGHT"].min()), float(df_used["HGHT"].max()))


if __name__ == "__main__":
    main()
