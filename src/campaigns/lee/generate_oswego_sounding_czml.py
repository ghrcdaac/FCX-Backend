#!/usr/bin/env python3
"""
generate_oswego_sounding_czml.py

Generate animated Cesium CZML for Oswego EDT sounding text files.

Folder pattern:
  public/OSWEGO/Nov18/data
  public/OSWEGO/Nov18/output

  public/OSWEGO/Nov19/data
  public/OSWEGO/Nov19/output

Input example:
  edt_20221118_2357.txt

Output example:
  public/OSWEGO/Nov18/output/oswego_edt_20221118_2357_animated.czml

Expected text columns:
  TIME_S HGHT PRES TEMP RELH DRCT WSPD_MS UNKNOWN LAT LON

Examples:

Nov18:
python generate_oswego_sounding_czml.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/OSWEGO/Nov18/data/edt_20221118_2357.txt \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/OSWEGO/Nov18/output \
  --name oswego_edt_20221118_2357 \
  --title "Oswego EDT Sounding 20221118 2357" \
  --launch-time 2022-11-18T23:57:00Z

Nov19:
python generate_oswego_sounding_czml.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/OSWEGO/Nov19/data/edt_20221119_0139.txt \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/OSWEGO/Nov19/output \
  --name oswego_edt_20221119_0139 \
  --title "Oswego EDT Sounding 20221119 0139" \
  --launch-time 2022-11-19T01:39:00Z
"""

import argparse
import json
import re
from pathlib import Path

import pandas as pd


COLS = [
    "TIME_S",
    "HGHT",
    "PRES",
    "TEMP",
    "RELH",
    "DRCT",
    "WSPD_MS",
    "UNKNOWN",
    "LAT",
    "LON",
]


def safe_slug(value):
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower() or "oswego_sounding"


def default_name_from_input(input_path):
    return safe_slug(Path(input_path).stem)


def resolve_output_path(out_arg, name):
    out = Path(out_arg)

    if out.suffix.lower() == ".czml":
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    out.mkdir(parents=True, exist_ok=True)
    return out / f"{safe_slug(name)}_animated.czml"


def parse_utc(value):
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def iso_z(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat().replace("+00:00", "Z")


def read_oswego_text(input_path):
    rows = []

    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.split()

            if len(parts) < 10:
                continue

            try:
                rows.append([float(x) for x in parts[:10]])
            except ValueError:
                continue

    if not rows:
        raise RuntimeError(f"No valid Oswego sounding rows found in: {input_path}")

    df = pd.DataFrame(rows, columns=COLS)
    df = df.dropna()

    df = df[
        (df["HGHT"] >= 0)
        & (df["LAT"].between(35, 50))
        & (df["LON"].between(-85, -65))
    ].copy()

    df = df.sort_values("TIME_S")

    if df.empty:
        raise RuntimeError("No valid rows after height/lat/lon filtering.")

    return df.reset_index(drop=True)


def temp_color(t):
    if t <= -30:
        return [90, 60, 255, 240]
    if t <= -20:
        return [70, 120, 255, 240]
    if t <= -10:
        return [0, 200, 255, 240]
    if t <= 0:
        return [0, 255, 170, 240]
    return [255, 220, 0, 240]


def wind_arrow(drct):
    arrows = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]
    return arrows[int(((float(drct) + 22.5) % 360) // 45)]


def build_profile_description(row, t0):
    return f"""
    <table>
      <tr><td><b>Time</b></td><td>{iso_z(t0)}</td></tr>
      <tr><td><b>Height</b></td><td>{float(row["HGHT"]):.1f} m MSL</td></tr>
      <tr><td><b>Pressure</b></td><td>{float(row["PRES"]):.1f} hPa</td></tr>
      <tr><td><b>Temperature</b></td><td>{float(row["TEMP"]):.1f} °C</td></tr>
      <tr><td><b>RH</b></td><td>{float(row["RELH"]):.0f} %</td></tr>
      <tr><td><b>Wind</b></td><td>{float(row["DRCT"]):.0f}° / {float(row["WSPD_MS"]):.1f} m/s</td></tr>
      <tr><td><b>Lat/Lon</b></td><td>{float(row["LAT"]):.4f}, {float(row["LON"]):.4f}</td></tr>
    </table>
    """


def build_czml(
    df,
    launch_time,
    name,
    title,
    downsample,
    point_every,
    wind_every,
    clock_multiplier,
):
    if downsample and downsample > 1:
        df = df.iloc[::downsample].copy().reset_index(drop=True)

    df["time"] = df["TIME_S"].apply(
        lambda s: launch_time + pd.Timedelta(seconds=float(s))
    )

    start = df["time"].iloc[0]
    end = df["time"].iloc[-1]

    entity_id = safe_slug(name)

    positions = []
    for _, r in df.iterrows():
        positions += [
            float(r["TIME_S"]),
            float(r["LON"]),
            float(r["LAT"]),
            float(r["HGHT"]),
        ]

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
            "id": f"{entity_id}_balloon",
            "name": "Oswego Sounding Balloon",
            "availability": interval,
            "position": {
                "epoch": iso_z(launch_time),
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
                "text": "Oswego Sonde",
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
                        "color": {"rgba": [255, 255, 255, 220]}
                    }
                },
            },
        },
    ]

    # Temperature profile points appear after balloon reaches them.
    for idx, r in df.iloc[::point_every].iterrows():
        t0 = r["time"]
        h = float(r["HGHT"])
        temp = float(r["TEMP"])

        czml.append(
            {
                "id": f"{entity_id}_profile_point_{idx}",
                "name": f"{h:.0f} m | {temp:.1f} °C",
                "availability": f"{iso_z(t0)}/{iso_z(end)}",
                "description": build_profile_description(r, t0),
                "position": {
                    "cartographicDegrees": [
                        float(r["LON"]),
                        float(r["LAT"]),
                        h,
                    ]
                },
                "point": {
                    "pixelSize": 8,
                    "color": {"rgba": temp_color(temp)},
                    "outlineColor": {"rgba": [0, 0, 0, 255]},
                    "outlineWidth": 1,
                    "disableDepthTestDistance": 1000000000,
                },
            }
        )

    # Wind labels.
    for idx, r in df.iloc[::wind_every].iterrows():
        t0 = r["time"]
        h = float(r["HGHT"])
        drct = float(r["DRCT"])
        wspd = float(r["WSPD_MS"])

        czml.append(
            {
                "id": f"{entity_id}_wind_{idx}",
                "name": f"Wind {h:.0f} m",
                "availability": f"{iso_z(t0)}/{iso_z(end)}",
                "position": {
                    "cartographicDegrees": [
                        float(r["LON"]) + 0.01,
                        float(r["LAT"]),
                        h,
                    ]
                },
                "label": {
                    "text": f"{wind_arrow(drct)} {wspd:.1f} m/s",
                    "font": "15px sans-serif",
                    "fillColor": {"rgba": [120, 220, 255, 255]},
                    "outlineColor": {"rgba": [0, 0, 0, 255]},
                    "outlineWidth": 3,
                    "style": "FILL_AND_OUTLINE",
                    "disableDepthTestDistance": 1000000000,
                },
            }
        )

    # Launch marker from first row.
    first = df.iloc[0]
    czml.append(
        {
            "id": f"{entity_id}_launch_site",
            "name": "Oswego Launch Site",
            "position": {
                "cartographicDegrees": [
                    float(first["LON"]),
                    float(first["LAT"]),
                    float(first["HGHT"]),
                ]
            },
            "point": {
                "pixelSize": 16,
                "color": {"rgba": [255, 160, 0, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 2,
                "disableDepthTestDistance": 1000000000,
            },
            "label": {
                "text": "Oswego Launch",
                "font": "16px sans-serif",
                "fillColor": {"rgba": [255, 255, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 3,
                "style": "FILL_AND_OUTLINE",
                "pixelOffset": {"cartesian2": [0, -35]},
                "disableDepthTestDistance": 1000000000,
            },
        }
    )

    return czml, df, start, end


def main():
    parser = argparse.ArgumentParser(
        description="Generate animated CZML for Oswego EDT sounding text files."
    )

    parser.add_argument("input", help="Input Oswego EDT sounding text file.")
    parser.add_argument(
        "--out",
        required=True,
        help="Output .czml path or output folder. If folder, writes <name>_animated.czml.",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Output/entity name. Default derived from input filename.",
    )
    parser.add_argument(
        "--title",
        default=None,
        help="Display title. Default derived from name.",
    )
    parser.add_argument(
        "--launch-time",
        required=True,
        help="Launch time UTC, e.g. 2022-11-18T23:57:00Z.",
    )
    parser.add_argument(
        "--downsample",
        type=int,
        default=3,
        help="Use every Nth row. Default: 3.",
    )
    parser.add_argument(
        "--point-every",
        type=int,
        default=2,
        help="Create profile point every N downsampled rows. Default: 2.",
    )
    parser.add_argument(
        "--wind-every",
        type=int,
        default=20,
        help="Create wind label every N downsampled rows. Default: 20.",
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
    title = args.title or name.replace("_", " ").title()

    out_path = resolve_output_path(args.out, name)
    launch_time = parse_utc(args.launch_time)

    df_raw = read_oswego_text(input_path)

    czml, df_used, start, end = build_czml(
        df=df_raw,
        launch_time=launch_time,
        name=name,
        title=title,
        downsample=args.downsample,
        point_every=args.point_every,
        wind_every=args.wind_every,
        clock_multiplier=args.clock_multiplier,
    )

    out_path.write_text(json.dumps(czml, indent=2), encoding="utf-8")

    print("Wrote:", out_path)
    print("Input:", input_path)
    print("Rows:", len(df_used))
    print("Start:", iso_z(start))
    print("End:", iso_z(end))
    print("Height:", float(df_used["HGHT"].min()), float(df_used["HGHT"].max()))
    print("Lat:", float(df_used["LAT"].min()), float(df_used["LAT"].max()))
    print("Lon:", float(df_used["LON"].min()), float(df_used["LON"].max()))


if __name__ == "__main__":
    main()
