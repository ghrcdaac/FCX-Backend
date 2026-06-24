#!/usr/bin/env python3
"""
generate_dow_3dtiles.py

Generate DOW RHI point-cloud 3D Tiles from CF/Radial NetCDF files.

This is the argument-based version of your DOW generator:
- Keeps separate high/low dBZ tilesets for the toggle.
- Uses tighter per-tile bounding spheres.
- Adds optional interpolation between radar beams and gates.
- Supports Nov18 / Nov19 folder pattern like GLM, NEXRAD, and sounding.

Folder pattern:
  public/DOW/Nov18/data
  public/DOW/Nov18/output/high
  public/DOW/Nov18/output/low

  public/DOW/Nov19/data
  public/DOW/Nov19/output/high
  public/DOW/Nov19/output/low

Examples:

Nov18:
python generate_dow_3dtiles.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov18/data \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov18/output \
  --date-label Nov18

Nov19:
python generate_dow_3dtiles.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov19/data \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/DOW/Nov19/output \
  --date-label Nov19

Outputs:
  public/DOW/Nov18/output/high/tileset.json
  public/DOW/Nov18/output/high/*.pnts
  public/DOW/Nov18/output/low/tileset.json
  public/DOW/Nov18/output/low/*.pnts
"""

import argparse
import glob
import json
import re
import shutil
import struct
import zipfile
from pathlib import Path

import numpy as np
import xarray as xr
from pyproj import Transformer


def remove_zone_identifier_files(root):
    root = Path(root)
    if not root.exists():
        return
    for p in root.rglob("*:Zone.Identifier"):
        if p.is_file():
            p.unlink()


def resolve_input_files(input_arg, extract_dir):
    """
    Accepts:
      - folder containing .nc files
      - folder containing .zip files
      - glob pattern
      - single .nc file
      - single .zip file

    If already extracted .nc files exist, they are used directly.
    """
    p = Path(input_arg)

    if p.is_dir():
        files = [x for x in p.rglob("*") if x.is_file()]
    else:
        matches = glob.glob(input_arg)
        if matches:
            files = [Path(x) for x in matches if Path(x).is_file()]
        elif p.is_file():
            files = [p]
        else:
            raise FileNotFoundError(f"No input files found: {input_arg}")

    clean = []
    for f in files:
        if ":Zone.Identifier" in str(f):
            continue
        if f.name.startswith("."):
            continue
        clean.append(f)

    zip_files = []
    direct_files = []

    for f in clean:
        try:
            if zipfile.is_zipfile(f):
                zip_files.append(f)
            else:
                direct_files.append(f)
        except Exception:
            direct_files.append(f)

    extract_dir = Path(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    for z in zip_files:
        out_dir = extract_dir / z.stem
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(out_dir)

    extracted = [
        f for f in extract_dir.rglob("*")
        if f.is_file() and ":Zone.Identifier" not in str(f)
    ]

    all_files = direct_files + extracted

    # Keep likely CF/Radial files only.
    nc_files = []
    for f in all_files:
        name = f.name.lower()
        if name.endswith((".nc", ".cdf", ".nc4")) or "cfrad" in name:
            nc_files.append(f)

    # Dedupe.
    seen = set()
    unique = []
    for f in nc_files:
        try:
            key = str(f.resolve())
        except Exception:
            key = str(f)
        if key not in seen:
            seen.add(key)
            unique.append(f)

    return sorted(unique)


def dbz_color(v):
    if v < 0:
        return [60, 80, 255]      # blue
    if v < 10:
        return [0, 180, 255]      # cyan/blue
    if v < 20:
        return [0, 180, 0]        # green
    if v < 30:
        return [255, 220, 0]      # yellow
    if v < 40:
        return [255, 120, 0]      # orange
    return [255, 0, 0]            # red


def radar_to_lonlatalt(az, el, rng, radar_lat, radar_lon, radar_alt):
    R = 6371000.0
    az = np.deg2rad(az)
    el = np.deg2rad(el)

    ground = rng * np.cos(el)
    alt = radar_alt + rng * np.sin(el)

    lat = radar_lat + np.rad2deg((ground * np.cos(az)) / R)
    lon = radar_lon + np.rad2deg(
        (ground * np.sin(az)) / (R * np.cos(np.deg2rad(radar_lat)))
    )

    return lon, lat, alt


def is_valid_dbz(v, min_dbz_keep):
    return np.isfinite(v) and v >= min_dbz_keep


def can_interpolate(v1, v2, min_dbz_keep, max_interp_dbz_diff):
    if not is_valid_dbz(v1, min_dbz_keep) or not is_valid_dbz(v2, min_dbz_keep):
        return False

    return abs(float(v1) - float(v2)) <= max_interp_dbz_diff


def add_radar_sample(
    v,
    az,
    el,
    rng,
    high_pts,
    high_cols,
    low_pts,
    low_cols,
    to_ecef,
    radar_lat,
    radar_lon,
    radar_alt,
    min_dbz_keep,
    low_dbz_threshold,
):
    if not is_valid_dbz(v, min_dbz_keep):
        return

    lon, lat, alt = radar_to_lonlatalt(
        float(az),
        float(el),
        float(rng),
        radar_lat=radar_lat,
        radar_lon=radar_lon,
        radar_alt=radar_alt,
    )
    x, y, z = to_ecef.transform(lon, lat, alt)

    point = [x, y, z]
    color = dbz_color(float(v))

    if v <= low_dbz_threshold:
        low_pts.append(point)
        low_cols.append(color)
    else:
        high_pts.append(point)
        high_cols.append(color)


def bounding_sphere(points):
    center = points.mean(axis=0)
    radius = np.linalg.norm(points - center, axis=1).max()

    # Padding prevents aggressive culling near tile edges.
    radius = float(radius * 1.05 + 25.0)

    return [
        float(center[0]),
        float(center[1]),
        float(center[2]),
        radius,
    ]


def make_pnts(path, xyz_ecef, rgb):
    n = xyz_ecef.shape[0]

    if n == 0:
        return None

    rtc_center = xyz_ecef.mean(axis=0)
    xyz_local = xyz_ecef - rtc_center

    feature_json = {
        "POINTS_LENGTH": int(n),
        "POSITION": {"byteOffset": 0},
        "RGB": {"byteOffset": int(n * 12)},
        "RTC_CENTER": [
            float(rtc_center[0]),
            float(rtc_center[1]),
            float(rtc_center[2]),
        ],
    }

    feature_json_bytes = json.dumps(feature_json, separators=(",", ":")).encode("utf-8")
    while len(feature_json_bytes) % 8 != 0:
        feature_json_bytes += b" "

    xyz_bytes = xyz_local.astype(np.float32).tobytes()
    rgb_bytes = rgb.astype(np.uint8).tobytes()

    feature_bin = xyz_bytes + rgb_bytes
    while len(feature_bin) % 8 != 0:
        feature_bin += b"\x00"

    header_len = 28
    byte_len = header_len + len(feature_json_bytes) + len(feature_bin)

    header = struct.pack(
        "<4sIIIIII",
        b"pnts",
        1,
        byte_len,
        len(feature_json_bytes),
        len(feature_bin),
        0,
        0,
    )

    with open(path, "wb") as f:
        f.write(header)
        f.write(feature_json_bytes)
        f.write(feature_bin)

    return bounding_sphere(xyz_ecef)


def public_url(path):
    p = Path(path)
    parts = list(p.parts)
    if "public" in parts:
        idx = parts.index("public")
        return "/" + "/".join(parts[idx + 1:])
    return str(path)


def write_tileset(outdir, tile_records, metadata=None):
    outdir = Path(outdir)
    if not tile_records:
        print(f"No points found for {outdir}. Skipping tileset.")
        return None

    all_points = np.vstack([r["points"] for r in tile_records])
    root_sphere = bounding_sphere(all_points)

    children = []

    for record in tile_records:
        children.append(
            {
                "boundingVolume": {
                    "sphere": record["sphere"],
                },
                "geometricError": 0,
                "content": {
                    "uri": record["name"],
                },
            }
        )

    tileset = {
        "asset": {
            "version": "1.0",
        },
        "metadata": metadata or {},
        "geometricError": 500,
        "root": {
            "boundingVolume": {
                "sphere": root_sphere,
            },
            "geometricError": 100,
            "refine": "ADD",
            "children": children,
        },
    }

    out_path = outdir / "tileset.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(tileset, f, indent=2)

    print("Wrote tileset:", out_path)
    print("Tiles:", len(children))
    print("Points:", len(all_points))
    print("Root sphere:", root_sphere)

    return out_path


def choose_dbz_field(ds, requested_field=None):
    if requested_field:
        if requested_field not in ds:
            raise KeyError(f"Requested field '{requested_field}' not found. Available: {list(ds.data_vars)}")
        return requested_field

    for candidate in ["DBZHCC", "DBZHC_F", "DBZ", "DBZH"]:
        if candidate in ds:
            return candidate

    raise KeyError(f"No known dBZ field found. Available fields: {list(ds.data_vars)}")


def get_az_el_range(ds):
    az = ds["azimuth"].values
    el = ds["elevation"].values
    rng = ds["range"].values

    return az, el, rng


def clean_output(outdir):
    outdir = Path(outdir)
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)


def process_file(
    file,
    high_outdir,
    low_outdir,
    high_tile_records,
    low_tile_records,
    args,
    to_ecef,
):
    ds = xr.open_dataset(file, decode_timedelta=False)

    try:
        field = choose_dbz_field(ds, args.field)

        dbz = ds[field].values
        az, el, rng = get_az_el_range(ds)

        high_pts = []
        high_cols = []
        low_pts = []
        low_cols = []

        n_rays = dbz.shape[0]
        n_gates = dbz.shape[1]

        for i in range(0, n_rays, args.ray_step):
            for j in range(0, n_gates, args.gate_step):
                v = dbz[i, j]

                # Original sampled point.
                add_radar_sample(
                    v,
                    az[i],
                    el[i],
                    rng[j],
                    high_pts,
                    high_cols,
                    low_pts,
                    low_cols,
                    to_ecef=to_ecef,
                    radar_lat=args.radar_lat,
                    radar_lon=args.radar_lon,
                    radar_alt=args.radar_alt,
                    min_dbz_keep=args.min_dbz_keep,
                    low_dbz_threshold=args.low_dbz_threshold,
                )

                if args.enable_interpolation:
                    # Fill gap between neighboring beams/rays at the same range gate.
                    if args.interpolate_between_beams:
                        i2 = i + args.ray_step

                        if i2 < n_rays:
                            v2 = dbz[i2, j]

                            if can_interpolate(
                                v,
                                v2,
                                min_dbz_keep=args.min_dbz_keep,
                                max_interp_dbz_diff=args.max_interp_dbz_diff,
                            ):
                                v_mid = 0.5 * (float(v) + float(v2))
                                az_mid = 0.5 * (float(az[i]) + float(az[i2]))
                                el_mid = 0.5 * (float(el[i]) + float(el[i2]))

                                add_radar_sample(
                                    v_mid,
                                    az_mid,
                                    el_mid,
                                    rng[j],
                                    high_pts,
                                    high_cols,
                                    low_pts,
                                    low_cols,
                                    to_ecef=to_ecef,
                                    radar_lat=args.radar_lat,
                                    radar_lon=args.radar_lon,
                                    radar_alt=args.radar_alt,
                                    min_dbz_keep=args.min_dbz_keep,
                                    low_dbz_threshold=args.low_dbz_threshold,
                                )

                    # Fill gap between neighboring range gates along the same beam.
                    if args.interpolate_between_gates:
                        j2 = j + args.gate_step

                        if j2 < n_gates:
                            v2 = dbz[i, j2]

                            if can_interpolate(
                                v,
                                v2,
                                min_dbz_keep=args.min_dbz_keep,
                                max_interp_dbz_diff=args.max_interp_dbz_diff,
                            ):
                                v_mid = 0.5 * (float(v) + float(v2))
                                rng_mid = 0.5 * (float(rng[j]) + float(rng[j2]))

                                add_radar_sample(
                                    v_mid,
                                    az[i],
                                    el[i],
                                    rng_mid,
                                    high_pts,
                                    high_cols,
                                    low_pts,
                                    low_cols,
                                    to_ecef=to_ecef,
                                    radar_lat=args.radar_lat,
                                    radar_lon=args.radar_lon,
                                    radar_alt=args.radar_alt,
                                    min_dbz_keep=args.min_dbz_keep,
                                    low_dbz_threshold=args.low_dbz_threshold,
                                )

        stem = Path(file).stem

        if high_pts:
            high_pts = np.array(high_pts, dtype=np.float64)
            high_cols = np.array(high_cols, dtype=np.uint8)

            high_name = f"{stem}.pnts"
            high_sphere = make_pnts(high_outdir / high_name, high_pts, high_cols)

            high_tile_records.append(
                {
                    "name": high_name,
                    "sphere": high_sphere,
                    "points": high_pts,
                }
            )

            print("Wrote HIGH", high_name, len(high_pts), "points")

        if low_pts:
            low_pts = np.array(low_pts, dtype=np.float64)
            low_cols = np.array(low_cols, dtype=np.uint8)

            low_name = f"{stem}.pnts"
            low_sphere = make_pnts(low_outdir / low_name, low_pts, low_cols)

            low_tile_records.append(
                {
                    "name": low_name,
                    "sphere": low_sphere,
                    "points": low_pts,
                }
            )

            print("Wrote LOW ", low_name, len(low_pts), "points")

    finally:
        ds.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate DOW high/low dBZ 3D Tiles from CF/Radial NetCDF files."
    )

    parser.add_argument(
        "input",
        help="Input data folder, glob, .nc file, or .zip file.",
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Output folder, e.g. public/DOW/Nov18/output.",
    )

    parser.add_argument(
        "--date-label",
        default=None,
        help="Optional date label stored in tileset metadata, e.g. Nov18.",
    )

    parser.add_argument(
        "--high-name",
        default="high",
        help="High dBZ output folder name under --out. Default: high.",
    )

    parser.add_argument(
        "--low-name",
        default="low",
        help="Low dBZ output folder name under --out. Default: low.",
    )

    parser.add_argument("--radar-lat", type=float, default=43.990895)
    parser.add_argument("--radar-lon", type=float, default=-76.026593)
    parser.add_argument("--radar-alt", type=float, default=95.0)

    parser.add_argument("--ray-step", type=int, default=2)
    parser.add_argument("--gate-step", type=int, default=3)

    parser.add_argument(
        "--enable-interpolation",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable interpolation. Default: true.",
    )
    parser.add_argument(
        "--interpolate-between-beams",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable interpolation between beams. Default: true.",
    )
    parser.add_argument(
        "--interpolate-between-gates",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable interpolation between gates. Default: true.",
    )

    parser.add_argument("--max-interp-dbz-diff", type=float, default=12.0)
    parser.add_argument("--min-dbz-keep", type=float, default=-5.0)
    parser.add_argument("--low-dbz-threshold", type=float, default=10.0)

    parser.add_argument(
        "--field",
        default=None,
        help="Optional dBZ field name. Default auto-selects DBZHCC, DBZHC_F, DBZ, DBZH.",
    )

    parser.add_argument(
        "--extract-dir",
        default=None,
        help="ZIP extraction folder. Default: <out>/_extracted.",
    )

    parser.add_argument(
        "--no-clean-output",
        action="store_true",
        help="Do not delete existing high/low output folders before writing.",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    out_root = Path(args.out)

    high_outdir = out_root / args.high_name
    low_outdir = out_root / args.low_name
    extract_dir = Path(args.extract_dir) if args.extract_dir else out_root / "_extracted"

    remove_zone_identifier_files(input_path if input_path.exists() else input_path.parent)

    if args.no_clean_output:
        high_outdir.mkdir(parents=True, exist_ok=True)
        low_outdir.mkdir(parents=True, exist_ok=True)
    else:
        clean_output(high_outdir)
        clean_output(low_outdir)

    extract_dir.mkdir(parents=True, exist_ok=True)

    to_ecef = Transformer.from_crs("EPSG:4979", "EPSG:4978", always_xy=True)

    files = resolve_input_files(args.input, extract_dir)

    print("Files found:", len(files))
    print("Input:", args.input)
    print("Output high:", high_outdir)
    print("Output low:", low_outdir)

    high_tile_records = []
    low_tile_records = []

    for file in files:
        try:
            process_file(
                file=file,
                high_outdir=high_outdir,
                low_outdir=low_outdir,
                high_tile_records=high_tile_records,
                low_tile_records=low_tile_records,
                args=args,
                to_ecef=to_ecef,
            )
        except Exception as e:
            print("SKIP:", file, e)

    metadata = {
        "date_label": args.date_label,
        "input": str(args.input),
        "out": str(out_root),
        "radar_lat": args.radar_lat,
        "radar_lon": args.radar_lon,
        "radar_alt": args.radar_alt,
        "ray_step": args.ray_step,
        "gate_step": args.gate_step,
        "enable_interpolation": args.enable_interpolation,
        "interpolate_between_beams": args.interpolate_between_beams,
        "interpolate_between_gates": args.interpolate_between_gates,
        "max_interp_dbz_diff": args.max_interp_dbz_diff,
        "min_dbz_keep": args.min_dbz_keep,
        "low_dbz_threshold": args.low_dbz_threshold,
    }

    high_tileset = write_tileset(high_outdir, high_tile_records, metadata={**metadata, "layer": "high"})
    low_tileset = write_tileset(low_outdir, low_tile_records, metadata={**metadata, "layer": "low"})

    print("Done.")
    print("High tileset:", high_tileset)
    print("Low tileset:", low_tileset)
    print("High browser path:", public_url(high_tileset) if high_tileset else None)
    print("Low browser path:", public_url(low_tileset) if low_tileset else None)


if __name__ == "__main__":
    main()
