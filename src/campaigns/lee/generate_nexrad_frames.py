#!/usr/bin/env python3
"""
generate_nexrad_frames.py

Generate georeferenced NEXRAD Level-3 radar PNG frames + frames.json.

This version supports BOTH:
1) ZIP files inside data/
2) Already-extracted Level-3 files inside data/

Folder pattern:
  public/NexRad/Nov18/data
  public/NexRad/Nov18/output

Output:
  public/NexRad/Nov18/output/frames.json
  public/NexRad/Nov18/output/frame_00000.png
  public/NexRad/Nov18/output/frame_00001.png
  ...

Quality mode also writes:
  public/NexRad/Nov18/output/grid_data/frame_00000.npz

Examples:

Nov18:
python generate_nexrad_frames.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/NexRad/Nov18/data \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/NexRad/Nov18/output \
  --date-label Nov18 \
  --radar BUF \
  --mode fast

Nov19:
python generate_nexrad_frames.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/NexRad/Nov19/data \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/NexRad/Nov19/output \
  --date-label Nov19 \
  --radar BUF \
  --mode fast
"""

import argparse
import glob
import json
import re
import shutil
import zipfile
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
from PIL import Image
from pyproj import Geod
from metpy.io import Level3File


geod = Geod(ellps="WGS84")


def remove_zone_identifier_files(root):
    root = Path(root)
    if not root.exists():
        return
    for p in root.rglob("*:Zone.Identifier"):
        if p.is_file():
            p.unlink()


def parse_time_from_name(fname: str):
    stem = Path(fname).stem

    # Original script expected final token: *_YYYYMMDDHHMM
    try:
        ts = stem.split("_")[-1]
        dt = datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # Fallback: any 12-digit UTC timestamp in filename.
    m = re.search(r"(20\d{10})", stem)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    return None


def public_url(path):
    p = Path(path)
    parts = list(p.parts)
    if "public" in parts:
        idx = parts.index("public")
        return "/" + "/".join(parts[idx + 1:])
    return str(path)


def resolve_input_files(input_arg):
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

    return sorted(clean)


def expand_zip_and_extracted_files(input_files, extract_dir):
    """
    If files are ZIPs, extract them.
    If files are already extracted Level-3 files, use them directly.
    """
    extract_dir = Path(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    zip_files = []
    direct_files = []

    for f in input_files:
        try:
            if zipfile.is_zipfile(f):
                zip_files.append(f)
            else:
                direct_files.append(f)
        except Exception:
            direct_files.append(f)

    print(f"ZIP files found: {len(zip_files)}")
    print(f"Already-extracted/direct files found: {len(direct_files)}")

    all_files = list(direct_files)

    for z in zip_files:
        out_dir = extract_dir / z.stem
        out_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(out_dir)

    for f in extract_dir.rglob("*"):
        if f.is_file() and ":Zone.Identifier" not in str(f):
            all_files.append(f)

    # Dedupe.
    seen = set()
    unique = []
    for f in all_files:
        try:
            key = str(Path(f).resolve())
        except Exception:
            key = str(f)
        if key not in seen:
            seen.add(key)
            unique.append(Path(f))

    return sorted(unique)


def compute_bounds(lat0, lon0, radius_km):
    lon_n, lat_n, _ = geod.fwd(lon0, lat0, 0, radius_km * 1000.0)
    lon_e, lat_e, _ = geod.fwd(lon0, lat0, 90, radius_km * 1000.0)
    lon_s, lat_s, _ = geod.fwd(lon0, lat0, 180, radius_km * 1000.0)
    lon_w, lat_w, _ = geod.fwd(lon0, lat0, 270, radius_km * 1000.0)

    return (
        min(lon_w, lon0),
        min(lat_s, lat0),
        max(lon_e, lon0),
        max(lat_n, lat0),
    )


def extract_level3_data(filepath):
    f = Level3File(filepath)

    lat0 = float(f.lat)
    lon0 = float(f.lon)

    block = f.sym_block[0][0]
    data = f.map_data(block["data"]).astype(np.float32)

    start_az = np.array(block["start_az"], dtype=np.float32)
    end_az = np.array(block["end_az"], dtype=np.float32)

    nrays, ngates = data.shape
    max_range_km = float(getattr(f, "max_range", 230.0))
    gate_spacing_km = max_range_km / ngates if ngates > 0 else 1.0

    return lat0, lon0, data, start_az, end_az, ngates, max_range_km, gate_spacing_km


def compute_all_ray_coords(lat0, lon0, azimuths, gate_spacing_km, ngates):
    ranges_m = (np.arange(ngates, dtype=np.float64) + 0.5) * gate_spacing_km * 1000.0

    all_lons = []
    all_lats = []

    for az in azimuths:
        az_arr = np.full(ngates, float(az), dtype=np.float64)
        lon_arr = np.full(ngates, lon0, dtype=np.float64)
        lat_arr = np.full(ngates, lat0, dtype=np.float64)
        lons, lats, _ = geod.fwd(lon_arr, lat_arr, az_arr, ranges_m)
        all_lons.append(lons)
        all_lats.append(lats)

    return np.stack(all_lons), np.stack(all_lats)


def dbz_grid_to_rgba_image(dbz_grid, min_dbz, fixed_alpha, use_alpha_scaling):
    h, w = dbz_grid.shape
    rgba = np.zeros((h, w, 4), dtype=np.uint8)

    valid = ~np.isnan(dbz_grid) & (dbz_grid >= min_dbz)
    if not np.any(valid):
        return rgba

    vals = dbz_grid[valid]
    out = np.zeros((vals.shape[0], 4), dtype=np.uint8)

    if use_alpha_scaling:
        alpha = np.clip(200 + (vals / 75.0) * 55, 0, 255).astype(np.uint8)
    else:
        alpha = np.full(vals.shape[0], fixed_alpha, dtype=np.uint8)

    masks = [
        vals < 5,
        (vals >= 5) & (vals < 10),
        (vals >= 10) & (vals < 15),
        (vals >= 15) & (vals < 20),
        (vals >= 20) & (vals < 25),
        (vals >= 25) & (vals < 30),
        (vals >= 30) & (vals < 35),
        (vals >= 35) & (vals < 40),
        (vals >= 40) & (vals < 45),
        (vals >= 45) & (vals < 50),
        (vals >= 50) & (vals < 55),
        (vals >= 55) & (vals < 60),
        (vals >= 60) & (vals < 65),
        (vals >= 65) & (vals < 70),
        (vals >= 70) & (vals < 75),
        vals >= 75,
    ]

    colors = [
        (4, 10, 80), (0, 45, 145), (0, 95, 220), (0, 170, 255),
        (0, 210, 180), (0, 200, 90), (80, 230, 80), (170, 240, 50),
        (240, 240, 0), (255, 200, 0), (255, 145, 0), (255, 80, 0),
        (230, 0, 0), (190, 0, 0), (220, 0, 220), (255, 255, 255),
    ]

    for mask, color in zip(masks, colors):
        out[mask, 0] = color[0]
        out[mask, 1] = color[1]
        out[mask, 2] = color[2]

    out[:, 3] = alpha
    rgba[valid] = out
    return rgba


def fill_small_gaps(arr, passes=1):
    out = arr.copy()
    h, w = out.shape

    for _ in range(passes):
        prev = out.copy()
        for y in range(h):
            y0 = max(0, y - 1)
            y1 = min(h, y + 2)
            for x in range(w):
                if not np.isnan(prev[y, x]):
                    continue

                x0 = max(0, x - 1)
                x1 = min(w, x + 2)
                neighborhood = prev[y0:y1, x0:x1]
                vals = neighborhood[~np.isnan(neighborhood)]

                if vals.size >= 3:
                    out[y, x] = np.max(vals)

    return out


def thicken_rgba(rgba, radius=1):
    if radius <= 0:
        return rgba

    h, w, _ = rgba.shape
    out = rgba.copy()
    ys, xs = np.where(rgba[:, :, 3] > 0)

    for y, x in zip(ys, xs):
        y0 = max(0, y - radius)
        y1 = min(h, y + radius + 1)
        x0 = max(0, x - radius)
        x1 = min(w, x + radius + 1)
        out[y0:y1, x0:x1] = np.maximum(out[y0:y1, x0:x1], rgba[y, x])

    return out


def mode_config(args):
    if args.mode == "quality":
        return {
            "out_w": args.out_w or 1800,
            "out_h": args.out_h or 1800,
            "grid_res_deg": args.grid_res_deg or 0.0025,
            "save_numeric_grid": True if args.save_numeric_grid is None else args.save_numeric_grid,
            "fill_gaps": True if args.fill_gaps is None else args.fill_gaps,
        }

    return {
        "out_w": args.out_w or 1400,
        "out_h": args.out_h or 1400,
        "grid_res_deg": args.grid_res_deg or 0.0035,
        "save_numeric_grid": False if args.save_numeric_grid is None else args.save_numeric_grid,
        "fill_gaps": False if args.fill_gaps is None else args.fill_gaps,
    }


def render_geospatial_frame(filepath, out_png, out_grid_npz, cfg, args):
    lat0, lon0, data, start_az, end_az, ngates, max_range_km, gate_spacing_km = extract_level3_data(filepath)

    west, south, east, north = compute_bounds(lat0, lon0, max_range_km)

    lon_vals = np.arange(west, east + cfg["grid_res_deg"], cfg["grid_res_deg"], dtype=np.float64)
    lat_vals = np.arange(south, north + cfg["grid_res_deg"], cfg["grid_res_deg"], dtype=np.float64)

    grid_h = len(lat_vals)
    grid_w = len(lon_vals)

    dbz_grid = np.full((grid_h, grid_w), np.nan, dtype=np.float32)
    azimuths = (start_az + end_az) / 2.0

    all_lons, all_lats = compute_all_ray_coords(lat0, lon0, azimuths, gate_spacing_km, ngates)

    xs_all = ((all_lons - west) / cfg["grid_res_deg"]).astype(np.int32)
    ys_all = ((all_lats - south) / cfg["grid_res_deg"]).astype(np.int32)

    valid = ~np.isnan(data) & (data >= args.min_dbz)
    inside = (xs_all >= 0) & (xs_all < grid_w) & (ys_all >= 0) & (ys_all < grid_h)
    mask = valid & inside

    xs = xs_all[mask]
    ys = ys_all[mask]
    vals = data[mask]

    for x, y, val in zip(xs, ys, vals):
        current = dbz_grid[y, x]
        if np.isnan(current) or val > current:
            dbz_grid[y, x] = val

    if cfg["fill_gaps"]:
        dbz_grid = fill_small_gaps(dbz_grid, passes=args.fill_gap_passes)

    rgba = dbz_grid_to_rgba_image(
        dbz_grid,
        min_dbz=args.min_dbz,
        fixed_alpha=args.fixed_alpha,
        use_alpha_scaling=args.use_alpha_scaling,
    )

    rgba = np.flipud(rgba)
    rgba = thicken_rgba(rgba, radius=args.thicken_radius)

    img = Image.fromarray(rgba, mode="RGBA")
    img = img.resize((cfg["out_w"], cfg["out_h"]), resample=Image.Resampling.BILINEAR)
    img.save(out_png)

    if cfg["save_numeric_grid"] and out_grid_npz:
        np.savez_compressed(
            out_grid_npz,
            dbz=dbz_grid,
            west=west,
            south=south,
            east=east,
            north=north,
            grid_res_deg=cfg["grid_res_deg"],
            radar_lat=lat0,
            radar_lon=lon0,
        )

    return {
        "timestamp": parse_time_from_name(Path(filepath).name),
        "image": public_url(out_png),
        "west": west,
        "south": south,
        "east": east,
        "north": north,
        "radar_lat": lat0,
        "radar_lon": lon0,
        "grid_npz": public_url(out_grid_npz) if out_grid_npz else None,
    }


def clean_output(out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for item in out_dir.iterdir():
        if item.name == "_extracted":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def main():
    parser = argparse.ArgumentParser(
        description="Generate NEXRAD PNG frames and frames.json from zipped or already-extracted Level-3 files."
    )

    parser.add_argument("input", help="Input data folder, glob, zip, or extracted Level-3 file.")
    parser.add_argument("--out", required=True, help="Output folder, e.g. public/NexRad/Nov18/output.")
    parser.add_argument("--date-label", default=None)
    parser.add_argument("--radar", default="BUF")
    parser.add_argument("--mode", choices=["fast", "quality"], default="fast")

    parser.add_argument("--min-dbz", type=float, default=2.0)
    parser.add_argument("--fixed-alpha", type=int, default=230)
    parser.add_argument("--use-alpha-scaling", action="store_true")
    parser.add_argument("--grid-res-deg", type=float, default=None)
    parser.add_argument("--thicken-radius", type=int, default=1)
    parser.add_argument("--fill-gap-passes", type=int, default=1)
    parser.add_argument("--out-w", type=int, default=None)
    parser.add_argument("--out-h", type=int, default=None)

    parser.add_argument("--save-numeric-grid", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--fill-gaps", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--extract-dir", default=None)
    parser.add_argument("--no-clean-output", action="store_true")

    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    remove_zone_identifier_files(input_path if input_path.exists() else input_path.parent)

    if not args.no_clean_output:
        clean_output(out_dir)

    extract_dir = Path(args.extract_dir) if args.extract_dir else out_dir / "_extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    grid_dir = out_dir / "grid_data"
    cfg = mode_config(args)

    if cfg["save_numeric_grid"]:
        grid_dir.mkdir(parents=True, exist_ok=True)

    input_files = resolve_input_files(args.input)
    candidate_files = expand_zip_and_extracted_files(input_files, extract_dir)

    print(f"Total candidate files: {len(candidate_files)}")
    print(f"MODE: {args.mode}")
    print(f"Output: {out_dir}")

    frames = []
    idx = 0

    for fpath in sorted(candidate_files):
        out_png = out_dir / f"frame_{idx:05d}.png"
        out_grid = grid_dir / f"frame_{idx:05d}.npz" if cfg["save_numeric_grid"] else None

        try:
            meta = render_geospatial_frame(
                filepath=fpath,
                out_png=out_png,
                out_grid_npz=out_grid,
                cfg=cfg,
                args=args,
            )

            if meta["timestamp"]:
                frames.append(meta)
                idx += 1
                print("OK:", fpath)
            else:
                print("SKIP no timestamp:", fpath)
        except Exception as e:
            print("SKIP:", fpath, e)

    frames.sort(key=lambda x: x["timestamp"])

    result = {
        "metadata": {
            "date_label": args.date_label,
            "radar": args.radar,
            "mode": args.mode,
            "input": str(args.input),
            "output": str(out_dir),
            "min_dbz": args.min_dbz,
            "grid_res_deg": cfg["grid_res_deg"],
            "out_w": cfg["out_w"],
            "out_h": cfg["out_h"],
            "save_numeric_grid": cfg["save_numeric_grid"],
            "fill_gaps": cfg["fill_gaps"],
        },
        "frames": frames,
    }

    frames_json = out_dir / "frames.json"
    frames_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print("\nDONE ->", frames_json)
    print("Frame count:", len(frames))


if __name__ == "__main__":
    main()
