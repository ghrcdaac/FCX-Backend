#!/usr/bin/env python3
"""
generate_glm_points.py

Generate Cesium PointPrimitive-ready points.json from GOES GLM NetCDF files.

Output format:
[
  {"lon": -76.12, "lat": 43.45, "alt": 12000, "time": 123.4, "value": 0.82},
  ...
]

This matches the React PointPrimitive viewer fields:
  lon, lat, alt, time, value

Examples:

1) Local folder or glob:
python generate_glm_points.py \
  "/home/dacharya/fcx-playground-backend/notebooks/tiles/tmp/GLM/nov-19/*.nc" \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/Nov19-glm/points.json \
  --date 2022-11-19 \
  --epoch 2022-11-19T00:00:00Z

2) Local folder:
python generate_glm_points.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/tmp/GLM/nov-19 \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/Nov19-glm/points.json \
  --date 2022-11-19 \
  --epoch 2022-11-19T00:00:00Z

3) HTTP/HTTPS URL to one .nc file, a .zip, a .tar/.tar.gz, or a simple directory listing:
python generate_glm_points.py \
  "https://example.com/path/to/glm-files/" \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/public/Nov19-glm/points.json \
  --date 2022-11-19 \
  --epoch 2022-11-19T00:00:00Z

Notes:
- If input URL is a directory listing, this script looks for .nc links in the HTML.
- For GLM data, it prefers event-level variables:
    event_lon, event_lat, event_energy, event_time_offset
  and falls back to group/flash variables when event variables are missing.
"""

import argparse
import datetime as dt
import glob
import json
import math
import os
import re
import shutil
import sys
import tarfile
import tempfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from html.parser import HTMLParser

import numpy as np


# -----------------------------
# Optional NetCDF readers
# -----------------------------

def open_netcdf(path):
    """
    Prefer xarray, fall back to netCDF4.
    Returns a reader object and backend name.
    """
    try:
        import xarray as xr
        return xr.open_dataset(path, decode_times=False, mask_and_scale=True), "xarray"
    except Exception as xr_err:
        try:
            from netCDF4 import Dataset
            return Dataset(path, "r"), "netCDF4"
        except Exception as nc_err:
            raise RuntimeError(
                f"Could not open NetCDF file: {path}\n"
                f"xarray error: {xr_err}\n"
                f"netCDF4 error: {nc_err}"
            )


def close_netcdf(ds, backend):
    try:
        ds.close()
    except Exception:
        pass


def has_var(ds, backend, name):
    if backend == "xarray":
        return name in ds.variables
    return name in ds.variables


def get_var_array(ds, backend, name):
    if not has_var(ds, backend, name):
        return None
    if backend == "xarray":
        return np.asarray(ds[name].values)
    return np.asarray(ds.variables[name][:])


def get_attr(obj, name, default=None):
    try:
        return getattr(obj, name)
    except Exception:
        return default


# -----------------------------
# URL/file discovery
# -----------------------------

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value)


def is_url(value):
    return value.startswith("http://") or value.startswith("https://")


def download_url(url, dest_dir):
    parsed = urllib.parse.urlparse(url)
    filename = Path(parsed.path).name or "downloaded_glm"
    out_path = Path(dest_dir) / filename

    print(f"Downloading: {url}")
    print(f"To: {out_path}")

    with urllib.request.urlopen(url) as response, open(out_path, "wb") as f:
        shutil.copyfileobj(response, f)

    return out_path


def discover_urls_from_listing(url):
    print(f"Reading URL listing: {url}")
    with urllib.request.urlopen(url) as response:
        html = response.read().decode("utf-8", errors="replace")

    parser = LinkParser()
    parser.feed(html)

    urls = []
    for href in parser.links:
        full = urllib.parse.urljoin(url, href)
        if full.lower().endswith(".nc"):
            urls.append(full)

    return sorted(set(urls))


def extract_archive(path, dest_dir):
    path = Path(path)
    extracted = []

    if zipfile.is_zipfile(path):
        print(f"Extracting zip: {path}")
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(dest_dir)
    elif tarfile.is_tarfile(path):
        print(f"Extracting tar: {path}")
        with tarfile.open(path, "r:*") as t:
            t.extractall(dest_dir)
    else:
        return [path]

    for p in Path(dest_dir).rglob("*"):
        if p.is_file() and p.suffix.lower() in [".nc", ".cdf", ".nc4"]:
            extracted.append(p)

    return extracted


def resolve_inputs(input_arg, work_dir):
    """
    input_arg can be:
      - local file
      - local folder
      - local glob
      - URL to .nc
      - URL to .zip/.tar/.tar.gz
      - URL to simple directory listing containing .nc links
    """
    files = []

    if is_url(input_arg):
        lower = input_arg.lower()

        if lower.endswith(".nc") or lower.endswith(".nc4") or lower.endswith(".cdf"):
            downloaded = download_url(input_arg, work_dir)
            files.extend(extract_archive(downloaded, work_dir))
        elif any(lower.endswith(ext) for ext in [".zip", ".tar", ".tar.gz", ".tgz"]):
            downloaded = download_url(input_arg, work_dir)
            files.extend(extract_archive(downloaded, work_dir))
        else:
            urls = discover_urls_from_listing(input_arg)
            if not urls:
                raise RuntimeError(f"No .nc links found at URL listing: {input_arg}")
            for u in urls:
                downloaded = download_url(u, work_dir)
                files.extend(extract_archive(downloaded, work_dir))
    else:
        p = Path(input_arg)

        if p.is_dir():
            files.extend(sorted(p.rglob("*.nc")))
            files.extend(sorted(p.rglob("*.nc4")))
            files.extend(sorted(p.rglob("*.cdf")))
        else:
            matches = glob.glob(input_arg)
            if matches:
                for m in matches:
                    mp = Path(m)
                    if mp.is_file():
                        if mp.suffix.lower() in [".zip", ".tar", ".gz", ".tgz"]:
                            files.extend(extract_archive(mp, work_dir))
                        else:
                            files.append(mp)
            elif p.is_file():
                files.append(p)
            else:
                raise FileNotFoundError(f"No files found for input: {input_arg}")

    # remove metadata junk and duplicates
    clean = []
    seen = set()
    for f in files:
        f = Path(f)
        if ":Zone.Identifier" in str(f):
            continue
        if f.suffix.lower() not in [".nc", ".nc4", ".cdf"]:
            continue
        key = str(f.resolve())
        if key not in seen:
            seen.add(key)
            clean.append(f)

    return sorted(clean)


# -----------------------------
# Time handling
# -----------------------------

def parse_iso_z(value):
    """
    Accepts:
      2022-11-19T00:00:00Z
      2022-11-19 00:00:00
      2022-11-19
    """
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "T" not in value and " " not in value:
        value = value + "T00:00:00+00:00"
    elif " " in value and "+" not in value:
        value = value.replace(" ", "T") + "+00:00"
    elif "T" in value and "+" not in value:
        value = value + "+00:00"

    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_date(value):
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def parse_time_coverage_start(ds, backend, path):
    """
    Tries NetCDF global attr time_coverage_start first.
    Falls back to GOES filename pattern:
      OR_GLM-L2-LCFA_G16_s20223221830000_e...
    """
    raw = get_attr(ds, "time_coverage_start", None)
    if raw:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            return parse_iso_z(str(raw))
        except Exception:
            pass

    name = Path(path).name

    # GOES filename has _sYYYYJJJHHMMSS...
    m = re.search(r"_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})", name)
    if m:
        year = int(m.group(1))
        jday = int(m.group(2))
        hh = int(m.group(3))
        mm = int(m.group(4))
        ss = int(m.group(5))
        return (
            dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
            + dt.timedelta(days=jday - 1, hours=hh, minutes=mm, seconds=ss)
        )

    return None


def offset_units_to_seconds(units):
    """
    Convert common NetCDF time-offset units to seconds.
    GLM often uses seconds since product time, but this is defensive.
    """
    if not units:
        return 1.0

    u = str(units).lower()
    if "microsecond" in u:
        return 1e-6
    if "millisecond" in u:
        return 1e-3
    if "second" in u:
        return 1.0
    if "minute" in u:
        return 60.0
    if "hour" in u:
        return 3600.0

    return 1.0


def get_var_units(ds, backend, name):
    if not has_var(ds, backend, name):
        return None
    try:
        if backend == "xarray":
            return ds[name].attrs.get("units")
        return getattr(ds.variables[name], "units", None)
    except Exception:
        return None


# -----------------------------
# GLM extraction
# -----------------------------

def choose_glm_level(ds, backend):
    """
    Prefer event level, then group, then flash.
    Returns variable prefix and readable level.
    """
    candidates = [
        ("event", "event_lat", "event_lon", "event_energy", "event_time_offset"),
        ("group", "group_lat", "group_lon", "group_energy", "group_time_offset"),
        ("flash", "flash_lat", "flash_lon", "flash_energy", "flash_time_offset_of_first_event"),
    ]

    for level, lat, lon, energy, time_var in candidates:
        if has_var(ds, backend, lat) and has_var(ds, backend, lon):
            return {
                "level": level,
                "lat": lat,
                "lon": lon,
                "energy": energy if has_var(ds, backend, energy) else None,
                "time": time_var if has_var(ds, backend, time_var) else None,
            }

    return None


def normalize_values(values):
    arr = np.asarray(values, dtype=float)
    good = np.isfinite(arr)

    if good.sum() == 0:
        return np.zeros_like(arr, dtype=float)

    # GLM energy can be very skewed, so log-scale first.
    safe = np.where(good, np.maximum(arr, 0.0), np.nan)
    logged = np.log10(safe + 1e-18)

    good_logged = np.isfinite(logged)
    if good_logged.sum() < 2:
        return np.where(good, 0.5, 0.0)

    lo = np.nanpercentile(logged, 5)
    hi = np.nanpercentile(logged, 95)

    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        lo = np.nanmin(logged)
        hi = np.nanmax(logged)

    if hi <= lo:
        return np.where(good, 0.5, 0.0)

    norm = (logged - lo) / (hi - lo)
    norm = np.clip(norm, 0.0, 1.0)
    norm[~np.isfinite(norm)] = 0.0
    return norm


def extract_points_from_file(path, epoch_dt, date_filter=None, bbox=None, alt_m=12000):
    ds, backend = open_netcdf(path)
    try:
        product_start = parse_time_coverage_start(ds, backend, path)
        if product_start is None:
            print(f"Warning: cannot determine product time for {path}; using epoch as base.")
            product_start = epoch_dt

        choice = choose_glm_level(ds, backend)
        if choice is None:
            print(f"Skipping {path}: no event/group/flash lat/lon variables found.")
            return []

        lat = get_var_array(ds, backend, choice["lat"]).astype(float).ravel()
        lon = get_var_array(ds, backend, choice["lon"]).astype(float).ravel()

        n = min(len(lat), len(lon))
        lat = lat[:n]
        lon = lon[:n]

        if choice["energy"]:
            energy = get_var_array(ds, backend, choice["energy"]).astype(float).ravel()[:n]
            value = normalize_values(energy)
        else:
            energy = np.zeros(n, dtype=float)
            value = np.full(n, 0.5, dtype=float)

        if choice["time"]:
            offsets = get_var_array(ds, backend, choice["time"]).astype(float).ravel()[:n]
            unit_factor = offset_units_to_seconds(get_var_units(ds, backend, choice["time"]))
            absolute_times = [
                product_start + dt.timedelta(seconds=float(o) * unit_factor)
                if np.isfinite(o)
                else product_start
                for o in offsets
            ]
        else:
            absolute_times = [product_start] * n

        points = []
        min_lon = min_lat = max_lon = max_lat = None
        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox

        for i in range(n):
            la = float(lat[i])
            lo = float(lon[i])

            if not np.isfinite(la) or not np.isfinite(lo):
                continue
            if la < -90 or la > 90 or lo < -180 or lo > 180:
                continue

            abs_time = absolute_times[i]
            if date_filter and abs_time.date() != date_filter:
                continue

            if bbox:
                if lo < min_lon or lo > max_lon or la < min_lat or la > max_lat:
                    continue

            rel_sec = (abs_time - epoch_dt).total_seconds()

            points.append({
                "lon": round(lo, 6),
                "lat": round(la, 6),
                "alt": float(alt_m),
                "time": round(float(rel_sec), 3),
                "value": round(float(value[i]), 6),
            })

        print(
            f"{Path(path).name}: {len(points)} points "
            f"from {choice['level']} level | product_start={product_start.isoformat()}"
        )

        return points
    finally:
        close_netcdf(ds, backend)


def parse_bbox(value):
    if not value:
        return None
    parts = [float(x.strip()) for x in value.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be min_lon,min_lat,max_lon,max_lat")
    return tuple(parts)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Cesium PointPrimitive points.json from GOES GLM NetCDF files."
    )

    parser.add_argument(
        "input",
        help="Local file/folder/glob OR HTTP/HTTPS URL to .nc, archive, or simple directory listing.",
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Output points.json path, e.g. public/Nov19-glm/points.json",
    )

    parser.add_argument(
        "--date",
        default=None,
        help="Optional UTC date filter, e.g. 2022-11-19",
    )

    parser.add_argument(
        "--epoch",
        default="2022-11-19T00:00:00Z",
        help="Epoch used for relative seconds in output. Use same value in frontend.",
    )

    parser.add_argument(
        "--bbox",
        default=None,
        help="Optional filter: min_lon,min_lat,max_lon,max_lat",
    )

    parser.add_argument(
        "--alt-m",
        type=float,
        default=12000.0,
        help="Display altitude in meters for GLM points. Default: 12000.",
    )

    parser.add_argument(
        "--max-points",
        type=int,
        default=0,
        help="Optional cap. 0 means no cap. If capped, points are evenly sampled.",
    )

    args = parser.parse_args()

    epoch_dt = parse_iso_z(args.epoch)
    date_filter = parse_date(args.date) if args.date else None
    bbox = parse_bbox(args.bbox)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="glm_points_") as tmp:
        files = resolve_inputs(args.input, tmp)

        if not files:
            raise RuntimeError(f"No GLM NetCDF files found from input: {args.input}")

        print(f"Found {len(files)} NetCDF files.")

        all_points = []
        for f in files:
            try:
                pts = extract_points_from_file(
                    path=f,
                    epoch_dt=epoch_dt,
                    date_filter=date_filter,
                    bbox=bbox,
                    alt_m=args.alt_m,
                )
                all_points.extend(pts)
            except Exception as e:
                print(f"ERROR processing {f}: {e}", file=sys.stderr)

    all_points.sort(key=lambda p: p["time"])

    if args.max_points and args.max_points > 0 and len(all_points) > args.max_points:
        step = math.ceil(len(all_points) / args.max_points)
        all_points = all_points[::step]
        print(f"Downsampled to {len(all_points)} points using step={step}")

    out_path.write_text(json.dumps(all_points, indent=2), encoding="utf-8")

    print("\nDone.")
    print(f"Wrote: {out_path}")
    print(f"Point count: {len(all_points)}")

    if all_points:
        print(f"Time range seconds: {all_points[0]['time']} to {all_points[-1]['time']}")
        print(f"Lon range: {min(p['lon'] for p in all_points)} to {max(p['lon'] for p in all_points)}")
        print(f"Lat range: {min(p['lat'] for p in all_points)} to {max(p['lat'] for p in all_points)}")


if __name__ == "__main__":
    main()
