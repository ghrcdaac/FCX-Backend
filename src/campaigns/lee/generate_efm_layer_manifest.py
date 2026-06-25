#!/usr/bin/env python3
"""
generate_efm_layer_manifest.py

Generate EFM Cesium layer manifest JSON from an EFM summary.json file.

This follows the same pattern as the GLM and NSSL sounding scripts:
- Pass input summary path as an argument.
- Pass output folder/file as an argument.
- Pass date label, product name, and optional S3 base URL.
- If --out is a folder, the script creates a default manifest filename.

Examples:

Nov19 altitude/default:
python generate_efm_layer_manifest.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/Nov19-efm/efm_cesium_outputs/summary.json \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/config \
  --date-label Nov19 \
  --product efm \
  --s3-base-url https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/REPLACE_CAMPAIGN_PATH/Nov19-efm/efm_cesium_outputs

Output:
  config/nov19_efm_layers.json

Nov19 ADC:
python generate_efm_layer_manifest.py \
  /home/dacharya/fcx-playground-backend/notebooks/tiles/public/Nov19-efm/efm_cesium_outputs_adc/summary.json \
  --out /home/dacharya/fcx-playground-backend/notebooks/tiles/config \
  --date-label Nov19 \
  --product efm_adc \
  --s3-base-url https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/REPLACE_CAMPAIGN_PATH/Nov19-efm/efm_cesium_outputs_adc

Output:
  config/nov19_efm_adc_layers.json
"""

import argparse
import json
import re
import sys
from pathlib import Path


def safe_slug(value):
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower() or "efm"


def add_src_to_path(src_path):
    src = Path(src_path).resolve()
    if not src.exists():
        raise FileNotFoundError(f"--src-path does not exist: {src}")
    sys.path.insert(0, str(src))


def resolve_output_path(out_arg, date_label, product):
    out = Path(out_arg)

    if out.suffix.lower() == ".json":
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    out.mkdir(parents=True, exist_ok=True)

    date_slug = safe_slug(date_label)
    product_slug = safe_slug(product)

    return out / f"{date_slug}_{product_slug}_layers.json"


def print_urls(title, urls):
    print(title)
    for url in urls:
        print(" ", url)


def main():
    parser = argparse.ArgumentParser(
        description="Generate EFM Cesium layer manifest JSON from summary.json."
    )

    parser.add_argument(
        "summary",
        help="Path to EFM summary.json, e.g. public/Nov19-efm/efm_cesium_outputs/summary.json",
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Output manifest .json file or folder. If folder, writes <date>_<product>_layers.json.",
    )

    parser.add_argument(
        "--date-label",
        required=True,
        help="Date label used in output filename, e.g. Nov18 or Nov19.",
    )

    parser.add_argument(
        "--product",
        default="efm",
        help="Product label used in output filename, e.g. efm or efm_adc. Default: efm.",
    )

    parser.add_argument(
        "--s3-base-url",
        default=None,
        help="Optional S3 base URL to use in manifest and printed S3 CZML URLs.",
    )

    parser.add_argument(
        "--src-path",
        default="/home/dacharya/fcx-playground-backend/src",
        help="Path to project src folder. Default: /home/dacharya/fcx-playground-backend/src",
    )

    parser.add_argument(
        "--print-local",
        action="store_true",
        help="Print local CZML URLs from processor.preprocess(summary).",
    )

    parser.add_argument(
        "--print-s3",
        action="store_true",
        help="Print S3 CZML URLs from processor.preprocess(summary, s3_base_url=...).",
    )

    args = parser.parse_args()

    summary_path = Path(args.summary)
    if not summary_path.exists():
        raise FileNotFoundError(f"summary.json not found: {summary_path}")

    add_src_to_path(args.src_path)

    from fcx_playground.fcx_dataprocess.czml_efm import (
        EfmCZMLDataProcess,
        build_efm_layer_manifest,
    )

    out_path = resolve_output_path(args.out, args.date_label, args.product)

    processor = EfmCZMLDataProcess()
    summary = processor.ingest(str(summary_path), type="local")

    if args.print_local:
        local_viz = processor.preprocess(summary)
        print_urls("Local CZML URLs:", local_viz.get("czml_urls", []))

    if args.s3_base_url and args.print_s3:
        s3_viz = processor.preprocess(summary, s3_base_url=args.s3_base_url)
        print_urls("\nS3 CZML URLs:", s3_viz.get("czml_urls", []))

    manifest = build_efm_layer_manifest(
        str(summary_path),
        s3_base_url=args.s3_base_url,
    )

    if isinstance(manifest, dict):
        manifest.setdefault("metadata", {})
        manifest["metadata"].update({
            "date_label": args.date_label,
            "product": args.product,
            "summary": str(summary_path),
            "s3_base_url": args.s3_base_url,
        })

    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("\nDone.")
    print("Summary:", summary_path)
    print("Manifest:", out_path.resolve())

    if args.s3_base_url:
        print("S3 base URL:", args.s3_base_url)


if __name__ == "__main__":
    main()
