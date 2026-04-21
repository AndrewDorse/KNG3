#!/usr/bin/env python3
"""
Replay PALADIN bucket CSV (prices + inventory) through paladin_engine for analysis / simulation.

Usage:
  python PALADIN/simulate_paladin.py
  python PALADIN/simulate_paladin.py --csv path/to/trace.csv
  python PALADIN/simulate_paladin.py --csv path/to/trace.csv --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from paladin_engine import (
    PaladinParams,
    analyze_snapshot,
    control_hint,
    load_bucket_csv,
    metrics_to_dict,
    validate_snapshot_against_row,
)

ROOT = Path(__file__).resolve().parent
DEFAULT_CSV = ROOT / "data" / "example_bucket_trace.csv"


def main() -> int:
    ap = argparse.ArgumentParser(description="PALADIN bucket CSV replay (simulation / analysis)")
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Bucket trace CSV")
    ap.add_argument("--json", action="store_true", help="Print one JSON object per row")
    ap.add_argument(
        "--validate-roi",
        action="store_true",
        help="Assert engine ROIs match roi_if_up / roi_if_down columns (within tolerance)",
    )
    args = ap.parse_args()
    path: Path = args.csv
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        return 2

    params = PaladinParams()
    rows = load_bucket_csv(path)
    if not rows:
        print("No rows loaded.", file=sys.stderr)
        return 1

    for row in rows:
        if args.validate_roi:
            try:
                validate_snapshot_against_row(row)
            except AssertionError as e:
                print(f"[validate_roi] {row.bucket_label}: {e}", file=sys.stderr)
                return 1
        m = analyze_snapshot(row, params)
        hint = control_hint(m, params)
        payload = {
            "metrics": metrics_to_dict(m),
            "control": {
                "allow_new_inventory": hint.allow_new_inventory,
                "rebalance_side": hint.rebalance_side,
                "note": hint.note,
            },
        }
        if args.json:
            print(json.dumps(payload, default=str))
        else:
            lock = "LOCK" if m.profit_locked else "trade"
            imb = "ok" if m.imbalance_ok else "skew"
            print(
                f"{m.bucket_label:8} | {lock:5} {imb:4} | "
                f"roi_up={m.roi_if_up:+.4f} roi_dn={m.roi_if_down:+.4f} | "
                f"imb={m.share_imbalance:+.1f} max_d={m.max_disbalance_shares:.1f} | "
                f"{hint.note}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
