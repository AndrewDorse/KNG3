#!/usr/bin/env python3
"""
Sweep market pair-sum cap (pair_sum_max) and optional blended book cap (max_blended_pair_avg_sum).
Uses round-2 style backbone: tighten 0.004, floor 0.90, bypass 10, relax 60s, else live defaults.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from paladin_engine import PaladinParams

from batch_sim_recent_windows import max_elapsed_in_prices_csv
from simulate_paladin_window import (
    DEFAULT_PROFIT_LOCK_CONFIG,
    forward_fill_prices,
    load_prices_by_elapsed,
    load_profit_lock_config,
    resolve_winner_from_last_prices,
    run_window,
    settled_pnl_usdc,
    window_slug_from_prices_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORTS = REPO_ROOT / "exports" / "window_price_snapshots_public"


def pick_windows(exports_dir: Path, count: int, min_max_elapsed: int) -> list[Path]:
    all_csv = sorted(
        exports_dir.glob("*_prices.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    picked: list[Path] = []
    seen: set[str] = set()
    for p in all_csv:
        slug = window_slug_from_prices_csv(p)
        if slug in seen or "unknown" in slug:
            continue
        if max_elapsed_in_prices_csv(p) < min_max_elapsed:
            continue
        seen.add(slug)
        picked.append(p)
        if len(picked) >= count:
            break
    return picked


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exports-dir", type=Path, default=DEFAULT_EXPORTS)
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--min-max-elapsed", type=int, default=800)
    args = ap.parse_args()

    picked = pick_windows(args.exports_dir, args.count, args.min_max_elapsed)
    if not picked:
        return 1

    pl = load_profit_lock_config(DEFAULT_PROFIT_LOCK_CONFIG)
    params = PaladinParams(
        profit_lock_min_shares_per_side=float(pl["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl["profit_lock_usdc_each_scenario"]),
    )

    pair_caps = [0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.0]
    blended_opts: list[float | None] = [None, 1.04, 1.02, 1.0, 0.98, 0.96, 0.95, 0.92]

    rows: list[tuple[str, float, float, float, float, float, float, int]] = []

    for pmax in pair_caps:
        for blend in blended_opts:
            tag = f"pm{pmax}_blend{blend if blend is not None else 'off'}"
            total_pnl = spent = 0.0
            nw = len(picked)
            n_win = n_loss = 0
            imb_sum = 0.0
            locks = 0
            for prices_path in picked:
                raw = load_prices_by_elapsed(prices_path)
                series = forward_fill_prices(raw)
                st = run_window(
                    series,
                    budget_usdc=80.0,
                    params=params,
                    pair_sum_max=pmax,
                    single_leg_max_px=0.55,
                    pair_only=True,
                    stagger_pair_entry=True,
                    stagger_hedge_force_after_seconds=45.0,
                    target_min_roi=0.05,
                    cooldown_seconds=0.0,
                    dynamic_clip_cap=10.0,
                    pair_size_pick="max_feasible",
                    max_shares_per_side=40.0,
                    pair_sum_tighten_per_fill=0.004,
                    pair_sum_min_floor=0.90,
                    pending_hedge_bypass_imbalance_shares=10.0,
                    discipline_relax_after_forced_sec=60.0,
                    max_blended_pair_avg_sum=blend,
                )
                m = st.snapshot_metrics()
                win, _, _ = resolve_winner_from_last_prices(series)
                pnl = settled_pnl_usdc(m, win)
                total_pnl += pnl
                spent += st.spent_usdc
                if pnl > 1e-6:
                    n_win += 1
                elif pnl < -1e-6:
                    n_loss += 1
                imb_sum += abs(float(m["size_up"]) - float(m["size_down"]))
                if st.locked:
                    locks += 1
            wl = n_win + n_loss
            wr = n_win / wl if wl else 0.0
            pr = total_pnl / spent if spent > 1e-9 else float("nan")
            rows.append((tag, wr, total_pnl, pr, spent / nw, imb_sum / nw, pmax, blend or 0.0, locks))

    rows.sort(key=lambda x: (-x[1], -x[2]))
    print(f"Sum-formula sweep | windows={nw} | backbone t=0.004 bypass=10 relax=60")
    print("tag\twin%\tsum_pnl\tport_roi\tmean_spent\tmean_|imb|\tpair_max\tblend_cap\tlocks")
    for tag, wr, tp, pr, ms, mi, pm, bl, lk in rows[:25]:
        print(
            f"{tag}\t{wr:.4f}\t{tp:.2f}\t{pr:.6f}\t{ms:.2f}\t{mi:.2f}\t{pm}\t{bl or 'off'}\t{lk}"
        )
    print()
    print("pair_max = max allowed pm_up+pm_down for disciplined legs; blend_cap = max avg_up+avg_down after a fill when both legs exist (off = no extra gate).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
