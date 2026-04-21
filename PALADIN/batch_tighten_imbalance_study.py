#!/usr/bin/env python3
"""
Compare PALADIN variants: pair-sum tightening vs imbalance/time-based hedge relaxation.
Picks N recent windows (same coverage gate as batch_sim_recent_windows), runs each variant,
prints an aggregate table (PnL, ROI, imbalance stats).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import math
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


@dataclass(frozen=True)
class Variant:
    name: str
    pair_sum_tighten_per_fill: float = 0.0
    pair_sum_min_floor: float = 0.88
    second_leg_book_improve_eps: float = 0.0
    pending_hedge_bypass_imbalance_shares: float | None = None
    discipline_relax_after_forced_sec: float | None = None


def pick_windows(exports_dir: Path, count: int, min_max_elapsed: int) -> tuple[list[Path], int]:
    all_csv = sorted(
        exports_dir.glob("*_prices.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    picked: list[Path] = []
    seen: set[str] = set()
    skipped_short = 0
    for p in all_csv:
        slug = window_slug_from_prices_csv(p)
        if slug in seen or "unknown" in slug:
            continue
        if max_elapsed_in_prices_csv(p) < min_max_elapsed:
            skipped_short += 1
            continue
        seen.add(slug)
        picked.append(p)
        if len(picked) >= count:
            break
    return picked, skipped_short


def main() -> int:
    ap = argparse.ArgumentParser(description="Tighten vs imbalance-relax study (batch PALADIN)")
    ap.add_argument("--exports-dir", type=Path, default=DEFAULT_EXPORTS)
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--min-max-elapsed", type=int, default=800)
    ap.add_argument("--config", type=Path, default=DEFAULT_PROFIT_LOCK_CONFIG)
    args = ap.parse_args()

    budget = 80.0
    pair_sum_max = 1.0
    target_min_roi = 0.05
    first_leg_max = 0.55
    cooldown = 0.0
    dynamic_clip = 10.0
    max_sh = 40.0
    hedge_force = 45.0

    pl_cfg = load_profit_lock_config(args.config)
    params = PaladinParams(
        profit_lock_min_shares_per_side=float(pl_cfg["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl_cfg["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl_cfg["profit_lock_usdc_each_scenario"]),
    )

    picked, skipped_short = pick_windows(args.exports_dir, args.count, args.min_max_elapsed)
    if not picked:
        print("No windows picked; check exports dir and --min-max-elapsed.")
        return 1

    variants: tuple[Variant, ...] = (
        Variant("A_baseline_no_tighten"),
        Variant("B_tighten004_floor090", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90),
        Variant("C_tighten004_bypass10", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90, pending_hedge_bypass_imbalance_shares=10.0),
        Variant("D_tighten004_bypass8", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90, pending_hedge_bypass_imbalance_shares=8.0),
        Variant("E_tighten004_relax30s", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90, discipline_relax_after_forced_sec=30.0),
        Variant("F_tighten004_relax60s", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90, discipline_relax_after_forced_sec=60.0),
        Variant("G_tighten004_book010_bypass10", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90, second_leg_book_improve_eps=0.010, pending_hedge_bypass_imbalance_shares=10.0),
        Variant("H_tighten003_floor090_bypass10", pair_sum_tighten_per_fill=0.003, pair_sum_min_floor=0.90, pending_hedge_bypass_imbalance_shares=10.0),
    )

    print(
        f"Study: {len(picked)} windows | skipped_short={skipped_short} | "
        f"budget={budget} pair_max={pair_sum_max} roi={target_min_roi} | "
        f"stagger hedge_force={hedge_force}s | max_sh/side={max_sh} | clip={dynamic_clip}"
    )
    print()

    rows: list[str] = []
    hdr = (
        "variant\twins\tlosses\twin%\tsum_pnl\tavg_pnl\tport_roi\t"
        "mean_spent\tmean_|imb|\tpct_imb>5\tpct_imb>10\tmean_roi_min\tlocked_n"
    )
    rows.append(hdr)
    rows.append("-" * len(hdr.expandtabs(4)))

    for v in variants:
        total_pnl = 0.0
        total_spent = 0.0
        n_win = n_loss = n_flat = 0
        locked_n = 0
        imb_abs_list: list[float] = []
        imb_gt5 = imb_gt10 = 0
        roi_mins: list[float] = []

        for prices_path in picked:
            raw = load_prices_by_elapsed(prices_path)
            series = forward_fill_prices(raw)
            st = run_window(
                series,
                budget_usdc=budget,
                params=params,
                pair_sum_max=pair_sum_max,
                single_leg_max_px=first_leg_max,
                pair_only=True,
                stagger_pair_entry=True,
                stagger_hedge_force_after_seconds=hedge_force,
                target_min_roi=target_min_roi,
                cooldown_seconds=cooldown,
                dynamic_clip_cap=dynamic_clip,
                pair_size_pick="max_feasible",
                max_shares_per_side=max_sh,
                pair_sum_tighten_per_fill=v.pair_sum_tighten_per_fill,
                pair_sum_min_floor=v.pair_sum_min_floor,
                second_leg_book_improve_eps=v.second_leg_book_improve_eps,
                pending_hedge_bypass_imbalance_shares=v.pending_hedge_bypass_imbalance_shares,
                discipline_relax_after_forced_sec=v.discipline_relax_after_forced_sec,
            )
            m = st.snapshot_metrics()
            win, _, _ = resolve_winner_from_last_prices(series)
            pnl = settled_pnl_usdc(m, win)
            spent = st.spent_usdc
            total_pnl += pnl
            total_spent += spent
            if pnl > 1e-6:
                n_win += 1
            elif pnl < -1e-6:
                n_loss += 1
            else:
                n_flat += 1
            if st.locked:
                locked_n += 1
            imb = abs(float(m["size_up"]) - float(m["size_down"]))
            imb_abs_list.append(imb)
            if imb > 5 + 1e-9:
                imb_gt5 += 1
            if imb > 10 + 1e-9:
                imb_gt10 += 1
            roi_mins.append(min(float(m["roi_up"]), float(m["roi_dn"])))

        nw = len(picked)
        wl = n_win + n_loss
        win_rate = (n_win / wl) if wl else 0.0
        mean_imb = sum(imb_abs_list) / nw
        pct5 = imb_gt5 / nw
        pct10 = imb_gt10 / nw
        mean_roi_min = sum(roi_mins) / nw
        avg_pnl = total_pnl / nw
        port_roi = total_pnl / total_spent if total_spent > 1e-9 else float("nan")
        mean_spent = total_spent / nw

        rows.append(
            f"{v.name}\t{n_win}\t{n_loss}\t{win_rate:.4f}\t{total_pnl:.2f}\t{avg_pnl:.4f}\t{port_roi:.6f}\t"
            f"{mean_spent:.2f}\t{mean_imb:.2f}\t{pct5:.4f}\t{pct10:.4f}\t{mean_roi_min:.4f}\t{locked_n}"
        )
        # keep mean_win_roi for extended debug — omit from narrow table

    for line in rows:
        print(line)
    print()
    print("port_roi = sum_pnl / sum_spent across windows; avg_pnl = sum_pnl / n_windows.")
    print("|imb| = |size_up - size_down| at end of replay (forward-filled 900s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
