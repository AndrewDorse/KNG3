#!/usr/bin/env python3
"""
Run PALADIN sim variants that favor cheaper pair adds (dynamic sum cap, book beats, force discipline).
Prints a comparison table for one window CSV (default: btc-updown-15m-1776809700).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from paladin_engine import PaladinParams

from simulate_paladin_window import (
    forward_fill_prices,
    load_prices_by_elapsed,
    load_profit_lock_config,
    resolve_winner_from_last_prices,
    run_window,
    settled_pnl_usdc,
    window_slug_from_prices_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRICES = (
    REPO_ROOT
    / "exports"
    / "window_price_snapshots_public"
    / "20260422_011500_btc-updown-15m-1776809700_prices.csv"
)
DEFAULT_CONFIG = Path(__file__).resolve().parent / "paladin_sim_config.json"


@dataclass(frozen=True)
class Scenario:
    name: str
    pair_sum_tighten_per_fill: float = 0.0
    pair_sum_min_floor: float = 0.88
    force_hedge_respects_effective_sum: bool = False
    second_leg_book_improve_eps: float = 0.0
    target_roi_per_fill: float = 0.0


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="PALADIN price-discipline comparison on one window")
    ap.add_argument("--prices", type=Path, default=DEFAULT_PRICES)
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ap.add_argument("--budget", type=float, default=80.0)
    ap.add_argument("--pair-sum-max", type=float, default=1.0)
    ap.add_argument("--target-min-roi", type=float, default=0.05)
    ap.add_argument("--single-max", type=float, default=0.55)
    ap.add_argument("--cooldown-seconds", type=float, default=0.0)
    ap.add_argument("--dynamic-clip-max", type=float, default=10.0)
    ap.add_argument("--max-shares-per-side", type=float, default=40.0)
    ap.add_argument("--hedge-force-sec", type=float, default=45.0)
    args = ap.parse_args()

    pl = load_profit_lock_config(args.config)
    params = PaladinParams(
        profit_lock_min_shares_per_side=float(pl["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl["profit_lock_usdc_each_scenario"]),
    )
    raw = load_prices_by_elapsed(args.prices)
    series = forward_fill_prices(raw)
    slug = window_slug_from_prices_csv(args.prices)
    win, _, _ = resolve_winner_from_last_prices(series)
    mx_sh = None if args.max_shares_per_side <= 0 else float(args.max_shares_per_side)
    hf = float(args.hedge_force_sec) if args.hedge_force_sec > 0 else None

    scenarios: tuple[Scenario, ...] = (
        Scenario("A_baseline_live_style"),
        Scenario("B_tighten_sum_002pf_floor092", pair_sum_tighten_per_fill=0.002, pair_sum_min_floor=0.92),
        Scenario("C_tighten_sum_003pf_floor090", pair_sum_tighten_per_fill=0.003, pair_sum_min_floor=0.90),
        Scenario("D_tighten_sum_006pf_floor088", pair_sum_tighten_per_fill=0.006, pair_sum_min_floor=0.88),
        Scenario("E_force_respects_cap_only", force_hedge_respects_effective_sum=True),
        Scenario("F_book_beat_012eps", second_leg_book_improve_eps=0.012),
        Scenario("G_book_beat_020eps", second_leg_book_improve_eps=0.020),
        Scenario("H_combo_t004_book010", pair_sum_tighten_per_fill=0.004, pair_sum_min_floor=0.90, second_leg_book_improve_eps=0.010),
        Scenario("I_roi_esc_0004pf", target_roi_per_fill=0.0004),
        Scenario("J_combo_t005_book008_roi2pf", pair_sum_tighten_per_fill=0.005, pair_sum_min_floor=0.89, second_leg_book_improve_eps=0.008, target_roi_per_fill=0.0002),
    )

    rows: list[tuple[str, ...]] = []
    for sc in scenarios:
        st = run_window(
            series,
            budget_usdc=args.budget,
            params=params,
            pair_sum_max=args.pair_sum_max,
            single_leg_max_px=args.single_max,
            pair_only=True,
            stagger_pair_entry=True,
            stagger_hedge_force_after_seconds=hf,
            target_min_roi=args.target_min_roi,
            cooldown_seconds=args.cooldown_seconds,
            dynamic_clip_cap=args.dynamic_clip_max,
            pair_size_pick="max_feasible",
            max_shares_per_side=mx_sh,
            pair_sum_tighten_per_fill=sc.pair_sum_tighten_per_fill,
            pair_sum_min_floor=sc.pair_sum_min_floor,
            force_hedge_respects_effective_sum=sc.force_hedge_respects_effective_sum,
            second_leg_book_improve_eps=sc.second_leg_book_improve_eps,
            target_roi_per_fill=sc.target_roi_per_fill,
        )
        m = st.snapshot_metrics()
        pnl = settled_pnl_usdc(m, win)
        avg_sum = float(m["avg_up"]) + float(m["avg_down"])
        roi_min = min(float(m["roi_up"]), float(m["roi_dn"]))
        n_force = sum(1 for tr in st.trades if "force" in tr.reason)
        imb = float(m["size_up"]) - float(m["size_down"])
        rows.append(
            (
                sc.name,
                str(len(st.trades)),
                f"{st.spent_usdc:.2f}",
                f"{m['size_up']:.1f}/{m['size_down']:.1f}",
                f"{imb:+.1f}",
                f"{avg_sum:.4f}",
                f"{m['pnl_if_up_usdc']:.2f}",
                f"{m['pnl_if_down_usdc']:.2f}",
                f"{pnl:.2f}",
                f"{roi_min:.4f}",
                str(n_force),
                str(st.locked),
            )
        )

    hdr = (
        "scenario\ttrades\tspent\tup/dn_sh\timb\tavg_sum\tpnl_up\tpnl_dn\tpnl_settle*\troi_min\tn_force\tlocked"
    )
    print(f"Window: {slug} | proxy winner: {win} | budget={args.budget} pair_max={args.pair_sum_max} roi={args.target_min_roi}")
    print("(Forward-filled to 900s like prior sim; CSV may end earlier.)")
    print()
    print(hdr)
    print("-" * len(hdr.expandtabs(4)))
    for r in rows:
        print("\t".join(r))
    print()
    print("* pnl_settle = PnL if final-second price proxy picks the winner (not on-chain settlement).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
