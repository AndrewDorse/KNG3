#!/usr/bin/env python3
"""
Run the same PALADIN window sim as simulate_paladin_window on the N most recent
unique windows (by *_prices.csv mtime). Skips exports whose CSV never reaches
min-max-elapsed (default 800s). Prints per-window rows + totals.
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

from paladin_engine import PaladinParams

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


def max_elapsed_in_prices_csv(path: Path) -> int:
    """Largest elapsed_sec in the export (no forward-fill). Empty file → -1."""
    mx = -1
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mx = max(mx, int(float(row["elapsed_sec"])))
    return mx


def main() -> int:
    ap = argparse.ArgumentParser(description="Batch PALADIN sim on recent windows")
    ap.add_argument("--exports-dir", type=Path, default=DEFAULT_EXPORTS)
    ap.add_argument("--count", type=int, default=20, help="Number of distinct windows (by slug).")
    ap.add_argument(
        "--min-max-elapsed",
        type=int,
        default=800,
        help="Skip exports whose max elapsed_sec in CSV is below this (coverage gate).",
    )
    ap.add_argument("--budget", type=float, default=500.0)
    ap.add_argument("--pair-sum-max", type=float, default=0.99)
    ap.add_argument("--target-min-roi", type=float, default=0.03)
    ap.add_argument(
        "--first-leg-max",
        type=float,
        default=0.54,
        help="Stagger: max mid on first leg (simulate_paladin --single-max).",
    )
    ap.add_argument(
        "--stagger-pair",
        action="store_true",
        help="Staggered pair entry (cheaper side first; second leg when sum+ROI ok).",
    )
    ap.add_argument(
        "--live-paladin",
        action="store_true",
        help="Align with main.py PALADIN defaults: budget=80, pair_sum=1, target_roi=5%%, clip<=10, cooldown=0s, stagger, first_leg=0.55.",
    )
    ap.add_argument(
        "--stagger-hedge-force-sec",
        type=float,
        default=-1.0,
        help="Force stagger 2nd leg after N sim s past hedge-ready (-1=45 if stagger else off, 0=off).",
    )
    ap.add_argument(
        "--max-shares-per-side",
        type=float,
        default=40.0,
        help="Stop adding when both legs reach this size (0=no cap).",
    )
    ap.add_argument(
        "--no-print-worst-buys",
        action="store_true",
        help="Skip per-buy table for the worst-PnL window (default: print it).",
    )
    ap.add_argument("--cooldown-seconds", type=float, default=2.0)
    ap.add_argument("--dynamic-clip-max", type=float, default=15.0)
    ap.add_argument(
        "--pair-size-pick",
        choices=["ascending", "max_feasible"],
        default="max_feasible",
    )
    ap.add_argument("--config", type=Path, default=DEFAULT_PROFIT_LOCK_CONFIG)
    ap.add_argument("--roi-lock-each", type=float, default=None)
    ap.add_argument("--profit-lock-min-shares", type=float, default=None)
    ap.add_argument("--no-usd-profit-lock", action="store_true")
    ap.add_argument("--profit-lock-usdc-each", type=float, default=None)
    args = ap.parse_args()
    if args.live_paladin:
        args.budget = 80.0
        args.pair_sum_max = 1.0
        args.target_min_roi = 0.05
        args.first_leg_max = 0.55
        args.stagger_pair = True
        args.cooldown_seconds = 0.0
        args.dynamic_clip_max = 10.0

    if args.stagger_hedge_force_sec < 0:
        hedge_force: float | None = 45.0 if args.stagger_pair else None
    elif args.stagger_hedge_force_sec == 0:
        hedge_force = None
    else:
        hedge_force = float(args.stagger_hedge_force_sec)

    max_sh_per_side: float | None = None if args.max_shares_per_side <= 0 else float(args.max_shares_per_side)

    pl_cfg = load_profit_lock_config(args.config)
    roi_lock = float(args.roi_lock_each) if args.roi_lock_each is not None else pl_cfg["roi_lock_min_each"]
    min_sh_lock = (
        float(args.profit_lock_min_shares)
        if args.profit_lock_min_shares is not None
        else pl_cfg["profit_lock_min_shares_per_side"]
    )
    if args.no_usd_profit_lock:
        usd_lock_thr = float("inf")
    elif args.profit_lock_usdc_each is not None:
        usd_lock_thr = float(args.profit_lock_usdc_each)
    else:
        usd_lock_thr = float(pl_cfg["profit_lock_usdc_each_scenario"])

    params = PaladinParams(
        profit_lock_min_shares_per_side=min_sh_lock,
        roi_lock_min_each=roi_lock,
        profit_lock_usdc_each_scenario=usd_lock_thr,
    )

    all_csv = sorted(
        args.exports_dir.glob("*_prices.csv"),
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
        if max_elapsed_in_prices_csv(p) < args.min_max_elapsed:
            skipped_short += 1
            continue
        seen.add(slug)
        picked.append(p)
        if len(picked) >= args.count:
            break

    print(
        f"Batch: {len(picked)} windows (max elapsed>={args.min_max_elapsed}s in CSV; "
        f"skipped_short={skipped_short}) | budget=${args.budget:.2f} | pair-only | "
        f"stagger={args.stagger_pair} first_leg_max={args.first_leg_max} | "
        f"hedge_force_s={hedge_force} | max_sh/side={max_sh_per_side} | "
        f"pair_sum_max={args.pair_sum_max} target_min_roi={args.target_min_roi} | "
        f"dynamic_clip_max={args.dynamic_clip_max} | "
        f"pair_size_pick={args.pair_size_pick} | cooldown={args.cooldown_seconds}s"
    )
    print(
        f"Profit lock: roi>={roi_lock:.0%} each leg (>={min_sh_lock:.0f} sh/side) | "
        f"USD>={'off' if not math.isfinite(usd_lock_thr) else f'${usd_lock_thr:.2f}'} each scenario"
    )
    print()
    hdr = (
        "slug\twinner\tlast_up\tlast_dn\tspent_usdc\ttrades\tlocked\tlock_reason\t"
        "sz_up\troi_min\tpnl_settled\tpnl_if_up\tpnl_if_dn"
    )
    print(hdr)
    print("-" * len(hdr.expandtabs(8)))

    total_spent = 0.0
    total_pnl = 0.0
    locked_n = 0
    n_win = 0
    n_loss = 0
    n_flat = 0
    roi_mins: list[float] = []
    window_rois: list[float] = []
    worst_pnl = float("inf")
    worst: dict[str, object] = {}

    for prices_path in picked:
        raw = load_prices_by_elapsed(prices_path)
        series = forward_fill_prices(raw)
        st = run_window(
            series,
            budget_usdc=args.budget,
            params=params,
            pair_sum_max=args.pair_sum_max,
            single_leg_max_px=args.first_leg_max,
            pair_only=True,
            stagger_pair_entry=args.stagger_pair,
            stagger_hedge_force_after_seconds=hedge_force,
            target_min_roi=args.target_min_roi,
            cooldown_seconds=args.cooldown_seconds,
            dynamic_clip_cap=args.dynamic_clip_max,
            pair_size_pick=args.pair_size_pick,  # type: ignore[arg-type]
            max_shares_per_side=max_sh_per_side,
        )
        m = st.snapshot_metrics()
        slug = window_slug_from_prices_csv(prices_path)
        roi_min = min(m["roi_up"], m["roi_dn"])
        win, lu, ld = resolve_winner_from_last_prices(series)
        pnl = settled_pnl_usdc(m, win)
        total_spent += st.spent_usdc
        total_pnl += pnl
        if pnl > 1e-6:
            n_win += 1
        elif pnl < -1e-6:
            n_loss += 1
        else:
            n_flat += 1
        roi_mins.append(roi_min)
        if st.spent_usdc > 1e-9:
            window_rois.append(pnl / st.spent_usdc)
        if pnl < worst_pnl:
            worst_pnl = pnl
            worst = {
                "slug": slug,
                "pnl": pnl,
                "spent": st.spent_usdc,
                "winner": win,
                "last_up": lu,
                "last_dn": ld,
                "trades": len(st.trades),
                "locked": st.locked,
                "lock_reason": (st.lock_reason or "").replace("\t", " ")[:80],
                "roi_min": roi_min,
                "pnl_if_up": m["pnl_if_up_usdc"],
                "pnl_if_dn": m["pnl_if_down_usdc"],
                "prices_path": prices_path,
                "trades_detail": list(st.trades),
            }
        if st.locked:
            locked_n += 1
        reason = (st.lock_reason or "").replace("\t", " ")[:60]
        print(
            f"{slug}\t{win}\t{lu:.4f}\t{ld:.4f}\t{st.spent_usdc:.2f}\t{len(st.trades)}\t{st.locked}\t{reason}\t"
            f"{m['size_up']:.2f}\t{roi_min:.4f}\t{pnl:.2f}\t{m['pnl_if_up_usdc']:.2f}\t{m['pnl_if_down_usdc']:.2f}"
        )

    print()
    print("=== TOTALS (sum over windows; pnl_settled from last snapshot up vs down price) ===")
    print(f"windows_run\t{len(picked)}")
    print(f"windows_locked_early\t{locked_n}")
    print(f"wins\t{n_win}\tlosses\t{n_loss}\tbreakeven\t{n_flat}")
    wl = n_win + n_loss
    if wl > 0:
        print(f"win_rate_w_over_w_plus_l\t{n_win / wl:.4f}")
    print(f"sum_spent_usdc\t{total_spent:.2f}")
    print(f"sum_pnl_settled_usdc\t{total_pnl:.2f}")
    nwin = len(picked)
    if nwin:
        print(f"avg_pnl_per_window_usdc\t{total_pnl / nwin:.4f}")
    if total_spent > 1e-9:
        print(f"portfolio_roi_sum_pnl_over_sum_spent\t{total_pnl / total_spent:.6f}")
    if window_rois:
        print(f"avg_window_roi_mean_pnl_over_spent\t{sum(window_rois) / len(window_rois):.6f}")
    if roi_mins:
        print(f"roi_min_mean\t{sum(roi_mins) / len(roi_mins):.4f}")
        print(f"roi_min_min\t{min(roi_mins):.4f}")
        print(f"roi_min_max\t{max(roi_mins):.4f}")
    if worst:
        sp = float(worst["spent"])
        wr = float(worst["pnl"]) / sp if sp > 1e-9 else float("nan")
        print()
        print("=== WORST WINDOW (min pnl_settled) ===")
        print(
            f"slug\t{worst['slug']}\n"
            f"pnl_settled\t{float(worst['pnl']):.2f}\n"
            f"spent_usdc\t{sp:.2f}\n"
            f"window_roi\t{wr:.6f}\n"
            f"winner\t{worst['winner']}\n"
            f"last_up\t{float(worst['last_up']):.4f}\tlast_dn\t{float(worst['last_dn']):.4f}\n"
            f"trades\t{int(worst['trades'])}\tlocked\t{worst['locked']}\n"
            f"lock_reason\t{worst['lock_reason']}\n"
            f"roi_min\t{float(worst['roi_min']):.4f}\n"
            f"pnl_if_up\t{float(worst['pnl_if_up']):.2f}\tpnl_if_dn\t{float(worst['pnl_if_dn']):.2f}"
        )
        if not args.no_print_worst_buys:
            trades_detail = worst.get("trades_detail")
            wpath = worst.get("prices_path")
            if trades_detail and wpath is not None:
                raw_w = load_prices_by_elapsed(Path(wpath))
                series_w = forward_fill_prices(raw_w)
                print()
                print("=== WORST WINDOW: per simulated buy (mids at trade second) ===")
                h = "i\telapsed\tside\tshares\tprice\tnotional\treason\tpm_up\tpm_dn\tsum"
                print(h)
                print("-" * len(h.expandtabs(8)))
                for i, tr in enumerate(trades_detail):
                    tix = max(0, min(tr.elapsed_sec, len(series_w) - 1))
                    pu, pd = series_w[tix]
                    print(
                        f"{i}\t{tr.elapsed_sec}\t{tr.side}\t{tr.shares:.4f}\t{tr.price:.4f}\t"
                        f"{tr.notional:.2f}\t{tr.reason}\t{pu:.4f}\t{pd:.4f}\t{pu + pd:.4f}"
                    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
