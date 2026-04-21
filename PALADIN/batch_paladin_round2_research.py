#!/usr/bin/env python3
"""
Round-2 PALADIN batch research: grid over tighten / bypass / floor / relax / gates / timing.
Sorts variants by win rate then sum PnL (same 200-window protocol as batch_tighten_imbalance_study).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Literal

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
    pair_sum_max: float = 1.0
    target_min_roi: float = 0.05
    hedge_force_sec: float = 45.0
    first_leg_max: float = 0.55
    pair_size_pick: Literal["ascending", "max_feasible"] = "max_feasible"
    target_roi_per_fill: float = 0.0


@dataclass
class ResultRow:
    v: Variant
    n_win: int
    n_loss: int
    win_rate: float
    sum_pnl: float
    port_roi: float
    mean_spent: float
    mean_imb: float
    pct_imb_gt5: float
    mean_roi_min: float
    locked_n: int


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


def run_variant(
    v: Variant,
    picked: list[Path],
    *,
    budget: float,
    cooldown: float,
    dynamic_clip: float,
    max_sh: float,
    params: PaladinParams,
) -> ResultRow:
    total_pnl = 0.0
    total_spent = 0.0
    n_win = n_loss = 0
    locked_n = 0
    imb_abs_list: list[float] = []
    imb_gt5 = 0
    roi_mins: list[float] = []

    for prices_path in picked:
        raw = load_prices_by_elapsed(prices_path)
        series = forward_fill_prices(raw)
        st = run_window(
            series,
            budget_usdc=budget,
            params=params,
            pair_sum_max=v.pair_sum_max,
            single_leg_max_px=v.first_leg_max,
            pair_only=True,
            stagger_pair_entry=True,
            stagger_hedge_force_after_seconds=v.hedge_force_sec,
            target_min_roi=v.target_min_roi,
            cooldown_seconds=cooldown,
            dynamic_clip_cap=dynamic_clip,
            pair_size_pick=v.pair_size_pick,
            max_shares_per_side=max_sh,
            pair_sum_tighten_per_fill=v.pair_sum_tighten_per_fill,
            pair_sum_min_floor=v.pair_sum_min_floor,
            second_leg_book_improve_eps=v.second_leg_book_improve_eps,
            pending_hedge_bypass_imbalance_shares=v.pending_hedge_bypass_imbalance_shares,
            discipline_relax_after_forced_sec=v.discipline_relax_after_forced_sec,
            target_roi_per_fill=v.target_roi_per_fill,
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
        if st.locked:
            locked_n += 1
        imb = abs(float(m["size_up"]) - float(m["size_down"]))
        imb_abs_list.append(imb)
        if imb > 5 + 1e-9:
            imb_gt5 += 1
        roi_mins.append(min(float(m["roi_up"]), float(m["roi_dn"])))

    nw = len(picked)
    wl = n_win + n_loss
    win_rate = (n_win / wl) if wl else 0.0
    port_roi = total_pnl / total_spent if total_spent > 1e-9 else float("nan")
    return ResultRow(
        v=v,
        n_win=n_win,
        n_loss=n_loss,
        win_rate=win_rate,
        sum_pnl=total_pnl,
        port_roi=port_roi,
        mean_spent=total_spent / nw,
        mean_imb=sum(imb_abs_list) / nw,
        pct_imb_gt5=imb_gt5 / nw,
        mean_roi_min=sum(roi_mins) / nw,
        locked_n=locked_n,
    )


def build_variants() -> list[Variant]:
    out: list[Variant] = []
    # Reference baselines
    out.append(Variant("ref_baseline"))
    out.append(
        Variant(
            "ref_t004_bp10_f090",
            pair_sum_tighten_per_fill=0.004,
            pair_sum_min_floor=0.90,
            pending_hedge_bypass_imbalance_shares=10.0,
        )
    )
    out.append(
        Variant(
            "ref_t004_relax60",
            pair_sum_tighten_per_fill=0.004,
            pair_sum_min_floor=0.90,
            discipline_relax_after_forced_sec=60.0,
        )
    )

    # Core grid (compact): tighten × bypass × floor
    for tf, bp, fl in product(
        [0.0035, 0.004, 0.0045],
        [7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        [0.89, 0.90, 0.91],
    ):
        name = f"g_tf{tf}_bp{int(bp)}_fl{str(fl).replace('.', 'p')}"
        out.append(
            Variant(
                name,
                pair_sum_tighten_per_fill=tf,
                pair_sum_min_floor=fl,
                pending_hedge_bypass_imbalance_shares=bp,
            )
        )

    # Tighten + bypass + time relax
    for tf, bp, rel in product(
        [0.0035, 0.004, 0.0045],
        [9.0, 10.0, 11.0],
        [30.0, 45.0, 60.0, 90.0],
    ):
        out.append(
            Variant(
                f"combo_tf{tf}_bp{int(bp)}_r{int(rel)}",
                pair_sum_tighten_per_fill=tf,
                pair_sum_min_floor=0.90,
                pending_hedge_bypass_imbalance_shares=bp,
                discipline_relax_after_forced_sec=rel,
            )
        )

    # Hedge timer + pair cap + target ROI (on t004 bp10 backbone)
    for hf, pmax, troi in product(
        [40.0, 45.0, 50.0],
        [0.99, 1.0],
        [0.045, 0.05, 0.055],
    ):
        out.append(
            Variant(
                f"hf{int(hf)}_pm{str(pmax).replace('.', 'p')}_roi{str(troi).replace('.', 'p')}",
                pair_sum_tighten_per_fill=0.004,
                pair_sum_min_floor=0.90,
                pending_hedge_bypass_imbalance_shares=10.0,
                pair_sum_max=pmax,
                target_min_roi=troi,
                hedge_force_sec=hf,
            )
        )

    # Clip pick + mild ROI-per-fill on best-ish manual point
    out.append(
        Variant(
            "t004_bp10_asc",
            pair_sum_tighten_per_fill=0.004,
            pair_sum_min_floor=0.90,
            pending_hedge_bypass_imbalance_shares=10.0,
            pair_size_pick="ascending",
        )
    )
    out.append(
        Variant(
            "t004_bp10_r60_asc",
            pair_sum_tighten_per_fill=0.004,
            pair_sum_min_floor=0.90,
            pending_hedge_bypass_imbalance_shares=10.0,
            discipline_relax_after_forced_sec=60.0,
            pair_size_pick="ascending",
        )
    )
    for rpf in [0.0001, 0.0002, 0.0003]:
        out.append(
            Variant(
                f"t004_bp10_rpf{rpf}",
                pair_sum_tighten_per_fill=0.004,
                pair_sum_min_floor=0.90,
                pending_hedge_bypass_imbalance_shares=10.0,
                target_roi_per_fill=rpf,
            )
        )
    # Softer first leg (more entries) with discipline
    for flx in [0.56, 0.58]:
        out.append(
            Variant(
                f"t004_bp10_1st{flx}".replace(".", "p"),
                pair_sum_tighten_per_fill=0.004,
                pair_sum_min_floor=0.90,
                pending_hedge_bypass_imbalance_shares=10.0,
                first_leg_max=flx,
            )
        )

    # Dedup by name (grid may duplicate - use dict)
    by_name: dict[str, Variant] = {v.name: v for v in out}
    return list(by_name.values())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exports-dir", type=Path, default=DEFAULT_EXPORTS)
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--min-max-elapsed", type=int, default=800)
    ap.add_argument("--config", type=Path, default=DEFAULT_PROFIT_LOCK_CONFIG)
    ap.add_argument("--top", type=int, default=18, help="Print this many rows after sorting.")
    args = ap.parse_args()

    budget = 80.0
    cooldown = 0.0
    dynamic_clip = 10.0
    max_sh = 40.0

    pl_cfg = load_profit_lock_config(args.config)
    params = PaladinParams(
        profit_lock_min_shares_per_side=float(pl_cfg["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl_cfg["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl_cfg["profit_lock_usdc_each_scenario"]),
    )

    picked, skipped = pick_windows(args.exports_dir, args.count, args.min_max_elapsed)
    if not picked:
        print("No windows picked.")
        return 1

    variants = build_variants()
    print(
        f"Round-2 research | windows={len(picked)} skipped_short={skipped} | "
        f"variants={len(variants)} | budget={budget} clip={dynamic_clip} max_sh={max_sh}"
    )
    print()

    results: list[ResultRow] = []
    for i, v in enumerate(variants):
        results.append(
            run_variant(
                v,
                picked,
                budget=budget,
                cooldown=cooldown,
                dynamic_clip=dynamic_clip,
                max_sh=max_sh,
                params=params,
            )
        )
        if (i + 1) % 25 == 0:
            print(f"  ... {i + 1}/{len(variants)} variants", flush=True)

    # Sort: win rate primary, sum_pnl secondary, lower imbalance tertiary
    results.sort(key=lambda r: (-r.win_rate, -r.sum_pnl, r.mean_imb, -r.port_roi))

    hdr = (
        "rank\tvariant\twin%\tsum_pnl\tport_roi\tmean_spent\tmean_|imb|\tpct_imb>5\tlocks\t"
        "tf\tfloor\tbypass\trelax\thf\tpmax\troi\t1st\tpick\trpf"
    )
    print(hdr)
    print("-" * len(hdr.expandtabs(4)))

    for rank, r in enumerate(results[: args.top], start=1):
        v = r.v
        print(
            f"{rank}\t{v.name}\t{r.win_rate:.4f}\t{r.sum_pnl:.2f}\t{r.port_roi:.6f}\t"
            f"{r.mean_spent:.2f}\t{r.mean_imb:.2f}\t{r.pct_imb_gt5:.4f}\t{r.locked_n}\t"
            f"{v.pair_sum_tighten_per_fill}\t{v.pair_sum_min_floor}\t"
            f"{v.pending_hedge_bypass_imbalance_shares or 0}\t"
            f"{v.discipline_relax_after_forced_sec or 0}\t{v.hedge_force_sec}\t{v.pair_sum_max}\t"
            f"{v.target_min_roi}\t{v.first_leg_max}\t{v.pair_size_pick}\t{v.target_roi_per_fill}"
        )

    best = results[0]
    ref_bp10 = next((r for r in results if r.v.name == "ref_t004_bp10_f090"), None)
    ref_r60 = next((r for r in results if r.v.name == "ref_t004_relax60"), None)
    print()
    print("=== vs round-1 anchors ===")
    if ref_bp10:
        print(
            f"ref_t004_bp10_f090: WR={ref_bp10.win_rate:.4f} sum_pnl={ref_bp10.sum_pnl:.2f} "
            f"port_roi={ref_bp10.port_roi:.6f} mean|imb|={ref_bp10.mean_imb:.2f}"
        )
    if ref_r60:
        print(
            f"ref_t004_relax60:   WR={ref_r60.win_rate:.4f} sum_pnl={ref_r60.sum_pnl:.2f} "
            f"port_roi={ref_r60.port_roi:.6f} mean|imb|={ref_r60.mean_imb:.2f}"
        )
    print(
        f"BEST_SORTED:        WR={best.win_rate:.4f} sum_pnl={best.sum_pnl:.2f} "
        f"port_roi={best.port_roi:.6f} mean|imb|={best.mean_imb:.2f} | {best.v.name}"
    )
    # Best by sum_pnl with WR within 0.5% of max WR
    max_wr = results[0].win_rate
    tier = [r for r in results if r.win_rate >= max_wr - 0.005]
    best_pnl_in_tier = max(tier, key=lambda r: r.sum_pnl)
    print(
        f"BEST_PNL_IN_TOP_WR: WR={best_pnl_in_tier.win_rate:.4f} sum_pnl={best_pnl_in_tier.sum_pnl:.2f} "
        f"| {best_pnl_in_tier.v.name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
