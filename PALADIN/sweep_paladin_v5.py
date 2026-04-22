#!/usr/bin/env python3
"""PALADIN v5 batch: per pool total settled PnL ($) and win rate (% windows with PnL > 0)."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from calibrate_ladder_wallet_windows import discover_windows_recent
from paladin_v5 import PaladinV5Params, run_window_v5
from simulate_paladin_window import (
    forward_fill_prices,
    load_prices_by_elapsed,
    resolve_winner_from_last_prices,
    settled_pnl_usdc,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PRICES_DIR = REPO_ROOT / "exports" / "window_price_snapshots_public"
MIN_MAX_ELAPSED = 800
POOLS = (10, 20, 50, 100, 200)
WIN_EPS = 1e-6

ORIGINAL = PaladinV5Params(
    budget_usdc=80.0,
    clip_shares=5.0,
    max_shares_per_side=10.0,
    first_leg_min_winner_px=0.5,
    second_leg_margin=0.03,
    hedge_force_after_seconds=90.0,
    pair_sum_max_on_forced_hedge=1.15,
    winner_drop_eps=0.05,
    winner_drop_window_seconds=7,
    improvement_buy_enabled=True,
)

VARIANTS: list[tuple[str, dict[str, float | int | bool]]] = [
    ("V1_dropW5", {"winner_drop_window_seconds": 5}),
    ("V2_dropW10", {"winner_drop_window_seconds": 10}),
    ("V3_dropW15", {"winner_drop_window_seconds": 15}),
    ("V4_dropEps04", {"winner_drop_eps": 0.04}),
    ("V5_dropEps06", {"winner_drop_eps": 0.06}),
    ("V6_force60_sum12", {"hedge_force_after_seconds": 60.0, "pair_sum_max_on_forced_hedge": 1.2}),
    ("V7_force120_sum13", {"hedge_force_after_seconds": 120.0, "pair_sum_max_on_forced_hedge": 1.3}),
    ("V8_margin02_sum115", {"second_leg_margin": 0.02, "pair_sum_max_on_forced_hedge": 1.15}),
    ("V9_margin04_sum125", {"second_leg_margin": 0.04, "pair_sum_max_on_forced_hedge": 1.25}),
    ("V10_sum105_fast", {"hedge_force_after_seconds": 45.0, "pair_sum_max_on_forced_hedge": 1.05}),
    ("V11_sum130_slow", {"hedge_force_after_seconds": 150.0, "pair_sum_max_on_forced_hedge": 1.3}),
    ("V12_no_improve", {"improvement_buy_enabled": False}),
]


@dataclass(slots=True)
class PoolStat:
    n: int
    total_pnl_usdc: float
    mean_pnl_usdc: float
    wins: int
    losses: int
    breakeven: int

    @property
    def win_rate_pct(self) -> float:
        return 100.0 * self.wins / self.n if self.n else 0.0


@dataclass(slots=True)
class Row:
    label: str
    by_n: dict[int, PoolStat]


def _merge_params(overrides: dict) -> PaladinV5Params:
    base = deepcopy(ORIGINAL)
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def eval_pool(series_list: list, p: PaladinV5Params) -> PoolStat:
    pnls: list[float] = []
    for series in series_list:
        st = run_window_v5(series, params=p)
        w, _, _ = resolve_winner_from_last_prices(series)
        pnls.append(float(settled_pnl_usdc(st.snapshot_metrics(), w)))
    n = len(pnls)
    tot = sum(pnls)
    wins = sum(1 for x in pnls if x > WIN_EPS)
    losses = sum(1 for x in pnls if x < -WIN_EPS)
    flat = n - wins - losses
    return PoolStat(
        n=n,
        total_pnl_usdc=tot,
        mean_pnl_usdc=tot / n if n else 0.0,
        wins=wins,
        losses=losses,
        breakeven=flat,
    )


def main() -> int:
    max_n = max(POOLS)
    wins = discover_windows_recent(
        prices_dir=PRICES_DIR, count=max_n, min_max_elapsed=MIN_MAX_ELAPSED
    )
    if len(wins) < max_n:
        print(f"WARN: only {len(wins)} windows (wanted {max_n})")
    series_all: list = []
    for w in wins:
        raw = load_prices_by_elapsed(w.prices_csv)
        series_all.append(forward_fill_prices(raw))

    labels = [("ORIGINAL", ORIGINAL)] + [(name, _merge_params(ov)) for name, ov in VARIANTS]
    rows: list[Row] = []
    for label, params in labels:
        by_n: dict[int, PoolStat] = {}
        for n in POOLS:
            by_n[n] = eval_pool(series_all[:n], params)
        rows.append(Row(label=label, by_n=by_n))

    ref_n = max(POOLS)
    print("PALADIN v5 | settled PnL proxy (last mid winner) | win = PnL > 0")
    print(f"pools (windows each): {POOLS} | min_max_elapsed>={MIN_MAX_ELAPSED} | pool size = {ref_n} newest")
    print()

    for n in POOLS:
        print(f"--- pool N={n} ({n} windows) ---")
        print("label\ttotal_pnl_usd\tmean_pnl_usd\tWR_pct\twins\tlosses\tflat")
        for r in rows:
            s = r.by_n[n]
            print(
                f"{r.label}\t{s.total_pnl_usdc:.2f}\t{s.mean_pnl_usdc:.4f}\t"
                f"{s.win_rate_pct:.1f}\t{s.wins}\t{s.losses}\t{s.breakeven}"
            )
        print()

    # Rank variants vs ORIGINAL on largest pool: total PnL then WR
    orig = rows[0]
    o_ref = orig.by_n[ref_n]

    def sort_key(r: Row) -> tuple[float, float]:
        s = r.by_n[ref_n]
        return (s.total_pnl_usdc, s.win_rate_pct)

    variants_sorted = sorted(rows[1:], key=sort_key, reverse=True)
    print(f"=== Top 5 variants vs ORIGINAL (N={ref_n}: sort by total_pnl_usd, then WR_pct) ===")
    print("label\ttotal_pnl_usd\tWR_pct\twins/loss/flat\tmean_pnl_usd")
    print(
        f"{orig.label}\t{o_ref.total_pnl_usdc:.2f}\t{o_ref.win_rate_pct:.1f}\t"
        f"{o_ref.wins}/{o_ref.losses}/{o_ref.breakeven}\t{o_ref.mean_pnl_usdc:.4f}"
    )
    for r in variants_sorted[:5]:
        s = r.by_n[ref_n]
        print(
            f"{r.label}\t{s.total_pnl_usdc:.2f}\t{s.win_rate_pct:.1f}\t"
            f"{s.wins}/{s.losses}/{s.breakeven}\t{s.mean_pnl_usdc:.4f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
