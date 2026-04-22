#!/usr/bin/env python3
"""Search PALADIN v5 extended params for positive total PnL on 100 recent windows."""

from __future__ import annotations

import itertools
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
N = 100
MIN_MAX_ELAPSED = 800
WIN_EPS = 1e-6


def eval_100(series_list: list, p: PaladinV5Params) -> tuple[float, float, int]:
    pnls: list[float] = []
    for series in series_list:
        st = run_window_v5(series, params=p)
        w, _, _ = resolve_winner_from_last_prices(series)
        pnls.append(float(settled_pnl_usdc(st.snapshot_metrics(), w)))
    tot = sum(pnls)
    wr = sum(1 for x in pnls if x > WIN_EPS)
    return tot, tot / len(pnls), wr


def main() -> int:
    wins = discover_windows_recent(prices_dir=PRICES_DIR, count=N, min_max_elapsed=MIN_MAX_ELAPSED)
    series_all = []
    for w in wins:
        raw = load_prices_by_elapsed(w.prices_csv)
        series_all.append(forward_fill_prices(raw))
    assert len(series_all) == N

    base_kw = dict(
        budget_usdc=80.0,
        clip_shares=5.0,
        max_shares_per_side=10.0,
        first_leg_min_winner_px=0.5,
        pair_sum_max_on_forced_hedge=1.15,
        winner_drop_eps=0.05,
        winner_drop_window_seconds=8,
        improvement_buy_enabled=False,
        min_notional=1.0,
    )

    grid = itertools.product(
        [1, 2],  # max_pair_cycles
        [0.56, 0.58, 0.60],  # first_leg_max_winner_px
        [0.99, 1.0],  # flat_entry_max_pair_sum
        [0.97, 0.985],  # additional_pair_max_pair_sum
        [0.04, 0.05],  # second_leg_margin
        [1.01, 1.03],  # forced_hedge_max_book_sum
        [55.0, 75.0, 100.0],  # hedge_force_after_seconds
    )

    best = (-1e18, None, None)
    positive: list[tuple[float, float, int, dict]] = []

    for (
        max_cyc,
        fmax,
        flat_mx,
        add_mx,
        margin,
        fsum,
        hf,
    ) in grid:
        p = PaladinV5Params(
            **base_kw,
            max_pair_cycles=int(max_cyc),
            first_leg_max_winner_px=float(fmax),
            flat_entry_max_pair_sum=float(flat_mx),
            additional_pair_max_pair_sum=float(add_mx),
            second_leg_margin=float(margin),
            forced_hedge_max_book_sum=float(fsum),
            hedge_force_after_seconds=float(hf),
        )
        tot, mean, wr = eval_100(series_all, p)
        cfg = {
            "max_pair_cycles": max_cyc,
            "first_leg_max_winner_px": fmax,
            "flat_entry_max_pair_sum": flat_mx,
            "additional_pair_max_pair_sum": add_mx,
            "second_leg_margin": margin,
            "forced_hedge_max_book_sum": fsum,
            "hedge_force_after_seconds": hf,
            "improvement_buy_enabled": False,
            "winner_drop_window_seconds": 8,
        }
        if tot > best[0]:
            best = (tot, cfg, (mean, wr))
        if tot > 0:
            positive.append((tot, mean, wr, cfg))

    positive.sort(key=lambda x: -x[0])
    print(f"PALADIN v5+ profitability search | N={N} windows | improvement=OFF")
    print(f"best_total_pnl_usd={best[0]:.2f}  mean={best[2][0]:.4f}  wins={best[2][1]}/{N}  WR={100*best[2][1]/N:.1f}%")
    print("best_config:", best[1])
    print()
    print(f"configs with total_pnl>0: {len(positive)}")
    for row in positive[:12]:
        tot, mean, wr, cfg = row
        print(f"  total={tot:.2f}  mean={mean:.4f}  WR={100*wr/N:.1f}% ({wr}/{N})  {cfg}")

    # Preset candidate for live / sim label V6_PROFIT_100
    if best[1] is not None:
        print()
        print("=== Recommended PaladinV5Params() kwargs (copy into harness) ===")
        for k, v in {**base_kw, **best[1]}.items():
            print(f"    {k}={v!r},")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
