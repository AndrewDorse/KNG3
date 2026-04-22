#!/usr/bin/env python3
"""
Grid of PALADIN variants aligned with wallet principles:
  - 5-share clips, 10/side max (via dynamic_clip_cap + max_shares_per_side)
  - Winning-side first stagger; second leg pair-sum / hedge discipline
  - No alternate-up/down ladder unless variant enables it

Ranks on mean settle PnL across N=100,200,400 recent windows (min-max-elapsed gate).
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from calibrate_ladder_wallet_windows import discover_windows_recent
from paladin_engine import PaladinParams
from simulate_paladin_window import (
    forward_fill_prices,
    load_prices_by_elapsed,
    resolve_winner_from_last_prices,
    run_window,
    settled_pnl_usdc,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PRICES_DIR = REPO_ROOT / "exports" / "window_price_snapshots_public"

PL = PaladinParams(
    profit_lock_min_shares_per_side=100.0,
    roi_lock_min_each=0.99,
    profit_lock_usdc_each_scenario=float("inf"),
)

BASE: dict = {
    "budget_usdc": 80.0,
    "params": PL,
    "pair_sum_max": 0.97,
    "pair_sum_max_on_forced_hedge": 1.0,
    "single_leg_max_px": 0.55,
    "pair_only": True,
    "stagger_pair_entry": True,
    "target_min_roi": 0.0,
    "cooldown_seconds": 0.0,
    "dynamic_clip_cap": 12.0,
    "pair_size_pick": "max_feasible",
    "max_shares_per_side": 10.0,
    "pair_sum_tighten_per_fill": 0.0,
    "pair_sum_min_floor": 0.90,
    "second_leg_book_improve_eps": 0.013,
    "max_blended_pair_avg_sum": 0.97,
    "pending_hedge_bypass_imbalance_shares": 10.0,
    "discipline_relax_after_forced_sec": 60.0,
    "min_elapsed_for_flat_open": 0,
    "stagger_winning_side_first_when_position": True,
    "stagger_alternate_first_leg_when_balanced": False,
    "stagger_symmetric_fallback_when_balanced": False,
    "stagger_symmetric_fallback_roi_discount": 0.03,
    "stagger_symmetric_fallback_skip_first_leg_blend_cap": False,
    "entry_trailing_min_low_seconds": None,
    "entry_trailing_low_slippage": 0.02,
    "second_leg_must_improve_leg_avg": False,
    "stagger_hedge_force_after_seconds": 90.0,
    "min_elapsed_between_pair_starts": 100.0,
}

# (label, overrides) — tweak hedge pacing, book discipline, dips, blended cap, fallback
VARIANTS: list[tuple[str, dict]] = [
    ("A_hf45_gap100_eps13", {"stagger_hedge_force_after_seconds": 45.0, "min_elapsed_between_pair_starts": 100.0}),
    ("B_hf60_gap100_eps13", {"stagger_hedge_force_after_seconds": 60.0, "min_elapsed_between_pair_starts": 100.0}),
    ("C_hf75_gap100_eps13", {"stagger_hedge_force_after_seconds": 75.0, "min_elapsed_between_pair_starts": 100.0}),
    ("D_hf90_gap100_eps13", {"stagger_hedge_force_after_seconds": 90.0, "min_elapsed_between_pair_starts": 100.0}),
    ("E_hf90_gap80_eps13", {"stagger_hedge_force_after_seconds": 90.0, "min_elapsed_between_pair_starts": 80.0}),
    ("F_hf90_gap120_eps13", {"stagger_hedge_force_after_seconds": 90.0, "min_elapsed_between_pair_starts": 120.0}),
    ("G_hf90_gap100_eps0", {"stagger_hedge_force_after_seconds": 90.0, "second_leg_book_improve_eps": 0.0}),
    ("H_hf90_gap100_eps18", {"stagger_hedge_force_after_seconds": 90.0, "second_leg_book_improve_eps": 0.018}),
    ("I_hf90_gap100_blend098", {"stagger_hedge_force_after_seconds": 90.0, "max_blended_pair_avg_sum": 0.98}),
    ("J_hf90_gap100_force099", {"stagger_hedge_force_after_seconds": 90.0, "pair_sum_max_on_forced_hedge": 0.99}),
    ("K_hf90_gap100_relax90", {"stagger_hedge_force_after_seconds": 90.0, "discipline_relax_after_forced_sec": 90.0}),
    ("L_hf90_gap100_imb15", {"stagger_hedge_force_after_seconds": 90.0, "pending_hedge_bypass_imbalance_shares": 15.0}),
    ("M_hf90_gap100_imb5", {"stagger_hedge_force_after_seconds": 90.0, "pending_hedge_bypass_imbalance_shares": 5.0}),
    ("N_hf90_gap100_tr30", {"stagger_hedge_force_after_seconds": 90.0, "entry_trailing_min_low_seconds": 30}),
    ("O_hf90_gap100_tr45", {"stagger_hedge_force_after_seconds": 90.0, "entry_trailing_min_low_seconds": 45}),
    ("P_hf90_gap100_tr60_sl025", {"stagger_hedge_force_after_seconds": 90.0, "entry_trailing_min_low_seconds": 60, "entry_trailing_low_slippage": 0.025}),
    ("Q_hf120_gap100", {"stagger_hedge_force_after_seconds": 120.0, "min_elapsed_between_pair_starts": 100.0}),
    ("R_blend098_hf90", {"max_blended_pair_avg_sum": 0.98, "stagger_hedge_force_after_seconds": 90.0}),
    ("S_roi1_hf90", {"target_min_roi": 0.01, "stagger_hedge_force_after_seconds": 90.0}),
    ("T_symfb_on", {"stagger_symmetric_fallback_when_balanced": True}),
    ("U_hf55_gap100", {"stagger_hedge_force_after_seconds": 55.0, "min_elapsed_between_pair_starts": 100.0}),
    ("V_hf90_skip1stblend", {"stagger_symmetric_fallback_when_balanced": True, "stagger_symmetric_fallback_skip_first_leg_blend_cap": True}),
    ("W_hf90_gap140", {"stagger_hedge_force_after_seconds": 90.0, "min_elapsed_between_pair_starts": 140.0}),
    ("X_hf90_tight002", {"pair_sum_tighten_per_fill": 0.002}),
    ("Y_hf45_tr45", {"stagger_hedge_force_after_seconds": 45.0, "entry_trailing_min_low_seconds": 45}),
    ("Z_hf90_pair097_force098", {"pair_sum_max_on_forced_hedge": 0.98}),
    # Post-fill blended cap / hedge timing (pair_sum_max stays 0.97 unless noted)
    ("AD_sum97_blend100_hf45", {"max_blended_pair_avg_sum": 1.0, "stagger_hedge_force_after_seconds": 45.0}),
    ("AE_sum97_blend100_hf90_eps0", {"max_blended_pair_avg_sum": 1.0, "second_leg_book_improve_eps": 0.0}),
    ("AF_hf30_gap100", {"stagger_hedge_force_after_seconds": 30.0}),
    ("AG_hf40_gap100", {"stagger_hedge_force_after_seconds": 40.0}),
    ("AH_gap60_hf90", {"min_elapsed_between_pair_starts": 60.0}),
    ("AI_gap180_hf90", {"min_elapsed_between_pair_starts": 180.0}),
    ("AJ_sym_on_hf45", {"stagger_symmetric_fallback_when_balanced": True, "stagger_hedge_force_after_seconds": 45.0}),
    ("AK_sym_on_hf90", {"stagger_symmetric_fallback_when_balanced": True}),
    ("AM_tight001_hf90", {"pair_sum_tighten_per_fill": 0.001}),
    ("AN_hf75_blend099", {"stagger_hedge_force_after_seconds": 75.0, "max_blended_pair_avg_sum": 0.99}),
    ("AP_im20_hf90", {"pending_hedge_bypass_imbalance_shares": 20.0}),
    ("AQ_rel120_hf90", {"discipline_relax_after_forced_sec": 120.0}),
    ("AR_hf50_tr40", {"stagger_hedge_force_after_seconds": 50.0, "entry_trailing_min_low_seconds": 40}),
    ("AS_hf90_eps010", {"second_leg_book_improve_eps": 0.010}),
]

COUNTS = (100, 200, 400)
MIN_MAX_ELAPSED = 800


@dataclass(slots=True)
class VariantResult:
    label: str
    mean100: float
    mean200: float
    mean400: float
    total100: float
    total200: float
    total400: float


def eval_variant_series(series_slice: list, kw: dict) -> tuple[float, float]:
    pnls: list[float] = []
    for series in series_slice:
        st = run_window(series, **kw)
        win_side, _, _ = resolve_winner_from_last_prices(series)
        pnls.append(float(settled_pnl_usdc(st.snapshot_metrics(), win_side)))
    n = len(pnls)
    return sum(pnls) / n, sum(pnls)


def main() -> int:
    pool400 = discover_windows_recent(
        prices_dir=PRICES_DIR, count=COUNTS[-1], min_max_elapsed=MIN_MAX_ELAPSED
    )
    if len(pool400) < COUNTS[-1]:
        print(f"WARN: only {len(pool400)} windows available for count={COUNTS[-1]}")
    series_all: list = []
    for w in pool400:
        raw = load_prices_by_elapsed(w.prices_csv)
        series_all.append(forward_fill_prices(raw))

    results: list[VariantResult] = []
    for label, overrides in VARIANTS:
        kw = deepcopy(BASE)
        pl = kw.pop("params")
        kw.update(overrides)
        kw["params"] = pl
        m100, t100 = eval_variant_series(series_all[:100], kw)
        m200, t200 = eval_variant_series(series_all[:200], kw)
        m400, t400 = eval_variant_series(series_all[:400], kw)
        results.append(
            VariantResult(
                label=label,
                mean100=m100,
                mean200=m200,
                mean400=m400,
                total100=t100,
                total200=t200,
                total400=t400,
            )
        )

    def score(r: VariantResult) -> tuple[float, float]:
        lo = min(r.mean100, r.mean200, r.mean400)
        avg = (r.mean100 + r.mean200 + r.mean400) / 3.0
        return (lo, avg)

    results.sort(key=lambda r: score(r), reverse=True)

    print("Principles baseline: win-side-first, 5-sh clips, 10/side, pair_sum<=0.97 2nd leg + hedge timer")
    print(f"pools: {COUNTS} windows | min_max_elapsed>={MIN_MAX_ELAPSED}")
    print()
    hdr = (
        "rank\tvariant\tmean100\tmean200\tmean400\tmin3\tavg3\t"
        "tot100\ttot200\ttot400"
    )
    print(hdr)
    for i, r in enumerate(results[:10], start=1):
        lo = min(r.mean100, r.mean200, r.mean400)
        av = (r.mean100 + r.mean200 + r.mean400) / 3.0
        print(
            f"{i}\t{r.label}\t{r.mean100:.4f}\t{r.mean200:.4f}\t{r.mean400:.4f}\t"
            f"{lo:.4f}\t{av:.4f}\t{r.total100:.2f}\t{r.total200:.2f}\t{r.total400:.2f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
