#!/usr/bin/env python3
"""
Batch PALADIN vs target wallet on the 35 BTC windows (public overlap), then compare
baseline live-style PALADIN to five research variants (WR + portfolio ROI).

Run from repo:  python PALADIN/batch_compare_wallet35.py
(or cd PALADIN && python batch_compare_wallet35.py)
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paladin_engine import PaladinParams, apply_buy_fill, pnl_if_down_usdc, pnl_if_up_usdc

from simulate_paladin_window import (
    DEFAULT_PROFIT_LOCK_CONFIG,
    forward_fill_prices,
    load_prices_by_elapsed,
    load_profit_lock_config,
    resolve_winner_from_last_prices,
    run_window,
    settled_pnl_usdc,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
OVERLAP_CSV = REPO_ROOT / "exports" / "target_wallet_e1_dataset" / "overlap_windows.csv"


@dataclass(slots=True)
class StrategySpec:
    key: str
    label: str
    min_elapsed_for_flat_open: int | None = None
    max_elapsed_to_start_flat: int | None = None
    dynamic_clip_max: float = 10.0
    pair_sum_tighten_per_fill: float = 0.004
    pair_sum_min_floor: float = 0.90
    pending_hedge_bypass_imbalance_shares: float = 10.0
    discipline_relax_after_forced_sec: float = 60.0
    second_leg_book_improve_eps: float = 0.0
    max_blended_pair_avg_sum: float | None = None


def _btc_windows_35() -> list[dict[str, str]]:
    rows = list(csv.DictReader(OVERLAP_CSV.open(newline="", encoding="utf-8")))
    btc = [
        r
        for r in rows
        if r.get("asset_symbol") == "BTC" and str(r.get("has_public_snapshot")).lower() == "true"
    ]
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for r in sorted(btc, key=lambda x: int(x["slug"].rsplit("-", 1)[-1])):
        s = r["slug"]
        if s in seen:
            continue
        seen.add(s)
        out.append(r)
        if len(out) >= 35:
            break
    return out


def _resolve(p: str) -> Path:
    path = Path(p.strip())
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def wallet_spent_and_settled_pnl(wallet_csv: Path, series: list[tuple[float, float]]) -> tuple[float, float]:
    su = au = sd = ad = 0.0
    spent = 0.0
    with wallet_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if str(row.get("trade_side") or "").upper() != "BUY":
                continue
            side_raw = str(row.get("side") or "").upper()
            if side_raw not in {"UP", "DOWN"}:
                continue
            side = "up" if side_raw == "UP" else "down"
            sh = float(row["size"])
            px = float(row["price"])
            notion = float(row["notional"]) if row.get("notional") not in (None, "") else sh * px
            spent += notion
            su, au, sd, ad = apply_buy_fill(su, au, sd, ad, side=side, add_shares=sh, fill_price=px)
    win, _, _ = resolve_winner_from_last_prices(series)
    pu = pnl_if_up_usdc(su, au, sd, ad)
    pd = pnl_if_down_usdc(su, au, sd, ad)
    pnl = settled_pnl_usdc(
        {"pnl_if_up_usdc": pu, "pnl_if_down_usdc": pd},
        win,  # type: ignore[arg-type]
    )
    return spent, float(pnl)


def _aggregate_window_results(
    rows: list[tuple[float, float]],
) -> dict[str, float]:
    total_spent = sum(s for s, _ in rows)
    total_pnl = sum(p for _, p in rows)
    n_win = n_loss = n_flat = 0
    for _, p in rows:
        if p > 1e-6:
            n_win += 1
        elif p < -1e-6:
            n_loss += 1
        else:
            n_flat += 1
    wl = n_win + n_loss
    wr = (n_win / wl) if wl else float("nan")
    roi = (total_pnl / total_spent) if total_spent > 1e-9 else float("nan")
    return {
        "n": float(len(rows)),
        "sum_spent": total_spent,
        "sum_pnl": total_pnl,
        "n_win": float(n_win),
        "n_loss": float(n_loss),
        "n_flat": float(n_flat),
        "win_rate_wl": wr,
        "portfolio_roi": roi,
    }


def run_paladin_on_windows(spec: StrategySpec, windows: list[dict[str, str]], pl_params: PaladinParams) -> dict[str, float]:
    per: list[tuple[float, float]] = []
    for w in windows:
        prices_path = _resolve(w["public_snapshot_csv"])
        raw = load_prices_by_elapsed(prices_path)
        series = forward_fill_prices(raw)
        mb = spec.max_blended_pair_avg_sum
        st = run_window(
            series,
            budget_usdc=80.0,
            params=pl_params,
            pair_sum_max=1.0,
            single_leg_max_px=0.55,
            pair_only=True,
            stagger_pair_entry=True,
            stagger_hedge_force_after_seconds=45.0,
            target_min_roi=0.05,
            cooldown_seconds=0.0,
            dynamic_clip_cap=spec.dynamic_clip_max,
            pair_size_pick="max_feasible",
            max_shares_per_side=40.0,
            pair_sum_tighten_per_fill=spec.pair_sum_tighten_per_fill,
            pair_sum_min_floor=spec.pair_sum_min_floor,
            second_leg_book_improve_eps=spec.second_leg_book_improve_eps,
            pending_hedge_bypass_imbalance_shares=spec.pending_hedge_bypass_imbalance_shares,
            discipline_relax_after_forced_sec=spec.discipline_relax_after_forced_sec,
            max_blended_pair_avg_sum=mb,
            max_elapsed_to_start_flat=spec.max_elapsed_to_start_flat,
            min_elapsed_for_flat_open=spec.min_elapsed_for_flat_open,
        )
        m = st.snapshot_metrics()
        win, _, _ = resolve_winner_from_last_prices(series)
        pnl = settled_pnl_usdc(m, win)
        per.append((st.spent_usdc, pnl))
    return _aggregate_window_results(per)


def run_wallet_on_windows(windows: list[dict[str, str]]) -> dict[str, float]:
    per: list[tuple[float, float]] = []
    for w in windows:
        wallet_csv = _resolve(w["wallet_per_window_csv"])
        prices_path = _resolve(w["public_snapshot_csv"])
        raw = load_prices_by_elapsed(prices_path)
        series = forward_fill_prices(raw)
        spent, pnl = wallet_spent_and_settled_pnl(wallet_csv, series)
        per.append((spent, pnl))
    return _aggregate_window_results(per)


def main() -> int:
    windows = _btc_windows_35()
    if len(windows) != 35:
        raise SystemExit(f"expected 35 BTC public windows, got {len(windows)}")

    pl_cfg = load_profit_lock_config(DEFAULT_PROFIT_LOCK_CONFIG)
    pl_params = PaladinParams(
        profit_lock_min_shares_per_side=float(pl_cfg["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl_cfg["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl_cfg["profit_lock_usdc_each_scenario"]),
    )

    specs: list[StrategySpec] = [
        StrategySpec(
            "baseline",
            "1) Live-style PALADIN + 24s entry delay (config BOT_STRATEGY_ENTRY_DELAY)",
            min_elapsed_for_flat_open=24,
        ),
        StrategySpec(
            "opt1_no_entry_delay",
            "2) No entry delay (0s vs live 24s; earlier first leg)",
            min_elapsed_for_flat_open=0,
        ),
        StrategySpec(
            "opt2_tighter_imb",
            "3) Tighter pair + earlier imb bypass (tighten 0.0045, bypass 8)",
            min_elapsed_for_flat_open=24,
            pair_sum_tighten_per_fill=0.0045,
            pending_hedge_bypass_imbalance_shares=8.0,
        ),
        StrategySpec(
            "opt3_small_clip",
            "4) Smaller dynamic clip cap (6 vs 10)",
            min_elapsed_for_flat_open=24,
            dynamic_clip_max=6.0,
        ),
        StrategySpec(
            "opt4_looser_disc",
            "5) Looser discipline grid (tighten 0.0035, bypass 12, relax 45s)",
            min_elapsed_for_flat_open=24,
            pair_sum_tighten_per_fill=0.0035,
            pending_hedge_bypass_imbalance_shares=12.0,
            discipline_relax_after_forced_sec=45.0,
        ),
        StrategySpec(
            "opt5_second_leg",
            "6) Stricter 2nd leg (book_eps=0.015, blended cap 1.03)",
            min_elapsed_for_flat_open=24,
            second_leg_book_improve_eps=0.015,
            max_blended_pair_avg_sum=1.03,
        ),
    ]

    print("Windows:", len(windows), "| slug range:", windows[0]["slug"], "…", windows[-1]["slug"])
    print()

    results: list[dict[str, Any]] = []

    w_stats = run_wallet_on_windows(windows)
    results.append({"key": "target_wallet", "label": "0) Target wallet (actual BUY replay)", **w_stats})

    for spec in specs:
        s = run_paladin_on_windows(spec, windows, pl_params)
        results.append({"key": spec.key, "label": spec.label, **s})

    hdr = (
        f"{'strategy':<22}\t{'WR%':>7}\t{'port_ROI%':>10}\t{'sum_PnL':>9}\t{'sum_spent':>10}\t"
        f"W/L/flat"
    )
    print(hdr)
    print("-" * len(hdr.expandtabs(4)))
    for r in results:
        wr_pct = 100.0 * r["win_rate_wl"] if r["win_rate_wl"] == r["win_rate_wl"] else float("nan")
        roi_pct = 100.0 * r["portfolio_roi"] if r["portfolio_roi"] == r["portfolio_roi"] else float("nan")
        wl = f"{int(r['n_win'])}/{int(r['n_loss'])}/{int(r['n_flat'])}"
        short = r["key"][:22]
        print(
            f"{short:<22}\t{wr_pct:7.2f}\t{roi_pct:10.3f}\t{r['sum_pnl']:9.2f}\t{r['sum_spent']:10.2f}\t{wl}"
        )

    print()
    print("Notes:")
    print("- WR = wins / (wins + losses); flat PnL windows excluded from WR denominator.")
    print("- portfolio_ROI = sum(settled PnL) / sum(spend) across the 35 windows.")
    print("- Settlement uses last-tick up vs down mid in the public snapshot (same as PALADIN batch).")
    print("- Target wallet spends can exceed $80/window; PALADIN sim is capped at $80 budget/window.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
