#!/usr/bin/env python3
"""Emit per-buy and per-30s PALADIN v3 vs target wallet for one window (paths below)."""

from __future__ import annotations

import csv
from pathlib import Path

from paladin_engine import PaladinParams, apply_buy_fill, roi_if_down, roi_if_up

from simulate_paladin_window import (
    DEFAULT_PROFIT_LOCK_CONFIG,
    forward_fill_prices,
    load_prices_by_elapsed,
    load_profit_lock_config,
    replay_inventory_from_trades,
    run_window,
    iter_bucket_trace_rows,
    each_trade_post_state,
)

REPO = Path(__file__).resolve().parents[1]
PRICES = REPO / "exports/window_price_snapshots_public/20260406_051501_btc-updown-15m-1775441700_prices.csv"
WALLET_RAW = REPO / "exports/target_wallet_e1_dataset/analysis_10windows_btc/02_btc-updown-15m-1775441700/btc-updown-15m-1775441700_wallet_raw.csv"
WALLET_30S = REPO / "exports/target_wallet_e1_dataset/analysis_10windows_btc/02_btc-updown-15m-1775441700/btc-updown-15m-1775441700_30s_state.csv"
OUT = REPO / "exports/paladin_v3_vs_wallet_1775441700_per_buy_and_30s.txt"


def main() -> int:
    pl = load_profit_lock_config(DEFAULT_PROFIT_LOCK_CONFIG)
    params = PaladinParams(
        profit_lock_min_shares_per_side=float(pl["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl["profit_lock_usdc_each_scenario"]),
    )
    raw = load_prices_by_elapsed(PRICES)
    series = forward_fill_prices(raw)
    st = run_window(
        series,
        budget_usdc=80.0,
        params=params,
        pair_sum_max=1.0,
        single_leg_max_px=0.55,
        pair_only=True,
        stagger_pair_entry=True,
        stagger_hedge_force_after_seconds=45.0,
        target_min_roi=0.05,
        cooldown_seconds=0.0,
        dynamic_clip_cap=12.0,
        pair_size_pick="max_feasible",
        max_shares_per_side=40.0,
        pair_sum_tighten_per_fill=0.004,
        pair_sum_min_floor=0.90,
        second_leg_book_improve_eps=0.013,
        pending_hedge_bypass_imbalance_shares=10.0,
        discipline_relax_after_forced_sec=60.0,
        max_blended_pair_avg_sum=1.03,
        min_elapsed_for_flat_open=24,
    )
    sim_states = replay_inventory_from_trades(st.trades)
    pal_buckets = iter_bucket_trace_rows(series, sim_states)

    lines: list[str] = []
    lines.append("btc-updown-15m-1775441700 | PALADIN v3 ($80 cap) vs target wallet (chain replay)")
    lines.append("=" * 100)
    lines.append("")
    lines.append("### A) PALADIN v3 — every simulated BUY")
    lines.append(
        "idx\telapsed\tside\tshares\tprice\tnotional\treason\tpost_sz_up\tpost_sz_dn\tavg_up\tavg_dn\troi_up\troi_dn"
    )
    for i, row in enumerate(each_trade_post_state(st.trades), 1):
        tr, su, sd, au, ad, ru, rd = row
        lines.append(
            f"{i}\t{tr.elapsed_sec}\t{tr.side}\t{tr.shares:.4f}\t{tr.price:.4f}\t{tr.notional:.2f}\t{tr.reason}\t"
            f"{su:.2f}\t{sd:.2f}\t{au:.4f}\t{ad:.4f}\t{ru:.4f}\t{rd:.4f}"
        )
    lines.append("")
    lines.append(f"PALADIN totals: trades={len(st.trades)} spent=${st.spent_usdc:.2f}")
    lines.append("")
    lines.append("### B) Target wallet — every on-chain BUY (raw export)")
    lines.append(
        "idx\telapsed\tside\tshares\tprice\tnotional\tpost_cum_up\tpost_cum_dn\tavg_up*\tavg_dn*\troi_up\troi_dn"
    )
    wbuys: list[dict[str, str]] = []
    with WALLET_RAW.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("trade_side") != "BUY":
                continue
            wbuys.append(row)
    # recompute running avgs for ROI columns (cum fields are in CSV but we verify from size/price)
    su = au = sd = ad = 0.0
    for i, row in enumerate(wbuys, 1):
        side = row["side"].lower()
        sh = float(row["size"])
        px = float(row["price"])
        nt = float(row["notional"]) if row.get("notional") else sh * px
        su, au, sd, ad = apply_buy_fill(su, au, sd, ad, side=side, add_shares=sh, fill_price=px)
        ru, rd = roi_if_up(su, au, sd, ad), roi_if_down(su, au, sd, ad)
        lines.append(
            f"{i}\t{row['elapsed_sec']}\t{side}\t{sh:.4f}\t{px:.4f}\t{nt:.2f}\t{su:.2f}\t{sd:.2f}\t{au:.4f}\t{ad:.4f}\t{ru:.4f}\t{rd:.4f}"
        )
    tot_w = sum(float(r["notional"] or float(r["size"]) * float(r["price"])) for r in wbuys)
    lines.append("")
    lines.append(f"Wallet totals: buys={len(wbuys)} spent~=${tot_w:.2f} (sum notionals)")
    lines.append("")
    lines.append("### C) PALADIN v3 — end of each 30s bucket (mids + inventory + ROI/PnL branches)")
    lines.append(
        "bucket\tpm_up\tpm_dn\tsz_up\tsz_dn\tavg_up\tavg_dn\troi_up\troi_dn\tpnl_up$\tpnl_dn$"
    )
    for br in pal_buckets:
        lines.append(
            f"{br['bucket']}\t{br['pm_up']:.4f}\t{br['pm_down']:.4f}\t{br['size_up']:.4f}\t{br['size_down']:.4f}\t"
            f"{br['avg_up']:.4f}\t{br['avg_down']:.4f}\t{br['roi_if_up']:.4f}\t{br['roi_if_down']:.4f}\t"
            f"{br['pnl_if_up']:.2f}\t{br['pnl_if_down']:.2f}"
        )
    lines.append("")
    lines.append("### D) Target wallet — 30s_state export (cumulative book at bucket end)")
    lines.append(
        "bucket\tpm_up\tpm_dn\tcum_up\tcum_dn\tavg_up\tavg_dn\tspent_cum$\tpnl_if_up$\tpnl_if_dn$\troi_up\troi_dn"
    )
    with WALLET_30S.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cu = float(row["cum_up_size"])
            cd = float(row["cum_down_size"])
            au = float(row["avg_up_buy_price"])
            ad = float(row["avg_down_buy_price"])
            sp = float(row["cum_total_notional"])
            pu = float(row["pnl_if_up"])
            pd = float(row["pnl_if_down"])
            ru = pu / sp if sp > 1e-9 else 0.0
            rd = pd / sp if sp > 1e-9 else 0.0
            lines.append(
                f"{row['bucket_label']}\t{float(row['pm_up_price']):.4f}\t{float(row['pm_down_price']):.4f}\t"
                f"{cu:.2f}\t{cd:.2f}\t{au:.4f}\t{ad:.4f}\t{sp:.2f}\t{pu:.2f}\t{pd:.2f}\t{ru:.4f}\t{rd:.4f}"
            )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
