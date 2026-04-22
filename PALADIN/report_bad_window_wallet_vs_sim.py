#!/usr/bin/env python3
"""
First ~-$2.5 wallet window (Apr 22 scope): 12:15–12:30 ET = slug btc-updown-15m-1776874500.

Prints:
  (1) Every Polymarket BUY row for that slug from exports activity CSV (may be truncated if file is last-N only).
  (2) End-of-30s bucket table for wallet inventory (replay buys at on-chain timestamps → window elapsed).
  (3) Every simulated v7 fill + same 30s bucket table for sim (tape from enriched *_prices.csv).

Run from repo root: python PALADIN/report_bad_window_wallet_vs_sim.py
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

PALADIN_DIR = Path(__file__).resolve().parent
REPO = PALADIN_DIR.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(PALADIN_DIR))

import simulate_dual_profit_hedge as dph  # noqa: E402
from paladin_engine import apply_buy_fill  # noqa: E402
from paladin_v7 import V7_SMALL_BUDGET_4ORDERS, load_ticks_with_btc, run_window_v7  # noqa: E402
from simulate_paladin_window import (  # noqa: E402
    forward_fill_prices,
    iter_bucket_trace_rows,
    load_prices_by_elapsed,
    pnl_if_down_usdc,
    pnl_if_up_usdc,
    replay_inventory_from_trades,
    roi_if_down,
    roi_if_up,
)

SLUG = "btc-updown-15m-1776874500"
ACTIVITY = REPO / "exports" / "polymarket_activity_0x94a73570cd0df2da112fb55da7bb914b34efa18d_last150.csv"
PRICES = REPO / "exports" / "ab_apr22_wallet94a73570_sim_work" / "20260422_191500_btc-updown-15m-1776874500_prices.csv"
OUT_TXT = REPO / "exports" / "report_1776874500_wallet_vs_v7_30s.txt"

_LOG_LINES: list[str] = []


def _p(*a: object) -> None:
    s = " ".join(str(x) for x in a)
    _LOG_LINES.append(s)
    print(s, flush=True)


def outcome_to_side(outcome: str) -> str:
    o = (outcome or "").strip().lower()
    if o == "up":
        return "up"
    if o == "down":
        return "down"
    return ""


def load_wallet_buys_for_slug(path: Path, slug: str) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (row.get("slug") or "").strip() != slug:
                continue
            if (row.get("type") or "").strip().upper() != "TRADE":
                continue
            if (row.get("side") or "").strip().upper() != "BUY":
                continue
            rows.append(row)
    rows.sort(key=lambda r: int(float(r.get("timestamp") or 0)))
    return rows


def wallet_replay_states(
    buys: list[dict[str, str]],
    *,
    start_ts: int,
    window_sec: int = 900,
) -> list[tuple[float, float, float, float]]:
    """Per-second (size_up, size_down, avg_up, avg_down) after applying wallet BUYs at event second."""
    by_elapsed: dict[int, list[tuple[str, float, float]]] = defaultdict(list)
    for row in buys:
        ts = int(float(row.get("timestamp") or 0))
        el = ts - start_ts
        if el < 0 or el >= window_sec:
            continue
        side = outcome_to_side(row.get("outcome") or "")
        if side not in ("up", "down"):
            continue
        sh = float(row.get("size") or 0)
        px = float(row.get("price") or 0)
        by_elapsed[el].append((side, sh, px))

    su = au = sd = ad = 0.0
    out: list[tuple[float, float, float, float]] = []
    for t in range(window_sec):
        for side, sh, px in by_elapsed.get(t, []):
            su, au, sd, ad = apply_buy_fill(su, au, sd, ad, side=side, add_shares=sh, fill_price=px)
        out.append((su, sd, au, ad))
    return out


def print_wallet_30s(pm_series: list[tuple[float, float]], w_states: list[tuple[float, float, float, float]]) -> None:
    _p()
    _p("=== WALLET - end of 30s bucket (inventory from on-chain BUY timestamps; pm from public tape) ===")
    hdr = (
        "bucket   | pm_up  | pm_down | size_up   | size_down | avg_up | avg_down | "
        "roi_if_up | roi_if_down | pnl_if_up | pnl_if_down"
    )
    _p(hdr)
    for row in iter_bucket_trace_rows(pm_series, w_states):
        _p(
            f"{row['bucket']!s:7} | {float(row['pm_up']):6.4f} | {float(row['pm_down']):6.4f}  | "
            f"{float(row['size_up']):9.4f} | {float(row['size_down']):9.4f} | "
            f"{float(row['avg_up']):6.4f} | {float(row['avg_down']):8.4f}   | "
            f"{float(row['roi_if_up']):9.4f} | {float(row['roi_if_down']):10.4f} | "
            f"{float(row['pnl_if_up']):9.2f} | {float(row['pnl_if_down']):10.2f}"
        )


def print_sim_30s(pm_series: list[tuple[float, float]], sim_states: list[tuple[float, float, float, float]]) -> None:
    _p()
    _p("=== SIM v7 (V7_SMALL_BUDGET_4ORDERS) - end of 30s bucket ===")
    hdr = (
        "bucket   | pm_up  | pm_down | size_up   | size_down | avg_up | avg_down | "
        "roi_if_up | roi_if_down | pnl_if_up | pnl_if_down"
    )
    _p(hdr)
    for row in iter_bucket_trace_rows(pm_series, sim_states):
        _p(
            f"{row['bucket']!s:7} | {float(row['pm_up']):6.4f} | {float(row['pm_down']):6.4f}  | "
            f"{float(row['size_up']):9.4f} | {float(row['size_down']):9.4f} | "
            f"{float(row['avg_up']):6.4f} | {float(row['avg_down']):8.4f}   | "
            f"{float(row['roi_if_up']):9.4f} | {float(row['roi_if_down']):10.4f} | "
            f"{float(row['pnl_if_up']):9.2f} | {float(row['pnl_if_down']):10.2f}"
        )


def main() -> int:
    _LOG_LINES.clear()
    start_ts = dph.start_ts_from_slug(SLUG)
    _p("window", SLUG, "start_ts_utc", start_ts, "title Apr 22 12:15PM-12:30PM ET")
    _p("activity_csv", ACTIVITY, "(NOTE: file is last-N rows; may omit older fills)")
    _p("prices_csv", PRICES)

    buys = load_wallet_buys_for_slug(ACTIVITY, SLUG)
    _p()
    _p("=== WALLET - each BUY (Polymarket activity) ===")
    if not buys:
        _p("(no BUY rows for this slug in this export)")
    for i, row in enumerate(buys, start=1):
        ts = int(float(row.get("timestamp") or 0))
        el = ts - start_ts
        _p(
            f"{i:2}  elapsed={el:3}  ts={ts}  outcome={row.get('outcome','')!r}  "
            f"size={row.get('size')}  price={row.get('price')}  usdcSize={row.get('usdcSize')}  "
            f"datetime_utc={row.get('datetime_utc','')}"
        )

    if not PRICES.is_file():
        _p("missing prices; run ab_wallet94a73570_apr22_vs_v7sim.py first to enrich work copy")
        return 1

    by_e = load_prices_by_elapsed(PRICES)
    pm_series = forward_fill_prices(by_e)

    w_states = wallet_replay_states(buys, start_ts=start_ts)
    print_wallet_30s(pm_series, w_states)

    slug2, ticks = load_ticks_with_btc(PRICES)
    if len(ticks) < 900:
        _p("sim: insufficient ticks")
        return 1
    st = run_window_v7(ticks, params=V7_SMALL_BUDGET_4ORDERS)
    sim_states = replay_inventory_from_trades(st.trades)

    _p()
    _p("=== SIM v7 - each fill (replay mids) ===")
    su = au = sd = ad = 0.0
    for tr in st.trades:
        su, au, sd, ad = apply_buy_fill(su, au, sd, ad, side=tr.side, add_shares=tr.shares, fill_price=tr.price)
        _p(
            f"elapsed={tr.elapsed_sec:3}  {tr.side:4}  sh={tr.shares:.4f}  px={tr.price:.4f}  "
            f"notional={tr.notional:.2f}  {tr.reason}  "
            f"cum_u={su:.2f} cum_d={sd:.2f}  roi_up={roi_if_up(su,au,sd,ad):.3f} roi_dn={roi_if_down(su,au,sd,ad):.3f}"
        )

    print_sim_30s(pm_series, sim_states)

    last_u, last_d = pm_series[-1]
    w_down = last_d > last_u + 1e-9
    w_el = 899
    su, sd, au, ad = sim_states[w_el]
    sim_pu = pnl_if_up_usdc(su, au, sd, ad)
    sim_pd = pnl_if_down_usdc(su, au, sd, ad)
    sim_settle = sim_pd if w_down else sim_pu
    _p()
    _p(
        f"sim end inventory: up={su:.4f}@{au:.4f} down={sd:.4f}@{ad:.4f} spent={st.spent_usdc:.2f} "
        f"last_mid_winner={'down' if w_down else 'up'}  settled_pnl~{sim_settle:.2f} (proxy)"
    )

    w_el = 899
    wu, wd, wau, wad = w_states[w_el]
    w_pu = pnl_if_up_usdc(wu, wau, wd, wad)
    w_pd = pnl_if_down_usdc(wu, wau, wd, wad)
    w_settle = w_pd if w_down else w_pu
    _p(
        f"wallet replay end (from activity BUYs only): up={wu:.4f}@{wau:.4f} down={wd:.4f}@{wad:.4f} "
        f"same_proxy_settle~{w_settle:.2f}"
    )

    OUT_TXT.write_text("\n".join(_LOG_LINES), encoding="utf-8")
    _p("wrote", OUT_TXT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
