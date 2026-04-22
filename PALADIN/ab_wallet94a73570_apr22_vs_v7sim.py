#!/usr/bin/env python3
"""
A/B: wallet `exports/wallet_94a73570_from_apr22_1100et_per_window_shares_vwap.csv`
vs PALADIN v7 sim (V7_SMALL_BUDGET_4ORDERS) on the same slugs' price tapes.

- Copies each window's *_prices.csv into a work dir and enriches with Binance if needed.
- A = wallet pnl_redeem_minus_buy_usdc (actual).
- B_proxy = sim settled PnL using last-tick mid winner (replay convention).
- B_actual = sim settled PnL using winner inferred from redeem vs shares_up/down.
"""

from __future__ import annotations

import csv
import shutil
import sys
from pathlib import Path
from typing import Literal

PALADIN_DIR = Path(__file__).resolve().parent
REPO = PALADIN_DIR.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(PALADIN_DIR))

import requests  # noqa: E402
import simulate_dual_profit_hedge as dph  # noqa: E402
from paladin_v7 import V7_SMALL_BUDGET_4ORDERS, load_ticks_with_btc, run_window_v7  # noqa: E402
from simulate_paladin_window import (  # noqa: E402
    resolve_winner_from_last_prices,
    settled_pnl_usdc,
)

WALLET_CSV = REPO / "exports" / "wallet_94a73570_from_apr22_1100et_per_window_shares_vwap.csv"
EXPORTS_SNAP = REPO / "exports" / "window_price_snapshots_public"
WORK_DIR = REPO / "exports" / "ab_apr22_wallet94a73570_sim_work"
OUT_CSV = REPO / "exports" / "ab_wallet94a73570_apr22_wallet_vs_v7sim.csv"

ROW_FIELDS = [
    "slug",
    "prices_csv",
    "wallet_buy_usdc",
    "wallet_redeem_usdc",
    "wallet_pnl",
    "sim_spent_usdc",
    "sim_n_trades",
    "winner_proxy",
    "winner_actual",
    "sim_proxy_pnl",
    "sim_actual_pnl",
    "delta_proxy_minus_wallet",
    "delta_actual_minus_wallet",
    "error",
]


def _row(**kwargs: object) -> dict[str, object]:
    out: dict[str, object] = {k: "" for k in ROW_FIELDS}
    out.update(kwargs)
    return out


def max_elapsed_in_csv(path: Path) -> int:
    mx = -1
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                mx = max(mx, int(float(row["elapsed_sec"])))
            except (KeyError, TypeError, ValueError):
                continue
    return mx


def find_prices_csv(slug: str) -> Path | None:
    hits = [p for p in EXPORTS_SNAP.glob("*_prices.csv") if slug in p.name]
    if not hits:
        return None
    hits.sort(
        key=lambda p: (csv_has_btc_volume(p), max_elapsed_in_csv(p), p.stat().st_mtime),
        reverse=True,
    )
    return hits[0]


def csv_has_btc_volume(path: Path) -> bool:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "btc_volume" not in r.fieldnames:
            return False
        for row in r:
            if str(row.get("btc_volume", "")).strip():
                return True
    return False


def infer_winner_from_redeem(
    shares_up: float, shares_down: float, redeem_usdc: float
) -> Literal["up", "down", "tie"]:
    su, sd, red = float(shares_up), float(shares_down), float(redeem_usdc)
    if red < 1e-6:
        if su < 1e-9 and sd < 1e-9:
            return "tie"
        if su > 1e-9 and sd < 1e-9:
            return "down"
        if sd > 1e-9 and su < 1e-9:
            return "up"
        return "tie"
    du = abs(red - su)
    dd = abs(red - sd)
    tol = max(0.15, 0.02 * max(su, sd, 1.0))
    if du <= tol and du <= dd:
        return "up"
    if dd <= tol and dd < du:
        return "down"
    return "up" if du < dd else "down"


def pm_series_from_ticks(ticks: list) -> list[tuple[float, float]]:
    return [(float(t.pm_u), float(t.pm_d)) for t in ticks]


def main() -> int:
    if not WALLET_CSV.is_file():
        print(f"missing wallet csv: {WALLET_CSV}")
        return 1

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    rows_out: list[dict[str, object]] = []
    session = requests.Session()

    with WALLET_CSV.open(newline="", encoding="utf-8") as f:
        wallet_rows = list(csv.DictReader(f))

    for wr in wallet_rows:
        slug = (wr.get("slug") or "").strip()
        if not slug:
            continue
        src = find_prices_csv(slug)
        if src is None:
            rows_out.append(
                _row(
                    slug=slug,
                    wallet_buy_usdc=wr.get("buy_usdc", ""),
                    wallet_redeem_usdc=wr.get("redeem_usdc", ""),
                    wallet_pnl=wr.get("pnl_redeem_minus_buy_usdc", ""),
                    error="no_prices_csv",
                )
            )
            continue

        if csv_has_btc_volume(src):
            sim_path = src
            sim_label = src.name
        else:
            import enrich_oldest_windows_with_btc_price as enrich  # noqa: PLC0415

            dst = WORK_DIR / src.name
            shutil.copy2(src, dst)
            w = enrich.WindowFile(path=dst, slug=slug, start_ts=dph.start_ts_from_slug(slug))
            enrich.enrich_window(w, session)
            sim_path = dst
            sim_label = dst.name

        slug2, ticks = load_ticks_with_btc(sim_path)
        if len(ticks) < 900 or not slug2:
            rows_out.append(
                _row(
                    slug=slug,
                    prices_csv=sim_label,
                    wallet_buy_usdc=wr.get("buy_usdc", ""),
                    wallet_redeem_usdc=wr.get("redeem_usdc", ""),
                    wallet_pnl=wr.get("pnl_redeem_minus_buy_usdc", ""),
                    error="no_ticks_or_btc",
                )
            )
            continue

        st = run_window_v7(ticks, params=V7_SMALL_BUDGET_4ORDERS)
        pm = pm_series_from_ticks(ticks)
        w_proxy, _, _ = resolve_winner_from_last_prices(pm)
        m = st.snapshot_metrics()
        pnl_proxy = settled_pnl_usdc(m, w_proxy)

        su = float(wr["shares_up"])
        sd = float(wr["shares_down"])
        red = float(wr["redeem_usdc"])
        w_act = infer_winner_from_redeem(su, sd, red)
        pnl_act = settled_pnl_usdc(m, w_act)

        rows_out.append(
            _row(
                slug=slug,
                prices_csv=sim_label,
                wallet_buy_usdc=wr.get("buy_usdc", ""),
                wallet_redeem_usdc=wr.get("redeem_usdc", ""),
                wallet_pnl=wr.get("pnl_redeem_minus_buy_usdc", ""),
                sim_spent_usdc=round(float(st.spent_usdc), 4),
                sim_n_trades=len(st.trades),
                winner_proxy=w_proxy,
                winner_actual=w_act,
                sim_proxy_pnl=round(pnl_proxy, 4),
                sim_actual_pnl=round(pnl_act, 4),
                delta_proxy_minus_wallet=round(pnl_proxy - float(wr["pnl_redeem_minus_buy_usdc"]), 4),
                delta_actual_minus_wallet=round(pnl_act - float(wr["pnl_redeem_minus_buy_usdc"]), 4),
                error="",
            )
        )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if rows_out:
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=ROW_FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows_out)

    # stdout summary
    def _f(x: str) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return 0.0

    ok = [r for r in rows_out if not r.get("error")]
    tw = sum(_f(str(r.get("wallet_pnl", 0))) for r in ok)
    tp = sum(_f(str(r.get("sim_proxy_pnl", 0))) for r in ok)
    ta = sum(_f(str(r.get("sim_actual_pnl", 0))) for r in ok)
    print("ab_wallet94a73570_apr22 | wallet vs V7_SMALL_BUDGET_4ORDERS")
    print(f"work_dir={WORK_DIR}")
    print(f"rows={len(rows_out)} ok={len(ok)}")
    print(f"sum_wallet_pnl_usdc={tw:.4f}")
    print(f"sum_sim_proxy_pnl_usdc={tp:.4f}  (delta vs wallet {tp - tw:+.4f})")
    print(f"sum_sim_actualwinner_pnl_usdc={ta:.4f}  (delta vs wallet {ta - tw:+.4f})")
    print(f"wrote {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
