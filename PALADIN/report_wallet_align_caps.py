#!/usr/bin/env python3
"""
Compare PALADIN v5 (tuned) vs target-wallet rows on the same BTC 15m slugs.

- Sim: 5-share clips, max_shares_per_side in {10,20,30,40,50}, budget scales with cap.
- Settlement for sim uses wallet CSV `winner` when slug matches (closer to wallet outcome),
  else falls back to last-mid winner from price series.
- Wallet: sums `wallet_realized_pnl_usdc` / win-rate on the same ordered window list.

Per cap, picks the best of a small param grid on the window list by sim total PnL (wallet-winner),
then prints pools 10,20,30,50,100,200 (truncated if fewer windows) for sim + wallet.

Runs two sections: (1) newest 200 BTC windows from public price exports; (2) newest windows whose
slug appears in the merged wallet CSVs so wallet_n equals the pool size (apples-to-apples vs wallet).
"""

from __future__ import annotations

import csv
import itertools
from pathlib import Path

from calibrate_ladder_wallet_windows import (
    WindowSpec,
    discover_windows_recent,
    max_elapsed_in_csv,
)
from paladin_v5 import PaladinV5Params, run_window_v5, settled_pnl_for_winner
from simulate_paladin_window import (
    forward_fill_prices,
    load_prices_by_elapsed,
    resolve_winner_from_last_prices,
    window_slug_from_prices_csv,
)

REPO = Path(__file__).resolve().parents[1]
PRICES_DIR = REPO / "exports" / "window_price_snapshots_public"
# Later entries win on duplicate slugs (prefer dataset_simulation rows when present).
WALLET_CSVS: list[tuple[Path, str, str]] = [
    (REPO / "exports" / "reconstruct_wallet_e1_archived_btc15" / "wallet_e1_archived_btc15_summary.csv", "wallet_realized_pnl", "wallet_total_spend"),
    (REPO / "exports" / "reconstruct_wallet_e1_recent20_btc15" / "window_summary.csv", "wallet_realized_pnl", "wallet_total_spend"),
    (REPO / "exports" / "compare_wallet_e1_full_overlap_vs_champ5" / "wallet_vs_champ5_per_window.csv", "wallet_realized_pnl_usdc", "wallet_spend_usdc"),
    (REPO / "exports" / "target_wallet_e1_dataset_champ7" / "wallet_vs_champ7_per_window.csv", "wallet_realized_pnl_usdc", "wallet_spend_usdc"),
    (REPO / "exports" / "target_wallet_e1_dataset_simulation" / "wallet_vs_champ5_per_window.csv", "wallet_realized_pnl_usdc", "wallet_spend_usdc"),
]
POOLS = (10, 20, 30, 50, 100, 200)
CAPS = (10, 20, 30, 40, 50)
MIN_MAX_ELAPSED = 800
WIN_EPS = 1e-6


def discover_windows_wallet_slugs(
    *,
    prices_dir: Path,
    slug_set: set[str],
    count: int,
    min_max_elapsed: int,
    slug_prefix: str = "btc-updown-15m-",
) -> list[WindowSpec]:
    """Newest price snapshots whose slug is in ``slug_set`` (intersection with exports)."""
    all_csv = sorted(
        prices_dir.glob("*_prices.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    picked: list[WindowSpec] = []
    seen: set[str] = set()
    for p in all_csv:
        slug = window_slug_from_prices_csv(p)
        if "unknown" in slug or not slug.startswith(slug_prefix) or slug not in slug_set:
            continue
        if slug in seen:
            continue
        if max_elapsed_in_csv(p) < min_max_elapsed:
            continue
        seen.add(slug)
        picked.append(WindowSpec(slug=slug, prices_csv=p.resolve()))
        if len(picked) >= count:
            break
    return picked


def load_wallet_btc() -> dict[str, dict[str, float | str]]:
    out: dict[str, dict[str, float | str]] = {}
    for path, pnl_key, spend_key in WALLET_CSVS:
        if not path.is_file():
            continue
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                slug = row.get("slug", "").strip()
                if not slug.startswith("btc-updown-15m-"):
                    continue
                if pnl_key not in row or spend_key not in row:
                    continue
                try:
                    out[slug] = {
                        "winner": str(row.get("winner", "tie")).strip().upper(),
                        "pnl": float(row[pnl_key]),
                        "spend": float(row[spend_key]),
                    }
                except (KeyError, ValueError):
                    continue
    return out


def sim_pnl_for_slug(
    series: list,
    slug: str,
    wallet: dict[str, dict[str, float | str]],
    p: PaladinV5Params,
) -> float:
    st = run_window_v5(series, params=p)
    m = st.snapshot_metrics()
    if slug in wallet:
        return settled_pnl_for_winner(m, str(wallet[slug]["winner"]))
    w, _, _ = resolve_winner_from_last_prices(series)
    return settled_pnl_for_winner(m, w.upper() if w != "tie" else "tie")


def eval_pool(
    windows: list,
    series_list: list,
    wallet: dict[str, dict[str, float | str]],
    p: PaladinV5Params,
    n: int,
) -> tuple[float, float, int, float, int]:
    """Returns sim_total, sim_mean, sim_wins, wallet_total, wallet_wins."""
    sim_pnls: list[float] = []
    wal_pnls: list[float] = []
    for i in range(n):
        slug = windows[i].slug
        sim_pnls.append(sim_pnl_for_slug(series_list[i], slug, wallet, p))
        if slug in wallet:
            wal_pnls.append(float(wallet[slug]["pnl"]))
    stot = sum(sim_pnls)
    sw = sum(1 for x in sim_pnls if x > WIN_EPS)
    wtot = sum(wal_pnls)
    ww = sum(1 for x in wal_pnls if x > WIN_EPS)
    return stot, stot / n, sw, wtot, ww


def base_params_for_cap(cap: float, **kw: object) -> PaladinV5Params:
    b = min(8000.0, max(150.0, float(cap) * 28.0))
    d: dict = {
        "budget_usdc": b,
        "clip_shares": 5.0,
        "max_shares_per_side": float(cap),
        "first_leg_min_winner_px": 0.5,
        "pair_sum_max_on_forced_hedge": 1.15,
        "winner_drop_eps": 0.05,
        "winner_drop_window_seconds": 8,
        "improvement_buy_enabled": False,
        "min_notional": 1.0,
        "max_pair_cycles": 0,
        "flat_entry_max_pair_sum": 0.99,
    }
    d.update(kw)
    return PaladinV5Params(**d)


def tune_cap(windows: list, series_all: list, wallet: dict, cap: float) -> PaladinV5Params:
    # Small grid (32) — tune on 200-window pool by sim PnL (wallet-winner settlement).
    grid = itertools.product(
        [0.56, 0.58],
        [0.045, 0.05],
        [1.01, 1.02],
        [0.99, 0.995],
        [90.0, 100.0],
    )
    best_p: PaladinV5Params | None = None
    best_tot = -1e18
    for fmax, margin, fsum, addm, hf in grid:
        p = base_params_for_cap(
            cap,
            first_leg_max_winner_px=float(fmax),
            second_leg_margin=float(margin),
            forced_hedge_max_book_sum=float(fsum),
            additional_pair_max_pair_sum=float(addm),
            hedge_force_after_seconds=float(hf),
        )
        tot, _, _, _, _ = eval_pool(windows, series_all, wallet, p, min(200, len(windows)))
        if tot > best_tot:
            best_tot = tot
            best_p = p
    assert best_p is not None
    return best_p


def run_section(
    label: str,
    wins: list[WindowSpec],
    series_all: list,
    wallet: dict[str, dict[str, float | str]],
) -> None:
    nwin = len(wins)
    matched = sum(1 for w in wins if w.slug in wallet)
    print(f"### {label}")
    print(f"windows={nwin} | wallet_slugs_matched={matched}/{nwin} | wallet_rows_total={len(wallet)}")
    print()

    for cap in CAPS:
        p = tune_cap(wins, series_all, wallet, cap)
        print(
            f"--- tuned cap={cap:g}/side | budget={p.budget_usdc:.0f} | "
            f"fmax={p.first_leg_max_winner_px} margin={p.second_leg_margin} "
            f"forced_sum={p.forced_hedge_max_book_sum} add_pair_sum={p.additional_pair_max_pair_sum} "
            f"hedge_s={p.hedge_force_after_seconds} ---"
        )
        print("pool_n\tsim_total_usd\tsim_WR_pct\tsim_wins\twallet_total_usd\twallet_WR_pct\twallet_wins\twallet_n")
        seen_nn: set[int] = set()
        for n in POOLS:
            nn = min(n, nwin)
            if nn <= 0 or nn in seen_nn:
                continue
            seen_nn.add(nn)
            stot, _, sw, wtot, ww = eval_pool(wins, series_all, wallet, p, nn)
            wn = sum(1 for i in range(nn) if wins[i].slug in wallet)
            print(
                f"{nn}\t{stot:.2f}\t{100.0 * sw / nn:.1f}\t{sw}\t"
                f"{wtot:.2f}\t{(100.0 * ww / wn if wn else 0.0):.1f}\t{ww}\t{wn}"
            )
        print()


def main() -> int:
    wallet = load_wallet_btc()
    if not wallet:
        print("WARN: wallet CSV missing or empty; checked:", len(WALLET_CSVS), "paths")

    print("wallet_align_report | BTC 15m | min_max_elapsed>=", MIN_MAX_ELAPSED)
    print(f"wallet_csvs_merged={len(WALLET_CSVS)} files")
    print()

    wins_recent = discover_windows_recent(
        prices_dir=PRICES_DIR, count=200, min_max_elapsed=MIN_MAX_ELAPSED
    )
    series_recent: list = []
    for w in wins_recent:
        raw = load_prices_by_elapsed(w.prices_csv)
        series_recent.append(forward_fill_prices(raw))
    run_section("recent_price_windows (mtime, n=200)", wins_recent, series_recent, wallet)

    slug_set = set(wallet.keys())
    wins_wal = discover_windows_wallet_slugs(
        prices_dir=PRICES_DIR,
        slug_set=slug_set,
        count=200,
        min_max_elapsed=MIN_MAX_ELAPSED,
    )
    series_wal: list = []
    for w in wins_wal:
        raw = load_prices_by_elapsed(w.prices_csv)
        series_wal.append(forward_fill_prices(raw))
    run_section(
        "wallet_slug_intersect (mtime, up to 200; full wallet PnL on every row)",
        wins_wal,
        series_wal,
        wallet,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
