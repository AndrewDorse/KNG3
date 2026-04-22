#!/usr/bin/env python3
"""
Grid-search causal ladder params (alternate first leg, pair pacing, trailing-min filter).

Window sets:
- wallet: BTC slugs from wallet_window_summary.csv that have a matching prices CSV.
- recent: newest btc-updown-15m price exports under window_price_snapshots_public (by file mtime).
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from paladin_engine import PaladinParams

from simulate_paladin_window import (
    forward_fill_prices,
    load_prices_by_elapsed,
    resolve_winner_from_last_prices,
    run_window,
    settled_pnl_usdc,
    window_slug_from_prices_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUMMARY = (
    REPO_ROOT / "exports" / "target_wallet_e1_dataset" / "prepared" / "wallet_window_summary.csv"
)
DEFAULT_PRICES_DIR = REPO_ROOT / "exports" / "window_price_snapshots_public"


def max_elapsed_in_csv(path: Path) -> int:
    mx = -1
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mx = max(mx, int(float(row["elapsed_sec"])))
    return mx


@dataclass(frozen=True, slots=True)
class WindowSpec:
    slug: str
    prices_csv: Path


def discover_windows(
    *,
    summary_path: Path,
    prices_dir: Path,
    asset: str,
    count: int,
    min_max_elapsed: int,
) -> list[WindowSpec]:
    rows: list[tuple[str, str]] = []
    with summary_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("asset_symbol", "").upper() != asset.upper():
                continue
            slug = row.get("slug", "").strip()
            if not slug:
                continue
            hits = list(prices_dir.glob(f"*{slug}_prices.csv"))
            if not hits:
                continue
            p = hits[0]
            if max_elapsed_in_csv(p) < min_max_elapsed:
                continue
            rows.append((slug, str(p.resolve())))

    # Stable order: by slug
    rows = sorted(set(rows), key=lambda x: x[0])
    out = [WindowSpec(slug=s, prices_csv=Path(p)) for s, p in rows[:count]]
    return out


def discover_windows_recent(
    *,
    prices_dir: Path,
    count: int,
    min_max_elapsed: int,
    slug_prefix: str = "btc-updown-15m-",
) -> list[WindowSpec]:
    """Most recent unique BTC windows: *_prices.csv sorted by file mtime (newest first)."""
    all_csv = sorted(
        prices_dir.glob("*_prices.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    picked: list[WindowSpec] = []
    seen: set[str] = set()
    for p in all_csv:
        slug = window_slug_from_prices_csv(p)
        if "unknown" in slug or not slug.startswith(slug_prefix):
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


def eval_config(
    windows: list[WindowSpec],
    *,
    gap: float,
    trail_w: int,
    slip: float,
    pl_params: PaladinParams,
    max_shares_per_side: float = 20.0,
) -> tuple[float, float, int, list[dict[str, object]]]:
    """Returns mean settlement pnl, total pnl, n_positive, per-window rows."""
    rows: list[dict[str, object]] = []
    pnls: list[float] = []
    mx_sh = float(max_shares_per_side)
    for w in windows:
        raw = load_prices_by_elapsed(w.prices_csv)
        series = forward_fill_prices(raw)
        st = run_window(
            series,
            budget_usdc=80.0,
            params=pl_params,
            pair_sum_max=0.97,
            pair_sum_max_on_forced_hedge=1.0,
            single_leg_max_px=0.55,
            pair_only=True,
            stagger_pair_entry=True,
            stagger_hedge_force_after_seconds=45.0,
            target_min_roi=0.0,
            cooldown_seconds=0.0,
            dynamic_clip_cap=12.0,
            pair_size_pick="max_feasible",
            max_shares_per_side=mx_sh,
            pair_sum_tighten_per_fill=0.0,
            pair_sum_min_floor=0.90,
            second_leg_book_improve_eps=0.013,
            max_blended_pair_avg_sum=0.97,
            pending_hedge_bypass_imbalance_shares=10.0,
            discipline_relax_after_forced_sec=60.0,
            min_elapsed_for_flat_open=24,
            stagger_winning_side_first_when_position=False,
            stagger_symmetric_fallback_when_balanced=True,
            stagger_symmetric_fallback_roi_discount=0.03,
            stagger_symmetric_fallback_skip_first_leg_blend_cap=False,
            stagger_alternate_first_leg_when_balanced=True,
            min_elapsed_between_pair_starts=gap,
            entry_trailing_min_low_seconds=trail_w if trail_w > 0 else None,
            entry_trailing_low_slippage=slip,
        )
        win_side, _, _ = resolve_winner_from_last_prices(series)
        fm = st.snapshot_metrics()
        pnl = settled_pnl_usdc(fm, win_side)
        pnls.append(float(pnl))
        rows.append(
            {
                "slug": w.slug,
                "spent": st.spent_usdc,
                "trades": len(st.trades),
                "size_up": st.size_up,
                "size_down": st.size_down,
                "pnl_settle_proxy": pnl,
                "roi_on_spent": float(pnl) / st.spent_usdc if st.spent_usdc > 1e-9 else 0.0,
            }
        )
    mean_p = sum(pnls) / len(pnls) if pnls else 0.0
    tot = sum(pnls)
    pos = sum(1 for x in pnls if x > 1e-9)
    return mean_p, tot, pos, rows


def print_aggregate_stats(
    detail: list[dict[str, object]],
    *,
    cap: float,
    gap: float,
    trail_w: int,
    slip: float,
) -> None:
    n = len(detail)
    if n == 0:
        print("No rows.")
        return
    pnls = [float(r["pnl_settle_proxy"]) for r in detail]
    spent = [float(r["spent"]) for r in detail]
    trades = [int(float(r["trades"])) for r in detail]
    su = [float(r["size_up"]) for r in detail]
    sd = [float(r["size_down"]) for r in detail]
    rois = [pnls[i] / spent[i] if spent[i] > 1e-9 else 0.0 for i in range(n)]
    pos = sum(1 for x in pnls if x > 1e-9)
    at_cap = sum(
        1
        for i in range(n)
        if abs(su[i] - cap) < 0.25 and abs(sd[i] - cap) < 0.25
    )
    imb = sum(1 for i in range(n) if abs(su[i] - sd[i]) > 0.5)

    print()
    print("=== Aggregate (settlement proxy = higher final mid) ===")
    print(f"Ladder: gap={gap} trail_low={trail_w}s slip={slip} | max_shares/side={cap}")
    print(f"Windows: {n}")
    print(f"Mean settle PnL:   {sum(pnls) / n:.4f}  |  Total settle PnL:   {sum(pnls):.4f}")
    print(f"Mean spend:        {sum(spent) / n:.4f}  |  Total spend:        {sum(spent):.4f}")
    print(f"Mean trades/window:{sum(trades) / n:.2f}  |  Total trades:      {sum(trades)}")
    print(f"Mean size UP:      {sum(su) / n:.4f}  |  Mean size DOWN:     {sum(sd) / n:.4f}")
    print(f"Mean ROI/spend:    {sum(rois) / n:.4f}  (avg of per-window ratios)")
    print(f"Positive PnL:      {pos} / {n}  ({100.0 * pos / n:.1f}%)")
    print(f"Ended ~{cap:.0f}/{cap:.0f} cap:  {at_cap} / {n}  ({100.0 * at_cap / n:.1f}%)")
    print(f"Ended imbalanced:  {imb} / {n}  ({100.0 * imb / n:.1f}%)")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Calibrate ladder PALADIN on BTC windows (wallet list or newest exports by mtime)"
    )
    ap.add_argument(
        "--window-source",
        choices=["wallet", "recent"],
        default="wallet",
        help="wallet=intersect wallet summary with prices; recent=newest btc-updown-15m CSVs by file mtime.",
    )
    ap.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    ap.add_argument("--prices-dir", type=Path, default=DEFAULT_PRICES_DIR)
    ap.add_argument("--asset", type=str, default="BTC")
    ap.add_argument("--count", type=int, default=35, help="Max windows to use (after gates).")
    ap.add_argument("--min-max-elapsed", type=int, default=800)
    ap.add_argument("--out-grid", type=Path, default=None)
    ap.add_argument("--out-best-per-window", type=Path, default=None)
    ap.add_argument(
        "--max-shares-per-side",
        type=float,
        default=20.0,
        help="Hard cap per leg (pair clips stay 5 until min leg>=20 in engine).",
    )
    ap.add_argument(
        "--no-grid",
        action="store_true",
        help="Skip grid search; run one ladder config (--pair-gap, --trail-seconds, --slip).",
    )
    ap.add_argument("--pair-gap", type=float, default=110.0)
    ap.add_argument("--trail-seconds", type=int, default=30)
    ap.add_argument("--slip", type=float, default=0.03)
    args = ap.parse_args()

    cnt = int(args.count)
    if args.window_source == "recent":
        windows = discover_windows_recent(
            prices_dir=args.prices_dir,
            count=cnt,
            min_max_elapsed=int(args.min_max_elapsed),
        )
    else:
        windows = discover_windows(
            summary_path=args.summary,
            prices_dir=args.prices_dir,
            asset=args.asset,
            count=cnt,
            min_max_elapsed=int(args.min_max_elapsed),
        )

    out_grid = args.out_grid
    out_best = args.out_best_per_window
    if out_grid is None:
        out_grid = REPO_ROOT / "exports" / (
            f"paladin_ladder_calib_grid_{args.window_source}_{cnt}btc.csv"
        )
    if out_best is None:
        out_best = REPO_ROOT / "exports" / (
            f"paladin_ladder_calib_best_per_window_{args.window_source}_{cnt}btc.csv"
        )

    if not windows:
        print("ERROR: no windows matched filters.")
        return 1

    if len(windows) < cnt:
        print(
            f"WARNING: only {len(windows)} windows match "
            f"(source={args.window_source}, max_elapsed>={args.min_max_elapsed}); "
            f"requested {cnt}."
        )

    pl_params = PaladinParams(
        profit_lock_min_shares_per_side=100.0,
        roi_lock_min_each=0.99,
        profit_lock_usdc_each_scenario=float("inf"),
    )

    mx_cap = float(args.max_shares_per_side)

    if args.no_grid:
        mean_p, tot, pos, best_detail = eval_config(
            windows,
            gap=float(args.pair_gap),
            trail_w=int(args.trail_seconds),
            slip=float(args.slip),
            pl_params=pl_params,
            max_shares_per_side=mx_cap,
        )
        best_key = (float(args.pair_gap), int(args.trail_seconds), float(args.slip))
        out_best.parent.mkdir(parents=True, exist_ok=True)
        with out_best.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "slug",
                    "spent",
                    "trades",
                    "size_up",
                    "size_down",
                    "pnl_settle_proxy",
                    "roi_on_spent",
                ],
            )
            w.writeheader()
            for r in best_detail:
                w.writerow(r)
        print(f"Windows used: {len(windows)} (single-eval, no grid)")
        print_aggregate_stats(
            best_detail,
            cap=mx_cap,
            gap=best_key[0],
            trail_w=best_key[1],
            slip=best_key[2],
        )
        print(f"Wrote {out_best}")
        return 0

    gaps = [75.0, 90.0, 100.0, 110.0]
    trails = [0, 30, 35, 45, 55]
    slips = [0.02, 0.03, 0.04]

    out_grid.parent.mkdir(parents=True, exist_ok=True)
    grid_rows: list[dict[str, object]] = []
    best_mean = -1e18
    best_key: tuple[float, int, float] | None = None
    best_detail: list[dict[str, object]] = []

    for gap in gaps:
        for trail_w in trails:
            for slip in slips:
                mean_p, tot, pos, detail = eval_config(
                    windows,
                    gap=gap,
                    trail_w=trail_w,
                    slip=slip,
                    pl_params=pl_params,
                    max_shares_per_side=mx_cap,
                )
                grid_rows.append(
                    {
                        "min_elapsed_between_pair_starts": gap,
                        "entry_trailing_min_low_seconds": trail_w,
                        "entry_trailing_low_slippage": slip,
                        "n_windows": len(windows),
                        "mean_settle_pnl": round(mean_p, 4),
                        "total_settle_pnl": round(tot, 4),
                        "n_positive_windows": pos,
                        "frac_positive": round(pos / len(windows), 4) if windows else 0.0,
                    }
                )
                if mean_p > best_mean:
                    best_mean = mean_p
                    best_key = (gap, trail_w, slip)
                    best_detail = detail

    assert best_key is not None
    with out_grid.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(grid_rows[0].keys()))
        w.writeheader()
        w.writerows(grid_rows)

    with out_best.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "slug",
                "spent",
                "trades",
                "size_up",
                "size_down",
                "pnl_settle_proxy",
                "roi_on_spent",
            ],
        )
        w.writeheader()
        for r in best_detail:
            w.writerow(r)

    print(f"Windows used: {len(windows)}")
    for w in windows:
        print(f"  {w.slug}")
    print(f"Best by mean settle PnL (proxy winner=final mid): gap={best_key[0]} trail={best_key[1]} slip={best_key[2]}")
    print(f"  mean_pnl={best_mean:.4f} total={sum(float(r['pnl_settle_proxy']) for r in best_detail):.4f}")
    print(f"  positive_windows={sum(1 for r in best_detail if float(r['pnl_settle_proxy']) > 1e-9)}/{len(best_detail)}")
    print_aggregate_stats(
        best_detail,
        cap=mx_cap,
        gap=best_key[0],
        trail_w=best_key[1],
        slip=best_key[2],
    )
    print(f"Wrote {out_grid}")
    print(f"Wrote {out_best}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
