"""Shared helpers for 35-window PALADIN evaluation and StrategySpec (de)serialization."""

from __future__ import annotations

import csv
import json
from copy import replace
from dataclasses import asdict, dataclass
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


def btc_windows_35() -> list[dict[str, str]]:
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


def resolve_export_path(p: str) -> Path:
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


def aggregate_window_results(rows: list[tuple[float, float]]) -> dict[str, float]:
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


def default_pl_params() -> PaladinParams:
    pl_cfg = load_profit_lock_config(DEFAULT_PROFIT_LOCK_CONFIG)
    return PaladinParams(
        profit_lock_min_shares_per_side=float(pl_cfg["profit_lock_min_shares_per_side"]),
        roi_lock_min_each=float(pl_cfg["roi_lock_min_each"]),
        profit_lock_usdc_each_scenario=float(pl_cfg["profit_lock_usdc_each_scenario"]),
    )


def run_paladin_on_windows(
    spec: StrategySpec, windows: list[dict[str, str]], pl_params: PaladinParams
) -> dict[str, float]:
    per: list[tuple[float, float]] = []
    for w in windows:
        prices_path = resolve_export_path(w["public_snapshot_csv"])
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
    return aggregate_window_results(per)


def run_wallet_on_windows(windows: list[dict[str, str]]) -> dict[str, float]:
    per: list[tuple[float, float]] = []
    for w in windows:
        wallet_csv = resolve_export_path(w["wallet_per_window_csv"])
        prices_path = resolve_export_path(w["public_snapshot_csv"])
        raw = load_prices_by_elapsed(prices_path)
        series = forward_fill_prices(raw)
        spent, pnl = wallet_spent_and_settled_pnl(wallet_csv, series)
        per.append((spent, pnl))
    return aggregate_window_results(per)


def spec_to_dict(spec: StrategySpec) -> dict[str, Any]:
    d = asdict(spec)
    return d


def spec_from_dict(d: dict[str, Any]) -> StrategySpec:
    return StrategySpec(
        key=str(d["key"]),
        label=str(d["label"]),
        min_elapsed_for_flat_open=d.get("min_elapsed_for_flat_open"),
        max_elapsed_to_start_flat=d.get("max_elapsed_to_start_flat"),
        dynamic_clip_max=float(d.get("dynamic_clip_max", 10.0)),
        pair_sum_tighten_per_fill=float(d.get("pair_sum_tighten_per_fill", 0.004)),
        pair_sum_min_floor=float(d.get("pair_sum_min_floor", 0.90)),
        pending_hedge_bypass_imbalance_shares=float(d.get("pending_hedge_bypass_imbalance_shares", 10.0)),
        discipline_relax_after_forced_sec=float(d.get("discipline_relax_after_forced_sec", 60.0)),
        second_leg_book_improve_eps=float(d.get("second_leg_book_improve_eps", 0.0)),
        max_blended_pair_avg_sum=(
            float(d["max_blended_pair_avg_sum"])
            if d.get("max_blended_pair_avg_sum") is not None
            else None
        ),
    )


def initial_baseline_spec() -> StrategySpec:
    return StrategySpec(
        key="baseline_v0",
        label="Initial: live-style 24s entry, tighten 0.004, floor 0.90, bypass 10, relax 60",
        min_elapsed_for_flat_open=24,
        dynamic_clip_max=10.0,
        pair_sum_tighten_per_fill=0.004,
        pair_sum_min_floor=0.90,
        pending_hedge_bypass_imbalance_shares=10.0,
        discipline_relax_after_forced_sec=60.0,
        second_leg_book_improve_eps=0.0,
        max_blended_pair_avg_sum=None,
        max_elapsed_to_start_flat=None,
    )


def research_target_wallet(windows: list[dict[str, str]]) -> dict[str, Any]:
    """Aggregate on-chain wallet behaviour on the 35 windows (for heuristic mutations)."""
    trade_counts: list[int] = []
    first_elapsed: list[int] = []
    spend_skews: list[float] = []
    for w in windows:
        trade_counts.append(int(float(w.get("wallet_trade_count") or 0)))
        first_elapsed.append(int(float(w.get("first_elapsed_sec") or 0)))
        wp = resolve_export_path(w["wallet_per_window_csv"])
        skew = 0.5
        if wp.is_file():
            with wp.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if rows:
                last = rows[-1]
                raw = last.get("cum_up_spend_ratio_0_1")
                if raw not in (None, ""):
                    try:
                        skew = float(raw)
                    except ValueError:
                        pass
        spend_skews.append(abs(skew - 0.5))

    def _median(xs: list[float]) -> float:
        s = sorted(xs)
        n = len(s)
        if not n:
            return float("nan")
        m = n // 2
        return s[m] if n % 2 else 0.5 * (s[m - 1] + s[m])

    tc = [float(x) for x in trade_counts]
    fe = [float(x) for x in first_elapsed]
    return {
        "n_windows": len(windows),
        "wallet_trade_count_median": _median(tc),
        "wallet_trade_count_p75": sorted(tc)[int(0.75 * (len(tc) - 1))] if tc else 0.0,
        "first_elapsed_median": _median(fe),
        "first_elapsed_p90": sorted(fe)[min(len(fe) - 1, int(0.90 * (len(fe) - 1)))] if fe else 0.0,
        "spend_skew_abs_mean": sum(spend_skews) / len(spend_skews) if spend_skews else 0.0,
        "spend_skew_abs_p75": sorted(spend_skews)[int(0.75 * (len(spend_skews) - 1))] if spend_skews else 0.0,
    }


def _snap(x: float, grid: list[float]) -> float:
    return min(grid, key=lambda g: abs(g - x))


def propose_candidates(
    base: StrategySpec,
    cycle: int,
    research: dict[str, Any],
) -> tuple[str, list[StrategySpec]]:
    """
    Return (research_one_liner, candidates). Candidates are variations around base
    driven by cycle phase + wallet statistics.
    """
    k = (cycle - 1) % 10
    ideas: list[str] = []
    candidates: list[StrategySpec] = []

    skew_m = float(research.get("spend_skew_abs_mean") or 0.0)
    fe_p90 = float(research.get("first_elapsed_p90") or 0.0)
    tc_med = float(research.get("wallet_trade_count_median") or 0.0)

    clips = [5.0, 6.0, 7.0, 8.0, 10.0, 12.0]
    tightens = [0.0025, 0.003, 0.0035, 0.004, 0.0045, 0.005]
    floors = [0.88, 0.89, 0.90, 0.91, 0.92]
    bypasses = [6.0, 8.0, 10.0, 12.0, 14.0]
    relaxes = [30.0, 45.0, 60.0, 75.0, 90.0]
    book_eps = [0.0, 0.008, 0.01, 0.012, 0.015, 0.018, 0.02]
    blended_opts: list[float | None] = [None, 1.02, 1.03, 1.04, 1.05]
    entries = [0, 12, 18, 24, 30]

    def add(label_suffix: str, **kwargs: Any) -> None:
        patch = spec_to_dict(base)
        patch.update(kwargs)
        patch["key"] = f"c{cycle}_{label_suffix}"[:48]
        patch["label"] = f"cycle{cycle}: {label_suffix}"
        candidates.append(spec_from_dict(patch))

    if k == 0:
        ideas.append("Axis A: dynamic clip ladder around baseline (wallet activity median %.0f)" % tc_med)
        for clip in sorted({clips[0], _snap(base.dynamic_clip_max - 1, clips), _snap(base.dynamic_clip_max, clips), _snap(base.dynamic_clip_max + 1, clips), clips[-1]}):
            if 5.0 <= clip <= 12.0:
                add(f"clip{clip:g}", dynamic_clip_max=clip)
    elif k == 1:
        ideas.append("Axis B: pair-sum tighten + floor (skew mean %.3f)" % skew_m)
        for tn in sorted(
            {_snap(base.pair_sum_tighten_per_fill - 0.0005, tightens), _snap(base.pair_sum_tighten_per_fill, tightens), _snap(base.pair_sum_tighten_per_fill + 0.0005, tightens)}
        ):
            for fl in sorted({_snap(base.pair_sum_min_floor - 0.01, floors), _snap(base.pair_sum_min_floor, floors), _snap(base.pair_sum_min_floor + 0.01, floors)}):
                add(f"tight{tn}_floor{fl:.2f}", pair_sum_tighten_per_fill=tn, pair_sum_min_floor=fl)
    elif k == 2:
        ideas.append("Axis C: bypass vs relax timer (late-entry p90 %.0fs)" % fe_p90)
        for bp in sorted({_snap(base.pending_hedge_bypass_imbalance_shares - 2, bypasses), _snap(base.pending_hedge_bypass_imbalance_shares, bypasses), _snap(base.pending_hedge_bypass_imbalance_shares + 2, bypasses)}):
            for rl in sorted({_snap(base.discipline_relax_after_forced_sec - 15, relaxes), _snap(base.discipline_relax_after_forced_sec, relaxes), _snap(base.discipline_relax_after_forced_sec + 15, relaxes)}):
                add(f"bypass{bp:g}_relax{rl:g}", pending_hedge_bypass_imbalance_shares=bp, discipline_relax_after_forced_sec=rl)
    elif k == 3:
        ideas.append("Axis D: second-leg book eps + blended cap")
        for eps in sorted({_snap(base.second_leg_book_improve_eps + 0.003, book_eps), _snap(base.second_leg_book_improve_eps, book_eps), _snap(base.second_leg_book_improve_eps + 0.006, book_eps)}):
            for bl in blended_opts[:4]:
                if bl is None:
                    add(f"eps{eps}_blend_off", second_leg_book_improve_eps=eps, max_blended_pair_avg_sum=None)
                else:
                    add(f"eps{eps}_blend{bl:.2f}", second_leg_book_improve_eps=eps, max_blended_pair_avg_sum=bl)
    elif k == 4:
        ideas.append("Axis E: entry delay sweep (wallet starts late in some windows)")
        for ent in sorted({0, 12, 18, 24, 30, int(_snap(float(base.min_elapsed_for_flat_open or 24), [float(x) for x in entries]))}):
            add(f"entry{ent}s", min_elapsed_for_flat_open=int(ent))
    elif k == 5:
        ideas.append(
            "Axis F: skew-response - if skew high, stress book discipline; else looser bypass"
        )
        if skew_m > 0.12:
            add("skew_high_eps", second_leg_book_improve_eps=min(0.02, base.second_leg_book_improve_eps + 0.005), max_blended_pair_avg_sum=1.03)
            add("skew_high_tight", pair_sum_tighten_per_fill=min(0.005, base.pair_sum_tighten_per_fill + 0.0005))
        else:
            add("skew_low_loose", pair_sum_tighten_per_fill=max(0.0025, base.pair_sum_tighten_per_fill - 0.0005), pending_hedge_bypass_imbalance_shares=min(14.0, base.pending_hedge_bypass_imbalance_shares + 2.0))
    elif k == 6:
        ideas.append("Axis G: high wallet activity -> smaller clips + slightly looser floor")
        if tc_med > 45:
            add("busy_clip6_floor088", dynamic_clip_max=6.0, pair_sum_min_floor=0.88)
            add("busy_clip7_tight003", dynamic_clip_max=7.0, pair_sum_tighten_per_fill=0.003)
        else:
            add("quiet_clip10_relax45", dynamic_clip_max=10.0, discipline_relax_after_forced_sec=45.0)
    elif k == 7:
        ideas.append("Axis H: composite touch-up (clip + bypass + eps)")
        add(
            "combo1",
            dynamic_clip_max=_snap(base.dynamic_clip_max, clips),
            pending_hedge_bypass_imbalance_shares=_snap(base.pending_hedge_bypass_imbalance_shares + 1, bypasses),
            second_leg_book_improve_eps=_snap(base.second_leg_book_improve_eps + 0.002, book_eps),
        )
        add(
            "combo2",
            pair_sum_tighten_per_fill=_snap(base.pair_sum_tighten_per_fill - 0.0005, tightens),
            discipline_relax_after_forced_sec=45.0,
            max_blended_pair_avg_sum=1.04,
        )
    elif k == 8:
        ideas.append("Axis I: floor-only grid (cheapens patience for pair cap)")
        for fl in [0.88, 0.89, 0.90, 0.91]:
            add(f"floor{fl:.2f}", pair_sum_min_floor=fl)
    else:
        ideas.append("Axis J: random-walk mix from baseline (explore cross-terms)")
        add("rw1", dynamic_clip_max=_snap(base.dynamic_clip_max + 1, clips), pair_sum_tighten_per_fill=_snap(base.pair_sum_tighten_per_fill + 0.0005, tightens))
        add("rw2", min_elapsed_for_flat_open=18, pending_hedge_bypass_imbalance_shares=11.0)
        add("rw3", second_leg_book_improve_eps=0.012, max_blended_pair_avg_sum=1.04, dynamic_clip_max=8.0)

    # Dedupe candidate param tuples
    seen: set[tuple[Any, ...]] = set()
    uniq: list[StrategySpec] = []
    for cnd in candidates:
        sig = (
            cnd.dynamic_clip_max,
            cnd.pair_sum_tighten_per_fill,
            cnd.pair_sum_min_floor,
            cnd.pending_hedge_bypass_imbalance_shares,
            cnd.discipline_relax_after_forced_sec,
            cnd.second_leg_book_improve_eps,
            cnd.max_blended_pair_avg_sum,
            cnd.min_elapsed_for_flat_open,
            cnd.max_elapsed_to_start_flat,
        )
        if sig in seen:
            continue
        seen.add(sig)
        uniq.append(cnd)

    return " | ".join(ideas), uniq


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
