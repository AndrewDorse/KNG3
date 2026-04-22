#!/usr/bin/env python3
"""
PALADIN v5 (sim): opening / hedge / add rules per wallet-style spec.

1) Flat: buy winning side (higher mid) if winner mid >= first_leg_min_winner_px — no cooldown.
2) Second leg: other side mid <= 1 - avg_first_leg_fill - margin; after hedge_force_seconds allow
   pm_up+pm_down <= pair_sum_max_on_forced_hedge.
3) Further first legs (balanced, no pending): winner mid dropped >= winner_drop_eps from max over last W seconds.
4) Second leg same as (2).
5) Improvement: both legs have size; buy on a side when mid < that leg's avg (and clip fits cap).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from simulate_paladin_window import SimState, can_afford, improves_leg, try_buy

Side = Literal["up", "down"]


@dataclass(slots=True)
class PaladinV5Params:
    budget_usdc: float = 80.0
    clip_shares: float = 5.0
    max_shares_per_side: float = 10.0
    first_leg_min_winner_px: float = 0.5
    # If set: do not buy winner above this (avoid paying 0.70+ on first leg).
    first_leg_max_winner_px: float | None = None
    second_leg_margin: float = 0.03
    hedge_force_after_seconds: float = 90.0
    pair_sum_max_on_forced_hedge: float = 1.15
    # If set: forced second leg also requires pm_u+pm_d <= this (tighter than pair_sum_max_on_forced_hedge).
    forced_hedge_max_book_sum: float | None = None
    winner_drop_eps: float = 0.05
    winner_drop_window_seconds: int = 7
    improvement_buy_enabled: bool = True
    min_notional: float = 1.0
    # Max completed stagger pairs (each pair = first+second leg). 0 = unlimited (inventory cap only).
    max_pair_cycles: int = 0
    # If set: flat first leg only when book sum <= this.
    flat_entry_max_pair_sum: float | None = None
    # If set: additional first legs (already balanced with inventory) require book sum <= this.
    additional_pair_max_pair_sum: float | None = None


@dataclass(slots=True)
class PaladinV5Runner:
    st: SimState = field(default_factory=SimState)
    # (side to buy, shares, avg_px of opening leg, elapsed when opening leg filled)
    pending_second: tuple[Side, float, float, int] | None = None
    pm_history: list[tuple[float, float]] = field(default_factory=list)
    pair_cycles_completed: int = 0


def _winner_side(pm_u: float, pm_d: float, eps: float = 1e-9) -> Side | None:
    if pm_u > pm_d + eps:
        return "up"
    if pm_d > pm_u + eps:
        return "down"
    return None


def _winner_high_last_w(
    hist: list[tuple[float, float]], window: int, win: Side
) -> float | None:
    if window <= 0 or not hist:
        return None
    chunk = hist[-window:] if len(hist) >= window else hist
    if not chunk:
        return None
    if win == "up":
        return max(u for u, _ in chunk)
    return max(d for _, d in chunk)


def _clamp_clip_for_side(
    st: SimState, side: Side, sh: float, cap: float, min_clip: float
) -> float:
    cur = st.size_up if side == "up" else st.size_down
    room = float(cap) - cur
    if room < min_clip - 1e-9:
        return 0.0
    return min(float(sh), room)


def paladin_v5_step(
    runner: PaladinV5Runner,
    t: int,
    pm_u: float,
    pm_d: float,
    *,
    params: PaladinV5Params,
) -> None:
    st = runner.st
    p = params
    runner.pm_history.append((float(pm_u), float(pm_d)))
    pm_u, pm_d = float(pm_u), float(pm_d)
    min_clip = float(p.clip_shares)
    buy = try_buy

    # --- Pending second leg ---
    if runner.pending_second is not None:
        side_w, sh_w, avg_first, t0 = runner.pending_second
        px = pm_u if side_w == "up" else pm_d
        forced = (float(t) - float(t0)) + 1e-9 >= float(p.hedge_force_after_seconds)
        ok_normal = px + 1e-9 <= (1.0 - float(avg_first) - float(p.second_leg_margin))
        cap_forced = float(p.pair_sum_max_on_forced_hedge)
        if p.forced_hedge_max_book_sum is not None:
            cap_forced = min(cap_forced, float(p.forced_hedge_max_book_sum))
        ok_forced = forced and (pm_u + pm_d) + 1e-9 <= cap_forced
        if ok_normal or ok_forced:
            sh_exec = _clamp_clip_for_side(st, side_w, sh_w, p.max_shares_per_side, min_clip)
            if sh_exec >= min_clip - 1e-9 and _min_notional_ok(
                px, sh_exec, p.min_notional, min_clip
            ):
                reason = "v5_second_forced" if forced and not ok_normal else "v5_second"
                if (
                    buy(
                        st,
                        t=t,
                        side=side_w,
                        shares=sh_exec,
                        px=px,
                        reason=reason,
                        budget=p.budget_usdc,
                        min_notional=p.min_notional,
                        min_shares=min_clip,
                    )
                    > 0
                ):
                    runner.pending_second = None
                    runner.pair_cycles_completed += 1
        return

    win = _winner_side(pm_u, pm_d)
    if win is None:
        return

    px_win = pm_u if win == "up" else pm_d
    balanced = abs(st.size_up - st.size_down) <= 1e-9
    flat = st.size_up <= 1e-9 and st.size_down <= 1e-9
    both_legs = st.size_up > 1e-9 and st.size_down > 1e-9

    # --- Symmetric improvement: both mids below leg avgs (keeps size_up == size_down) ---
    if p.improvement_buy_enabled and both_legs and balanced:
        sh_u = _clamp_clip_for_side(st, "up", p.clip_shares, p.max_shares_per_side, min_clip)
        sh_d = _clamp_clip_for_side(st, "down", p.clip_shares, p.max_shares_per_side, min_clip)
        if (
            sh_u >= min_clip - 1e-9
            and sh_d >= min_clip - 1e-9
            and pm_u + 1e-9 < st.avg_up
            and pm_d + 1e-9 < st.avg_down
            and improves_leg(st.size_up, st.avg_up, pm_u, sh_u)
            and improves_leg(st.size_down, st.avg_down, pm_d, sh_d)
        ):
            if (
                buy(
                    st,
                    t=t,
                    side="up",
                    shares=sh_u,
                    px=pm_u,
                    reason="v5_improve_up",
                    budget=p.budget_usdc,
                    min_notional=p.min_notional,
                    min_shares=min_clip,
                )
                > 0
            ):
                sh_d2 = _clamp_clip_for_side(
                    st, "down", p.clip_shares, p.max_shares_per_side, min_clip
                )
                if sh_d2 >= min_clip - 1e-9 and improves_leg(
                    st.size_down, st.avg_down, pm_d, sh_d2
                ):
                    buy(
                        st,
                        t=t,
                        side="down",
                        shares=sh_d2,
                        px=pm_d,
                        reason="v5_improve_dn",
                        budget=p.budget_usdc,
                        min_notional=p.min_notional,
                        min_shares=min_clip,
                    )
            return

    # --- First leg: flat OR (balanced both legs, new pair) ---
    can_open_new_pair = flat or (balanced and both_legs)
    if not can_open_new_pair:
        return

    if int(p.max_pair_cycles) > 0 and runner.pair_cycles_completed >= int(p.max_pair_cycles):
        return

    if flat and p.flat_entry_max_pair_sum is not None:
        if pm_u + pm_d > float(p.flat_entry_max_pair_sum) + 1e-9:
            return
    if (
        not flat
        and balanced
        and both_legs
        and p.additional_pair_max_pair_sum is not None
    ):
        if pm_u + pm_d > float(p.additional_pair_max_pair_sum) + 1e-9:
            return

    if px_win + 1e-9 < float(p.first_leg_min_winner_px):
        return
    if p.first_leg_max_winner_px is not None and px_win > float(p.first_leg_max_winner_px) + 1e-9:
        return

    if not flat:
        hi = _winner_high_last_w(runner.pm_history, int(p.winner_drop_window_seconds), win)
        if hi is None:
            return
        if px_win + 1e-9 > hi - float(p.winner_drop_eps):
            return

    sh_f = _clamp_clip_for_side(st, win, p.clip_shares, p.max_shares_per_side, min_clip)
    if sh_f < min_clip - 1e-9:
        return
    if not can_afford(st.spent_usdc, sh_f * px_win, p.budget_usdc):
        return

    if buy(
        st,
        t=t,
        side=win,
        shares=sh_f,
        px=px_win,
        reason="v5_first",
        budget=p.budget_usdc,
        min_notional=p.min_notional,
        min_shares=min_clip,
    ) > 0:
        other: Side = "down" if win == "up" else "up"
        runner.pending_second = (other, sh_f, px_win, int(t))


def _min_notional_ok(px: float, sh: float, min_notional: float, min_clip: float) -> bool:
    if sh < min_clip - 1e-9:
        return False
    return sh * px >= min_notional - 1e-9


def settled_pnl_for_winner(metrics: dict[str, float], winner: str) -> float:
    """Settled PnL using an explicit winner label ('UP'/'DOWN'/other → tie split)."""
    w = (winner or "").strip().upper()
    pu = float(metrics["pnl_if_up_usdc"])
    pd = float(metrics["pnl_if_down_usdc"])
    if w == "UP":
        return pu
    if w == "DOWN":
        return pd
    return 0.5 * (pu + pd)


def run_window_v5(
    prices: list[tuple[float, float]],
    *,
    params: PaladinV5Params | None = None,
) -> SimState:
    p = params or PaladinV5Params()
    runner = PaladinV5Runner()
    for t, (pm_u, pm_d) in enumerate(prices):
        paladin_v5_step(runner, t, pm_u, pm_d, params=p)
    return runner.st


# Tuned on 100 most recent BTC 15m windows (exports, min coverage 800s): total settled PnL > 0 vs last-mid proxy.
PROFITABLE_100_PRESET = PaladinV5Params(
    budget_usdc=80.0,
    clip_shares=5.0,
    max_shares_per_side=10.0,
    first_leg_min_winner_px=0.5,
    first_leg_max_winner_px=0.56,
    second_leg_margin=0.05,
    hedge_force_after_seconds=100.0,
    pair_sum_max_on_forced_hedge=1.15,
    forced_hedge_max_book_sum=1.01,
    winner_drop_eps=0.05,
    winner_drop_window_seconds=8,
    improvement_buy_enabled=False,
    min_notional=1.0,
    max_pair_cycles=0,
    flat_entry_max_pair_sum=0.99,
    additional_pair_max_pair_sum=0.985,
)


# Re-export types for sweep harness
__all__ = [
    "PaladinV5Params",
    "PaladinV5Runner",
    "PROFITABLE_100_PRESET",
    "paladin_v5_step",
    "run_window_v5",
    "settled_pnl_for_winner",
]
