#!/usr/bin/env python3
"""
Pattern-based signal analyzer -- LIVE ORDER PLACEMENT.

Runs as a background thread alongside the main bot engine.
Monitors live UP/DOWN prices, detects backtested patterns,
places 5-share buy orders on signals, and sets TP sell at 0.99
after each fill to free cash.

Set BOT_STRATEGY_MODE=signal_only to let this module handle all orders
while the engine still polls prices, heartbeats, and detects fills.

46 active patterns.
v2(4) + v3(7) + v4(8) + v5(4) + v6(6) + v7(17).

All prob/EV values below are ACTUAL TESTED (not claimed).
EV = average net profit per fire (5 shares).
Win$ = avg profit on correct pick.  Loss$ = avg loss on wrong pick.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

SIGLOG = logging.getLogger("polymarket_btc_ladder")

CLIP = 5
TP_PRICE = 0.99
SIGNAL_BUY_PRICE_PAD = 0.03
MAX_SIGNAL_TRIGGER_PRICE = 0.90
MAX_ORDERS_PER_WINDOW = 8
BTC_LAYER_PATTERN_COUNT = 10
LEFTOVER_CLEANUP_PRICE = 0.98
LEFTOVER_CLEANUP_START_ELAPSED = 600.0
LEFTOVER_CLEANUP_INTERVAL_SECONDS = 5.0
SIGNAL_WINDOW_CLOSE_PRICE = 0.98

# Best blended set from BTC overlay search.
# If BTC data is available, these classic signals require the listed BTC confirmation
# filters before firing. If BTC is unavailable, the classic signal still fires.
CLASSIC_BTC_CONFIRMATION_FILTERS: dict[str, tuple[str, ...]] = {
    "vshape_t600_lb240_b0.12": ("range_le_120_0.0016",),
    "rdiv_t600_w180_r0.08_f0.01": ("range_le_120_0.0016",),
    "vshape_t600_lb240_b0.08": ("range_le_120_0.0016",),
    "diverge_t345_w60_r005": ("range_le_30_0.0004",),
    "reversal_300_to_600": ("range_le_120_0.0012",),
    "dom_t720_lead30": ("range_le_45_0.0012",),
    "spread_t720_ge06": ("range_le_45_0.0012",),
    "spread_squeeze_t720_drop20": ("range_le_45_0.0012",),
    "crossover_t585_k45": ("moveabs_ge_90_0.0002",),
    "crossover_t585_k60": ("moveabs_ge_90_0.0002",),
    "crossover_t600_k60": ("range_le_90_0.0016",),
    "ratio_t720_ge4": ("range_le_45_0.0012",),
    "rddrecov_t360_dd0.2_r0.75": ("range_le_30_0.001",),
    "accum_t615_b20_n3": ("range_le_75_0.0016", "rebound_ge_120_0.0004"),
    "rddrecov_t360_dd0.15_r0.75": ("range_le_30_0.001",),
    "low_vol_t720_flip2": ("range_le_45_0.0008",),
    "loserfloor_t495": ("range_le_15_0.0012", "range_le_30_0.0012"),
    "lbounce_t240_r60_f15_rm005_fm006": ("range_le_30_0.0008", "rebound_ge_180_0.0004"),
    "crossover_t600_k30": ("range_le_60_0.0008",),
    "flipband_t720_0to1": ("range_le_45_0.0008",),
    "velocity_t720_w60": ("range_le_90_0.0025",),
    "low_vol_t600_flip2": ("range_le_45_0.0005",),
    "vel_t693_w60_v004": ("range_le_60_0.0016",),
    "vel_t645_w90_v003": ("range_le_60_0.0012", "rebound_ge_120_0.0004"),
    "mix_loserdrop_t690_w30_v0.002_br60_0.0008": ("rebound_ge_120_0.0004",),
    "loserdrop_t585_w45_v002": ("range_le_60_0.0016",),
    "lbounce_t585_r30_f30_rm003_fm006": ("moveabs_ge_90_0.0002",),
}

# Pattern-specific risk blockers derived from late-reversal stress tests.
# If a blocker condition is met, the signal is skipped. BTC-based blockers are
# ignored when BTC data is unavailable so classic fallback behavior still works.
PATTERN_ENTRY_RISK_BLOCKERS: dict[str, tuple[str, ...]] = {
    "dom_t720_lead30": ("elapsed_ge_720", "btc_move_abs_lt_90_0.0002"),
    "btcsqz_t720_lb45_r0.0012_l0.3": ("elapsed_ge_720", "btc_move_abs_lt_90_0.0002"),
    "btcsqz_t720_lb75_r0.0016_l0.2": ("elapsed_ge_720", "btc_move_abs_lt_90_0.0002"),
    "vel_t315_w30_v004": ("flips_ge_5",),
    "vshape_t600_lb240_b0.12": ("elapsed_ge_600",),
    "vshape_t600_lb240_b0.08": ("ratio_ge_5.0", "elapsed_ge_600"),
    "vshape_t510_lb120_b0.08_c0.85": ("loser_lt_0.15",),
    "vshape_t600_lb240_b0.12_btcm240dn0002": ("btc_range_ge_90_0.0008",),
    "vshape_t585_lb240_b0.15_c0.95": ("btc_range_ge_90_0.0008",),
    "mix_vshape_t585_lb240_b0.12_br120_0.0016": ("btc_range_ge_90_0.0008",),
    "mix_loserdrop_t750_w20_v0.0015_br60_0.0005": ("elapsed_ge_750",),
    "spread_squeeze_t720_drop20": ("btc_range_ge_45_0.0008", "btc_range_ge_60_0.001",),
    "twapgap_t585_lb300_g005": ("btc_range_ge_120_0.0016",),
    "btcrev_t585_lb180_r0.0005": ("btc_range_ge_120_0.0016",),
    "btcsqz_t690_lb30_r0.0006_l0.12": ("price_ge_0.75",),
    "crossover_t600_k60": ("elapsed_ge_600",),
    "crossover_t600_k30": ("elapsed_ge_600",),
    "rddrecov_t360_dd0.15_r0.75": ("elapsed_ge_360",),
    "rddrecov_t360_dd0.2_r0.75": ("elapsed_ge_360",),
    "ddrecov_t615_dd01_r075": ("price_ge_0.75",),
    "nearpeak_t645_g001": ("btc_range_ge_60_0.0006",),
    "loserdrop_t840_w60_v0.0015": ("price_ge_0.8",),
    "ratio_t720_ge4": ("elapsed_ge_720",),
    "spread_t720_ge06": ("elapsed_ge_720",),
    "low_vol_t600_flip2": ("btc_range_ge_60_0.0008",),
    "low_vol_t720_flip2": ("btc_range_ge_30_0.0004",),
    "btcbreak_t600_sq30_mv45_r0.0006_m0.0004": ("elapsed_ge_600",),
    "vel_t693_w60_v004": ("loser_lt_0.12",),
    "diverge_t345_w60_r005": ("ratio_ge_5.0",),
    "retrace_t585_r085": ("loser_lt_0.1",),
}


@dataclass
class _PriceSnap:
    ts: float
    elapsed: float
    up: float
    down: float


class SignalAnalyzer:
    """Observes engine state, fires live buy orders + TP sells on pattern signals."""

    def __init__(self) -> None:
        self._engine = None
        self._trader = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()

        self._window_slug: str | None = None
        self._window_start_ts: float = 0.0
        self._history: list[_PriceSnap] = []
        self._dom_flips: int = 0
        self._flip_times: list[float] = []
        self._last_dom: str | None = None
        self._signals_fired: set[str] = set()
        self._loser_at_60: float | None = None
        self._dom_at_60: str | None = None
        self._last_order_ts: float = 0.0
        self._pending_tp: list[tuple[str, int]] = []
        self._orders_placed: int = 0
        self._last_leftover_cleanup_ts: float = 0.0
        self._signal_window_closed: bool = False

    def attach(self, engine) -> None:
        self._engine = engine
        self._trader = engine.trader
        self._live = engine.config.strategy_mode == "signal_only"
        self._thread = threading.Thread(target=self._run, daemon=True, name="signal_analyzer")
        self._thread.start()
        mode_label = "LIVE -- orders enabled" if self._live else "log-only"
        SIGLOG.info("[SIGNAL] analyzer thread started (%s) | 46 patterns active", mode_label)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:
                SIGLOG.exception("[SIGNAL] tick error")
            time.sleep(1.0)

    def _tick(self) -> None:
        eng = self._engine
        if eng is None:
            return
        slug = eng._current_window_slug
        if slug is None:
            return

        if slug != self._window_slug:
            self._reset_window(slug, eng._window_start_ts)

        up = eng._last_up_price
        down = eng._last_down_price
        if up is None or down is None or up <= 0 or down <= 0:
            return

        now = time.time()
        elapsed = now - self._window_start_ts
        if elapsed < 0:
            return

        snap = _PriceSnap(ts=now, elapsed=elapsed, up=up, down=down)
        self._history.append(snap)

        cur_dom = "Up" if up >= down else "Down"
        if self._last_dom is not None and cur_dom != self._last_dom:
            self._dom_flips += 1
            self._flip_times.append(elapsed)
        self._last_dom = cur_dom

        if self._loser_at_60 is None and elapsed >= 60:
            self._loser_at_60 = min(up, down)
            self._dom_at_60 = cur_dom

        if self._live:
            self._check_pending_tp()
            self._cleanup_small_leftovers(elapsed, now)
        if max(up, down) >= SIGNAL_WINDOW_CLOSE_PRICE:
            self._signal_window_closed = True
        self._eval_patterns(snap, elapsed, cur_dom)

    def _reset_window(self, slug: str, start_ts: float) -> None:
        self._window_slug = slug
        self._window_start_ts = start_ts
        self._history.clear()
        self._dom_flips = 0
        self._flip_times.clear()
        self._last_dom = None
        self._signals_fired.clear()
        self._loser_at_60 = None
        self._dom_at_60 = None
        self._pending_tp.clear()
        self._orders_placed = 0
        self._last_leftover_cleanup_ts = 0.0
        self._signal_window_closed = False
        SIGLOG.info("[SIGNAL] new window %s", slug)

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------
    def _get_token(self, side: str):
        eng = self._engine
        if eng is None or eng._last_contract is None:
            return None
        return eng._last_contract.up if side == "Up" else eng._last_contract.down

    def _place_buy(self, name: str, side: str, price: float, prob: str, ev: str, extra: str = "") -> bool:
        if self._orders_placed >= MAX_ORDERS_PER_WINDOW:
            SIGLOG.info(
                "[SIGNAL] BUY SKIPPED %s | max orders reached (%d) | window=%s",
                name, MAX_ORDERS_PER_WINDOW, self._window_slug,
            )
            return False
        if price > MAX_SIGNAL_TRIGGER_PRICE:
            SIGLOG.info(
                "[SIGNAL] BUY SKIPPED %s | %s current price above %.2f (%.2f) | window=%s",
                name, side, MAX_SIGNAL_TRIGGER_PRICE, price, self._window_slug,
            )
            return False
        token = self._get_token(side)
        if token is None:
            SIGLOG.warning("[SIGNAL] no token for %s -- skipping %s", side, name)
            return False
        limit = round(min(price + SIGNAL_BUY_PRICE_PAD, MAX_SIGNAL_TRIGGER_PRICE), 2)
        notional = limit * CLIP
        try:
            resp = self._trader.place_limit_buy(token, limit, CLIP)
            order_id = resp.get("orderID") or resp.get("id") or "?"
            self._last_order_ts = time.time()
            self._orders_placed += 1
            SIGLOG.info(
                "[SIGNAL] *** BUY PLACED %s | %s @ %.2f x%d ($%.2f) | prob=%s ev(5sh)=%s %s| order=%s | orders=%d | window=%s",
                name, side, limit, CLIP, notional, prob, ev,
                f"| {extra} " if extra else "",
                order_id, self._orders_placed, self._window_slug,
            )
            self._pending_tp.append((side, CLIP))
            return True
        except Exception as exc:
            SIGLOG.error("[SIGNAL] BUY FAILED %s | %s @ %.2f | %s", name, side, limit, exc)
            return False

    def _check_pending_tp(self) -> None:
        if not self._pending_tp:
            return
        remaining: list[tuple[str, int]] = []
        for side, shares in self._pending_tp:
            token = self._get_token(side)
            if token is None:
                remaining.append((side, shares))
                continue
            try:
                resp = self._trader.place_limit_sell(token, TP_PRICE, shares)
                order_id = resp.get("orderID") or resp.get("id") or "?"
                SIGLOG.info(
                    "[SIGNAL] TP SELL placed %s @ %.2f x%d | order=%s | window=%s",
                    side, TP_PRICE, shares, order_id, self._window_slug,
                )
            except Exception as exc:
                SIGLOG.debug("[SIGNAL] TP SELL failed %s x%d: %s -- will retry", side, shares, exc)
                remaining.append((side, shares))
        self._pending_tp = remaining

    def _cleanup_small_leftovers(self, elapsed: float, now: float) -> None:
        if elapsed < LEFTOVER_CLEANUP_START_ELAPSED:
            return
        if now - self._last_leftover_cleanup_ts < LEFTOVER_CLEANUP_INTERVAL_SECONDS:
            return
        self._last_leftover_cleanup_ts = now

        for side in ("Up", "Down"):
            token = self._get_token(side)
            if token is None:
                continue
            current_price = self._engine._last_up_price if side == "Up" else self._engine._last_down_price
            if current_price is None or current_price < LEFTOVER_CLEANUP_PRICE:
                continue
            balance = self._trader.token_balance(token.token_id)
            if balance <= 0.0 or balance >= 5.0:
                continue
            try:
                resp = self._trader.place_marketable_sell(token, LEFTOVER_CLEANUP_PRICE, round(balance, 4))
                order_id = resp.get("orderID") or resp.get("id") or "?"
                SIGLOG.info(
                    "[SIGNAL] LEFTOVER CLEANUP SELL %s @ %.2f x%.4f | order=%s | window=%s",
                    side, LEFTOVER_CLEANUP_PRICE, balance, order_id, self._window_slug,
                )
            except Exception as exc:
                SIGLOG.debug(
                    "[SIGNAL] LEFTOVER CLEANUP failed %s x%.4f @ %.2f: %s",
                    side, balance, LEFTOVER_CLEANUP_PRICE, exc,
                )

    # ------------------------------------------------------------------
    # Signal fire (with live order)
    # ------------------------------------------------------------------
    def _fire(self, name: str, side: str, price: float, prob: str, ev: str, extra: str = "") -> None:
        if self._signal_window_closed:
            return
        if not self._btc_overlay_allows(name, side):
            return
        if self._entry_risk_blocked(name, side, price):
            return
        if name in self._signals_fired:
            return
        self._signals_fired.add(name)
        SIGLOG.info(
            "[SIGNAL] DETECTED %s | side=%s price=%.2f | prob=%s ev(5sh)=%s %s| window=%s",
            name, side, price, prob, ev,
            f"| {extra} " if extra else "",
            self._window_slug,
        )
        if self._live:
            self._place_buy(name, side, price, prob, ev, extra)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _dom_side(self, snap: _PriceSnap) -> str:
        return "Up" if snap.up >= snap.down else "Down"

    def _dom_price(self, snap: _PriceSnap) -> float:
        return max(snap.up, snap.down)

    def _loser_price(self, snap: _PriceSnap) -> float:
        return min(snap.up, snap.down)

    def _snap_near(self, target_elapsed: float, tolerance: float = 30.0) -> _PriceSnap | None:
        best: _PriceSnap | None = None
        best_dist = 999.0
        for s in self._history:
            d = abs(s.elapsed - target_elapsed)
            if d < best_dist:
                best_dist = d
                best = s
        if best is None or best_dist > tolerance:
            return None
        return best

    def _dom_at_elapsed(self, target_elapsed: float) -> str | None:
        s = self._snap_near(target_elapsed)
        return self._dom_side(s) if s is not None else None

    def _flips_before(self, elapsed: float) -> int:
        return sum(1 for t in self._flip_times if t <= elapsed)

    def _side_price(self, snap: _PriceSnap, side: str) -> float:
        return snap.up if side == "Up" else snap.down

    def _btc_ready(self) -> bool:
        eng = self._engine
        return eng is not None and getattr(eng, "_last_btc_price", None) is not None

    def _btc_price_near(self, target_elapsed: float, tolerance: float = 15.0) -> float | None:
        eng = self._engine
        if eng is None:
            return None
        history = getattr(eng, "_btc_price_history", None)
        if not history:
            return None
        target_ts = self._window_start_ts + target_elapsed
        best = None
        best_dist = 999.0
        for point in history:
            dist = abs(point.ts - target_ts)
            if dist < best_dist:
                best_dist = dist
                best = point
        if best is None or best_dist > tolerance:
            return None
        return float(best.price)

    def _btc_move(self, end_elapsed: float, lookback_seconds: float) -> float | None:
        end_elapsed = max(0.0, end_elapsed)
        now_px = self._btc_price_near(end_elapsed)
        old_px = self._btc_price_near(max(0.0, end_elapsed - lookback_seconds))
        if now_px is None or old_px is None or old_px <= 0:
            return None
        return (now_px - old_px) / old_px

    def _btc_range(self, end_elapsed: float, lookback_seconds: float, step_seconds: float = 5.0) -> float | None:
        start_elapsed = max(0.0, end_elapsed - lookback_seconds)
        prices: list[float] = []
        probe = start_elapsed
        while probe <= end_elapsed:
            px = self._btc_price_near(probe)
            if px is not None:
                prices.append(px)
            probe += step_seconds
        if len(prices) < 2:
            return None
        lo = min(prices)
        hi = max(prices)
        if lo <= 0:
            return None
        return (hi - lo) / lo

    def _btc_rebound(self, end_elapsed: float, lookback_seconds: float, side: str) -> float | None:
        start_elapsed = max(0.0, end_elapsed - lookback_seconds)
        prices: list[float] = []
        probe = start_elapsed
        while probe <= end_elapsed:
            px = self._btc_price_near(probe)
            if px is not None:
                prices.append(px)
            probe += 5.0
        if len(prices) < 2:
            return None
        last = prices[-1]
        if side == "Up":
            lo = min(prices)
            if lo <= 0:
                return None
            return (last - lo) / lo
        hi = max(prices)
        if hi <= 0:
            return None
        return (hi - last) / hi

    def _btc_accel(self, end_elapsed: float, lookback_seconds: float) -> float | None:
        if end_elapsed < 2 * lookback_seconds:
            return None
        prev_move = self._btc_move(end_elapsed - lookback_seconds, lookback_seconds)
        cur_move = self._btc_move(end_elapsed, lookback_seconds)
        if prev_move is None or cur_move is None:
            return None
        return abs(cur_move) - abs(prev_move)

    def _btc_filter_passes(self, filter_spec: str, side: str, elapsed: float) -> bool:
        parts = filter_spec.split("_")
        if len(parts) < 3:
            return True
        kind = "_".join(parts[:-2])
        lookback = float(parts[-2])
        threshold = float(parts[-1])
        if kind == "range_le":
            value = self._btc_range(elapsed, lookback)
            return value is not None and value <= threshold
        if kind == "rebound_ge":
            value = self._btc_rebound(elapsed, lookback, side)
            return value is not None and value >= threshold
        if kind == "move_up":
            value = self._btc_move(elapsed, lookback)
            return value is not None and value >= threshold
        if kind == "move_dn":
            value = self._btc_move(elapsed, lookback)
            return value is not None and value <= -threshold
        if kind == "moveabs_ge":
            value = self._btc_move(elapsed, lookback)
            return value is not None and abs(value) >= threshold
        if kind == "accel_ge":
            value = self._btc_accel(elapsed, lookback)
            return value is not None and value >= threshold
        return True

    def _btc_overlay_allows(self, name: str, side: str) -> bool:
        filters = CLASSIC_BTC_CONFIRMATION_FILTERS.get(name)
        if not filters:
            return True
        # Fallback behavior: if BTC feed/history is unavailable, keep the classic signal live.
        if not self._btc_ready():
            return True
        elapsed = self._history[-1].elapsed if self._history else 0.0
        return all(self._btc_filter_passes(spec, side, elapsed) for spec in filters)

    def _entry_risk_blocked(self, name: str, side: str, price: float) -> bool:
        specs = PATTERN_ENTRY_RISK_BLOCKERS.get(name)
        if not specs or not self._history:
            return False
        snap = self._history[-1]
        d_px = self._dom_price(snap)
        l_px = self._loser_price(snap)
        ratio = d_px / l_px if l_px > 0.01 else 999.0
        elapsed = snap.elapsed
        for spec in specs:
            parts = spec.split("_")
            if len(parts) == 3 and parts[0] == "elapsed" and parts[1] == "ge":
                if elapsed >= float(parts[2]):
                    return True
                continue
            if len(parts) == 3 and parts[0] == "price" and parts[1] == "ge":
                if price >= float(parts[2]):
                    return True
                continue
            if len(parts) == 3 and parts[0] == "ratio" and parts[1] == "ge":
                if ratio >= float(parts[2]):
                    return True
                continue
            if len(parts) == 3 and parts[0] == "flips" and parts[1] == "ge":
                if self._dom_flips >= int(float(parts[2])):
                    return True
                continue
            if len(parts) == 3 and parts[0] == "loser" and parts[1] == "lt":
                if l_px < float(parts[2]):
                    return True
                continue
            if len(parts) == 5 and parts[0] == "btc" and parts[1] == "range" and parts[2] == "ge":
                btc_range = self._btc_range(elapsed, float(parts[3]))
                if btc_range is not None and btc_range >= float(parts[4]):
                    return True
                continue
            if len(parts) == 6 and parts[0] == "btc" and parts[1] == "move" and parts[2] == "abs" and parts[3] == "lt":
                btc_move = self._btc_move(elapsed, float(parts[4]))
                if btc_move is not None and abs(btc_move) < float(parts[5]):
                    return True
                continue
        return False

    # ------------------------------------------------------------------
    # Pattern evaluators  (44 active -- tested on 178 windows)
    # ------------------------------------------------------------------
    # REMOVED PATTERNS (kept for reference, no longer active):
    #   - consistent_dom_300_720    (v2, removed: WR degraded 93%->87%)
    #   - flipband_t540_0to1        (v4, removed: PnL<$10 & WR<100%)
    #   - squeeze_t135_d02          (v4, removed: PnL<$10 & WR<100%)
    #   - flipband_t465_0to0        (v4, removed: PnL<$10 & WR<100%)
    #   - late_cert_t780_dp95       (v2, removed: low EV +$0.09)
    #   - compound_t720_f3_s03      (v3, removed: low EV +$0.20)
    # ------------------------------------------------------------------
    def _eval_patterns(self, snap: _PriceSnap, elapsed: float, dom: str) -> None:
        d_px = self._dom_price(snap)
        l_px = self._loser_price(snap)
        lead = d_px - l_px
        spread = lead

        # ==============================================================
        # v2 PATTERNS (1-6)
        # ==============================================================

        # 1. low_vol_t600_flip2  (96% | n=47 | win +$0.60 | loss -$3.58 | ev +$0.42)
        if 598 <= elapsed <= 605 and self._dom_flips <= 2:
            self._fire("low_vol_t600_flip2", dom, d_px, "96%", "+$0.42",
                       f"flips={self._dom_flips}")

        # 2. low_vol_t720_flip2  (100% | n=46 | win +$0.45 | loss $0 | ev +$0.45)
        if 718 <= elapsed <= 725 and self._dom_flips <= 2:
            self._fire("low_vol_t720_flip2", dom, d_px, "100%", "+$0.45",
                       f"flips={self._dom_flips}")

        # 3. spread_squeeze_t720_drop20  (98% | n=103 | win +$0.30 | loss -$4.68 | ev +$0.20)
        if 718 <= elapsed <= 730 and self._loser_at_60 is not None:
            drop = self._loser_at_60 - l_px
            if drop >= 0.20:
                self._fire("spread_squeeze_t720_drop20", dom, d_px, "98%", "+$0.20",
                           f"loser_drop={drop:.3f}")

        # 4. dom_t720_lead30  (96% | n=121 | win +$0.42 | loss -$4.17 | ev +$0.23)
        if 718 <= elapsed <= 725 and lead >= 0.30:
            self._fire("dom_t720_lead30", dom, d_px, "96%", "+$0.23",
                       f"lead={lead:.3f}")

        # ==============================================================
        # v3 PATTERNS (6-14)
        # ==============================================================

        # 6. crossover_t600_k60  (93% | n=14 | win +$2.07 | loss -$2.70 | ev +$1.73)
        if 598 <= elapsed <= 605:
            dom_540 = self._dom_at_elapsed(540)
            if dom_540 is not None and dom_540 != dom:
                self._fire("crossover_t600_k60", dom, d_px, "93%", "+$1.73",
                           f"dom_540={dom_540} dom_600={dom}")

        # 7. crossover_t600_k30  (89% | n=9 | win +$2.04 | loss -$2.70 | ev +$1.51)
        if 598 <= elapsed <= 605:
            dom_570 = self._dom_at_elapsed(570)
            if dom_570 is not None and dom_570 != dom:
                self._fire("crossover_t600_k30", dom, d_px, "89%", "+$1.51",
                           f"dom_570={dom_570} dom_600={dom}")

        # 8. velocity_t720_w60  (92% | n=13 | win +$1.29 | loss -$1.65 | ev +$1.06)
        if 718 <= elapsed <= 725:
            snap_660 = self._snap_near(660)
            if snap_660 is not None:
                vel_up = (snap.up - snap_660.up) / 60.0
                vel_dn = (snap.down - snap_660.down) / 60.0
                if vel_up >= 0.003 and vel_up > vel_dn:
                    self._fire("velocity_t720_w60", "Up", snap.up, "92%", "+$1.06",
                               f"vel_up={vel_up:.4f}/s")
                elif vel_dn >= 0.003 and vel_dn > vel_up:
                    self._fire("velocity_t720_w60", "Down", snap.down, "92%", "+$1.06",
                               f"vel_dn={vel_dn:.4f}/s")

        # 9. reversal_300_to_600  (94% | n=32 | win +$1.11 | loss -$4.03 | ev +$0.79)
        if 598 <= elapsed <= 605:
            dom_300 = self._dom_at_elapsed(300)
            if dom_300 is not None and dom_300 != dom:
                self._fire("reversal_300_to_600", dom, d_px, "94%", "+$0.79",
                           f"was={dom_300} now={dom}")

        # 10. flipband_t720_0to1  (100% | n=34 | win +$0.44 | loss $0 | ev +$0.44)
        if 718 <= elapsed <= 725 and self._dom_flips <= 1:
            self._fire("flipband_t720_0to1", dom, d_px, "100%", "+$0.44",
                       f"flips={self._dom_flips}")

        # 11. ratio_t720_ge4  (95% | n=58 | loss -$4.50 | ev +$0.19)
        if 718 <= elapsed <= 725:
            if l_px > 0.01:
                ratio = d_px / l_px
                if ratio >= 4.0:
                    self._fire("ratio_t720_ge4", dom, d_px, "95%", "+$0.19",
                               f"ratio={ratio:.1f}")

        # 12. spread_t720_ge06  (97% | n=104 | win +$0.27 | loss -$4.50 | ev +$0.13)
        if 718 <= elapsed <= 725 and spread >= 0.60:
            self._fire("spread_t720_ge06", dom, d_px, "97%", "+$0.13",
                       f"spread={spread:.3f}")

        # ==============================================================
        # v4 PATTERNS (20-29)
        # ==============================================================

        # 21. crossover_t585_k45  (100% | n=10 | win +$2.12 | loss $0 | ev +$2.12)
        if 583 <= elapsed <= 590:
            dom_540 = self._dom_at_elapsed(540)
            if dom_540 is not None and dom_540 != dom:
                self._fire("crossover_t585_k45", dom, d_px, "100%", "+$2.12",
                           f"dom_540={dom_540} now={dom}")

        # 22. crossover_t585_k60  (100% | n=12 | win +$1.97 | loss $0 | ev +$1.97)
        if 583 <= elapsed <= 590:
            dom_525 = self._dom_at_elapsed(525)
            if dom_525 is not None and dom_525 != dom:
                self._fire("crossover_t585_k60", dom, d_px, "100%", "+$1.97",
                           f"dom_525={dom_525} now={dom}")

        # 23. vel_t315_w30_v004  (89% | n=18 | win +$1.87 | loss -$3.12 | ev +$1.32)
        if 313 <= elapsed <= 320:
            s285 = self._snap_near(285)
            if s285 is not None:
                vu = (snap.up - s285.up) / 30.0
                vd = (snap.down - s285.down) / 30.0
                if vu >= 0.004 and vu > vd:
                    self._fire("vel_t315_w30_v004", "Up", snap.up, "89%", "+$1.32",
                               f"vel={vu:.4f}/s")
                elif vd >= 0.004 and vd > vu:
                    self._fire("vel_t315_w30_v004", "Down", snap.down, "89%", "+$1.32",
                               f"vel={vd:.4f}/s")

        # 24. vel_t693_w60_v004  (100% | n=14 | win +$1.28 | loss $0 | ev +$1.28)
        if 691 <= elapsed <= 698:
            s633 = self._snap_near(633)
            if s633 is not None:
                vu = (snap.up - s633.up) / 60.0
                vd = (snap.down - s633.down) / 60.0
                if vu >= 0.004 and vu > vd:
                    self._fire("vel_t693_w60_v004", "Up", snap.up, "100%", "+$1.28",
                               f"vel={vu:.4f}/s")
                elif vd >= 0.004 and vd > vu:
                    self._fire("vel_t693_w60_v004", "Down", snap.down, "100%", "+$1.28",
                               f"vel={vd:.4f}/s")

        # 25. vel_t645_w90_v003  (100% | n=16 | win +$1.07 | loss $0 | ev +$1.07)
        if 643 <= elapsed <= 650:
            s555 = self._snap_near(555)
            if s555 is not None:
                vu = (snap.up - s555.up) / 90.0
                vd = (snap.down - s555.down) / 90.0
                if vu >= 0.003 and vu > vd:
                    self._fire("vel_t645_w90_v003", "Up", snap.up, "100%", "+$1.07",
                               f"vel={vu:.4f}/s")
                elif vd >= 0.003 and vd > vu:
                    self._fire("vel_t645_w90_v003", "Down", snap.down, "100%", "+$1.07",
                               f"vel={vd:.4f}/s")

        # ==============================================================
        # v5 PATTERNS (29-32)
        # ==============================================================

        # 29. loserdrop_t585_w45_v002  (93% | n=43 | win +$1.23 | loss -$3.58 | ev +$0.89)
        if 583 <= elapsed <= 590:
            s540 = self._snap_near(540)
            if s540 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_540 = s540.down if loser_side == "Down" else s540.up
                l_now = snap.down if loser_side == "Down" else snap.up
                drop_vel = (l_540 - l_now) / 45.0
                if drop_vel >= 0.002:
                    self._fire("loserdrop_t585_w45_v002", dom, d_px, "93%", "+$0.89",
                               f"drop_vel={drop_vel:.4f}/s")

        # 30. diverge_t345_w60_r005  (82% | n=77 | win +$1.30 | loss -$3.30 | ev +$0.46)
        if 343 <= elapsed <= 350:
            s285 = self._snap_near(285)
            if s285 is not None:
                d_change = self._side_price(snap, dom) - self._side_price(s285, dom)
                loser_side = "Down" if dom == "Up" else "Up"
                l_change = self._side_price(snap, loser_side) - self._side_price(s285, loser_side)
                if d_change >= 0.05 and l_change <= -0.01:
                    self._fire("diverge_t345_w60_r005", dom, d_px, "82%", "+$0.46",
                               f"dom_rise={d_change:.3f} loser_drop={l_change:.3f}")

        # 31. ddrecov_t615_dd01_r075  (100% | n=10 | win +$1.42 | loss $0 | ev +$1.42)
        if 613 <= elapsed <= 620:
            dom_prices = [self._side_price(s, dom) for s in self._history if s.elapsed <= elapsed]
            if len(dom_prices) >= 30:
                peak = max(dom_prices)
                trough = peak
                peak_hit = False
                for px in dom_prices:
                    if px == peak:
                        peak_hit = True
                    if peak_hit:
                        trough = min(trough, px)
                dd = peak - trough
                if dd >= 0.10:
                    recovery = (d_px - trough) / dd if dd > 0 else 0
                    if recovery >= 0.75:
                        self._fire("ddrecov_t615_dd01_r075", dom, d_px, "100%", "+$1.42",
                                   f"dd={dd:.3f} recov={recovery:.0%}")

        # ==============================================================
        # v6 PATTERNS (33-41)
        # ==============================================================

        # 33. twapgap_t585_lb300_g005  (92% | n=112 | win +$0.77 | loss -$3.60 | ev +$0.42)
        if 583 <= elapsed <= 590:
            dom_prices_300 = [self._side_price(s, dom) for s in self._history if 285 <= s.elapsed <= elapsed]
            if len(dom_prices_300) >= 30:
                twap = sum(dom_prices_300) / len(dom_prices_300)
                gap = d_px - twap
                if gap >= 0.05:
                    self._fire("twapgap_t585_lb300_g005", dom, d_px, "92%", "+$0.42",
                               f"twap={twap:.3f} gap={gap:.3f}")

        # 34. retrace_t585_r085  (95% | n=119 | win +$0.58 | loss -$3.95 | ev +$0.36)
        if 583 <= elapsed <= 590:
            dom_all = [self._side_price(s, dom) for s in self._history if s.elapsed <= elapsed]
            if len(dom_all) >= 30:
                hi, lo = max(dom_all), min(dom_all)
                rng = hi - lo
                if rng >= 0.05:
                    retrace = (d_px - lo) / rng
                    if retrace >= 0.85:
                        self._fire("retrace_t585_r085", dom, d_px, "95%", "+$0.36",
                                   f"hi={hi:.3f} lo={lo:.3f} retrace={retrace:.2f}")

        # 35. lbounce_t585_r30_f45_rm008_fm002  (100% | n=11 | win +$1.59 | loss $0 | ev +$1.59)
        if 583 <= elapsed <= 590:
            s510 = self._snap_near(510)
            s540 = self._snap_near(540)
            if s510 is not None and s540 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_start = self._side_price(s510, loser_side)
                l_peak = self._side_price(s540, loser_side)
                l_now = self._side_price(snap, loser_side)
                rise = l_peak - l_start
                fall = l_peak - l_now
                if rise >= 0.08 and fall >= 0.02:
                    self._fire("lbounce_t585_r30_f45_rm008_fm002", dom, d_px, "100%", "+$1.59",
                               f"rise={rise:.3f} fall={fall:.3f}")

        # 36. lbounce_t240_r60_f15_rm005_fm006  (100% | n=10 | win +$1.63 | loss $0 | ev +$1.64)
        if 238 <= elapsed <= 245:
            s165 = self._snap_near(165)
            s225 = self._snap_near(225)
            if s165 is not None and s225 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_start = self._side_price(s165, loser_side)
                l_peak = self._side_price(s225, loser_side)
                l_now = self._side_price(snap, loser_side)
                rise = l_peak - l_start
                fall = l_peak - l_now
                if rise >= 0.05 and fall >= 0.06:
                    self._fire("lbounce_t240_r60_f15_rm005_fm006", dom, d_px, "100%", "+$1.64",
                               f"rise={rise:.3f} fall={fall:.3f}")

        # 37. accum_t615_b20_n3  (100% | n=30 | win +$0.82 | loss $0 | ev +$0.82)
        if 613 <= elapsed <= 620:
            s555 = self._snap_near(555)
            s575 = self._snap_near(575)
            s595 = self._snap_near(595)
            if s555 is not None and s575 is not None and s595 is not None:
                g1 = self._side_price(s575, dom) - self._side_price(s555, dom)
                g2 = self._side_price(s595, dom) - self._side_price(s575, dom)
                g3 = self._side_price(snap, dom) - self._side_price(s595, dom)
                if g1 > 0 and g2 > 0 and g3 > 0:
                    self._fire("accum_t615_b20_n3", dom, d_px, "100%", "+$0.82",
                               f"g1={g1:.3f} g2={g2:.3f} g3={g3:.3f}")

        # 38. lbounce_t585_r30_f30_rm003_fm006  (100% | n=17 | win +$1.19 | loss $0 | ev +$1.19)
        if 583 <= elapsed <= 590:
            s525 = self._snap_near(525)
            s555 = self._snap_near(555)
            if s525 is not None and s555 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_start = self._side_price(s525, loser_side)
                l_peak = self._side_price(s555, loser_side)
                l_now = self._side_price(snap, loser_side)
                rise = l_peak - l_start
                fall = l_peak - l_now
                if rise >= 0.03 and fall >= 0.06:
                    self._fire("lbounce_t585_r30_f30_rm003_fm006", dom, d_px, "100%", "+$1.19",
                               f"rise={rise:.3f} fall={fall:.3f}")

        # 39. nearpeak_t645_g001  (100% | n=66 | win +$0.30 | loss $0 | ev +$0.30)
        if 643 <= elapsed <= 650:
            dom_all = [self._side_price(s, dom) for s in self._history if s.elapsed <= elapsed]
            if len(dom_all) >= 30:
                peak = max(dom_all)
                if peak >= 0.55 and d_px >= peak - 0.01:
                    self._fire("nearpeak_t645_g001", dom, d_px, "100%", "+$0.30",
                               f"peak={peak:.3f} current={d_px:.3f}")

        # ==============================================================
        # v7 ADDED FROM LATEST SEARCH (42-49)
        # ==============================================================

        # 42. vshape_t600_lb240_b0.12  (84% | n=140 | win +$0.96 | loss -$2.78 | ev +$0.35)
        if 598 <= elapsed <= 605:
            for side in ("Up", "Down"):
                px_min = min(self._side_price(s, side) for s in self._history if 360 <= s.elapsed <= 480)
                px_now = self._side_price(snap, side)
                if px_now - px_min >= 0.12:
                    self._fire("vshape_t600_lb240_b0.12", side, px_now, "84%", "+$0.35",
                               f"v_bounce={px_now - px_min:.3f}")
                    break

        # 43. vshape_t600_lb240_b0.08  (83% | n=148 | win +$0.96 | loss -$2.90 | ev +$0.31)
        if 598 <= elapsed <= 605:
            for side in ("Up", "Down"):
                px_min = min(self._side_price(s, side) for s in self._history if 360 <= s.elapsed <= 480)
                px_now = self._side_price(snap, side)
                if px_now - px_min >= 0.08:
                    self._fire("vshape_t600_lb240_b0.08", side, px_now, "83%", "+$0.31",
                               f"v_bounce={px_now - px_min:.3f}")
                    break

        # 44. rdiv_t600_w180_r0.08_f0.01  (91% | n=98 | win +$0.86 | loss -$3.74 | ev +$0.44)
        if 598 <= elapsed <= 605:
            s420 = self._snap_near(420)
            if s420 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                dchg = self._side_price(snap, dom) - self._side_price(s420, dom)
                lchg = self._side_price(snap, loser_side) - self._side_price(s420, loser_side)
                if dchg >= 0.08 and lchg <= -0.01:
                    self._fire("rdiv_t600_w180_r0.08_f0.01", dom, d_px, "91%", "+$0.44",
                               f"dom_rise={dchg:.3f} loser_drop={lchg:.3f}")

        # 45. rddrecov_t360_dd0.15_r0.75  (100% | n=14 | win +$1.91 | loss $0 | ev +$1.91)
        if 358 <= elapsed <= 365:
            dom_prices = [self._side_price(s, dom) for s in self._history if s.elapsed <= elapsed]
            if len(dom_prices) >= 30:
                peak = max(dom_prices)
                trough = peak
                peak_hit = False
                for px in dom_prices:
                    if px == peak:
                        peak_hit = True
                    if peak_hit:
                        trough = min(trough, px)
                dd = peak - trough
                if dd >= 0.15:
                    recovery = (d_px - trough) / dd if dd > 0 else 0
                    if recovery >= 0.75:
                        self._fire("rddrecov_t360_dd0.15_r0.75", dom, d_px, "100%", "+$1.91",
                                   f"dd={dd:.3f} recov={recovery:.0%}")

        # 46. rddrecov_t360_dd0.2_r0.75  (100% | n=13 | win +$1.92 | loss $0 | ev +$1.92)
        if 358 <= elapsed <= 365:
            dom_prices = [self._side_price(s, dom) for s in self._history if s.elapsed <= elapsed]
            if len(dom_prices) >= 30:
                peak = max(dom_prices)
                trough = peak
                peak_hit = False
                for px in dom_prices:
                    if px == peak:
                        peak_hit = True
                    if peak_hit:
                        trough = min(trough, px)
                dd = peak - trough
                if dd >= 0.20:
                    recovery = (d_px - trough) / dd if dd > 0 else 0
                    if recovery >= 0.75:
                        self._fire("rddrecov_t360_dd0.2_r0.75", dom, d_px, "100%", "+$1.92",
                                   f"dd={dd:.3f} recov={recovery:.0%}")

        # 47. loserfloor_t495  (100% | n=45 | win +$0.46 | loss $0 | ev +$0.46)
        if 493 <= elapsed <= 500:
            loser_side = "Down" if dom == "Up" else "Up"
            loser_now = self._side_price(snap, loser_side)
            loser_min = min(self._side_price(s, loser_side) for s in self._history if s.elapsed <= elapsed)
            if abs(loser_now - loser_min) < 0.005:
                self._fire("loserfloor_t495", dom, d_px, "100%", "+$0.46",
                           f"loser_now={loser_now:.3f} loser_min={loser_min:.3f}")

        # 48. vshape_t330_lb120_b0.08_c0.85
        if 328 <= elapsed <= 335:
            for side in ("Up", "Down"):
                start = 210
                mid = 270
                segment = [self._side_price(s, side) for s in self._history if start <= s.elapsed <= mid]
                if not segment:
                    continue
                px_min = min(segment)
                px_now = self._side_price(snap, side)
                if px_now <= 0.85 and px_now - px_min >= 0.08:
                    self._fire("vshape_t330_lb120_b0.08_c0.85", side, px_now, "69%", "+$0.39",
                               f"v_bounce={px_now - px_min:.3f}")
                    break

        # 49. vshape_t510_lb120_b0.08_c0.85
        if 508 <= elapsed <= 515:
            for side in ("Up", "Down"):
                start = 390
                mid = 450
                segment = [self._side_price(s, side) for s in self._history if start <= s.elapsed <= mid]
                if not segment:
                    continue
                px_min = min(segment)
                px_now = self._side_price(snap, side)
                if px_now <= 0.85 and px_now - px_min >= 0.08:
                    self._fire("vshape_t510_lb120_b0.08_c0.85", side, px_now, "65%", "+$0.55",
                               f"v_bounce={px_now - px_min:.3f}")
                    break

        # 50. vshape_t585_lb240_b0.15_c0.95
        if 583 <= elapsed <= 590:
            for side in ("Up", "Down"):
                start = 345
                mid = 465
                segment = [self._side_price(s, side) for s in self._history if start <= s.elapsed <= mid]
                if not segment:
                    continue
                px_min = min(segment)
                px_now = self._side_price(snap, side)
                if px_now <= 0.95 and px_now - px_min >= 0.15:
                    self._fire("vshape_t585_lb240_b0.15_c0.95", side, px_now, "78%", "+$0.44",
                               f"v_bounce={px_now - px_min:.3f}")
                    break

        # 51. loserdrop_t840_w60_v0.0015
        if 838 <= elapsed <= 845:
            s780 = self._snap_near(780)
            if s780 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_780 = self._side_price(s780, loser_side)
                l_now = self._side_price(snap, loser_side)
                drop_vel = (l_780 - l_now) / 60.0
                if drop_vel >= 0.0015:
                    self._fire("loserdrop_t840_w60_v0.0015", dom, d_px, "100%", "+$0.69",
                               f"drop_vel={drop_vel:.4f}/s")

        # ==============================================================
        # BTC LAYER PATTERNS (52-60) -- optional, require live BTC feed
        # ==============================================================
        if not self._btc_ready():
            return

        # 52. vshape_t600_lb240_b0.12_btcm240dn0002
        # Mixed layer: strong v-shape works better when BTC has been weak over prior 240s.
        if 598 <= elapsed <= 605:
            btc_m240 = self._btc_move(elapsed, 240)
            if btc_m240 is not None and btc_m240 <= -0.000202:
                for side in ("Up", "Down"):
                    px_min = min(self._side_price(s, side) for s in self._history if 360 <= s.elapsed <= 480)
                    px_now = self._side_price(snap, side)
                    if px_now - px_min >= 0.12:
                        self._fire(
                            "vshape_t600_lb240_b0.12_btcm240dn0002",
                            side,
                            px_now,
                            "91%",
                            "+$0.78",
                            f"v_bounce={px_now - px_min:.3f} btc_m240={btc_m240:.4%}",
                        )
                        break

        # 53. btcagree_t525_lb180_m0.001
        if 523 <= elapsed <= 530 and lead >= 0.05:
            btc_m180 = self._btc_move(elapsed, 180)
            if btc_m180 is not None and abs(btc_m180) >= 0.001:
                btc_side = "Up" if btc_m180 > 0 else "Down"
                if btc_side == dom:
                    self._fire(
                        "btcagree_t525_lb180_m0.001",
                        dom,
                        d_px,
                        "100%",
                        "+$0.55",
                        f"lead={lead:.3f} btc_m180={btc_m180:.4%}",
                    )

        # 54. btcagree_t525_lb180_m0.001_l0.05
        if 523 <= elapsed <= 530 and lead >= 0.05:
            btc_m180 = self._btc_move(elapsed, 180)
            if btc_m180 is not None and abs(btc_m180) >= 0.001:
                btc_side = "Up" if btc_m180 > 0 else "Down"
                if btc_side == dom:
                    self._fire(
                        "btcagree_t525_lb180_m0.001_l0.05",
                        dom,
                        d_px,
                        "100%",
                        "+$0.79",
                        f"lead={lead:.3f} btc_m180={btc_m180:.4%}",
                    )

        # 55. btcbreak_t600_sq30_mv45_r0.0006_m0.0004
        if 598 <= elapsed <= 605 and lead >= 0.05:
            btc_rng30 = self._btc_range(elapsed, 30)
            btc_m45 = self._btc_move(elapsed, 45)
            if (
                btc_rng30 is not None
                and btc_m45 is not None
                and btc_rng30 <= 0.0006
                and abs(btc_m45) >= 0.0004
            ):
                btc_side = "Up" if btc_m45 > 0 else "Down"
                if btc_side == dom:
                    self._fire(
                        "btcbreak_t600_sq30_mv45_r0.0006_m0.0004",
                        dom,
                        d_px,
                        "100%",
                        "+$0.66",
                        f"lead={lead:.3f} btc_rng30={btc_rng30:.4%} btc_m45={btc_m45:.4%}",
                    )

        # 56. btcsqz_t690_lb30_r0.0006_l0.12
        if 688 <= elapsed <= 695 and lead >= 0.12:
            btc_rng30 = self._btc_range(elapsed, 30)
            if btc_rng30 is not None and btc_rng30 <= 0.0006:
                self._fire(
                    "btcsqz_t690_lb30_r0.0006_l0.12",
                    dom,
                    d_px,
                    "94%",
                    "+$0.30",
                    f"lead={lead:.3f} btc_rng30={btc_rng30:.4%}",
                )

        # 57. btcrev_t585_lb180_r0.0005
        if 583 <= elapsed <= 590 and lead >= 0.05:
            btc_rebound180 = self._btc_rebound(elapsed, 180, dom)
            if btc_rebound180 is not None and btc_rebound180 >= 0.0005:
                self._fire(
                    "btcrev_t585_lb180_r0.0005",
                    dom,
                    d_px,
                    "92%",
                    "+$0.40",
                    f"lead={lead:.3f} btc_rebound180={btc_rebound180:.4%}",
                )

        # 58. btcsqz_t720_lb45_r0.0012_l0.3
        if 718 <= elapsed <= 725 and lead >= 0.30:
            btc_rng45 = self._btc_range(elapsed, 45)
            if btc_rng45 is not None and btc_rng45 <= 0.0012:
                self._fire(
                    "btcsqz_t720_lb45_r0.0012_l0.3",
                    dom,
                    d_px,
                    "96%",
                    "+$0.25",
                    f"lead={lead:.3f} btc_rng45={btc_rng45:.4%}",
                )

        # 59. btcsqz_t720_lb75_r0.0016_l0.2
        if 718 <= elapsed <= 725 and lead >= 0.20:
            btc_rng75 = self._btc_range(elapsed, 75)
            if btc_rng75 is not None and btc_rng75 <= 0.0016:
                self._fire(
                    "btcsqz_t720_lb75_r0.0016_l0.2",
                    dom,
                    d_px,
                    "94%",
                    "+$0.24",
                    f"lead={lead:.3f} btc_rng75={btc_rng75:.4%}",
                )

        # 60. mix_vshape_t585_lb240_b0.12_br120_0.0016
        if 583 <= elapsed <= 590:
            btc_rng120 = self._btc_range(elapsed, 120)
            if btc_rng120 is not None and btc_rng120 <= 0.0016:
                for side in ("Up", "Down"):
                    segment = [self._side_price(s, side) for s in self._history if 345 <= s.elapsed <= 465]
                    if not segment:
                        continue
                    px_min = min(segment)
                    px_now = self._side_price(snap, side)
                    if px_now - px_min >= 0.12:
                        self._fire(
                            "mix_vshape_t585_lb240_b0.12_br120_0.0016",
                            side,
                            px_now,
                            "79%",
                            "+$0.32",
                            f"v_bounce={px_now - px_min:.3f} btc_rng120={btc_rng120:.4%}",
                        )
                        break

        # 61. mix_loserdrop_t750_w20_v0.0015_br60_0.0005
        if 748 <= elapsed <= 755:
            btc_rng60 = self._btc_range(elapsed, 60)
            s730 = self._snap_near(730)
            if btc_rng60 is not None and btc_rng60 <= 0.0005 and s730 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_730 = self._side_price(s730, loser_side)
                l_now = self._side_price(snap, loser_side)
                drop_vel = (l_730 - l_now) / 20.0
                if drop_vel >= 0.0015:
                    self._fire(
                        "mix_loserdrop_t750_w20_v0.0015_br60_0.0005",
                        dom,
                        d_px,
                        "100%",
                        "+$1.01",
                        f"drop_vel={drop_vel:.4f}/s btc_rng60={btc_rng60:.4%}",
                    )

        # 62. mix_loserdrop_t690_w30_v0.002_br60_0.0008
        if 688 <= elapsed <= 695:
            btc_rng60 = self._btc_range(elapsed, 60)
            s660 = self._snap_near(660)
            if btc_rng60 is not None and btc_rng60 <= 0.0008 and s660 is not None:
                loser_side = "Down" if dom == "Up" else "Up"
                l_660 = self._side_price(s660, loser_side)
                l_now = self._side_price(snap, loser_side)
                drop_vel = (l_660 - l_now) / 30.0
                if drop_vel >= 0.002:
                    self._fire(
                        "mix_loserdrop_t690_w30_v0.002_br60_0.0008",
                        dom,
                        d_px,
                        "100%",
                        "+$0.99",
                        f"drop_vel={drop_vel:.4f}/s btc_rng60={btc_rng60:.4%}",
                    )
