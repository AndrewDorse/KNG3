#!/usr/bin/env python3
"""SHAMAN v1: Binance 5m/15m candle-close pattern signals -> Polymarket UP/DOWN FAK clip."""

from __future__ import annotations

import importlib.util
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from config import ActiveContract, BotConfig, TokenMarket
from market_locator import GammaMarketLocator
from trader import PolymarketTrader

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


def _load_shaman_eval():
    path = Path(__file__).resolve().parent / "PALADIN" / "shaman_v1_eval.py"
    spec = importlib.util.spec_from_file_location("shaman_v1_eval", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load shaman eval from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_eval_mod = _load_shaman_eval()
_aggregate_signals = _eval_mod.aggregate_signals


def _default_rules_path(config: BotConfig) -> Path:
    raw = (config.shaman_v1_rules_path or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path(__file__).resolve().parent / "PALADIN" / "shaman_v1_rules.json"


def _load_rules_json(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("rules JSON must be a list")
    return [x for x in data if isinstance(x, dict)]


def _fetch_binance_klines(
    session: requests.Session,
    symbol: str,
    interval: str,
    limit: int,
    timeout: float,
) -> tuple[list[int], list[float], list[float], list[float], list[float], list[float]]:
    r = session.get(
        _BINANCE_KLINES,
        params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        timeout=timeout,
    )
    r.raise_for_status()
    rows = r.json()
    opens_ms: list[int] = []
    o, hi, lo, c, v = [], [], [], [], []
    for row in rows:
        opens_ms.append(int(row[0]))
        o.append(float(row[1]))
        hi.append(float(row[2]))
        lo.append(float(row[3]))
        c.append(float(row[4]))
        v.append(float(row[5]))
    return opens_ms, o, hi, lo, c, v


def _last_closed_bar_open_time_ms(interval_ms: int, now_ms: int) -> int:
    return (now_ms // interval_ms) * interval_ms - interval_ms


def _notional_usdc(winning_count: int, cfg: BotConfig) -> float:
    n = max(1, winning_count)
    raw = cfg.shaman_v1_notional_base_usdc + (n - 1) * cfg.shaman_v1_notional_per_extra_signal_usdc
    return min(float(cfg.shaman_v1_notional_max_usdc), float(raw))


@dataclass(slots=True)
class _EvalResult:
    timeframe: str
    candle_open_ms: int
    n_g: int
    n_r: int
    side: str | None  # "UP" | "DOWN"
    winning: int
    notional: float


class ShamanV1Engine:
    """On each Binance 5m (and 15m) candle close, aggregate SHAMAN rules and optionally FAK one leg."""

    def __init__(
        self,
        config: BotConfig,
        locator: GammaMarketLocator,
        trader: PolymarketTrader,
    ) -> None:
        self.config = config
        self.locator = locator
        self.trader = trader
        self._log = logging.getLogger("shaman_v1")
        rules_path = _default_rules_path(config)
        all_rules = _load_rules_json(rules_path)
        self._rules_5m = [r for r in all_rules if str(r.get("timeframe", "")).strip() == "5m"]
        self._rules_15m = [r for r in all_rules if str(r.get("timeframe", "")).strip() == "15m"]
        self._http = requests.Session()
        self._last_5m_open_ms: int | None = None
        self._last_15m_open_ms: int | None = None

    def _pm_seconds_remaining(self, contract: ActiveContract) -> float:
        now = datetime.now(timezone.utc)
        return (contract.end_time - now).total_seconds()

    def _evaluate(
        self,
        *,
        timeframe: str,
        rules: list[dict[str, Any]],
        candle_open_ms: int,
        o: list[float],
        hi: list[float],
        lo: list[float],
        c: list[float],
        v: list[float],
    ) -> _EvalResult | None:
        if len(o) < 50 or not rules:
            return None
        t = len(o) - 2
        ng, nr = _aggregate_signals(rules, o, c, v, hi, lo, t)
        if ng > nr:
            side, winning = "UP", ng
        elif nr > ng:
            side, winning = "DOWN", nr
        else:
            return _EvalResult(timeframe, candle_open_ms, ng, nr, None, 0, 0.0)
        return _EvalResult(
            timeframe,
            candle_open_ms,
            ng,
            nr,
            side,
            winning,
            _notional_usdc(winning, self.config),
        )

    def _maybe_fire_order(self, ev: _EvalResult) -> None:
        if ev.side is None or ev.winning < 1:
            self._log.info(
                "SHAMAN v1 %s close=%s tie n_G=%d n_R=%d — no trade",
                ev.timeframe,
                ev.candle_open_ms,
                ev.n_g,
                ev.n_r,
            )
            return

        contract = self.locator.get_active_contract()
        if contract is None:
            self._log.warning("SHAMAN v1: no active PM contract; skip")
            return

        rem = self._pm_seconds_remaining(contract)
        if rem <= float(self.config.strategy_new_order_cutoff_seconds):
            self._log.info(
                "SHAMAN v1: skip order (T-remaining=%.1fs <= cutoff=%ds)",
                rem,
                int(self.config.strategy_new_order_cutoff_seconds),
            )
            return

        token = contract.up if ev.side == "UP" else contract.down
        ask = self.trader.get_best_ask(token.token_id)
        if ask is None or ask <= 0:
            self._log.warning("SHAMAN v1: no best ask for %s; skip", ev.side)
            return

        pad = max(0.0, float(self.config.shaman_v1_price_pad))
        limit_px = min(0.99, round(float(ask) + pad, 2))
        if limit_px < 0.01:
            self._log.warning("SHAMAN v1: bad limit_px=%.4f; skip", limit_px)
            return

        shares = int(ev.notional / limit_px)
        min_sh = max(1, int(self.config.shaman_v1_min_shares))
        if shares < min_sh:
            self._log.info(
                "SHAMAN v1: computed shares=%d < min_sh=%d (notional=%.2f px=%.2f); skip",
                shares,
                min_sh,
                ev.notional,
                limit_px,
            )
            return

        notion_est = shares * limit_px
        if notion_est + 1e-9 < float(self.config.shaman_v1_min_notional_usdc):
            self._log.info(
                "SHAMAN v1: est notional %.2f < min %.2f; skip",
                notion_est,
                float(self.config.shaman_v1_min_notional_usdc),
            )
            return

        self._log.info(
            "SHAMAN v1 %s close=%s signals n_G=%d n_R=%d -> %s winning=%d notional~$%.2f "
            "FAK shares=%d @<=%.2f (ask=%.2f slug=%s T-=%.0fs)",
            ev.timeframe,
            ev.candle_open_ms,
            ev.n_g,
            ev.n_r,
            ev.side,
            ev.winning,
            ev.notional,
            shares,
            limit_px,
            ask,
            contract.slug,
            rem,
        )

        if self.config.dry_run:
            self._log.info("SHAMAN v1: dry_run — no POST")
            return

        self.trader.place_marketable_buy_with_result(
            token,
            limit_px,
            shares,
            confirm_get_order=self.config.polymarket_fak_confirm_get_order,
        )

    def _run_eval_for_interval(
        self,
        *,
        label: str,
        interval: str,
        rules: list[dict[str, Any]],
        last_open_ms: int,
        candle_open_ms: int,
    ) -> int:
        if candle_open_ms <= last_open_ms:
            return last_open_ms
        try:
            opens_ms, o, hi, lo, c, v = _fetch_binance_klines(
                self._http,
                self.config.btc_feed_symbol,
                interval,
                max(120, int(self.config.shaman_v1_kline_limit)),
                float(self.config.request_timeout_seconds),
            )
        except Exception as exc:
            self._log.warning("SHAMAN v1: Binance %s klines failed: %s", interval, exc)
            return last_open_ms

        if len(opens_ms) < 3:
            return last_open_ms
        closed_open_ms = opens_ms[-2]
        if closed_open_ms != candle_open_ms:
            self._log.debug(
                "SHAMAN v1 %s: API last-closed open_ms=%d expected=%d (clock skew?)",
                label,
                closed_open_ms,
                candle_open_ms,
            )

        ev = self._evaluate(
            timeframe=label,
            rules=rules,
            candle_open_ms=closed_open_ms,
            o=o,
            hi=hi,
            lo=lo,
            c=c,
            v=v,
        )
        if ev is not None:
            self._maybe_fire_order(ev)
        return max(last_open_ms, closed_open_ms)

    def run(self) -> None:
        sym = self.config.btc_feed_symbol
        self._log.info(
            "SHAMAN v1 live: symbol=%s rules=%d (5m) + %d (15m) dry_run=%s",
            sym,
            len(self._rules_5m),
            len(self._rules_15m),
            self.config.dry_run,
        )
        while True:
            now_ms = int(time.time() * 1000)
            last_5 = _last_closed_bar_open_time_ms(300_000, now_ms)
            last_15 = _last_closed_bar_open_time_ms(900_000, now_ms)

            if self._last_5m_open_ms is None:
                self._last_5m_open_ms = last_5
            elif last_5 > self._last_5m_open_ms:
                self._last_5m_open_ms = self._run_eval_for_interval(
                    label="5m",
                    interval="5m",
                    rules=self._rules_5m,
                    last_open_ms=self._last_5m_open_ms,
                    candle_open_ms=last_5,
                )

            if self._last_15m_open_ms is None:
                self._last_15m_open_ms = last_15
            elif last_15 > self._last_15m_open_ms:
                self._last_15m_open_ms = self._run_eval_for_interval(
                    label="15m",
                    interval="15m",
                    rules=self._rules_15m,
                    last_open_ms=self._last_15m_open_ms,
                    candle_open_ms=last_15,
                )

            time.sleep(max(0.2, min(2.0, float(self.config.poll_interval_seconds))))


__all__ = ["ShamanV1Engine"]
