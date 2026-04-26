#!/usr/bin/env python3
"""SHAMAN v1: Binance 5m/15m signals at each interval boundary; optional PM FAK. Logs only at window edges."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
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
_LOG = logging.getLogger("shaman_v1")


def configure_shaman_runtime_logging() -> None:
    """Only ``shaman_v1`` logs to stdout; silence root and library noise between boundaries."""
    root = logging.getLogger()
    root.setLevel(logging.CRITICAL)
    for noisy in (
        "urllib3",
        "requests",
        "charset_normalizer",
        "polymarket_btc_ladder",
        "http_session",
        "trader",
        "market_locator",
        "web3",
        "web3.providers",
    ):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)
        logging.getLogger(noisy).propagate = False

    _LOG.setLevel(logging.INFO)
    _LOG.propagate = False
    if not _LOG.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(logging.INFO)
        h.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        _LOG.addHandler(h)


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


def _binance_rg(i: int, o: list[float], c: list[float]) -> str | None:
    if c[i] > o[i]:
        return "G"
    if c[i] < o[i]:
        return "R"
    return None


def _notional_usdc(winning_count: int, cfg: BotConfig) -> float:
    n = max(1, winning_count)
    raw = cfg.shaman_v1_notional_base_usdc + (n - 1) * cfg.shaman_v1_notional_per_extra_signal_usdc
    return min(float(cfg.shaman_v1_notional_max_usdc), float(raw))


@dataclass(slots=True)
class _Pending:
    label: str
    interval_ms: int
    target_bar_open_ms: int
    pred: str | None
    n_g: int
    n_r: int
    pm_side: str | None
    notional: float
    shares: int
    entry_ask: float | None
    entry_limit_px: float
    token_id: str | None
    slug: str


class ShamanV1Engine:
    """5m/15m Binance boundaries: WINDOW_START (signals), WINDOW_END (Binance right/wrong + dry PnL)."""

    def __init__(
        self,
        config: BotConfig,
        locator: GammaMarketLocator,
        trader: PolymarketTrader,
    ) -> None:
        self.config = config
        self.locator = locator
        self.trader = trader
        rules_path = _default_rules_path(config)
        all_rules = _load_rules_json(rules_path)
        self._rules_5m = [r for r in all_rules if str(r.get("timeframe", "")).strip() == "5m"]
        self._rules_15m = [r for r in all_rules if str(r.get("timeframe", "")).strip() == "15m"]
        self._http = requests.Session()
        self._last_5m_open_ms: int | None = None
        self._last_15m_open_ms: int | None = None
        self._pending_5m: _Pending | None = None
        self._pending_15m: _Pending | None = None

    def _pm_seconds_remaining(self, contract: ActiveContract) -> float:
        now = datetime.now(timezone.utc)
        return (contract.end_time - now).total_seconds()

    def _aggregate_at_t(
        self,
        rules: list[dict[str, Any]],
        o: list[float],
        hi: list[float],
        lo: list[float],
        c: list[float],
        v: list[float],
        t: int,
    ) -> tuple[int, int]:
        return _aggregate_signals(rules, o, c, v, hi, lo, t)

    def _resolve_and_start(
        self,
        *,
        label: str,
        interval: str,
        interval_ms: int,
        rules: list[dict[str, Any]],
        last_closed_open_ms: int,
        pending: _Pending | None,
    ) -> tuple[int, _Pending | None]:
        """Resolve previous window END (if pending matches this close), then emit START and return new pending."""
        try:
            opens_ms, o, hi, lo, c, v = _fetch_binance_klines(
                self._http,
                self.config.btc_feed_symbol,
                interval,
                max(120, int(self.config.shaman_v1_kline_limit)),
                float(self.config.request_timeout_seconds),
            )
        except Exception:
            return last_closed_open_ms, pending

        if len(o) < 50 or len(opens_ms) < 3:
            return last_closed_open_ms, pending

        closed_open_ms = opens_ms[-2]
        t = len(o) - 2
        if closed_open_ms != last_closed_open_ms:
            return max(last_closed_open_ms, closed_open_ms), pending

        if pending is not None and pending.target_bar_open_ms == closed_open_ms:
            act = _binance_rg(t, o, c)
            if pending.pred is None:
                match = "NO_SIGNAL"
            elif act is None:
                match = "N/A"
            elif act == pending.pred:
                match = "RIGHT"
            else:
                match = "WRONG"

            pnl_part = ""
            if self.config.dry_run and pending.pred is not None and pending.shares > 0 and pending.token_id and pending.entry_ask is not None:
                bid = self.trader.get_best_bid(pending.token_id)
                if bid is not None and bid > 0:
                    pnl = pending.shares * (float(bid) - float(pending.entry_ask))
                    pnl_part = f" dry_pnl_usdc={pnl:+.4f} exit_bid={bid:.2f} entry_ask={pending.entry_ask:.2f} sh={pending.shares}"
                else:
                    pnl_part = " dry_pnl_usdc=N/A (no bid)"
            elif not self.config.dry_run:
                pnl_part = " pnl=live_not_marked_in_logs"
            elif self.config.dry_run:
                if pending.pred is None:
                    pnl_part = " dry_pnl_usdc=N/A (no_signal)"
                elif pending.shares <= 0:
                    pnl_part = " dry_pnl_usdc=N/A (no_clip)"

            _LOG.info(
                "%s WINDOW_END target_open_ms=%s actual_binance=%s pred=%s match=%s nG=%d nR=%d pm_side=%s%s",
                label,
                pending.target_bar_open_ms,
                act or "DOJI",
                pending.pred or "NONE",
                match,
                pending.n_g,
                pending.n_r,
                pending.pm_side or "NONE",
                pnl_part,
            )
            pending = None

        ng, nr = self._aggregate_at_t(rules, o, hi, lo, c, v, t)
        if ng > nr:
            pred, win_side, winning = "G", "UP", ng
        elif nr > ng:
            pred, win_side, winning = "R", "DOWN", nr
        else:
            pred, win_side, winning = None, None, 0

        notional = _notional_usdc(winning, self.config) if pred is not None else 0.0
        next_open_ms = closed_open_ms + interval_ms

        sig_part = f"nG={ng} nR={nr} pred_binance={pred or 'TIE'} pred_PM={win_side or 'NONE'} notional_usdc={notional:.2f}"

        entry_ask = None
        entry_limit_px = 0.0
        shares = 0
        token_id: str | None = None
        slug = ""
        contract = self.locator.get_active_contract()
        if contract is not None:
            slug = contract.slug
        action = "no_PM_order"

        if pred is not None and contract is not None:
            rem = self._pm_seconds_remaining(contract)
            if rem > float(self.config.strategy_new_order_cutoff_seconds):
                tok = contract.up if win_side == "UP" else contract.down
                ask = self.trader.get_best_ask(tok.token_id)
                if ask is not None and ask > 0:
                    pad = max(0.0, float(self.config.shaman_v1_price_pad))
                    entry_limit_px = min(0.99, round(float(ask) + pad, 2))
                    shares = int(notional / entry_limit_px) if entry_limit_px > 0 else 0
                    min_sh = max(1, int(self.config.shaman_v1_min_shares))
                    if shares >= min_sh and shares * entry_limit_px >= float(self.config.shaman_v1_min_notional_usdc):
                        entry_ask = float(ask)
                        token_id = tok.token_id
                        action = f"PM_clip shares={shares} limit<={entry_limit_px:.2f} ask={ask:.2f} slug={slug} Tminus_s={rem:.0f}"
                        if not self.config.dry_run:
                            self.trader.place_marketable_buy_with_result(
                                tok,
                                entry_limit_px,
                                shares,
                                confirm_get_order=self.config.polymarket_fak_confirm_get_order,
                            )
                            action += " SENT"
                    else:
                        action = "PM_skip size_or_notional"
                else:
                    action = "PM_skip no_ask"
            else:
                action = f"PM_skip cutoff rem_s={rem:.1f}"
        elif pred is not None:
            action = "PM_skip no_contract"

        _LOG.info(
            "%s WINDOW_START next_bar_open_ms=%s %s | %s",
            label,
            next_open_ms,
            sig_part,
            action,
        )

        new_pending = _Pending(
            label=label,
            interval_ms=interval_ms,
            target_bar_open_ms=next_open_ms,
            pred=pred,
            n_g=ng,
            n_r=nr,
            pm_side=win_side,
            notional=notional,
            shares=shares,
            entry_ask=entry_ask,
            entry_limit_px=entry_limit_px,
            token_id=token_id,
            slug=slug,
        )

        return closed_open_ms, new_pending

    def run(self) -> None:
        while True:
            now_ms = int(time.time() * 1000)
            last_5 = _last_closed_bar_open_time_ms(300_000, now_ms)
            last_15 = _last_closed_bar_open_time_ms(900_000, now_ms)

            if self._last_5m_open_ms is None:
                self._last_5m_open_ms = last_5
            elif last_5 > self._last_5m_open_ms:
                self._last_5m_open_ms, self._pending_5m = self._resolve_and_start(
                    label="5m",
                    interval="5m",
                    interval_ms=300_000,
                    rules=self._rules_5m,
                    last_closed_open_ms=last_5,
                    pending=self._pending_5m,
                )

            if self._last_15m_open_ms is None:
                self._last_15m_open_ms = last_15
            elif last_15 > self._last_15m_open_ms:
                self._last_15m_open_ms, self._pending_15m = self._resolve_and_start(
                    label="15m",
                    interval="15m",
                    interval_ms=900_000,
                    rules=self._rules_15m,
                    last_closed_open_ms=last_15,
                    pending=self._pending_15m,
                )

            time.sleep(max(0.2, min(2.0, float(self.config.poll_interval_seconds))))


__all__ = ["ShamanV1Engine", "configure_shaman_runtime_logging"]
