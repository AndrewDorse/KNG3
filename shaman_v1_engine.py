#!/usr/bin/env python3
"""SHAMAN v1: Binance 5m/15m signals at each closed kline; optional PM FAK. Logs at edges + INIT.

Each poll: fetch 5m + 15m at the same ``now_ms``, then emit all ``WINDOW_END`` lines (5m, 15m) before
any ``WINDOW_START`` so a new 5m start never appears above an unresolved 15m end in the same tick.
For a single label, end always precedes start (same as before)."""

from __future__ import annotations

import importlib.util
import json
import logging
import math
import sys
import time
from dataclasses import dataclass
from typing import Any, NamedTuple
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import ActiveContract, BotConfig
from market_locator import GammaMarketLocator
from trader import PolymarketTrader

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
_FAK_NO_MATCH_ERROR_SNIPPET = "no orders found to match with fak order"
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
    """Fetch at least ``limit`` klines, oldest first.

    The public API returns **at most 1000** bars per call; the previous single-call path
    could only ever load ≤1000. Research/backtests use ~8k+ bars. Pattern ``aux`` (body
    and range percentiles) is computed on the **full** array — a short live window is why
    live often saw ``nG=0 nR=0`` (nothing matches) even when the closed candle is clearly G/R
    (see ``candle=`` in logs) — *not* a broken Binance feed.
    """
    need_total = int(limit)
    need_total = min(max(120, need_total), 10_000)
    rows: list = []
    end_time: int | None = None
    while len(rows) < need_total:
        need = min(1000, need_total - len(rows))
        params: dict[str, str | int] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": need,
        }
        if end_time is not None:
            params["endTime"] = end_time
        r = session.get(_BINANCE_KLINES, params=params, timeout=timeout)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows = batch + rows
        end_time = int(batch[0][0]) - 1
        if len(batch) < need:
            break
    tail = rows[-need_total:]
    opens_ms: list[int] = []
    o, hi, lo, c, v = [], [], [], [], []
    for row in tail:
        opens_ms.append(int(row[0]))
        o.append(float(row[1]))
        hi.append(float(row[2]))
        lo.append(float(row[3]))
        c.append(float(row[4]))
        v.append(float(row[5]))
    return opens_ms, o, hi, lo, c, v


def _index_open_ms(opens_ms: list[int], open_ms: int) -> int | None:
    for i, t in enumerate(opens_ms):
        if t == open_ms:
            return i
    return None


def _index_last_fully_closed_bar(opens_ms: list[int], interval_ms: int, now_ms: int) -> int | None:
    """Index of the most recent kline whose interval has fully ended (Binance bar open = start time).

    Default REST behavior: the last row is the **in-progress** candle, so the last *fully* closed
    bar is at ``len-2``. Some feeds / mirrors return **only completed** rows; then the last row is
    already closed and the signal bar is at ``len-1``. Using ``len-2`` in that case evaluates the
    **previous** period — rules look "dead" (nG=nR=0) while the candle still moves on the chart.
    """
    if len(opens_ms) < 2:
        return None
    im = int(interval_ms)
    nm = int(now_ms)
    for i in range(len(opens_ms) - 1, -1, -1):
        if int(opens_ms[i]) + im <= nm:
            return i
    return None


def _binance_rg(i: int, o: list[float], c: list[float]) -> str | None:
    if c[i] > o[i]:
        return "G"
    if c[i] < o[i]:
        return "R"
    return None


def _integer_clip_notional_usdc(x: float) -> float:
    """Clip budget in whole USDC only ($1, $2, …), never < $1."""
    if x <= 0:
        return 0.0
    return float(max(1, int(math.floor(float(x) + 1e-9))))


def _is_fak_no_match_error(exc: Exception) -> bool:
    """True when CLOB rejected FAK because no resting orders matched the clip."""
    return _FAK_NO_MATCH_ERROR_SNIPPET in str(exc).lower()


def _notional_usdc(winning_count: int, cfg: BotConfig) -> float:
    """USDC clip: raw $ from rules, cap, then **integer** dollars (min $1)."""
    if winning_count <= 0:
        return 0.0
    if winning_count == 1:
        raw = float(cfg.shaman_v1_usdc_single_signal)
    else:
        raw = float(winning_count) * float(cfg.shaman_v1_usdc_per_signal)
    capped = min(float(cfg.shaman_v1_notional_max_usdc), raw)
    return _integer_clip_notional_usdc(capped)


class _KlineSnapshot(NamedTuple):
    """One Binance kline buffer + last fully closed index (``closed_open_ms = opens[i_closed]``)."""

    opens_ms: list[int]
    o: list[float]
    hi: list[float]
    lo: list[float]
    c: list[float]
    v: list[float]
    i_closed: int
    closed_open_ms: int


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
    shares: float
    entry_ask: float | None
    entry_limit_px: float
    token_id: str | None
    slug: str


class ShamanV1Engine:
    """Advance when the last *fully closed* kline advances (see ``_index_last_fully_closed_bar``)."""

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
        self._watermark_5m: int | None = None
        self._watermark_15m: int | None = None
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

    def _log_init(self) -> None:
        bal_s = "n/a"
        try:
            b = self.trader.wallet_balance_usdc()
            bal_s = f"{b:.2f}"
        except Exception as exc:
            bal_s = f"error:{exc!r}"
        _LOG.info(
            "INIT version=%s dry_run=%s (if true: Polymarket orders are NOT sent; set POLY_DRY_RUN=false for live) "
            "wallet_usdc=%s rules_5m=%d rules_15m=%d "
            "order_usdc=$%.2f_if_1_signal_else_$%.2f_each max=$%.2f btc=%s poll_s=%.2f",
            self.config.bot_version,
            self.config.dry_run,
            bal_s,
            len(self._rules_5m),
            len(self._rules_15m),
            float(self.config.shaman_v1_usdc_single_signal),
            float(self.config.shaman_v1_usdc_per_signal),
            float(self.config.shaman_v1_notional_max_usdc),
            self.config.btc_feed_symbol,
            float(self.config.poll_interval_seconds),
        )

    def _emit_end(
        self,
        *,
        label: str,
        pending: _Pending,
        act: str | None,
        late: bool,
    ) -> None:
        if pending.pred is None:
            match = "NO_SIGNAL"
        elif act is None:
            match = "N/A"
        elif act == pending.pred:
            match = "RIGHT"
        else:
            match = "WRONG"

        pnl_part = ""
        if self.config.dry_run and pending.pred is not None and pending.shares > 1e-9 and pending.token_id and pending.entry_ask is not None:
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

        late_tag = " late_tick=1" if late else ""
        _LOG.info(
            "%s WINDOW_END target_open_ms=%s actual_binance=%s pred=%s match=%s nG=%d nR=%d pm_side=%s%s%s",
            label,
            pending.target_bar_open_ms,
            act or "DOJI",
            pending.pred or "NONE",
            match,
            pending.n_g,
            pending.n_r,
            pending.pm_side or "NONE",
            pnl_part,
            late_tag,
        )

    def _resolve_pending_for_closed_bar(
        self,
        *,
        label: str,
        pending: _Pending | None,
        closed_open_ms: int,
        opens_ms: list[int],
        o: list[float],
        c: list[float],
    ) -> _Pending | None:
        if pending is None:
            return None
        if pending.target_bar_open_ms > closed_open_ms:
            _LOG.info(
                "%s WINDOW sync_reset target_open_ms=%s > api_last_closed_open_ms=%s (dropping stale pending)",
                label,
                pending.target_bar_open_ms,
                closed_open_ms,
            )
            return None
        late = pending.target_bar_open_ms < closed_open_ms
        ix = _index_open_ms(opens_ms, pending.target_bar_open_ms)
        act = _binance_rg(ix, o, c) if ix is not None and 0 <= ix < len(o) else None
        if ix is None:
            _LOG.info(
                "%s WINDOW_END target_open_ms=%s actual_binance=N/A pred=%s match=N/A "
                "(target not in kline buffer; widen BOT_SHAMAN_V1_KLINE_LIMIT)",
                label,
                pending.target_bar_open_ms,
                pending.pred or "NONE",
            )
        else:
            self._emit_end(label=label, pending=pending, act=act, late=late)
        return None

    def _start_for_closed_signal_bar(
        self,
        *,
        label: str,
        interval_ms: int,
        window_minutes: int,
        rules: list[dict[str, Any]],
        closed_open_ms: int,
        signal_t: int,
        opens_ms: list[int],
        o: list[float],
        hi: list[float],
        lo: list[float],
        c: list[float],
        v: list[float],
    ) -> _Pending:
        t = signal_t
        if t >= 0 and t < len(opens_ms) and opens_ms[t] != closed_open_ms:
            _LOG.error(
                "SHAMAN kline index bug: opens_ms[t]!=closed_open_ms t=%s %s!=%s",
                t,
                opens_ms[t],
                closed_open_ms,
            )
        _candle = _binance_rg(t, o, c)
        candle_s = "X" if _candle is None else _candle
        ng, nr = self._aggregate_at_t(rules, o, hi, lo, c, v, t)
        if ng > nr:
            pred, win_side, winning = "G", "UP", ng
        elif nr > ng:
            pred, win_side, winning = "R", "DOWN", nr
        else:
            pred, win_side, winning = None, None, 0

        notional = _notional_usdc(winning, self.config) if pred is not None else 0.0
        next_open_ms = closed_open_ms + interval_ms
        # ``pred`` is the **rule vote** (G majority / R majority / no majority "TIE"), not the candle color.
        sig_part = (
            f"klines_n={len(o)} i_sig={t} nG={ng} nR={nr} "
            f"candle={candle_s} rules_vote={pred or 'TIE'} pred_PM={win_side or 'NONE'} "
            f"winning_signals={winning} "
            f"usdc_1={float(self.config.shaman_v1_usdc_single_signal):.2f} usdc_n_each={float(self.config.shaman_v1_usdc_per_signal):.2f} "
            f"notional_usdc={notional:.2f}"
        )

        entry_ask = None
        entry_limit_px = 0.0
        shares = 0.0
        token_id: str | None = None
        slug = ""
        contract = self.locator.get_active_contract_for_window_minutes(window_minutes)
        if contract is not None:
            slug = contract.slug
        action = "no_PM_order"

        if pred is not None and contract is not None:
            rem = self._pm_seconds_remaining(contract)
            cutoff = float(self.config.strategy_new_order_cutoff_seconds)
            if rem > cutoff:
                tok = contract.up if win_side == "UP" else contract.down
                ask = self.trader.get_best_ask(tok.token_id)
                if ask is not None and ask > 0:
                    pad = max(0.0, float(self.config.shaman_v1_price_pad))
                    entry_limit_px = min(0.99, round(float(ask) + pad, 2))
                    entry_ask = float(ask)
                    token_id = tok.token_id
                    # Live: USDC budget only — ``create_market_order`` sizes from the book (no local share math).
                    # Dry-run: approximate position in shares for WINDOW_END PnL logging.
                    shares = notional / max(float(ask), 0.01) if notional > 1e-12 else 0.0
                    if notional > 1e-12:
                        action = (
                            f"PM_mkt usdc=${notional:.2f} (book-priced FAK) ref_ask={ask:.2f} "
                            f"ref_limit≈{entry_limit_px:.2f} slug={slug} Tminus_s={rem:.0f}"
                        )
                        if not self.config.dry_run:
                            max_attempts = 3  # first try + 2 retries
                            sent = False
                            last_exc: Exception | None = None
                            for attempt in range(1, max_attempts + 1):
                                try:
                                    # Wait for each FAK result before any retry (sequential attempts).
                                    self.trader.place_market_buy_usdc_with_result(
                                        tok,
                                        notional,
                                        confirm_get_order=self.config.polymarket_fak_confirm_get_order,
                                    )
                                    sent = True
                                    if attempt == 1:
                                        action += " SENT"
                                    else:
                                        action += f" SENT_RETRY{attempt}"
                                    break
                                except Exception as exc:
                                    last_exc = exc
                                    if attempt < max_attempts and _is_fak_no_match_error(exc):
                                        _LOG.warning(
                                            "SHAMAN_FAK_RETRY %s attempt=%d/%d no-match FAK; retrying",
                                            label,
                                            attempt,
                                            max_attempts,
                                        )
                                        continue
                                    break
                            if not sent and last_exc is not None:
                                action += f" ORDER_ERR={last_exc!r}"
                                _LOG.warning(
                                    "SHAMAN_ORDER_ERR %s: %s (CLOB creds, balance, allowance, or API)",
                                    label,
                                    last_exc,
                                    exc_info=False,
                                )
                        else:
                            action += " NO_SEND_POLY_DRY_RUN"
                    else:
                        action = "PM_skip notional<=0"
                else:
                    action = "PM_skip no_ask"
                    if not self.config.dry_run:
                        _LOG.warning(
                            "SHAMAN_NO_ASK: signal=%s nG=%d nR=%d side=%s notional=%.2f — CLOB has no ask "
                            "(empty book or no sell-side liquidity) token=%s slug=%s",
                            label,
                            ng,
                            nr,
                            win_side,
                            notional,
                            (tok.token_id[:20] + "…")
                            if tok and getattr(tok, "token_id", None)
                            else "?",
                            slug,
                        )
            else:
                action = f"PM_skip cutoff rem_s={rem:.1f} (cutoff_s={cutoff:.0f})"
                if not self.config.dry_run and pred is not None:
                    _LOG.warning(
                        "SHAMAN_CUTOFF: %s nG=%d nR=%d notional=%.2f rem=%.1fs (no new orders in last %.0fs of window) slug=%s",
                        label,
                        ng,
                        nr,
                        notional,
                        rem,
                        cutoff,
                        slug,
                    )
        elif pred is not None:
            action = "PM_skip no_contract"
            if not self.config.dry_run:
                _LOG.warning(
                    "SHAMAN_NO_CONTRACT: %s nG=%d nR=%d — Gamma has no market for this %dm window (check slug / API)",
                    label,
                    ng,
                    nr,
                    window_minutes,
                )

        _LOG.info(
            "%s WINDOW_START pm_window=%dm closed_signal_open_ms=%s next_bar_open_ms=%s %s | %s",
            label,
            window_minutes,
            closed_open_ms,
            next_open_ms,
            sig_part,
            action,
        )

        return _Pending(
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

    def _snapshot_interval(
        self,
        *,
        label: str,
        interval: str,
        interval_ms: int,
        now_ms: int,
    ) -> _KlineSnapshot | None:
        try:
            opens_ms, o, hi, lo, c, v = _fetch_binance_klines(
                self._http,
                self.config.btc_feed_symbol,
                interval,
                max(120, int(self.config.shaman_v1_kline_limit)),
                float(self.config.request_timeout_seconds),
            )
        except Exception as exc:
            _LOG.info("%s poll_binance_error err=%s (will retry)", label, exc)
            return None

        if len(o) < 50 or len(opens_ms) < 3:
            return None

        i_closed = _index_last_fully_closed_bar(opens_ms, interval_ms, int(now_ms))
        if i_closed is None:
            return None
        return _KlineSnapshot(
            opens_ms=opens_ms,
            o=o,
            hi=hi,
            lo=lo,
            c=c,
            v=v,
            i_closed=i_closed,
            closed_open_ms=opens_ms[i_closed],
        )

    def run(self) -> None:
        self._log_init()
        while True:
            try:
                # One wall-clock instant for both fetches, then: all WINDOW_END before any WINDOW_START.
                # (Previously: full 5m step ran before 15m, so 5m WINDOW_START could log before 15m WINDOW_END.)
                now_ms = int(time.time() * 1000)
                s5 = self._snapshot_interval(
                    label="5m", interval="5m", interval_ms=300_000, now_ms=now_ms
                )
                s15 = self._snapshot_interval(
                    label="15m", interval="15m", interval_ms=900_000, now_ms=now_ms
                )

                if s5 is not None and self._watermark_5m is None:
                    self._watermark_5m = s5.closed_open_ms
                if s15 is not None and self._watermark_15m is None:
                    self._watermark_15m = s15.closed_open_ms

                adv5 = bool(
                    s5 is not None
                    and self._watermark_5m is not None
                    and s5.closed_open_ms > self._watermark_5m
                )
                adv15 = bool(
                    s15 is not None
                    and self._watermark_15m is not None
                    and s15.closed_open_ms > self._watermark_15m
                )

                if adv5:
                    self._pending_5m = self._resolve_pending_for_closed_bar(
                        label="5m",
                        pending=self._pending_5m,
                        closed_open_ms=s5.closed_open_ms,
                        opens_ms=s5.opens_ms,
                        o=s5.o,
                        c=s5.c,
                    )
                if adv15:
                    self._pending_15m = self._resolve_pending_for_closed_bar(
                        label="15m",
                        pending=self._pending_15m,
                        closed_open_ms=s15.closed_open_ms,
                        opens_ms=s15.opens_ms,
                        o=s15.o,
                        c=s15.c,
                    )
                if adv5 and s5 is not None:
                    self._pending_5m = self._start_for_closed_signal_bar(
                        label="5m",
                        interval_ms=300_000,
                        window_minutes=5,
                        rules=self._rules_5m,
                        closed_open_ms=s5.closed_open_ms,
                        signal_t=s5.i_closed,
                        opens_ms=s5.opens_ms,
                        o=s5.o,
                        hi=s5.hi,
                        lo=s5.lo,
                        c=s5.c,
                        v=s5.v,
                    )
                    self._watermark_5m = s5.closed_open_ms
                if adv15 and s15 is not None:
                    self._pending_15m = self._start_for_closed_signal_bar(
                        label="15m",
                        interval_ms=900_000,
                        window_minutes=15,
                        rules=self._rules_15m,
                        closed_open_ms=s15.closed_open_ms,
                        signal_t=s15.i_closed,
                        opens_ms=s15.opens_ms,
                        o=s15.o,
                        hi=s15.hi,
                        lo=s15.lo,
                        c=s15.c,
                        v=s15.v,
                    )
                    self._watermark_15m = s15.closed_open_ms
            except Exception:
                _LOG.exception("SHAMAN loop_error (continuing)")
            time.sleep(max(0.2, min(2.0, float(self.config.poll_interval_seconds))))


__all__ = ["ShamanV1Engine", "configure_shaman_runtime_logging"]
