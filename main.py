#!/usr/bin/env python3
"""BTC 15-minute continuous redeem-hold bot entry point."""

from __future__ import annotations

import sys

from config import BotConfig, BotConfigError, configure_logging, LOGGER
from btc15_redeem_engine import Btc15RedeemEngine
from market_locator import GammaMarketLocator
from trader import PolymarketTrader
from signal_analyzer import SignalAnalyzer


def main() -> int:
    try:
        config = BotConfig.from_env()
    except BotConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    configure_logging(config.log_level)

    LOGGER.info("=" * 60)
    LOGGER.info("BTC 15-MIN CONTINUOUS BOT")
    LOGGER.info("=" * 60)
    LOGGER.info("version      = %s", config.bot_version)
    LOGGER.info("dry_run      = %s", config.dry_run)
    LOGGER.info("strategy_mode= %s", config.strategy_mode)
    LOGGER.info("signal_preset= %s", config.signal_preset)
    if config.signal_preset == "w1":
        LOGGER.info("strategy preset= W1 (live-ready)")
    else:
        LOGGER.info("strategy preset= %s", config.signal_preset)
    LOGGER.info("market       = %s", config.market_slug_prefix)
    LOGGER.info("shares/order = %d", config.shares_per_level)
    LOGGER.info("budget cap   = $%.2f", config.strategy_budget_cap_usdc)
    LOGGER.info("reserve      = $%.2f", config.strategy_wallet_reserve_usdc)
    LOGGER.info("entry delay  = %ds", config.strategy_entry_delay_seconds)
    LOGGER.info("new cutoff   = %ds before end", config.strategy_new_order_cutoff_seconds)
    LOGGER.info("force_exit   = %ds before end", config.force_exit_before_end_seconds)
    LOGGER.info("poll         = %.1fs", config.poll_interval_seconds)
    LOGGER.info("continuous   = %s", not config.trade_one_window)
    LOGGER.info("=" * 60)

    locator = GammaMarketLocator(config)
    trader = PolymarketTrader(config)
    engine = Btc15RedeemEngine(config, locator, trader)

    signals = SignalAnalyzer(signal_preset=config.signal_preset)
    signals.attach(engine)
    if config.strategy_mode == "signal_only":
        LOGGER.info("Signal analyzer attached (LIVE — placing orders on signals)")
    else:
        LOGGER.info("Signal analyzer attached (log-only alongside %s)", config.strategy_mode)

    engine.run()
    signals.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
