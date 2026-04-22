#!/usr/bin/env python3
"""KNG3 image entry: PALADIN v7 live only (Polymarket BTC 15m)."""

from __future__ import annotations

import sys

from config import BotConfig, BotConfigError, LOGGER, configure_logging
from market_locator import GammaMarketLocator
from paladin_v7_live_engine import PaladinV7LiveEngine
from trader import PolymarketTrader


def main() -> int:
    try:
        config = BotConfig.from_env()
    except BotConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    if config.strategy_mode != "paladin_v7":
        print(
            "This repository / Docker image supports BOT_STRATEGY_MODE=paladin_v7 only. "
            f"Got {config.strategy_mode!r}.",
            file=sys.stderr,
        )
        return 2

    configure_logging(config.log_level)
    LOGGER.info("=" * 60)
    LOGGER.info("KNG3 — PALADIN v7 live")
    LOGGER.info("=" * 60)
    LOGGER.info("version      = %s", config.bot_version)
    LOGGER.info("dry_run      = %s", config.dry_run)
    LOGGER.info("strategy_id  = PALADIN_v7_binance_spike_live")
    LOGGER.info("poly_ws      = %s (%s)", config.polymarket_ws_enabled, config.polymarket_ws_url)
    LOGGER.info(
        "paladin_v7   = budget=$%.2f clip=%.1f max/side=%.0f max_orders=%d vol_ratio=%.2f lookback=%ds btc_move>=%.2f",
        float(config.strategy_budget_cap_usdc),
        float(config.paladin_v7_clip_shares),
        float(config.paladin_v7_max_shares_per_side),
        int(config.paladin_v7_max_orders),
        float(config.paladin_v7_volume_spike_ratio),
        int(config.paladin_v7_volume_lookback_sec),
        float(config.paladin_v7_btc_abs_move_min_usd),
    )
    LOGGER.info("forced_hedge = pm_up+pm_dn <= %.3f after hedge timeout", config.paladin_v7_forced_hedge_max_book_sum)
    LOGGER.info("market       = %s", config.market_slug_prefix)
    LOGGER.info("=" * 60)

    locator = GammaMarketLocator(config)
    trader = PolymarketTrader(config)

    if config.dry_run:
        LOGGER.warning("PALADIN v7: POLY_DRY_RUN=true — paper only (no CLOB orders).")
    else:
        LOGGER.warning("PALADIN v7: LIVE — FAK buys will execute on Polymarket. Ctrl+C stops the loop.")

    PaladinV7LiveEngine(config, locator, trader).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
