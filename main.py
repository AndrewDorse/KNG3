#!/usr/bin/env python3
"""KNG3 Docker entry: PALADIN v7 only. No top-level btc15/paladin_v4 imports (avoids ModuleNotFoundError on minimal images)."""

from __future__ import annotations

import sys

from config import BotConfig, BotConfigError, LOGGER, configure_logging
from market_locator import GammaMarketLocator
from trader import PolymarketTrader


def main() -> int:
    try:
        config = BotConfig.from_env()
    except BotConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    if config.strategy_mode != "paladin_v7":
        print(
            "KNG3 Docker: BOT_STRATEGY_MODE must be paladin_v7 only. "
            f"Got {config.strategy_mode!r}.",
            file=sys.stderr,
        )
        return 2

    configure_logging(config.log_level)

    LOGGER.info("=" * 60)
    LOGGER.info("BTC 15-MIN CONTINUOUS BOT (KNG3 / PALADIN v7)")
    LOGGER.info("=" * 60)
    LOGGER.info("version      = %s", config.bot_version)
    LOGGER.info("dry_run      = %s", config.dry_run)
    LOGGER.info("strategy_mode= %s", config.strategy_mode)
    LOGGER.info("strategy_id  = %s", "PALADIN_v7_binance_spike_live")
    LOGGER.info("poly_ws      = %s (%s)", config.polymarket_ws_enabled, config.polymarket_ws_url)
    LOGGER.info(
        "paladin_v7   = budget=$%.2f base_order=%.1f max/side=%.0f layer2_dip=%.3f vol_ratio=%.2f lookback=%ds btc_move>=%.2f",
        float(config.strategy_budget_cap_usdc),
        float(config.paladin_v7_base_order_shares),
        float(config.paladin_v7_max_shares_per_side),
        float(config.paladin_v7_layer2_dip_below_avg),
        float(config.paladin_v7_volume_spike_ratio),
        int(config.paladin_v7_volume_lookback_sec),
        float(config.paladin_v7_btc_abs_move_min_usd),
    )
    LOGGER.info(
        "paladin_v7 our_pair_cap<=%.4f (cheap hedge held+opp; not raw pm_u+pm_d) | cheap_min_delay=%.1fs | hedge_timeout=%.1fs | slip=%.4f",
        float(config.paladin_v7_cheap_pair_avg_sum_nonforced_max),
        float(config.paladin_v7_cheap_hedge_min_delay_sec),
        float(config.paladin_v7_hedge_timeout_seconds),
        float(config.paladin_v7_cheap_hedge_slip_buffer),
    )
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

    from paladin_v7_live_engine import PaladinV7LiveEngine  # noqa: PLC0415

    if config.dry_run:
        LOGGER.warning("PALADIN v7: POLY_DRY_RUN=true — paper only (no CLOB orders).")
    else:
        LOGGER.warning("PALADIN v7: LIVE — FAK buys will execute on Polymarket. Ctrl+C stops the loop.")
    PaladinV7LiveEngine(config, locator, trader).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
