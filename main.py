#!/usr/bin/env python3
"""KNG3 Docker entry: PALADIN v9 live (default); paladin_v7 optional. No top-level btc15 imports."""

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

    allowed = ("paladin_v7", "paladin_v9")
    if config.strategy_mode not in allowed:
        print(
            f"KNG3 Docker: BOT_STRATEGY_MODE must be one of {allowed} "
            f"(default in this image / .env.example: paladin_v9). "
            f"Got {config.strategy_mode!r}.",
            file=sys.stderr,
        )
        return 2

    configure_logging(config.log_level)

    LOGGER.info("=" * 60)
    LOGGER.info("KNG3 | BTC 15m | PALADIN v9 product (kernel=paladin_v7_step)")
    LOGGER.info("=" * 60)
    LOGGER.info("version      = %s", config.bot_version)
    LOGGER.info("dry_run      = %s", config.dry_run)
    LOGGER.info(
        "strategy_mode= %s  (KNG3 default if BOT_STRATEGY_MODE unset: paladin_v9; see .env.example)",
        config.strategy_mode,
    )
    LOGGER.info(
        "strategy_id  = %s",
        "PALADIN_v9_live_kernel_paladin_v7_step"
        if config.strategy_mode == "paladin_v9"
        else "PALADIN_v7_binance_spike_live",
    )
    LOGGER.info("poly_ws      = %s (%s)", config.polymarket_ws_enabled, config.polymarket_ws_url)
    LOGGER.info(
        "shared paladin_v7_step kernel tunables (Polymarket env still named BOT_PALADIN_V7_*) = budget=$%.2f base_order=%.1f max/side=%.0f layer2_hi_dip=%.3f layer2_lo_dip=%.3f bal_tol=%.2fsh "
        "layer2_cd=%.1fs imb_repair<%.3f pair_cd=%.0fs vol_ratio=%.2f lookback=%ds btc_move>=%.2f",
        float(config.strategy_budget_cap_usdc),
        float(config.paladin_v7_base_order_shares),
        float(config.paladin_v7_max_shares_per_side),
        float(config.paladin_v7_layer2_dip_below_avg),
        float(config.paladin_v7_layer2_low_vwap_dip_below_avg),
        float(config.paladin_v7_balance_share_tolerance),
        float(config.paladin_v7_layer2_cooldown_sec),
        float(config.paladin_v7_imbalance_repair_max_pair_sum),
        float(config.paladin_v7_pair_cooldown_sec),
        float(config.paladin_v7_volume_spike_ratio),
        int(config.paladin_v7_volume_lookback_sec),
        float(config.paladin_v7_btc_abs_move_min_usd),
    )
    LOGGER.info(
        "cheap hedge pair cap<=%.4f (held VWAP+opp+slip; not raw pm_u+pm_d) | cheap_min_delay=%.1fs | hedge_timeout=%.1fs | slip=%.4f",
        float(config.paladin_v7_cheap_pair_avg_sum_nonforced_max),
        float(config.paladin_v7_cheap_hedge_min_delay_sec),
        float(config.paladin_v7_hedge_timeout_seconds),
        float(config.paladin_v7_cheap_hedge_slip_buffer),
    )
    LOGGER.info(
        "CLOB limit buys cancel_after=%.1fs",
        float(config.paladin_v7_limit_order_cancel_seconds),
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

    if config.strategy_mode == "paladin_v9":
        from paladin_v9_live_engine import PaladinV9LiveEngine  # noqa: PLC0415

        if config.dry_run:
            LOGGER.warning("PALADIN v9: POLY_DRY_RUN=true — paper only (no CLOB orders).")
        else:
            LOGGER.warning(
                "PALADIN v9: LIVE — limit buys post to Polymarket (kernel=v7_step). "
                "Cancel after %.1fs. Ctrl+C stops the loop.",
                float(config.paladin_v7_limit_order_cancel_seconds),
            )
        PaladinV9LiveEngine(config, locator, trader).run()
        return 0

    from paladin_v7_live_engine import PaladinV7LiveEngine  # noqa: PLC0415

    if config.dry_run:
        LOGGER.warning("PALADIN v7: POLY_DRY_RUN=true — paper only (no CLOB orders).")
    else:
        LOGGER.warning(
            "PALADIN v7: LIVE — limit buys will post to Polymarket and cancel after %.1fs. Ctrl+C stops the loop.",
            float(config.paladin_v7_limit_order_cancel_seconds),
        )
    PaladinV7LiveEngine(config, locator, trader).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
