#!/usr/bin/env python3
"""Lightweight real-time BTC price feed backed by Binance Vision."""

from __future__ import annotations

import time
from dataclasses import dataclass

import requests


BINANCE_VISION_TICKER_URL = "https://data-api.binance.vision/api/v3/ticker/price"


@dataclass(slots=True)
class BtcPricePoint:
    ts: float
    price: float


class RealtimeBtcPriceFeed:
    """Polls Binance Vision ticker data with simple client-side rate limiting."""

    def __init__(self, config) -> None:
        self.config = config
        self.session = requests.Session()
        self._last_poll_ts = 0.0
        self._last_price: float | None = None

    def poll(self) -> float:
        now = time.time()
        if (
            self._last_price is not None
            and now - self._last_poll_ts < max(0.1, float(self.config.btc_feed_poll_seconds))
        ):
            return self._last_price

        response = self.session.get(
            BINANCE_VISION_TICKER_URL,
            params={"symbol": self.config.btc_feed_symbol},
            timeout=self.config.request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        price = float(payload["price"])
        self._last_poll_ts = now
        self._last_price = price
        return price
