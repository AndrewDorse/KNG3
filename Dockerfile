FROM python:3.11-slim

# Bump when syncing Paladin v7 from kng_bot3 (labels only; COPY list below is the real contract).
# This tag: non-forced pair cap 0.96, FAK anchors, max_orders excl. reconcile/sync, refill VWAP fix.
ARG KNG3_IMAGE_TAG=2026-04-23-slim-main-guard
LABEL org.opencontainers.image.title="KNG3 Paladin v7" \
      org.opencontainers.image.version="${KNG3_IMAGE_TAG}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN useradd --create-home --shell /usr/sbin/nologin appuser

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

COPY main.py /app/main.py
COPY config.py /app/config.py
COPY trader.py /app/trader.py
COPY market_locator.py /app/market_locator.py
COPY btc_price_feed.py /app/btc_price_feed.py
COPY http_session.py /app/http_session.py
COPY clob_fak.py /app/clob_fak.py
COPY polymarket_ws.py /app/polymarket_ws.py
COPY paladin_v7_live_engine.py /app/paladin_v7_live_engine.py

RUN mkdir -p /app/PALADIN
COPY PALADIN/paladin_engine.py /app/PALADIN/paladin_engine.py
COPY PALADIN/paladin_v7.py /app/PALADIN/paladin_v7.py
COPY PALADIN/simulate_paladin_window.py /app/PALADIN/simulate_paladin_window.py
COPY PALADIN/paladin_sim_config.json /app/PALADIN/paladin_sim_config.json

# Fail build if monolithic kng_bot3 main.py was copied by mistake (image has no btc15_redeem_engine.py).
RUN python -c "s=open('/app/main.py',encoding='utf-8').read(); \
  assert 'btc15_redeem_engine' not in s, 'WRONG main.py: use KNG3 slim entrypoint only'; \
  assert 'paladin_v7_live_engine' in s, 'KNG3 main.py must import paladin_v7_live_engine'"

RUN mkdir -p /app/logs /app/exports && \
    chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
