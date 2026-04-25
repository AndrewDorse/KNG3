FROM python:3.11-slim

# Bump when syncing from kng_bot3 (labels only; COPY list below is the real contract).
# Default live strategy: PALADIN v9 (BOT_STRATEGY_MODE unset -> paladin_v9; kernel still paladin_v7_step).
ARG KNG3_IMAGE_TAG=2026-04-26-v9-live-log-prefix
LABEL org.opencontainers.image.title="KNG3 PALADIN v9 (default)" \
      org.opencontainers.image.description="Docker image: paladin_v9 live unless BOT_STRATEGY_MODE=paladin_v7" \
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
# Guard: baked main must be KNG3 paladin-only entry (no top-level btc15 import).
RUN python -c "s=open('/app/main.py',encoding='utf-8').read(); assert 'btc15_redeem_engine' not in s, 'main.py must not name btc15_redeem_engine'"
COPY config.py /app/config.py
COPY trader.py /app/trader.py
COPY market_locator.py /app/market_locator.py
COPY btc_price_feed.py /app/btc_price_feed.py
COPY http_session.py /app/http_session.py
COPY clob_fak.py /app/clob_fak.py
COPY polymarket_ws.py /app/polymarket_ws.py
COPY paladin_v7_live_engine.py /app/paladin_v7_live_engine.py
COPY paladin_v9_live_engine.py /app/paladin_v9_live_engine.py
COPY btc15_redeem_engine.py /app/btc15_redeem_engine.py
COPY paladin_live_engine.py /app/paladin_live_engine.py
COPY signal_analyzer.py /app/signal_analyzer.py

RUN mkdir -p /app/PALADIN
COPY PALADIN/paladin_engine.py /app/PALADIN/paladin_engine.py
COPY PALADIN/paladin_v7.py /app/PALADIN/paladin_v7.py
COPY PALADIN/simulate_paladin_window.py /app/PALADIN/simulate_paladin_window.py
COPY PALADIN/paladin_sim_config.json /app/PALADIN/paladin_sim_config.json
COPY PALADIN/V7_ENTRY_RULES.md /app/PALADIN/V7_ENTRY_RULES.md

RUN mkdir -p /app/logs /app/exports && \
    chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
