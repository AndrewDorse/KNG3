FROM python:3.11-slim

# Bump when syncing from kng_bot3 (labels only; COPY list below is the real contract).
# SHAMAN v1 only: Binance 5m/15m signals -> optional Polymarket FAK.
ARG KNG3_IMAGE_TAG=2026-04-25-shaman-rules-5m-15m
LABEL org.opencontainers.image.title="KNG3 SHAMAN v1" \
      org.opencontainers.image.description="Docker: SHAMAN v1 (Binance 5m/15m -> Polymarket UP/DOWN)" \
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
RUN python -c "s=open('/app/main.py',encoding='utf-8').read(); assert 'btc15_redeem_engine' not in s, 'main.py must not name btc15_redeem_engine'"
COPY config.py /app/config.py
COPY trader.py /app/trader.py
COPY market_locator.py /app/market_locator.py
COPY http_session.py /app/http_session.py
COPY clob_fak.py /app/clob_fak.py
COPY polymarket_ws.py /app/polymarket_ws.py
COPY shaman_v1_engine.py /app/shaman_v1_engine.py

RUN mkdir -p /app/PALADIN
COPY PALADIN/shaman_v1_eval.py /app/PALADIN/shaman_v1_eval.py
COPY PALADIN/shaman_v1_rules.json /app/PALADIN/shaman_v1_rules.json

RUN mkdir -p /app/logs /app/exports && \
    chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
