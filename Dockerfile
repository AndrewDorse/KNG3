FROM python:3.11-slim

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
COPY btc15_redeem_engine.py /app/btc15_redeem_engine.py
COPY market_locator.py /app/market_locator.py
COPY trader.py /app/trader.py
COPY signal_analyzer.py /app/signal_analyzer.py
COPY http_session.py /app/http_session.py
COPY btc_price_feed.py /app/btc_price_feed.py
COPY strategy_params /app/strategy_params

RUN mkdir -p /app/logs /app/exports && \
    chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
