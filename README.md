# KNG3 — **SHAMAN v1** (Docker)

Minimal image: **Binance 5m and 15m** candle boundaries → pattern signals → optional **Polymarket** FAK on the active BTC up/down market. Logs **only** at each interval edge: `WINDOW_START` (signals + clip intent) and `WINDOW_END` (Binance **RIGHT/WRONG** vs prediction; **dry_pnl_usdc** when `POLY_DRY_RUN=true`).

- Copy `.env.example` → `.env`, set `POLY_PRIVATE_KEY`, `POLY_FUNDER`, `POLY_DRY_RUN`, and `BOT_STRATEGY_MODE=shaman_v1`.
- `docker compose build --no-cache` then `docker compose up -d` after each pull.

**Build from this repo** (root `Dockerfile`). Rebuild if you see missing-module errors.

Full research and other strategies live in **kng_bot3**; this repo ships only what the `Dockerfile` copies.

**Sync from kng_bot3:** `powershell -File deploy\sync_kng3_mirror.ps1` (see `kng_bot3/deploy/KNG3_MIRROR.txt`). Sync does **not** overwrite `KNG3/main.py` — edit here, then commit.

## Verify (local, no Docker)

From this repo root:

```powershell
python -m py_compile main.py config.py trader.py market_locator.py http_session.py clob_fak.py polymarket_ws.py shaman_v1_engine.py PALADIN\shaman_v1_eval.py
python -c "import shaman_v1_engine; import config; print('imports_ok')"
```

Then with Docker:

```powershell
docker compose build
```
