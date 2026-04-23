# KNG3 — PALADIN v7 (Docker only)

Minimal mirror to run **PALADIN v7** live on Polymarket BTC 15m markets.

- Copy `.env.example` to `.env` and set keys / `POLY_DRY_RUN` (e.g. **`BOT_STRATEGY_BUDGET_CAP_USDC=80`** to match common live sizing).
- `docker compose build --no-cache` then `docker compose up -d` after each pull (see **`Dockerfile`** `KNG3_IMAGE_TAG` bump).

`BOT_STRATEGY_MODE` must be `paladin_v7` for the bundled **KNG3 `main.py`** (it rejects other modes).

The image also copies **`btc15_redeem_engine.py`**, **`paladin_live_engine.py`**, and **`signal_analyzer.py`** so a **monolithic `kng_bot3/main.py`** (same imports as upstream) can start without `ModuleNotFoundError` after rebuild.

**Build from this repo** (root `Dockerfile`). If an old image still errors on a missing module, rebuild with `docker compose build --no-cache`.

If logs show **`ModuleNotFoundError: btc15_redeem_engine`** at **`main.py` line 8**, the container is **not** running this repo’s **`main.py`** (line 8 here is `from config`). You are on a **stale image** or a **volume/bind mount** is replacing `/app/main.py` with **`kng_bot3` monolithic `main.py`**. Fix: **`docker compose build --no-cache`**, remove any mount over `/app` or `main.py`, redeploy, and **`git pull`** this repo so the build uses the current **`Dockerfile`** (includes a build-time check that `main.py` does not mention `btc15_redeem_engine`).

Full strategy development stays in **kng_bot3**; this repo only ships what the `Dockerfile` copies.

**Sync:** from `kng_bot3` run `powershell -File deploy\sync_kng3_mirror.ps1` (see `kng_bot3/deploy/KNG3_MIRROR.txt`). Sync copies everything in the `Dockerfile` `COPY` list **except** `main.py` — keep **this repo’s** v7-only `main.py` in git. Image label: see **`Dockerfile`** `KNG3_IMAGE_TAG`.

## Verify before deploy (local)

From this repo root (no Docker required for the first two):

```powershell
python -m py_compile main.py config.py trader.py market_locator.py btc_price_feed.py http_session.py clob_fak.py polymarket_ws.py paladin_v7_live_engine.py btc15_redeem_engine.py paladin_live_engine.py signal_analyzer.py PALADIN\paladin_engine.py PALADIN\paladin_v7.py PALADIN\simulate_paladin_window.py
python -c "import paladin_v7_live_engine; import btc15_redeem_engine; import config; print('imports_ok')"
```

Then with Docker installed (`docker compose` reads **`.env`** — create it from `.env.example` first):

```powershell
docker compose build
```
