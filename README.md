# KNG3 — PALADIN **v9** (Docker default) + optional **SHAMAN v1**

Minimal mirror to run **PALADIN v9** live on Polymarket BTC 15m markets. **`BOT_STRATEGY_MODE` defaults to `paladin_v9`** if unset (`config.py` + `.env.example`). Optional **`paladin_v7`** uses the same rule kernel (`paladin_v7_step`) and the same `BOT_PALADIN_V7_*` tunables; v9 is the product entry (`PaladinV9LiveEngine`) and matches the v9 second-sim backtest naming. Optional **`shaman_v1`** runs `shaman_v1_engine.py`: on each Binance **5m** and **15m** candle close it evaluates bundled rules in `PALADIN/shaman_v1_rules.json` and may place a **FAK** on UP or DOWN for the active Polymarket window (same slug discovery as PALADIN).

- Copy `.env.example` to `.env` and set keys / `POLY_DRY_RUN`. Default **`BOT_STRATEGY_MODE=paladin_v9`** uses a **$400** per-window budget cap unless you set **`BOT_STRATEGY_BUDGET_CAP_USDC`** (v7 default cap in code is **$10** when mode is v7 and the cap env is unset).
- `docker compose build --no-cache` then `docker compose up -d` after each pull (see **`Dockerfile`** `KNG3_IMAGE_TAG` bump).

**Entry logic (first legs & layers):** see **`PALADIN/V7_ENTRY_RULES.md`** in this repo (also copied into the image at `/app/PALADIN/V7_ENTRY_RULES.md`).

`BOT_STRATEGY_MODE` must be **`paladin_v7`**, **`paladin_v9`**, or **`shaman_v1`** for the bundled **KNG3 `main.py`** (other modes are rejected).

The image also copies **`btc15_redeem_engine.py`**, **`paladin_live_engine.py`**, and **`signal_analyzer.py`** so a **monolithic `kng_bot3/main.py`** (same imports as upstream) can start without `ModuleNotFoundError` after rebuild.

**Build from this repo** (root `Dockerfile`). If an old image still errors on a missing module, rebuild with `docker compose build --no-cache`.

If logs show **`ModuleNotFoundError: btc15_redeem_engine`** at **`main.py` line 8**, the container is **not** running this repo’s **`main.py`** (line 8 here is `from config`). You are on a **stale image** or a **volume/bind mount** is replacing `/app/main.py` with **`kng_bot3` monolithic `main.py`**. Fix: **`docker compose build --no-cache`**, remove any mount over `/app` or `main.py`, redeploy, and **`git pull`** this repo so the build uses the current **`Dockerfile`** (includes a build-time check that `main.py` does not mention `btc15_redeem_engine`).

Full strategy development stays in **kng_bot3**; this repo only ships what the `Dockerfile` copies.

**Sync:** from `kng_bot3` run `powershell -File deploy\sync_kng3_mirror.ps1` (see `kng_bot3/deploy/KNG3_MIRROR.txt`). Sync copies everything in the `Dockerfile` `COPY` list **except** `main.py` — keep **this repo’s** paladin-only `main.py` in git. Image label: see **`Dockerfile`** `KNG3_IMAGE_TAG`.

## Verify before deploy (local)

From this repo root (no Docker required for the first two):

```powershell
python -m py_compile main.py config.py trader.py market_locator.py btc_price_feed.py http_session.py clob_fak.py polymarket_ws.py paladin_v7_live_engine.py paladin_v9_live_engine.py shaman_v1_engine.py btc15_redeem_engine.py paladin_live_engine.py signal_analyzer.py PALADIN\paladin_engine.py PALADIN\paladin_v7.py PALADIN\simulate_paladin_window.py PALADIN\shaman_v1_eval.py
python -c "import paladin_v7_live_engine; import paladin_v9_live_engine; import shaman_v1_engine; import btc15_redeem_engine; import config; print('imports_ok')"
```

Then with Docker installed (`docker compose` reads **`.env`** — create it from `.env.example` first):

```powershell
docker compose build
```
