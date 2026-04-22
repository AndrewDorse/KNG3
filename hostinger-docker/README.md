# Hostinger VPS Docker Setup

This folder is isolated from the current project runtime. It adds only the files needed to deploy the live bot through Hostinger Docker Manager from a GitHub URL.

## Files

- `Dockerfile`: minimal production image for the current bot entrypoint
- `.env.example`: environment variables to define in Hostinger

## What This Image Runs

- Entrypoint: `python main.py`
- Working directory inside container: `/app`
- Writable runtime folders:
  - `/app/logs`
  - `/app/exports`

The image copies only the files required for the live bot:

- `requirements.txt`
- `main.py`
- `config.py`
- `btc15_redeem_engine.py`
- `market_locator.py`
- `trader.py`
- `signal_analyzer.py`
- `http_session.py`

It also copies PALADIN live support:

- `polymarket_ws.py`, `clob_fak.py`, `paladin_live_engine.py`
- `PALADIN/` (shared sim engine + `paladin_sim_config.json` for profit-lock params)

It does not copy local logs, exports, virtualenv files, backups, or analysis scripts into the image.

## Hostinger Docker Manager

Use these values when creating the app from GitHub:

- Repository: this repo
- Dockerfile path: `hostinger-docker/Dockerfile`
- Build context: **repository root** (not the `hostinger-docker/` folder alone), so `COPY main.py` and friends resolve.
- Start command: leave empty, use Dockerfile default
- Port mapping: none needed

## Environment Variables

Set these in Hostinger Docker Manager, not in Git:

- Required:
  - `POLY_PRIVATE_KEY`
  - `POLY_FUNDER`
- Usually needed:
  - `POLY_SIGNATURE_TYPE`
  - `POLY_DRY_RUN`
  - `BOT_STRATEGY_MODE`
- Optional relayer values:
  - `RELAYER_API_KEY`
  - `RELAYER_SECRET`
  - `RELAYER_PASSPHRASE`

Use `.env.example` in this folder as the reference set.

## Persistent Storage

If Hostinger supports host path or named volume mounts, mount these paths so data survives redeploys:

- `/app/logs`
- `/app/exports`

Recommended:

- keep `logs` persistent
- keep `exports` persistent if you want snapshots, reports, or strategy artifacts to survive redeploys

## Go live checklist

1. Set secrets only in Hostinger env (never in Git).
2. First deploy with `POLY_DRY_RUN=true`; confirm logs show `PALADIN_pair_live_v3`, `paladin_ladder=`, heartbeat lines, and WS status (or REST fallback).
3. Set `POLY_DRY_RUN=false` to trade for real.

## Strategy note (default: `paladin`)

This repo is the **KNG3 deploy mirror** of development tree `kng_bot3`. Sync PALADIN sources from there before production bumps.

The deployment defaults to **`BOT_STRATEGY_MODE=paladin`** (**PALADIN v3**: **10 shares/side**, **100s** between pair starts, **no trailing** unless you set `BOT_PALADIN_ENTRY_TRAILING_MIN_LOW_SEC` ≥ 0). Behavior:

- Pair-only FAK buys on BTC 15m up/down; optional CLOB market WebSocket for mids (`BOT_POLY_WS_*`)
- Gates: pair sum cap, marginal ROI on the second leg, staggered first leg, optional hedge-force timer, per-side share cap
- Tuning: `BOT_PALADIN_*` in `.env.example`; profit-lock thresholds in `/app/PALADIN/paladin_sim_config.json`

Set `BOT_STRATEGY_MODE=iy2` to run the legacy overlap engine instead (params in `/app/strategy_params/iy2_summary.json`).

## Security

- Do not commit private keys or relayer secrets into GitHub.
- Put secrets only in Hostinger environment variables.
- If any private key has already been committed anywhere in this repo, rotate it before deployment.

## Notes

- This container is for a background worker, not a web service.
- No reverse proxy, domain, or HTTP port is required.
- If Hostinger builds too slowly because the repo is large, the next step would be a root-level `.dockerignore`. That is intentionally not added here to avoid changing the current project layout.
