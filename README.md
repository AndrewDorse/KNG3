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

It does not copy local logs, exports, virtualenv files, backups, or analysis scripts into the image.

## Hostinger Docker Manager

Use these values when creating the app from GitHub:

- Repository: this repo
- Dockerfile path: `hostinger-docker/Dockerfile`
- Build context: repository **root** (so `COPY main.py` resolves). If your UI asks for context, set it to `.` / repo root, not `hostinger-docker/` alone.
- Start command: leave empty, use Dockerfile default
- Port mapping: none needed

Local checks from repo root:

```bash
python -m py_compile main.py config.py btc15_redeem_engine.py
docker build -f hostinger-docker/Dockerfile -t kng3-bot .
```

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

1. Set `POLY_PRIVATE_KEY`, `POLY_FUNDER`, and (if needed) relayer env vars in Hostinger only — never in Git.
2. Deploy with `POLY_DRY_RUN=true` first; confirm logs show market discovery, BTC feed, and `[STRATEGY PARAMS]` for `btc_perp15`.
3. When satisfied, set `POLY_DRY_RUN=false` and redeploy so the bot places real orders.

## Strategy note (`btc_perp15`)

The deployment example uses `BOT_STRATEGY_MODE=btc_perp15`. Behavior:

- Monitor the first `240s` of the window and sample BTC every `5s`.
- Trade `UP` only, and only when BTC trend is at least `+0.05%` over the monitor window.
- No cheaper-side fallback and no `DOWN` trades.
- Only enter when `UP` is in the entry band `0.05` to `0.80`.
- Place a `0.98` TP limit after entry.
- If TP is still open, force-dump in the final `15s` of the window.

Optional env vars: `BOT_PERP15_ENTRY_MIN`, `BOT_PERP15_ENTRY_MAX`, `BOT_PERP15_MONITOR_SECONDS`, `BOT_PERP15_SAMPLE_INTERVAL_SECONDS`, `BOT_PERP15_BTC_TREND_THRESHOLD`, `BOT_PERP15_TP_PRICE`, `BOT_PERP15_END_DUMP_SECONDS_REMAINING`, `BOT_PERP15_MIN_SHARES`.

The live runtime uses the repo's poll-based BTC feed and CLOB access, not a streaming L2 WebSocket.

If you switch to `mimic_lot`, the bot may look for:

- `/app/exports/wallet10_mimic_search.json`

In that case, provide the file through the mounted `/app/exports` volume before starting the container.

## Security

- Do not commit private keys or relayer secrets into GitHub.
- Put secrets only in Hostinger environment variables.
- If any private key has already been committed anywhere in this repo, rotate it before deployment.

## Notes

- This container is for a background worker, not a web service.
- No reverse proxy, domain, or HTTP port is required.
- If Hostinger builds too slowly because the repo is large, the next step would be a root-level `.dockerignore`. That is intentionally not added here to avoid changing the current project layout.
