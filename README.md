# KNG3 — PALADIN v7 (Docker only)

Minimal mirror to run **PALADIN v7** live on Polymarket BTC 15m markets.

- Copy `.env.example` to `.env` and set keys / `POLY_DRY_RUN`.
- `docker compose up --build`

`BOT_STRATEGY_MODE` must be `paladin_v7` (the entrypoint rejects other modes).

Full strategy development stays in **kng_bot3**; this repo only ships what the `Dockerfile` copies.

**Sync:** from `kng_bot3` run `powershell -File deploy\sync_kng3_mirror.ps1` (see `kng_bot3/deploy/KNG3_MIRROR.txt`). Runtime files in this repo were last aligned with **`kng_bot3` @ `62302fa`** (same bytes as that checkout for the `Dockerfile` `COPY` list).
