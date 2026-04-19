# Composer Simulation Guide

This repo can run simulations only if you first provide per-window snapshot CSVs with Polymarket prices plus BTC price/volume data.

## Current repo state

- Live trading logic exists in [btc15_redeem_engine.py](/c:/Users/Lenovo/Documents/Git/KNG3/btc15_redeem_engine.py).
- The only bundled replay script is [simulate_volume_scalp_grid.py](/c:/Users/Lenovo/Documents/Git/KNG3/simulate_volume_scalp_grid.py).
- Public snapshot recording is currently disabled in this repo:
  - `_maybe_record_price_snapshot()` in [btc15_redeem_engine.py](/c:/Users/Lenovo/Documents/Git/KNG3/btc15_redeem_engine.py) returns immediately.
- So for now, you must either:
  - copy enriched window CSVs into `exports/window_price_snapshots_public/`, or
  - point the simulator at the sibling repo fallback dataset in `..\kng_bot3\exports\window_price_snapshots_public\`.

## What the simulator expects

Each window file must be named like:

```text
exports/window_price_snapshots_public/20260419_120000_btc-updown-15m-1776502800_prices.csv
```

The simulator scans for `*_prices.csv` files and only keeps files that contain:

- `btc_price`
- one of `btc_quote_volume` or `btc_volume`

Recommended columns per row:

```csv
recorded_at,slug,question,elapsed_sec,remaining_sec,up_price,down_price,btc_price,btc_volume,btc_quote_volume,btc_trade_count,source
```

Minimum practical columns:

```csv
slug,elapsed_sec,up_price,down_price,btc_price,btc_quote_volume
```

Notes:

- `elapsed_sec` should increase from `0` to about `900`.
- `up_price` and `down_price` should be the PM prices for that second or poll.
- `btc_price` should be the BTC spot/perp price captured for that same row.
- `btc_quote_volume` is preferred.
- `btc_volume` also works.
- `btc_trade_count` is not required by the current grid sim, but keep it if you have it.

## Fastest way to fill windows

If you already have enriched files in the sibling analysis repo, copy them:

```powershell
New-Item -ItemType Directory -Force -Path .\exports\window_price_snapshots_public | Out-Null
Copy-Item ..\kng_bot3\exports\window_price_snapshots_public\*_prices.csv .\exports\window_price_snapshots_public\
```

If you want to verify the files are usable:

```powershell
Get-ChildItem .\exports\window_price_snapshots_public\*_prices.csv | Select-Object -First 5
Import-Csv (Get-ChildItem .\exports\window_price_snapshots_public\*_prices.csv | Select-Object -First 1).FullName | Select-Object -First 3
```

## How to run simulations

Run the bundled grid simulator:

```powershell
python .\simulate_volume_scalp_grid.py
```

It will:

- look in `.\exports\window_price_snapshots_public`
- fall back to `..\kng_bot3\exports\window_price_snapshots_public` if local files do not exist
- filter to files that include BTC price plus BTC volume
- test combinations of:
  - `target_price`: `0.09`, `0.10`, `0.11`, `0.12`
  - `max_time`: `600`, `720`, `840`
  - `max_trades_per_window`: `2`, `3`, `4`

Outputs:

- [exports/volume_scalp_grid/scalp_grid_results.csv](/c:/Users/Lenovo/Documents/Git/KNG3/exports/volume_scalp_grid/scalp_grid_results.csv)
- [exports/volume_scalp_grid/scalp_best_trades.csv](/c:/Users/Lenovo/Documents/Git/KNG3/exports/volume_scalp_grid/scalp_best_trades.csv)

## How the current simulator decides trades

This is important so you do not assume it is simulating `champ4_6s`.

The current bundled sim:

- waits until at least `60s`
- checks BTC quote-volume ratio against the previous `30` rows
- requires ratio `> 2.5`
- chooses side from BTC move vs window open:
  - BTC up => buy `UP`
  - BTC down => buy `DOWN`
- uses fixed `6` shares per trade
- allows up to `2/3/4` trades per window depending on grid setting
- exits at TP if reached, otherwise settles at `1` or `0`

So this script is a BTC-volume scalp backtest, not a true `champ4_6s` replay.

## If you want true `champ4_6s` simulations in this repo

You need a separate replay script. The data requirements are the same:

- full PM path for each window
- `btc_price`
- `btc_quote_volume` or `btc_volume`
- ideally `btc_trade_count`

Then the replay script should:

1. Read each enriched window CSV.
2. Determine BTC direction from window open.
3. At `24s`, `90s`, `540s`, and `600s`, apply the live `champ4_6s` rules.
4. Use `6`-share clips only.
5. Hold to settlement.
6. Write per-window PnL/ROI and fill logs.

## Composer workflow

If you are using Composer only as an operator workflow, use this order:

1. Fill `exports/window_price_snapshots_public/` with enriched per-window CSVs.
2. Verify headers include `btc_price` and `btc_quote_volume` or `btc_volume`.
3. Run `python .\simulate_volume_scalp_grid.py`.
4. Read:
   - `exports/volume_scalp_grid/scalp_grid_results.csv`
   - `exports/volume_scalp_grid/scalp_best_trades.csv`
5. If you want `champ4_6s` replay instead of scalp replay, add a dedicated `champ4` simulation script next.

## Important limitation

This repo does not currently auto-build enriched public window files during live runtime because `_maybe_record_price_snapshot()` is stubbed out. If you want local generation here, that function needs to be implemented or ported from the analysis repo.
