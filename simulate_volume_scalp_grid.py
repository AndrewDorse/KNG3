from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path


TARGET_PRICES = [0.09, 0.10, 0.11, 0.12]
MAX_TIMES = [600, 720, 840]
MAX_TRADES_OPTIONS = [2, 3, 4]

MIN_TIME = 60
VOLUME_LOOKBACK_SECONDS = 30
VOLUME_RATIO_THRESHOLD = 2.5
ENTRY_MIN_PRICE = 0.05
ENTRY_MAX_PRICE = 0.95
FIXED_SHARES = 6

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = REPO_ROOT / "exports" / "window_price_snapshots_public"
FALLBACK_DATA_DIR = REPO_ROOT.parent / "kng_bot3" / "exports" / "window_price_snapshots_public"
OUT_DIR = REPO_ROOT / "exports" / "volume_scalp_grid"


@dataclass
class SimTrade:
    slug: str
    path: str
    target_price: float
    max_time: int
    max_trades_per_window: int
    trade_index: int
    side: str
    entry_elapsed: int
    entry_price: float
    tp_price: float
    exit_elapsed: int
    exit_price: float
    exit_reason: str
    winner: str
    pnl: float
    volume_ratio: float
    btc_return: float


def dataset_dir() -> Path:
    if DEFAULT_DATA_DIR.exists():
        return DEFAULT_DATA_DIR
    return FALLBACK_DATA_DIR


def enriched_files() -> list[Path]:
    root = dataset_dir()
    files = sorted(root.glob("*_prices.csv"), key=lambda path: path.stat().st_mtime)
    out: list[Path] = []
    for path in files:
        with path.open(newline="", encoding="utf-8") as fh:
            header = next(csv.reader(fh))
        if "btc_price" in header and ("btc_quote_volume" in header or "btc_volume" in header):
            out.append(path)
    return out


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    rows = [row for row in rows if (row.get("btc_price") or "").strip()]
    rows.sort(key=lambda row: int(float(row["elapsed_sec"])))
    return rows


def winner_side(rows: list[dict[str, str]]) -> str:
    return "UP" if float(rows[-1]["btc_price"]) > float(rows[0]["btc_price"]) else "DOWN"


def volume_ratio(rows: list[dict[str, str]], idx: int) -> float | None:
    if idx < VOLUME_LOOKBACK_SECONDS:
        return None
    current = float(rows[idx].get("btc_quote_volume") or rows[idx].get("btc_volume") or 0.0)
    if current <= 0:
        return None
    prev_vals = [
        float(row.get("btc_quote_volume") or row.get("btc_volume") or 0.0)
        for row in rows[idx - VOLUME_LOOKBACK_SECONDS:idx]
    ]
    if not prev_vals:
        return None
    avg_prev = sum(prev_vals) / len(prev_vals)
    if avg_prev <= 0:
        return None
    return current / avg_prev


def side_price(row: dict[str, str], side: str) -> float:
    return float(row["up_price"]) if side == "UP" else float(row["down_price"])


def simulate_config(
    files: list[Path],
    *,
    target_price: float,
    max_time: int,
    max_trades_per_window: int,
) -> tuple[dict[str, object], list[SimTrade]]:
    trades: list[SimTrade] = []
    for path in files:
        rows = load_rows(path)
        if not rows:
            continue
        winner = winner_side(rows)
        open_btc = float(rows[0]["btc_price"])
        opens: list[tuple[int, int, str, float, float, float]] = []
        for idx, row in enumerate(rows):
            elapsed = int(float(row["elapsed_sec"]))
            if elapsed < MIN_TIME or elapsed > max_time:
                continue
            if len(opens) >= max_trades_per_window:
                break
            vr = volume_ratio(rows, idx)
            if vr is None or vr <= VOLUME_RATIO_THRESHOLD:
                continue
            btc_now = float(row["btc_price"])
            btc_return = (btc_now - open_btc) / open_btc if open_btc > 0 else 0.0
            if btc_return == 0:
                continue
            side = "UP" if btc_return > 0 else "DOWN"
            entry_price = side_price(row, side)
            if not (ENTRY_MIN_PRICE < entry_price <= ENTRY_MAX_PRICE):
                continue
            opens.append((len(opens) + 1, idx, side, entry_price, vr, btc_return))

        for trade_index, idx, side, entry_price, vr, btc_return in opens:
            tp_price = round(min(0.99, entry_price + target_price), 2)
            exit_price = 1.0 if side == winner else 0.0
            exit_reason = "settle"
            exit_elapsed = int(float(rows[-1]["elapsed_sec"]))
            for future in rows[idx + 1:]:
                future_price = side_price(future, side)
                if future_price >= tp_price - 1e-9:
                    exit_price = tp_price
                    exit_reason = "tp"
                    exit_elapsed = int(float(future["elapsed_sec"]))
                    break
            trades.append(
                SimTrade(
                    slug=rows[0]["slug"],
                    path=str(path),
                    target_price=target_price,
                    max_time=max_time,
                    max_trades_per_window=max_trades_per_window,
                    trade_index=trade_index,
                    side=side,
                    entry_elapsed=int(float(rows[idx]["elapsed_sec"])),
                    entry_price=round(entry_price, 4),
                    tp_price=tp_price,
                    exit_elapsed=exit_elapsed,
                    exit_price=round(exit_price, 4),
                    exit_reason=exit_reason,
                    winner=winner,
                    pnl=round(FIXED_SHARES * (exit_price - entry_price), 4),
                    volume_ratio=round(vr, 4),
                    btc_return=round(btc_return, 6),
                )
            )

    wins = sum(1 for trade in trades if trade.pnl > 0)
    losses = sum(1 for trade in trades if trade.pnl < 0)
    tp_hits = sum(1 for trade in trades if trade.exit_reason == "tp")
    summary = {
        "target_price": target_price,
        "max_time": max_time,
        "max_trades_per_window": max_trades_per_window,
        "windows_tested": len(files),
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / len(trades), 4) if trades else 0.0,
        "total_pnl": round(sum(trade.pnl for trade in trades), 4),
        "avg_pnl": round(sum(trade.pnl for trade in trades) / len(trades), 4) if trades else 0.0,
        "tp_hits": tp_hits,
        "tp_hit_rate": round(tp_hits / len(trades), 4) if trades else 0.0,
        "up_trades": sum(1 for trade in trades if trade.side == "UP"),
        "down_trades": sum(1 for trade in trades if trade.side == "DOWN"),
    }
    return summary, trades


def main() -> int:
    files = enriched_files()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []
    best_summary: dict[str, object] | None = None
    best_trades: list[SimTrade] = []

    for target_price in TARGET_PRICES:
        for max_time in MAX_TIMES:
            for max_trades in MAX_TRADES_OPTIONS:
                summary, trades = simulate_config(
                    files,
                    target_price=target_price,
                    max_time=max_time,
                    max_trades_per_window=max_trades,
                )
                results.append(summary)
                if best_summary is None:
                    best_summary = summary
                    best_trades = trades
                    continue
                if (
                    float(summary["total_pnl"]),
                    float(summary["win_rate"]),
                    int(summary["trades"]),
                ) > (
                    float(best_summary["total_pnl"]),
                    float(best_summary["win_rate"]),
                    int(best_summary["trades"]),
                ):
                    best_summary = summary
                    best_trades = trades

    results.sort(
        key=lambda row: (
            float(row["total_pnl"]),
            float(row["win_rate"]),
            int(row["trades"]),
        ),
        reverse=True,
    )

    grid_path = OUT_DIR / "scalp_grid_results.csv"
    with grid_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    best_trades_path = OUT_DIR / "scalp_best_trades.csv"
    with best_trades_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(best_trades[0]).keys()))
        writer.writeheader()
        writer.writerows(asdict(trade) for trade in best_trades)

    top10 = results[:10]
    print(json.dumps(
        {
            "dataset_dir": str(dataset_dir()),
            "windows_tested": len(files),
            "fixed_shares": FIXED_SHARES,
            "best_config": best_summary,
            "top10": top10,
            "grid_results_csv": str(grid_path),
            "best_trades_csv": str(best_trades_path),
        },
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
