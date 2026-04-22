#!/usr/bin/env python3
"""
Run N research cycles: wallet-informed candidate PALADIN specs on 35 BTC windows,
promote baseline when portfolio ROI improves (tie-break: higher sum PnL).

Outputs:
  exports/strategy_cycle_state.json   — current baseline spec + version
  exports/strategy_cycle_history.json — full cycle log

Usage (from repo):  python PALADIN/strategy_cycle_runner.py
                     python PALADIN/strategy_cycle_runner.py --cycles 20
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from strategy_cycle_lib import (
    btc_windows_35,
    default_pl_params,
    initial_baseline_spec,
    propose_candidates,
    research_target_wallet,
    run_paladin_on_windows,
    run_wallet_on_windows,
    save_json,
    spec_from_dict,
    spec_to_dict,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = REPO_ROOT / "exports" / "strategy_cycle_state.json"
HISTORY_PATH = REPO_ROOT / "exports" / "strategy_cycle_history.json"
TARGET_REF_ROI = 4.066 / 100.0  # wallet portfolio ROI on 35w (reference only)


def _better(m_new: dict[str, float], m_old: dict[str, float]) -> bool:
    if m_new["portfolio_roi"] > m_old["portfolio_roi"] + 1e-15:
        return True
    if abs(m_new["portfolio_roi"] - m_old["portfolio_roi"]) <= 1e-15:
        return m_new["sum_pnl"] > m_old["sum_pnl"] + 1e-9
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycles", type=int, default=20)
    ap.add_argument("--reset", action="store_true", help="Ignore saved state; start from initial_baseline_spec")
    args = ap.parse_args()

    windows = btc_windows_35()
    if len(windows) != 35:
        raise SystemExit(f"need 35 BTC public windows, got {len(windows)}")

    pl_params = default_pl_params()
    wallet_metrics = run_wallet_on_windows(windows)
    research = research_target_wallet(windows)

    history: list[dict[str, Any]] = []
    if STATE_PATH.is_file() and not args.reset:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        baseline = spec_from_dict(state["baseline_spec"])
        version = int(state.get("version", 0))
        promotion_count = int(state.get("promotion_count", 0))
        if HISTORY_PATH.is_file():
            try:
                history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                history = []
    else:
        baseline = initial_baseline_spec()
        version = 0
        promotion_count = 0
        history = []

    baseline_metrics = run_paladin_on_windows(baseline, windows, pl_params)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for cycle in range(1, args.cycles + 1):
        idea, candidates = propose_candidates(baseline, cycle, research)
        best_spec = None
        best_m: dict[str, float] | None = None
        for cand in candidates:
            m = run_paladin_on_windows(cand, windows, pl_params)
            if best_m is None or _better(m, best_m):
                best_spec = cand
                best_m = m

        promoted = False
        winner_key = ""
        if best_m is not None and _better(best_m, baseline_metrics):
            baseline = best_spec  # type: ignore[assignment]
            baseline_metrics = best_m
            version += 1
            promotion_count += 1
            promoted = True
            baseline = spec_from_dict(
                {
                    **spec_to_dict(baseline),
                    "key": f"baseline_v{version}",
                    "label": f"Promoted cycle {cycle} | ROI={100*baseline_metrics['portfolio_roi']:.3f}% WR={100*baseline_metrics['win_rate_wl']:.2f}%",
                }
            )
            winner_key = baseline.key

        gap_to_target_pct = 100.0 * (TARGET_REF_ROI - baseline_metrics["portfolio_roi"])

        history.append(
            {
                "run_id": run_id,
                "cycle": cycle,
                "research_theme": idea,
                "wallet_reference": {
                    "portfolio_roi_pct": 100.0 * wallet_metrics["portfolio_roi"],
                    "win_rate_pct": 100.0 * wallet_metrics["win_rate_wl"],
                    "sum_pnl": wallet_metrics["sum_pnl"],
                    "sum_spent": wallet_metrics["sum_spent"],
                },
                "research_snapshot": deepcopy(research),
                "candidates_tried": len(candidates),
                "promoted": promoted,
                "winner_key": winner_key,
                "baseline_after": {
                    "key": baseline.key,
                    "portfolio_roi_pct": 100.0 * baseline_metrics["portfolio_roi"],
                    "win_rate_pct": 100.0 * baseline_metrics["win_rate_wl"],
                    "sum_pnl": baseline_metrics["sum_pnl"],
                    "sum_spent": baseline_metrics["sum_spent"],
                    "n_win": int(baseline_metrics["n_win"]),
                    "n_loss": int(baseline_metrics["n_loss"]),
                },
                "gap_to_wallet_roi_pct": gap_to_target_pct,
                "spec": spec_to_dict(baseline),
            }
        )

        save_json(
            STATE_PATH,
            {
                "updated_utc": run_id,
                "version": version,
                "promotion_count": promotion_count,
                "baseline_spec": spec_to_dict(baseline),
                "baseline_metrics_35w": {k: float(v) for k, v in baseline_metrics.items()},
                "wallet_metrics_35w": {k: float(v) for k, v in wallet_metrics.items()},
                "target_wallet_roi_pct_reference": 100.0 * TARGET_REF_ROI,
            },
        )
        save_json(HISTORY_PATH, history)

    # Console report
    print("Strategy cycle run:", run_id, "| cycles:", args.cycles)
    print(f"Wallet (35w) reference: ROI={100*wallet_metrics['portfolio_roi']:.3f}% WR={100*wallet_metrics['win_rate_wl']:.2f}%")
    print(f"Final baseline v{version}: ROI={100*baseline_metrics['portfolio_roi']:.3f}% WR={100*baseline_metrics['win_rate_wl']:.2f}% sum_pnl={baseline_metrics['sum_pnl']:.2f}")
    print(f"Promotions this run: {sum(1 for h in history if h.get('run_id')==run_id and h.get('promoted'))} (total promotions ever in file: {promotion_count})")
    print()
    for h in history[-args.cycles :]:
        if h.get("run_id") != run_id:
            continue
        c = h["cycle"]
        pr = "PROMOTE" if h["promoted"] else "hold"
        b = h["baseline_after"]
        print(
            f"Cycle {c:2d} [{pr:7s}] ROI {b['portfolio_roi_pct']:6.3f}% | WR {b['win_rate_pct']:5.2f}% | "
            f"PnL {b['sum_pnl']:7.2f} | tried {h['candidates_tried']:2d} | gap to wallet ROI {h['gap_to_wallet_roi_pct']:6.3f}pp"
        )
        theme = h["research_theme"].encode("ascii", "replace").decode("ascii")
        print(f"         theme: {theme[:120]}{'...' if len(theme) > 120 else ''}")

    print()
    print("State:", STATE_PATH.relative_to(REPO_ROOT))
    print("History:", HISTORY_PATH.relative_to(REPO_ROOT))
    print()
    print(
        "Note: wallet ROI uses full notional (~$47k over 35w); PALADIN uses $80/window cap. "
        "Matching ~4% portfolio ROI under the cap may be infeasible; gap shows distance to wallet reference."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
