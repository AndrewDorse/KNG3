# PALADIN — Strategy Core (Immutable)

**Authority:** This document defines the non-negotiable rules of the PALADIN strategy. Implementation details may evolve; these principles change **only** when you explicitly instruct an update to this file.

---

## Market & cadence

- **Instrument / window:** Trade **BTC** on the **15-minute** window (each 15m session as the operational unit).

## Positioning philosophy

- **Both sides:** We **buy both sides** (long and short / YES and NO — per whatever the bot’s contract model is). This is an **inventory-building** strategy: we accumulate exposure on both legs according to the rules below, not a one-sided directional bet.

## Improvement & rebalancing

- **Buy when it helps:** Add size **when price is good** — i.e. when the fill **improves** the position (better average, better economics vs. current marks and limits).
- **Then fix skew:** After improving one leg, **reduce imbalance** by **rebuilding the other side** (bring exposure back toward balance within the allowed disbalance band).

## Data & loop

- **Price feed:** Use **WebSocket** for live price monitoring (no polling HTTP for primary prices unless WebSocket is unavailable and you add an explicit fallback in code — not in this doc).
- **Decision cadence:** **Every second**, recompute intended actions from **current positions**, **current prices**, and **limits** (risk, size, exchange/bot caps — whatever “limits” means in code must align with this cadence).

## Balance tolerance

- **Allowed disbalance:** Positions may deviate from perfect symmetry by the **lesser** of:
  - **~5 shares**, or
  - **25% of the largest single-leg position** (by share count),

  Treat this as the band inside which we do not force an immediate full rebalance unless other limits or risk rules require it.

## Order size (clips)

- **Minimum clip:** **5 shares** (venue / bot floor).
- **Scaling:** First orders use **5 shares**; once inventory is established, clips may **scale up** (e.g. **7**, **8**, …) for rebalancing and inventory adds, subject to limits and notional checks.

## Profit lock (stop trading until resolution)

- When the position is **fully profitable** under **either** threshold below, **stop trading** (no new orders for inventory building) and **wait for window resolution**:
  - **ROI:** At least **10% minimum ROI on both settlement branches** (both ROIs ≥ 10%) — but this ROI stop applies only once **each leg has at least 20 shares** (avoids locking on tiny positions where percentages are noisy).
  - **Dollar P&L:** At least **$5 profit on each side** (each leg meets ≥$5).

  Rationale: at that point the book is **100% profitable** at resolution under the chosen measure; do not risk giving it back by continuing to trade.

---

*End of core strategy. Agents and contributors must treat this file as the source of truth for PALADIN behavior.*
