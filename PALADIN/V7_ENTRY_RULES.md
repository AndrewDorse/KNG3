# Paladin v7 — when we buy (first legs & layers)

Each market second (`paladin_v7_step`): **(0) hedge** if `pending_second` or material skew → **(1) PM-lead new risk** when flat or when balanced with both legs. There is no separate first-window, VWAP dip ladder, or Binance spike gate in this build.

---

## Shared vocabulary

- **Flat**: no UP and no DOWN inventory.
- **Balanced**: `|size_up − size_down| ≤ balance_share_tolerance` (default 1 share).
- **Both-sided enough for layers**: `min(size_up, size_down) ≥ max(0, min_shares − balance_share_tolerance)`.
- **Lead side**: higher Polymarket mid (`_lead_side`; tie → up).
- **Clip size**: new-risk buys use `base_order_shares` (e.g. 5). Hedges use the same pending-second path as before.
- **Live execution**: v7 buys are limit orders at the target price, canceled after `paladin_v7_limit_order_cancel_seconds`.

---

## 0) Pending hedge (second leg) — always first

**When:** `pending_second` is set, or the book is materially imbalanced (gap sets `pending_second` for the lighter side).

**Cheap / forced:** unchanged — cheap cap with slip, forced at `hedge_timeout_seconds`.

---

## 1) PM-lead new risk — flat (`v7_first_binance_spike`)

**When:** Flat.

**Side / price:** Lead side; buy at that side’s mid.

**Gates:** `mid ≤ first_leg_max_pm`; clip clears `min_notional` / room under `max_shares_per_side`.

**Cooldown:** none on the first open from flat (no `pair_cooldown_sec` wait).

**After fill:** `pending_second` on the opposite side.

---

## 2) PM-lead layer — balanced + both legs (`v7_balanced_btc_spike`)

**When:** Balanced, both-sided enough, not flat.

**Cooldown / window:** `elapsed − last_completed_pair_elapsed ≥ max(pair_cooldown_sec, layer2_cooldown_sec)` (with mins 5s / 1s on those params). No new layers in the last `no_new_layers_last_seconds` of the window.

**Dip:** Lead mid must satisfy `mid_lead ≤ avg_lead − 0.07` (7¢ under that leg’s held VWAP).

**Band:** Lead mid in `[balanced_entry_min_pm, balanced_entry_max_pm]` (default 20¢–80¢) and `≤ first_leg_max_pm`.

**After fill:** `pending_second` on the opposite side.

---

## Hedge after any first leg

Same §0 rules for hedges after flat entry or balanced layer.

---

*Source of truth: `PALADIN/paladin_v7.py` → `paladin_v7_step`.*
