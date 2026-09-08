# v5.2 Batch 4 — Upside Capture Mechanics

## Scope

Batch 4 implements the v5.2 challenger mechanics for:

1. starter positions;
2. evidence-confirmed pyramiding;
3. fresh two-sided amount-specific requotes before every scale-in;
4. liquidity-adjusted maximum position size;
5. staged de-risking;
6. a persistent runner state for exceptional winners;
7. second-leg / profitable re-entry after a genuinely new continuation event.

This batch is **research/test-only**. It does not alter v5.1 authority, production composition, strategy thresholds, signing, transaction submission, or live-money capability.

## Economic-parameter boundary

The architecture is implemented without freezing production percentages from hindsight.

`UpsideCapturePolicy` requires explicit experiment inputs for starter size, scale increment, staged de-risk fractions, runner fraction, and minimum exit-depth coverage. Batch 4 itself does not promote any of those values to production authority.

## Starter contract

A starter:

- must be smaller than or equal to the intended target;
- is capped by current executable sell depth;
- requires exact amount-specific buy and sell quotes;
- requires structural exitability;
- remains inside the existing 20-second hard operational ceiling;
- remains observe-only when candidate-relative chase is above 40%.

## Scale-in contract

A scale-in is allowed only when:

- the position is already open;
- genuinely new forward evidence appeared after entry;
- at least one strength-evidence flag is positive;
- continuation remains healthy;
- hazards remain acceptable for scaling;
- current price is not below the last add price, so the engine does not average down;
- a fresh exact buy quote covers the increment;
- a fresh exact sell quote covers the full resulting position;
- executable sell depth supports the resulting size.

No scale-in is authorized simply because price declined.

## Liquidity-adjusted sizing

The maximum modeled position is capped by:

`min(target_notional, executable_sell_depth / coverage_ratio, exact_sell_quote_notional)`

This keeps portfolio target size subordinate to what can actually be exited at the decision instant.

## Staged de-risking

Deterioration can be triggered by:

- attention decay together with weakening price structure;
- seller-pressure deterioration;
- hazard deterioration.

The first deterioration event takes the configured first partial realization. Persistent deterioration takes the configured second partial realization. A structural hard stop requests a full exit immediately, still subject to exact executable sell evidence.

## Runner state

After staged de-risking, the remaining position may transition to `runner` only while:

- continuation remains healthy;
- independent participation persists;
- seller pressure remains controlled;
- hazards remain acceptable;
- attention decay is not active;
- no structural hard stop is present.

The runner retains only the experiment-configured fraction of the original target. When runner conditions fail, the engine requests a full exit and leaves the candidate in `reentry_watch`.

## Second-leg / re-entry contract

An exited candidate is never forgotten. Re-entry requires a new impulse identifier plus the complete sequence:

`consolidation -> new independent buyers -> liquidity expansion -> renewed acceleration`

The new event must independently satisfy the same exact two-sided quote, structural exitability, latency, liquidity, and candidate-relative chase gates as a fresh starter. Reusing the prior impulse is rejected. A >40% current-impulse chase remains observe-only.

## Five-lane applicability

The mechanics are validated across the canonical research lanes:

- Pump.fun
- Pump AMM / PumpSwap
- Raydium
- FOMO
- Robinhood

This does not grant any lane new production entry authority.

## Acceptance gates

Batch 4 is technically complete only when:

1. focused starter/sizing tests pass across all five canonical lanes;
2. scaling tests prove new-evidence gating, no averaging down, fresh two-sided requotes, and liquidity caps;
3. position-management tests prove two-stage de-risking, runner retention, and runner exit;
4. re-entry tests prove new-impulse requirements and >40% observe-only behavior;
5. safety-manifest tests prove v5.1 remains authoritative and v5.2 remains research-only/paper-only;
6. the repository's full required CI gate passes on the PR head;
7. the PR is merged to `main`;
8. the full required CI gate passes again on the exact post-merge `main` SHA.

Technical completion of Batch 4 is not economic promotion of v5.2. Forward challenger comparison remains required.
