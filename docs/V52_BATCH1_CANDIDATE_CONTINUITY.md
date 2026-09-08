# v5.2 Batch 1 — candidate continuity

Batch 1 replaces one-shot challenger candidate handling with a canonical, durable lifecycle identity while preserving the locked v5.1/v5.2 authority boundary.

## Locked authority

- Challenger: `roi-convergence-v5.2-continuation-capture-1`
- Challenger epoch: `v52-weekend-review-20260908`
- Incumbent: `roi-convergence-v5.1-context-exactness-1`
- v5.1 remains the sole incumbent strategy authority.
- Batch 1 is research-only and paper-only.
- It grants no paper-entry authority, signing authority, transaction-submission authority, live-money authority, or challenger-promotion authority.
- It adds no production composition hook.

## Canonical lifecycle

The Batch 1 state machine retains one canonical candidate identity through:

`discovered → developing → pre_breakout → actionable → entered → scaling → partial_exit → runner → exited → reentry_watch`

`reentry_watch → actionable` supports a new continuation/re-entry cycle without creating a new candidate identity.

Position-management states exist here only to preserve lifecycle continuity for later batches. Batch 1 does not make entries, scale positions, or execute exits.

## Venue and route continuity

The same candidate/asset identity retains append-only surface history as evidence moves from Pump.fun through graduation/PumpSwap and later secondary-pool routing. Surface changes never create a replacement candidate.

## Temporary versus permanent rejection

Temporary/contextual rejection does **not** reset a candidate to discovery. It retains:

- last valid lifecycle state;
- blocker reason;
- latest exact executable two-sided quote;
- quote timestamp;
- distance to actionable;
- active event subscriptions.

A temporary rejection cannot advance through the ordinary transition function. Only a subscribed lifecycle event can trigger reevaluation. If the blocker remains, the candidate stays paused in its retained state. If the blocker is resolved, the rejection is cleared and the candidate can resume from that retained state, including a legal state transition carried by the event.

Permanent rejection is terminal. It clears event subscriptions and does not reactivate when later events arrive. The historical lifecycle state is retained for auditability rather than destructively erased.

## Durable research store

`CandidateContinuityStore` provides a minimal SQLite-backed canonical store owned by its caller. It persists candidate identity, lifecycle state, rejection context, surface history, source signatures, and an append-only processed-event audit log. The module does not open or modify production storage on import.

The store enforces one canonical candidate per `asset_id`, preventing the same mint from being silently rediscovered into a parallel challenger identity.

## Event-driven reactivation

Batch 1 defines explicit events for curve progress, buyer acceleration, wallet cascades, liquidity, quote refreshes, graduation, PumpSwap routing, later pool routing, chase resets, blocker resolution, and later lifecycle/position milestones. These are subscription keys only in Batch 1; signal calculation and economic admission remain responsibilities of later v5.2 batches.

## Scope boundary

Batch 1 implements continuity mechanics only. It does **not** implement the later v5.2 batches for early detection, graduation execution, upside capture, detection intelligence, or measurement/governance. Production composition and any future challenger authority require a separate evidence-backed change.