# v5.2 continuation capture boundary

This document defines the first v5.2 economic-research boundary without changing production strategy authority.

- Challenger: `roi-convergence-v5.2-continuation-capture-1`
- Measurement epoch: `v52-weekend-review-20260908`
- Incumbent: `roi-convergence-v5.1-context-exactness-1`

## Authority

v5.1 remains the incumbent. v5.2 continuation capture is research-only and has no paper-entry, signing, transaction-submission, live-money, or direct-promotion authority. Historical or weekend-review evidence can motivate what to measure, but it cannot directly promote the challenger.

This boundary is deliberately passive: it adds no production composition hook and does not alter candidate admission, wallet/entity authority, sizing, execution, exits, storage, continuity, certification, providers, or deployment behavior.

## Locked safety controls

The capture boundary preserves the current safety semantics instead of redefining them:

- immediate-copy authority remains bounded at 20 seconds;
- observations after 20 seconds are challenger research only;
- chase above 40% remains observe-only for this challenger boundary;
- an observation is not labeled an executable snapshot without an exact entry quote;
- an observation is not labeled an executable snapshot without an exact exit quote;
- structural exitability remains required;
- all v5.2 outputs remain research-only even when an observation falls inside the incumbent timing/chase envelope.

## Purpose

The boundary makes continuation evidence machine-readable across Pump.fun, Pump AMM/PumpSwap, Raydium, FOMO, and Robinhood Chain so future forward evidence can compare continuation contexts without contaminating the v5.1 validation epoch. Any future production composition, promotion rule, or economic change requires a separate evidence-backed change.
