# v5.2 Batch 3 — Five-Lane Positive/Negative E2E and Synthetic Isolation

## Scope

Batch 3 certifies the existing canonical seeded five-lane capability harness across:

1. Pump.fun
2. Pump AMM / PumpSwap
3. Raydium
4. FOMO
5. Robinhood Chain

This batch does **not** create a second E2E generator, alter v5.1 strategy economics, or add production composition. The existing `v51_lane_capability_e2e` and `v51_seeded_e2e` paths remain the source of the synthetic cases. Batch 3 adds a research/test-only verifier around their persisted evidence.

## Acceptance contract

For every lane, Batch 3 requires:

- one qualifying synthetic case that reaches the canonical seeded paper settlement and learning stages;
- one legitimate negative case that ends in an explicit paper rejection and never silently disappears;
- immutable synthetic provenance registered on the isolated `SEEDED_E2E` surface;
- zero eligibility for certification or promotion;
- zero contribution to canonical profitability/statistical tables.

The ten synthetic cases are reconciled through the Batch 2 lossless-accounting boundary. Qualifying cases are terminally accounted as `exited`; negative cases are terminally accounted as `permanently_rejected`. Any unexplained disappearance remains a hard blocker.

## Cross-lane economic-event accounting

Batch 3 separates an economic event identity from a lane observation identity. Multiple lanes may observe the same economic event, but the event is counted once economically while every lane observation remains independently visible. Exact duplicate lane observations are invalid and fail closed.

This is validation infrastructure only. It does not change production strategy scoring, candidate routing, sizing, or settlement.

## Synthetic isolation

The verifier checks the known canonical economic/statistical surfaces for any Batch 3 synthetic candidate identifier. A matching synthetic identifier in any supported canonical trial/outcome table is treated as contamination and blocks Batch 3 readiness.

The canonical seeded harness already writes only to the synthetic provenance registry and candidate-pipeline audit surface. Batch 3 verifies that boundary instead of granting synthetic rows any economic authority.

## Lane-specific invariants

### FOMO

FOMO continuation remains shadow/research-only for this batch. Its positive and negative synthetic cases must remain promotion-ineligible, and Batch 3 has no challenger entry authority or production composition hook.

### Robinhood

Robinhood synthetic provenance must survive end to end with `economic_surface=ROBINHOOD_CHAIN` and `venue=UNISWAP_V3` for both the qualifying and rejection cases.

## Authority boundary

Batch 3 preserves all existing safety invariants:

- v5.1 remains authoritative;
- v5.2 remains research/test-only;
- paper-only operation;
- no signing;
- no transaction submission;
- no live-money authority;
- no production composition hook;
- no strategy-authority changes;
- no economic-threshold changes.

## Completion criteria

Batch 3 is technically complete only after:

1. focused Batch 3 regressions pass;
2. the repository's complete required CI gate passes on the exact PR head;
3. the PR is merged to `main`;
4. the complete required CI gate passes again on the exact merge SHA.

Technical completion of Batch 3 does not itself promote v5.2 and does not constitute live/prospective economic certification of the strategy.
