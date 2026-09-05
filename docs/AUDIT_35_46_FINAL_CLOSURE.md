# Audit 35–46 — Final Closure

This closure repairs production proof composition and measurement plumbing discovered only after the first 35–46 release was exercised against live Render. It does not change frozen v5.1 economics or grant any live-money capability.

## Production defects closed

1. **Cross-surface certificate composition.** The first certificate rebuilt evidence only from the canonical Solana/FOMO store. Isolated Robinhood proof was therefore absent from release attestation, promotion evidence, forward SLOs, rejected-opportunity counterfactuals, hazard diagnostics, correlation and portfolio reconciliation. The final certificate consumes the already-published isolated Robinhood proof cache and combines it with canonical evidence while keeping Robinhood transport readiness as an independent gate.

2. **Rejected-opportunity accounting.** Robinhood had thousands of durable rejected candidates while check 43 incorrectly reported zero because it could not see the isolated store. The final cross-surface counterfactual proof sums local and Robinhood counts. Pending rejected candidates now correctly prevent final certification until their future outcomes resolve.

3. **Append-only Robinhood stage telemetry.** Robinhood coverage still wrote to the legacy current-state audit helper. It now writes through `v51_candidate_ledger.record_stage_event`, producing immutable `v51_candidate_stage_events` plus current-state compatibility rows. Existing history is not fabricated or retrospectively timestamped; only new forward transitions use the append-only measurement path.

4. **Empty-epoch measurement semantics.** The isolated proof now initializes the append-only candidate-stage schema before computing its SLO. An epoch with no new events can therefore report a valid measurement plane with zero recent flow instead of falsely claiming the measurement schema is unavailable. Zero forward flow remains evidence debt and does not satisfy check 40.

5. **Robinhood proof refresh cost.** Rejected counterfactual materialization previously reprocessed every durable Robinhood rejection on every proof refresh. It is now incremental: only previously unseen rejects are inserted, existing rows are not rewritten, and any future resolution already attached to a counterfactual is preserved. This removes repeated O(N) SQLite work from the isolated worker proof path.

6. **Post-deploy HTTP budget.** Live proof aggregation can legitimately exceed 10 seconds while reading durable evidence across planes. The manual post-deploy probe now uses a 60-second per-request budget. This changes verification timeout only; it does not relax any transport, measurement, evidence, or economic gate.

## Fail-closed semantics retained

- Solana, FOMO and Robinhood transport readiness remain independent hard gates.
- A stale or incompatible Robinhood proof cannot satisfy release attestation or forward-SLO proof.
- No recent candidate stage events yields `collecting_forward_evidence` once the measurement plane itself is valid; it is never converted into synthetic activity.
- No family is promoted unless the existing frozen-v5.1 validation/locked-holdout claim is valid.
- Unresolved rejected-opportunity counterfactuals keep check 43 and final check 46 false.
- Hazard calibration remains diagnostic only.
- Correlation maturity cannot raise the active frozen family cap.
- All portfolio proof remains one shared paper capital base.
- `paper_only=true`, `live_money_authority=false`, signing unavailable and transaction submission unavailable remain invariant.

## What “finished” means

The 35–46 engineering/proof architecture is finished when this exact code passes full CI and the deployed exact release can expose the cross-surface certificate without a software/measurement plumbing defect. The final certificate may still correctly remain `collecting_forward_evidence` while real prospective validation, holdout, settlement and rejected-counterfactual outcomes accumulate. That evidence cannot be manufactured by engineering closure.
