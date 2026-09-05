# Audit recommendations 23–34 closure

This release strengthens evidence validity without changing the frozen v5.1 economic authority, entry rules, sizing rules, exit rules, hazard multipliers, promotion thresholds, kill thresholds, signing boundary, transaction-submission boundary, or paper-only scope.

## 23. Live-release measurement attestation

A current release no longer receives promotion authority merely because its SHA is current. It begins unattested. Solana, FOMO, and Robinhood earn surface-specific attestation from their primary production tables. Attestation does not depend on an API/status poll.

## 24. Audit certification and promotion certification are separate

`/v1/strategy/economic-certification` remains the frozen-epoch audit view and may include historical/non-promotable measurements. `/v1/strategy/promotion-certification` and `/v1/strategy/research-allocation` consume only live-attested, measurement-compatible promotion evidence.

## 25. Independent-event clustering

Promotion evidence is clustered by family + token + lifecycle. Repeated wallet touches/trials around the same token/lifecycle do not count as independent outcomes. This is deliberately conservative when a token has several correlated touches.

## 26. Locked evidence partition and multiple-testing control

Each event cluster receives a stable precommitted discovery/validation/holdout partition. Discovery evidence has no promotion authority. Promotion certification requires validation and holdout evidence and applies Benjamini-Hochberg false-discovery control across simultaneous family claims. The partition is deterministic from immutable event identity and cannot be changed after seeing returns.

## 27. Unified percentage execution cost

A canonical cost ledger stores round-trip execution cost as fraction of notional. FOMO cost is recovered from the amount-specific unified `profit_first_final_trials.round_trip_cost_fraction` instead of being left in the `unknown` bucket. The ledger remains paper/execution-evidence only.

## 28. Rejected-opportunity counterfactual ledger

Every terminal non-entry found in canonical candidate state is materialized for forward counterfactual evaluation. When an existing shadow outcome later exists, the rejection is resolved without creating a retrospective paper entry. Robinhood forward rejects are also materialized inside its private worker store. Retrospective entry authority is permanently false.

## 29. Hazard calibration

Forward entered outcomes and resolved rejected counterfactuals are summarized by clean/low/moderate/high/extreme hazard bins. This is diagnostic evidence for a future economic epoch only. Current hazard evidence burdens and bootstrap multipliers do not change.

## 30. Cross-family correlation proof

Promotion-compatible event clusters are aligned by UTC settlement day across Pump AMM, Raydium, FOMO, Pump.fun and cached Robinhood evidence. Correlation is reported only after a minimum aligned-period count; unknown correlation is never treated as zero.

## 31. Maturity-aware allocation proof

The active frozen family cap remains 25%. The proof reports whether a family could someday qualify for the authority's permanent 50% ceiling only when promotion proof, mature correlation evidence, and material execution stress all support it. This diagnostic cannot raise the active cap.

## 32. One-capital-base portfolio reconciliation

Chronological paper entries share one unit of capital. Overlapping requested fractions are capped by available cash and shortfalls are reported. Family NAVs are not summed as if they each owned an independent portfolio. When isolated-store entry time cannot be safely surfaced, the proof reports settlement-time fallback explicitly instead of fabricating precision.

## 33. Forward-proof SLO

Candidate stage events now support candidate→context, context→execution-evidence, and execution-evidence→decision latency summaries plus oldest coverage debt, settlement backlog, and recent stage-event throughput. Coverage debt older than the proof SLO degrades proof state.

## 34. Architecture retirement

The superseded `v51_final_production_install.py` import-order hook is deleted. Final economic authority remains the explicit `install_v51_production_authority(app, ingestion_runtime)` call at the end of production composition. A retirement registry records other compatibility layers that are not yet safe to delete.

## Safety and merge acceptance

The release is acceptable only if focused 23–34 regressions, the launched-production HTTP smoke, architecture invariants, wallet/entity regressions, full repository regressions, forward-cohort certification regressions, and source compilation all pass. The exact tested head must be merged, post-merge `main` CI must pass, and Render must deploy that exact SHA automatically.
