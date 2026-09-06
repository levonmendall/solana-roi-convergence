# Audit 47–58 — Prospective Alpha Validation & Promotion Readiness

This phase begins only after the 35–46 measurement/proof architecture. Its purpose is to answer a different question: **does frozen v5.1 have independently validated, executable, after-cost positive compounded alpha?**

The authority remains `roi-convergence-v5.1-consolidated-proof-1`, strategy `roi-convergence-v5.1-context-exactness-1`, economic epoch `v51-consolidated-proof-20260905`. No threshold, entry/exit rule, chase rule, latency rule, hazard burden, allocation cap, signing, transaction submission, custody, or live-money authority is changed by this phase.

## 47–58 gates

47. **Production transport continuity.** Consumes the already-fail-closed 35–46 forward certificate. Transport or measurement degradation prevents any alpha claim.
48. **Prospective candidate-flow completeness.** Uses the canonical append-only candidate/stage plane plus the isolated Robinhood candidate ledger. A system with no current forward flow cannot prove alpha merely from historical rows.
49. **Rejected-opportunity counterfactual resolution.** Every rejected candidate remains visible until an exact forward outcome resolves it. Pending rejects remain evidence debt and never receive retrospective entry authority.
50. **Settlement and realized execution proof.** Authorized paper entries must have complete settlement lineage. Open/unsettled entries remain explicit debt; zero settled entries is insufficient alpha evidence.
51. **Locked validation/holdout accumulation.** Discovery evidence remains non-authoritative. A family must accumulate real validation and locked-holdout event clusters.
52. **Independent-event statistical proof.** Uses the existing token/lifecycle clustering, robust expected-log analysis and Benjamini-Hochberg FDR control. Raw transactions do not substitute for independent event clusters.
53. **Family promotion engine.** Reports each family as discovery, validation, holdout, promoted, degrading or killed from existing frozen-v5.1 proof only. The lifecycle is read-only and cannot mutate strategy authority.
54. **Adaptive wallet/entity intelligence.** Ranks entity identity only by forward percentage-return residual relative to matched context—not dollar profit—and retains family/context separation. A challenger recommendation has no automatic paper-trade authority.
55. **Research capital vs promoted capital.** Reports separate one-capital-base NAVs for promotion-compatible observations and research/probe observations, plus combined audit NAV. This is accounting separation, not a new allocation instruction.
56. **Portfolio-level scaling proof.** Reuses cross-family correlation/maturity evidence and one-capital-base reconciliation. The active immature-family cap remains frozen; future permanent-ceiling eligibility remains diagnostic until a future economic epoch explicitly changes authority.
57. **Promotion degradation and kill proof.** Mature negative growth or the existing kill profile becomes visible as degrading/killed evidence state. This phase does not silently remove or resize an active family.
58. **Forward Alpha Certificate.** `after_cost_positive_compounded_alpha_proven=true` requires all operational/candidate/settlement/counterfactual/statistical gates, at least one valid locked-holdout family promotion claim, and positive promoted-strategy one-capital-base paper ROI.

## Fail-closed state semantics

The certificate may return:

- `operationally_degraded` — transport/measurement continuity is not trustworthy;
- `collecting_candidate_evidence` — no sufficient current prospective candidate flow;
- `collecting_settlement_evidence` — accepted entries are not yet fully represented by settled outcomes;
- `resolving_rejected_counterfactuals` — rejected opportunities still lack exact future outcomes;
- `collecting_validation_holdout` — no family has earned the frozen-v5.1 promotion claim;
- `alpha_not_yet_proven` — evidence exists but the complete after-cost alpha proof still fails;
- `forward_alpha_proven` — all required gates are simultaneously satisfied.

Engineering closure does not require `forward_alpha_proven`. Real market observations, settlements and counterfactual outcomes must accumulate prospectively. A missing result is evidence debt, not an invitation to manufacture or backfill favorable observations.

## Robinhood isolation

The isolated Robinhood worker now publishes its economic audit rows inside the existing deep-copied proof cache. The main API can therefore reconcile research/promoted NAV and entity attribution across Solana, FOMO and Robinhood without reading the private Robinhood SQLite database from Uvicorn.

## Safety invariants

Every 47–58 proof surface declares:

- `paper_only=true`;
- `live_money_authority=false`;
- signing unavailable;
- transaction submission unavailable;
- `changes_strategy_authority=false`;
- `changes_economic_thresholds=false`.

The 47–58 phase is proof and learning infrastructure, not v5.2.
