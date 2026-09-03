# Integration contract for current production release

Observed deployed baseline before this change:

- release: `74b218cb32de656611a7c46cbbcf995aa9f4cd74`
- strategy: `roi-convergence-v3.1-forward-1`
- continuity healthy (`continuity_ok=true`, `unresolved_gap=false`)
- process-wide RPC governor installed
- wallet research isolated as workload `research`
- v3.1 active cohort mutation disabled
- current risk/entity plane connected

## Required repository integration

The GitHub connector was unavailable in the build session, so exact current source filenames for the wallet discovery/intelligence owner could not be fetched. The merge session must identify that owner and wire the new engine without source-text guessing.

The production adapter should:

1. Instantiate `ProfitFirstEntityStrategy` in the existing **wallet research process**, never in the critical continuity/hydration path.
2. Feed point-in-time entity links from the existing risk/entity plane into `EntityGraph`.
3. On every fresh copyability-eligible wallet/token observation, create an `OpportunitySnapshot` using only evidence available at that timestamp.
4. Emit shadow trials for every eligible lane on the same chronological observation.
5. Record realized residual net return from the first executable quote available to the system; do not use the source wallet's privileged fill.
6. Persist `ForwardOutcome` rows append-only and release-bound.
7. Expose `profit_first_entity_strategy` status alongside `wallet_intelligence` and `wallet_discovery`.
8. Keep current `strategy_version=roi-convergence-v3.1-forward-1` until a governed v4 cohort/manifest is explicitly armed. Do not silently replace it at import time.
9. Preserve the current RPC workload governor and tag all additional reads as `research`.
10. Add no signing, submission or live-money capability.

## Merge acceptance

- current main SHA checked before branch creation;
- no equivalent v4 strategy already landed;
- existing continuity/capacity tests remain green;
- focused v4 tests green;
- full repository CI green;
- paper-only/no-signing tests green;
- production status after deploy reports the exact merged release;
- continuity remains healthy before any v4 paper cohort is armed.
