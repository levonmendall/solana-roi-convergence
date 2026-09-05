# Integration contract for canonical v5.1 production

The repository's current production authority is:

- strategy version: `roi-convergence-v5.1-context-exactness-1`;
- economic authority: `roi-convergence-v5.1-consolidated-proof-1`;
- economic freeze epoch: `v51-consolidated-proof-20260905`;
- production ASGI entrypoint: `solana_roi.production:app`;
- final economic composition boundary: `install_v51_production_authority(app, ingestion_runtime)`;
- execution authority: paper-only, with no signer, transaction submission, custody, deposits, withdrawals, or live-money balance authority.

Older v3.1/v4/v5 modules remain only where required for transport, liveness, audit lineage, immutable baseline compatibility, or historical evidence. Import order cannot make those modules the final economic authority.

## Required production integration

1. Observe the configured Solana market scope through direct standard Solana HTTP/WebSocket providers. Legacy Helius compatibility endpoints have no canonical readiness or promotion authority.
2. Register canonical Solana candidates at the normalized scout ingress before risk, quote, strategy, or wallet-research filtering.
3. Preserve append-only candidate stage history through `ingestion → candidate → context → execution_evidence → decision → position → settlement → learning`.
4. Use venue-native executed instruction/transfer proof for Pump.fun, Pump AMM and Raydium candidate attribution. Account-key presence alone is not venue authority.
5. Preserve programIdIndex, address-lookup-table, temporary-token-account, sponsored execution, split quote-leg, compiled SPL/System transfer and fail-closed ambiguity handling.
6. Keep wallet/entity discovery a secondary research consumer with immutable release/economic/measurement/execution lineage; it is not candidate-coverage truth.
7. Keep Robinhood Chain forward-only for strategy opportunity detection. Historical/backfill state is archival only and cannot authorize retrospective entries.
8. Keep Robinhood SQLite ownership inside its isolated worker. The main API thread consumes only cached proof/status payloads.
9. Require amount-specific executable evidence and unsigned/read-only simulation where applicable. Missing execution evidence may create explicit zero-allocation research evidence but cannot create a paper entry or promotion sample.
10. Publish proof freshness, release SHA, economic epoch, measurement epoch, execution-model epoch and proof state on canonical v5.1 proof surfaces.
11. Preserve measurement compatibility: known defective and unclassified historical measurement releases remain auditable but cannot contribute live promotion evidence.
12. Add no private-key, signing, transaction-submission or live-money capability.

## Merge acceptance

Every production change must satisfy all of the following:

- branch begins from the latest canonical `main`;
- exact dependencies are installed from `requirements.lock` in CI;
- canonical architecture and transfer-certification invariants pass;
- wallet/entity regressions pass;
- full repository regressions pass;
- forward-cohort certification regressions pass;
- audit 11–22 closure regressions pass;
- a separate smoke starts the real `uvicorn solana_roi.production:app` process and verifies the v5.1 authority over HTTP;
- `python -m compileall -q src` passes;
- merge is performed against the exact tested PR head SHA;
- post-merge `main` CI is green;
- Render automatically deploys that exact `main` SHA and reaches `live`.

GitHub branch/ruleset administration should require the stable `required-ci` check and pull-request review before `main` is updated. The repository workflow supplies that stable check name; server-side enforcement is a GitHub repository setting.
