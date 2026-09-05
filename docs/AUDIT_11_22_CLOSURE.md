# Audit recommendations 11–22 closure

This document records the operational closure contract for audit recommendations 11–22 on top of the canonical ROI Convergence v5.1 economic authority.

The economic authority remains `roi-convergence-v5.1-consolidated-proof-1`, strategy version `roi-convergence-v5.1-context-exactness-1`, economic freeze epoch `v51-consolidated-proof-20260905`. Nothing in this closure grants signing, transaction submission, custody, withdrawal, or live-money authority.

## 11. Helius is not canonical readiness authority

Canonical Solana intake and readiness use direct standard Solana HTTP/WebSocket providers plus Jupiter execution evidence. Legacy Helius modules may remain in repository history or compatibility code, but absence or state of Helius cannot make the canonical v5.1 strategy ready, profitable, promoted, or executable.

## 12. Root documentation and dependency reproducibility

`README.md`, `INTEGRATION.md`, package metadata, and CI must describe the current v5.1 production boundary. CI installs the repository's exact tested dependency lock before installing the editable package without dependency re-resolution.

## 13. Launched-production end-to-end smoke

CI must start the real `uvicorn solana_roi.production:app` process, wait for the HTTP surface, query canonical strategy authority over TCP, and assert paper-only/no-live-money semantics. Import-only or TestClient-only checks do not satisfy this item.

## 14. Main protection / required CI

The repository publishes a stable `required-ci` job on pull requests and pushes to `main`. GitHub branch/ruleset administration must require this check and PR review before merge. The repository code can provide the stable check and document the required rule; the GitHub account administrator owns the server-side protection setting.

## 15. Venue-native candidate graph

Pump.fun, Pump AMM and Raydium candidate attribution must use same-venue instruction/transfer graphs with `programIdIndex` support, outer and inner instructions, sponsored/relayed execution, proven trade direction, fail-closed ambiguity, durable continuation opportunity state, and no ledger entry authority.

## 16. Venue graph is primary candidate authority

Scout wallet token-balance deltas are not a prerequisite when the supported venue graph proves actor flow. Address lookup table accounts, temporary token accounts/ownership hints, sponsored execution and split quote legs remain supported, with ambiguous actor flow rejected.

## 17. Compiled transfer decoding and coordinated Robinhood throttling

Compiled SPL Token/System transfer instructions remain decodable in the venue graph. Failed scout attribution publishes bounded sanitized shape diagnostics only. Robinhood public-RPC HTTP 429 handling respects `Retry-After`, coordinates a shared cooldown/backoff, retries the same read, and does not silently skip block ranges.

## 18. Actual executed venue invocation is required

A supported program address merely appearing in transaction account keys is not venue proof. Candidate venue source authority requires an actual executed supported-program invocation, including indexed/ALT resolution; account-key-only pseudo-sources remain diagnostic-only.

## 19. Robinhood historical catch-up cannot authorize current entries

Robinhood strategy evaluation is prospective. Historical cursor/backfill state is archival/research lineage only and cannot create retrospective paper entries or be described as current strategy readiness.

## 20. Current-context shadow evidence remains observable without fabricating execution

When context is useful but exact executable evidence is absent, the system may retain explicit zero-allocation/current-context research evidence. Missing execution evidence cannot be converted into a paper entry, profitable outcome, or promotion sample.

## 21. Robinhood proof remains isolated and nonblocking

Robinhood's private SQLite store remains owned by its isolated worker. The main API thread consumes only the worker's cached proof/status payload. No main-thread Robinhood SQLite read is introduced for proof composition.

## 22. Production proof is bound to actual live call sites

Candidate proof scheduling is attached to the actual economic scout normalizer. Pump WebSocket frontier proof requires real mapped WebSocket provenance paired with exact durable SQLite identity. Synthetic/import-time markers cannot substitute for those production boundaries.

## Merge acceptance

A closure release is mergeable only when:

- the branch starts from the latest canonical `main`;
- focused 11–22 regressions pass;
- the existing full repository regression suite passes;
- the launched-production smoke passes;
- `python -m compileall -q src` passes;
- paper-only/no-signing/no-submission invariants remain true;
- the exact merged `main` release becomes live on Render without a manual deploy override.
