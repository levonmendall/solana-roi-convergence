# Five-Phase Certification Architecture

This document is the canonical implementation contract for the September 2026 V4 certification program. It does not grant live-money authority and does not change the frozen V3.1 strategy thresholds, 20-second entry ceiling, 12-second continuity lease, bounded recovery policy, or paper-only boundary.

## Phase 1 — Architecture consolidation

Production authority is direct standard Solana WebSocket/RPC plus Jupiter amount-specific order construction and unsigned simulation. Helius webhook code is retained only as opt-in compatibility. It is not required for production readiness, cannot promote a cohort, and its worker is disabled unless `SOLANA_ROI_LEGACY_HELIUS_COMPAT_ENABLED=true` is explicitly set.

The production planes are:

1. **Market observation** — full frozen program/scout observation, minimal normalization, durable receipts and bounded prospective recovery.
2. **Wallet/entity intelligence** — cheap broad discovery, historical screening and point-in-time entity research. Historical evidence has zero promotion authority.
3. **Prospective alpha** — release-bound V4 forward evidence from current observations only. Old-release rows are never replayed as new forward authority.
4. **Paper execution/certification** — amount-specific Jupiter order, unsigned Solana simulation, paper outcome, execution-transfer stress and profitability certification.

`/v1/architecture/status` reports the active contract.

## Phase 2 — Data-path efficiency

The existing certification research architecture and PR96–PR99 production composition remain authoritative:

- raw observation remains full-scope and durable;
- candidate/recovery work has priority over broad historical research;
- historical discovery yields under measured event-loop/continuity pressure;
- forward wallet tracking remains active;
- candidate hydration is work-conserving and bounded by actual RPC capacity;
- urgent real-gap recovery retains the existing fixed lease and hard recovery bound;
- background research cannot gain paper or live authority.

The final Render bootstrap invokes `install_final_certification_failure_accounting()`, which is the exact-release composition point for the universal continuity, candidate scheduling/RPC fairness, final realtime V4 handoff, and failure-accounting installers.

## Phase 3 — V4 scientific instrumentation

V4 remains release-bound and paper-only. The existing final research adapter persists:

- immutable evidence epochs tied to strategy version and release commit;
- five parallel observation rows: Clean Scout Alpha, Elite Wallet Continuation, Creator/Insider Continuation, Entity-Flow Momentum and Unified Profit Maximizer;
- point-in-time token-specific entity context;
- first-system-observable executable entry economics;
- forward-only outcomes;
- exit-alpha observations;
- signal-decay buckets;
- lane/regime robustness.

Parallel rows are research comparisons, not independent episodes. Certification counts independent policy-selected Unified Profit Maximizer episodes and cannot multiply the denominator by the five lanes.

## Phase 4 — Execution transferability

`V4ExecutionTransferCertification` adds a stricter, read-only transferability layer over the final V4 tables.

For each policy-selected closed episode it requires complete paper execution inputs and applies an additional conservative failed-attempt stress equal to one extra *observed entry-fee equivalent*. This uses recorded Jupiter/Solana fees rather than an invented percentage. It also reports 1/2/5/10/20-second delayed-entry price-path stress from later recorded price marks.

Delayed price marks are diagnostic only. They are explicitly not asserted executable fills and have zero promotion authority. They show whether residual alpha decays before a plausible later landing without pretending that a spot mark equals a live transaction fill.

The existing Jupiter order and unsigned simulation remain the execution evidence source. No private key, signing or transaction submission is introduced.

## Phase 5 — Profitability certification

`/v1/certification/profitability` is the authoritative V4 profitability proof surface.

Certification is fail-closed and currently requires at least:

- 300 policy-selected closed Unified Profit Maximizer episodes;
- 300 independent token episodes;
- positive aggregate net P&L;
- positive geometric growth;
- profit factor above one;
- Wilson lower hit-rate bound above the frozen break-even threshold;
- positive P&L after removing the best trade;
- positive P&L after removing the best wallet, best economic entity and best token;
- positive P&L after removing the top 5% of winning episodes;
- complete execution-transfer inputs for every selected episode;
- positive execution-stressed P&L and geometric growth;
- execution-stressed profit factor above one;
- no policy-selected episode beyond the unchanged 20-second entry ceiling.

The endpoint also reports performance by latency band, regime and trigger entity, plus 1/2/5/10/20-second delayed-entry diagnostics.

A `certified=true` result is evidence that the paper strategy passed the defined prospective and execution-stressed tests. It is **not** live-money authority, a guarantee of future profit, or permission to sign/submit a transaction.
