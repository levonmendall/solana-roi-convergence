# Current production architecture — ROI Convergence v5.1

Architecture package release: `0.5.1`

Economic authority and package versioning are intentionally separate. The package release describes repository/runtime architecture; the current economic authority remains:

- authority id: `roi-convergence-v5.1-consolidated-proof-1`;
- strategy version: `roi-convergence-v5.1-context-exactness-1`;
- economic freeze epoch: `v51-consolidated-proof-20260905`;
- machine-readable authority: `strategy_v51_authority.json`.

Historical v3.1/v4 strategy documents are archived and have no current decision authority.

## Objective

Prospectively determine where **executable residual alpha** remains after observation latency, chase, exact amount-specific entry/exit costs, risk state and lifecycle context, while maximizing compounded **paper** growth after costs. Source-wallet headline ROI, deployment health and retrospective backtests are evidence inputs, not profitability authority.

The system is intentionally paper-only. It has no signer, no transaction submission, no custody, no deposit/withdrawal path and no live-money balance authority.

## Final production composition

Render launches:

`uvicorn solana_roi.production:app --host 0.0.0.0 --port $PORT`

`solana_roi.production` installs transport/reliability boundaries first and then composes the frozen v5.1 economic authority at the explicit final boundary in `v51_production_authority.py`. Compatibility modules may repair transport, persistence, measurement or proof publication, but import order cannot make them the final economic authority.

The canonical decision/evidence path is:

`ingestion → candidate → context → execution_evidence → decision → position → settlement → learning`

Candidate coverage is explicit. Missing downstream stages create coverage debt or fail-closed terminal outcomes; they are not silently counted as successful evaluation.

## Surface separation

### Solana

Direct standard Solana HTTP/WebSocket observation is the canonical production data plane. The normal topology uses two independent public providers and an Alchemy mainnet observer to satisfy the prospective continuity quorum. Legacy Helius endpoints are compatibility-only and the legacy Helius background worker is disabled when direct Solana is canonical.

The system observes the configured frozen program scope broadly, but expensive context/risk/execution work is bounded and prioritized. Candidate/scout work has reserved capacity ahead of broad research. Raw continuity and candidate evidence remain durable; operational queues are not substitutes for canonical evidence.

Venue/lifecycle evidence remains distinct:

- `PUMP_FUN`: residual continuation/information, not first-slot millisecond sniping;
- `PUMP_AMM`: graduation and post-graduation continuation;
- `RAYDIUM`: independent native/post-Pump continuation; no pooling with PumpSwap;
- `FOMO`: clean and hazard flow-acceleration cohorts, kept separate for proof and promotion.

Pump.fun → PumpSwap lifecycle research persists explicit transition identity. An exact graduation timestamp is published only when directly observed; otherwise the system records an inferred transition window and does not fabricate exactness.

### Robinhood Chain

Robinhood Chain is an independent forward experimental surface. Decision-authoritative paper evaluation requires a production provider HTTP RPC + WebSocket pair. The public Robinhood RPC/sequencer are research-observation-only and cannot become authoritative by catch-up or historical replay.

Robinhood uses an isolated worker/store boundary. Candidate identity is recorded before venue/lane preselection; concrete forward opportunities receive terminal paper-enter or explicit paper-reject outcomes. Historical/catch-up state has no readiness or retrospective entry authority. Proof publication is consumed through cached/isolated status rather than putting Robinhood SQLite work onto the main Uvicorn event loop.

## Economic authority

The frozen v5.1 authority keeps a hard **20-second maximum observation boundary**, but being inside 20 seconds is not economic approval. Residual edge in the actual latency × chase × execution-cost context still governs.

Chase policy remains:

- baseline context through 15%;
- challenger research 15–25% and 25–40%;
- above 40% observe-only.

Missing exact entry/exit execution evidence, unavailable sell route, transfer restrictions, unexitable liquidity, authority capable of blocking transfer/exit, or a linked entity able to remove required exit liquidity remain mechanical fail-closed conditions.

Probabilistic hazards such as bundling, creator linkage/distribution, sniper concentration, common funding, early-holder distribution and high snipe tax are modeled with higher evidence burdens rather than automatically vetoed.

## Forward proof and promotion

Economic proof is prospective and release/epoch-aware. Known defective or incompatible measurement releases remain auditable but cannot silently contribute current promotion evidence.

Promotion proof includes, as applicable:

- raw and independent outcome counts;
- holdout evidence;
- net ROI and compounded paper NAV;
- expected log growth and lower confidence bound;
- expected shortfall and max drawdown;
- winner concentration;
- top-1/top-3 removal robustness;
- latency and cost sensitivity;
- execution stress;
- promotion/kill state.

Hierarchical pooling can improve estimation but cannot let another wallet/entity or venue grant promotion authority to an exact context that lacks its own required evidence. Venue success is not transferred across PumpSwap, Raydium, FOMO or Robinhood without forward proof.

## Canonical proof and readiness plane

`/v1/system-proof` is the canonical release-bound proof snapshot. It composes release identity, authority, runtime state, candidate coverage, execution evidence, strategy evidence, paper portfolio, settlement, learning and resource health from one shared proof snapshot.

Operational endpoints are deliberately separated:

- `/health` — constant-time process liveness;
- `/v1/liveness` — explicit liveness identity, no SQLite/runtime readiness requirement;
- `/readiness` — deep trading-research readiness;
- `/v1/system-proof` — canonical machine-readable proof;
- `/v1/system-proof/dashboard` — presentation of the same canonical proof.

Render uses liveness. External certification should monitor readiness. A healthy process does not imply economic readiness or profitability.

Canonical proof is precomputed off the Uvicorn event loop and shared by proof/readiness/dashboard requests. Static SQLite schema metadata used by the proof plane is cached and automatically invalidated on schema-version changes.

## Resource and continuity proof

Resource/backpressure proof reports, where available, cycle duration/work counters plus queue depth, oldest pending age, producer/consumer rates, lag, dropped work and retries for Solana ingestion, wallet discovery, risk enrichment, FOMO, Robinhood, proof publication and HTTP.

Persistent producer-over-consumer imbalance, aged backlog or dropped work degrades readiness; it does not make process liveness false.

Restart/continuity proof persists per-subsystem process/worker start identity, restart count/reason, current/previous release, cursor-restore status and the stable continuity epoch. A process restart does **not** create a new economic epoch.

## Storage and point-in-time evidence

Canonical state uses persistent SQLite WAL plus append-only/hash-chained evidence where defined by the subsystem. Historical evidence remains available for audit and compatibility analysis, but current decisions may use only evidence observable at the relevant decision time and compatible with the active release/measurement/execution epoch.

Future wallet performance, later-discovered risk, future prices or retrospective replay cannot rewrite an earlier decision.

## Dependency and release integrity

Production dependencies are installed from exact `requirements.lock`. `/v1/deployment/preflight` publishes the lock SHA-256 and verifies that it matches `dependency_compatibility.json`.

Dependency changes are pull-request-only maintenance. They must regenerate the lock deliberately, update the compatibility review manifest and pass full production-composition/forward-proof CI. A dependency bump cannot silently change economic authority or reuse incompatible measurement evidence.

## Safety invariant

Every architecture/reliability/research change must preserve:

- `paper_only = true`;
- `live_money_authority = false`;
- signing unavailable;
- transaction submission unavailable;
- frozen v5.1 economics unless a separately governed future economic epoch is explicitly created.
