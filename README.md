# Solana ROI Convergence

Architecture package release: **0.5.1**

A **paper-only** forward-validation and economic-certification engine for a continuation-first, risk-conditioned strategy across Pump.fun, PumpSwap, Raydium, FOMO and Robinhood Chain.

## Canonical strategy authority

The current economic authority is **ROI Convergence v5.1 consolidated proof**:

- strategy version: `roi-convergence-v5.1-context-exactness-1`;
- authority id: `roi-convergence-v5.1-consolidated-proof-1`;
- frozen economic evidence epoch: `v51-consolidated-proof-20260905`;
- machine-readable authority: [`strategy_v51_authority.json`](strategy_v51_authority.json);
- certified production entrypoint: `solana_roi.production:app`;
- explicit final economic composition: `v51_production_authority.py`.

Package/architecture versioning is separate from strategy/economic versioning. Historical v3.1/v4 strategy documents are stored under [`docs/archive/`](docs/archive/) with explicit no-authority headers. They do **not** override current v5.1 selection, sizing, promotion, exit-learning or allocation authority.

## Objective

Maximize **forward executable percentage ROI and compounded paper growth after costs**. Source-wallet headline ROI is not enough: the system must prove that residual edge remains after observation latency, chase, exact amount-specific entry/exit costs, risk state and lifecycle context.

## Strategy

- **Pump.fun:** residual continuation/information source, not first-slot or millisecond sniping.
- **PumpSwap / Pump AMM:** first-class graduation and post-graduation continuation across explicit lifecycle windows.
- **Raydium:** isolated native/post-Pump continuation evidence; no success transfer from other venues.
- **FOMO:** clean and hazard cohorts, flow acceleration/exhaustion, capped paper sizing.
- **Robinhood Chain:** independent forward experimental surface with exact chain quote evidence and its own paper outcomes.

Mechanical inability to exit remains a hard stop. Bundling, creator linkage/distribution, sniper concentration, common funding, early-holder distribution, high snipe tax and similar probabilistic hazards are modeled rather than blanket-vetoed. Higher hazard severity requires stronger forward evidence and smaller bootstrap sizing.

The 20-second observation boundary is a **maximum operational ceiling**, not economic approval. A candidate below 20 seconds still has to demonstrate residual edge in its actual latency × chase × execution-cost context. Chase above 15% is a challenger context through 40%; above 40% remains observe-only under the frozen authority.

## Evidence and promotion

The current economic freeze epoch prevents incompatible older measurement releases from silently gaining promotion authority. Historical rows remain durable and auditable, but only evidence compatible with the current economic/measurement/execution lineage can contribute current proof.

Promotion uses hierarchical shrinkage without allowing another wallet/entity or venue to grant promotion authority to a specific context that lacks its own required evidence. Exact and same-entity evidence must satisfy minimum counts, robust expected log growth and leave-best-trade-out profitability. Higher-risk contexts require more evidence. Sufficiently mature contexts with robust negative evidence are killed rather than consuming active paper capital.

The system also measures **incremental wallet alpha** against a matched context model that excludes wallet/entity identity. Wallet identity receives special research priority only if it adds positive forward residual lift.

## Explicit decision pipeline

Canonical evidence follows this auditable path:

`ingestion → candidate → context → execution_evidence → decision → position → settlement → learning`

Candidate coverage reports coverage debt instead of silently treating missing decisions as successful evaluation. Robinhood registers each concrete forward-only v2/v3 opportunity before strategy preselection can return, and each receives either `paper_enter` or an explicit fail-closed `paper_reject`.

## Canonical production proof

The primary production truth surface is:

- `/v1/system-proof` — release-bound canonical proof covering authority, runtime, candidate coverage, execution, strategy evidence, paper portfolio, settlement, learning and resource health.

Operational identity is deliberately split:

- `/health` — constant-time process liveness;
- `/v1/liveness` — explicit liveness identity with no deep SQLite/readiness requirement;
- `/readiness` — deep production trading-research readiness;
- `/v1/system-proof/dashboard` — visual presentation of the same canonical proof snapshot.

Supporting economic surfaces remain available for focused inspection:

- `/v1/strategy/authority` — machine-readable economic rules and safety boundary;
- `/v1/strategy/candidate-coverage` — stage-by-stage attribution and coverage debt;
- `/v1/strategy/economic-certification` — independent N, net ROI, compounded NAV, expected log growth, confidence interval, expected shortfall, drawdown, winner-removal robustness, latency/cost sensitivity and execution stress;
- `/v1/strategy/incremental-alpha` — wallet/entity residual lift versus identity-free matched context;
- `/v1/strategy/research-allocation` — family ranking by forward capital efficiency with cash retained when evidence is immature;
- `/v1/strategy/execution-stress` — combined and mechanism-specific stress diagnostics;
- `/v1/strategy/latency-challengers` — research-only latency/decay evidence with no >20-second entry authority.

No strategy lane is called profitable because deployment is healthy, a source wallet made money, a backtest looked good, or an individual winner was large. Frozen forward evidence must demonstrate executable after-cost compounded alpha and robustness to winner removal and execution stress.

## Resource and continuity proof

Canonical proof includes worker/resource attribution and backpressure indicators for Solana ingestion, wallet discovery, risk enrichment, FOMO, Robinhood, proof publication and HTTP. Persistent producer-over-consumer imbalance, aged backlog or dropped work degrades readiness without redefining process liveness.

Restart proof persists worker/process start identity, restart count/reason, current/previous release, cursor restoration and a stable continuity epoch. A process restart does not manufacture a new economic epoch.

The canonical system proof is precomputed off the Uvicorn event loop and shared by proof/readiness/dashboard requests. Static SQLite schema metadata used by the proof plane is cached and automatically invalidated after DDL.

## Dependency and release integrity

Production installs the exact Python 3.11 set in [`requirements.lock`](requirements.lock). `/v1/deployment/preflight` records its SHA-256 and verifies that it matches [`dependency_compatibility.json`](dependency_compatibility.json).

Dependency updates are PR-only. Dependabot may propose updates, but CI rejects a changed lock until the compatibility manifest is deliberately reviewed and synchronized. See [`docs/DEPENDENCY_UPDATE_POLICY.md`](docs/DEPENDENCY_UPDATE_POLICY.md).

## Safety boundary

This repository intentionally has **no live execution authority**:

- no private keys or seed phrases;
- no signer;
- no transaction submission;
- no custody, deposits or withdrawals;
- no real-money balance authority.

The system may construct unsigned/read-only execution evidence for paper evaluation. Missing critical evidence fails closed.

## Production

Render launches the certified `solana_roi.production:app` entrypoint. Transport/reliability compatibility is composed before the explicit v5.1 economic boundary. Legacy Helius background consumption is disabled when direct Solana is canonical; Robinhood historical catch-up has no readiness or retrospective-entry authority.

The current provider model is represented in `.env.example`: two independent public standard Solana providers plus the Alchemy observer for prospective continuity, and a production HTTP/WebSocket provider pair for decision-authoritative Robinhood paper evaluation. Public Robinhood RPC/sequencer endpoints remain research-only.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps
pytest
uvicorn solana_roi.production:app --reload
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the current runtime composition and [`docs/V51_CONSOLIDATED_STRATEGY.md`](docs/V51_CONSOLIDATED_STRATEGY.md) for the economic design/proof contract.
