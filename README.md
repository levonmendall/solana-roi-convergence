# Solana ROI Convergence

A **paper-only** forward-validation system for a continuation-first, risk-conditioned strategy across Pump.fun, PumpSwap, Raydium, FOMO and Robinhood Chain.

## Canonical strategy authority

The current economic authority is **ROI Convergence v5.1 consolidated proof**:

- strategy version: `roi-convergence-v5.1-context-exactness-1`;
- authority id: `roi-convergence-v5.1-consolidated-proof-1`;
- frozen economic evidence epoch: `v51-consolidated-proof-20260905`;
- machine-readable authority: [`strategy_v51_authority.json`](strategy_v51_authority.json);
- production entrypoint: `solana_roi.v51_production:app`.

Older v3.1/v4/v5 documents, scout cohorts, tables and repair modules remain only where needed for audit, evidence lineage or transport/reliability compatibility. They do **not** override the canonical v5.1 selection, sizing, promotion, exit-learning or allocation authority.

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

The consolidation deliberately starts a new economic freeze epoch because its latency-decay, hazard-burden and hierarchical-evidence rules change economic behavior. Pre-epoch evidence remains durable and auditable but has no promotion authority in this epoch.

Promotion uses hierarchical shrinkage without allowing another wallet/entity to grant promotion authority to a specific entity. Exact and same-entity evidence must satisfy minimum counts, robust expected log growth and leave-best-trade-out profitability. Higher-risk contexts require more evidence. Sufficiently mature contexts with robust negative evidence are killed rather than consuming active paper capital.

The system also measures **incremental wallet alpha** against a matched context model that excludes wallet/entity identity. Wallet identity receives special research priority only if it adds positive forward residual lift.

## Explicit decision pipeline

Canonical evidence follows this auditable path:

`ingestion → candidate → context → execution_evidence → decision → position → settlement → learning`

Candidate coverage reports coverage debt instead of silently treating missing decisions as successful evaluation. Robinhood instruments its existing lane-selection path rather than adding a second polling system.

## Economic certification

The canonical proof endpoints are:

- `/v1/strategy/authority` — machine-readable economic rules and safety boundary;
- `/v1/strategy/consolidation` — installed authority/freeze epoch;
- `/v1/strategy/candidate-coverage` — stage-by-stage attribution and coverage debt;
- `/v1/strategy/economic-certification` — independent N, net ROI, compounded NAV, expected log growth, confidence interval, expected shortfall, drawdown, winner-removal robustness, latency/cost sensitivity and execution stress;
- `/v1/strategy/incremental-alpha` — wallet/entity residual lift versus identity-free matched context;
- `/v1/strategy/research-allocation` — family ranking by forward capital efficiency with cash retained when evidence is immature;
- `/v1/strategy/execution-stress` — modeled deterioration from extra latency, cost, adverse selection and execution failure.

No strategy lane is called profitable because deployment is healthy, a source wallet made money, a backtest looked good, or an individual winner was large. The frozen forward evidence must demonstrate executable after-cost compounded alpha and robustness to winner removal and execution stress.

## Safety boundary

This repository intentionally has **no live execution authority**:

- no private keys or seed phrases;
- no signer;
- no transaction submission;
- no custody, deposits or withdrawals;
- no real-money balance authority.

The system may construct unsigned/read-only execution evidence for paper evaluation. Missing critical evidence fails closed.

## Production

The Render Blueprint runs one web service with a persistent SQLite disk. Standard Solana RPC/WebSocket ingestion, Jupiter amount-specific quotes, adaptive wallet research, FOMO and Robinhood Chain are composed under the canonical v5.1 production entrypoint.

The legacy Jijo/Wugi/The Doc public cohort remains configured only for baseline/audit and compatibility ingestion. It is not final strategy authority.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pytest
uvicorn solana_roi.v51_production:app --reload
```

See [`docs/V51_CONSOLIDATED_STRATEGY.md`](docs/V51_CONSOLIDATED_STRATEGY.md) for the design and proof contract.
