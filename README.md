# Solana ROI Convergence

Architecture package release: **0.5.1**

A **paper-only** forward-validation and economic-certification engine for a continuation-first, risk-conditioned strategy across Pump.fun, PumpSwap/Pump AMM, Raydium, FOMO and Robinhood Chain.

## Canonical strategy authority

The current economic authority is **ROI Convergence v5.2 max-profit/max-confidence**:

- strategy version: `roi-convergence-v5.2-continuation-capture-1`;
- authority id: `roi-convergence-v5.2-authoritative-1`;
- baseline economic epoch: `v52-authoritative-cutover-20260908`;
- canonical profit/confidence revision: `v52-direct-max-profit-confidence-20260910`;
- machine-readable authority: [`strategy_v52_authority.json`](strategy_v52_authority.json);
- profit/confidence policy: [`strategy_v52_profit_confidence_completion.json`](strategy_v52_profit_confidence_completion.json);
- certified production entrypoint: `solana_roi.production:app`;
- explicit final economic composition: [`src/solana_roi/v52_production_authority.py`](src/solana_roi/v52_production_authority.py).

v5.1 is retained only as a read-only control/compatibility substrate for transport, evidence, exact execution, paper capital, settlement and comparison. It does **not** have final economic decision authority. Package/architecture versioning is separate from strategy/economic versioning.

v5.2 supports governed continuous evolution. The baseline epoch is therefore a lineage anchor, not a prohibition on later validated forward changes. Protected strategy changes still require forward validation, and the paper-only/no-signing/no-submission/no-live-money boundary is immutable.

## Objective

Maximize **forward executable percentage ROI and compounded paper growth after costs**. Source-wallet headline ROI is not enough: the system must prove that residual edge remains after observation latency, chase, exact amount-specific entry/exit costs, risk state, portfolio competition and lifecycle context.

## v5.2 strategy and portfolio

The canonical lanes are:

- **Pump.fun:** `elite_wallet_continuation` on the bonding curve; no first-slot Pump.fun sniping.
- **Pump AMM / PumpSwap:** `graduation_continuation` in early post-graduation continuation.
- **Raydium:** `raydium_cross_venue_persistence` after migration, with isolated venue evidence.
- **FOMO:** a dedicated continuation lane with independent paper outcomes and capped sizing.
- **Robinhood Chain:** `robinhood_entity_continuation` on new WETH pools with exact chain quote evidence and its own position lifecycle.

Mechanical inability to exit remains a hard stop. Bundling, creator linkage/distribution, sniper concentration, common funding, early-holder distribution, high snipe tax and similar probabilistic hazards are modeled rather than blanket-vetoed. Higher hazard severity requires stronger forward evidence and smaller bootstrap sizing.

The canonical small-account policy is built around a **$500 paper NAV**. The profit/confidence completion enforces a three-position small-account limit, portfolio risk/exposure constraints, a reserve floor, global capital competition, correlation discount and replacement hurdle. Position sizing remains subordinate to lane caps and exact execution evidence.

The hard observation/execution latency ceiling is **20 seconds**. v5.2 also imposes an absolute signal-age ceiling and final chase guards. Exact amount-specific entry and exit quotes are required, final sizing is re-quoted, exact exit-depth coverage must be at least 2x, averaging down is disabled, and no layer may create signing, submission or live-money authority.

v5.2 adds evidence-confirmed scaling, staged de-risking, dynamic runners, second-leg/re-entry logic and learned lane-specific decay while preserving structural hard stops and final numeric lane caps.

## Wallet intelligence

Wallet intelligence is part of v5.2, but wallet identity is not treated as free alpha.

Wallet discovery is prospective and forward-only. Newly discovered wallets begin without automatic economic authority, entity clustering prevents related wallets from being counted as independent confirmation, and wallet/entity signals must demonstrate incremental forward value in comparable context.

The active authority records wallet lead outcomes and uses contextual wallet-alpha weighting only after sufficient forward samples. Partial wallet evidence may affect ranking/utilization but cannot independently grant entry authority. Same-candidate-stream attribution is required so wallet contribution can be separated from the base strategy rather than credited with returns that the non-wallet strategy would have captured anyway.

## Continuous learning and governance

v5.2 is the single paper economic authority and includes governed continuous evolution. Forward promotion/demotion, learned decay, wallet distribution reversal, challenger generation and same-stream tournaments are composed under the production authority.

Historical outcomes cannot grant v5.2 promotion. Promotion requires fresh same-stream forward evidence, and the governance hardening prevents old forward evidence from being reused to manufacture the next promotion. Challenger evidence remains analytical until its promotion requirements are satisfied.

The machine-readable authority currently requires at least 30 paired forward episodes for automatic evolution, an improvement ratio of at least 1.05, posterior probability of at least 0.95, and drawdown within the governed limit. These are governance requirements, not permission to weaken protected execution or safety boundaries.

## Explicit decision pipeline

Canonical evidence follows this auditable path:

`ingestion → candidate → context → execution_evidence → decision → position → settlement → learning`

Candidate continuity and lossless-accounting work preserve candidate identity across lifecycle/venue transitions and distinguish temporary blockers from permanent rejection. Candidate coverage must report unexplained disappearance or coverage debt rather than silently treating missing decisions as successful evaluation.

## Backtest and replay readiness

v5.2 materially improves backtestability compared with the older v5.1 README description. The current authority already includes point-in-time signal records, forward wallet outcomes, counterfactual rejected-opportunity settlement, staged position/exit records, provider/reliability economics, same-candidate-stream attribution requirements, global $500 capital competition, and read-only 24-hour/7-day performance reporting.

However, **the repository is not yet documented or certified as having a complete arbitrary 24h/7d/30d deterministic historical replay boundary**. A valid backtest must not infer missing point-in-time facts from present-day state or use eventual blockchain truth before the system could have observed it.

### Readiness assessment at canonical main `92c0c1620f78116e7ecbeade039e9aedaf3a51a9`

| Requirement | Status | v5.2 assessment |
| --- | --- | --- |
| Exact strategy/config identification | Present | `strategy_v52_authority.json`, profit/confidence policy and release/epoch lineage identify the active authority and governed parameters. |
| $500 portfolio competition | Present | Canonical profit/confidence policy explicitly uses a $500 paper NAV and bounded concurrent positions/risk. |
| Same-stream comparison semantics | Present | v5.2 governance requires same-candidate-stream control comparison and same-candidate-stream attribution. |
| Forward-only wallet measurement | Present | Wallet discovery/lead outcomes are prospective; historical identity is not allowed to manufacture promotion authority. |
| Rejected/missed-opportunity accounting | Present | v5.2 includes counterfactual rejected-opportunity settlement, MFE/MAE learning and avoided-loss/opportunity-cost accounting. |
| Execution/exit economics | Substantial but not replay-certified | Exact amount-specific quotes, latency/chase controls, staged fills, exit depth and provider/reliability economics exist, but complete export/replay coverage for every historical decision has not been certified. |
| Complete candidate/rejection chronology | Substantial but not replay-certified | Candidate continuity and lossless accounting exist, but an immutable replay bundle proving every required row for an arbitrary historical window is not yet part of the backtest contract. |
| Point-in-time wallet-off vs wallet-on A/B runner | Partial | Same-stream attribution is required, but a deterministic offline runner that masks wallet inputs while holding every non-wallet input identical is not yet a certified artifact. |
| Deterministic offline portfolio replay | Missing as a certified boundary | The system has forward paper accounting and performance reporting, but no documented immutable offline replay contract for arbitrary windows. |
| Immutable evidence export + integrity manifest | Missing as a certified boundary | Backtest inputs need a bounded, read-only, hashable export with completeness/chronology checks before numerical results are considered authoritative. |
| 24-hour report | Present | Read-only v5.2 performance surface exists. |
| 7-day report | Present | Read-only v5.2 performance surface exists. |
| 30-day report | Not yet documented as present | A 30-day replay/report must be added or certified before the requested 30-day backtest is authoritative. |

### Successful backtest contract

A backtest is considered defensible only when all of the following are true:

1. The experiment pins the exact release commit, v5.2 authority fingerprint, active strategy epoch/configuration and evidence-bundle hash. This freezes the **experiment**, not ongoing strategy development.
2. The requested horizon is exported through a bounded, read-only evidence path that cannot mutate production state and does not require history-scaled production HTTP responses.
3. Every strategy-observed candidate has a deterministic chronological disposition, including rejection, timeout, provider failure, execution failure, entry, exit and end-of-window open state where applicable.
4. Replay orders information by when it became available to the system (`available_at`/received chronology plus a deterministic sequence), not by hindsight or eventual market truth.
5. Amount-specific entry/exit quotes, route/liquidity/depth, fees, slippage, latency, partial fills/failures and settlement evidence are linked to the decision they informed.
6. Portfolio state is reconstructable at every decision so cash, reserved capital, open positions and simultaneous candidates compete for the same $500 rather than reusing capital.
7. Wallet-off and wallet-on receive the identical non-wallet candidate stream and market/execution evidence. Wallet-off masks wallet-derived features; wallet-on receives only wallet evidence that was actually available at that timestamp.
8. Evidence completeness fails closed. Missing critical point-in-time candidate, wallet, quote, execution, settlement or chronology evidence produces `UNVERIFIABLE` for the affected result rather than an invented return.
9. Replaying the same immutable bundle with the same configuration produces identical trade-ledger and result hashes.
10. Each run publishes an integrity manifest, evidence coverage report, strategy-only trade ledger, wallet-on trade ledger, equity curves, missed/rejected-opportunity ledger and machine-readable summary for the 24h, 7d or 30d horizon.

### Required backtest infrastructure changes

The next implementation boundary is **evidence/replay infrastructure, not a strategy rewrite**:

- create a canonical bounded backtest evidence exporter over the authoritative append-only/paper stores;
- preserve or join complete point-in-time decision inputs, including wallet state and amount-specific execution evidence;
- add a deterministic offline $500 portfolio replay engine;
- add strict wallet-off vs wallet-on same-stream A/B execution;
- add integrity/completeness certification and immutable run manifests;
- add/certify the 30-day reporting path and standard 24h/7d/30d artifacts.

These additions must not alter v5.2 selection thresholds, sizing authority, risk rules, exits, continuous-learning governance or paper-only safety merely to produce a favorable backtest.

### Historical salvage rule

Backtest infrastructure added today cannot recreate point-in-time facts that were never persisted. Older windows may therefore be classified as **historical salvage** with explicit evidence-coverage percentages. Missing wallet state, quotes, candidate visibility or execution facts must remain missing; they must not be reconstructed from present-day quality scores or later market outcomes.

## Canonical production proof

The primary production truth surface is:

- `/v1/system-proof` — release-bound canonical proof covering authority, runtime, candidate coverage, execution, strategy evidence, paper portfolio, settlement, learning and resource health.

Operational identity is deliberately split:

- `/health` — constant-time process liveness;
- `/v1/liveness` — explicit liveness identity with no deep SQLite/readiness requirement;
- `/readiness` — deep production trading-research readiness;
- `/v1/system-proof/dashboard` — visual presentation of the same canonical proof snapshot.

Supporting economic surfaces include:

- `/v1/strategy/authority` — machine-readable economic rules and safety boundary;
- `/v1/strategy/candidate-coverage` — stage-by-stage attribution and coverage debt;
- `/v1/strategy/economic-certification` — independent N, net ROI, compounded NAV, expected log growth, confidence interval, expected shortfall, drawdown, winner-removal robustness, latency/cost sensitivity and execution stress;
- `/v1/strategy/incremental-alpha` — wallet/entity residual lift versus identity-free matched context;
- `/v1/strategy/research-allocation` — family ranking by forward capital efficiency with cash retained when evidence is immature;
- `/v1/strategy/execution-stress` — combined and mechanism-specific stress diagnostics;
- `/v1/strategy/latency-challengers` — research-only latency/decay evidence;
- `/v1/strategy/v52/performance/24h` — read-only v5.2 24-hour performance/economic attribution;
- `/v1/strategy/v52/performance/7d` — read-only v5.2 7-day performance/economic attribution.

No strategy lane is called profitable because deployment is healthy, a source wallet made money, a backtest looked good, or an individual winner was large. Forward evidence must demonstrate executable after-cost compounded alpha and robustness to winner removal, execution stress and portfolio competition.

## Resource and continuity proof

Canonical proof includes worker/resource attribution and backpressure indicators for Solana ingestion, wallet discovery, risk enrichment, FOMO, Robinhood, proof publication and HTTP. Persistent producer-over-consumer imbalance, aged backlog or dropped work degrades readiness without redefining process liveness.

Restart proof persists worker/process start identity, restart count/reason, current/previous release, cursor restoration and continuity lineage. A process restart does not manufacture a new economic epoch or authorize hindsight.

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

Render launches the certified `solana_roi.production:app` entrypoint. Transport/reliability compatibility is composed before the explicit v5.2 economic boundary. v5.2 owns the final Solana, FOMO and Robinhood economic decision surfaces while v5.1-named components may remain as compatibility transport/evidence/storage substrate.

The current provider model is represented in `.env.example`. Provider availability and reliability are economic inputs because missed/late evidence can create opportunity cost, but provider health cannot independently authorize a paper trade.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps
pytest
uvicorn solana_roi.production:app --reload
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for runtime composition. Machine-readable v5.2 economic authority is defined in [`strategy_v52_authority.json`](strategy_v52_authority.json), with the max-profit/max-confidence policy in [`strategy_v52_profit_confidence_completion.json`](strategy_v52_profit_confidence_completion.json).
