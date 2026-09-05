# V5.1 Consolidated Strategy and Proof Contract

## Authority

`strategy_v51_authority.json` is the canonical machine-readable economic specification. The active authority id is `roi-convergence-v5.1-consolidated-proof-1` and the economic freeze epoch is `v51-consolidated-proof-20260905`.

Economic selection, sizing, promotion, kill, exit-learning and allocation rules are frozen for the evidence epoch. Reliability fixes may be made only when they do not change those economics. A change to the economics requires a new freeze epoch; older outcomes remain audit evidence and cannot silently promote the changed strategy.

The system remains paper-only with no signing, transaction submission, private-key or live-money authority.

## Market thesis

The system does not attempt first-slot Pump.fun sniping. It looks for residual executable continuation edge after the system can observe the market. PumpSwap graduation/post-graduation, isolated Raydium continuation, clean/hazard FOMO and Robinhood Chain are separate research families.

Hazards are not automatically losses. Bundling, creator linkage/distribution, sniper concentration, common funding, early-holder distribution, high snipe tax and similar probabilistic warnings increase the evidence burden and reduce bootstrap size. A mechanically unavailable exit, transfer restriction, unavailable exact quote or equivalent structural untradeability remains a hard stop.

## Latency and chase

Twenty seconds is the maximum operational latency boundary, not a profitability threshold. Being observed at 19 seconds does not make a trade economically valid. Bootstrap sizing decays as latency, chase and round-trip cost worsen, and mature promotion remains context-specific. The 15% chase level is a baseline; 15–25% and 25–40% are challenger contexts. Above 40% is observe-only during this epoch.

## Hierarchical evidence

Exact context evidence is retained, but the system no longer treats every 11-dimensional cell as statistically independent from all nearby cells. Same-entity parent evidence may provide capped shrinkage support. Cross-entity family evidence may be used for diagnostics/prior information but cannot satisfy promotion minima for a specific wallet/entity.

Promotion requires both exact evidence and sufficient independent same-entity evidence, positive shrunk expected log growth above the hazard-specific hurdle, and positive leave-best-trade-out mean. Hazard-specific minimum counts and growth hurdles are in the authority JSON.

## Kill criteria

A context is killed only after sufficient independent forward evidence and all of the following are non-positive: shrunk expected log growth, leave-best-trade-out mean and the 95% upper confidence bound of mean return. Killed contexts receive no active paper allocation; a small research floor may remain for detecting regime recovery without re-granting authority automatically.

## Candidate pipeline

Every canonical economic opportunity follows:

1. ingestion
2. candidate
3. context
4. execution evidence
5. decision
6. position
7. settlement
8. learning

Solana and FOMO are reconciled from their durable observation/trial/outcome records. Robinhood instruments the existing `_maybe_open_v2/_maybe_open_v3` path when a candidate reaches canonical lane selection, so no second polling path is introduced. A candidate selected for a lane but not opened is explicitly classified as an exact-quote / executable-round-trip cost failure. A candidate with no viable lane is rejected with the corresponding evidence/exposure reason.

## Incremental wallet alpha

Wallet/entity identity is not assumed to be the source of returns. Outcomes are matched against contexts that keep lane, venue, lifecycle, regime, risk signature, flow state, chase, latency and execution-cost bands while excluding entity identity. Wallet-specific research priority requires positive forward residual lift versus peer entities in the same matched context.

## Capital-efficiency ranking

Research families are ranked using forward expected log growth adjusted for evidence maturity, drawdown and expected shortfall. The initial deterministic tie-break research priority is:

1. PumpSwap / Pump AMM
2. Raydium
3. clean FOMO
4. hazard FOMO
5. Pump.fun continuation
6. Robinhood Chain

Actual mature forward capital efficiency overrides this tie-break. Unknown correlation is not assumed to be zero. While correlation/evidence is immature, each family is capped at 25% and the remainder stays in paper cash; permanent family ceiling is 50%.

## Economic certification

The certification surface reports closed N, independent event N, sum of net returns, compounded NAV using actual paper fractions, expected log growth, 95% confidence interval, expected shortfall, drawdown, top-1/top-3/top-5 winner-removal results, latency sensitivity, execution-cost sensitivity, and execution-stress scenarios.

Execution stress deliberately worsens latency, round-trip costs, adverse selection and failure probability. This quantifies the paper-to-live uncertainty but does not grant live execution authority or claim that paper results equal real-money results.

## Architecture boundary

Legacy `*_repair.py` modules remain compatibility internals where removing them immediately would risk production continuity. They may transport observations or preserve old audit surfaces. Render keeps the separately certified `solana_roi.production:app` ASGI entrypoint; `v51_final_production_install.py` wraps the final existing Robinhood production-installer boundary so the consolidated v5.1 strategy is applied after every Solana/FOMO/Robinhood compatibility installer has composed. New economic behavior must be expressed through the canonical authority/economic core rather than by adding another independent strategy wrapper.
