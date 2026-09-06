# ARCHIVED — Solana ROI Convergence v4

> Historical strategy record only. This document has **no current selection, sizing, promotion, exit, allocation, readiness, or live-money authority**. Current production authority is `roi-convergence-v5.1-consolidated-proof-1`; see `README.md`, `docs/V51_CONSOLIDATED_STRATEGY.md`, and `strategy_v51_authority.json`.

---

# Solana ROI Convergence v4 — Profit-First Entity-Aware Research Strategy

Strategy version: `roi-convergence-v4.0-profit-first-entity-research-1`

## Objective

Maximize expected compounded **paper** return after observable latency, executable entry/exit costs and adverse selection. Wallet headline PnL is evidence, not the objective.

## Core change

Creator/deployer/side-wallet involvement is no longer an automatic rejection. Instead, related addresses are collapsed to economic entities and creator state is treated as a predictive feature. Profit measurement starts at the first entry the system could actually execute after observing the signal.

## Parallel lanes

1. `clean_scout`
2. `unfiltered_elite_wallet` benchmark
3. `creator_aware_continuation`
4. `smart_money_swarm`
5. `unified_profit_maximizer` selects the eligible lane with the highest positive empirical expected log growth.

All lanes are shadow/paper research until their own forward residual-return evidence is sufficient.

## Entity rules

- Addresses linked by point-in-time funding/control evidence collapse to one entity.
- Creator-side wallets do not count as independent confirmations.
- Unrelated profitable wallets do count.
- The engine never invents an entity relationship; the existing risk/entity plane supplies links.

## Risk treatment

Creator association, bundles, sniper concentration and related-wallet behavior are **features/risk premiums**, not automatic vetoes.

Hard rejection is reserved for structural inability to realize the trade, including no executable entry/exit route, transfer restrictions, unexitable liquidity, or authority capable of blocking transfer/exit.

Existing 15% chase and 20-second observation-lag boundaries are retained in this research implementation.

## Residual-return model

Every forward outcome is measured from the system-observable executable entry through the strategy exit, net of execution costs. Outcomes are bucketed by:

- lane,
- independent economic-entity confirmations,
- chase distance,
- creator cluster accumulation/neutral/distribution state,
- early-buyer exit state,
- soft risk state.

The model uses exact feature cohorts where sufficient, then hierarchical lane cohorts. It does not assign a fabricated expected return to unseen conditions.

## Sizing

Sizing is not fixed at 0.5%. The research engine evaluates a paper-only grid from 0.5% through 20% and selects the fraction with the highest empirical expected log bankroll growth. This is a research output, not live authority.

## Governance preserved

- current v3.1 active cohort is not silently mutated;
- paper-only;
- no private key;
- no signing;
- no submission;
- direct Solana data plane unchanged;
- no Helius dependency;
- 12-second continuity lease unchanged;
- 3 × 1000 recovery bound unchanged;
- certification thresholds unchanged.

The v4 strategy should be activated only as a new governed future strategy/cohort after repository integration and forward proof.
