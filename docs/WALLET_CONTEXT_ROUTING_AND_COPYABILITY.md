# Wallet Context Routing and Copyability Architecture

Status: production-shadow research design

This document records the non-FOMO architecture changes that accompany the FOMO work being developed separately. The purpose is to prevent wallet evidence from being transferred across venues, lifecycle stages, roles, or market regimes without prospective proof.

## Objective

The wallet system optimizes for **percentage copyable executable residual return**, not source-wallet dollar profit and not source-wallet headline ROI that occurred before the system could observe the signal.

The governing assignment is:

`wallet/economic entity × venue × lifecycle stage × role × regime`

A wallet has no universal "good wallet" authority. It can earn multiple context roles independently, and it can lose them independently as forward evidence deteriorates.

## Pump.fun execution boundary

Solana ROI Convergence does not target millisecond, first-slot, bundle-positioning, or source-wallet-pre-observation Pump.fun alpha. Any edge that requires those capabilities is structurally disqualified for strategy return attribution.

Pump.fun is retained as an important observation environment for:

- launch discovery;
- early wallet/entity information;
- creator and related-entity behavior;
- residual continuation evidence that survives the system's real observation and execution latency.

A Pump.fun scout can therefore be valuable even when its own earliest trade is not copyable. The scout's inaccessible pre-observation gain never receives strategy credit.

Pump.fun's current documentation confirms that launches begin on a bonding curve and, after graduation, liquidity migrates to PumpSwap. This reinforces the need to distinguish bonding-curve behavior from post-graduation behavior rather than treating the whole token lifecycle as one venue state.

Reference: https://pump.fun/docs/bonding-curve

## Raydium distinction

Raydium contains different liquidity and launch structures from Pump.fun. LaunchLab tokens can begin on a bonding curve and graduate to a Raydium AMM pool, while Raydium also supports established CPMM/CLMM liquidity. Wallet performance must therefore remain segmented rather than inheriting Pump.fun authority.

References:

- https://docs.raydium.io/user-flows/launchlab-overview
- https://docs.raydium.io/introduction/what-is-raydium

## Why early-wallet identity is not enough

Current Pump.fun research identifies persistent cohorts that repeatedly appear among the first buyers, but the updated contamination-adjusted analysis finds that much of the naive apparent buyer-flow effect is selection rather than a clean cohort-specific causal effect. Wallet identity is therefore treated as a predictive feature whose incremental value must be proven, not an automatic trading trigger.

Reference: https://arxiv.org/abs/2607.02795

Current Pump.fun graduation research also shows strong regime dependence and a very low fast-window graduation rate in the observed 2026 sample, reinforcing the need for lifecycle and regime conditioning rather than a universal launch rule.

Reference: https://arxiv.org/abs/2607.02823

Recent Solana rug-pull research finds that early trading behavior can be informative within the first several minutes and that cross-platform data helps mitigate domain shift between PumpFun and Raydium. That supports keeping Pump.fun as an information source even when the system explicitly refuses to compete for millisecond launch execution.

Reference: https://arxiv.org/abs/2608.20271

## Role separation

The router preserves distinct meanings for:

- `scout_alpha` — early discovery value;
- `creator_alpha` — creator/related-entity predictive value;
- `momentum_alpha` — continuation/momentum value;
- `confirmation_alpha` — incremental independent confirmation value;
- `exit_alpha` — exit-timing value;
- `distribution_warning_value` — predictive value of distribution warnings;
- `copyable_return_on_capital` — residual return actually available to this system;
- `signal_decay` — residual return as observed latency increases.

Scout confirmations and momentum/confirmation signals are not interchangeable. Three momentum wallets following one scout do not become four scout confirmations.

Related addresses continue to collapse through the token-specific point-in-time entity model before independence is counted.

## Percentage ROI metrics

Context ranking is based on percentage returns, not dollar PnL. Each context profile records:

- mean residual ROI;
- median residual ROI;
- trimmed mean residual ROI after removing the best 1, 3, and 5 outcomes when sample size permits;
- copyable return on deployed fraction;
- compounded fraction-scaled return;
- positive rate;
- max drawdown on fraction-scaled returns;
- median signal-to-entry delay;
- latency-bucket residual ROI at 1, 2, 5, 10, 20, 30, and 60 seconds.

The trimmed metrics are specifically intended to expose wallets whose apparent edge depends on one or a few extreme winners.

## Structural accessibility

A forward observation is classified as structurally inaccessible when current evidence shows one or more of the following:

- unsupported/unknown venue;
- observation is not copyable;
- observed pipeline time exceeds the unchanged 20-second strategy entry ceiling;
- chase distance exceeds the unchanged strategy maximum.

The production processing target remains 5 seconds. Passing the 20-second operational ceiling does not imply economic viability; the latency residual-return curve determines whether a signal still has positive copyable value.

## Dynamic wallet governance

Historical evidence can prioritize who deserves research bandwidth, but it cannot promote a wallet into strategy authority.

The production-shadow router produces:

- context-specific wallet profiles;
- context-specific role leaders;
- a diversity-aware recommended tracking set constrained by the existing tracking capacity;
- percentage copyable-ROI leaders;
- explicit structural-accessibility accounting.

Those recommendations have **zero tracking-mutation or trading authority**. They are prospective evidence for a future governed cohort. Existing active strategy and tracking authority are not silently mutated.

A later governed promotion cycle may promote or demote wallets separately by context. A wallet can remain a Pump.fun discovery source while losing Raydium continuation value, or remain an exit signal while losing scout value.

## Safety and continuity contracts preserved

This architecture changes no live-money boundary and no existing production continuity/certification threshold:

- paper-only;
- no private keys;
- no signing;
- no transaction submission;
- no live-money authority;
- historical evidence has no promotion authority;
- active strategy is not silently mutated;
- 15% chase boundary unchanged;
- 5-second production processing target unchanged;
- 20-second strategy entry ceiling unchanged;
- 12-second continuity lease unchanged;
- 3 × 1000 real-gap recovery bound unchanged;
- existing RPC workload governor and reserved critical capacity unchanged.

## FOMO interaction

FOMO is intentionally not implemented again in this component. The context router is the boundary that the separate FOMO work can consume: FOMO/momentum evidence can be attached to the correct venue, lifecycle stage, role, and regime without contaminating scout semantics or inheriting authority from another platform.
