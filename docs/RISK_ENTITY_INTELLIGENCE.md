# Risk + Entity Intelligence Plane

This layer converts point-in-time Solana evidence into the hard-veto `RiskSnapshot` consumed by ROI Convergence v3.1.

## Forward-cohort safety

The $500 forward portfolio remains disabled. Live Solana swaps are classified in **shadow mode** while the risk/entity plane accumulates evidence. A swap cannot become a paper signal unless every required risk dimension is present, was received before the decision timestamp, and is still fresh.

No private keys, transaction construction, signing, order submission, custody, deposits, withdrawals, or live-money authority exist in this layer.

## Required dimensions

Every token requires all six dimensions:

1. **authority** — mint and freeze authority state;
2. **liquidity** — current USD liquidity and market cap when known;
3. **launch** — bundled-launch and sniper-heavy determinations;
4. **flow** — early-buyer exiting and abnormal sell-pressure determinations;
5. **funding** — early-buyer wallet identities used for economic-entity clustering;
6. **deployer** — deployer wallet identity used for scout/deployer linkage.

Missing or stale evidence returns no risk snapshot and therefore cannot create a paper signal.

## Point-in-time rule

Risk evidence stores both `observed_at`, when the underlying state was observed, and `received_at`, when the system actually received the evidence. A decision at time T can only use rows with `received_at <= T`. This prevents a later risk observation from being retroactively attached to an earlier first touch.

## Entity resolution

Wallet profiles remain the primary explicit economic-entity anchors. The entity graph can add point-in-time links with a relationship, confidence, source, and timestamps.

Only links at or above the configured 0.95 confidence boundary collapse addresses for confirmation purposes. Suspected or low-confidence relationships remain stored but cannot independently veto a confirmation.

The graph is used to detect:

- a confirmation wallet controlled by the scout entity;
- multiple early buyers that resolve to one economic entity/common-funded cluster;
- scout/deployer entity connections.

## Initial evidence-quality policy

These are pre-forward-cohort collection defaults rather than optimized return parameters:

- authority freshness: 60 seconds;
- liquidity freshness: 5 seconds;
- launch freshness: 30 seconds;
- flow freshness: 5 seconds;
- funding freshness: 30 seconds;
- deployer freshness: 60 seconds;
- minimum known liquidity: $1,500;
- when market cap is known, liquidity must also be at least 2% of market cap.

These values can be revised while the system is still collecting shadow evidence. Once the prospective cohort starts, the complete decision policy must be frozen and versioned.

## Current boundary

This build adds the durable composer and entity graph. Concrete collectors that populate authority, liquidity, launch, flow, funding, and deployer evidence are the next integration step. Until those collectors are operational and tested, runtime paper-signal promotion remains false.
