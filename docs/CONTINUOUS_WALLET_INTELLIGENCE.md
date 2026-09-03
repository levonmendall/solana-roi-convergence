# Continuous Wallet Intelligence and Adaptive Scout Promotion

## Objective

Run a research lane beside the frozen ROI Convergence forward cohort that continually asks:

> Which Solana wallets are producing the strongest *forward-copyable, risk-adjusted percentage returns now*, how are they doing it, and has any wallet proven superior to a wallet in the active strategy cohort?

The research lane exists so the strategy is not permanently anchored to Jijo, Wugi, The Doc, or any other historical scout.

## Two-lane authority model

### Lane 1 — frozen forward validation

The currently frozen/armed cohort is immutable. Results remain attributable to the exact strategy version, scout set, thresholds, release and point-in-time evidence that produced them.

A challenger wallet can never rewrite an already-running experiment.

### Lane 2 — continuous wallet intelligence

The research lane may continuously discover, score, reject, shadow-test, rank and propose wallets. A wallet that proves materially superior can replace the weakest incumbent **in a new immutable strategy cohort/version**.

This is adaptation without look-ahead contamination.

## What "more successful" means

Raw dollar P&L or headline wallet ROI is insufficient. Promotion evidence must be based on returns the ROI system could plausibly have copied after its own observation latency, chase ceiling, liquidity, fees and execution constraints.

The first promotion policy therefore evaluates:

- forward closed episode count;
- copyable return on deployed capital;
- geometric growth;
- profit factor;
- hit rate;
- maximum drawdown;
- copyability rate after observation/execution delay;
- manipulation risk;
- side-wallet/entity risk;
- median observable entry lag.

A wallet with spectacular raw ROI but non-copyable entries, creator privileges, coordinated side wallets, excessive manipulation risk, or inadequate forward evidence is not promotable.

## Governed promotion rule

A challenger must first clear absolute evidence gates. It is then compared with the weakest incumbent on a risk-adjusted copyable score.

The default first policy requires at least 30 forward episodes, positive copyable return and geometric growth, profit factor above one, at least 80% copyability, no more than 10% manipulation risk, no more than 10% side-wallet risk, and no more than 60% maximum drawdown.

Against the incumbent, the challenger must have no lower profit factor, no materially worse drawdown, and at least a 15% advantage in risk-adjusted copyable score when the incumbent score is positive.

These are research-promotion thresholds; they do **not** alter the frozen v3.1 trade-entry, sizing, exit or 300-trade profitability-certification thresholds.

## Entity independence

Addresses are not assumed to be independent traders. A challenger that resolves to the same economic entity as the incumbent it would replace is blocked. Side-wallet and coordinated-funding evidence remains a veto.

## Adaptation lifecycle

1. Observe broad Solana wallet activity.
2. Build point-in-time wallet performance episodes.
3. Reprice every episode under the ROI system's actual observation and execution constraints.
4. Calculate forward-copyable wallet metrics.
5. Run manipulation, funding, creator and entity-clustering checks.
6. Rank eligible challengers against incumbents.
7. Shadow-test the superior wallet under the same strategy mechanics.
8. If superiority is proven, persist a proposed next cohort and exact rationale.
9. Start that cohort only as a new immutable strategy version/epoch.
10. Continue evaluating both promoted and demoted wallets so later reversals can be discovered.

## Current implementation boundary

`solana_roi.wallet_intelligence.ContinuousWalletIntelligence` now provides the durable point-in-time snapshot ledger, risk-adjusted ranking, fail-closed promotion comparison, incumbent evidence requirement, same-entity veto, and immutable next-cohort proposal record.

The current direct Solana data plane remains optimized for prospective launch coverage and the frozen scouts. Program traffic is deterministically sampled once coverage is sufficient. Therefore the repository must **not claim ecosystem-wide continuous wallet discovery yet**. A broader wallet-outcome acquisition lane must feed forward-copyable snapshots before automatic discovery can be considered production-complete.

That acquisition lane should remain observation-only and paper-only. It must not require a signer, private key, transaction submission or live-money authority.

## Non-negotiable safety and scientific boundaries

- paper-only;
- no private keys, signing or transaction submission;
- no retroactive modification of an active cohort;
- no promotion from raw ROI alone;
- no future information in historical decisions;
- no same-entity confirmation or promotion masquerading as independence;
- missing incumbent/challenger evidence fails closed;
- every cohort change is append-only, versioned and attributable to the evidence that caused it.
