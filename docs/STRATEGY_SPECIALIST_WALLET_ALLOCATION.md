# Strategy-Specialist Wallet Allocation

This layer aligns Solana wallet tracking with the PR #120 risk-conditioned v5 strategy model.

## Objective

Scarce high-priority wallet tracking is no longer allocated only by a single global ROI leaderboard. The allocator first preserves minimum specialist coverage for active strategy/regime combinations, then gives any remaining challenger capacity to the strongest forward ROI contexts globally.

The exact assignment metadata is:

`strategy × venue × lifecycle × regime × role × risk signature`

Success in one context does not automatically transfer to another.

## PR #120 alignment

The allocator consumes both the existing wallet-context router and the new v5 forward outcome surfaces:

- `elite_wallet_continuation`
- `creator_insider_continuation`
- `entity_flow_momentum`
- `graduation_continuation`
- `raydium_cross_venue_persistence`
- `hazard_continuation`
- FOMO clean and hazard contexts

Pump.fun first-slot/millisecond sniping remains outside strategy capability. PumpSwap lifecycle segmentation and Raydium post-Pump isolation remain unchanged.

## Allocation order

1. Preserve immutable/current incumbent tracking required for experimental continuity.
2. Reserve one challenger specialist for each mature positive strategy × regime surface when capacity permits.
3. Treat clean FOMO and hazard FOMO as separate specialist surfaces.
4. Preserve a bounded observation-only slot for an immature hazard/FOMO specialist when available, so bundled, creator-linked, or otherwise risky alpha is not silently discarded before it can be measured.
5. Fill all remaining challenger capacity by robust forward ROI.
6. Use ordinary tracking candidates only as bootstrap observation slots if capacity remains.

If specialist demand exceeds the configured challenger capacity, the uncovered strategy/regime surfaces are published as `coverage_debt`; the system does not claim those contexts are covered.

## Risk boundary

Danger labels are not blanket tracking vetoes. A risky wallet may remain in the observation pool and can later earn strategy-specific influence from robust forward evidence.

Mechanical hard stops from v5 are unchanged. Untradeable, transfer-restricted, unexitable, or execution-incomplete opportunities are not converted into specialist authority.

Observation assignment never grants paper-trade authority by itself. Historical evidence cannot directly promote a wallet.

## FOMO

FOMO gets a dedicated specialist pool derived from same-release forward FOMO outcomes. Clean and hazard FOMO are ranked separately by wallet × venue × lifecycle × regime.

This prevents a strong general scout from being treated as a strong FOMO wallet without FOMO-specific evidence, while still allowing the same address to qualify independently in both contexts if it proves both.

## Robinhood isolation

Robinhood Chain remains independent. This allocator modifies only `WalletEntityUniverseV4` on Solana and does not import Solana wallet success into Robinhood entity authority.

## Fixed authority boundary

- paper-only
- no private keys
- no signing
- no transaction submission
- no live-money authority
- no retroactive mutation of an active experiment
- no cross-context success transfer
