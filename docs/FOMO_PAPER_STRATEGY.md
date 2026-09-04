# FOMO Paper Strategy

The FOMO implementation has two deliberately separate layers.

1. `fomo_continuation_shadow` remains the point-in-time evidence collector. Keeping that surface shadow-only preserves the existing audit contract and makes it possible to compare FOMO state classifications without granting the collector execution authority.
2. `fomo_continuation_paper` is the active simulation strategy. It can create and settle paper positions only. It has no signing, transaction-submission, or live-money authority.

## Wallet selection

FOMO wallet performance is evaluated only inside the exact `wallet × venue × lifecycle × regime` context and only from same-release forward FOMO outcomes. Historical wallet performance may help discovery, but it cannot promote a wallet into FOMO paper authority.

A FOMO wallet context becomes mature after the existing minimum forward sample requirement. Promotion requires positive median residual ROI, positive mean after removing the single best trade, and at least a 50% positive-outcome rate. A mature context with non-positive trimmed or median ROI is demoted for FOMO entries. Success in one FOMO context does not transfer automatically to another venue/lifecycle/regime.

Before maturity, accessible `pre_fomo` and `active_fomo` observations receive a 1% bootstrap paper probe so forward evidence can accumulate. This is the mechanism by which paper trading proves or rejects the FOMO thesis rather than waiting for a separate research-only promotion event.

## Entry and sizing

FOMO paper entries require the existing point-in-time FOMO classifier to report `pre_fomo` or `active_fomo` and `structurally_accessible=true`. The existing FOMO classifier already fails closed on incomplete risk evidence, missing/late execution timing, chase above the unchanged 15% ceiling, creator distribution, material early-holder distribution, and deteriorating execution evidence.

The paper strategy reuses the already-canonical amount-specific Jupiter/unsigned-simulation entry and immediate-exit snapshot from the unified forward research trial. It does not issue an additional quote or RPC fanout merely to create a FOMO paper position.

Promoted FOMO wallet contexts choose among 0.5%, 1%, 2%, and 5% paper fractions by forward expected log growth, with a hard 5% per-position cap. Bootstrap probes use 1%. Only one FOMO paper position per token can remain open at a time.

## Settlement and learning

FOMO paper outcomes settle from the same-release unified forward settlement record. The outcome retains the FOMO trigger wallet, venue, lifecycle, regime, FOMO state, chosen paper fraction, residual ROI, and fractional paper-NAV contribution. Those forward outcomes continuously rebuild the FOMO-specific wallet cohort and can promote or demote contexts prospectively.

The FOMO cohort is logically separate from scout wallet authority. This release does not mutate the existing scout cohort or allow FOMO performance to promote a wallet into the scout strategy.

## Fixed safety boundary

The FOMO strategy is paper-only. `live_money_authority=false`, signing is unavailable, and transaction submission is unavailable. Existing 20-second entry, 15% chase, continuity, RPC, certification, and canonical-evidence boundaries are unchanged.
