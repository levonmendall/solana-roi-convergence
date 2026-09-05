# FOMO Paper Strategy

> **Canonical authority:** `strategy_v51_authority.json` and `docs/V51_CONSOLIDATED_STRATEGY.md`. This document describes the FOMO transport/paper mechanism; any older threshold or promotion wording that conflicts with the canonical v5.1 authority is superseded.

The FOMO implementation has two deliberately separate layers.

1. `fomo_continuation_shadow` remains the point-in-time evidence collector. Keeping that surface shadow-only preserves the audit contract and lets FOMO state classifications be compared without granting the collector execution authority.
2. `fomo_continuation_paper` is the active simulation strategy. It can create and settle paper positions only. It has no signing, transaction-submission, or live-money authority.

## Wallet and context evidence

FOMO evidence remains exact-context and forward-only for promotion. Under the consolidated v5.1 epoch, compatible deployment SHAs may pool only after registration to `v51-consolidated-proof-20260905`; pre-epoch outcomes remain audit-only.

FOMO is split into **clean** and **hazard** cohorts. Wallet identity does not automatically receive authority because it was historically profitable: the economic-certification layer compares wallet/entity outcomes with matched contexts that exclude identity and grants wallet-specific research priority only when identity adds positive forward residual lift.

## Entry and sizing

An actionable FOMO observation must be `pre_fomo` or `active_fomo`, structurally tradeable and supported by exact entry/immediate-exit execution evidence. Twenty seconds remains the maximum operational observation boundary, but being inside it is not economic approval. Residual edge is conditioned on actual latency, chase and execution cost.

The 15% chase level is the baseline rather than a universal hard reject. The 15–25% and 25–40% bands remain paper challenger contexts; above 40% is observe-only during the frozen epoch.

Creator distribution, early-holder distribution, bundling, sniper concentration, common funding, moderate quote/slippage deterioration and similar probabilistic hazards are not automatic vetoes. They increase the forward sample/growth burden and reduce bootstrap paper sizing. Mechanical inability to exit or unavailable critical execution evidence remains fail-closed.

The active FOMO per-position cap remains 5%. Hazard bootstrap sizing is smaller than clean FOMO sizing. Mature fractions are governed by robust forward expected log growth and the cross-family allocator rather than hit rate alone.

## Settlement and learning

FOMO paper outcomes settle from the same canonical forward settlement evidence. The outcome retains trigger wallet, venue, lifecycle, regime, FOMO state, paper fraction and residual ROI. Those outcomes are available to the frozen-epoch economic certification, winner-removal tests, latency/cost sensitivity and execution-stress analysis.

A mature FOMO context can be killed after sufficient independent evidence when shrunk expected log growth, leave-best-trade-out mean and the 95% upper confidence bound are all non-positive. A killed lane cannot retain active paper allocation merely to keep generating trades.

## Safety boundary

The FOMO strategy is paper-only. `live_money_authority=false`, signing is unavailable and transaction submission is unavailable.
