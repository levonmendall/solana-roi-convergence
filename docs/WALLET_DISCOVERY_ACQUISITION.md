# Continuous Wallet Discovery and Acquisition

## Purpose

This lane supplies the previously missing input to `ContinuousWalletIntelligence`: it continually searches broad Solana program activity for wallets worth studying, screens them, then measures them prospectively under the ROI system's own observation and execution constraints.

It is research-only and paper-only. It cannot sign, submit, hold a private key, authorize a paper trade, or mutate the active v3.1 scout cohort.

## Data path

1. The existing direct Solana plane receives the unchanged seven-program WebSocket scope and writes compact raw receipts to the durable receipt journal.
2. Wallet discovery advances an independent durable cursor through that journal.
3. A deterministic sample of program receipts is hydrated with standard Solana `getTransaction` and normalized into simple SOL/WSOL-to-token swaps.
4. Previously unknown fee-payer wallets become discovery candidates. They are not inserted into `wallet_profiles`, so discovery can never turn a research wallet into a strategy scout by accident.
5. A bounded historical `getSignaturesForAddress`/`getTransaction` screen reconstructs recent wallet buy/sell episodes. Historical evidence is allowed only to decide whether the wallet deserves prospective tracking bandwidth.
6. A passing wallet receives an immutable `forward_started_at` boundary and current signature anchor. No transaction before that boundary can contribute to promotion evidence.
7. The worker then polls only the incumbents and the bounded challenger shortlist. New wallet swaps are repriced at observation time with the current deepest WSOL DexScreener mark.
8. A forward observation is copyable only when its observed chase is within the unchanged v3.1 15% chase ceiling and its acquisition lag is within the research observation-lag bound.
9. Forward buys also run the existing launch/funding/authority/liquidity/flow/deployer evidence collectors. Missing risk evidence fails closed for promotion scoring.
10. The worker converts the prospective observations into `WalletPerformanceSnapshot` rows for `ContinuousWalletIntelligence`.
11. When a challenger has enough forward episodes and proves materially superior under the existing promotion policy, the intelligence layer may persist a proposed next immutable cohort/version.

## Fail-closed chronology

Historical screening has zero promotion authority. A candidate must first cross `forward_started_at` and then accumulate new observations.

If the forward signature anchor falls outside bounded pagination, the worker does not silently backfill the gap as if it were prospective. It deletes that wallet's current forward evidence, advances to a new signature anchor, records a `wallet_discovery_forward_epoch_reset`, and starts a new prospective evidence epoch.

## Copyable performance

The forward replay uses prices available after the ROI system observes the wallet transaction, not the successful wallet's original price by itself. This directly penalizes wallets whose apparent alpha disappears after detection latency.

For every prospective swap the lane records:

- wallet execution price;
- current copyable price;
- chase fraction;
- observation lag;
- whether the observation met copyability constraints;
- whether complete risk evidence was available;
- manipulation-risk flag;
- side-wallet/entity flag.

Closed copyable episodes produce return on deployed capital, geometric growth, profit factor, hit rate, maximum drawdown, copyability rate, manipulation risk, side-wallet risk, and median entry lag.

## Manipulation and entity controls

The forward lane reuses the existing risk and entity graph. Bundled launches, sniper-heavy launches, common-funded early-wallet clusters, scout/deployer connections, or incomplete risk evidence can block promotion. If the candidate resolves into a multi-wallet high-confidence entity component, side-wallet risk fails closed.

The downstream wallet-intelligence layer separately blocks a challenger that resolves to the same economic entity as any incumbent cohort member.

## Resource bounds

Production defaults deliberately avoid turning discovery into another full transaction-processing engine:

- deterministic 1-in-20 raw program receipt discovery sample;
- at most 600 raw receipts examined per cycle;
- at most 120 historical signatures for an initial wallet screen;
- six concurrent historical transaction reads;
- at most 12 challenger wallets tracked prospectively, plus incumbents;
- ten-second wallet polling cadence;
- at most three bounded forward signature pages per wallet poll;
- six concurrent forward transaction reads.

These are research acquisition limits, not strategy thresholds.

## Runtime visibility

`GET /v1/wallet-discovery/status` reports candidate states, sample counts, forward observation counts, copyable fraction, tracked wallets, evidence-epoch resets, errors, and the downstream wallet-intelligence status.

`GET /v1/ingestion/status` also embeds both `wallet_discovery` and `wallet_intelligence`.

## Current completeness statement

The feature now performs continuous broad wallet discovery, bounded historical screening, prospective wallet tracking, copyable repricing, risk/entity evaluation, snapshot generation, and governed future-cohort proposal evaluation.

It intentionally does **not** claim exhaustive enumeration of every Solana wallet because broad discovery is a deterministic sample of the full program receipt stream. The sampling rate can be increased independently later if production RPC and CPU telemetry prove sufficient headroom.

No part of this lane creates live-money capability or changes the frozen ROI Convergence v3.1 trading rules.