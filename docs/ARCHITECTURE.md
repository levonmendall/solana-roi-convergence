# Architecture

## Objective

Prospectively prove or disprove Solana ROI strategies under real market conditions using a continuously compounded $500 paper account while preserving strict point-in-time evidence and zero live-money authority.

The active v3.1 cohort remains immutable until its governed certification gates are satisfied. The v4 profit-first/entity strategy is research-only and release-bound until final-version forward evidence proves that it is superior under the existing promotion policy.

## Observation and evaluation funnel

The system intentionally separates **broad observation** from **deep economic evaluation**.

`Full frozen seven-program + three-scout raw observation`

-> `cheap broad wallet/entity discovery`

-> `historical profitability screen (discovery only; no promotion authority)`

-> `bounded dynamic high-priority wallet/entity set`

-> `dedicated realtime logsSubscribe forward tracking`

-> `point-in-time copyability + entity/risk evidence`

-> `release-bound v4 five-lane shadow comparison`

-> `forward profitability / robustness selection`

-> `paper-only strategy authority after governed certification`

The whole observed Solana scope is not deeply hydrated or risk-scored. Broad receipts identify promising economic actors; expensive analysis is reserved for launches, active scouts, and the bounded tracked wallet/entity set. Historical wallet success can nominate a challenger, but it cannot promote one. Strategy influence requires prospective observations first seen after the wallet's live forward boundary.

## Continuity architecture

Prospective evidence is observed through two independent public Solana WebSocket providers plus a continuously maintained read-only HTTP standby lane for every frozen target.

For high-volume Pump.fun / Pump AMM targets, replaying thousands of signatures that were already observed by healthy WebSockets is not the normal standby-maintenance mechanism. While at least one real target WebSocket remains continuously authoritative, the standby watermark is advanced only from a **confirmed exact target WebSocket receipt**, with the effective cursor kept one slot behind the confirmed receipt so same-slot signatures are replay-safe.

If real WebSocket coverage reaches zero, the confirmed target frontier at gap onset becomes the minimum recovery boundary for that generation and the canonical bounded recovery path runs immediately. The certification constraints remain unchanged:

- recoverability lease: 12 seconds;
- maximum recovery delta: 3 pages x 1000 signatures;
- no historical backfill may relabel an irrecoverable interval as prospective evidence;
- uncertainty fails the release closed.

The standby checkpoint itself is never persisted as a Solana receipt and has no hydration, candidate, strategy, paper-trading, signing, or submission authority.

## Research / certification resource boundary

Continuity recovery has the critical RPC reservation. Candidate acquisition has reserved noncritical capacity ahead of research. High-volume standby maintenance has its own noncritical priority below candidates and cannot consume the critical reserve. Wallet research uses a separate RPC pool object and is governed process-wide so broad discovery cannot starve certification or live wallet tracking.

This keeps the certification evidence-producing path independent from broad market research while preserving the full raw observation scope.

## Wallet/entity forward-learning boundary

Realtime wallet observations are the canonical prospective input to the v4 profit-first/entity evaluator. The final production composition installs point-in-time wallet evidence semantics first and the release-bound v4 adapter last, so a later wrapper cannot silently bypass v4 shadow sampling.

A new release starts a clean v4 evidence epoch. Older historical or forward rows remain available for audit/research but are not replayed into the new release as executable forward evidence. Challengers may replace weaker wallets for future influence only after satisfying the current forward promotion requirements.

## Authority boundary

The repository is paper-only. Provider adapters and research evaluators are observation-only. There is intentionally no transaction builder, signer, private-key loader, live balance, `sendTransaction` path, custody layer, or withdrawal path.

No continuity or research repair is permitted to change strategy thresholds, the frozen market scope, signing/submission capability, or the live-money prohibition.

## Point-in-time rule

Every wallet tier, entity relationship, risk flag, confirmation, price, and cost value used to make a decision must have an observation timestamp no later than that decision. Future follower counts, future peak prices, future wallet performance, later-discovered risk relationships, and retroactive risk completion cannot alter an earlier decision.

## Storage

Canonical state uses persistent SQLite WAL plus a hash-chained append-only event ledger. Raw/normalized prospective evidence is retained according to its governed evidence contract; bounded operational queues and terminal work-state retention are not substitutes for canonical evidence.
