# Live Evidence Ingestion

This phase adds the first production-facing Solana evidence boundary without enabling paper allocation.

## Current mode

The service accepts authenticated Helius enhanced-webhook deliveries at `POST /v1/ingestion/helius`. The webhook must be configured with an authorization value and the same exact value must be set in `HELIUS_WEBHOOK_AUTH`.

The initial parser normalizes only unambiguous SOL↔SPL swap events. Token-token routes, ambiguous users, failed transactions, missing timestamps, and unsupported parser shapes fail closed.

Normalized evidence includes signature, slot, chain observation time, local receipt time, measured ingestion latency, wallet, token mint, buy/sell direction, token amount, SOL amount, SOL-denominated reference price, and provider source.

## Wallet profiles and entities

`SOLANA_ROI_WALLET_PROFILES_JSON` seeds point-in-time wallet identity and eligibility records. A wallet address is not assumed to equal an economic entity. Side wallets may share one `entity_id`; wallets in the same entity cannot confirm one another.

Example:

```json
[{"wallet":"WALLET_ADDRESS","entity_id":"economic-entity-001","tier":"S","first_touch_sample_size":100,"historically_eligible":true}]
```

## Fail-closed paper boundary

Live swap ingestion is record-only until an authoritative point-in-time token-risk provider is connected. The frozen v3.1 strategy requires token-risk evidence before a first touch can create a starter or before an independent buy can confirm a candidate.

The default runtime therefore reports `paper_signal_promotion_enabled=false` and `paper_signal_promotion_blocker=point-in-time token risk provider not connected`.

This lets the system accumulate real latency and transaction evidence without contaminating the $500 prospective cohort.

## Idempotency and lineage

Every normalized swap is unique on `(signature, wallet, token_mint, side)`. Duplicate webhook deliveries do not produce duplicate strategy actions. Normalized swaps and ingestion decisions are appended to the existing hash-chained event ledger. `/v1/ingestion/status` exposes evidence counts and chain verification.

## Provider evolution

Helius webhooks are the first inexpensive collector. The normalized swap contract is provider-neutral. Future `transactionSubscribe`, LaserStream, Yellowstone, or other low-latency adapters should emit the same normalized evidence without changing strategy, portfolio, or certification logic.
