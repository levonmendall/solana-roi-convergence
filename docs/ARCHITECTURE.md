# Architecture

## Objective

Build the smallest possible system that can prospectively prove or disprove ROI Convergence v3.1 under real Solana conditions using a continuously compounded $500 paper account.

## Data plane

`Solana transaction stream`

-> `wallet/entity identity`

-> `historical wallet score at T`

-> `first-touch event`

-> `point-in-time token risk snapshot`

-> `independent confirmation`

-> `price/chase/liquidity evidence`

-> `strategy state machine`

-> `paper execution simulator`

-> `append-only $500 portfolio ledger`

-> `forward profitability certification`

## Provider boundary

The core strategy is vendor-neutral. The first concrete live adapter should favor low-cost Helius raw/enhanced webhooks or transactionSubscribe. Higher-speed Yellowstone-compatible streaming can be substituted without changing strategy code.

## Authority boundary

The repository is paper-only. Provider adapters are observation-only. There is intentionally no transaction builder, signer, private-key loader, live balance, sendTransaction method, custody layer, or withdrawal path.

## Point-in-time rule

Every wallet tier, entity relationship, risk flag, confirmation, price, and cost value used to make a decision must have an observation timestamp no later than that decision. Future follower counts, future peak prices, future wallet performance, and later-discovered risk relationships cannot alter an earlier decision.

## Storage

Initial durable storage is SQLite WAL with a hash-chained append-only event ledger. This is sufficient for the first forward cohort and keeps the system easy to audit and deploy.
