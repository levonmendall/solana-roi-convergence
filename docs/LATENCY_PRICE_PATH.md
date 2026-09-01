# Latency and Price-Path Certification

This stage measures the two facts that must be proven before the $500 forward paper cohort can begin: how quickly complete risk evidence becomes available after an observed swap, and what executable SOL-denominated price path existed after the signal.

## Risk latency evidence

Every live collector refresh now records:

- trigger chain timestamp;
- webhook receipt timestamp;
- collector start and completion timestamps;
- collector elapsed milliseconds;
- chain-to-complete end-to-end milliseconds;
- ingestion latency;
- whether all six risk dimensions were complete and fresh at completion.

The initial pre-cohort latency gate requires at least 100 observations, at least 95% complete-and-fresh bundles, p95 end-to-end latency <=5 seconds, p99 <=10 seconds, and p95 ingestion latency <=2 seconds. Passing this gate never activates trading automatically.

## Price paths

Every normalized Helius swap is persisted as a SOL/token price mark. An optional one-second shadow clock can also poll the deepest WSOL-quoted DEX Screener pool for first-touched tokens during the five-minute strategy observation horizon. USDC or other quote pairs are not accepted as SOL marks.

The clock is disabled by default. `SOLANA_ROI_SHADOW_CLOCK_ENABLED=true` enables shadow observation only; it does not create paper positions.

## Activation-safety repair

The previous future activation path could have used the scout's earlier transaction price even if risk collection finished seconds later. That would create an optimistic simulated fill. Paper promotion is now hard-blocked in the production ingestion service until a post-risk executable reference-price handoff is separately certified.

Therefore all of the following must be true before the cohort can start:

1. program-wide launch/swap coverage is proved;
2. all six risk dimensions can complete prospectively;
3. latency certification passes;
4. a fresh executable post-risk mark is available at decision time;
5. strategy entry/exit clocks are driven continuously;
6. paper-only authority remains the only execution authority.

No private keys, transaction signing, live order submission, custody, deposits, or withdrawals are introduced by this stage.
