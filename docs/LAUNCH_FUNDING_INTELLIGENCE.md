# Launch and Funding Intelligence

This stage completes the *data architecture* for the two remaining risk dimensions while keeping forward paper promotion disabled.

## Coverage gate

`SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE` defaults to `false` and must remain false unless the live Solana stream is configured to observe every supported launch/swap program from pool creation onward. When false, `launch` and `funding` remain missing and the risk composer fails closed.

Even when the flag is true, launch evidence is recorded only if the earliest normalized swap aligns with the deepest DEX Screener pool creation timestamp within the configured tolerance. A service that started late therefore cannot retroactively manufacture clean launch evidence.

## Launch evidence

The initial observational model uses the first eight seconds after pool creation and requires at least three buys from three wallets. It records:

- `bundled_launch` when three or more distinct buyers land in the same slot;
- `sniper_heavy` when the top two buyers account for at least 65% of early SOL buy flow.

These are frozen pre-cohort heuristics for evidence collection, not claims that the thresholds are optimal.

## Funding provenance

The first five distinct early buyers are traced backward through Helius transaction history for up to seven days. Funding evidence is recorded only when every selected buyer has an identifiable inbound SOL funding source and the requested history range is exhaustively paginated.

Two buyers are linked as the same economic cluster only when a common recent funder sent nearly equal amounts within a short interval; the resulting graph edge is recorded at 0.99 confidence. Incomplete provenance leaves `funding` missing.

The Helius Enhanced Transactions history endpoint is used here because it exposes `nativeTransfers` directly. Helius currently keeps this endpoint operational but recommends newer transaction-history APIs for new integrations; replacing this transport is a follow-on task before latency certification.

## Forward-cohort boundary

All six risk dimensions can now be represented by concrete collectors when coverage is explicitly asserted, but `paper_signal_promotion_enabled` remains false and the collector status reports `latency_certified_for_forward_cohort=false`. The next certification stage must measure collector completion latency and prove the risk bundle can be available within the strategy's entry window before the $500 paper cohort begins.
