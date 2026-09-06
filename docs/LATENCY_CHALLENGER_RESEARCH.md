# V5.1 Latency Challenger Research

## Purpose

The active v5.1 strategy keeps its frozen **20.0 second hard maximum** for paper-entry authority. This research plane exists to measure whether that universal cliff is too restrictive for continuation-oriented venues and lifecycles.

It does **not** change `strategy_v51_authority.json`, the economic freeze epoch, selection, sizing, promotion, kill, exit, signing, transaction submission, or live-money authority.

## Research cohorts

Rejected candidates are grouped by final amount-specific `signal_to_entry_seconds`:

- `authorized_le_20s`: <= 20 seconds
- `challenger_20_40s`: > 20 and <= 40 seconds
- `challenger_40_90s`: > 40 and <= 90 seconds
- `later_lifecycle_gt_90s`: > 90 seconds
- `unknown`: no comparable numeric signal-to-entry measurement is available

The cohorts are segmented by surface, venue, and lifecycle. This prevents Pump.fun launch timing from being assumed equivalent to PumpSwap graduation, Raydium continuation, FOMO continuation, or Robinhood Chain.

## Evidence semantics

For Solana/FOMO, the research plane uses the existing amount-specific final quote path and rejected-counterfactual outcomes. It does not substitute RPC latency or ingestion latency for economic observation latency.

For Robinhood Chain, rejected opportunities already receive forward market-return counterfactual resolution. Where an equivalent numeric `signal_to_entry_seconds` is not durably available, the proof reports explicit measurement debt and leaves those observations in the `unknown` latency cohort rather than inventing timing.

## Future epoch decision rule

A future strategy epoch may consider venue/lifecycle-specific latency authority only after the relevant challenger cohort has sufficient independent forward evidence and remains positive after costs, leave-best-trade-out robustness, tail/drawdown analysis, and execution stress.

No positive challenger result can retroactively create a v5.1 paper position or satisfy current v5.1 promotion authority.

## Production proof surface

`GET /v1/strategy/latency-challengers`

The endpoint composes the canonical Solana/FOMO store with the already-cached isolated Robinhood proof. The main Uvicorn process does not read Robinhood SQLite directly.
