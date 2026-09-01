# Solana ROI Convergence

A focused, **paper-only** forward-validation system for **ROI Convergence v3.1** on Solana.

The initial objective is deliberately narrow:

> Determine whether a continuously compounded $500 paper account can profit from historically predictive wallet first-touches, genuinely independent wallet convergence, strict launch/manipulation risk vetoes, fast simulated execution, rapid failure exits, a +50% capital harvest, and an uncapped runner.

## Current status

The first executable core is implemented on the build branch:

- frozen v3.1 strategy configuration;
- deterministic S-tier and A-tier entry state machine;
- independent-entity confirmation requirement;
- hard risk veto model;
- 20-second confirmation window and 15% chase ceiling;
- $500 continuously compounded paper portfolio;
- 0.75% NAV risk / 2.5% full spot notional sizing;
- conservative ~5% round-trip execution stress;
- 90/180-second thesis exits and -30% catastrophic stop;
- 70% harvest at +50% plus 30% runner;
- 40% runner trailing drawdown;
- append-only hash-chained SQLite evidence;
- 300-trade forward profitability certification;
- vendor-neutral Solana provider interfaces;
- read-only FastAPI health and baseline endpoints.

## Safety boundary

This repository intentionally has **no live execution path**:

- no private keys;
- no transaction builder;
- no signing;
- no `sendTransaction` integration;
- no custody, deposits, or withdrawals;
- no real-money balance authority.

Missing or stale required evidence fails closed.

## Baseline strategy

See [`docs/BASELINE_STRATEGY.md`](docs/BASELINE_STRATEGY.md). The initial forward parameters are frozen so later performance cannot be improved by rewriting the rules after outcomes are known.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

The core is vendor-neutral. The intended first live-data integration is a low-cost Solana webhook/WebSocket adapter; a Yellowstone-compatible low-latency stream can be substituted later without changing strategy or portfolio logic.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pytest
solana-roi baseline
uvicorn solana_roi.api:app --reload
```

## Certification target

The system cannot call the strategy profitable until at least 300 independent closed token episodes clear the configured evidence gates, including positive net P&L, positive geometric growth, profit factor above one, a 95% Wilson lower hit-rate bound above the modeled break-even rate, and profitability that survives removal of the best trade and best scout.
