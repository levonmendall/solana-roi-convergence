# Solana ROI Convergence

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/levonmendall/solana-roi-convergence)

A focused, **paper-only** forward-validation system for **ROI Convergence v3.1** on Solana.

The initial objective is deliberately narrow:

> Determine whether a continuously compounded $500 paper account can profit from historically predictive wallet first-touches, genuinely independent wallet convergence, strict launch/manipulation risk vetoes, fast simulated execution, rapid failure exits, a +50% capital harvest, and an uncapped runner.

## Current status

The repository is ready for the live-shadow certification phase:

- frozen v3.1 strategy configuration;
- deterministic S-tier and A-tier entry state machine;
- frozen public S-tier scout cohort for Jijo, Wugi, and The Doc;
- independent-entity confirmation requirement;
- hard six-dimension risk veto model;
- 20-second confirmation window and 15% chase ceiling;
- $500 continuously compounded paper portfolio;
- 0.75% NAV risk / 2.5% full spot notional sizing;
- conservative execution-cost model;
- amount-specific Jupiter pricing;
- dedicated public-address-only Solana shadow identity;
- exact unsigned taker transaction construction and mainnet `simulateTransaction` observations;
- 90/180-second thesis exits and -30% catastrophic stop;
- 70% harvest at +50% plus 30% runner;
- 40% runner trailing drawdown;
- append-only hash-chained SQLite evidence;
- restart-safe durable paper-engine checkpoints;
- durable, retrying Helius webhook intake;
- per-source Pump/Pump AMM/Raydium coverage certification;
- 300-trade forward profitability certification;
- deployment preflight and automatic Helius webhook bootstrap.

## Deploy live shadow mode

The Render Blueprint creates one paid web service with a persistent SQLite disk and auto-deploys disabled. On initial Blueprint creation, Render only needs three user-owned values:

1. `HELIUS_API_KEY`
2. `JUPITER_API_KEY`
3. `SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY` — a **public Solana address only**

Render generates the webhook-authentication and cohort-administration secrets internally. Once the service starts, it uses `RENDER_EXTERNAL_URL` to idempotently create/update its own Helius enhanced webhook for the frozen Pump, Pump AMM, and Raydium program set.

After deployment, check `/v1/deployment/preflight`. The paper cohort remains unarmed while prospective coverage, latency, quote, risk, and unsigned transaction-simulation evidence accumulates.

See [`docs/DEPLOYMENT_AUTOMATION_PLAN.md`](docs/DEPLOYMENT_AUTOMATION_PLAN.md).

## Safety boundary

This repository intentionally has **no live execution authority**:

- no private keys or seed phrases;
- no signer;
- no `sendTransaction` integration;
- no transaction submission;
- no custody, deposits, or withdrawals;
- no real-money balance authority.

It can construct an **unsigned** proposed transaction for a public taker address solely so Solana RPC can simulate whether that exact transaction would plausibly execute. Missing or stale required evidence fails closed.

## Baseline strategy

See [`docs/BASELINE_STRATEGY.md`](docs/BASELINE_STRATEGY.md). The initial forward parameters are frozen so later performance cannot be improved by rewriting the rules after outcomes are known.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pytest
solana-roi baseline
solana-roi preflight
uvicorn solana_roi.api:app --reload
```

## Certification target

The system cannot call the strategy profitable until at least 300 independent closed token episodes clear the configured evidence gates, including positive net P&L, positive geometric growth, profit factor above one, a 95% Wilson lower hit-rate bound above the modeled break-even rate, and profitability that survives removal of the best trade and best scout.
