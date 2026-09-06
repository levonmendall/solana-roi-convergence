# V5.1 Roadmap 59–64 Research Plane

This implementation completes roadmap items 59–64 as **forward research and measurement only**. It does not alter the canonical v5.1 economic authority, the 20-second hard maximum, paper-entry rules, sizing, exits, promotion thresholds, signing, transaction submission, or live-money authority.

## 59 — FOMO signal half-life

The research plane measures clean and hazard FOMO cohorts separately across exact signal-to-entry latency bands:

- 0–2 seconds
- 2–5 seconds
- 5–10 seconds
- 10–20 seconds
- >20 seconds

Each bucket reports sample and independent-event counts, mean/median net return, hit rate, 1% expected log growth, leave-best-trade-out mean, a 95% mean-return interval, and available chase/execution-cost telemetry.

A conservative empirical half-life marker is emitted only when the first later populated bucket falls to at most half of the 0–2 second mean forward return. If the data do not support that claim, the state remains `insufficient_evidence` or `half_edge_not_observed`; no value is fabricated.

The >20-second cohort is research-only and cannot create paper-entry authority in the v5.1 epoch.

## 60 — Pump.fun first-slot policy

Pump.fun first-slot activity remains research-only. The active thesis remains residual continuation/information after the system can observe and price the opportunity; this implementation does not attempt to compete with millisecond first-slot snipers and does not create first-slot promotion authority.

## 61 — Pump.fun → PumpSwap lifecycle

`v51_token_lifecycle_research_events` is a persistent research ledger keyed to the economic freeze epoch, token mint, and first observed PumpSwap/PUMP_AMM transition.

The lifecycle plane follows observable evidence across:

1. Pump.fun bonding-curve observations
2. the last Pump.fun observation before PumpSwap
3. the first observed PUMP_AMM/PumpSwap state
4. 0–30s, 30–120s, 120–300s, and >300s post-transition observation windows
5. separately recorded Raydium evidence

Where settled execution evidence exists for the token on PUMP_AMM, the event includes an after-cost execution profile. Missing exact executable horizon evidence remains explicit measurement debt.

## 62 — Graduation event cluster

Graduation becomes a first-class research event. The system publishes an exact graduation timestamp only when an explicit graduation observation is present. Otherwise it publishes the bounded transition window from the last observed Pump.fun state to the first observed PUMP_AMM state.

The research plane never infers an exact timestamp from absence of observations and never fabricates forward prices or quotes.

## 63 — Raydium remains separate from PumpSwap

PUMP_AMM/PumpSwap and Raydium are independent venue/lifecycle contexts. Their return evidence is never pooled by the roadmap 59–64 research surface. A token can have both a PumpSwap transition and later Raydium evidence, but Raydium remains an independent branch of the lifecycle.

## 64 — Venue-specific execution decay

The research plane estimates residual forward edge separately by venue, lifecycle, and—within FOMO—clean versus hazard risk class.

For every available segment it reports forward-return profiles across:

**Latency**
- 0–2s
- 2–5s
- 5–10s
- 10–20s
- 20–40s
- 40–90s
- >90s

**Chase**
- ≤15%
- 15–25%
- 25–40%
- >40%

**Round-trip execution cost**
- ≤2%
- 2–5%
- 5–10%
- >10%
- unknown when exact cost evidence was not captured

These surfaces are diagnostic inputs for a future economic freeze epoch. They have no selection, sizing, exit, or promotion authority in v5.1.

## API

`GET /v1/strategy/latency-challengers` now includes:

```text
roadmap_59_64_research
```

alongside the existing broad latency challenger and isolated Robinhood challenger proof.

The new payload explicitly reports:

- `current_authority_changed = false`
- `latency_hard_max_seconds_changed = false`
- `above_20s_paper_entry_authority = false`
- `selection_authority = false`
- `sizing_authority = false`
- `exit_authority = false`
- `promotion_authority = false`
- `paper_only = true`
- `live_money_authority = false`
- `signing_available = false`
- `transaction_submission_available = false`

This keeps the current v5.1 evidence epoch scientifically comparable while collecting the data needed to decide whether a future venue/lifecycle-specific authority should replace the universal 20-second boundary.
