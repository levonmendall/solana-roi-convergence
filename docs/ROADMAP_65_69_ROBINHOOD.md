# Robinhood Phase 9 — Roadmap 65–69

This block tightens Robinhood Chain runtime and evidence contracts without changing frozen v5.1 economic authority. The canonical 20-second latency hard maximum, selection economics, position limits, exits, promotion/kill rules, paper-only boundary, no-signing boundary, no-submission boundary, and no-live-money boundary remain unchanged.

## 65 — Historical scan no longer controls readiness

Robinhood production is forward-only. Historical cursor closure is archival/audit information and has no readiness authority.

Canonical status semantics are now:

- `caught_up = true`
- `catchup_mode = latest_seed_plus_reorg_insurance`
- `historical_block_lag = 0`
- historical swap backfill disabled
- latest live head is the runtime anchor
- only bounded short reorg/metadata insurance remains

A production provider/WebSocket outage can still fail runtime readiness closed; it is simply no longer confused with historical catch-up debt.

## 66 — Robinhood remains isolated

The existing dedicated Robinhood worker architecture remains authoritative:

- dedicated OS thread
- private asyncio loop
- dedicated Robinhood SQLite store
- nonblocking copied status cache
- proof refresh on a separate SQLite connection/threadpool
- no Robinhood writes to canonical Solana SQLite
- no Robinhood worker on the Uvicorn event loop

Phase 9 exposes an explicit isolation contract so a Robinhood stall, restart, stale cache, or local-store failure cannot be interpreted as canonical Solana evidence corruption.

## 67 — Proof cache freshness is explicit and fail-closed

Robinhood cached proof now exposes and enforces:

- `runtime_ready`
- `proof_generated_at`
- `proof_age_seconds`
- `anchor_policy_passed`
- `max_snapshot_age_seconds`

The Robinhood snapshot freshness ceiling is 15 seconds, three normal five-second proof publication intervals. A missing proof timestamp, stale proof, failed runtime, failed anchor policy, or unusable proof state forces `available = false`. A stale or timestamp-less proof can no longer remain available merely because an older cached payload exists.

## 68 — Every v2/v3 opportunity gets durable pre-lane disposition

The candidate ledger now covers both market creation and live reserve/swap updates.

- created Uniswap-v3/Pons-v1 pools and Pons-v2 curves receive a durable `rh-create:` candidate before any later lane selection
- live v2/v3 reserve/swap opportunities receive exact event identities (`rh-event:`) before `_maybe_open_v2` / `_maybe_open_v3` lane selection
- the same exact event identity is used by candidate coverage and v5.1 consolidation
- every candidate row ends in `paper_enter` or explicit `paper_reject` with a nonempty reason
- created markets without flow evidence are explicitly rejected as `created_market_requires_forward_flow_or_reserve_update_before_lane_selection`
- rejected ledger rows remain the durable source for the existing rejected-counterfactual materializer

The ledger primary key gives one terminal disposition row per candidate; append-only stage history can still record the path to that terminal disposition.

## 69 — Venue economics cannot pool

Robinhood promotion, sizing, and learned-exit evidence remain scoped by venue and lifecycle. Phase 9 makes that boundary explicit and regression-protected.

The protected economic families are:

1. `UNISWAP_V3`
2. `PONS_V2`
3. `POST_GRADUATION_CONTINUATION`

Evidence scope remains same-entity + lane + venue + lifecycle + regime + risk signature (with only conservative same-venue/lifecycle backoff). Pons-v2 results cannot promote or size Uniswap-v3 positions, Uniswap-v3 results cannot promote or size Pons-v2 positions, and a future explicit post-graduation lifecycle cannot borrow promotion evidence from either pre-graduation family.

## Authority invariants

Phase 9 has no live-money or strategy-expansion authority:

- v5.1 economic freeze unchanged
- 20-second hard maximum unchanged
- paper-only `true`
- signing `false`
- transaction submission `false`
- live-money authority `false`
