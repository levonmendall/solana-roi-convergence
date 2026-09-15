# Robinhood Production Attribution — 2026-09-15

Workspace: `My Workspace`

Authoritative service: `solana-roi-convergence`

Production service ID: `srv-dabnrtn40ujc739r19vg`

Current live release at audit time: `92f79a69bd7141f94f246e3c2e3539aa365a6ff9` (PR #405 merge)

This document records read-only production evidence. It does not authorize a strategy, provider, retention, or production-configuration change.

## Executive result

Two independent Robinhood cost defects are now separated:

1. **Memory/page-cache amplification:** materially repaired by PR #405.
2. **Provider inquiry amplification:** still active after PR #405, with the dominant private-provider burst traced to the broad research screener being globally redirected through Validation Cloud.

V2/V4 adds additional provider work and remains unsafe to reactivate, but the largest observed private-provider burst is not explained by V2/V4 alone.

## Release timeline used for attribution

- PR #403 / V2+V4 observation release `bf3b016033e5a8cf46ef6e67d96797abb06f94e5` first became live at approximately 05:04 UTC.
- The same V2/V4 release was redeployed at approximately 13:54 UTC, giving a fresh-process memory-growth observation window without changing strategy code.
- PR #405 / page-cache-bounded Robinhood status+proof release `92f79a69bd7141f94f246e3c2e3539aa365a6ff9` became live at approximately 15:25 UTC.

## Memory / CPU evidence

### V2/V4 release after fresh start at 05:04

Render memory usage rose approximately:

- 05:05 — 288 MB
- 05:10 — 536 MB
- 05:30 — 745 MB
- 06:05 — 903 MB

CPU over the same interval was generally about 0.63–0.76 CPU.

### Same release after fresh redeploy at 13:54

Render memory usage again climbed materially:

- 13:55 — 310 MB
- 14:05 — 675 MB
- 14:30 — 871 MB
- 14:55 — 964 MB
- 15:20 — 1.005 GB

CPU was commonly about 0.81–0.95 CPU.

This repeated fresh-process growth supports the prior PR #405 attribution that Robinhood history-scaled status/proof reads were repeatedly warming the dedicated SQLite file into cgroup page cache.

### After PR #405 at 15:25

The fresh production instance measured approximately:

- 15:30 — 434 MB
- 15:35 — 692 MB
- 15:40 — 662 MB
- 15:45 — 653 MB

CPU measured approximately 0.60, 0.34, and 0.50 CPU at 15:35, 15:40, and 15:45 respectively.

Conclusion: **PR #405 materially reduced the Robinhood memory/page-cache and CPU burden.** Provider inquiry amplification nevertheless continued, so those are distinct defects.

## Private `eth_getLogs` production evidence

### Early after V2/V4 release

At approximately 05:10 UTC, Validation Cloud telemetry showed roughly one `eth_getLogs` success per advancing range in the sampled interval. The observer/provider state was still young and the fan-out was comparatively small.

### Mature process before PR #405

At approximately 14:30 UTC, one exact range (`63729038` through `63729147`) generated about **97 Validation Cloud `eth_getLogs` requests** in roughly seven seconds.

That cannot be explained by the V2/V4 observer's default six-logical-request shape. It is a separate broad-universe acquisition burst.

The provider-budget research screener uses `RESEARCH_BATCH_SIZE = 64` and scans all persisted paper-eligible markets in the current release. A roughly 97-batch pass is consistent with approximately **6.1k–6.2k market-address slots** spread across the V3 and Pons-V2 research groups.

### After PR #405

Validation Cloud `eth_getLogs` traffic remained active despite the lower memory/CPU footprint. A sampled current interval around 15:43 UTC still showed repeated same-range request clusters and roughly 150 requests/minute in the observed short window.

Therefore PR #405 fixed the page-cache/status problem but did **not** fix provider inquiry amplification.

## Root cause of the largest private-provider burst

`robinhood_provider_budget_transport._research_async()` deliberately creates a `RobinhoodRpc` with the official public Robinhood RPC and runs the broad research screener there. This is the intended architecture: broad research is public/read-only and only a small promoted live shortlist receives private/provider-budget authority.

However, `robinhood_getlogs_provider_guard` is installed globally on `RobinhoodRpc.get_logs`. When Validation Cloud is configured, `_dispatch_range()` prefers Validation Cloud without checking whether that specific `RobinhoodRpc` instance was explicitly created for the official public research endpoint.

The result is an authority/transport mismatch:

**intended:** all-market research screen -> official public Robinhood RPC

**actual:** all-market research screen -> global getLogs guard -> Validation Cloud

This converts a deliberately broad, low-authority public research pass into high-volume private-provider traffic.

## V2/V4 contribution after root-cause separation

V2/V4 remains a real contributor:

- it performs a second observation pass over canonical live ranges;
- default 255 tracked V2 pairs can add up to six logical range requests per observation interval before retries/splits;
- V4 Swap acquisition runs even with zero tracked V4 pools;
- V4 downloads broad PoolManager Swap evidence and discards untracked pool IDs locally;
- accepted V2/V4 swaps have a candidate duplicate persistence surface (structured swap + event observation).

But V2/V4 alone does not explain the approximately 97-request single-range burst. The globally redirected broad research screener is the dominant proven inquiry defect.

## Repair on draft PR #409

PR #409 now contains two branch-only safeguards:

1. **V2/V4 explicit opt-in pause guard** — missing/blank/false configuration stays disabled.
2. **Public research getLogs isolation** — a `RobinhoodRpc` explicitly targeting the official public research endpoint bypasses only the global Validation Cloud/private getLogs dispatcher and uses the pre-guard public read implementation. Non-public/production clients continue through the existing governed Validation Cloud/range-bound/failover stack.

The second repair reduces private-provider work without reducing candidate discovery or changing strategy economics.

## Remaining gates

Do not reactivate V2/V4 yet. Before merge/deploy or reactivation:

- full CI on PR #409 must pass;
- final composition must prove the public research isolation cannot be overwritten by later installers;
- V4 zero-consumer acquisition should be removed;
- V4 provider-side pool filtering should be validated if supported;
- structured/event duplicate persistence needs downstream-consumer proof before any retention change;
- after a green repair release, production must demonstrate a major collapse in Validation Cloud `eth_getLogs` rate while broad research universe size and candidate coverage remain unchanged;
- memory must remain bounded under PR #405's repaired status/proof boundary.

Current V2/V4 decision: **KEEP PAUSED — UNSAFE**.
