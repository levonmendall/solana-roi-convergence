# Robinhood V2/V4 Efficiency Audit

Audit base: `main` at `92f79a69bd7141f94f246e3c2e3539aa365a6ff9` (PR #405 merge)

Status: **PRODUCTION ATTRIBUTION COMPLETE ENOUGH TO IDENTIFY PRIMARY INQUIRY DEFECT; SAFE REPAIRS IN DRAFT PR #409**

## Executive verdict

### Robinhood lane

**MAJOR CONTRIBUTOR** to the recent resource/stability problems, with two distinct mechanisms now separated.

1. **Memory/page-cache amplification:** PR #405 materially repaired the history-scaled one-second status and deep-proof SQLite read path. Fresh-process Render memory is materially lower after that release.
2. **Provider inquiry amplification:** still active after PR #405. The largest private-provider bursts are caused by the broad research screener being globally redirected through Validation Cloud even though that screener explicitly constructs the official public Robinhood RPC.

### V2/V4 integration

**KEEP PAUSED — UNSAFE TO REACTIVATE.**

V2/V4 is a real incremental inquiry/storage contributor, but production telemetry proves it is not sufficient to explain the largest observed private-provider burst. The dominant inquiry defect is the public-research/private-provider transport crossover described below.

## Current canonical boundary

- Canonical `main` at audit start: `92f79a69bd7141f94f246e3c2e3539aa365a6ff9`.
- PR #404 merged the canonical V3/Pons HTTP fallback batching repair.
- PR #405 merged the Robinhood status/proof page-cache repair.
- PR #407 is stale/diverged and its final-composition regression failed; do not merge it as-is.
- Draft PR #409 contains the current audit and safe repairs. It is not merged or deployed until full CI is green.

## Production attribution from Render `My Workspace`

Authoritative service: `solana-roi-convergence` (`srv-dabnrtn40ujc739r19vg`), 2 GiB instance with 25 GB persistent disk.

Release timeline used for comparison:

- V2/V4 release `bf3b016033e5a8cf46ef6e67d96797abb06f94e5` live about 05:04 UTC.
- Same V2/V4 release freshly redeployed about 13:54 UTC.
- PR #405 release `92f79a69bd7141f94f246e3c2e3539aa365a6ff9` live about 15:25 UTC.

### Memory / CPU

The V2/V4 release showed repeatable fresh-process memory growth:

- first run: about 288 MB at 05:05 -> 903 MB by 06:05;
- fresh redeploy: about 310 MB at 13:55 -> 1.005 GB by 15:20;
- CPU commonly about 0.8–0.95 CPU later in the release.

After PR #405, the fresh instance was about 434 MB at 15:30 and stabilized around 650–690 MB in the next sampled interval, with materially lower CPU samples.

Conclusion: **PR #405 materially reduced the page-cache/status/proof burden.** The provider inquiry problem continued independently.

### Private provider request burst

At approximately 14:30 UTC, one exact block interval (`63729038` through `63729147`) generated about **97 Validation Cloud `eth_getLogs` successes** in roughly seven seconds.

A V2/V4 observer at its default 255-pair cap can add up to six logical requests per observation range before retries/splits. Therefore V2/V4 alone cannot explain a 97-request exact-range burst.

After PR #405, a short current sample around 15:43 UTC still showed repeated same-range Validation Cloud requests at roughly 150 requests/minute. Thus the inquiry amplification remained after the memory repair.

## Primary provider inquiry root cause

The provider-budget research design intends:

**all persisted paper-eligible Robinhood markets -> official public Robinhood RPC research screen -> ranked shortlist -> bounded private live lane**

`robinhood_provider_budget_transport._research_async()` explicitly constructs:

`RobinhoodRpc(rpc_url=ROBINHOOD_PUBLIC_RPC)`

and `_research_pass()` scans the complete persisted paper-eligible universe in batches of 64.

However, `robinhood_getlogs_provider_guard` globally wraps `RobinhoodRpc.get_logs`. When Validation Cloud is configured, its dispatch prefers Validation Cloud without checking that a particular RPC instance was explicitly created for the official public research endpoint.

Actual behavior therefore became:

**broad public research screen -> global getLogs guard -> Validation Cloud**

instead of the intended public endpoint.

The approximately 97-request burst is consistent with about 6.1k–6.2k market-address slots split into 64-address research batches.

This is the **dominant proven private-provider inquiry defect**.

## Safe repair in PR #409: public research transport isolation

PR #409 adds an order-independent dispatch-seam repair:

- an RPC instance explicitly targeting `ROBINHOOD_PUBLIC_RPC` uses the captured pre-private-dispatch read path;
- private/production RPC instances continue through the existing Validation Cloud, range-bound, retry, and failover dispatch;
- broad candidate universe is unchanged;
- strategy thresholds, sizing, exits, wallet policy, paper authority, signing, and submission remain unchanged;
- the repair patches the provider guard's dispatch seam rather than depending on a particular class-wrapper import order.

This removes the largest private-provider inquiry amplification **at the source** instead of merely adding a larger quota or batching around it.

## V2/V4 inquiry amplification

The V2/V4 observer wraps canonical live-frontier market-log acquisition and performs a second observation pass over the same block range.

Per observation range it currently performs:

1. one V2 PairCreated + V4 Initialize discovery query;
2. `ceil(tracked_v2_pairs / 64)` V2 Swap queries;
3. one V4 PoolManager Swap query unconditionally.

At the default 255 V2 pair cap this is up to:

`1 + ceil(255/64) + 1 = 6`

additional logical range requests before retries/splits.

At the source maximum 1,024 V2 pairs it can be 18.

Even with zero tracked V2 pairs and zero tracked V4 pools, discovery and V4 Swap activity acquisition still run. Therefore V2/V4 remains an avoidable incremental cost and must stay paused.

## V4 result-volume amplification

The V4 activity query requests all Swap events from the PoolManager for the interval, then filters `topics[1]` pool IDs locally against the tracked V4 registry.

Consequences:

- untracked pools consume provider response bytes and parsing/sorting work;
- zero tracked V4 pools still cause an activity request with no useful consumer;
- the runtime already counts `untracked_v4_swaps_ignored`, proving this discard path is intentional.

Before reactivation, skip V4 activity when no tracked pool can consume it and validate a provider-side topic1 pool-id filter/batching design.

## V2/V4 pause-safety defect and repair

Current `main` defaults `ROBINHOOD_V2_V4_OBSERVATION_ENABLED` to true when the variable is missing. That is unsafe for a lane that is operationally supposed to remain paused.

PR #409 adds an explicit-opt-in guard:

- missing, blank, `0`, `false`, `no`, or `off` -> disabled;
- only explicit `1`, `true`, `yes`, or `on` -> enabled;
- the guard reasserts itself if a legacy composition path restores the previous default-on predicate.

This repair is branch-only until CI passes and it is merged.

## Canonical transport and map-bound observation

The private decision-authoritative WebSocket architecture is already designed to be bounded to factory discovery plus a small promoted live target set. Do not weaken that boundary.

A secondary source risk exists because `_ensure_runtime_market()` can add promoted markets to `self.v3_pools` / `self.v2_curves` without directly invoking `_trim_tracking()`, while HTTP helper code can read those registries. This deserves a regression, but it is **not necessary to explain the 97-request Validation Cloud burst** now that the global public-research redirection is proven. Any map-retention repair must protect open positions and currently selected targets rather than blindly evicting metadata.

## Proven memory/page-cache defect

PR #405 established and repaired a separate Robinhood problem:

- one-second status publication had history-scaled SQLite analytics;
- five-second proof refreshes could rebuild deep proof state;
- those reads warmed the dedicated Robinhood SQLite file into cgroup page cache;
- fast status is now in-memory-only;
- deep proof is generation-gated on proof-relevant inputs;
- raw swap growth no longer triggers deep proof reconstruction.

Production memory after PR #405 materially supports this repair.

## Storage and write amplification

V2/V4 currently persists:

- latest observation cursor in `robinhood_chain_state`;
- bounded V2 pair registry;
- bounded V4 pool registry;
- market observations;
- swaps through the structured Robinhood swap path;
- an additional `robinhood_swap_observation` event for each newly inserted V2/V4 swap.

The structured swap row plus observation event is a **candidate duplicate-retention surface**. No destructive deletion is authorized until downstream consumer and certification proof establishes that one copy is unnecessary.

Current retention classification:

| Surface | Classification |
| --- | --- |
| V2/V4 cursor | KEEP LATEST 1 |
| V2 registry | KEEP LATEST N |
| V4 registry | KEEP LATEST N |
| structured swaps | RETAIN BOUNDED HISTORY; exact horizon still to prove |
| `robinhood_swap_observation` | REVIEW FOR AGGREGATE/DISCARD or bounded history |
| market discovery events | BOUNDED HISTORY unless full lineage is proven necessary |
| operational status snapshots | EPHEMERAL |
| deep proof snapshot | KEEP LATEST / generation-addressable only as required |

## PR #407

PR #407 should remain unmerged. Its goal—preserving combined provider efficiency under final V2/V4 composition—is valid, but its branch diverged from current main and its final-composition regression failed. Do not weaken that assertion. Rebuild any needed composition change on current main after PR #409 establishes the correct transport boundary.

## Reactivation gate

Current decision: **KEEP PAUSED — UNSAFE**.

V2/V4 can move to a controlled shadow test only when all of these are true:

- explicit default-off safety is merged and proven;
- broad public research no longer consumes Validation Cloud/private-provider inquiries;
- measured private `eth_getLogs` rate collapses to the workload actually requiring the private provider;
- broad candidate-universe coverage remains unchanged;
- V4 emits no activity request when there is no tracked consumer;
- provider-side V4 filtering/batching is proven or its unfiltered cost is explicitly bounded;
- restart/failover does not replay completed observation intervals;
- structured/event persistence has an explicit retention decision backed by consumer proof;
- memory/file-cache/WAL/storage remain bounded under production-shaped history;
- full CI and final-composition regressions are green.

## Current success-criteria status

| Criterion | Status |
| --- | --- |
| Why private inquiry volume explodes | **PROVEN** — public research getLogs is globally redirected to Validation Cloud |
| Largest observed amplification | **PROVEN** — ~97 VC requests for one exact range |
| V2/V4 incremental request shape | **PROVEN** — up to 6 logical requests at default pair cap, 18 at source maximum |
| Robinhood memory/page-cache contribution | **PROVEN and materially repaired by PR #405** |
| Same broad research coverage with less private cost | **REPAIR IMPLEMENTED ON PR #409; CI REQUIRED** |
| V2/V4 pause fail-safe | **REPAIR IMPLEMENTED ON PR #409; CI REQUIRED** |
| V4 zero-consumer request | **STILL OPEN** |
| Duplicate V2/V4 persistence | **CANDIDATE DUPLICATION; consumer proof required** |
| Safe to reactivate V2/V4 | **NO** |

## Target boundary

The desired architecture is:

**one necessary Robinhood observation -> correct provider authority for that observation -> one canonical evidence object -> version-appropriate interpretation -> one justified persistence decision -> bounded downstream work.**

Broad research must stay broad, but it does not need to consume the scarce private provider. Private/provider-budget capacity must remain reserved for the small set of observations that actually require decision-authoritative service quality.
