# Robinhood Efficiency Audit

Date: 2026-09-15

Workspace: `My Workspace`

Authoritative service: `solana-roi-convergence`

Starting canonical SHA: `92f79a69bd7141f94f246e3c2e3539aa365a6ff9` (PR #405 merge)

Current repair PR: #409

Current stage: **REPAIR / PRE-REACTIVATION PROOF**

V2/V4 production configuration during repair: `ROBINHOOD_V2_V4_OBSERVATION_ENABLED=false`

## Executive verdict

Two materially different Robinhood resource defects were identified and separated.

1. **Memory/page-cache amplification** was caused in large part by high-frequency history-scaled SQLite status reads and deep proof reconstruction. PR #405 materially repaired this boundary by making high-frequency status publication in-memory, generation-gating deep proof work, and preventing ordinary raw-swap growth from forcing deep proof refreshes.
2. **Private-provider inquiry amplification** remained after PR #405. Production demonstrated an exact 110-block interval causing approximately 97 successful Validation Cloud `eth_getLogs` calls in several seconds with no failures, retries, range splits, or failovers. The dominant source is the broad public research screener being globally redirected into Validation Cloud.

V2/V4 did not cause the 97-call burst. With V2/V4 explicitly disabled in Render, the same broad request-count pattern remained. V2/V4 nevertheless contains separate inefficiencies and must remain disabled until all reactivation gates pass.

## Production baseline

### Memory / CPU before PR #405

On the pre-#405 V2/V4 release, fresh-process memory repeatedly grew toward approximately 1 GB and CPU was commonly around 0.8-0.95 CPU. This reproduced across restarts and supported SQLite/page-cache attribution.

### After PR #405

PR #405 materially lowered the status/proof resource burden. The service still has a 2 GiB memory limit, and later measurements remain well below the prior near-limit failure boundary.

### Explicit V2/V4-disabled baseline

During this audit, production was explicitly configured with:

`ROBINHOOD_V2_V4_OBSERVATION_ENABLED=false`

on the unchanged #405 release before any repair code was merged.

A fresh disabled instance measured approximately 294 MB at 16:12 UTC, 451 MB at 16:17, briefly about 894 MB at 16:18, and then stabilized around 799-834 MB through the sampled interval. CPU was approximately 0.31-0.79 CPU.

Crucially, broad same-range Validation Cloud request bursts still occurred and provider request counters reached the ~97-request shape with V2/V4 disabled. Therefore V2/V4 is not the root cause of the dominant broad private-provider fan-out.

## Root cause: public research crossed the private-provider boundary

The provider-budget architecture deliberately constructs an RPC client targeted at the official public Robinhood RPC for broad paper-eligible market research.

The intended flow is:

`all-market research -> official public Robinhood RPC -> ranking/promotion -> bounded private live provider`

The installed global `eth_getLogs` provider guard instead preferred Validation Cloud whenever it was configured, including for that explicitly public research client.

The effective flow became:

`all-market research -> global getLogs guard -> Validation Cloud`

The broad research screener uses 64-address batches. Approximately 97 batches are consistent with roughly 6.1k-6.2k market-address slots in the persisted research universe.

The lower-level provider failover wrapper already exempts an RPC client whose endpoint is the official public Robinhood RPC. Therefore isolating the getLogs dispatch seam is sufficient to keep explicit public research public end-to-end while retaining private failover for private clients.

## Repair: public research provider isolation

PR #409 installs explicit public-research getLogs isolation at the governed dispatch seam.

Required semantics:

- official-public research `eth_getLogs` stays on the official public endpoint;
- Validation Cloud does not capture broad public research merely because it is configured;
- private/live acquisition continues through the existing provider guard, range bounding, retries, verification, and failover;
- installer order cannot remove the separation;
- the candidate universe is unchanged.

No v5.2 thresholds, economics, candidate authority, signing, transaction submission, or live-money authority are changed.

## Repair: V2/V4 fail-safe scheduling

The historical V2/V4 source default could evaluate enabled when the deployment variable was absent. That conflicts with the repair boundary.

PR #409 makes production scheduling explicit-opt-in:

- missing -> disabled;
- blank / `0` / `false` / `no` / `off` -> disabled;
- `1` / `true` / `yes` / `on` -> enabled.

A CI regression exposed that the first pause-guard composition also suppressed direct observer resume tests. The repair now separates **schedule authority** from the **observer primitive**:

- production fetch/scheduling checks the enable flag;
- the observer primitive itself remains exact and resumable when explicitly invoked for deterministic recovery/tests;
- disabled production scheduling cannot silently invoke the observer.

## Repair: V4 zero-consumer acquisition

Previous V2/V4 observation sent a V4 PoolManager Swap request even when zero tracked V4 pools could consume the result.

The repair enforces:

`tracked_v4_pools == 0 -> V4 Swap activity requests == 0`

Discovery remains available so new V4 pools can still be learned.

## Repair: tracked V4 pool filtering

Previous V4 activity acquisition requested all PoolManager Swap events for the interval and discarded untracked pool IDs only after download and decode.

The repaired query uses Ethereum JSON-RPC topic-array semantics to constrain `topic1` to the exact tracked pool IDs:

`topics = [V4_SWAP_TOPIC, [tracked_pool_id_1, ...]]`

This preserves tracked-pool coverage while eliminating unrelated PoolManager Swap logs from provider response buffers, decoding, sorting, and local filtering work.

## Repair: expected-versus-actual request budget

PR #409 makes inquiry volume a first-class runtime invariant.

Public research records per pass:

- V3 market count;
- V2 market count;
- batch size;
- expected getLogs requests;
- actual getLogs requests;
- cumulative expected/actual requests;
- unexplained-budget violations.

The deterministic public-research formula is:

`ceil(v3_markets / 64) + ceil(v2_markets / 64)`

when the research frontier advances, otherwise zero.

V2/V4 range telemetry separately records expected and actual market activity request counts from tracked V2 and V4 sets.

Private Validation Cloud proof counters remain exposed for attempted/succeeded/failed ranges, splits, and fallbacks.

Unexplained request amplification is a certification failure rather than an acceptable steady state.

## Repair: research-universe SQLite read amplification

The broad research loop previously rebuilt the full persisted paper-eligible `robinhood_launches` dictionary from SQLite on every research pass.

The research cadence is approximately five seconds and the universe is thousands of markets, so this repeatedly touched immutable historical/current-release launch rows.

The launch table is safe for incremental caching because the canonical write path is append-only for a release cohort:

- `UNIQUE(release_commit, protocol, token)`;
- writer uses `INSERT OR IGNORE`;
- existing launch rows are not later mutated by the canonical writer.

The repair therefore:

1. loads all current-release paper-eligible launch rows once;
2. records the last row ID;
3. on subsequent passes reads only `id > cached_last_id` for that release;
4. reproduces the original candidate descriptor semantics exactly;
5. resets on release change;
6. never evicts a market for provider-budget purposes.

This changes database work, not market scope.

## Checkpoint / retry / failover boundary

V2/V4 already has a durable observation cursor and bounded recovery. The repaired observer preserves:

- resume from observer cursor + 1;
- bounded recovery/re-anchor behavior;
- cursor advancement only after successful requested-range processing;
- idempotent structured swap insertion;
- no production scheduling when the enable gate is false.

Private provider acquisition retains existing retry, range split, capability verification, and provider failover semantics. Public research is intentionally outside the private provider pool.

Reactivation requires deterministic regressions and live evidence that retry/failover do not replay completed work.

## Storage and retention matrix

| Surface | Policy | Rationale |
| --- | --- | --- |
| Raw HTTP/RPC response objects | **EPHEMERAL / DO NOT PERSIST** | Only decoded canonical evidence is required; provider buffers must die after processing. |
| Public research decoded logs | **EPHEMERAL / DO NOT PERSIST** except normalized research events | Broad raw scan data should not become an unbounded duplicate archive. |
| `robinhood_launches` | **RETAIN BOUNDED HISTORY / certification-defined release lineage** | Durable discovery universe and point-in-time launch metadata; current-release hot path is incrementally cached. |
| V2 observation registry | **KEEP LATEST N** | Source already enforces a bounded registry. |
| V4 observation registry | **KEEP LATEST N** | Source already enforces a bounded registry. |
| V2/V4 observation cursor | **KEEP LATEST 1** | Recovery frontier; old cursor versions add no operational value. |
| `robinhood_swaps` | **RETAIN BOUNDED HISTORY** | Canonical normalized swap evidence used for analysis/replay; exact destructive horizon must be certified before cleanup. |
| `robinhood_swap_observation` | **RETAIN BOUNDED AUDIT LINEAGE** | Carries V2/V4 observation version/source/raw decode and explicit no-execution/paper-only metadata not present in the structured swap row. |
| `robinhood_market_observation` | **RETAIN BOUNDED AUDIT LINEAGE** | Records observational V2/V4 discovery and authority semantics; does not authorize execution. |
| High-frequency status snapshots | **EPHEMERAL / DO NOT PERSIST AS HISTORY** | PR #405 boundary; status is operational view, not a historical data lake. |
| Deep proof snapshots | **KEEP LATEST / generation-addressable evidence only where certification requires** | Avoid five-second history-scaled reconstruction and duplicate snapshot growth. |
| Provider health/quarantine state | **KEEP LATEST + bounded incident history** | Needed for routing and debugging, not permanent per-poll retention. |
| Request-budget counters | **KEEP LATEST counters + bounded anomaly evidence** | Detect amplification without creating another high-frequency data lake. |
| Open-position state | **RETAIN UNTIL SETTLED + required lineage** | Cannot be pruned while position authority depends on it. |
| Settled paper trial/outcome evidence | **RETAIN BOUNDED CERTIFICATION HISTORY** | Needed for forward performance/strategy evaluation; deletion boundary must be independently proven. |

No destructive cleanup is authorized by this audit. The immediate objective is to stop avoidable acquisition/read/write accumulation at source.

## Structured swap versus observation event

The V2/V4 structured swap and append-only observation event are not semantically identical.

The structured row contains the canonical normalized market fact used for analytics. The append-only event additionally carries:

- observation version;
- acquisition source;
- V2/V4 raw decoded fields;
- `paper_eligible=false`;
- `execution_authorized=false`;
- paper-only/no-live-money lineage.

Because that authority/provenance is unique, this audit does **not** remove the event at source. It should receive a bounded audit-lineage retention policy rather than be treated as an accidental duplicate.

## Regression requirements

PR #409 must remain unmerged until all required CI is green, including:

- public research remains public;
- private clients remain governed;
- composition order is idempotent;
- disabled V2/V4 schedule does not invoke observation;
- explicit enable invokes observation without changing canonical market results;
- direct observer cursor resume remains exact;
- zero tracked V4 pools issue zero V4 activity requests;
- tracked V4 pools are sent as provider-side topic1 filters;
- expected-versus-actual request budget matches the batch formula;
- unexplained request multiplication is flagged;
- research universe initial load preserves canonical descriptors;
- subsequent universe reads load only new rows;
- release changes reset the research cache;
- other-release and ineligible rows never enter the cache;
- all existing architecture, strategy, paper-only, statistical, exit, certification and memory regressions remain green.

## Pre-reactivation deployment gate

After PR #409 is fully green and merged:

1. deploy the exact merge SHA with V2/V4 still explicitly disabled;
2. confirm authoritative Render release SHA;
3. prove broad research universe size is unchanged;
4. prove public research expected requests equal actual requests;
5. prove Validation Cloud no longer receives the broad ~97-batch research pass;
6. prove private acquisition, retries, splits, and failovers remain bounded;
7. verify memory remains within the repaired #405 boundary;
8. verify Robinhood health and candidate production remain functional.

Only after all eight pass may V2/V4 be reactivated.

## Reactivation command

When and only when the pre-reactivation gate is green, apply to the authoritative Render service in My Workspace:

`ROBINHOOD_V2_V4_OBSERVATION_ENABLED=true`

Then verify the resulting deployment/restart is still the exact repaired canonical code SHA.

## Post-reactivation gate

After enabling V2/V4, compare against the disabled baseline and require:

- no return of broad same-range private-provider fan-out;
- V2 request count equals its bounded batch formula;
- V4 request count is zero with zero tracked pools;
- V4 tracked-pool queries remain topic-filtered;
- expected requests match actual requests apart from explicitly measured bounded retry/split overhead;
- cursor advancement remains contiguous and restart-safe;
- no unexplained retry/failover replay;
- memory/file-cache/WAL/storage remain bounded;
- intended research universe and V2/V3/V4 coverage remain unchanged;
- v5.2 economics and paper-only authority remain unchanged.

If any post-reactivation gate fails, immediately restore:

`ROBINHOOD_V2_V4_OBSERVATION_ENABLED=false`

preserve the evidence, repair the exact defect, and rerun certification.

## Merge / deployment / post-resume evidence

To be filled from exact live release evidence after the current repair head passes complete CI.

- Repaired merge SHA: **PENDING**
- Pre-reactivation production SHA: **PENDING**
- Validation Cloud private getLogs before/after: **PENDING REPAIRED DEPLOYMENT**
- Public research expected/actual request proof: **PENDING REPAIRED DEPLOYMENT**
- Pre-reactivation memory/CPU proof: **PENDING REPAIRED DEPLOYMENT**
- V2/V4 resume action: **PENDING ALL GATES**
- Post-reactivation request proof: **PENDING**
- Post-reactivation memory/storage proof: **PENDING**

## Current state

**ROBINHOOD REPAIR INCOMPLETE — V2/V4 REMAINS PAUSED**
