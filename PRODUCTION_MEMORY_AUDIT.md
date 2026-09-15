# Production Memory Audit

Date: 2026-09-10

## Executive conclusion

The production memory audit is complete. The system is **not memory-certified** on the current 2 GiB authoritative Render instance.

Exact current GitHub/production release under final verification: `0fbe9e7aee9bc5b923cb537891982233c79cfa90`.

The recurring certification failure is now conclusively attributed to **history-scaled canonical SQLite file-cache/writeback pressure**, not to a Python heap leak and not to thread proliferation during the reproduced current failure. A second, historically observed failure mode involving extreme task/thread proliferation remains a residual certification risk and must not be conflated with the current file-cache failure.

Final verdict: `MEMORY_REPAIR_NOT_CERTIFIED`

## Exact-release production proof

Both authoritative `solana-trading-bot` and isolated `solana-trading-bot-certifier` reached exact release `0fbe9e7aee9bc5b923cb537891982233c79cfa90`.

On the fresh authoritative instance, startup still traversed history-scaled canonical state. Main-thread stall telemetry showed `_bounded_verify_engine_snapshot` during durable restore, followed by a later ~32.6 second stall in `certification_incremental_replication._ensure_tracking_locked`. Uvicorn completed startup at approximately 18:33:55 UTC.

The certifier then initiated a new logical bootstrap at approximately 18:34:20 UTC and began deterministic 250-row keyset pages. By logical-bootstrap cursor approximately 2,500, authoritative cgroup memory had already reached 1,708,998,656 bytes (79.58%), including 1,489,604,608 bytes of file cache and 262,856,704 bytes dirty, while the uvicorn process used only about 167 MiB anonymous memory.

By approximately 18:34:33 UTC, cgroup memory reached 2,147,274,752 bytes of the 2,147,483,648-byte limit (99.99%), leaving only 208,896 bytes of headroom. At that point:

- anonymous cgroup memory: 147,517,440 bytes;
- file cache: 1,944,768,512 bytes;
- dirty file pages: 251,768,832 bytes;
- kernel stack: 540,672 bytes;
- uvicorn anonymous RSS: approximately 143.7 MiB;
- uvicorn threads: 32;
- cgroup tasks (`pids.current`): 33;
- cgroup task-limit events: 0;
- cgroup OOM kills: 0.

The next logical-bootstrap page, at cursor approximately 3,000, returned HTTP 503 under the unchanged fail-closed memory guard.

This exact-release reproduction closes the primary attribution question: **the active recurrence is driven by file-cache/writeback accumulation caused by history-scale SQLite reads while certification bootstrap is replaying canonical tables.**

## Root-cause classification

| Cause | Status | Evidence / conclusion |
| --- | --- | --- |
| Python anonymous-memory leak | Excluded as primary current recurrence | Current failure reached the 2 GiB boundary with only ~144 MiB uvicorn anonymous RSS. Earlier anonymous-memory repairs remain effective in normal operation. |
| Canonical SQLite file-cache pressure | **Confirmed primary blocker** | Current release reproduced ~1.945 GB file cache and a fail-closed 503 within seconds of logical bootstrap. |
| Dirty/writeback amplification | **Confirmed contributor** | Dirty pages repeatedly measured in the ~150-490 MB range; WAL/checkpoint/writeback work does not reclaim enough cache to preserve safe headroom. |
| Reclaim mechanism insufficiency | Confirmed | Render does not permit the application's privileged `memory.reclaim` path; POSIX cache advice, heap trim, writeback and WAL checkpointing are insufficient against repeated history-scale reads. |
| Release/deploy-driven replica rebootstrap | **Confirmed systemic amplifier** | A new certifier deployment begins logical bootstrap from early tables instead of continuing from a durable compatible replica. Exact release remains correctly enforced for certification artifacts, but release/deploy lifecycle currently causes database history to be reread. |
| Certifier replica durability | **Confirmed architecture gap** | The certifier service has no persistent Render disk. Replica/checkpoint state is therefore not durable across instance replacement/deployment. |
| Release SHA used as replica compatibility identity | **Confirmed architecture issue** | Existing resumable-bootstrap contract invalidates partial replica state when release identity changes. Release identity is appropriate for artifact truth but is unnecessarily strict for compatible database replica continuity when replication epoch/schema identity remain valid. |
| Full event-ledger integrity verification on authoritative startup | **Confirmed scaling risk** | Current startup still executes `_bounded_verify_engine_snapshot`; prior repairs bound its cache behavior but deliberately preserve full hash-chain verification. It materially lengthens startup and warms canonical history before certification begins. |
| Runaway task/thread mode | Historically confirmed; not reproduced in current failure | Prior exact-live telemetry observed roughly 12,770 simultaneous Python/OS threads before a restart and large kernel-stack/slab charges. Current exact-release failure had only 32 uvicorn threads / 33 tasks, so it is not the cause of today's recurring bootstrap failure. Ownership of the historical runaway family still requires a recurrence capture or an explicit bounded-concurrency proof before final memory certification. |
| No-op SQLite update amplification | Secondary contributor / open repair | PR #333 safely suppresses unchanged conflict UPSERT updates in two ledgers. It can reduce dirty-page churn but cannot solve history-scale read cache by itself and is currently based on older main. |

## Why previous memory repairs did not permanently solve the problem

The repairs addressed real defects but progressively exposed the next layer rather than eliminating the history-scaled workload itself:

1. large snapshot materialization was replaced with incremental/logical replication;
2. page sizes and anonymous response materialization were bounded;
3. raw-cgroup guards correctly prevented OOM/restart and returned 503;
4. file-cache advice, fdatasync and WAL checkpoints improved reclaimability;
5. resumable bootstrap preserved progress across transient failures within a surviving certifier filesystem;
6. detailed cgroup/process attribution proved that the remaining recurring limit is file cache.

The residual architecture still requires the authoritative service to reread a large amount of canonical history whenever the certifier loses or invalidates its local replica. Cache tuning cannot make that workload reliably fit inside a 2 GiB cgroup with sufficient production headroom.

## Permanent repair boundary

The permanent repair should be implemented in this order:

1. **Make certifier replica/checkpoint state durable across deployments.** Add certifier-owned persistent storage for the replica and replication sidecar/checkpoint. Do not share the authoritative writable SQLite disk and do not grant the certifier canonical write authority.
2. **Separate database-replica compatibility from certification release identity.** Preserve exact current release SHA on every certification artifact, but retain a local replica across a code release when replication protocol version, replication epoch, schema fingerprint/version and watermark continuity prove compatibility. Force bootstrap only for incompatible schema/epoch/protocol state, invalid watermark, corruption, missing durable state or explicit recovery.
3. **Make authoritative integrity verification incremental and tamper-evident.** Persist a validated integrity checkpoint containing the verified terminal event ID/hash and database/schema identity, verify the checkpoint itself, and verify only the append-only tail on ordinary startup. Retain an explicit bounded full-history audit/reconciliation path for independent integrity assurance; do not simply skip hash-chain verification.
4. **Reduce write amplification after the history-read architecture is fixed.** Rebase and revalidate the useful no-op update suppression from PR #333 against current main.
5. **Close the historical runaway-thread risk.** Require bounded ownership for every long-lived worker/executor and retain `pids.current` + per-process thread telemetry. If the high-task mode recurs, capture the owning thread family before changing strategy/runtime semantics.

## Memory budget required for certification

A passing production design should not depend on touching the 2 GiB hard boundary and hoping Linux reclaims cache. The authoritative service should preserve material operating headroom through startup, normal ingestion, Robinhood + Solana concurrency, certification replication and recovery.

A reasonable acceptance target on the current 2 GiB instance is to keep sustained total cgroup usage below roughly 75-80% during normal certification work, with transient peaks remaining materially below the critical fail-closed boundary. The exact budget should be justified by measured post-repair telemetry rather than by lowering the guard.

At minimum the post-repair budget must separately track anonymous process memory, clean file cache, dirty/writeback cache, kernel/slab, WAL/temp growth, task/thread count and certification-replication I/O.

## Certification acceptance criteria

Memory repair may be certified only after all of the following are proven on one exact GitHub/Render SHA:

- authoritative and certifier exact-SHA equality;
- no history-from-zero bootstrap on an ordinary compatible redeploy/restart when durable replica state is valid;
- bounded startup integrity verification without a history-scaled hot-path scan;
- multiple consecutive logical/incremental certification cycles with no progressive raw-cgroup growth;
- no normal-operation memory-guard 503s;
- no authoritative restart attributable to memory/resource pressure;
- no OOM/oom_kill events;
- bounded anonymous RSS;
- bounded and naturally reclaimable file cache with meaningful headroom;
- dirty/writeback pages do not accumulate toward the cgroup ceiling;
- WAL/temp/snapshot files remain bounded;
- task/thread counts remain bounded and do not reproduce the historical runaway mode;
- Robinhood and Solana lanes can operate concurrently through the certification window;
- paper-only, signing-disabled, submission-disabled, no-live-money, fail-closed and exact-evidence controls remain unchanged.

A single successful run is insufficient. Certification requires repeated cycles and a redeploy/restart continuity test because deployment currently participates in the failure mechanism.

## Safety/governance confirmation

No audit change lowered a strategy threshold, changed v5.2 economic authority, granted live-money capability, enabled signing or transaction submission, reset canonical history, deleted evidence, weakened exact-release truth, weakened certification thresholds, weakened stale/continuity gates or bypassed the fail-closed resource guard.

The fail-closed 503 behavior is currently correct. It is exposing an architectural capacity defect and must not be disabled to make certification appear green.

## Final verdict

`MEMORY_REPAIR_NOT_CERTIFIED`

The audit itself is complete. The active production root cause is proven and reproducible: certifier history replay causes canonical SQLite file-cache/writeback pressure to fill the authoritative 2 GiB cgroup. Durable replica continuity plus compatible cross-release incremental replication are the highest-leverage repairs; incremental tamper-evident startup verification is the next authoritative scaling repair. Thread runaway remains a separate historical risk that must stay bounded/observable but is not the cause of the reproduced current failure.