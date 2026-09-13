# Memory attribution on canonical 92c0c162 — evidence only

**Root cause: ATTRIBUTION INCOMPLETE**

**Merge decision: DO NOT MERGE — MORE ATTRIBUTION REQUIRED**

No runtime repair, merge, deployment, production deletion, cleanup enablement, worker disablement, provider-coverage reduction, guard change, strategy/certification threshold change, paper-authority change, or resize was performed.

## Canonical state

- Main, remotely verified: `92c0c1620f78116e7ecbeade039e9aedaf3a51a9`.
- PR #379: open, draft, unmerged; head `2feef30f140faa22d4a79bec6dc5aea933d4b271`.
- Authoritative: same SHA in live composition, cleanup disk-ownership status, and Render deployment `dep-daj0l2dg1s2s73c364bg`.
- Certifier: same SHA in live `/health` and Render deployment `dep-daj0grcs728c73av52q0`; 0 successes, 15 failures, LogicalBootstrapPause, no completed replica/bootstrap.
- Cleanup: `enabled=false`, `status=disabled`, verified through production `/v1/operations/production-data-cleanup`.
- Render workspace: user-confirmed My Workspace, `tea-d9l2potbedkc73bog020`. Authoritative `srv-dabnrtn40ujc739r19vg`, certifier `srv-dagp82eq1p3s73bvce3g`.

## Fresh production evidence

Early fresh sample: 2,147,383,296 bytes cgroup usage, 1,566,048,256 bytes anonymous memory (~1,493.5 MiB), 552,828,928 bytes file cache (~527.2 MiB), dirty 36,864 bytes, writeback 0, zero OOM/OOM-kill. Later a successful publication was briefly fresh; this is intermittent recovery, not sustained stability.
The bounded eight-sample window covered publication worker timestamps 05:25:03–05:28:51 UTC on 2026-09-13. Successes stayed 72; guard rejections increased 585→600; age increased 67.707→296.850 seconds against 45 seconds. Memory fractions ranged from about 91.1% to 100%. The publication path has its own existing 90% post-build guard; the bootstrap guard remains 94%. No thresholds changed.

## Corrected response lifecycle

Locked FastAPI 0.141.1 / Starlette 1.6.0, Python 3.11.16. Actual type: `starlette.responses.Response`, not JSONResponse. FastAPI routing uses the Pydantic dump_json branch, then Response(content=serialized_bytes, media_type="application/json"). The Response body owns those serialized bytes until the object releases them.
The canonical logical-bootstrap endpoint and lifecycle gate run unmodified. A minimal FastAPI app installs those exact route modules; this response experiment does **not** claim the entire production installer stack or production worker composition. The separate worker experiment uses the actual production entrypoint.
Same page request limit: 250. Existing 4 MiB byte bound returns 170 rows; 171 cursor rows are fetched. Body: 4,193,584 bytes. Logical row/content digest is equal across stores after excluding store identity/release metadata. All five lifecycle pages execute the concrete Response hook. Removing that hook fails with `AssertionError: Concrete production Response hook did not execute`.
The ASGI sender only hashes/counts body bytes, retaining no response body. Response lifetime is observed using weak references/finalization. SQLite fetch and Python conversion actually interleave; first and last row boundaries plus completed-page materialization are recorded rather than fabricating an all-rows-fetch phase. The call-return measurement is immediately after the original Response.__call__ returns inside the diagnostic wrapper; a later ASGI-return boundary asserts that the weak reference is dead.

Representative warmed request (page index 1), absolute MiB:

| Boundary | 1× process anon | 1× cgroup anon | 1× clean cache* | 10× process anon | 10× clean cache* |
|---|---:|---:|---:|---:|---:|
| Before next page enters | 39.934 | 143.984 | 1631.254 | 39.941 | 1408.977 |
| Before SQLite page SELECT | 40.066 | 144.602 | 1631.273 | 40.074 | 1408.977 |
| After last SQLite fetch (streamed) | 40.137 | 135.746 | 1647.262 | 40.141 | 1424.672 |
| After last row materialization | 40.137 | 135.746 | 1647.262 | 40.141 | 1424.672 |
| After page materialization | 40.137 | 135.746 | 1647.262 | 40.141 | 1424.672 |
| After serialization / Response creation | 48.137 | 143.738 | 1647.262 | 48.137 | 1424.672 |
| Before first ASGI body send | 48.137 | 143.738 | 1647.262 | 48.137 | 1424.672 |
| After final ASGI body send | 48.137 | 143.738 | 1647.262 | 48.137 | 1424.672 |
| Response alive, before existing trim | 48.141 | 143.742 | 1639.254 | 48.137 | 1402.043 |
| Response alive, after existing trim | 43.961 | 139.562 | 1639.254 | 43.957 | 1403.043 |
| Original Response.__call__ returned; wrapper still owns self | 43.969 | 139.562 | 1639.254 | 43.965 | 1403.043 |
| ASGI app returned; Response weak reference dead | 43.969 | 139.562 | 1639.254 | 43.969 | 1403.043 |
| After extra diagnostic trim/cache cleanup | 39.961 | 135.566 | 1639.254 | 39.957 | 1403.043 |

*Clean cache is cgroup-wide and contaminated by other activity. It is not a valid per-process attribution measurement in this environment. All raw events also contain RSS, cgroup anonymous bytes, dirty/writeback, DB/WAL/SHM sizes, rchar/read_bytes, threads, pids.current, timestamps, and monotonic time.
In the warmed request, original Response.__call__ return and final reference release caused essentially no additional immediate RSS drop; about 4 MiB remained allocator-resident until an extra diagnostic trim. In the initial cold request the body release did reduce RSS. This is allocator-dependent bounded retention, not proof of the production sustained-memory mechanism.
Control without extra GC/trim/cache cleanup between requests: 20 pages in fresh processes for each store. 1× post-response anonymous RSS was 39.95 MiB on page 0, peaked about 44.04 MiB early, and finished 40.77 MiB; 10× behaved essentially identically. These runs preserve the original production cleanup only. They do not reproduce hundreds of MiB of sustained anonymous growth. Short runs do not rule out retention under concurrent real production traffic.

## 1× / 10× comparison

Synthetic schema-shaped baseline: 50,000 rows / 1,256,415,232-byte SQLite DB. 10×: 500,000 rows / 12,564,168,704 bytes. Both use 24 KiB reason strings, deliberately stressing the actual response byte bound. This approximates GB-scale history, **not the full production distribution/cardinalities**. Live production DB is 2,209,046,528 bytes with substantial freelist space. Index name in the endpoint fixture comes from the prior canonical test; column definition/index coverage matches. The full-composition disposable copy additionally has the exact production index name.
Warm-page metrics below are medians of request indexes 1–4. VM steps come from a separate unmodified `_page` run with only a progress handler, excluding EXPLAIN observer overhead. The richer lifecycle probe counts 2,902 callbacks including 7 observer callbacks; the clean measurement is 2,895 for both.

| Metric | 1× | 10× | 10× / 1× |
|---|---:|---:|---:|
| Historical rows | 50,000 | 500,000 | 10.000 |
| VM steps | 2,895 | 2,895 | 1.000 |
| Cursor rows fetched / response rows | 171 / 170 | 171 / 170 | 1.000 |
| Internal rows visited (NVISIT) | unavailable | unavailable | unavailable |
| Response bytes | 4,193,584 | 4,193,584 | 1.000 |
| Physical read bytes | 8,396,800 B | 8,396,800 B | 1.0000 |
| Anonymous increase at serialization | 8,591,360 B | 8,589,312 B | 0.9998 |
| Raw clean-cache increase at serialization* | 16,785,408 B | 16,490,496 B | 0.9824 (not attributable) |
| Residual anon before extra trim | 4,231,168 B | 4,227,072 B | 0.9990 |
| Raw residual clean cache before extra trim* | 8,388,608 B | -4,161,536 B | -0.4961 (not attributable) |
| Residual anon after extra trim | 20,480 B | 18,432 B | 0.9000 |
| Raw residual clean cache after extra trim* | 8,388,608 B | -7,286,784 B | -0.8687 (not attributable) |

SQLite lacks `ENABLE_STMT_SCANSTATUS`; fetched rows must not be mislabeled NVISIT. Main SELECT plan: `SEARCH anonymous_candidate_latency_failures USING INTEGER PRIMARY KEY (rowid>?)`; no temp B-tree. SQL:

```sql
SELECT rowid,"id","failed_at","reason","outcome","count","max_age_ms"
FROM "anonymous_candidate_latency_failures"
WHERE rowid>? ORDER BY rowid LIMIT ?;
```
Parameters `(0, 251)`. Caller: `certification_logical_bootstrap._page` → `_stream_page_records`. One page SELECT per request, same keyset/output. The 10× history does not increase measured query or response work.

An idle-control process performed zero physical reads and only 26,073 rchar bytes while cgroup clean cache increased 251,498,496 bytes (~240 MiB) over 11 seconds. Therefore clean-cache deltas/ratios above are raw observations, not evidence of SQLite refault attributable to the probe. An exclusive cgroup or file-specific residency measurement is still required.

## Worker activation and material intervals

Actual Render start command verified: `uvicorn solana_roi.production:app --host 0.0.0.0 --port $PORT`. Local experiment uses uvicorn.Server with the identical ASGI import/entrypoint on localhost:8768 and a disposable SQLite copy; 45 literal/non-secret configuration values are taken from the canonical Render blueprint plus explicit disposable paths/release binding and disabled cleanup. Private production provider bindings and the real certifier peer are absent. No canonical DB is accessed.
First attempt failed before worker activation because local SOCKS transport support was missing. Local-only `socksio` was installed; production dependencies/lockfile were unchanged. Second run completed the bounded 90-second observation. Public Solana notifications occurred. Robinhood had no private provider binding, and proxy/ReadTimeout/InvalidStateError failures occurred. This is not a production-equivalent steady-state or 2 GiB reproduction.
Task hooks measure before the first coroutine slice and after its first suspension; thread hooks measure scheduling, not thread work completion. SQL hooks measure execute/fetchall/fetchmany intervals, retain caller/SQL/frequency/query plan, and do not retain returned row bodies. Measurement overhead is material for small SQL calls; timings are diagnostic rather than performance benchmarks. SQL `count` includes execute and instrumented fetch operations, not always logical-query frequency.

| Required area | Observed locally | Attribution limit |
|---|---|---|
| Direct-Solana ingestion/hydration | Worker/task activations, provider-state updates, receipts, hydration claims | Real production history and full provider load absent |
| Wallet discovery/intelligence | StartupIsolatedWalletDiscovery.run and selection/research SQL | Historical wallet population not production-shaped |
| Shadow price | ShadowPriceClock activation, tracking queries | Real token population not reconstructed |
| Forward evidence | Forward hydration claims and compatibility worker | Forward history mostly empty |
| Runtime restoration | Real import/build/bootstrap executed | Synthetic latency history only; real append-only event/checkpoint state absent |
| Certification/replication | Proof workers and replication schema/triggers | No authenticated certifier peer/bootstrap traffic |
| Portfolio/control and lifecycle | Paper lifecycle/task/control SQL | No realistic open-position/state history |
| Provider polling | Public Solana activity; Robinhood supervisor | Private bindings missing; proxy failures |
| Storage maintenance | Existing prune/checkpoint phases | Concurrent intervals, no exclusive attribution |

Local anonymous memory rose 59.39→101.45 MiB over the sampled startup/90-second window, with 4→23 threads. Physical reads rose only 126,976→495,616 bytes while cgroup clean cache rose ~1.71 GiB. Idle control proves that global cache rise cannot be assigned to these workers.
Local first-slice material anonymous intervals: wallet startup +4.34 MiB; Robinhood isolated worker +9.79 MiB; smaller wrapper/forward intervals ~1 MiB. They overlap other tasks; no causal attribution is claimed. Every interval at the 2 MiB resource or 100 ms duration threshold, plus SQL caller/frequency/plans, is retained in `worker-events.jsonl` inside the evidence archive.

Production startup phase intervals (same instance `srv-dabnrtn40ujc739r19vg-s6rcg`, 2026-09-13):

| UTC end | Worker/phase | Duration | Anon delta | Clean-cache delta | Physical reads | Causal proof |
|---|---|---:|---:|---:|---:|---|
| 2026-09-13T02:30:18.250658848Z | direct-solana-storage-maintenance:prune | 84.778 ms | 5.39 MiB | 16.24 MiB | 15.82 MiB | No; overlapping process/cgroup intervals |
| 2026-09-13T02:30:18.722887469Z | worker:direct-solana-ingestion:activated | 2090.018 ms | 30.43 MiB | 219.82 MiB | 219.19 MiB | No; overlapping process/cgroup intervals |
| 2026-09-13T02:30:18.726133467Z | worker:continuous-wallet-discovery:activated | 2005.830 ms | 26.07 MiB | 219.75 MiB | 219.60 MiB | No; overlapping process/cgroup intervals |
| 2026-09-13T02:30:18.737549771Z | worker:shadow-price-clock:activated | 618.261 ms | 11.24 MiB | 109.90 MiB | 109.37 MiB | No; overlapping process/cgroup intervals |
| 2026-09-13T02:33:13.003036246Z | direct-solana-storage-maintenance:prune | 515.544 ms | 1.07 MiB | 10.21 MiB | 6.86 MiB | No; overlapping process/cgroup intervals |

These three activation intervals are generated by `sqlite_phase_observability._async_worker_phase` using a scheduled after-activation marker. The direct and wallet intervals overlap almost entirely, and the shadow interval overlaps their tail. They cannot identify the responsible SQL. Do not sum their deltas.
100 later production phase records span about 04:38–05:28 UTC. They predominantly cover storage prune/checkpoint and shadow tracked-mint phases, not every worker/query or response allocation. One 05:03:38 prune interval coincides with -184.13 MiB clean cache and -4.87 MiB anon; it does not prove pruning caused recovery. The logs contain other concurrent work and no per-query attribution. Full records are archived.

Concrete repeated SQL locations captured locally include (frequencies aggregate the SQL shape across all callers; the retained stack is the first observed caller, so counts cannot be assigned solely to that stack):

- `v52_profit_confidence_completion._hydrate_provider_stats` → `_table_exists`: `SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1` (2,895 instrumented operations).
- `robinhood_pumpfun_wallet_selection._candidate_research_rows` → `_candidate_forward_profile`: `SELECT token,mark_return FROM robinhood_wallet_selection_forward WHERE actor=? AND side='buy' AND mark_return IS NOT NULL ORDER BY swap_id` (2,124 execute/fetch operations).
- `public_data_economics._process_selective_notification_sync` → `direct_solana.touch_provider`: `UPDATE direct_solana_provider_state SET last_message_at=? WHERE provider=?` (2,146 operations).
- `forward_evidence_runtime_repair._claim_forward_work` → `candidate_completion_continuity_repair._deadline_aware_claim_candidate`: pending hydration-queue SELECT (full SQL and plans in archive).
No listed SQL is established as the cause of production sustained pressure. SQL plans showing scans/sorts against locally empty non-latency tables are candidate locators, not quantitative causal proofs.

## Decision and smallest next experiment

**Outcome D: ATTRIBUTION INCOMPLETE. DO NOT MERGE — MORE ATTRIBUTION REQUIRED.**
Missing measurement: exclusive per-process/worker allocation and file-residency/SQL evidence during the actual sustained anonymous increase, with the production history distribution and active provider/replication composition. Existing production logs label overlapping intervals; they cannot identify an allocator owner or query. The local test cannot provide an exclusive cache measurement.
Smallest next experiment: reproduce one production startup/publication interval in an exclusive 2 GiB disposable environment, using the exact production composition and representative event, wallet, hydration, forward/certification histories with recorded provider responses. Capture per-thread allocation stacks and SQLite execute/fetch/cursor-lifetime records around the observed anonymous increase; collect per-file residency before/after, VM steps, and exact query plans. Then replay the single implicated operation alone with 1×/10× history and a no-operation control. Do not change runtime behavior unless that isolated operation reproduces the production-sized residual. Provider traffic and coverage must remain represented in the replay.
The corrected lifecycle probe should also be run against that complete composition, with concurrent bootstrap traffic and no extra diagnostic trim. If real Python/body ownership is implicated, a failing regression must precede the smallest repair. This turn does not justify PR #379 or another runtime change.

## Reproduction and artifacts

Diagnostic scripts are intentionally separate from `src/solana_roi`; no runtime module or existing test changed. `response_diagnostic.py` and `worker_diagnostic.py` contain absolute disposable-path safety assertions for this workspace; adapt only that scratch-root constant in a new environment. The worker probe needs `composition-env.json` and local transport-only socksio if the environment uses a SOCKS proxy.
```bash
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py seed disposable/history-1x.sqlite3 --rows 50000
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py seed disposable/history-10x.sqlite3 --rows 500000
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py probe disposable/history-1x.sqlite3 --output response-1x.json
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py probe disposable/history-10x.sqlite3 --output response-10x.json
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py probe disposable/history-1x.sqlite3 --output response-natural-1x.json --natural
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py probe disposable/history-10x.sqlite3 --output response-natural-10x.json --natural
# Must fail: deliberately remove the concrete Response instrumentation hook.
PYTHONPATH=canonical/src .venv/bin/python response_diagnostic.py probe disposable/history-1x.sqlite3 --output negative-control.json --skip-hook
```
Raw evidence is in `measurements.zip`; large disposable databases are excluded and can be recreated. The canonical SQL/row-output comparison is in `logical-comparison.json`. Production startup/later phase logs, public health/cleanup/composition observations, and local worker/idle-control observations are included. Not all 30-day production logs were fetched; bounded queries and their limits are retained. No full CI was run because no runtime repair is proposed.
