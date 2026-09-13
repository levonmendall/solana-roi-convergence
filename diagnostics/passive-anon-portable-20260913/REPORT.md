# Passive production anonymous-memory ownership and portable reproduction

## Canonical safety state

- Canonical main: `92c0c1620f78116e7ecbeade039e9aedaf3a51a9`, remotely reverified.
- Authoritative release: same SHA, returned by the existing cached cleanup/disk-ownership endpoint.
- PR #379: open, draft, unmerged; head `2feef30f140faa22d4a79bec6dc5aea933d4b271`.
- Cleanup: `enabled=false`, `status=disabled`.
- Production mutation performed: **NO**.
- Production data deleted: **NO**.
- Guards changed: **NO**.
- Strategy/provider/wallet/scouting behavior changed: **NO**.
- No restart, worker kill/disablement, extra work/publication cycle, profiler attachment, instrumentation deployment or service resize was performed.

## Track A — capability and safety

The canonical `memory_pressure_observability` module already samples `/proc` and
cgroup state every five seconds when pressure is high. Retrieving those existing
`ROI_MEMORY_DETAIL` logs through Render does not trigger application diagnostics.
Two bounded log queries retrieved 30 records each. Existing SQLite-phase logs were
read once for already-collected process I/O and DB/WAL context.

| Visibility | Finding |
|---|---|
| Authenticated production shell | Not available in this execution path. A bounded SSH attempt failed DNS resolution before authentication or command execution. |
| Application can read its own `/proc` | Proven by existing PID 39 status and smaps_rollup fields. |
| Sibling visibility | PID 1 (`bash`) is visible alongside PID 39 (`uvicorn`). Host-wide visibility is not established. |
| Cgroup membership | The installed sampler reads `cgroup.procs`; these records contain two process rows. It returns at most 16 rows, sorts after reading membership, and does not recursively enumerate child cgroups. The observed task count is consistent with the visible threads. Do not infer unrestricted host visibility. |
| `smaps_rollup` | Readable for both visible PIDs, as demonstrated by PSS/private/anonymous fields. |
| Full `smaps` | No existing exposed mapping-level surface found. Direct shell unavailable; not collected. |
| `/proc/status` | Selected fields exposed by existing sampler: threads, VmSize, VmRSS, RssAnon, RssFile. |
| `/proc/statm` | Existing forensics sampler reads current-process RSS; its full snapshot is not requested through the composition endpoint in this pass. |
| `/proc/io` | Existing SQLite-phase logs expose process-self rchar/read_bytes/wchar/write_bytes counters; per-PID I/O enumeration is not exposed. |
| PPID, exact process start, FDs, swap | Not present in the safely retrieved process records. No inferred values. |
| Python/allocator telemetry | No existing exposed GC/object-count/tracemalloc/arena/allocator-stats surface found in the canonical source review. Allocator trim implementation is not allocator measurement. |

The `/v1/strategy/forward-certification/cache` route returns an in-memory counter
snapshot under a lock and does not invoke the deep builder. It was read three times,
approximately 65 seconds apart including transport time. The cleanup status route
returns cached app-state values and was read once. These are the only application
HTTP observations added in this pass.

The composition endpoint was deliberately **not called** in this pass: its paper
lifecycle status path can execute capital-reconciliation/aggregate SQL. The current
prompt forbids uncertain diagnostic overhead, so a read-only label alone was not
treated as sufficient safety evidence. No full smaps scrape or new instrumentation
was attempted.

## Track A — production anonymous ownership

**ANON CONCENTRATED IN ONE VISIBLE PROCESS**

Latest included memory record: **2026-09-13 15:29:48.471 UTC**, same Render instance
`srv-dabnrtn40ujc739r19vg-s6rcg`.

| Metric | PID 39 — uvicorn | PID 1 — bash |
|---|---:|---:|
| Anonymous resident memory | 1,676.957 MiB | 0.359 MiB |
| RSS | 1,692.160 MiB | 3.145 MiB |
| PSS | 1,684.569 MiB | 0.411 MiB |
| Private/USS (private clean + dirty) | 1,684.141 MiB | 0.359 MiB |
| Private dirty | 1,676.977 MiB | 0.359 MiB |
| Private clean | 7.164 MiB | 0 MiB |
| File-backed RSS | 15.203 MiB | 2.785 MiB |
| Threads | 35 | 1 |
| Virtual size | 4,151.699 MiB | 4.395 MiB |

At the same log boundary, cgroup anonymous memory is **1,677.316 MiB**; PID 39's
anonymous resident total explains approximately **99.98%** of that amount.
The cgroup contains **345.055 MiB file cache**, and total usage is
**2,147,385,344 / 2,147,483,648 bytes (99.9954%)**. Reads are sequential and not an
atomic process/cgroup snapshot, so small accounting differences are expected.

The verified production entrypoint is `uvicorn solana_roi.production:app`.
PID 39 is the web/API process hosting multiple workers; identifying this process
does not identify a particular worker, Python object, allocator, native subsystem,
cursor lifetime or provider activity.

The live runtime ownership record names PID 39 and lease acquisition at
**2026-09-13 02:30:16.521 UTC**. This is an approximately 13-hour runtime-lease age,
not an exact `/proc/stat` process-age measurement. PID and Render instance remain
unchanged across the bounded memory observations; this is not a proof covering all
restarts outside that window.

### Mapping and thread-stack limits

- Largest private mapping class: **unknown**. smaps_rollup aggregates mappings and
  cannot distinguish `[heap]`, anonymous mmap, user stacks or native client regions.
- Thread-stack contribution: **not measurable with available mapping visibility**.
- Threads remain between **33 and 35**. Cgroup `kernel_stack` is about
  **0.531–0.563 MiB**, which is kernel-stack residency only and says nothing decisive
  about resident user-space stacks. Configured stack limits were not multiplied by
  thread count or used as resident-memory evidence.
- Python-visible explanation: unavailable. The ~1.68 GiB process anonymous total
  cannot be compared with absent Python allocation counters.
- Unexplained private anon: ownership within PID 39 remains unallocated to a
  subsystem; no exact Python-versus-native unexplained-byte subtraction is possible.
  Missing Python telemetry does **not** prove native retention.

### Bounded natural time series

60 pre-existing memory samples span **15:23:34.101–15:29:48.471 UTC**, in two
bounded blocks with a gap between them; this is not a continuous new profiler run.

| Metric | First | Last | Range |
|---|---:|---:|---:|
| Cgroup anonymous | 1,680.082 MiB | 1,677.316 MiB | 1,670.211–1,687.078 MiB |
| Cgroup file | 341.074 MiB | 345.055 MiB | 312.246–348.723 MiB |
| PID 39 anonymous | 1,679.723 MiB | 1,676.957 MiB | closely follows cgroup anon |
| PID 39 threads | 34 | 35 | 33–35 |
| Visible process count | 2 | 2 | 2 |

Anonymous memory has plateaued at an unhealthy level, with small natural increases
and decreases. The approximately 2.8 MiB net decrease is not recovery toward the
earlier 40–100 MiB local range. The worker/cycle cause remains unknown.

Three cached publication observations cover natural worker timestamps
**15:26:41–15:28:57 UTC**:

| Counter | First | Last |
|---|---:|---:|
| Attempts | 3,035 | 3,044 |
| Successes | 87 | 87 |
| Guard rejections | 2,948 | 2,957 |
| Consecutive failures | 904 | 913 |
| Publication age | 13,686.342 s | 13,816.932 s |
| Stale threshold | 45 s | 45 s |

No publication progress or meaningful anonymous-memory recovery is demonstrated.
The final publication age is approximately **3 hours 50 minutes**. Temporal overlap
with failures is not causal attribution. Earlier file-cache-dominant pressure
remains a separate unproven causal chain.

## Track B — infrastructure capability gate

**NO VALID EXCLUSIVE ENVIRONMENT AVAILABLE**

| Requirement | Current evidence |
|---|---|
| Existing execution boundary | Shared cgroup v2, `/proc/self/cgroup = 0::/` |
| Hard memory maximum | 15,032,385,536 bytes = 14 GiB |
| Exclusive 2 GiB membership | Not available/proven |
| Unrelated processes | Present; prior verified 23 members; this is not an application-exclusive boundary |
| cgroup controls | Read-only mount; not writable |
| Docker/Podman | No executable or daemon socket found |
| systemd | No running service manager/system bus |
| Kernel capabilities | Effective/bounding capability masks zero in the inspected process |
| Legitimate new-resource option | Render API advertises new native `1c-2g` service creation; none is provisioned or verified here. It lacks Docker/disk/process controls for the supplied portable harness. No service was created. |
| Storage and source | Scratch storage and exact canonical source are available; that alone is insufficient |
| Exact entrypoint | Available in canonical source |
| Full worker composition | Code available; no valid isolated run |
| Representative full state | Not supplied; prior synthetic latency-only fixture is insufficient |
| Production-shaped provider replay | No sanitized recordings/complete adapter supplied |

Track B stopped at this gate. No representative state was created or loaded for a
new run. The shared cgroup was not modified, no ulimit/RSS substitute was used, and
no attempt was made to bypass container permissions.

## Portable artifacts and validation

The accompanying README, Dockerfile, run.sh, gate.py, collect.py,
validate_inputs.py and environment-contract.json provide the commands,
configuration, missing inputs, provider-replay contract, measurement workflow and
acceptance criteria needed elsewhere. They preserve normal startup and production
application code. The gate refuses an incorrect memory maximum, extra/invisible
members, child cgroups or preloaded state before application import.

Validation completed locally: shell syntax; Python compilation; collector CLI;
**six gate unit tests passed**, including incorrect 14 GiB limit, extra member,
invisible member, child cgroup and preloaded-state refusal. These are synthetic
gate-contract tests only. Docker build/networking, image compatibility, fixture
loading, replay and full composition remain unexecuted. No full CI or runtime
regression was run because there is no proposed runtime repair.

## Reproduction and decision

- Run actually started: **NO**.
- Dominant reproduced memory class: **not measured**.
- Peak reproduced anon/file: **not measured**.
- Reproduced recovery/publication behavior: **not measured**.
- Major gaps: exclusive runtime, native-image parity, complete sanitized histories,
  active-provider/inbound-certifier replay and workload/failure-cadence fidelity.

**REPRODUCTION NOT ATTEMPTED — INFRASTRUCTURE BLOCKER**

**INFRASTRUCTURE BLOCKER REMAINS**

Track A has localized current anonymous memory to PID 39. It has not established
causality or justified any repair. Do not proceed to causal isolation until a valid
high-fidelity disposable reproduction exists. PR #379 remains draft; cleanup and
all production behavior remain unchanged.
