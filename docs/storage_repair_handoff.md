# Storage repair handoff

## Recovered-source closeout — 2026-09-16

**Status: recovered implementation packaged for review; locked-environment CI is
still a merge gate.** Source recovery is complete. This is not production
stability, deletion authorization, deployment approval, or trading certification.
The original patch, original handoff, and historical closeout remain preserved
separately and unchanged. No GitHub or production writes were performed.

### Exact source identity and reproducibility

- Canonical baseline: `648c8d9d84604a915ae31994c874b53e8a5898c0`.
- Baseline Git tree: `7ca1024cc9cf87657bf538efaf84b04b23b3d968`.
- Uploaded ZIP SHA-256:
  `176c4d0da18ed2da7b0c95f9f4f513f2785a5aba7f5986b0caa178c8b83a948f`.
- Every uploaded tracked path, file mode, and byte reconstructs the baseline tree
  returned by GitHub. The exact signed baseline commit object was reconstructed
  from GitHub's signature/payload and verified by its Git object hash; ancestors
  before that baseline were not imported into this local shallow repository.
- Recovered original patch tree:
  `2322e048dbe3fb70c205975bbefeaee2eb4ff7b2`.
- Prior corrected candidate tree:
  `e64bdb6e3140da497ebdcb53bc283af27581f46b`.
- The combined corrected patch applied cleanly with `git apply --check --index`
  and actual application to the complete exact baseline. All 31 before/after
  Git blobs matched its full-index metadata. Applying the untouched recovered
  patch followed by the continuity supplement produced that same complete tree.
- This closeout additionally hardens only the PID test helper and its regressions
  as described below. The final exported patch, source tree, and local commit are
  recorded in the accompanying `FINALIZATION_EVIDENCE.json`; the exported patch
  is checked again against a fresh exact-baseline worktree before delivery.
- The final commit is a new local child of the baseline. It must not be called
  the original recorded `037c81c09aca7ccf4fd59a259c23b1e0df0eb9be` commit, whose
  object was not available. No upstream commit is fabricated or relabeled.

### Continuity deletion-safety correction retained from the prior candidate

The recovered, undeployed module could retire an otherwise-eligible raw receipt
without canonical continuity evidence when the global-state table was missing in
raw-cursor mode, or its `id=1` row was absent in either consumer mode. The shared
predicate now blocks those cases. The 120-second floor, durable acknowledgements,
consumer cursors, and byte budget are unchanged. Retirement resumes only after
required continuity state is durably restored.

The same 22 focused cases produced 6 failures/16 passes against the original
recovered module and 22 passes against the correction. Those original before/after
logs are preserved. The complete repository's focused retention/recovery/guard
selection subsequently passed 55 tests in the available local environment.

### Process-growth regression remains able to detect real child workers

A real child-process probe found that this proc mount does not expose
`/proc/<pid>/task/<tid>/children`. The recovered helper silently missed an actual
child and its live thread on this mount. This was a test measurement defect,
not evidence that the production implementation leaked a worker.

The helper now derives descendants from `PPid` in proc status files and counts
all native task IDs for each owned process. It still excludes unrelated
shared-cgroup processes. The original `baseline + 2` allowance, live-thread
assertion, and 64-request async lifecycle are unchanged. Three deterministic
regressions cover genuine thread growth, descendant growth without children
files, and exclusion of unrelated process trees. The existing lifecycle plus
those three tests passed. The real probe also detects the child and its thread;
three deliberately held root threads increase the measured count from 10 to 13,
which would fail the unchanged growth allowance, and cleanup returns it to 10.
No production resource guard or strategy code was changed by this test repair.

### Test evidence and remaining environment gate

The complete workflow test-command sequence is executed in fresh disposable
local state; compatibility partitions retain the repository's fresh-interpreter
boundaries. Exact commands, exit codes, XML where emitted, environment versions,
and final counts are in the accompanying evidence package. Failure results are
not suppressed, retried until green, or relabeled as a clean CI run.

The available execution environment is Python 3.13.5 / pytest 9.0.2 / SQLite
3.46.1, not the required Python 3.11.16 and `requirements.lock`. In particular,
FastAPI is 0.128.2 instead of the pinned 0.141.1. The unchanged dependency-integrity
assertion fails identically on the exact untouched baseline. Attempts to obtain
the exact Python and locked packages were unsuccessful in this offline runtime.
The original 2,149-pass historical report is not used as proof of this final tree.

Before any merge, run the existing required GitHub CI in its exact locked
environment on the published repair commit. Keep the version pins, strategy
thresholds, and assertions unchanged. Local source/test verification does not
satisfy that release gate. The HTTP smoke starts only a disposable loopback
service with providers disabled; its existing test-only memory-guard isolation
means it does not prove production raw-cgroup stability. Production deletion,
deployment, V2/V4 reactivation, and continuous live paper operation remain
separately gated and were not performed.

## Original implementation handoff (historical evidence)

## Identity and boundaries

- Baseline: `648c8d9d84604a915ae31994c874b53e8a5898c0`
- Branch: `repair/storage-total-retention`
- Initial repair: `933f72fd54c030f29349ae8e6b45ffe6500740e9`
- Production changes: none; no production data was deleted.
- Strategy/authority changes: none. v5.2 thresholds, market coverage,
  wallet intelligence, accounting, the shared $500 paper portfolio, Robinhood
  V2/V4 pause, and disabled signing/submission/live-money authority are intact.

## Established causes and repairs

1. The v5.2 normalized consumer left `last_raw_receipt_id` at zero, so the raw
   Direct-Solana receipt stream lived for its full 15-minute expiry. Raw receipt
   retirement now requires the 120-second floor **and** durable downstream
   acknowledgement: terminal hydration queue and metric rows, plus a canonical
   normalized swap at or below the durable wallet cursor when normalization
   succeeded. A terminal negative hydration is also an acknowledgement. Expiry,
   schema mode, or age alone never authorizes deletion. Missing schema,
   interrupted writes and unresolved continuity gaps fail closed. Rollover copy
   uses the same predicate.
2. The operational maintenance variants could delete hydration acknowledgements
   before the raw receipt owner evaluated them. The active compatibility,
   canonical, nonblocking, and isolated maintenance paths now protect queue and
   metric acknowledgements while a matching raw receipt exists. A later pass
   removes them after the raw row is safely retired.
3. Normalized swaps, risk-refresh measurements and program-coverage state were
   persisted in specialized canonical tables and again as variable-size generic
   events despite having no event-ledger consumer. The owning writers no longer
   append the duplicate representation.
4. Incremental vacuum reclaimed one page per call on the representative SQLite
   build. Maintenance now performs a bounded sequence of one-page attempts and
   checkpoints the WAL after reclamation.
5. The reconciliation wrapper previously had a second pruning implementation
   with a different dependency order. It now delegates to the canonical v5.2
   implementation, preserving decisions/entries until their outcome proof has
   been evaluated.
6. Every rollover retained a complete predecessor. Inventory now counts hard
   links by `(device,inode)`, blocks creation of a third unreclaimed physical
   generation, and labels pre-build and post-rollover disk measurements
   separately. `disk_free_bytes` from the rollover report is explicitly the
   pre-build value.
7. Guarded reclamation proves the predecessor/successor chain, exact release and
   checkpoint binding, registered late evidence, file identity, quiescent lease
   ownership and physical bytes. It records each unlink durably and resumes an
   interrupted batch idempotently. An uncertain target stays protected without
   blocking a separately proven eligible prefix.
8. Existing production can already exceed the two-generation ceiling before the
   repaired release reaches full runtime. The authoritative installer now allows
   one narrow pre-start recovery: a prior full-runtime marker for the exact
   canonical path, a legitimate ordinary release-rollforward checkpoint, more
   than two predecessors, the live exclusive lease, and exact checkpoint
   approval are all required. It neither rewrites release identity nor adds a
   parallel deletion path.

## Measurements

The final disposable 120,000-row reproduction used the production retirement
function, normalized-consumer schema, canonical rows and durable queue/metric
acknowledgements:

- raw receipt rows: `120,000 -> 16,000` (`104,000` retired in six bounded work
  batches plus a terminal no-op)
- raw receipt table/index allocation: `19,890,176 -> 2,686,976` bytes
- total database allocation: `45,002,752 -> 27,779,072` bytes after bounded
  incremental reclamation
- filesystem allocation reduction: `17,223,680` bytes
- freelist: `4,200 -> 0` pages; `4,200` bounded vacuum attempts

The recovery integration used four valid predecessors plus one uncertain SQLite
target. At modeled free space of 64 MiB (well below the 2 GiB rollover reserve),
dry-run selected the four exact eligible files and protected the uncertain file.
Installer-owned execution reported a modeled 64 MiB physical-free increase,
then normal startup established the current release. A subsequent rollover and
same-release maintenance cycle reclaimed its newly proven predecessor and left
the physical predecessor count below the two-generation ceiling.

## Resolved PID regression

The old assertion used cgroup-wide `pids.current`; this runner shares that cgroup
with unrelated services and jobs. Both baseline and repair also failed before
the PID assertion when the independent raw-cgroup memory guard saw the shared
20 GiB cgroup above its fail-closed threshold. Under the same isolated memory
precondition, the exact regression passes on baseline and repair.

The test now retains `pids.current` as diagnostic telemetry but makes the growth
assertion over Linux task IDs owned by the pytest process and its descendants,
discovered through `/proc/<pid>/task/*/children`. The allowance remains exactly
`baseline + 2`; the live-thread bound and 64-request lifecycle checks remain.
This classifies the observed `48 -> 52` cgroup movement as unrelated
shared-cgroup activity, not an implementation worker leak.

## Test evidence

Full closeout logs are outside the commit in
`storage-repair-closeout-logs/`. Exact relevant commands and outcomes:

```bash
python -m pytest \
  tests/test_storage_total_retention_repair.py -vv --tb=long
# 15 passed; exit 0

python -m pytest \
  tests/test_storage_recovery_startup_sequence.py \
  tests/test_storage_total_retention_repair.py::test_unresolved_gap_protects_consumed_receipts_but_not_expired_waste \
  tests/test_sealed_epoch_reclamation_executor.py::test_runtime_executor_requires_same_release_establishment \
  -vv --tb=long
# 4 passed; exit 0

python -m pytest \
  tests/test_certification_logical_bootstrap_async_wrapper_regression.py::test_wrapped_logical_bootstrap_stays_async_and_recovers_without_thread_growth \
  -vv --tb=long
# repair: 1 passed; exit 0
# baseline with the same isolated test precondition: 1 passed; exit 0
```

The final required workflow command set was executed with proxy inheritance
disabled and a fresh disposable `SOLANA_ROI_DB_PATH`. The shared runner began
above 98% cgroup memory because of unrelated clean file cache. The first attempt
therefore failed closed at the existing memory guard; a second attempt also
encountered an already-malformed ignored local database. Neither file was
deleted or rewritten. After releasing clean cache for four explicit disposable
diagnostic databases and preserving the dirty database untouched, execution
continued from the exact failed workflow command:

- workflow commands before that boundary: passed in the clean-path attempt;
- workflow commands from that boundary through compile: exit `0`;
- production-composed regression partition: `2149 passed`, 3 deprecation/import
  warnings, exit `0`;
- all nine native compatibility partitions: passed in fresh interpreters;
- launched production smoke: passed with canonical v5.2 and paper-only/no-live-
  money authority;
- forward activation: `5 passed`, exit `0`;
- dead-module audit, monkeypatch audit, package compile and `git diff --check`:
  passed.

The launched smoke now isolates its new tiny disposable database from unrelated
shared-cgroup file cache by replacing only the memory guard inside the smoke
child before production import. Production installation and thresholds are
unchanged, and the dedicated memory-guard fail-closed tests remain in the green
composed suite. Generated XML and full logs remain outside the commit.

## Guarded operator procedure

Prerequisites: canonical service quiescent; exact active path; no reader/writer
other than the authoritative runtime installer; existing prior full-runtime
establishment marker for that path; reviewed current release/checkpoint; enough
space for SQLite/WAL safety work (reclamation itself does not build a successor);
and separate authorization for production deletion.

Read-only inventory and proof (default; deletes nothing):

```bash
export SOLANA_ROI_DB_PATH=/var/data/solana-roi.sqlite3
export RENDER_GIT_COMMIT=<exact-deployed-release-sha>
python - <<'PY'
import json, os
from solana_roi.sealed_epoch_reclamation import preflight_sealed_epoch_reclamation
print(json.dumps(preflight_sealed_epoch_reclamation(
    os.environ["SOLANA_ROI_DB_PATH"],
    expected_release_sha=os.environ["RENDER_GIT_COMMIT"],
), indent=2, sort_keys=True))
PY
```

Review `checkpoint_id`, release/schema binding, `eligible_candidates`, exact
device/inode identities, `eligible_physical_bytes`, and every protected blocker.
Bind approval to that exact checkpoint, then use the existing authoritative
runtime installer—never call the destructive executor directly:

```bash
export SOLANA_ROI_SEALED_EPOCH_RECLAMATION_ENABLED=true
export SOLANA_ROI_SEALED_EPOCH_RECLAMATION_CHECKPOINT_ID=<reviewed-checkpoint-id>
uvicorn solana_roi.production:app --host 0.0.0.0 --port "$PORT"
```

The installer must acquire the live canonical disk lease. For an over-ceiling
recovery, it verifies the prior establishment marker and ordinary release
rollforward before cleanup, writes crash-safe receipts per unlink, then proceeds
to normal startup and writes the current-release establishment marker. Disable
the execution flag again after the reviewed recovery window; future invocations
remain dry-run/read-only by default.

## Protected data and deployment prerequisites

Active SQLite, WAL/SHM, positions, capital reservations, settlements, NAV,
pending transports, unresolved gaps, wallet/strategy/audit/replay evidence, and
any predecessor with missing, stale or ambiguous proof remain protected.
`mismatched_sections=[]` and a retained hash are never deletion authority; all
still-required late-registered records must be reconciled into the survivor.

Before deployment, review the baseline-to-head binary patch and execute only the
separately authorized quiescent recovery above if production remains over the
ceiling. After deployment, bounded live verification must establish: raw rows
and bytes converge; acknowledgment queues drain only after raw retirement;
duplicate event writes stay absent; WAL/freelist and total allocated bytes remain
bounded; pre-build versus post-rollover disk values are correctly distinguished;
physical predecessor count stays below the ceiling across repeated restarts; and
paper-only/shared-capital invariants are unchanged.

The original closeout is historical evidence. The recovered-source closeout
above and the accompanying final evidence define the current boundary. Production
deletion and deployment were not performed; production stability and trading
certification remain unproven.
