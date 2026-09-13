# Portable current-anonymous-pressure reproduction

This is an unexecuted reproduction harness, not a runtime repair or a causal proof.
Canonical source: `92c0c1620f78116e7ecbeade039e9aedaf3a51a9`.
PR #379 stays draft. Production is never a target of these scripts.

## Missing infrastructure

Use a separately authorized disposable **native Linux Docker host with cgroup v2**.
The operator must be able to configure Docker memory limits, read host `/proc` and
container cgroup files, and inspect all application descendants. The host-side
collector runs outside the application's cgroup. Provide disk space for sanitized
state, writable WAL/SHM, the build, and evidence (at least 30 GiB recommended for the
earlier 10× fixture; size against the actual fixture manifest). Do not substitute
ulimit or polling in a shared cgroup.

The current workspace cannot run this harness: cgroup controls are read-only;
the shared memory limit is 14 GiB; Docker/Podman and systemd are unavailable.
A connected Render tool advertises creation of new `1c-2g` native services. That
is a possible separately provisioned environment, **not an existing verified
exclusive runtime**. The exposed creation API does not provide the Docker,
disk-mount, or process-inspection controls required by this particular harness.
No new Render resource was created, and no existing service was reconfigured.

## Required inputs

1. Git checkout containing the exact canonical commit. `run.sh` exports that commit
   into a fresh build context, excluding uncommitted source modifications.
2. A verified immutable **Python 3.11 base-image digest**, with pip. Record Python,
   libc, OpenSSL, SQLite, architecture and build-tool versions. The supplied
   Dockerfile installs the exact canonical requirements lock and editable project.
   Patch-level/native-library differences from Render are fidelity gaps until measured.
3. A **sanitized consistent SQLite state directory** and `manifest.json` as below.
   Never mount `/var/data` from production, use the canonical production database as
   an input path, or collect a live DB by copying an independently changing WAL/SHM.
   State acquisition/sanitization must be a separately reviewed, consistent process.
4. A sanitized environment file using the same worker flags, limits, cadences,
   strategy/freeze settings, provider ordering and retries as production. Include:

   ```dotenv
   PAPER_ONLY=true
   SOLANA_ROI_PRODUCTION_CLEANUP_ENABLED=false
   SOLANA_ROI_DB_PATH=/state/solana-roi.sqlite3
   SOLANA_ROI_RELEASE_COMMIT=92c0c1620f78116e7ecbeade039e9aedaf3a51a9
   SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME=true
   SOLANA_ROI_CERTIFICATION_SHARED_TOKEN=disposable-fixture-token
   ```

   This is **not a complete environment file**. `environment-contract.json` lists
   blueprint values and secret-bound variable names without their secrets.
   Live production overrides are not fully available; record those gaps. Replace
   credentials only with fixture credentials. Provider endpoints must resolve to
   the isolated replay adapter while preserving provider identities/order and all
   client retry/failover behavior. For hard-coded HTTPS domains, the adapter must
   provide deterministic DNS/TLS routing with disposable trust material; never
   silently drop that provider or change application source to avoid it.
5. A verified immutable **provider-replay image digest**. No recordings or working
   replay adapter have been supplied in this session. The harness deliberately
   does not invent them or represent an empty replay as adequate fidelity.

## Provider-replay contract

The adapter runs in its **own** cgroup/container on the same internal Docker
network, with hostname `replay`; application hostname is `app`. It accepts:

```text
--manifest /fixtures/manifest.json
```

The manifest names provider HTTP/WebSocket routes and sanitized recordings. The
adapter must preserve observed request/response sizes, logical cadence, latency
distribution, timeout and failure rates, cancellation, concurrency, retry inputs,
provider-switch conditions, and pagination. Keep public Solana, private backup,
Robinhood/dRPC/Alchemy, Blockscout, Jupiter, wallet and metadata paths represented
where enabled in production. Record exact coverage rather than assuming all listed
providers are active. Recordings must contain no live credentials.

The adapter must also represent natural inbound certifier bootstrap/delta traffic
at the recorded cadence, or a separate disposable certifier must do so in another
cgroup. Do not force extra application publication cycles. If neither is provided,
certification/replication workload fidelity is missing and attribution is gated.
The internal network has no live egress: missing replay paths must fail visibly.

## Fixture manifest

Required top-level values:

- `source_release`: exact canonical SHA;
- `sanitized: true`, with sanitization method/provenance;
- `snapshot_consistent: true`, with consistency validation method;
- `files`: relative path → SHA-256 for every state file;
- `dimensions`: each required category below, with `tables`, `production_count`
  (null if not safely known), `fixture_count`, `history_depth`, `fidelity_gap`;
- `providers`: entries with `name`, `transport`, `cadence`, `sizes`, `latency`,
  `timeouts`, `retries`, `failover`, `concurrency`, `cancellation`, `fidelity_gap`.

Required dimensions: `wallet`, `direct_solana`, `hydration`, `forward_evidence`,
`events_checkpoints`, `lifecycle_portfolio`, `certification_replication`,
`shadow_price_tokens`. Record DB/WAL/SHM bytes, counts/depths/populations and
production-versus-fixture differences. Presence of a table name alone does not
establish fidelity. The expected DB path is `state/solana-roi.sqlite3`.
`validate_inputs.py` verifies this contract and file hashes, not the truth of
provenance or representative adequacy. No representative fixture set is included.

## Run

Install host Docker, Python 3, Git and standard shell tools on the disposable host.
Do not run Docker-in-Docker or use an unrelated host process inside the measured
application cgroup. Choose an unused localhost port 18768 (or deliberately update
the publish/collector pair). All output directories and resource names are new.

```bash
bash run.sh /path/to/canonical-repo /path/to/sanitized-fixtures \
  /path/to/disposable.env \
  'python:3.11-slim-bookworm@sha256:VERIFIED_DIGEST' \
  'your-replay-adapter@sha256:VERIFIED_DIGEST' \
  /path/to/new-observation-output
```

The digest labels above are explicit placeholders, not real image digests.

The gate container starts with an empty dedicated state volume and no application
imports. It checks kernel `memory.max == 2147483648`, cgroup v2, a single initial
member and no unexpected child cgroups. The host then copies only validated
disposable state, releases the gate, and the gate process uses `exec` for:

```text
uvicorn solana_roi.production:app --host 0.0.0.0 --port 10000
```

Normal worker startup ordering/composition is preserved. No workers are staggered,
no SQL is traced, and no allocator profiler is enabled. The Docker memory/swap
budget is 2 GiB with no additional swap, and one CPU is used. Check this against
actual Render CPU/native-library/swap characteristics and record differences.
All child processes inherit the cgroup. Host collector membership includes
descendant cgroups and retains per-PID identity; inspect it for unexpected members.

The collector records at 60-second cadence for an initial 900-second window:
cgroup memory/stat/events/refault fields, PIDs, process status/rollup/io, FD counts,
age inputs, DB/WAL/SHM sizes, and the existing cached publication endpoint. It
does not scrape full smaps, query SQLite, force GC/trim, or trigger publication.
Application logging may have overhead; measure collector-on/off variance in a
subsequent fresh run if reproduction appears. The initial 15-minute window is
not sufficient to rule out a defect that requires hours; extend duration according
to the observed cycle count/history/startup-to-failure interval before classifying.

Isolation proof, image/container inspection, raw observations and logs are written
to the new host output directory. Containers remain available afterward; lifecycle
management belongs to the operator and must target only the newly created IDs.

## Acceptance criteria

Before loading fixtures: kernel limit exactly 2 GiB, exclusive initial membership,
no unrelated workloads, expected storage location and host collector separation.
Before causal profiling: match current production **anonymous-dominant** pressure,
substantial persistent private/anonymous growth, repeated-cycle baseline drift,
failure to recover, and publication rejection/staleness. Compare starting/peak
anon/file, per-cycle drift, 30/60/120-second recovery at natural cycle boundaries,
I/O/WAL/dirty/writeback, threads/PIDs and publication progression. Collect finer
recovery timestamps only in a bounded follow-up once cycle boundaries are known.

Do not label a partial history/replay fixture high-fidelity. Track earlier file-cache
pressure independently. A rise in RSS alone does not identify a causal producer.
If infrastructure is absent: **REPRODUCTION NOT ATTEMPTED — INFRASTRUCTURE BLOCKER**.
If membership cannot be proven: **REPRODUCTION NOT ATTEMPTED — ISOLATION NOT PROVEN**.
Only an actually executed valid run can receive **FAILURE NOT REPRODUCED**.

## Validation performed here

Shell syntax, Python compilation/help and deterministic negative tests of the
isolation/input gates can run without Docker. These checks do not validate image
build, Docker networking, replay behavior, full composition or reproduction fidelity.
No isolated run was started in this workspace.
