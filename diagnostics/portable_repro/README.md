# Portable 2 GiB reproduction harness

**Status: INCOMPLETE. This is not a valid causal reproduction yet.**

Canonical source: `92c0c1620f78116e7ecbeade039e9aedaf3a51a9`.
Production is unchanged. PR #379 remains outside this harness.

The canonical Render entrypoint is:

```text
uvicorn solana_roi.production:app --host 0.0.0.0 --port $PORT
```

`solana_roi.production` composes production during import, so `run.sh` deliberately
runs the cgroup and fidelity gates before invoking uvicorn/importing that module.

## What is implemented

- cgroup-v2 2 GiB pre-import gate;
- fidelity-manifest fail-closed validation;
- loopback-only socket guard for explicit reproduction mode;
- in-container cgroup + `/proc` JSONL collector;
- outside-container cgroup collector for target-death/OOM evidence;
- bounded loopback HTTP observer for health/readiness/system-proof/composition;
- bounded container runner with Docker termination-state capture;
- Python 3.11.16 Docker definition matching `.python-version`.

## What is deliberately missing

- representative deterministic fixtures for the required state families;
- justified production scale parameters;
- deterministic HTTP/WebSocket provider replay;
- proof that replay preserves canonical retry/failover behavior;
- executed Docker proof on an exclusive 2 GiB host;
- executed canonical startup/thread/task/executor comparison.

The included `fidelity.example.json` intentionally uses `NOT REPRESENTED` provider
classifications. It MUST fail validation and therefore cannot start the application.

## Intended execution after blockers are complete

Place the validated fixture set under `fixtures/`, including `fixtures/fidelity.json`,
then run from a Linux Docker host using cgroup v2:

```bash
bash diagnostics/portable_repro/run_docker.sh
```

The host runner builds with the locked source SHA, creates the target with a hard
2 GiB memory limit and no additional swap, disables container networking, starts a
host-side cgroup collector, waits for bounded completion/termination, and preserves
Docker state plus both in-container and host-side evidence.

Do not enable live container networking for a causal run. The Python socket guard is
defense in depth, not the sole network boundary; deterministic provider replay must
use loopback transport inside the target.
