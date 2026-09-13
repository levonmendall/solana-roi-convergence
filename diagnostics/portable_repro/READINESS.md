# Portable reproduction readiness

## Decision

`PORTABLE REPRODUCTION INCOMPLETE`

This branch is harness-only. It does not repair production and must not be used as
causal memory evidence.

## Verified from canonical source

- SHA: `92c0c1620f78116e7ecbeade039e9aedaf3a51a9`.
- Render plan declares 2 GiB (`1c-2g`).
- Render starts one uvicorn command: `uvicorn solana_roi.production:app --host 0.0.0.0 --port $PORT`.
- `solana_roi.production` imports `production_system`, whose module-level
  `production_system = build_production_system()` performs canonical composition.
- Therefore the isolation/fidelity gate must precede importing the production module.

## Implemented foundation

- `preflight.py`: rejects unlimited or non-2-GiB cgroup v2 boundaries.
- `fidelity.py`: rejects missing state families/provider replay and canonical mismatch.
- `network_guard/sitecustomize.py`: blocks non-loopback socket connections in reproduction mode.
- `collector.py`: captures cgroup memory/events/stat plus per-process rollups.
- `run.sh`: gates before uvicorn, bounds runtime, packages basic termination evidence.
- `Dockerfile`: Python 3.11.16 and explicit canonical-SHA build assertion.

## Blocking gaps

1. Deterministic representative fixtures are not implemented.
2. Production scale/cardinality provenance is not complete.
3. HTTP/WebSocket replay is not implemented.
4. Canonical retry/cancellation/failover fidelity is not demonstrated.
5. Canonical runtime process/thread/task/executor composition has not been executed under the harness.
6. Publication success/rejection/staleness observation is not integrated.
7. Docker build/run and graceful/OOM termination behavior have not been validated on a genuine exclusive 2 GiB host.

Until all blockers are closed, the harness must fail closed before application startup.
