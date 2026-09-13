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
- low-overhead cgroup + `/proc` JSONL collector;
- bounded runner with termination evidence;
- Python 3.11.16 Docker definition matching `.python-version`.

## What is deliberately missing

- representative deterministic fixtures for the required state families;
- justified production scale parameters;
- deterministic HTTP/WebSocket provider replay;
- proof that replay preserves canonical retry/failover behavior;
- executed Docker proof on an exclusive 2 GiB host;
- executed canonical startup/thread/task/executor comparison;
- publication observer integration.

The included `fidelity.example.json` intentionally uses `NOT REPRESENTED` provider
classifications. It MUST fail validation and therefore cannot start the application.

## Intended execution after blockers are complete

```bash
docker build --build-arg SOURCE_SHA=92c0c1620f78116e7ecbeade039e9aedaf3a51a9 \
  -f diagnostics/portable_repro/Dockerfile -t solana-roi-portable-repro .
docker run --rm --memory=2g --memory-swap=2g \
  --network=none \
  -v "$PWD/fixtures:/fixtures:ro" \
  -v "$PWD/evidence:/evidence" \
  -e PORTABLE_REPRO_MANIFEST=/fixtures/fidelity.json \
  solana-roi-portable-repro
```

Do not remove `--network=none`; the Python guard is defense in depth, not the sole
network boundary.
