from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

JOB_VERSION = "isolated-certifier-job-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp = Path(raw)
    try:
        tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _assert_release(surface: str, payload: dict[str, Any], expected: str) -> None:
    if surface in {"e2e", "forward"}:
        observed = str(payload.get("release_commit") or "")
    else:
        release = payload.get("release")
        observed = str(release.get("release_commit") or "") if isinstance(release, dict) else ""
    if observed != expected:
        raise RuntimeError(f"{surface} release mismatch:{observed or 'missing'}:{expected}")


def _assert_safety(surface: str, payload: dict[str, Any]) -> None:
    safety = payload.get("overall") if surface == "e2e" and isinstance(payload.get("overall"), dict) else payload
    if safety.get("paper_only") is not True:
        raise RuntimeError(f"{surface} paper_only invariant failed")
    if safety.get("live_money_authority") is not False:
        raise RuntimeError(f"{surface} live-money invariant failed")
    if safety.get("signing_available") is not False:
        raise RuntimeError(f"{surface} signing invariant failed")
    if safety.get("transaction_submission_available") is not False:
        raise RuntimeError(f"{surface} submission invariant failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)

    db_path = os.getenv("SOLANA_ROI_DB_PATH", "").strip()
    if not db_path or not Path(db_path).is_file():
        raise SystemExit("isolated certifier requires a point-in-time SQLite snapshot")

    expected = _release_commit()

    # This disposable process never enters FastAPI lifespan. It starts no live
    # ingestion or execution workers and can only operate on its private DB copy.
    from .production_system import build_production_system
    from . import certification_generation_runtime_repair as certification_runtime
    from . import e2e_status_read_boundary_repair as e2e
    from . import production_proof_read_boundary_repair as production_proof
    from . import robinhood_runtime_install as robinhood_runtime

    system = build_production_system()
    runtime_provider = system.ingestion_runtime
    if not callable(runtime_provider):
        raise RuntimeError("isolated certifier runtime provider is not callable")

    e2e_payload = e2e.build_bounded_e2e_status(runtime_provider, robinhood_runtime._status)
    if not isinstance(e2e_payload, dict):
        raise RuntimeError("isolated E2E builder returned non-object")
    _assert_release("e2e", e2e_payload, expected)
    _assert_safety("e2e", e2e_payload)
    e2e_payload.setdefault("isolated_certifier", {}).update({"job_version": JOB_VERSION, "child_process_isolation": True, "canonical_db_snapshot": True, "shared_writable_disk": False, "paper_only": True, "live_money_authority": False, "signing_available": False, "transaction_submission_available": False})
    e2e._publish_snapshot(e2e_payload)
    _atomic_json(output / "e2e.json", e2e_payload)

    forward_endpoint = certification_runtime._ORIGINAL_FORWARD_ENDPOINT
    if not callable(forward_endpoint):
        raise RuntimeError("isolated forward-certification delegate unavailable")
    forward_payload = forward_endpoint()
    if not isinstance(forward_payload, dict):
        raise RuntimeError("isolated forward builder returned non-object")
    _assert_release("forward", forward_payload, expected)
    _assert_safety("forward", forward_payload)
    forward_payload.setdefault("isolated_certifier", {}).update({"job_version": JOB_VERSION, "child_process_isolation": True, "canonical_db_snapshot": True, "shared_writable_disk": False, "paper_only": True, "live_money_authority": False, "signing_available": False, "transaction_submission_available": False})
    certification_runtime._publish_forward(forward_payload)
    _atomic_json(output / "forward.json", forward_payload)

    proof_builder = production_proof._ORIGINAL_PRODUCTION_PROOF
    if not callable(proof_builder):
        raise RuntimeError("isolated production-proof delegate unavailable")
    production_payload = proof_builder()
    if not isinstance(production_payload, dict):
        raise RuntimeError("isolated production-proof builder returned non-object")
    _assert_release("production", production_payload, expected)
    _assert_safety("production", production_payload)
    production_payload.setdefault("isolated_certifier", {}).update({"job_version": JOB_VERSION, "child_process_isolation": True, "canonical_db_snapshot": True, "shared_writable_disk": False, "paper_only": True, "live_money_authority": False, "signing_available": False, "transaction_submission_available": False})
    _atomic_json(output / "production.json", production_payload)


if __name__ == "__main__":
    main()
