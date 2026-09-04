from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any, Mapping

from .deployment import (
    FORBIDDEN_SECRET_ENV_NAMES,
    FROZEN_PROGRAM_ADDRESSES,
    PreflightCheck,
    _profiles_check,
    _truthy,
)
from .shadow_execution import validate_solana_public_key
from .solana_rpc import rpc_endpoints_from_env


def _install_scout_candidate_repair_if_ready() -> None:
    """Install the final scout repair only after production composition exists.

    ``production.py`` imports this module through ``api.py`` after the candidate
    execution-evidence plane has been installed. Some unit tests import deployment
    helpers directly before production composition, so the readiness check keeps
    those imports inert while the production import deterministically installs the
    repair before the FastAPI runtime can be built.
    """

    try:
        from . import candidate_execution_evidence_plane as candidate_plane
        if candidate_plane._ORIGINAL_SERVICE_INGEST is None:
            return
        from .scout_candidate_continuity_repair import (
            install_scout_candidate_continuity_repair,
        )
        install_scout_candidate_continuity_repair()
        # The scout wrapper deliberately composes over the existing high-volume
        # provider-affinity function. Preserve that intrinsic composition marker so
        # production invariants and later installers can still prove PR #99 remains
        # installed rather than mistaking a compatible outer wrapper for removal.
        from . import continuity_storage_capacity_repair as storage
        setattr(storage._assigned_endpoint, "_roi_high_volume_poll_affinity", True)
    except (ImportError, RuntimeError):
        # Direct deployment/preflight imports must remain safe outside the fully
        # composed production entrypoint. Production will call this helper again
        # below and on every preflight evaluation.
        return


def deployment_preflight(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    _install_scout_candidate_repair_if_ready()
    values: Mapping[str, str] = env or os.environ
    checks: list[PreflightCheck] = []
    checks.append(PreflightCheck("paper_only", _truthy(values.get("PAPER_ONLY")), "PAPER_ONLY must be true"))
    checks.append(PreflightCheck("mainnet", values.get("SOLANA_NETWORK") == "mainnet-beta", "SOLANA_NETWORK must be mainnet-beta"))
    checks.append(PreflightCheck(
        "program_coverage_enabled",
        _truthy(values.get("SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE")),
        "full frozen program observation must remain enabled",
    ))
    checks.append(PreflightCheck(
        "direct_solana_enabled",
        _truthy(values.get("SOLANA_ROI_DIRECT_SOLANA_ENABLED") or "true"),
        "direct standard Solana intake must be enabled",
    ))
    checks.append(PreflightCheck(
        "continuous_clock_enabled",
        _truthy(values.get("SOLANA_ROI_SHADOW_CLOCK_ENABLED")),
        "continuous paper price clock must be enabled",
    ))

    endpoint_error: str | None = None
    try:
        endpoints = rpc_endpoints_from_env(dict(values))
    except Exception as exc:
        endpoints = ()
        endpoint_error = type(exc).__name__
    checks.append(PreflightCheck(
        "redundant_standard_rpc",
        len(endpoints) >= 2,
        "at least two distinct standard Solana HTTP/WebSocket endpoints are required",
    ))
    checks.append(PreflightCheck(
        "independent_standard_rpc_quorum",
        len(endpoints) >= 3,
        "at least three distinct standard Solana HTTP/WebSocket providers are required for prospective continuity",
    ))

    if _truthy(values.get("RENDER")):
        persistent = str(values.get("SOLANA_ROI_DB_PATH") or "").startswith("/var/data/")
        checks.append(PreflightCheck("persistent_sqlite", persistent, "Render deployment must use /var/data persistent disk"))
        commit = str(values.get("RENDER_GIT_COMMIT") or "")
        checks.append(PreflightCheck("release_commit", len(commit) == 40, "RENDER_GIT_COMMIT must bind the exact deployed release"))

    for name in ("JUPITER_API_KEY", "SOLANA_ROI_COHORT_ARM_AUTH"):
        checks.append(PreflightCheck(
            name.lower(),
            bool(str(values.get(name) or "").strip()),
            f"{name} must be configured",
        ))

    shadow_wallet = str(values.get("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY") or "").strip()
    try:
        validate_solana_public_key(shadow_wallet)
        wallet_ok = True
    except ValueError:
        wallet_ok = False
    checks.append(PreflightCheck("shadow_wallet_public_key", wallet_ok, "a valid public Solana address is required; no private key"))

    forbidden = [name for name in FORBIDDEN_SECRET_ENV_NAMES if str(values.get(name) or "").strip()]
    checks.append(PreflightCheck(
        "no_private_key_material",
        not forbidden,
        "application environment must contain no ROI wallet private key, seed phrase, or mnemonic",
    ))

    profiles_ok, profiles_detail = _profiles_check(str(values.get("SOLANA_ROI_WALLET_PROFILES_JSON") or ""))
    checks.append(PreflightCheck("frozen_scout_cohort", profiles_ok, profiles_detail))

    return {
        "ready_for_live_shadow_collection": all(check.ok for check in checks),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "data_plane": "direct-standard-solana",
        "provider_enhanced_webhook_required": False,
        "strategy_scope_reduced": False,
        "checks": [asdict(check) for check in checks],
        "program_addresses": list(FROZEN_PROGRAM_ADDRESSES),
        "rpc_endpoints": [
            {
                "name": endpoint.name,
                "http_host": endpoint.http_url.split("/", 3)[2],
                "ws_host": endpoint.ws_url.split("/", 3)[2],
            }
            for endpoint in endpoints
        ],
        "rpc_endpoint_config_error_type": endpoint_error,
    }


# In the production entrypoint this import occurs after the candidate execution
# plane is composed and before ``api.ingestion_runtime`` can build an instance.
_install_scout_candidate_repair_if_ready()
