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


_POSTCOMPOSE_HOOKS_INSTALLED = False


def _install_postcompose_repairs() -> None:
    """Install or deterministically defer repairs that require final composition.

    PR #119 originally installed the scout repair only when deployment preflight
    happened to run after the candidate plane. Exact-release telemetry proved that
    import timing was not a valid composition contract: the candidate plane was live
    while the scout repair status was absent and all scout normalizations still
    failed. This coordinator works in either import order without creating runtime
    state or doing network I/O.
    """

    global _POSTCOMPOSE_HOOKS_INSTALLED
    if _POSTCOMPOSE_HOOKS_INSTALLED:
        return

    from . import candidate_execution_evidence_plane as candidate_plane
    from . import continuity_storage_capacity_repair as storage
    from . import poll_watermark_repair as watermark
    from .high_volume_signature_cursor_repair import (
        install_high_volume_signature_cursor_repair,
    )
    from .release_bound_scout_classification_repair import (
        install_release_bound_scout_classification_repair,
    )
    from .scout_candidate_continuity_repair import (
        install_scout_candidate_continuity_repair,
    )

    def install_scout_and_preserve_markers() -> None:
        install_scout_candidate_continuity_repair()
        # Once exact-scout identity is the active normalizer, complete the release-
        # bound repair so supported-source-missing scout transactions can become
        # terminal non-candidates instead of later anonymous expiry failures.
        install_release_bound_scout_classification_repair()
        # The scout provider-assignment wrapper composes over the established
        # high-volume affinity function. Preserve its intrinsic marker so later
        # invariants can prove that PR #99 remains installed beneath the wrapper.
        setattr(storage._assigned_endpoint, "_roi_high_volume_poll_affinity", True)

    if candidate_plane._ORIGINAL_SERVICE_INGEST is not None:
        install_scout_and_preserve_markers()
    else:
        current_candidate_install = candidate_plane.install_candidate_execution_evidence_plane
        if not bool(getattr(current_candidate_install, "_roi_postcompose_scout_install", False)):
            def install_candidate_then_scout() -> None:
                current_candidate_install()
                install_scout_and_preserve_markers()

            try:
                install_candidate_then_scout.__dict__.update(getattr(current_candidate_install, "__dict__", {}))
            except Exception:
                pass
            setattr(install_candidate_then_scout, "_roi_postcompose_scout_install", True)
            candidate_plane.install_candidate_execution_evidence_plane = install_candidate_then_scout

    if bool(getattr(watermark._slot_poll_page, "_roi_routine_provider_sharding", False)):
        install_high_volume_signature_cursor_repair()
    else:
        current_storage_install = storage.install_continuity_storage_capacity_repair
        if not bool(getattr(current_storage_install, "_roi_postcompose_high_volume_poll", False)):
            def install_storage_then_high_volume() -> None:
                current_storage_install()
                install_high_volume_signature_cursor_repair()

            try:
                install_storage_then_high_volume.__dict__.update(getattr(current_storage_install, "__dict__", {}))
            except Exception:
                pass
            setattr(install_storage_then_high_volume, "_roi_postcompose_high_volume_poll", True)
            storage.install_continuity_storage_capacity_repair = install_storage_then_high_volume

    _POSTCOMPOSE_HOOKS_INSTALLED = True


def _install_scout_candidate_repair_if_ready() -> None:
    """Backward-compatible entrypoint retained for existing callers/tests."""

    _install_postcompose_repairs()


def deployment_preflight(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    _install_postcompose_repairs()
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


# Production may import this module before or after the lower-level installers. The
# coordinator above makes either order deterministic and performs no runtime I/O.
_install_postcompose_repairs()
