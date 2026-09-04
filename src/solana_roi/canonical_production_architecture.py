from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any


LEGACY_HELIUS_COMPAT_ENV = "SOLANA_ROI_LEGACY_HELIUS_COMPAT_ENABLED"
CANONICAL_DATA_PLANE = "direct-standard-solana"
CANONICAL_QUOTE_PLANE = "jupiter-order-plus-redundant-standard-solana-rpc"

_ORIGINAL_BASE_READINESS: Callable[[Any], dict[str, Any]] | None = None


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def legacy_helius_compat_enabled() -> bool:
    """Legacy Helius webhook ingestion is opt-in and has no certification authority."""

    return _truthy(os.getenv(LEGACY_HELIUS_COMPAT_ENV))


def _canonical_base_readiness(self: Any) -> dict[str, Any]:
    if _ORIGINAL_BASE_READINESS is None:
        raise RuntimeError("canonical production architecture is not installed")
    status = _ORIGINAL_BASE_READINESS(self)
    requirements = status.get("requirements")
    if not isinstance(requirements, dict):
        requirements = {}
        status["requirements"] = requirements

    # The durable Helius queue is retained only as a compatibility/audit surface.
    # Direct Solana continuity, prospective coverage, quote/simulation evidence and
    # the existing cohort gates remain authoritative. A disabled legacy transport
    # must never block a direct-Solana production cohort.
    requirements.pop("durable_webhook_queue_drained", None)
    status["passed"] = bool(requirements) and all(bool(value) for value in requirements.values())
    status["canonical_data_plane"] = CANONICAL_DATA_PLANE
    status["canonical_quote_plane"] = CANONICAL_QUOTE_PLANE
    status["provider_enhanced_webhook_required"] = False
    status["legacy_helius_compat_enabled"] = legacy_helius_compat_enabled()
    status["legacy_webhook_queue_has_readiness_authority"] = False
    status["legacy_webhook_queue_has_promotion_authority"] = False
    status["paper_only"] = True
    status["live_money_authority"] = False
    status["signing_available"] = False
    status["transaction_submission_available"] = False
    return status


setattr(_canonical_base_readiness, "_roi_canonical_direct_architecture", True)


def architecture_status(runtime: Any | None = None) -> dict[str, Any]:
    direct_status: dict[str, Any] = {}
    rpc_status: dict[str, Any] = {}
    quote_client = None
    if runtime is not None:
        try:
            direct_status = runtime.direct_ingestion.status()
        except Exception:
            direct_status = {"available": False}
        try:
            rpc_status = runtime.rpc_pool.status()
        except Exception:
            rpc_status = {"available": False}
        try:
            quote_client = type(runtime.quote_handoff.client).__name__ if runtime.quote_handoff.client is not None else None
        except Exception:
            quote_client = None

    return {
        "installed": True,
        "canonical_data_plane": CANONICAL_DATA_PLANE,
        "canonical_quote_plane": CANONICAL_QUOTE_PLANE,
        "quote_client": quote_client,
        "helius_required": False,
        "legacy_helius_compat_enabled": legacy_helius_compat_enabled(),
        "legacy_helius_has_readiness_authority": False,
        "legacy_helius_has_promotion_authority": False,
        "planes": {
            "market_observation": {
                "role": "full frozen program/scout observation and durable receipts",
                "deep_universe_evaluation": False,
            },
            "wallet_entity_intelligence": {
                "role": "cheap discovery, historical screening, point-in-time entity research",
                "historical_promotion_authority": False,
            },
            "prospective_alpha": {
                "role": "release-bound forward v4 five-lane residual-return evidence",
                "old_release_replay_authority": False,
            },
            "paper_execution_certification": {
                "role": "amount-specific Jupiter order, unsigned simulation, paper outcome and certification",
                "live_money_authority": False,
            },
        },
        "strategy_boundaries": {
            "candidate_processing_target_seconds": 5.0,
            "strategy_entry_ceiling_seconds": 20.0,
            "processing_target_is_not_entry_authority": True,
        },
        "direct_solana": direct_status,
        "rpc": rpc_status,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_canonical_production_architecture() -> None:
    """Make direct Solana/Jupiter the one production authority contract.

    Legacy Helius code remains import-compatible for audit/migration purposes but is
    opt-in and cannot affect readiness, promotion, signing, submission or paper
    strategy authority.
    """

    global _ORIGINAL_BASE_READINESS
    from .runtime import RuntimeForwardCohortController

    current = RuntimeForwardCohortController._base_readiness
    if bool(getattr(current, "_roi_canonical_direct_architecture", False)):
        return
    _ORIGINAL_BASE_READINESS = current
    try:
        _canonical_base_readiness.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_canonical_base_readiness, "_roi_canonical_direct_architecture", True)
    RuntimeForwardCohortController._base_readiness = _canonical_base_readiness  # type: ignore[method-assign]


__all__ = [
    "CANONICAL_DATA_PLANE",
    "CANONICAL_QUOTE_PLANE",
    "LEGACY_HELIUS_COMPAT_ENV",
    "architecture_status",
    "install_canonical_production_architecture",
    "legacy_helius_compat_enabled",
]
