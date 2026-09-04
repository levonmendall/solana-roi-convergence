from __future__ import annotations

import math
from typing import Any, Callable

from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import WalletRole
from . import wallet_context_router as router_module
from .wallet_context_router import WalletContextRouter


PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
REPAIR_VERSION = "wallet-context-router-v1.1-percent-fail-closed"

_ORIGINAL_CLASSIFY: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_CONTEXT_METRICS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _pct(value: Any) -> float | None:
    numeric = _safe_float(value)
    return numeric * 100.0 if numeric is not None else None


def _fail_closed_accessibility(row: dict[str, Any]) -> dict[str, Any]:
    """Require explicit timing/chase evidence before calling an observation accessible.

    The v1 router correctly excluded first-slot/sub-second sniping from the target
    capability, but missing numeric evidence was converted to zero by its display
    helper. This wrapper preserves every existing structural blocker and makes
    unknown evidence fail closed instead of looking artificially fast or cheap.
    """

    if _ORIGINAL_CLASSIFY is None:
        raise RuntimeError("wallet context precision repair is not installed")
    payload = _ORIGINAL_CLASSIFY(row)
    reasons = list(dict.fromkeys(str(value) for value in payload.get("reasons", [])))

    lag_ms = _safe_float(row.get("observation_lag_ms"))
    processing_ms = _safe_float(row.get("processing_delay_ms"))
    chase = _safe_float(row.get("chase_fraction"))

    if lag_ms is None:
        reasons.append("observation_latency_unknown")
    if processing_ms is None:
        reasons.append("processing_delay_unknown")
    if chase is None:
        reasons.append("chase_unknown")
    if "risk_complete" in row and not bool(row.get("risk_complete")):
        reasons.append("risk_incomplete_at_observation")

    reasons = list(dict.fromkeys(reasons))
    payload["reasons"] = reasons
    payload["structurally_accessible"] = not reasons
    payload["observed_pipeline_seconds"] = (
        max(0.0, lag_ms + processing_ms) / 1000.0
        if lag_ms is not None and processing_ms is not None
        else None
    )
    payload["chase_fraction"] = chase
    payload["missing_accessibility_evidence_fails_closed"] = True
    return payload


def _percent_context_metrics(role: WalletRole, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Retain fraction fields for compatibility and add explicit percentage fields."""

    if _ORIGINAL_CONTEXT_METRICS is None:
        raise RuntimeError("wallet context precision repair is not installed")
    payload = _ORIGINAL_CONTEXT_METRICS(role, rows)
    payload.update(
        {
            "mean_residual_roi_pct": _pct(payload.get("mean_residual_roi")),
            "median_residual_roi_pct": _pct(payload.get("median_residual_roi")),
            "trimmed_mean_residual_roi_ex_best_1_pct": _pct(
                payload.get("trimmed_mean_residual_roi_ex_best_1")
            ),
            "trimmed_mean_residual_roi_ex_best_3_pct": _pct(
                payload.get("trimmed_mean_residual_roi_ex_best_3")
            ),
            "trimmed_mean_residual_roi_ex_best_5_pct": _pct(
                payload.get("trimmed_mean_residual_roi_ex_best_5")
            ),
            "copyable_return_on_deployed_fraction_pct": _pct(
                payload.get("copyable_return_on_deployed_fraction")
            ),
            "compounded_fraction_scaled_return_pct": _pct(
                payload.get("compounded_fraction_scaled_return")
            ),
            "positive_rate_pct": _pct(payload.get("positive_rate")),
            "max_drawdown_fraction_scaled_pct": _pct(
                payload.get("max_drawdown_fraction_scaled")
            ),
        }
    )

    curve = payload.get("latency_residual_roi_curve")
    if isinstance(curve, dict):
        for bucket in curve.values():
            if not isinstance(bucket, dict):
                continue
            bucket["mean_residual_roi_pct"] = _pct(bucket.get("mean_residual_roi"))
            bucket["median_residual_roi_pct"] = _pct(bucket.get("median_residual_roi"))
    return payload


def _status_with_percent_and_fail_closed(self: WalletContextRouter) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("wallet context precision repair is not installed")
    payload = _ORIGINAL_STATUS(self)
    payload["router_version"] = REPAIR_VERSION
    payload["roi_output_unit"] = "percentage"
    payload["roi_percentage_fields_explicit"] = True
    payload["raw_fraction_fields_retained_for_backward_compatibility"] = True
    payload["accessibility_missing_evidence_fails_closed"] = True

    leaders = payload.get("copyable_roi_leaders")
    if isinstance(leaders, list):
        for row in leaders:
            if not isinstance(row, dict):
                continue
            row["copyable_return_on_deployed_fraction_pct"] = _pct(
                row.get("copyable_return_on_deployed_fraction")
            )
            row["median_residual_roi_pct"] = _pct(row.get("median_residual_roi"))
            row["trimmed_mean_residual_roi_ex_best_1_pct"] = _pct(
                row.get("trimmed_mean_residual_roi_ex_best_1")
            )
            row["compounded_fraction_scaled_return_pct"] = _pct(
                row.get("compounded_fraction_scaled_return")
            )

    routes = payload.get("route_map")
    if isinstance(routes, list):
        for route in routes:
            if not isinstance(route, dict):
                continue
            roles = route.get("roles")
            if not isinstance(roles, dict):
                continue
            for role_rows in roles.values():
                if not isinstance(role_rows, list):
                    continue
                for row in role_rows:
                    if isinstance(row, dict):
                        row["copyable_return_on_deployed_fraction_pct"] = _pct(
                            row.get("copyable_return_on_deployed_fraction")
                        )
    return payload


def _manifest_with_percent_and_fail_closed(
    self: FinalProfitFirstResearchAdapter,
) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("wallet context precision repair manifest is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "wallet_context_router_precision_repair": REPAIR_VERSION,
            "wallet_roi_output_unit": "percentage",
            "wallet_roi_raw_fraction_fields_retained_for_backward_compatibility": True,
            "wallet_accessibility_missing_latency_fails_closed": True,
            "wallet_accessibility_missing_processing_delay_fails_closed": True,
            "wallet_accessibility_missing_chase_fails_closed": True,
            "wallet_accessibility_incomplete_risk_fails_closed_when_present": True,
            "strategy_thresholds_unchanged": True,
            "active_strategy_mutation_allowed": False,
            "historical_evidence_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_wallet_context_router_precision_repair() -> None:
    """Install precision, governance, bandwidth, FOMO and context assignment exactly once."""

    global _ORIGINAL_CLASSIFY, _ORIGINAL_CONTEXT_METRICS, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST

    current_classify = router_module.classify_observation_accessibility
    if _ORIGINAL_CLASSIFY is None:
        _ORIGINAL_CLASSIFY = current_classify
        setattr(_fail_closed_accessibility, "_roi_wallet_context_precision_repair", True)
        router_module.classify_observation_accessibility = _fail_closed_accessibility

    current_metrics = router_module._context_metrics
    if _ORIGINAL_CONTEXT_METRICS is None:
        _ORIGINAL_CONTEXT_METRICS = current_metrics
        setattr(_percent_context_metrics, "_roi_wallet_context_precision_repair", True)
        router_module._context_metrics = _percent_context_metrics

    current_status = WalletContextRouter.status
    if _ORIGINAL_STATUS is None:
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_percent_and_fail_closed.__dict__.update(
                getattr(current_status, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_status_with_percent_and_fail_closed, "_roi_wallet_context_precision_repair", True)
        WalletContextRouter.status = _status_with_percent_and_fail_closed  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if _ORIGINAL_MANIFEST is None:
        _ORIGINAL_MANIFEST = current_manifest
        try:
            _manifest_with_percent_and_fail_closed.__dict__.update(
                getattr(current_manifest, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_manifest_with_percent_and_fail_closed, "_roi_wallet_context_precision_repair", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_percent_and_fail_closed  # type: ignore[method-assign]

    from . import wallet_context_governance as governance_module

    if governance_module._ORIGINAL_FINAL_MANIFEST is None:
        governance_module.install_wallet_context_governance()

    from . import context_research_bandwidth_governor as bandwidth_module

    if bandwidth_module._ORIGINAL_MANIFEST is None:
        bandwidth_module.install_context_research_bandwidth_governor()

    # FOMO is a subordinate production-shadow consumer of the already-governed
    # prospective evidence. Its installer also captures each wrapped method once.
    from .fomo_runtime_install import install_fomo_runtime

    install_fomo_runtime()

    # Report FOMO profitability without pooling outcomes across venue/lifecycle.
    from .fomo_venue_lifecycle_reporting import install_fomo_venue_lifecycle_reporting

    install_fomo_venue_lifecycle_reporting()

    # Scarce challenger tracking is partitioned by proven exact context. Bootstrap
    # observation remains possible, but bootstrap wallets receive no strategy authority.
    from .wallet_context_tracking_assignment import install_wallet_context_tracking_assignment

    install_wallet_context_tracking_assignment()


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "REPAIR_VERSION",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "install_wallet_context_router_precision_repair",
]
