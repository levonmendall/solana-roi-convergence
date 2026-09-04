from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable

from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_context_router import WalletContextRouter
from .wallet_entity_universe_v4 import SEED_BY_ADDRESS


PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
ACTIVE_TRACKING_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
GOVERNANCE_VERSION = "wallet-context-governance-v1"

_ORIGINAL_ROUTER_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _context_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("venue") or "UNKNOWN"),
        str(row.get("lifecycle_stage") or "unknown_or_unsupported_venue"),
        str(row.get("regime") or "unknown"),
        str(row.get("role") or "unknown"),
    )


def _robust_positive(row: dict[str, Any]) -> bool:
    if not bool(row.get("mature_forward_context")):
        return False
    score = _safe_float(row.get("context_score"))
    trimmed = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1"))
    return bool(score is not None and score > 0.0 and trimmed is not None and trimmed > 0.0)


def _robust_negative(row: dict[str, Any]) -> bool:
    if not bool(row.get("mature_forward_context")):
        return False
    score = _safe_float(row.get("context_score"))
    trimmed = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1"))
    if score is not None and score <= 0.0:
        return True
    return bool(trimmed is not None and trimmed <= 0.0)


def _evidence_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "sample_count": int(row.get("sample_count") or 0),
        "context_score": _safe_float(row.get("context_score")),
        "context_confidence": _safe_float(row.get("context_confidence")),
        "copyable_return_on_deployed_fraction": _safe_float(
            row.get("copyable_return_on_deployed_fraction")
        ),
        "copyable_return_on_deployed_fraction_pct": _safe_float(
            row.get("copyable_return_on_deployed_fraction_pct")
        ),
        "median_residual_roi": _safe_float(row.get("median_residual_roi")),
        "median_residual_roi_pct": _safe_float(row.get("median_residual_roi_pct")),
        "trimmed_mean_residual_roi_ex_best_1": _safe_float(
            row.get("trimmed_mean_residual_roi_ex_best_1")
        ),
        "trimmed_mean_residual_roi_ex_best_1_pct": _safe_float(
            row.get("trimmed_mean_residual_roi_ex_best_1_pct")
        ),
        "positive_rate": _safe_float(row.get("positive_rate")),
        "positive_rate_pct": _safe_float(row.get("positive_rate_pct")),
    }


class WalletContextGovernance:
    """Prospective promotion/demotion research for exact wallet contexts.

    A wallet never receives a universal good/bad label. Decisions are evaluated only
    inside an exact venue × lifecycle × regime × role context. This layer produces
    recommendations for a future governed cohort; it cannot mutate tracking, entry,
    sizing, exits, signing, submission, or live-money authority.
    """

    def __init__(self, router: WalletContextRouter):
        self.router = router
        self.universe = router.universe
        self.store = router.store

    def _tracking_states(self) -> dict[str, str]:
        try:
            with self.store._lock:
                exists = self.store.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='wallet_discovery_candidates' LIMIT 1"
                ).fetchone()
                if exists is None:
                    return {}
                rows = self.store.db.execute(
                    "SELECT wallet,state FROM wallet_discovery_candidates"
                ).fetchall()
            return {str(row["wallet"]): str(row["state"]) for row in rows}
        except Exception:
            return {}

    @staticmethod
    def _is_incumbent(wallet: str, state: str | None) -> bool:
        # Seed entities are hypotheses, not permanent whitelists, but they are the
        # current named reference set and therefore must be eligible for demotion.
        return state == "incumbent_tracking" or wallet in SEED_BY_ADDRESS

    def evaluate(self, profiles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        rows = list(profiles if profiles is not None else self.router.context_profiles())
        states = self._tracking_states()

        annotated: list[dict[str, Any]] = []
        for row in rows:
            wallet = str(row.get("wallet") or "")
            if not wallet:
                continue
            state = states.get(wallet)
            incumbent = self._is_incumbent(wallet, state)
            robust_positive = _robust_positive(row)
            robust_negative = _robust_negative(row)
            if not bool(row.get("mature_forward_context")):
                action = "observe_only_insufficient_forward_context"
            elif robust_positive and incumbent:
                action = "keep_for_future_context_influence"
            elif robust_positive:
                action = "promote_for_future_context_influence"
            elif robust_negative and incumbent:
                action = "demote_for_future_context_influence"
            elif robust_negative:
                action = "withhold_from_future_context_influence"
            else:
                action = "observe_only_mixed_forward_context"
            annotated.append(
                {
                    "wallet": wallet,
                    "seed_name": row.get("seed_name"),
                    "tracking_state": state,
                    "is_current_reference_or_incumbent": incumbent,
                    "context": {
                        "venue": _context_key(row)[0],
                        "lifecycle_stage": _context_key(row)[1],
                        "regime": _context_key(row)[2],
                        "role": _context_key(row)[3],
                    },
                    "robust_positive_after_best_trade_trim": robust_positive,
                    "robust_negative_or_nonpositive_after_best_trade_trim": robust_negative,
                    "recommended_action": action,
                    "evidence": _evidence_summary(row),
                    "recommendation_has_tracking_mutation_authority": False,
                    "recommendation_has_strategy_authority": False,
                }
            )

        promotion = [
            row for row in annotated
            if row["recommended_action"] == "promote_for_future_context_influence"
        ]
        demotion = [
            row for row in annotated
            if row["recommended_action"] == "demote_for_future_context_influence"
        ]
        keep = [
            row for row in annotated
            if row["recommended_action"] == "keep_for_future_context_influence"
        ]

        by_context: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
        source_by_wallet_context: dict[tuple[str, tuple[str, str, str, str]], dict[str, Any]] = {}
        for source in rows:
            wallet = str(source.get("wallet") or "")
            if not wallet:
                continue
            key = _context_key(source)
            source_by_wallet_context[(wallet, key)] = source
            by_context[key].append(source)

        replacements: list[dict[str, Any]] = []
        for key, context_rows in sorted(by_context.items()):
            incumbents = [
                row
                for row in context_rows
                if self._is_incumbent(str(row.get("wallet") or ""), states.get(str(row.get("wallet") or "")))
                and bool(row.get("mature_forward_context"))
            ]
            challengers = [
                row
                for row in context_rows
                if not self._is_incumbent(str(row.get("wallet") or ""), states.get(str(row.get("wallet") or "")))
                and _robust_positive(row)
            ]
            if not incumbents or not challengers:
                continue
            for incumbent in incumbents:
                incumbent_score = _safe_float(incumbent.get("context_score"))
                if incumbent_score is None:
                    continue
                better = [
                    row
                    for row in challengers
                    if (_safe_float(row.get("context_score")) or float("-inf")) > incumbent_score
                ]
                if not better:
                    continue
                challenger = max(
                    better,
                    key=lambda row: (
                        _safe_float(row.get("context_score")) or float("-inf"),
                        int(row.get("sample_count") or 0),
                    ),
                )
                replacements.append(
                    {
                        "context": {
                            "venue": key[0],
                            "lifecycle_stage": key[1],
                            "regime": key[2],
                            "role": key[3],
                        },
                        "incumbent_wallet": str(incumbent.get("wallet") or ""),
                        "challenger_wallet": str(challenger.get("wallet") or ""),
                        "incumbent_evidence": _evidence_summary(incumbent),
                        "challenger_evidence": _evidence_summary(challenger),
                        "recommended_action": (
                            "replace_incumbent_for_future_context_influence"
                            if _robust_negative(incumbent)
                            else "challenge_incumbent_for_future_context_influence"
                        ),
                        "same_venue_lifecycle_regime_role_required": True,
                        "cross_context_success_transfer_allowed": False,
                        "recommendation_has_tracking_mutation_authority": False,
                        "recommendation_has_strategy_authority": False,
                    }
                )

        replacements.sort(
            key=lambda row: (
                _safe_float(row["challenger_evidence"].get("context_score")) or float("-inf")
            ),
            reverse=True,
        )
        promotion.sort(
            key=lambda row: _safe_float(row["evidence"].get("context_score")) or float("-inf"),
            reverse=True,
        )
        demotion.sort(
            key=lambda row: _safe_float(row["evidence"].get("context_score")) or float("inf")
        )

        return {
            "governance_version": GOVERNANCE_VERSION,
            "assignment_key": "wallet_or_entity_x_venue_x_lifecycle_x_role_x_regime",
            "evaluated_context_rows": len(annotated),
            "promotion_candidates": promotion,
            "keep_candidates": keep,
            "demotion_candidates": demotion,
            "context_replacement_pairs": replacements,
            "all_context_recommendations": annotated,
            "named_seed_is_permanent_whitelist": False,
            "incumbent_can_lose_future_context_influence": True,
            "challenger_can_replace_incumbent_only_in_same_context": True,
            "historical_evidence_can_directly_promote": False,
            "recommendations_require_forward_context_evidence": True,
            "best_trade_trim_required_for_positive_recommendation": True,
            "fomo_scope_modified": False,
            "recommendations_have_tracking_mutation_authority": False,
            "recommendations_have_strategy_authority": False,
            "active_strategy_mutation_allowed": False,
            "active_tracking_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }

    def status(self) -> dict[str, Any]:
        return self.evaluate()


def _status_with_context_governance(self: WalletContextRouter) -> dict[str, Any]:
    if _ORIGINAL_ROUTER_STATUS is None:
        raise RuntimeError("wallet context governance is not installed")
    payload = _ORIGINAL_ROUTER_STATUS(self)
    try:
        payload["context_governance"] = WalletContextGovernance(self).status()
    except Exception as exc:
        payload["context_governance"] = {
            "governance_version": GOVERNANCE_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: wallet context governance unavailable",
            "fomo_scope_modified": False,
            "recommendations_have_tracking_mutation_authority": False,
            "recommendations_have_strategy_authority": False,
            "active_strategy_mutation_allowed": False,
            "active_tracking_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    return payload


def _manifest_with_context_governance(
    self: FinalProfitFirstResearchAdapter,
) -> dict[str, Any]:
    if _ORIGINAL_FINAL_MANIFEST is None:
        raise RuntimeError("wallet context governance manifest is not installed")
    payload = _ORIGINAL_FINAL_MANIFEST(self)
    payload.update(
        {
            "wallet_context_governance": GOVERNANCE_VERSION,
            "wallet_context_governance_assignment_key": (
                "wallet_or_entity_x_venue_x_lifecycle_x_role_x_regime"
            ),
            "named_seed_is_permanent_whitelist": False,
            "incumbent_can_lose_future_context_influence": True,
            "challenger_replacement_requires_same_context": True,
            "best_trade_trim_required_for_positive_context_recommendation": True,
            "wallet_context_governance_fomo_scope_modified": False,
            "context_governance_has_tracking_mutation_authority": False,
            "context_governance_has_strategy_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_evidence_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_wallet_context_governance() -> None:
    """Install prospective context governance exactly once."""

    global _ORIGINAL_ROUTER_STATUS, _ORIGINAL_FINAL_MANIFEST

    current_status = WalletContextRouter.status
    if not bool(getattr(current_status, "_roi_wallet_context_governance", False)):
        _ORIGINAL_ROUTER_STATUS = current_status
        try:
            _status_with_context_governance.__dict__.update(
                getattr(current_status, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_status_with_context_governance, "_roi_wallet_context_governance", True)
        WalletContextRouter.status = _status_with_context_governance  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if not bool(getattr(current_manifest, "_roi_wallet_context_governance", False)):
        _ORIGINAL_FINAL_MANIFEST = current_manifest
        try:
            _manifest_with_context_governance.__dict__.update(
                getattr(current_manifest, "__dict__", {})
            )
        except Exception:
            pass
        setattr(_manifest_with_context_governance, "_roi_wallet_context_governance", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_context_governance  # type: ignore[method-assign]


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "ACTIVE_TRACKING_MUTATION_ALLOWED",
    "GOVERNANCE_VERSION",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "WalletContextGovernance",
    "install_wallet_context_governance",
]
