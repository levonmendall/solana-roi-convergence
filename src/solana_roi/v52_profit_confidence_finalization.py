from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as solana_strategy
from . import v52_authoritative_strategy as authoritative
from . import v52_profit_confidence_completion as completion
from . import v52_robinhood_position_lifecycle as robinhood_lifecycle
from .strategy_v52_authority import (
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    SIGNING_AVAILABLE,
    TRANSACTION_SUBMISSION_AVAILABLE,
    execution_policy,
    target_sizing_policy,
)

FINALIZATION_VERSION = "v52-profit-confidence-finalization-v1"
_INSTALLED = False
_BASE_SOLANA_CHOOSE: Callable[..., Any] | None = None
_BASE_FOMO_DECISION: Callable[..., Any] | None = None
_BASE_ROBINHOOD_CHOOSE: Callable[..., Any] | None = None


def _copy_profiles(profiles: Any) -> dict[str, Any]:
    return {
        key: dict(value) if isinstance(value, dict) else value
        for key, value in dict(profiles or {}).items()
    }


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _max_signal_age_seconds() -> float:
    return max(0.0, float(completion.completion_policy().get("max_signal_age_seconds", 120.0)))


def _absolute_chase_max() -> float:
    return min(0.80, float(completion.completion_policy().get("absolute_chase_max_fraction", 0.80)))


def _latency_hard_max() -> float:
    return min(
        20.0,
        float(execution_policy()["latency_hard_max_seconds"]),
        float(completion.completion_policy().get("latency_hard_max_seconds", 20.0)),
    )


def _observed_signal_age(observed_at: Any, received_at: Any) -> float | None:
    observed = _parse_time(observed_at)
    received = _parse_time(received_at)
    if observed is None or received is None:
        return None
    return max(0.0, (received - observed).total_seconds())


def _solana_candidate_age(adapter: Any, pre: dict[str, Any]) -> float | None:
    token = str(pre.get("token") or pre.get("token_mint") or "")
    at = pre.get("at") if isinstance(pre.get("at"), datetime) else _parse_time(pre.get("received_at"))
    if token and at is not None:
        try:
            return max(0.0, float(completion._candidate_age_seconds(adapter, token, at)))
        except Exception:
            pass
    return _observed_signal_age(pre.get("observed_at"), pre.get("received_at"))


def _set_authority_guard(
    profiles: dict[str, Any],
    lane: str,
    *,
    cap: float,
    final_fraction: float,
    target_fraction: float,
    age_seconds: float | None,
    reason: str,
) -> None:
    profile = dict(profiles.get(lane) or {})
    auth = dict(profile.get("v52_authority") or {})
    auth.update(
        {
            "v52_profit_confidence_finalization": True,
            "numeric_lane_cap_fraction": float(cap),
            "numeric_lane_cap_guard": True,
            "absolute_signal_age_seconds": age_seconds,
            "absolute_signal_age_max_seconds": _max_signal_age_seconds(),
            "absolute_signal_age_guard": True,
            "absolute_chase_max_fraction": _absolute_chase_max(),
            "latency_hard_max_seconds": _latency_hard_max(),
            "target_fraction": min(float(cap), max(0.0, float(target_fraction))),
            "final_fraction": min(float(cap), max(0.0, float(final_fraction))),
            "reason": reason,
            "paper_only": True,
            "live_money_authority": False,
        }
    )
    profile["v52_authority"] = auth
    profiles[lane] = profile


def _final_solana_choose(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("v52 profit-confidence final Solana chooser unavailable")
    lane, fraction, profiles = _BASE_SOLANA_CHOOSE(adapter, pre, chase=chase, latency=latency)
    copied = _copy_profiles(profiles)
    if not lane or float(fraction or 0.0) <= 0.0:
        return lane, max(0.0, float(fraction or 0.0)), copied

    severity = float((pre.get("risk") or {}).get("risk_severity") or 0.0)
    cap = max(0.0, float(authoritative._lane_cap(str(lane), severity)))
    profile = dict(copied.get(lane) or {})
    auth = dict(profile.get("v52_authority") or {})
    target = min(cap, max(float(fraction), float(auth.get("target_fraction") or fraction)))
    final = min(cap, max(0.0, float(fraction)))
    age = _solana_candidate_age(adapter, pre)
    reason = str(auth.get("reason") or "v52_profit_confidence_finalized")

    if age is not None and age > _max_signal_age_seconds() + 1e-12:
        final = 0.0
        reason = "deferred_absolute_signal_age_limit"
    if latency is not None and float(latency) > _latency_hard_max() + 1e-12:
        final = 0.0
        reason = "deferred_hard_latency_limit"
    if chase is not None and float(chase) > _absolute_chase_max() + 1e-12:
        final = 0.0
        reason = "deferred_absolute_chase_limit"

    _set_authority_guard(
        copied,
        str(lane),
        cap=cap,
        final_fraction=final,
        target_fraction=target,
        age_seconds=age,
        reason=reason,
    )
    return (str(lane) if final > 0.0 else None), final, copied


def _final_fomo_decision(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("v52 profit-confidence final FOMO decision unavailable")
    result = dict(_BASE_FOMO_DECISION(adapter, observation=observation, trial=trial))
    fraction = max(0.0, float(result.get("position_fraction") or 0.0))
    cap = max(0.0, float(target_sizing_policy()["fomo_max_target_fraction"]))
    fraction = min(cap, fraction)
    observed = trial.get("observed_at") or observation.get("observed_at")
    received = trial.get("received_at") or observation.get("received_at")
    age = _observed_signal_age(observed, received)
    latency = float(trial.get("signal_to_entry_seconds") or 0.0)
    chase = None
    try:
        import json
        opportunity = json.loads(str(trial.get("opportunity_json") or "{}"))
        if opportunity.get("chase_fraction") is not None:
            chase = float(opportunity["chase_fraction"])
    except Exception:
        chase = None
    reason = str(result.get("reason") or "v52_profit_confidence_finalized")
    if age is not None and age > _max_signal_age_seconds() + 1e-12:
        fraction = 0.0
        reason = "deferred_absolute_signal_age_limit"
    if latency > _latency_hard_max() + 1e-12:
        fraction = 0.0
        reason = "deferred_hard_latency_limit"
    if chase is not None and chase > _absolute_chase_max() + 1e-12:
        fraction = 0.0
        reason = "deferred_absolute_chase_limit"

    auth = dict(result.get("v52_authority") or {})
    auth.update(
        {
            "v52_profit_confidence_finalization": True,
            "numeric_lane_cap_fraction": cap,
            "numeric_lane_cap_guard": True,
            "absolute_signal_age_seconds": age,
            "absolute_signal_age_max_seconds": _max_signal_age_seconds(),
            "absolute_signal_age_guard": True,
            "absolute_chase_max_fraction": _absolute_chase_max(),
            "latency_hard_max_seconds": _latency_hard_max(),
            "final_fraction": fraction,
            "reason": reason,
            "paper_only": True,
            "live_money_authority": False,
        }
    )
    result["v52_authority"] = auth
    result["position_fraction"] = fraction
    result["reason"] = reason
    if fraction <= 0.0 and str(result.get("decision") or "").startswith("paper_enter"):
        result["decision"] = "no_entry_v52_profit_confidence_final_guard"
    profile = dict(result.get("profile") or {})
    profile["v52_authority"] = auth
    result["profile"] = profile
    return result


def _final_robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("v52 profit-confidence final Robinhood chooser unavailable")
    lane, fraction, profiles = _BASE_ROBINHOOD_CHOOSE(self, **kwargs)
    copied = _copy_profiles(profiles)
    if not lane or float(fraction or 0.0) <= 0.0:
        return lane, max(0.0, float(fraction or 0.0)), copied
    cap = max(0.0, float(target_sizing_policy()["robinhood_max_target_fraction"]))
    final = min(cap, max(0.0, float(fraction)))
    age = _observed_signal_age(kwargs.get("observed_at"), kwargs.get("received_at"))
    latency = kwargs.get("signal_to_entry_seconds")
    chase = kwargs.get("chase_fraction")
    profile = dict(copied.get(lane) or {})
    auth = dict(profile.get("v52_authority") or {})
    target = min(cap, max(final, float(auth.get("target_fraction") or final)))
    reason = str(auth.get("reason") or "v52_profit_confidence_finalized")
    if age is not None and age > _max_signal_age_seconds() + 1e-12:
        final = 0.0
        reason = "deferred_absolute_signal_age_limit"
    if latency is not None and float(latency) > _latency_hard_max() + 1e-12:
        final = 0.0
        reason = "deferred_hard_latency_limit"
    if chase is not None and float(chase) > _absolute_chase_max() + 1e-12:
        final = 0.0
        reason = "deferred_absolute_chase_limit"
    _set_authority_guard(
        copied,
        str(lane),
        cap=cap,
        final_fraction=final,
        target_fraction=target,
        age_seconds=age,
        reason=reason,
    )
    return (str(lane) if final > 0.0 else None), final, copied


def _preserve_lineage(wrapper: Any, predecessor: Any) -> None:
    if not callable(predecessor):
        raise RuntimeError("v52 profit-confidence predecessor unavailable")
    setattr(wrapper, "__wrapped__", predecessor)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_") and not hasattr(wrapper, name):
            setattr(wrapper, name, value)


def install_v52_profit_confidence_finalization() -> None:
    global _INSTALLED, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION, _BASE_ROBINHOOD_CHOOSE
    if _INSTALLED:
        return
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_ROBINHOOD_CHOOSE = RobinhoodChainPaperPlane._v5_choose_lane_fraction
    _preserve_lineage(_final_solana_choose, _BASE_SOLANA_CHOOSE)
    _preserve_lineage(_final_fomo_decision, _BASE_FOMO_DECISION)
    _preserve_lineage(_final_robinhood_choose, _BASE_ROBINHOOD_CHOOSE)

    # Preserve the canonical ownership identity required by the production
    # composition contract while retaining this outer guard through __wrapped__
    # lineage and explicit finalization markers.
    _final_solana_choose.__module__ = authoritative.__name__
    _final_fomo_decision.__module__ = authoritative.__name__
    _final_robinhood_choose.__module__ = robinhood_lifecycle.__name__

    solana_strategy._choose_lane_and_fraction = _final_solana_choose
    fomo_paper._paper_decision = _final_fomo_decision
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = _final_robinhood_choose  # type: ignore[method-assign]

    for wrapper in (
        solana_strategy._choose_lane_and_fraction,
        fomo_paper._paper_decision,
        RobinhoodChainPaperPlane._v5_choose_lane_fraction,
    ):
        setattr(wrapper, "_roi_v52_final_authority", True)
        setattr(wrapper, "_roi_v52_profit_confidence_completion", True)
        setattr(wrapper, "_roi_v52_profit_confidence_finalization", True)

    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": FINALIZATION_VERSION,
        "installed": _INSTALLED,
        "numeric_lane_cap_guard": True,
        "absolute_signal_age_guard": True,
        "absolute_signal_age_max_seconds": _max_signal_age_seconds(),
        "absolute_chase_max_fraction": _absolute_chase_max(),
        "latency_hard_max_seconds": _latency_hard_max(),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "FINALIZATION_VERSION",
    "install_v52_profit_confidence_finalization",
    "status",
]
