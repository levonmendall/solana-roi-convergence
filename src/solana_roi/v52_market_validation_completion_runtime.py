from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from . import v52_market_validation_completion as completion_module
from . import v52_market_validation_completion_hardening as hardening_module

VERSION = "v52-market-validation-completion-runtime-v1"
_RUNTIME: "HardenedRuntime | None" = None
_INSTALLED = False

_SEQUENTIAL_COMPONENT_PAIRS = {
    "wallet_intelligence": ("C_v52_full", "B_v52_no_wallet"),
    "pre_graduation_entry_bundle": ("B_v52_no_wallet", "A_graduation_only"),
    "graduation_quality": ("D_v52_plus_graduation_quality", "C_v52_full"),
    "post_graduation_decay": ("E_v52_plus_graduation_quality_decay", "D_v52_plus_graduation_quality"),
    "lane_relative_calibration": (
        "F_v52_plus_graduation_quality_decay_lane_calibration",
        "E_v52_plus_graduation_quality_decay",
    ),
    "lane_gating": (
        "G_full_proposed_alpha_gated",
        "F_v52_plus_graduation_quality_decay_lane_calibration",
    ),
}


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None


def _iso(value: Any) -> str:
    return hardening_module._iso(value)


class HardenedRuntime(hardening_module.MarketValidationCompletionHardening):
    def record_market_mark(self, engine: completion_module.MarketValidationCompletion, **kwargs: Any) -> int:
        token = str(kwargs.get("token_mint") or "")
        state = kwargs.get("graduation_state")
        if token and hardening_module._graduated(state):
            with self.store._lock, self.store.db:
                rows = self.store.db.execute(
                    "SELECT e.candidate_key,e.observed_at,c.decision_fraction "
                    "FROM v52_market_validation_lane_events e "
                    "JOIN v52_market_validation_shadow_variants c "
                    "ON c.candidate_key=e.candidate_key AND c.observed_at=e.observed_at AND c.variant_id='C_v52_full' "
                    "WHERE e.token_mint=?",
                    (token,),
                ).fetchall()
                for row in rows:
                    self.store.db.execute(
                        "UPDATE v52_market_validation_shadow_variants SET decision_fraction=?,decision='paper_counterfactual_enter' "
                        "WHERE candidate_key=? AND observed_at=? AND variant_id='A_graduation_only' "
                        "AND NOT EXISTS (SELECT 1 FROM v52_market_validation_shadow_entries se "
                        "WHERE se.candidate_key=? AND se.observed_at=? AND se.variant_id='A_graduation_only')",
                        (
                            float(row["decision_fraction"] or 0.0),
                            str(row["candidate_key"]),
                            str(row["observed_at"]),
                            str(row["candidate_key"]),
                            str(row["observed_at"]),
                        ),
                    )
        return super().record_market_mark(engine, **kwargs)

    def resolve_outcome(self, engine: completion_module.MarketValidationCompletion, **kwargs: Any) -> None:
        super().resolve_outcome(engine, **kwargs)
        variants: Mapping[str, Any] = dict(kwargs.get("variant_returns") or {})
        if not variants:
            return
        candidate = str(kwargs["candidate_key"])
        observed = _iso(kwargs["observed_at"])
        resolved_at = _iso(kwargs.get("resolved_at") or datetime.now(timezone.utc))
        with self.store._lock, self.store.db:
            for component, (with_key, without_key) in _SEQUENTIAL_COMPONENT_PAIRS.items():
                with_value = _finite(variants.get(with_key))
                without_value = _finite(variants.get(without_key))
                if with_value is None or without_value is None:
                    continue
                self.store.db.execute(
                    "INSERT OR REPLACE INTO v52_market_validation_component_ablation("
                    "candidate_key,observed_at,component,with_component_return,without_component_return,incremental_return,resolved_at,"
                    "causal_claim,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,0,1,0)",
                    (candidate, observed, component, with_value, without_value, with_value - without_value, resolved_at),
                )

    def status(self) -> dict[str, Any]:
        payload = dict(super().status())
        payload.update(
            {
                "version": VERSION,
                "safe_instance_method_installation": True,
                "graduation_only_fraction_begins_at_graduation": True,
                "sequential_a_to_g_ablation": True,
                "sequential_component_pairs": {key: list(value) for key, value in _SEQUENTIAL_COMPONENT_PAIRS.items()},
            }
        )
        return payload


def _patch_engine(engine: completion_module.MarketValidationCompletion, runtime_layer: HardenedRuntime) -> None:
    engine.lane_capital_state = lambda lane, decision_at: runtime_layer.lane_capital_state(engine, lane, decision_at)  # type: ignore[method-assign]
    engine.evaluate_candidate = lambda **kwargs: runtime_layer.evaluate_candidate(engine, **kwargs)  # type: ignore[method-assign]
    engine.record_market_mark = lambda **kwargs: runtime_layer.record_market_mark(engine, **kwargs)  # type: ignore[method-assign]
    engine.resolve_outcome = lambda **kwargs: runtime_layer.resolve_outcome(engine, **kwargs)  # type: ignore[method-assign]


def install_v52_market_validation_completion_runtime(
    engine: completion_module.MarketValidationCompletion,
) -> HardenedRuntime:
    global _RUNTIME, _INSTALLED
    if _RUNTIME is None or _RUNTIME.engine is not engine:
        _RUNTIME = HardenedRuntime(engine)
        _patch_engine(engine, _RUNTIME)
    elif not _INSTALLED:
        _patch_engine(engine, _RUNTIME)
    hardening_module._HARDENING = _RUNTIME
    hardening_module._INSTALLED = True
    _INSTALLED = True
    return _RUNTIME


def runtime() -> HardenedRuntime:
    if _RUNTIME is None:
        raise RuntimeError("v5.2 market-validation completion runtime not installed")
    return _RUNTIME


def status() -> dict[str, Any]:
    if _RUNTIME is None:
        return {"version": VERSION, "installed": False, "paper_only": True, "live_money_authority": False}
    payload = dict(_RUNTIME.status())
    payload["installed"] = _INSTALLED
    return payload


__all__ = [
    "VERSION",
    "HardenedRuntime",
    "install_v52_market_validation_completion_runtime",
    "runtime",
    "status",
]
