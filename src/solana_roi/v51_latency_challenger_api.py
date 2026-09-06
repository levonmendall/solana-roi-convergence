from __future__ import annotations

from typing import Any, Callable

from .v51_latency_challenger import build_latency_challenger_research
from .v51_measurement_integrity import cached_proof_state, decorate_proof
from .v51_roadmap_59_64_research import build_roadmap_59_64_research


API_VERSION = "v51-latency-challenger-api-v2-roadmap-59-64"
_INSTALLED = False


def _runtime(provider: Callable[[], Any] | Any) -> Any:
    return provider() if callable(provider) else provider


def _isolated_robinhood_latency(
    status_provider: Callable[[], dict[str, Any]] | None,
) -> tuple[dict[str, Any] | None, str]:
    if status_provider is None:
        return None, "unavailable"
    try:
        status = status_provider()
    except Exception:
        return None, "unavailable"
    if not isinstance(status, dict) or bool(status.get("failed_closed")) or not bool(status.get("runtime_ready")):
        return None, "unavailable"
    proof = status.get("v51_proof")
    if not isinstance(proof, dict) or not bool(proof.get("available")):
        return None, "unavailable"
    proof_state = cached_proof_state(proof)
    if proof_state not in {"confirmed", "partial"}:
        return None, proof_state
    challenger = proof.get("latency_challenger")
    return (challenger if isinstance(challenger, dict) else None), proof_state


def install_v51_latency_challenger_api(
    app: Any,
    runtime_provider: Callable[[], Any] | Any,
    *,
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    @app.get("/v1/strategy/latency-challengers")
    def v51_latency_challengers() -> dict[str, Any]:
        runtime = _runtime(runtime_provider)
        local = build_latency_challenger_research(runtime.store)
        roadmap = build_roadmap_59_64_research(runtime.store)
        robinhood, rh_state = _isolated_robinhood_latency(robinhood_status_provider)
        payload = {
            "api_version": API_VERSION,
            "local_solana_fomo": local,
            "roadmap_59_64_research": roadmap,
            "isolated_robinhood": robinhood,
            "robinhood_proof_state": rh_state,
            "current_authority_changed": False,
            "retrospective_entry_authority": False,
            "purpose": (
                "measure venue/lifecycle-specific forward edge, FOMO sub-20 signal half-life, "
                "Pump.fun-to-PumpSwap lifecycle transitions, and latency/chase/cost decay without changing frozen v5.1 economics"
            ),
            "paper_only": True,
            "live_money_authority": False,
        }
        overall = "confirmed" if rh_state == "confirmed" else "partial"
        return decorate_proof(payload, runtime.store, proof_state=overall)

    _INSTALLED = True


__all__ = ["API_VERSION", "install_v51_latency_challenger_api"]
