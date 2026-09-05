from __future__ import annotations

from typing import Any, Callable

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority_fingerprint

HARDENING_VERSION = "v51-measurement-integrity-hardening-v2"
_ORIGINAL_WALLET_RECORD: Callable[..., Any] | None = None
_INSTALLED = False


def _ensure_release_compatibility_fail_closed(store: Any, release_commit: str | None = None) -> dict[str, Any] | None:
    from . import v51_measurement_integrity as measurement

    measurement._compat_schema(store)
    release = (release_commit or measurement.current_release_commit() or "").strip().lower()
    if not release:
        return None
    with store._lock:
        existing = store.db.execute(
            "SELECT * FROM v51_release_compatibility WHERE release_commit=?",
            (release,),
        ).fetchone()
    if existing is not None:
        return dict(existing)

    if release == measurement.current_release_commit():
        original = getattr(measurement, "_ORIGINAL_ENSURE_RELEASE_COMPATIBILITY", None)
        if not callable(original) or original is _ensure_release_compatibility_fail_closed:
            raise RuntimeError("base measurement compatibility registrar unavailable")
        return original(store, release)

    with store._lock, store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO v51_release_compatibility("
            "release_commit,authority_id,economic_epoch,economic_fingerprint,measurement_epoch,measurement_fingerprint,"
            "execution_model_epoch,execution_model_fingerprint,candidate_coverage_valid,latency_measurement_valid,"
            "execution_measurement_valid,wallet_attribution_valid,fomo_measurement_valid,robinhood_measurement_valid,"
            "promotion_eligible,reason,registered_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,0,0,0,0,0,0,0,?,?,1,0)",
            (
                release,
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
                authority_fingerprint(),
                "unclassified-historical-release",
                "unclassified-historical-release",
                measurement.EXECUTION_MODEL_EPOCH,
                measurement.execution_model_fingerprint(),
                "historical_release_not_explicitly_registered_while_running; promotion fails closed",
                measurement._utcnow(),
            ),
        )
        row = store.db.execute(
            "SELECT * FROM v51_release_compatibility WHERE release_commit=?",
            (release,),
        ).fetchone()
    return dict(row) if row is not None else None


async def _wallet_record_with_candidate_schema(self: Any, swap: Any) -> bool:
    if _ORIGINAL_WALLET_RECORD is None:
        raise RuntimeError("measurement-integrity wallet hardening is not installed")
    from .v51_candidate_ledger import ensure_schema

    ensure_schema(self.store)
    return bool(await _ORIGINAL_WALLET_RECORD(self, swap))


setattr(_wallet_record_with_candidate_schema, "_roi_v51_measurement_integrity_hardening", True)


def install_measurement_integrity_hardening() -> None:
    global _INSTALLED, _ORIGINAL_WALLET_RECORD
    if _INSTALLED:
        return
    from . import runtime
    from . import v51_measurement_compatibility_filters as filters
    from . import v51_measurement_integrity as measurement
    from .wallet_discovery import ContinuousWalletDiscovery

    if not hasattr(measurement, "_ORIGINAL_ENSURE_RELEASE_COMPATIBILITY"):
        measurement._ORIGINAL_ENSURE_RELEASE_COMPATIBILITY = measurement.ensure_release_compatibility  # type: ignore[attr-defined]
    measurement.ensure_release_compatibility = _ensure_release_compatibility_fail_closed  # type: ignore[assignment]
    filters.ensure_release_compatibility = _ensure_release_compatibility_fail_closed  # type: ignore[assignment]

    # Keep the immutable v3.1 runtime policy factory itself unchanged for baseline
    # lineage/certification compatibility. The v5.1 measurement wrapper reclassifies
    # persisted wallet observations using the <=40% research ceiling, so research
    # evidence is no longer censored without pretending the legacy policy changed.
    original_policy = getattr(measurement, "_ORIGINAL_WALLET_POLICY", None)
    if callable(original_policy):
        runtime._wallet_discovery_policy = original_policy  # type: ignore[assignment]

    current = ContinuousWalletDiscovery._record_forward_swap
    if not bool(getattr(current, "_roi_v51_measurement_integrity_hardening", False)):
        _ORIGINAL_WALLET_RECORD = current
        try:
            _wallet_record_with_candidate_schema.__dict__.update(getattr(current, "__dict__", {}))
        except Exception:
            pass
        setattr(_wallet_record_with_candidate_schema, "_roi_v51_measurement_integrity_hardening", True)
        ContinuousWalletDiscovery._record_forward_swap = _wallet_record_with_candidate_schema  # type: ignore[method-assign]

    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": HARDENING_VERSION,
        "installed": _INSTALLED,
        "only_current_release_can_auto_register_measurement_valid": True,
        "unclassified_historical_release_promotion_eligible": False,
        "legacy_wallet_policy_factory_preserved": True,
        "v51_wallet_research_reclassification_ceiling_fraction": 0.40,
        "wallet_lineage_candidate_schema_precreated": True,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["HARDENING_VERSION", "install_measurement_integrity_hardening", "status"]
