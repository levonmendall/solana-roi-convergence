from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority, authority_fingerprint

MEASUREMENT_INTEGRITY_VERSION = "v51-measurement-integrity-v1"
MEASUREMENT_EPOCH = "v51-measurement-post185-20260905-1"
EXECUTION_MODEL_EPOCH = "v51-execution-model-20260905-1"
PROOF_MAX_AGE_SECONDS = 120.0

# These releases remain available for audit/economic-history purposes, but production
# proved their observation/proof plumbing was not statistically exchangeable with the
# post-PR185 measurement path. They therefore cannot authorize promotion in this
# measurement epoch.
KNOWN_MEASUREMENT_DEFECTS: dict[str, str] = {
    "cec4887ec9f1a737b21b9a0a2096f7961b4d8762": "pre-production-proof-readiness; candidate/proof coverage not established",
    "f74b9d3db7c1bae081ea91e2853dc1af6095ec2d": "production proved current-context proof scheduler and exact PUMP websocket frontier were attached below live call sites",
    "bed5d6c582efd388845e4defd6cb999efc37a0cf": "pre-PR185 production-proof wiring; measurement defects remained unresolved",
}

_ORIGINAL_CANDIDATE_DELEGATE: Callable[..., Any] | None = None
_ORIGINAL_WALLET_POLICY: Callable[..., Any] | None = None
_ORIGINAL_WALLET_RECORD: Callable[..., Any] | None = None
_ORIGINAL_SHADOW_INIT: Callable[..., Any] | None = None
_ORIGINAL_RH_CACHED_PROOF: Callable[..., Any] | None = None
_INSTALLED = False


def _utcnow_dt() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow() -> str:
    return _utcnow_dt().isoformat()


def _canonical_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def current_release_commit() -> str | None:
    for name in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GITHUB_SHA", "GIT_COMMIT"):
        value = os.getenv(name, "").strip().lower()
        if len(value) == 40 and all(ch in "0123456789abcdef" for ch in value):
            return value
    return None


def research_chase_observation_max() -> float:
    return float(authority()["execution"]["chase_observe_only_above_fraction"])


def measurement_fingerprint() -> str:
    return _canonical_hash(
        {
            "version": MEASUREMENT_INTEGRITY_VERSION,
            "measurement_epoch": MEASUREMENT_EPOCH,
            "solana_candidate_ingress": "candidate_execution_delegate_normalized_scout_pre_risk_quote_strategy",
            "candidate_ledger": "append_only_v51_candidates_and_stage_events",
            "wallet_research_chase_observation_max": research_chase_observation_max(),
            "wallet_observation_lineage": "immutable_companion_history",
            "pump_websocket_provenance": "post185_real_mapped_ws_plus_exact_durable_sqlite_identity",
            "candidate_proof_scheduler": "post185_actual_economic_scout_normalizer",
        }
    )


def execution_model_fingerprint() -> str:
    # This repair does not change v5.1 economic sizing or entry/exit rules. The
    # execution-model fingerprint describes the observation/simulation semantics used
    # to judge executable residual edge, independently from the economic authority.
    return _canonical_hash(
        {
            "execution_model_epoch": EXECUTION_MODEL_EPOCH,
            "solana": "amount_specific_jupiter_v2_order_unsigned_mainnet_simulation",
            "robinhood": "amount_specific_onchain_quote_paper_only",
            "live_submission": False,
            "signing": False,
            "observation_chase_ceiling": research_chase_observation_max(),
        }
    )


def _compat_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_release_compatibility ("
            "release_commit TEXT PRIMARY KEY, authority_id TEXT NOT NULL, economic_epoch TEXT NOT NULL, "
            "economic_fingerprint TEXT NOT NULL, measurement_epoch TEXT NOT NULL, measurement_fingerprint TEXT NOT NULL, "
            "execution_model_epoch TEXT NOT NULL, execution_model_fingerprint TEXT NOT NULL, "
            "candidate_coverage_valid INTEGER NOT NULL, latency_measurement_valid INTEGER NOT NULL, "
            "execution_measurement_valid INTEGER NOT NULL, wallet_attribution_valid INTEGER NOT NULL, "
            "fomo_measurement_valid INTEGER NOT NULL, robinhood_measurement_valid INTEGER NOT NULL, "
            "promotion_eligible INTEGER NOT NULL, reason TEXT NOT NULL, registered_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        for release, reason in KNOWN_MEASUREMENT_DEFECTS.items():
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
                    "pre-v51-measurement-integrity",
                    "invalid-known-measurement-release",
                    EXECUTION_MODEL_EPOCH,
                    execution_model_fingerprint(),
                    reason,
                    _utcnow(),
                ),
            )


def ensure_release_compatibility(store: Any, release_commit: str | None = None) -> dict[str, Any] | None:
    _compat_schema(store)
    release = (release_commit or current_release_commit() or "").strip().lower()
    if not release:
        return None
    with store._lock, store.db:
        existing = store.db.execute(
            "SELECT * FROM v51_release_compatibility WHERE release_commit=?",
            (release,),
        ).fetchone()
        if existing is None:
            store.db.execute(
                "INSERT INTO v51_release_compatibility("
                "release_commit,authority_id,economic_epoch,economic_fingerprint,measurement_epoch,measurement_fingerprint,"
                "execution_model_epoch,execution_model_fingerprint,candidate_coverage_valid,latency_measurement_valid,"
                "execution_measurement_valid,wallet_attribution_valid,fomo_measurement_valid,robinhood_measurement_valid,"
                "promotion_eligible,reason,registered_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,1,1,1,1,1,1,1,?,?,1,0)",
                (
                    release,
                    AUTHORITY_ID,
                    ECONOMIC_FREEZE_EPOCH,
                    authority_fingerprint(),
                    MEASUREMENT_EPOCH,
                    measurement_fingerprint(),
                    EXECUTION_MODEL_EPOCH,
                    execution_model_fingerprint(),
                    "post185_measurement_integrity_release",
                    _utcnow(),
                ),
            )
        row = store.db.execute(
            "SELECT * FROM v51_release_compatibility WHERE release_commit=?",
            (release,),
        ).fetchone()
    return dict(row) if row is not None else None


def compatibility_status(store: Any, release_commit: str | None = None) -> dict[str, Any]:
    row = ensure_release_compatibility(store, release_commit)
    if row is None:
        return {
            "release_commit": None,
            "proof_state": "unavailable",
            "promotion_eligible": False,
            "reason": "release_commit_unbound",
        }
    result = dict(row)
    for key in (
        "candidate_coverage_valid",
        "latency_measurement_valid",
        "execution_measurement_valid",
        "wallet_attribution_valid",
        "fomo_measurement_valid",
        "robinhood_measurement_valid",
        "promotion_eligible",
        "paper_only",
        "live_money_authority",
    ):
        result[key] = bool(result.get(key))
    result["proof_state"] = "confirmed" if result["promotion_eligible"] else "invalid_measurement_epoch"
    return result


def _max_timestamp(store: Any, table: str, column: str) -> str | None:
    try:
        with store._lock:
            exists = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone()
            if exists is None:
                return None
            cols = {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}
            if column not in cols:
                return None
            row = store.db.execute(f"SELECT MAX({column}) AS value FROM {table}").fetchone()
        return str(row["value"]) if row is not None and row["value"] is not None else None
    except Exception:
        return None


def evidence_watermark(store: Any) -> str | None:
    values = [
        _max_timestamp(store, "v51_candidate_stage_events", "observed_at"),
        _max_timestamp(store, "risk_conditioned_alpha_v5_outcomes", "settled_at"),
        _max_timestamp(store, "fomo_paper_outcomes", "settled_at"),
        _max_timestamp(store, "robinhood_paper_outcomes", "settled_at"),
    ]
    clean = [value for value in values if value]
    return max(clean) if clean else None


def proof_metadata(store: Any | None = None, *, proof_state: str = "confirmed", generated_at: str | None = None) -> dict[str, Any]:
    generated = generated_at or _utcnow()
    release = current_release_commit()
    compatibility = compatibility_status(store, release) if store is not None else None
    state = proof_state
    if compatibility is not None and not bool(compatibility.get("promotion_eligible")):
        state = "invalid_measurement_epoch"
    return {
        "generated_at": generated,
        "proof_age_seconds": 0.0,
        "evidence_through": evidence_watermark(store) if store is not None else None,
        "release_commit": release,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "execution_model_epoch": EXECUTION_MODEL_EPOCH,
        "measurement_fingerprint": measurement_fingerprint(),
        "execution_model_fingerprint": execution_model_fingerprint(),
        "proof_state": state,
        "proof_max_age_seconds": PROOF_MAX_AGE_SECONDS,
        "promotion_eligible_measurement": bool(compatibility.get("promotion_eligible")) if compatibility is not None else release is None,
        "paper_only": True,
        "live_money_authority": False,
    }


def decorate_proof(payload: dict[str, Any], store: Any | None = None, *, proof_state: str | None = None) -> dict[str, Any]:
    result = dict(payload)
    requested = proof_state or str(result.get("proof_state") or "confirmed")
    result.update(proof_metadata(store, proof_state=requested))
    return result


def proof_age_seconds(proof: dict[str, Any] | None) -> float | None:
    if not isinstance(proof, dict):
        return None
    raw = proof.get("generated_at")
    if not raw:
        return None
    try:
        generated = datetime.fromisoformat(str(raw))
        if generated.tzinfo is None:
            generated = generated.replace(tzinfo=timezone.utc)
        return max(0.0, (_utcnow_dt() - generated.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


def cached_proof_state(proof: dict[str, Any] | None) -> str:
    if not isinstance(proof, dict) or not bool(proof.get("available", True)):
        return "unavailable"
    if str(proof.get("measurement_epoch") or "") not in {"", MEASUREMENT_EPOCH}:
        return "epoch_mismatch"
    if str(proof.get("execution_model_epoch") or "") not in {"", EXECUTION_MODEL_EPOCH}:
        return "epoch_mismatch"
    age = proof_age_seconds(proof)
    if age is None:
        return "partial"
    if age > PROOF_MAX_AGE_SECONDS:
        return "stale"
    state = str(proof.get("proof_state") or "confirmed")
    return state if state in {"confirmed", "partial", "unavailable", "stale", "epoch_mismatch", "invalid_measurement_epoch"} else "partial"


def chase_band(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return "unknown"
    value = float(value)
    if value <= 0.15:
        return "baseline_le_15pct"
    if value <= 0.25:
        return "challenger_15_25pct"
    if value <= 0.40:
        return "challenger_25_40pct"
    return "observe_only_gt_40pct"


def _wallet_lineage_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_wallet_discovery_forward_lineage ("
            "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, side TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, release_commit TEXT, authority_id TEXT NOT NULL, "
            "economic_epoch TEXT NOT NULL, measurement_epoch TEXT NOT NULL, execution_model_epoch TEXT NOT NULL, "
            "source_candidate_id TEXT, chase_fraction REAL, chase_band TEXT NOT NULL, research_eligible INTEGER NOT NULL, "
            "observation_lag_ms REAL NOT NULL, source TEXT NOT NULL, payload_json TEXT NOT NULL, recorded_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_wallet_lineage_wallet ON v51_wallet_discovery_forward_lineage(wallet,received_at)"
        )


async def _wallet_record_with_v51_research_lineage(self: Any, swap: Any) -> bool:
    if _ORIGINAL_WALLET_RECORD is None:
        raise RuntimeError("v5.1 wallet research lineage is not installed")
    inserted = bool(await _ORIGINAL_WALLET_RECORD(self, swap))
    _wallet_lineage_schema(self.store)
    with self.store._lock, self.store.db:
        row = self.store.db.execute(
            "SELECT * FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
            (str(getattr(swap, "signature", "") or ""),),
        ).fetchone()
        if row is None:
            return inserted
        item = dict(row)
        chase = item.get("chase_fraction")
        try:
            chase_value = float(chase) if chase is not None else None
        except (TypeError, ValueError):
            chase_value = None
        lag_ms = float(item.get("observation_lag_ms") or 0.0)
        price = item.get("copyable_price_sol")
        research_eligible = bool(
            price is not None
            and float(price) > 0.0
            and chase_value is not None
            and chase_value <= research_chase_observation_max()
            and lag_ms <= float(self.policy.max_observation_lag_seconds) * 1000.0
        )
        # `copyable` is legacy schema vocabulary. In v5.1 wallet discovery it now
        # means research-eligible under the 40% observation ceiling; final trading
        # authority remains exclusively in v5.1 strategy economics.
        self.store.db.execute(
            "UPDATE wallet_discovery_forward_observations SET copyable=? WHERE signature=?",
            (1 if research_eligible else 0, str(item["signature"])),
        )
        candidate = self.store.db.execute(
            "SELECT candidate_id FROM v51_candidates WHERE surface='SOLANA' AND candidate_id=? LIMIT 1",
            (str(item["signature"]),),
        ).fetchone()
        source_candidate_id = str(candidate["candidate_id"]) if candidate is not None else None
        release = current_release_commit()
        ensure_release_compatibility(self.store, release)
        self.store.db.execute(
            "INSERT OR IGNORE INTO v51_wallet_discovery_forward_lineage("
            "signature,wallet,token_mint,side,observed_at,received_at,release_commit,authority_id,economic_epoch,"
            "measurement_epoch,execution_model_epoch,source_candidate_id,chase_fraction,chase_band,research_eligible,"
            "observation_lag_ms,source,payload_json,recorded_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                str(item["signature"]),
                str(item["wallet"]),
                str(item["token_mint"]),
                str(item["side"]),
                str(item["observed_at"]),
                str(item["received_at"]),
                release,
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
                MEASUREMENT_EPOCH,
                EXECUTION_MODEL_EPOCH,
                source_candidate_id,
                chase_value,
                chase_band(chase_value),
                1 if research_eligible else 0,
                lag_ms,
                str(item.get("source") or ""),
                json.dumps(item, sort_keys=True, separators=(",", ":"), default=str),
                _utcnow(),
            ),
        )
    if inserted:
        try:
            self.store.append(
                "wallet_discovery_v51_measurement_classification",
                _utcnow(),
                {
                    "signature": str(getattr(swap, "signature", "") or ""),
                    "research_eligible": research_eligible,
                    "chase_band": chase_band(chase_value),
                    "research_chase_observation_max": research_chase_observation_max(),
                    "trading_authority": False,
                    "measurement_epoch": MEASUREMENT_EPOCH,
                },
            )
        except Exception:
            pass
    return inserted


setattr(_wallet_record_with_v51_research_lineage, "_roi_v51_measurement_integrity", True)


def _wallet_policy_v51() -> Any:
    if _ORIGINAL_WALLET_POLICY is None:
        raise RuntimeError("v5.1 wallet discovery policy is not installed")
    policy = _ORIGINAL_WALLET_POLICY()
    return replace(policy, max_chase_fraction=research_chase_observation_max())


setattr(_wallet_policy_v51, "_roi_v51_measurement_integrity", True)


def _shadow_init_v51(self: Any, *args: Any, **kwargs: Any) -> None:
    if _ORIGINAL_SHADOW_INIT is None:
        raise RuntimeError("v5.1 shadow quote observation policy is not installed")
    kwargs["max_chase_fraction"] = research_chase_observation_max()
    _ORIGINAL_SHADOW_INIT(self, *args, **kwargs)


setattr(_shadow_init_v51, "_roi_v51_measurement_integrity", True)


async def _candidate_delegate_with_ledger(self: Any, swap: Any) -> Any:
    if _ORIGINAL_CANDIDATE_DELEGATE is None:
        raise RuntimeError("v5.1 canonical candidate delegate is not installed")
    from .v51_candidate_ledger import record_solana_candidate

    plane = getattr(self, "_roi_candidate_execution_plane", None)
    store = getattr(plane, "store", None) or getattr(self, "store", None)
    if store is None:
        raise RuntimeError("canonical candidate ledger store unavailable")
    record_solana_candidate(store, swap, release_commit=current_release_commit())
    return await _ORIGINAL_CANDIDATE_DELEGATE(self, swap)


setattr(_candidate_delegate_with_ledger, "_roi_v51_measurement_integrity", True)


def _rh_cached_proof_with_measurement(store: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    if _ORIGINAL_RH_CACHED_PROOF is None:
        raise RuntimeError("v5.1 Robinhood proof measurement wrapper is not installed")
    proof = dict(_ORIGINAL_RH_CACHED_PROOF(store, *args, **kwargs))
    generated = str(proof.get("generated_at") or _utcnow())
    proof.setdefault("generated_at", generated)
    proof.setdefault("release_commit", current_release_commit())
    proof.setdefault("authority_id", AUTHORITY_ID)
    proof.setdefault("economic_freeze_epoch", ECONOMIC_FREEZE_EPOCH)
    proof.setdefault("measurement_epoch", MEASUREMENT_EPOCH)
    proof.setdefault("execution_model_epoch", EXECUTION_MODEL_EPOCH)
    proof.setdefault("measurement_fingerprint", measurement_fingerprint())
    proof.setdefault("execution_model_fingerprint", execution_model_fingerprint())
    proof.setdefault("evidence_through", evidence_watermark(store))
    proof.setdefault("proof_state", "confirmed")
    proof["proof_age_seconds"] = proof_age_seconds(proof)
    proof["proof_max_age_seconds"] = PROOF_MAX_AGE_SECONDS
    return proof


setattr(_rh_cached_proof_with_measurement, "_roi_v51_measurement_integrity", True)


def baseline_policy_leakage_audit() -> dict[str, Any]:
    return {
        "audit_version": "v51-baseline-policy-leakage-audit-v1",
        "wallet_discovery_research_chase_source": "strategy_v51_authority.execution.chase_observe_only_above_fraction",
        "wallet_discovery_research_chase_max": research_chase_observation_max(),
        "shadow_quote_observation_chase_source": "strategy_v51_authority.execution.chase_observe_only_above_fraction",
        "shadow_quote_observation_chase_max": research_chase_observation_max(),
        "baseline_strategy_version_surfaces": "lineage_and_legacy_forward-cohort compatibility only",
        "legacy_candidate_activation_gate": "retained as v3.1 certification substrate; v5.1 final economic authority remains separate and no longer has evidence censored by its 15% quote-acquisition ceiling",
        "economic_rules_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def install_measurement_integrity() -> None:
    """Install measurement integrity before the runtime object is lazily constructed.

    This changes upstream evidence acquisition/lineage only. It does not alter v5.1
    entry, sizing, promotion, kill or exit economics and introduces no signing or
    transaction-submission capability.
    """
    global _INSTALLED, _ORIGINAL_CANDIDATE_DELEGATE, _ORIGINAL_WALLET_POLICY
    global _ORIGINAL_WALLET_RECORD, _ORIGINAL_SHADOW_INIT, _ORIGINAL_RH_CACHED_PROOF
    if _INSTALLED:
        return

    from . import candidate_execution_evidence_plane as execution_plane
    from . import runtime
    from . import v51_robinhood_proof
    from .shadow_execution import ShadowWalletExecutableQuoteHandoff
    from .wallet_discovery import ContinuousWalletDiscovery

    current_delegate = execution_plane._ORIGINAL_SERVICE_INGEST
    if current_delegate is None:
        raise RuntimeError("candidate execution-evidence delegate unavailable for canonical ledger installation")
    if not bool(getattr(current_delegate, "_roi_v51_measurement_integrity", False)):
        _ORIGINAL_CANDIDATE_DELEGATE = current_delegate
        try:
            _candidate_delegate_with_ledger.__dict__.update(getattr(current_delegate, "__dict__", {}))
        except Exception:
            pass
        setattr(_candidate_delegate_with_ledger, "_roi_v51_measurement_integrity", True)
        execution_plane._ORIGINAL_SERVICE_INGEST = _candidate_delegate_with_ledger

    current_policy = runtime._wallet_discovery_policy
    if not bool(getattr(current_policy, "_roi_v51_measurement_integrity", False)):
        _ORIGINAL_WALLET_POLICY = current_policy
        runtime._wallet_discovery_policy = _wallet_policy_v51  # type: ignore[assignment]

    current_record = ContinuousWalletDiscovery._record_forward_swap
    if not bool(getattr(current_record, "_roi_v51_measurement_integrity", False)):
        _ORIGINAL_WALLET_RECORD = current_record
        try:
            _wallet_record_with_v51_research_lineage.__dict__.update(getattr(current_record, "__dict__", {}))
        except Exception:
            pass
        ContinuousWalletDiscovery._record_forward_swap = _wallet_record_with_v51_research_lineage  # type: ignore[method-assign]

    current_shadow_init = ShadowWalletExecutableQuoteHandoff.__init__
    if not bool(getattr(current_shadow_init, "_roi_v51_measurement_integrity", False)):
        _ORIGINAL_SHADOW_INIT = current_shadow_init
        ShadowWalletExecutableQuoteHandoff.__init__ = _shadow_init_v51  # type: ignore[method-assign]

    current_rh = v51_robinhood_proof.cached_robinhood_proof
    if not bool(getattr(current_rh, "_roi_v51_measurement_integrity", False)):
        _ORIGINAL_RH_CACHED_PROOF = current_rh
        v51_robinhood_proof.cached_robinhood_proof = _rh_cached_proof_with_measurement  # type: ignore[assignment]

    _INSTALLED = True


def status(store: Any | None = None) -> dict[str, Any]:
    payload = {
        "version": MEASUREMENT_INTEGRITY_VERSION,
        "installed": _INSTALLED,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "execution_model_epoch": EXECUTION_MODEL_EPOCH,
        "economic_fingerprint": authority_fingerprint(),
        "measurement_fingerprint": measurement_fingerprint(),
        "execution_model_fingerprint": execution_model_fingerprint(),
        "known_measurement_defective_releases": dict(KNOWN_MEASUREMENT_DEFECTS),
        "baseline_policy_leakage_audit": baseline_policy_leakage_audit(),
        "paper_only": True,
        "live_money_authority": False,
    }
    if store is not None:
        payload["current_release_compatibility"] = compatibility_status(store)
        payload.update(proof_metadata(store, proof_state="confirmed"))
    return payload


__all__ = [
    "EXECUTION_MODEL_EPOCH",
    "KNOWN_MEASUREMENT_DEFECTS",
    "MEASUREMENT_EPOCH",
    "MEASUREMENT_INTEGRITY_VERSION",
    "PROOF_MAX_AGE_SECONDS",
    "baseline_policy_leakage_audit",
    "cached_proof_state",
    "compatibility_status",
    "current_release_commit",
    "decorate_proof",
    "ensure_release_compatibility",
    "execution_model_fingerprint",
    "install_measurement_integrity",
    "measurement_fingerprint",
    "proof_age_seconds",
    "proof_metadata",
    "research_chase_observation_max",
    "status",
]
