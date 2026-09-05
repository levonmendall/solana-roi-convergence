from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import candidate_execution_evidence_plane as execution_plane
from . import direct_transaction as tx
from . import scout_candidate_continuity_repair as scout
from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import LAMPORTS_PER_SOL, NormalizedSwap
from .observation import WSOL_MINT


ARCHITECTURE_VERSION = "semantic-candidate-attribution-v1"
ATTRIBUTION_VERSION = "venue-native-wsol-directional-endpoint-v1"
IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED = 20.0
MAX_CHASE_FRACTION_UNCHANGED = 0.15
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_SUPPORTED_SEMANTIC_SOURCES = frozenset({"PUMP_FUN", "PUMP_AMM", "RAYDIUM"})
_ORIGINAL_SCOUT_NORMALIZER: Callable[..., tuple[NormalizedSwap | None, str | None]] | None = None
_ORIGINAL_INIT: Callable[..., None] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(plane: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_semantic_candidate_{name}"
    setattr(plane, attr, int(getattr(plane, attr, 0) or 0) + int(amount))


def _venue_from_swap(swap: NormalizedSwap) -> str:
    parts = str(swap.source or "").split(":")
    if len(parts) >= 3 and parts[0] == "solana-direct":
        return parts[1].upper()
    return "UNKNOWN"


def _deadline(observed_at: datetime) -> str:
    return (observed_at + timedelta(seconds=IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED)).isoformat()


def _ensure_schema(plane: Any) -> None:
    store = getattr(plane, "store", None)
    if store is None:
        service = getattr(plane, "service", None)
        store = getattr(service, "store", None)
    if store is None or not hasattr(store, "db") or not hasattr(store, "_lock"):
        raise RuntimeError("semantic candidate architecture requires canonical SQLite store")

    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS semantic_candidate_events ("
            "signature TEXT PRIMARY KEY, "
            "token_mint TEXT NOT NULL, "
            "venue TEXT NOT NULL, "
            "wallet TEXT NOT NULL, "
            "side TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, "
            "received_at TEXT NOT NULL, "
            "immediate_deadline TEXT NOT NULL, "
            "attribution_method TEXT NOT NULL, "
            "architecture_version TEXT NOT NULL"
            ")"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_semantic_candidate_events_mint_venue_time "
            "ON semantic_candidate_events(token_mint, venue, observed_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS semantic_candidate_opportunities ("
            "token_mint TEXT NOT NULL, "
            "venue TEXT NOT NULL, "
            "first_seen TEXT NOT NULL, "
            "last_seen TEXT NOT NULL, "
            "last_signature TEXT NOT NULL, "
            "last_wallet TEXT NOT NULL, "
            "last_side TEXT NOT NULL, "
            "signal_count INTEGER NOT NULL, "
            "latest_immediate_deadline TEXT NOT NULL, "
            "state TEXT NOT NULL DEFAULT 'watching', "
            "continuation_eligible INTEGER NOT NULL DEFAULT 1, "
            "entry_authority INTEGER NOT NULL DEFAULT 0, "
            "architecture_version TEXT NOT NULL, "
            "PRIMARY KEY(token_mint, venue)"
            ")"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS semantic_candidate_risk_state ("
            "token_mint TEXT NOT NULL, "
            "venue TEXT NOT NULL, "
            "assessed_at TEXT NOT NULL, "
            "complete INTEGER NOT NULL, "
            "fresh INTEGER NOT NULL, "
            "fresh_dimensions_json TEXT NOT NULL, "
            "entry_authority INTEGER NOT NULL DEFAULT 0, "
            "architecture_version TEXT NOT NULL, "
            "PRIMARY KEY(token_mint, venue)"
            ")"
        )


def _persist_opportunity(plane: Any, swap: NormalizedSwap) -> bool:
    """Persist a scout-proven market fact before it can enter the candidate plane.

    The ledger is evidence/watch state only. It deliberately has no entry authority.
    Every later proven scout signal advances the same mint+venue row and receives a
    fresh prospective twenty-second immediate-entry clock; it never backdates entry.
    """

    _ensure_schema(plane)
    store = getattr(plane, "store", None) or getattr(getattr(plane, "service", None), "store", None)
    venue = _venue_from_swap(swap)
    deadline = _deadline(swap.observed_at)
    inserted = False
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO semantic_candidate_events("
            "signature,token_mint,venue,wallet,side,observed_at,received_at,"
            "immediate_deadline,attribution_method,architecture_version"
            ") VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                swap.signature,
                swap.token_mint,
                venue,
                swap.wallet,
                swap.side,
                swap.observed_at.isoformat(),
                swap.received_at.isoformat(),
                deadline,
                ATTRIBUTION_VERSION,
                ARCHITECTURE_VERSION,
            ),
        )
        inserted = bool(cursor.rowcount)
        if inserted:
            store.db.execute(
                "INSERT INTO semantic_candidate_opportunities("
                "token_mint,venue,first_seen,last_seen,last_signature,last_wallet,last_side,"
                "signal_count,latest_immediate_deadline,state,continuation_eligible,"
                "entry_authority,architecture_version"
                ") VALUES(?,?,?,?,?,?,?,?,?,'watching',1,0,?) "
                "ON CONFLICT(token_mint,venue) DO UPDATE SET "
                "last_seen=excluded.last_seen,last_signature=excluded.last_signature,"
                "last_wallet=excluded.last_wallet,last_side=excluded.last_side,"
                "signal_count=semantic_candidate_opportunities.signal_count+1,"
                "latest_immediate_deadline=excluded.latest_immediate_deadline,"
                "state='watching',continuation_eligible=1,entry_authority=0,"
                "architecture_version=excluded.architecture_version",
                (
                    swap.token_mint,
                    venue,
                    swap.observed_at.isoformat(),
                    swap.observed_at.isoformat(),
                    swap.signature,
                    swap.wallet,
                    swap.side,
                    1,
                    deadline,
                    ARCHITECTURE_VERSION,
                ),
            )
    if inserted:
        _inc(plane, "durable_events")
    else:
        _inc(plane, "durable_duplicates")
    return inserted


def _persist_risk_readthrough(plane: Any, swap: NormalizedSwap) -> None:
    """Snapshot already-known risk readiness without invoking an external collector."""

    service = getattr(plane, "service", None)
    provider = getattr(service, "risk_provider", None)
    readiness_fn = getattr(provider, "readiness", None)
    if not callable(readiness_fn):
        _inc(plane, "risk_state_unavailable")
        return
    try:
        readiness = readiness_fn(swap.token_mint, as_of=swap.received_at)
    except Exception:
        _inc(plane, "risk_state_errors")
        return
    if not isinstance(readiness, dict):
        _inc(plane, "risk_state_unavailable")
        return

    _ensure_schema(plane)
    store = getattr(plane, "store", None) or getattr(service, "store", None)
    venue = _venue_from_swap(swap)
    fresh_dimensions = readiness.get("fresh_dimensions")
    if not isinstance(fresh_dimensions, dict):
        fresh_dimensions = {}
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO semantic_candidate_risk_state("
            "token_mint,venue,assessed_at,complete,fresh,fresh_dimensions_json,"
            "entry_authority,architecture_version"
            ") VALUES(?,?,?,?,?,?,0,?) "
            "ON CONFLICT(token_mint,venue) DO UPDATE SET "
            "assessed_at=excluded.assessed_at,complete=excluded.complete,fresh=excluded.fresh,"
            "fresh_dimensions_json=excluded.fresh_dimensions_json,entry_authority=0,"
            "architecture_version=excluded.architecture_version",
            (
                swap.token_mint,
                venue,
                swap.received_at.isoformat(),
                int(bool(readiness.get("complete"))),
                int(bool(readiness.get("fresh"))),
                json.dumps(fresh_dimensions, sort_keys=True, separators=(",", ":")),
                ARCHITECTURE_VERSION,
            ),
        )
    _inc(plane, "risk_state_snapshots")


def _native_wsol_flow(
    result: dict[str, Any],
    *,
    wallet: str,
    wallet_index: int,
    deltas: dict[str, float],
) -> float | None:
    meta = result.get("meta")
    if not isinstance(meta, dict):
        return None
    pre_balances = meta.get("preBalances") or []
    post_balances = meta.get("postBalances") or []
    if wallet_index >= len(pre_balances) or wallet_index >= len(post_balances):
        return None
    try:
        native_change_sol = (
            float(post_balances[wallet_index]) - float(pre_balances[wallet_index])
        ) / LAMPORTS_PER_SOL
    except (TypeError, ValueError, IndexError):
        return None

    fee_payer = tx._fee_payer(result)
    if fee_payer == wallet:
        try:
            native_change_sol += float(meta.get("fee") or 0.0) / LAMPORTS_PER_SOL
        except (TypeError, ValueError):
            return None
    return native_change_sol + float(deltas.get(WSOL_MINT, 0.0) or 0.0)


def _directional_endpoint(
    deltas: dict[str, float],
    *,
    side: str,
) -> tuple[tuple[str, float] | None, str | None]:
    material = [
        (mint, float(delta))
        for mint, delta in deltas.items()
        if mint != WSOL_MINT and abs(float(delta)) > 1e-18
    ]
    if side == "buy":
        endpoints = [(mint, delta) for mint, delta in material if delta > 1e-18]
    else:
        endpoints = [(mint, delta) for mint, delta in material if delta < -1e-18]
    if not endpoints:
        return None, "semantic_directional_endpoint_missing"
    if len(endpoints) > 1:
        return None, "semantic_multiple_directional_endpoints"
    return endpoints[0], None


def _decode_supported_venue(
    result: dict[str, Any],
    *,
    wallet: str,
    received_at: datetime,
    source_hint: str | None,
    source: str,
) -> tuple[NormalizedSwap | None, str | None]:
    """Decode a supported venue from transaction facts before scout association.

    Pump.fun, Pump AMM and Raydium use the same canonical native/WSOL settlement
    contract here, but dispatch stays venue-explicit so venue-specific instruction
    semantics can evolve without changing the downstream candidate contract.
    """

    if source not in _SUPPORTED_SEMANTIC_SOURCES:
        return None, "semantic_unsupported_venue"
    meta = result.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None, "transaction_failed_or_meta_missing"
    keys = tx._account_keys(result)
    wallet_index = scout._wallet_account_index(keys, wallet)
    if wallet_index is None:
        return None, "tracked_scout_not_in_transaction_accounts"

    deltas = tx._token_deltas_for_owner(result, wallet)
    net_native = _native_wsol_flow(
        result,
        wallet=wallet,
        wallet_index=wallet_index,
        deltas=deltas,
    )
    if net_native is None or abs(net_native) <= 1e-18:
        return None, "semantic_native_wsol_direction_ambiguous"

    side = "buy" if net_native < 0 else "sell"
    endpoint, endpoint_error = _directional_endpoint(deltas, side=side)
    if endpoint is None:
        return None, endpoint_error
    token_mint, token_delta = endpoint
    token_amount = abs(float(token_delta))
    native_amount_sol = abs(float(net_native))
    if token_amount <= 0.0 or native_amount_sol <= 0.0:
        return None, "semantic_amount_nonpositive"

    signature = tx._signature(result)
    slot = result.get("slot")
    block_time = result.get("blockTime")
    if not signature or slot is None or block_time is None:
        return None, "transaction_identity_or_time_missing"
    try:
        observed_at = datetime.fromtimestamp(float(block_time), tz=timezone.utc)
        slot_int = int(slot)
    except (TypeError, ValueError, OSError):
        return None, "transaction_identity_or_time_invalid"

    return (
        NormalizedSwap(
            signature=signature,
            slot=slot_int,
            observed_at=observed_at,
            received_at=received_at,
            wallet=wallet,
            token_mint=token_mint,
            side=side,
            token_amount=token_amount,
            native_amount_sol=native_amount_sol,
            reference_price_sol=native_amount_sol / token_amount,
            ingestion_latency_ms=max(0.0, (received_at - observed_at).total_seconds() * 1000.0),
            source=f"solana-direct:{source}:{side}",
        ),
        None,
    )


def _semantic_normalize_tracked_wallet(
    result: dict[str, Any],
    *,
    wallet: str,
    received_at: datetime,
    source_hint: str | None,
) -> tuple[NormalizedSwap | None, str | None]:
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is not None:
        _inc(plane, "attempts")

    source, source_error = scout._source_for_transaction(result, source_hint)
    if source is None:
        if plane is not None:
            _inc(plane, "source_failures")
        return None, source_error

    swap, error = _decode_supported_venue(
        result,
        wallet=wallet,
        received_at=received_at,
        source_hint=source_hint,
        source=source,
    )
    if swap is None:
        if plane is not None:
            _inc(plane, "semantic_failures")
            if error == "semantic_multiple_directional_endpoints":
                _inc(plane, "multiple_directional_endpoints")
            elif error == "semantic_directional_endpoint_missing":
                _inc(plane, "directional_endpoint_missing")
            elif error == "semantic_native_wsol_direction_ambiguous":
                _inc(plane, "native_direction_ambiguous")
        return None, error

    if plane is not None:
        _inc(plane, "supported_swaps_decoded")
        try:
            _persist_opportunity(plane, swap)
        except Exception:
            _inc(plane, "ledger_persistence_errors")
            return None, "semantic_candidate_ledger_persist_failed"
        _persist_risk_readthrough(plane, swap)
        _inc(plane, "scout_associated")
        _inc(plane, "endpoint_resolved")
    return swap, None


setattr(_semantic_normalize_tracked_wallet, "_roi_semantic_candidate_attribution", True)


def _semantic_init(self: DirectSolanaIngestionPlane, *args: Any, **kwargs: Any) -> None:
    if _ORIGINAL_INIT is None:
        raise RuntimeError("semantic candidate attribution architecture is not installed")
    _ORIGINAL_INIT(self, *args, **kwargs)
    _ensure_schema(self)


setattr(_semantic_init, "_roi_semantic_candidate_attribution", True)


def _table_counts(plane: Any) -> dict[str, int]:
    store = getattr(plane, "store", None) or getattr(getattr(plane, "service", None), "store", None)
    if store is None:
        return {
            "durable_events": 0,
            "durable_opportunities": 0,
            "continuation_eligible": 0,
            "risk_state_rows": 0,
            "risk_state_complete_fresh": 0,
        }
    try:
        _ensure_schema(plane)
        with store._lock:
            events = int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_events").fetchone()[0])
            opportunities = int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_opportunities").fetchone()[0])
            continuation = int(
                store.db.execute(
                    "SELECT COUNT(*) FROM semantic_candidate_opportunities WHERE continuation_eligible=1"
                ).fetchone()[0]
            )
            risk_rows = int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_risk_state").fetchone()[0])
            risk_ready = int(
                store.db.execute(
                    "SELECT COUNT(*) FROM semantic_candidate_risk_state WHERE complete=1 AND fresh=1"
                ).fetchone()[0]
            )
        return {
            "durable_events": events,
            "durable_opportunities": opportunities,
            "continuation_eligible": continuation,
            "risk_state_rows": risk_rows,
            "risk_state_complete_fresh": risk_ready,
        }
    except Exception:
        return {
            "durable_events": 0,
            "durable_opportunities": 0,
            "continuation_eligible": 0,
            "risk_state_rows": 0,
            "risk_state_complete_fresh": 0,
        }


def _semantic_status(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("semantic candidate attribution architecture is not installed")
    payload = _ORIGINAL_STATUS(self)
    counts = _table_counts(self)
    payload["semantic_candidate_attribution_architecture"] = {
        "installed": True,
        "version": ARCHITECTURE_VERSION,
        "attribution_version": ATTRIBUTION_VERSION,
        "architecture": (
            "transaction-facts->venue-proof->directional-swap-endpoint->exact-scout-association->"
            "durable-mint-venue-ledger->risk-state-readthrough->candidate-execution"
        ),
        "transaction_semantics_before_candidate_decision": True,
        "venue_specific_decoder_boundary": True,
        "supported_venues": sorted(_SUPPORTED_SEMANTIC_SOURCES),
        "exact_scout_association_after_swap_direction": True,
        "owner_token_delta_is_not_single_mint_authority": True,
        "multiple_same_direction_endpoints_fail_closed": True,
        "attempts_session": int(getattr(self, "_roi_semantic_candidate_attempts", 0) or 0),
        "supported_swaps_decoded_session": int(
            getattr(self, "_roi_semantic_candidate_supported_swaps_decoded", 0) or 0
        ),
        "scout_associated_session": int(getattr(self, "_roi_semantic_candidate_scout_associated", 0) or 0),
        "endpoint_resolved_session": int(getattr(self, "_roi_semantic_candidate_endpoint_resolved", 0) or 0),
        "semantic_failures_session": int(getattr(self, "_roi_semantic_candidate_semantic_failures", 0) or 0),
        "multiple_directional_endpoints_session": int(
            getattr(self, "_roi_semantic_candidate_multiple_directional_endpoints", 0) or 0
        ),
        "native_direction_ambiguous_session": int(
            getattr(self, "_roi_semantic_candidate_native_direction_ambiguous", 0) or 0
        ),
        "ledger_persistence_errors_session": int(
            getattr(self, "_roi_semantic_candidate_ledger_persistence_errors", 0) or 0
        ),
        "risk_state_snapshots_session": int(
            getattr(self, "_roi_semantic_candidate_risk_state_snapshots", 0) or 0
        ),
        **counts,
        "durable_opportunity_key": "token_mint+venue",
        "durable_ledger_has_entry_authority": False,
        "continuation_watch_has_entry_authority": False,
        "later_signal_gets_fresh_prospective_clock": True,
        "immediate_entry_window_seconds_unchanged": IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED,
        "candidate_processing_target_seconds_unchanged": 5.0,
        "max_chase_fraction_unchanged": MAX_CHASE_FRACTION_UNCHANGED,
        "risk_state_readthrough_only_on_attribution_path": True,
        "authoritative_six_dimension_refresh_remains_candidate_execution_plane": True,
        "certification_thresholds_changed": False,
        "full_market_observation_reduced": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "semantic_candidate_attribution": True,
                "candidate_attribution_uses_transaction_semantics_first": True,
                "durable_mint_venue_opportunity_ledger": True,
                "opportunity_ledger_has_entry_authority": False,
                "later_activity_re_evaluation_preserved": True,
                "risk_readthrough_does_not_replace_authoritative_refresh": True,
                "candidate_entry_window_unchanged": True,
                "max_chase_unchanged": True,
                "full_raw_market_scope_preserved": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_semantic_status, "_roi_semantic_candidate_attribution", True)


def install_semantic_candidate_attribution_architecture() -> None:
    """Install the post-compose candidate attribution architecture exactly once."""

    global _ORIGINAL_SCOUT_NORMALIZER, _ORIGINAL_INIT, _ORIGINAL_STATUS

    # The semantic layer is intentionally post-compose. Make its prerequisites
    # explicit for focused tests while remaining idempotent in production.
    if execution_plane._ORIGINAL_SERVICE_INGEST is None:
        execution_plane.install_candidate_execution_evidence_plane()
    if not bool(getattr(DirectSolanaIngestionPlane.status, "_roi_scout_candidate_continuity", False)):
        scout.install_scout_candidate_continuity_repair()

    current_normalizer = scout._normalize_tracked_wallet
    if not bool(getattr(current_normalizer, "_roi_semantic_candidate_attribution", False)):
        _ORIGINAL_SCOUT_NORMALIZER = current_normalizer
        scout._normalize_tracked_wallet = _semantic_normalize_tracked_wallet  # type: ignore[assignment]

    current_init = DirectSolanaIngestionPlane.__init__
    if not bool(getattr(current_init, "_roi_semantic_candidate_attribution", False)):
        _ORIGINAL_INIT = current_init
        try:
            _semantic_init.__dict__.update(getattr(current_init, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.__init__ = _semantic_init  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_semantic_candidate_attribution", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _semantic_status.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _semantic_status  # type: ignore[method-assign]


__all__ = [
    "ARCHITECTURE_VERSION",
    "ATTRIBUTION_VERSION",
    "IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED",
    "MAX_CHASE_FRACTION_UNCHANGED",
    "_decode_supported_venue",
    "_directional_endpoint",
    "_persist_opportunity",
    "_semantic_normalize_tracked_wallet",
    "install_semantic_candidate_attribution_architecture",
]
