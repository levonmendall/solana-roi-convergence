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
_SUPPORTED_SOURCES = frozenset({"PUMP_FUN", "PUMP_AMM", "RAYDIUM"})

_ORIGINAL_SCOUT_NORMALIZER: Callable[..., tuple[NormalizedSwap | None, str | None]] | None = None
_ORIGINAL_INIT: Callable[..., None] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _inc(plane: Any, name: str) -> None:
    attr = f"_roi_semantic_candidate_{name}"
    setattr(plane, attr, int(getattr(plane, attr, 0) or 0) + 1)


def _store(plane: Any) -> Any:
    store = getattr(plane, "store", None)
    if store is None:
        store = getattr(getattr(plane, "service", None), "store", None)
    if store is None or not hasattr(store, "db") or not hasattr(store, "_lock"):
        raise RuntimeError("semantic candidate architecture requires canonical SQLite store")
    return store


def _venue(swap: NormalizedSwap) -> str:
    parts = str(swap.source or "").split(":")
    return parts[1].upper() if len(parts) >= 3 and parts[0] == "solana-direct" else "UNKNOWN"


def _ensure_schema(plane: Any) -> None:
    store = _store(plane)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS semantic_candidate_events ("
            "signature TEXT PRIMARY KEY, token_mint TEXT NOT NULL, venue TEXT NOT NULL, "
            "wallet TEXT NOT NULL, side TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "received_at TEXT NOT NULL, immediate_deadline TEXT NOT NULL, "
            "attribution_method TEXT NOT NULL, architecture_version TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_semantic_candidate_events_mint_venue_time "
            "ON semantic_candidate_events(token_mint, venue, observed_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS semantic_candidate_opportunities ("
            "token_mint TEXT NOT NULL, venue TEXT NOT NULL, first_seen TEXT NOT NULL, "
            "last_seen TEXT NOT NULL, last_signature TEXT NOT NULL, last_wallet TEXT NOT NULL, "
            "last_side TEXT NOT NULL, signal_count INTEGER NOT NULL, "
            "latest_immediate_deadline TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'watching', "
            "continuation_eligible INTEGER NOT NULL DEFAULT 1, "
            "entry_authority INTEGER NOT NULL DEFAULT 0, architecture_version TEXT NOT NULL, "
            "PRIMARY KEY(token_mint, venue))"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS semantic_candidate_risk_state ("
            "token_mint TEXT NOT NULL, venue TEXT NOT NULL, assessed_at TEXT NOT NULL, "
            "complete INTEGER NOT NULL, fresh INTEGER NOT NULL, fresh_dimensions_json TEXT NOT NULL, "
            "entry_authority INTEGER NOT NULL DEFAULT 0, architecture_version TEXT NOT NULL, "
            "PRIMARY KEY(token_mint, venue))"
        )


def _persist_opportunity(plane: Any, swap: NormalizedSwap) -> bool:
    """Persist watch state only; this ledger can never authorize an entry."""
    _ensure_schema(plane)
    store = _store(plane)
    venue = _venue(swap)
    deadline = (
        swap.observed_at + timedelta(seconds=IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED)
    ).isoformat()
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO semantic_candidate_events("
            "signature,token_mint,venue,wallet,side,observed_at,received_at,immediate_deadline,"
            "attribution_method,architecture_version) VALUES(?,?,?,?,?,?,?,?,?,?)",
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
                "signal_count,latest_immediate_deadline,state,continuation_eligible,entry_authority,"
                "architecture_version) VALUES(?,?,?,?,?,?,?,?,?,'watching',1,0,?) "
                "ON CONFLICT(token_mint,venue) DO UPDATE SET "
                "last_seen=excluded.last_seen,last_signature=excluded.last_signature,"
                "last_wallet=excluded.last_wallet,last_side=excluded.last_side,"
                "signal_count=semantic_candidate_opportunities.signal_count+1,"
                "latest_immediate_deadline=excluded.latest_immediate_deadline,state='watching',"
                "continuation_eligible=1,entry_authority=0,architecture_version=excluded.architecture_version",
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
    _inc(plane, "durable_events" if inserted else "durable_duplicates")
    return inserted


def _persist_risk_readthrough(plane: Any, swap: NormalizedSwap) -> None:
    """Persist existing readiness only; never invoke risk collectors here."""
    service = getattr(plane, "service", None)
    readiness_fn = getattr(getattr(service, "risk_provider", None), "readiness", None)
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
    fresh_dimensions = readiness.get("fresh_dimensions")
    if not isinstance(fresh_dimensions, dict):
        fresh_dimensions = {}
    _ensure_schema(plane)
    store = _store(plane)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO semantic_candidate_risk_state("
            "token_mint,venue,assessed_at,complete,fresh,fresh_dimensions_json,entry_authority,"
            "architecture_version) VALUES(?,?,?,?,?,?,0,?) "
            "ON CONFLICT(token_mint,venue) DO UPDATE SET assessed_at=excluded.assessed_at,"
            "complete=excluded.complete,fresh=excluded.fresh,"
            "fresh_dimensions_json=excluded.fresh_dimensions_json,entry_authority=0,"
            "architecture_version=excluded.architecture_version",
            (
                swap.token_mint,
                _venue(swap),
                swap.received_at.isoformat(),
                int(bool(readiness.get("complete"))),
                int(bool(readiness.get("fresh"))),
                json.dumps(fresh_dimensions, sort_keys=True, separators=(",", ":")),
                ARCHITECTURE_VERSION,
            ),
        )
    _inc(plane, "risk_state_snapshots")


def _net_native_wsol_flow(
    result: dict[str, Any], *, wallet: str, wallet_index: int, deltas: dict[str, float]
) -> float | None:
    meta = result.get("meta")
    if not isinstance(meta, dict):
        return None
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    try:
        net = (int(post[wallet_index]) - int(pre[wallet_index])) / LAMPORTS_PER_SOL
    except (IndexError, TypeError, ValueError):
        return None
    payer = tx._fee_payer(result)
    if payer is not None and str(payer[0]) == wallet and int(payer[1]) == int(wallet_index):
        try:
            net += int(meta.get("fee") or 0) / LAMPORTS_PER_SOL
        except (TypeError, ValueError):
            return None
    return net + float(deltas.get(WSOL_MINT, 0.0) or 0.0)


def _directional_endpoint(
    deltas: dict[str, float], *, side: str
) -> tuple[tuple[str, float] | None, str | None]:
    material = [
        (mint, float(delta))
        for mint, delta in deltas.items()
        if mint != WSOL_MINT and abs(float(delta)) > 1e-18
    ]
    endpoints = (
        [(mint, delta) for mint, delta in material if delta > 1e-18]
        if side == "buy"
        else [(mint, delta) for mint, delta in material if delta < -1e-18]
    )
    if not endpoints:
        return None, "semantic_directional_endpoint_missing"
    if len(endpoints) > 1:
        return None, "semantic_multiple_directional_endpoints"
    return endpoints[0], None


def _decode_supported_venue(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    wallet: str,
    source: str,
) -> tuple[NormalizedSwap | None, str | None]:
    """Resolve venue + direction + traded endpoint before creating a candidate fact."""
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    if source not in _SUPPORTED_SOURCES:
        return None, "semantic_unsupported_venue"
    meta = result.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None, "transaction_failed_or_meta_missing"
    wallet_index = scout._wallet_account_index(result, wallet)
    if wallet_index is None:
        return None, "tracked_scout_account_index_missing"

    deltas = tx._token_deltas_for_owner(result, wallet)
    net_native = _net_native_wsol_flow(
        result, wallet=wallet, wallet_index=wallet_index, deltas=deltas
    )
    if net_native is None or abs(net_native) <= 1e-18:
        return None, "semantic_native_wsol_direction_ambiguous"
    side = "buy" if net_native < 0 else "sell"
    endpoint, endpoint_error = _directional_endpoint(deltas, side=side)
    if endpoint is None:
        return None, endpoint_error
    token_mint, token_delta = endpoint
    token_amount = abs(float(token_delta))
    native_amount = abs(float(net_native))
    if token_amount <= 0.0 or native_amount <= 0.0:
        return None, "semantic_amount_nonpositive"
    try:
        slot = int(result["slot"])
    except (KeyError, TypeError, ValueError):
        return None, "slot_missing"
    try:
        block_time = int(result.get("blockTime") or 0)
    except (TypeError, ValueError):
        block_time = 0
    observed_at = (
        datetime.fromtimestamp(block_time, tz=timezone.utc)
        if block_time > 0
        else trigger_received_at
    )
    return (
        NormalizedSwap(
            signature=signature,
            slot=slot,
            observed_at=observed_at,
            received_at=trigger_received_at,
            wallet=wallet,
            token_mint=str(token_mint),
            side=side,
            token_amount=token_amount,
            native_amount_sol=native_amount,
            reference_price_sol=native_amount / token_amount,
            source=f"solana-direct:{source}:{side}",
        ),
        None,
    )


def _semantic_normalize_tracked_wallet(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    wallet: str,
    source_hint: str | None = None,
) -> tuple[NormalizedSwap | None, str | None]:
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is not None:
        _inc(plane, "attempts")
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    source, source_error = scout._source_for_transaction(result, source_hint)
    if source is None:
        if plane is not None:
            _inc(plane, "source_failures")
        return None, source_error
    swap, error = _decode_supported_venue(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        wallet=wallet,
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


def _counts(plane: Any) -> dict[str, int]:
    try:
        _ensure_schema(plane)
        store = _store(plane)
        with store._lock:
            return {
                "durable_events": int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_events").fetchone()[0]),
                "durable_opportunities": int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_opportunities").fetchone()[0]),
                "continuation_eligible": int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_opportunities WHERE continuation_eligible=1").fetchone()[0]),
                "risk_state_rows": int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_risk_state").fetchone()[0]),
                "risk_state_complete_fresh": int(store.db.execute("SELECT COUNT(*) FROM semantic_candidate_risk_state WHERE complete=1 AND fresh=1").fetchone()[0]),
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
    payload["semantic_candidate_attribution_architecture"] = {
        "installed": True,
        "version": ARCHITECTURE_VERSION,
        "attribution_version": ATTRIBUTION_VERSION,
        "architecture": "transaction-facts->venue-proof->directional-endpoint->exact-scout->durable-mint-venue-ledger->risk-readthrough->candidate-execution",
        "transaction_semantics_before_candidate_decision": True,
        "venue_specific_decoder_boundary": True,
        "supported_venues": sorted(_SUPPORTED_SOURCES),
        "owner_token_delta_is_not_single_mint_authority": True,
        "multiple_same_direction_endpoints_fail_closed": True,
        "attempts_session": int(getattr(self, "_roi_semantic_candidate_attempts", 0) or 0),
        "supported_swaps_decoded_session": int(getattr(self, "_roi_semantic_candidate_supported_swaps_decoded", 0) or 0),
        "scout_associated_session": int(getattr(self, "_roi_semantic_candidate_scout_associated", 0) or 0),
        "semantic_failures_session": int(getattr(self, "_roi_semantic_candidate_semantic_failures", 0) or 0),
        "multiple_directional_endpoints_session": int(getattr(self, "_roi_semantic_candidate_multiple_directional_endpoints", 0) or 0),
        "ledger_persistence_errors_session": int(getattr(self, "_roi_semantic_candidate_ledger_persistence_errors", 0) or 0),
        "risk_state_snapshots_session": int(getattr(self, "_roi_semantic_candidate_risk_state_snapshots", 0) or 0),
        **_counts(self),
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
    global _ORIGINAL_SCOUT_NORMALIZER, _ORIGINAL_INIT, _ORIGINAL_STATUS
    if bool(getattr(DirectSolanaIngestionPlane.status, "_roi_semantic_candidate_attribution", False)):
        return
    if execution_plane._ORIGINAL_SERVICE_INGEST is None:
        execution_plane.install_candidate_execution_evidence_plane()
    if not bool(getattr(DirectSolanaIngestionPlane.status, "_roi_scout_candidate_continuity", False)):
        scout.install_scout_candidate_continuity_repair()

    current_normalizer = scout._normalize_tracked_wallet
    if not bool(getattr(current_normalizer, "_roi_semantic_candidate_attribution", False)):
        _ORIGINAL_SCOUT_NORMALIZER = current_normalizer
        scout._normalize_tracked_wallet = _semantic_normalize_tracked_wallet  # type: ignore[assignment]

    current_init = DirectSolanaIngestionPlane.__init__
    _ORIGINAL_INIT = current_init
    try:
        _semantic_init.__dict__.update(getattr(current_init, "__dict__", {}))
    except Exception:
        pass
    DirectSolanaIngestionPlane.__init__ = _semantic_init  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
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
    "_persist_risk_readthrough",
    "_semantic_normalize_tracked_wallet",
    "install_semantic_candidate_attribution_architecture",
]
