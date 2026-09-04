from __future__ import annotations

import asyncio
import contextvars
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from . import candidate_execution_evidence_plane as execution_plane
from . import candidate_risk_quote_v4_handoff as candidate_v4
from . import continuity_exact_durable_signature_repair as exact
from . import continuity_high_volume_poll_affinity_repair as affinity
from . import continuity_storage_capacity_repair as storage
from . import direct_solana as direct_module
from . import direct_transaction as tx
from . import post104_production_architecture_repair as post104
from . import production_capacity_repair as capacity
from . import raw_receipt_dispatch_repair as raw_dispatch
from .direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal, WatchTarget
from .ingestion import LAMPORTS_PER_SOL, IngestionDecision, NormalizedSwap
from .observation import WSOL_MINT
from .source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


REPAIR_VERSION = "scout-candidate-continuity-v1"
SCOUT_REASONS = candidate_v4.SCOUT_REASONS
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
RECOVERABILITY_LEASE_SECONDS_UNCHANGED = 12.0
HARD_RECOVERY_BOUND_UNCHANGED = "3x1000"
CANDIDATE_PROCESSING_TARGET_SECONDS_UNCHANGED = 5.0
CANDIDATE_ENTRY_WINDOW_SECONDS_UNCHANGED = 20.0
MAX_CHASE_FRACTION_UNCHANGED = 0.15

_SCOUT_HYDRATION_PLANE: contextvars.ContextVar[DirectSolanaIngestionPlane | None] = contextvars.ContextVar(
    "roi_scout_hydration_plane",
    default=None,
)

_ORIGINAL_HYDRATE: Callable[..., Any] | None = None
_ORIGINAL_NORMALIZE: Callable[..., NormalizedSwap | None] | None = None
_ORIGINAL_HANDOFF_SCHEDULE: Callable[[Any, str], None] | None = None
_ORIGINAL_EXECUTION_INGEST: Callable[..., Any] | None = None
_ORIGINAL_RECORD_RECEIPT: Callable[..., bool] | None = None
_ORIGINAL_EXACT_SOURCE_KEY: Callable[[WatchTarget], str | None] | None = None
_ORIGINAL_BURST_PREDICATE: Callable[[WatchTarget], bool] | None = None
_ORIGINAL_ASSIGNED_ENDPOINT: Callable[..., Any] | None = None
_ORIGINAL_BATCH_ROWS: Callable[[list[Any]], list[dict[str, Any]]] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_scout_candidate_continuity_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _normalization_failures(obj: Any) -> Counter[str]:
    value = getattr(obj, "_roi_scout_candidate_continuity_normalization_failures", None)
    if isinstance(value, Counter):
        return value
    value = Counter()
    setattr(obj, "_roi_scout_candidate_continuity_normalization_failures", value)
    return value


def _account_entries(result: Any) -> list[tuple[str, bool, int]]:
    if not isinstance(result, dict):
        return []
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    rows = message.get("accountKeys") if isinstance(message, dict) else None
    if not isinstance(rows, list):
        return []
    entries: list[tuple[str, bool, int]] = []
    for index, row in enumerate(rows):
        if isinstance(row, str):
            pubkey = row
            signer = False
        elif isinstance(row, dict):
            pubkey = str(row.get("pubkey") or "")
            signer = bool(row.get("signer"))
        else:
            continue
        if pubkey:
            entries.append((pubkey, signer, index))
    return entries


def _tracked_scout_wallet(result: Any, scouts: tuple[str, ...] | list[str] | set[str]) -> tuple[str | None, str | None]:
    configured = {str(value) for value in scouts if str(value)}
    entries = [entry for entry in _account_entries(result) if entry[0] in configured]
    signer_matches = list(dict.fromkeys(pubkey for pubkey, signer, _index in entries if signer))
    if len(signer_matches) == 1:
        return signer_matches[0], None
    if len(signer_matches) > 1:
        return None, "multiple_tracked_scout_signers"
    account_matches = list(dict.fromkeys(pubkey for pubkey, _signer, _index in entries))
    if len(account_matches) == 1:
        return account_matches[0], None
    if len(account_matches) > 1:
        return None, "multiple_tracked_scout_accounts"
    return None, "tracked_scout_not_present_in_transaction"


def _wallet_account_index(result: Any, wallet: str) -> int | None:
    for pubkey, _signer, index in _account_entries(result):
        if pubkey == wallet:
            return index
    return None


def _source_for_transaction(result: dict[str, Any], source_hint: str | None) -> tuple[str | None, str | None]:
    supported = {source for source, _ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE}
    sources = tx.transaction_sources(result)
    hint = str(source_hint or "").upper()
    if hint:
        if hint not in supported:
            return None, "unsupported_source_hint"
        if sources and hint not in sources:
            return None, "source_hint_not_present"
        return hint, None
    if len(sources) == 1:
        return next(iter(sources)), None
    if not sources:
        return None, "supported_swap_source_missing"
    return None, "multiple_supported_swap_sources"


def _normalize_tracked_wallet(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    wallet: str,
    source_hint: str | None = None,
) -> tuple[NormalizedSwap | None, str | None]:
    """Normalize the exact tracked scout rather than assuming the fee payer is the trader.

    Standard Solana RPC already exposes owner-scoped token balance changes. The
    previous parser chose the fee payer first, which is correct for simple direct
    swaps but fails for routed/relayed scout transactions where a different signer
    or sponsor pays the transaction fee. This parser remains fail-closed: one
    tracked scout, one supported venue source and one non-WSOL material mint are
    still required.
    """

    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    meta = result.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None, "transaction_failed_or_meta_missing"

    wallet_index = _wallet_account_index(result, wallet)
    if wallet_index is None:
        return None, "tracked_scout_account_index_missing"
    source, source_error = _source_for_transaction(result, source_hint)
    if source is None:
        return None, source_error or "source_unresolved"

    deltas = tx._token_deltas_for_owner(result, wallet)
    wsol_delta = float(deltas.pop(WSOL_MINT, 0.0))
    material = [(mint, delta) for mint, delta in deltas.items() if abs(float(delta)) > 1e-18]
    if len(material) != 1:
        return None, "tracked_scout_token_delta_ambiguous"
    token_mint, token_delta = material[0]

    pre_balances = meta.get("preBalances")
    post_balances = meta.get("postBalances")
    try:
        pre_lamports = int(pre_balances[wallet_index])
        post_lamports = int(post_balances[wallet_index])
    except (IndexError, TypeError, ValueError):
        return None, "tracked_scout_native_balance_missing"

    fee_adjustment = 0
    payer = tx._fee_payer(result)
    if payer is not None and str(payer[0]) == wallet and int(payer[1]) == int(wallet_index):
        try:
            fee_adjustment = int(meta.get("fee") or 0)
        except (TypeError, ValueError):
            fee_adjustment = 0

    native_change_sol = (
        (post_lamports - pre_lamports + fee_adjustment) / LAMPORTS_PER_SOL
        + wsol_delta
    )
    if token_delta > 0 and native_change_sol < 0:
        side = "buy"
    elif token_delta < 0 and native_change_sol > 0:
        side = "sell"
    else:
        return None, "tracked_scout_native_token_direction_ambiguous"

    token_amount = abs(float(token_delta))
    native_amount = abs(float(native_change_sol))
    if token_amount <= 0.0 or native_amount <= 0.0:
        return None, "tracked_scout_zero_amount"
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


def _normalize_with_exact_scout_identity(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    source_hint: str | None = None,
) -> NormalizedSwap | None:
    if _ORIGINAL_NORMALIZE is None:
        raise RuntimeError("scout candidate normalization repair is not installed")
    plane = _SCOUT_HYDRATION_PLANE.get()
    # Candidate launch-context prefill happens inside the same hydrate call but
    # supplies an explicit program source. Keep those unrelated context rows on the
    # canonical parser and apply tracked-wallet identity only to the scout trigger.
    if plane is None or source_hint is not None:
        return _ORIGINAL_NORMALIZE(
            result,
            signature=signature,
            trigger_received_at=trigger_received_at,
            source_hint=source_hint,
        )

    _inc(plane, "normalization_attempts")
    wallet, wallet_error = _tracked_scout_wallet(result, tuple(getattr(plane, "scout_wallets", ()) or ()))
    if wallet is None:
        reason = wallet_error or "tracked_scout_unresolved"
        _normalization_failures(plane)[reason] += 1
        _inc(plane, "normalization_failed")
        return None
    swap, error = _normalize_tracked_wallet(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        wallet=wallet,
        source_hint=None,
    )
    if swap is None:
        _normalization_failures(plane)[error or "unknown"] += 1
        _inc(plane, "normalization_failed")
        return None
    _inc(plane, "normalization_complete")
    return swap


setattr(_normalize_with_exact_scout_identity, "_roi_scout_candidate_continuity", True)


async def _hydrate_with_scout_identity_context(
    self: DirectSolanaIngestionPlane,
    row: dict[str, Any],
) -> None:
    if _ORIGINAL_HYDRATE is None:
        raise RuntimeError("scout hydration context repair is not installed")
    reason = str(row.get("reason") or "")
    if reason not in SCOUT_REASONS:
        await _ORIGINAL_HYDRATE(self, row)
        return
    token = _SCOUT_HYDRATION_PLANE.set(self)
    try:
        await _ORIGINAL_HYDRATE(self, row)
    finally:
        _SCOUT_HYDRATION_PLANE.reset(token)


setattr(_hydrate_with_scout_identity_context, "_roi_scout_candidate_continuity", True)


def _defer_pre_persistence_v4_handoff(obj: Any, signature: str) -> None:
    if _ORIGINAL_HANDOFF_SCHEDULE is None:
        raise RuntimeError("candidate v4 handoff repair is not installed")
    plane = _SCOUT_HYDRATION_PLANE.get()
    if plane is obj and getattr(getattr(obj, "service", None), "_roi_candidate_execution_plane", None) is obj:
        _inc(obj, "v4_handoff_deferred_until_candidate_execution")
        return
    _ORIGINAL_HANDOFF_SCHEDULE(obj, signature)


setattr(_defer_pre_persistence_v4_handoff, "_roi_scout_candidate_continuity", True)


async def _candidate_ingest_then_v4_handoff(
    self: Any,
    swap: NormalizedSwap,
) -> IngestionDecision:
    if _ORIGINAL_EXECUTION_INGEST is None:
        raise RuntimeError("candidate execution delegate repair is not installed")
    decision = await _ORIGINAL_EXECUTION_INGEST(self, swap)
    plane = getattr(self, "_roi_candidate_execution_plane", None)
    if plane is not None and _ORIGINAL_HANDOFF_SCHEDULE is not None:
        _ORIGINAL_HANDOFF_SCHEDULE(plane, swap.signature)
        _inc(plane, "v4_handoff_after_durable_candidate_execution")
    return decision


setattr(_candidate_ingest_then_v4_handoff, "_roi_scout_candidate_continuity", True)


def _scout_source_key(target: WatchTarget) -> str | None:
    if str(getattr(target, "kind", "") or "") == "scout":
        address = str(getattr(target, "address", "") or "")
        return f"SCOUT:{address}" if address else None
    if _ORIGINAL_EXACT_SOURCE_KEY is None:
        return None
    return _ORIGINAL_EXACT_SOURCE_KEY(target)


def _burst_sensitive_target(target: WatchTarget) -> bool:
    if str(getattr(target, "kind", "") or "") == "scout":
        return True
    if _ORIGINAL_BURST_PREDICATE is None:
        return False
    return bool(_ORIGINAL_BURST_PREDICATE(target))


def _assigned_endpoint_preserving_scout_shard(self: Any, target: WatchTarget) -> Any:
    # Extending the burst-sensitive continuity predicate must not collapse all scout
    # routine polling onto the non-official provider. Keep the established shard for
    # scouts while Pump high-volume affinity remains unchanged.
    if str(getattr(target, "kind", "") or "") == "scout":
        base = getattr(affinity, "_ORIGINAL_ASSIGNED_ENDPOINT", None)
        if callable(base):
            return base(self, target)
    if _ORIGINAL_ASSIGNED_ENDPOINT is None:
        raise RuntimeError("scout routine provider assignment repair is not installed")
    return _ORIGINAL_ASSIGNED_ENDPOINT(self, target)


setattr(_assigned_endpoint_preserving_scout_shard, "_roi_scout_candidate_continuity", True)


def _publish_scout_durable_frontier(
    journal: DirectSolanaJournal,
    *,
    signature: str,
    source_key: str,
    slot: int,
    received_at: datetime,
) -> None:
    try:
        parsed_slot = int(slot)
    except (TypeError, ValueError):
        return
    if not signature or parsed_slot <= 0 or not source_key.startswith("SCOUT:"):
        return
    row = {
        "signature": str(signature),
        "slot": parsed_slot,
        "source_key": source_key,
        "received_at": received_at.isoformat(),
        "committed_monotonic": time.monotonic(),
        "durable": True,
        "transport": "websocket",
        "scout_exact_boundary": True,
    }
    frontiers = exact._journal_frontiers(journal)
    current = frontiers.get(source_key)
    if not isinstance(current, dict) or (
        parsed_slot,
        float(row["committed_monotonic"]),
    ) >= (
        int(current.get("slot", 0) or 0),
        float(current.get("committed_monotonic", 0.0) or 0.0),
    ):
        frontiers[source_key] = row
        setattr(
            journal,
            "_roi_exact_durable_ws_frontier_updates",
            int(getattr(journal, "_roi_exact_durable_ws_frontier_updates", 0) or 0) + 1,
        )


def _record_receipt_with_scout_exact_frontier(
    self: DirectSolanaJournal,
    *,
    signature: str,
    source_key: str,
    slot: int,
    received_at: datetime,
    launch_like: bool,
) -> bool:
    if _ORIGINAL_RECORD_RECEIPT is None:
        raise RuntimeError("scout exact durable frontier repair is not installed")
    inserted = bool(
        _ORIGINAL_RECORD_RECEIPT(
            self,
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
            launch_like=launch_like,
        )
    )
    # Live-poll rows also use record_receipt. Only the raw WebSocket dispatcher sets
    # this context, so synthetic poll/history evidence can never create the lower
    # boundary used for prospective recovery.
    if inserted and source_key.startswith("SCOUT:") and raw_dispatch._RECEIPT_WALL_TIME.get() is not None:
        _publish_scout_durable_frontier(
            self,
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
        )
    return inserted


setattr(_record_receipt_with_scout_exact_frontier, "_roi_scout_candidate_continuity", True)


def _batch_rows_with_scout_frontiers(items: list[Any]) -> list[dict[str, Any]]:
    rows = list(_ORIGINAL_BATCH_ROWS(items)) if _ORIGINAL_BATCH_ROWS is not None else []
    seen = {(str(row.get("signature") or ""), str(row.get("source_key") or "")) for row in rows}
    for item in items:
        try:
            _priority, _mono, _sequence, received_at, provider, _targets, _message = capacity._parse_dispatch_item(item)
            fields = capacity._dispatch_fields(item)
        except Exception:
            continue
        if fields is None or str(provider) in post104.LIVE_POLL_PROVIDER_NAMES:
            continue
        target, slot, signature, _failed, _source = fields
        if str(getattr(target, "kind", "") or "") != "scout":
            continue
        address = str(getattr(target, "address", "") or "")
        source_key = f"SCOUT:{address}" if address else ""
        signature = str(signature or "")
        try:
            parsed_slot = int(slot)
        except (TypeError, ValueError):
            parsed_slot = 0
        key = (signature, source_key)
        if not signature or not source_key or parsed_slot <= 0 or key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "signature": signature,
                "source_key": source_key,
                "slot": parsed_slot,
                "received_at": received_at,
                "provider": str(provider),
                "target_kind": "scout",
            }
        )
    return rows


setattr(_batch_rows_with_scout_frontiers, "_roi_scout_candidate_continuity", True)


def _status_with_scout_candidate_continuity(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("scout candidate continuity status repair is not installed")
    payload = _ORIGINAL_STATUS(self)
    failures = dict(_normalization_failures(self))
    payload["scout_candidate_continuity_repair"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "candidate_normalization_identity": "exact-configured-scout-owner-not-assumed-fee-payer",
        "candidate_normalization_attempts_session": int(getattr(self, "_roi_scout_candidate_continuity_normalization_attempts", 0) or 0),
        "candidate_normalization_complete_session": int(getattr(self, "_roi_scout_candidate_continuity_normalization_complete", 0) or 0),
        "candidate_normalization_failed_session": int(getattr(self, "_roi_scout_candidate_continuity_normalization_failed", 0) or 0),
        "candidate_normalization_failure_reasons": failures,
        "pre_persistence_v4_handoff_disabled": True,
        "v4_handoff_deferred_until_candidate_execution_session": int(getattr(self, "_roi_scout_candidate_continuity_v4_handoff_deferred_until_candidate_execution", 0) or 0),
        "v4_handoff_after_durable_candidate_execution_session": int(getattr(self, "_roi_scout_candidate_continuity_v4_handoff_after_durable_candidate_execution", 0) or 0),
        "scout_exact_durable_signature_boundary": True,
        "scout_proactive_pre_gap_frontier": True,
        "scout_routine_provider_sharding_preserved": True,
        "recoverability_lease_seconds_unchanged": RECOVERABILITY_LEASE_SECONDS_UNCHANGED,
        "hard_recovery_bound_unchanged": HARD_RECOVERY_BOUND_UNCHANGED,
        "candidate_processing_target_seconds_unchanged": CANDIDATE_PROCESSING_TARGET_SECONDS_UNCHANGED,
        "candidate_entry_window_seconds_unchanged": CANDIDATE_ENTRY_WINDOW_SECONDS_UNCHANGED,
        "max_chase_fraction_unchanged": MAX_CHASE_FRACTION_UNCHANGED,
        "full_market_scope_reduced": False,
        "strategy_thresholds_changed": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }
    exact_status = payload.get("exact_durable_signature_continuity")
    if isinstance(exact_status, dict):
        exact_status["scout_targets_enabled"] = True
        exact_status["scout_source_key_model"] = "SCOUT:<configured-wallet-address>"
    pre_gap_status = payload.get("high_volume_pre_gap_frontier")
    if isinstance(pre_gap_status, dict):
        pre_gap_status["scout_targets_enabled"] = True
        pre_gap_status["routine_poll_interval_unchanged"] = True
    handoff = payload.get("candidate_risk_quote_v4_handoff")
    if isinstance(handoff, dict):
        handoff["pre_persistence_hydration_handoff_disabled"] = True
        handoff["handoff_after_candidate_execution_persistence"] = True
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "scout_candidate_exact_identity_normalization": True,
                "candidate_v4_handoff_after_durable_execution": True,
                "scout_exact_durable_gap_boundary": True,
                "scout_proactive_pre_gap_frontier": True,
                "scout_routine_provider_sharding_preserved": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "candidate_entry_window_unchanged": True,
                "strategy_thresholds_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_scout_candidate_continuity, "_roi_scout_candidate_continuity", True)


def install_scout_candidate_continuity_repair() -> None:
    """Repair the exact boundaries proved broken by the PR117 production telemetry.

    This installer must run after the candidate execution-evidence plane so it can
    move the V4 bridge from the pre-persistence hydrate return to the durable
    candidate execution completion boundary. No strategy or certification threshold
    is changed.
    """

    global _ORIGINAL_HYDRATE, _ORIGINAL_NORMALIZE, _ORIGINAL_HANDOFF_SCHEDULE
    global _ORIGINAL_EXECUTION_INGEST, _ORIGINAL_RECORD_RECEIPT, _ORIGINAL_EXACT_SOURCE_KEY
    global _ORIGINAL_BURST_PREDICATE, _ORIGINAL_ASSIGNED_ENDPOINT, _ORIGINAL_BATCH_ROWS
    global _ORIGINAL_STATUS

    if bool(getattr(DirectSolanaIngestionPlane.status, "_roi_scout_candidate_continuity", False)):
        return
    if execution_plane._ORIGINAL_SERVICE_INGEST is None:
        raise RuntimeError("candidate execution-evidence plane must be installed first")

    _ORIGINAL_NORMALIZE = direct_module.normalize_standard_transaction
    direct_module.normalize_standard_transaction = _normalize_with_exact_scout_identity  # type: ignore[assignment]

    _ORIGINAL_HYDRATE = DirectSolanaIngestionPlane._hydrate_one
    try:
        _hydrate_with_scout_identity_context.__dict__.update(getattr(_ORIGINAL_HYDRATE, "__dict__", {}))
    except Exception:
        pass
    DirectSolanaIngestionPlane._hydrate_one = _hydrate_with_scout_identity_context  # type: ignore[method-assign]

    _ORIGINAL_HANDOFF_SCHEDULE = candidate_v4._schedule_candidate_handoff
    candidate_v4._schedule_candidate_handoff = _defer_pre_persistence_v4_handoff  # type: ignore[assignment]

    _ORIGINAL_EXECUTION_INGEST = execution_plane._ORIGINAL_SERVICE_INGEST
    execution_plane._ORIGINAL_SERVICE_INGEST = _candidate_ingest_then_v4_handoff

    _ORIGINAL_EXACT_SOURCE_KEY = exact._source_key
    exact._source_key = _scout_source_key  # type: ignore[assignment]

    _ORIGINAL_BURST_PREDICATE = affinity._is_high_volume_target
    affinity._is_high_volume_target = _burst_sensitive_target  # type: ignore[assignment]

    _ORIGINAL_ASSIGNED_ENDPOINT = storage._assigned_endpoint
    storage._assigned_endpoint = _assigned_endpoint_preserving_scout_shard  # type: ignore[assignment]

    _ORIGINAL_RECORD_RECEIPT = DirectSolanaJournal.record_receipt
    DirectSolanaJournal.record_receipt = _record_receipt_with_scout_exact_frontier  # type: ignore[method-assign]

    _ORIGINAL_BATCH_ROWS = post104._actual_ws_high_volume_rows
    post104._actual_ws_high_volume_rows = _batch_rows_with_scout_frontiers  # type: ignore[assignment]

    _ORIGINAL_STATUS = DirectSolanaIngestionPlane.status
    try:
        _status_with_scout_candidate_continuity.__dict__.update(getattr(_ORIGINAL_STATUS, "__dict__", {}))
    except Exception:
        pass
    DirectSolanaIngestionPlane.status = _status_with_scout_candidate_continuity  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "_burst_sensitive_target",
    "_normalize_tracked_wallet",
    "_tracked_scout_wallet",
    "install_scout_candidate_continuity_repair",
]
