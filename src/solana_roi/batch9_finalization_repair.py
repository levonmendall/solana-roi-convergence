from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from . import batch9_continuity_frontier_proof_repair as batch9
from . import batch9_scout_checkpoint_commit_repair as checkpoint_commit
from . import high_volume_signature_cursor_repair as high_volume
from . import live_poll_redundancy as live_poll
from . import poll_pagination_context as pagination
from . import poll_watermark_repair as watermark
from . import render_runtime_bootstrap_repair as render_bootstrap
from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_production_ws_transport as prod_ws
from . import robinhood_usage_bounded_transport as bounded_ws
from . import robinhood_worker_isolation_repair as robinhood_isolation
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .robinhood_chain_paper import RobinhoodChainPaperPlane


REPAIR_VERSION = "batch9-continuity-frontier-proof-v3-finalized"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_DELTA_HOOK: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_BOUNDED_PROCESS_BLOCK: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_READER_GENERATION_START: Callable[[Any], int] | None = None
_INSTALLED = False


def _strict_record_reconciliation_audit_rows(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    """Commit every restart-reconciliation receipt before its checkpoint can move.

    These rows remain audit-only and never receive retrospective entry authority.
    Unlike the earlier best-effort helper, a receipt-store exception propagates so
    the same-release checkpoint remains at its prior durable slot and the next
    restart retries the exact missing interval.
    """

    journal = getattr(self, "journal", None)
    if journal is None or not hasattr(journal, "record_receipt"):
        return 0
    source_key = target.source_hint or f"SCOUT:{target.address}"
    inserted = 0
    for row in rows:
        signature = str(row.get("signature") or "")
        slot = watermark._row_slot(row)
        if not signature or slot <= 0:
            continue
        if journal.record_receipt(
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=batch9.direct_module.utcnow(),
            launch_like=False,
        ):
            inserted += 1
    return inserted


async def _scout_then_existing_delta_hook(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> Any:
    """Specialize scouts below canonical pagination/exception-rearm identities."""

    if target.kind == "scout":
        return await checkpoint_commit._fetch_with_commit_order(self, target, cursor_slot)
    if _ORIGINAL_DELTA_HOOK is None:
        return None
    return await _ORIGINAL_DELTA_HOOK(self, target, cursor_slot)


setattr(_scout_then_existing_delta_hook, "_roi_batch9_scout_checkpoint", True)


def _item_block(item: dict[str, Any]) -> int:
    try:
        return int(str(item.get("log", {}).get("blockNumber") or "0x0"), 16)
    except (TypeError, ValueError):
        return 0


async def _process_block_with_generation_anchor(
    self: Any,
    items: list[dict[str, Any]],
    *,
    generation: int,
) -> None:
    """Anchor each real WSS generation before its first live block can authorize.

    The usage-bounded production runner remains the canonical runner. It already
    advances the live cursor only after this function completes. This wrapper adds
    the missing generation anchor beneath that runner and quarantines the anchor
    block itself from entry authority. Subsequent blocks in the same generation use
    the unchanged bounded transport semantics.
    """

    if _ORIGINAL_BOUNDED_PROCESS_BLOCK is None:
        raise RuntimeError("Batch 9 finalizer missing bounded Robinhood block processor")

    state = prod_ws._state(self)
    current_generation = int(state.get("generation", 0) or 0)
    requested_generation = int(generation)
    has_live_authority = any(bool(item.get("live_authority", False)) for item in items)
    blocks = [block for block in (_item_block(item) for item in items) if block > 0]
    block = min(blocks) if blocks else 0
    anchor_missing = (
        getattr(self, "_roi_prod_ws_epoch_generation", None) != requested_generation
        or getattr(self, "_roi_live_epoch_anchor_block", None) is None
        or not bool(getattr(self, "_roi_live_epoch_started_at", None))
    )

    if (
        requested_generation == current_generation
        and has_live_authority
        and block > 0
        and anchor_missing
    ):
        frontier._start_epoch(
            self,
            anchor_block=block,
            reason="production_ws_generation_anchor",
        )
        setattr(self, "_roi_prod_ws_epoch_generation", requested_generation)
        setattr(self, "_roi_prod_ws_anchor_quarantine_through_block", block)

        # The first block establishes prospective time zero. Process its market
        # definitions/observations, but never let that same anchor block authorize a
        # paper entry. The canonical runner will move the cursor only after this call
        # succeeds, so a processing exception cannot publish a false frontier.
        quarantined = [dict(item, live_authority=False) for item in items]
        prior_suppress = bool(getattr(self, "_roi_live_epoch_suppress_entries", False))
        setattr(self, "_roi_live_epoch_suppress_entries", True)
        try:
            await _ORIGINAL_BOUNDED_PROCESS_BLOCK(
                self,
                quarantined,
                generation=requested_generation,
            )
        finally:
            setattr(self, "_roi_live_epoch_suppress_entries", prior_suppress)
        setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        setattr(self, "_roi_live_epoch_last_error_type", None)
        return

    await _ORIGINAL_BOUNDED_PROCESS_BLOCK(
        self,
        items,
        generation=requested_generation,
    )
    if (
        requested_generation == current_generation
        and getattr(self, "_roi_prod_ws_epoch_generation", None) == requested_generation
        and batch9._strict_live_epoch_active(self)
    ):
        setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        setattr(self, "_roi_live_epoch_last_error_type", None)


setattr(_process_block_with_generation_anchor, "_roi_batch9_generation_anchor", True)


def _reader_generation_start_with_epoch_reset(self: Any) -> int:
    if _ORIGINAL_READER_GENERATION_START is None:
        raise RuntimeError("Batch 9 finalizer missing Robinhood generation starter")
    generation = int(_ORIGINAL_READER_GENERATION_START(self))
    setattr(self, "_roi_prod_ws_epoch_generation", None)
    self._caught_up = False
    setattr(self, "_roi_live_epoch_ready", False)
    setattr(
        self,
        "_roi_production_transport_block_reason",
        "robinhood_production_websocket_reanchoring",
    )
    return generation


setattr(_reader_generation_start_with_epoch_reset, "_roi_batch9_generation_anchor", True)


def _install_solana_scout_recovery() -> None:
    global _ORIGINAL_DELTA_HOOK

    # The high-volume wrapper is the canonical top-level page identity. Insert the
    # scout baseline/restart checkpoint immediately below it instead of replacing it.
    if high_volume._ORIGINAL_SLOT_POLL_PAGE is None:
        raise RuntimeError("Batch 9 requires the canonical high-volume poll wrapper")
    if high_volume._ORIGINAL_SLOT_POLL_PAGE is not batch9._slot_page_with_durable_scout_checkpoint:
        batch9._ORIGINAL_SLOT_PAGE = high_volume._ORIGINAL_SLOT_POLL_PAGE
        high_volume._ORIGINAL_SLOT_POLL_PAGE = batch9._slot_page_with_durable_scout_checkpoint

    # Pagination intentionally exposes one lower specialization hook. Compose the
    # scout delegate ahead of the already-installed high-volume delegate while
    # leaving watermark._slot_fetch_delta == exception-rearm canonical identity.
    if pagination._HIGH_VOLUME_DELTA_HOOK is not _scout_then_existing_delta_hook:
        _ORIGINAL_DELTA_HOOK = pagination._HIGH_VOLUME_DELTA_HOOK
        pagination._HIGH_VOLUME_DELTA_HOOK = _scout_then_existing_delta_hook

    # Fallback receipts must durably commit before the scout cursor advances.
    if live_poll._record_poll_rows is not checkpoint_commit._record_rows_then_checkpoint:
        checkpoint_commit._ORIGINAL_RECORD_ROWS = live_poll._record_poll_rows
        live_poll._record_poll_rows = checkpoint_commit._record_rows_then_checkpoint

    batch9._record_reconciliation_audit_rows = _strict_record_reconciliation_audit_rows

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_batch9_scout_checkpoint", False)):
        batch9._ORIGINAL_DIRECT_STATUS = current_status
        try:
            batch9._direct_status_with_batch9.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(batch9._direct_status_with_batch9, "_roi_batch9_scout_checkpoint", True)
        DirectSolanaIngestionPlane.status = batch9._direct_status_with_batch9


def _install_robinhood_proof_snapshot() -> None:
    if robinhood_isolation._refresh_proof_on_separate_connection is not batch9._snapshot_robinhood_proof_refresh:
        batch9._ORIGINAL_PROOF_REFRESH = robinhood_isolation._refresh_proof_on_separate_connection
        robinhood_isolation._refresh_proof_on_separate_connection = batch9._snapshot_robinhood_proof_refresh

    if robinhood_isolation._worker_isolation_metadata is not batch9._proof_metadata_with_batch9:
        batch9._ORIGINAL_PROOF_METADATA = robinhood_isolation._worker_isolation_metadata
        try:
            batch9._proof_metadata_with_batch9.__dict__.update(
                getattr(batch9._ORIGINAL_PROOF_METADATA, "__dict__", {})
            )
        except Exception:
            pass
        robinhood_isolation._worker_isolation_metadata = batch9._proof_metadata_with_batch9


def _install_robinhood_generation_anchor() -> None:
    global _ORIGINAL_BOUNDED_PROCESS_BLOCK, _ORIGINAL_READER_GENERATION_START

    # Preserve the usage-bounded runner itself. Only its lower block processor and
    # generation-start hook are specialized, so all existing provider-usage and
    # compatibility contracts retain their canonical function identities.
    if bounded_ws._process_block is not _process_block_with_generation_anchor:
        _ORIGINAL_BOUNDED_PROCESS_BLOCK = bounded_ws._process_block
        bounded_ws._process_block = _process_block_with_generation_anchor
        prod_ws._process_block = _process_block_with_generation_anchor

    if prod_ws._reader_generation_start is not _reader_generation_start_with_epoch_reset:
        _ORIGINAL_READER_GENERATION_START = prod_ws._reader_generation_start
        prod_ws._reader_generation_start = _reader_generation_start_with_epoch_reset

    current_status = RobinhoodChainPaperPlane.status
    if not bool(getattr(current_status, "_roi_batch9_epoch_integrity", False)):
        batch9._ORIGINAL_PLANE_STATUS = current_status
        try:
            batch9._plane_status_with_epoch_integrity.__dict__.update(
                getattr(current_status, "__dict__", {})
            )
        except Exception:
            pass
        setattr(batch9._plane_status_with_epoch_integrity, "_roi_batch9_epoch_integrity", True)
        RobinhoodChainPaperPlane.status = batch9._plane_status_with_epoch_integrity


def _install_proof_precompute() -> None:
    current_workers = render_bootstrap._run_runtime_workers
    if not bool(getattr(current_workers, "_roi_batch9_proof_precompute", False)):
        batch9._ORIGINAL_RUNTIME_WORKERS = current_workers
        setattr(batch9._runtime_workers_with_proof_precompute, "_roi_batch9_proof_precompute", True)
        render_bootstrap._run_runtime_workers = batch9._runtime_workers_with_proof_precompute


def install_batch9_finalization_repair(app: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    _install_solana_scout_recovery()
    _install_robinhood_proof_snapshot()
    _install_robinhood_generation_anchor()
    _install_proof_precompute()

    app.state.roi_batch9_continuity_frontier_proof_repair = True
    app.state.roi_batch9_continuity_frontier_proof_repair_version = REPAIR_VERSION
    app.state.roi_batch9_canonical_contracts_preserved = True
    app.state.roi_batch9_paper_only = True
    app.state.roi_batch9_live_money_authority = False
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_process_block_with_generation_anchor",
    "_reader_generation_start_with_epoch_reset",
    "_scout_then_existing_delta_hook",
    "_strict_record_reconciliation_audit_rows",
    "install_batch9_finalization_repair",
]
