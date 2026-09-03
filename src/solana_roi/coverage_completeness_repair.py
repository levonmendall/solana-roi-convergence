from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from . import launch_coverage_bridge as bridge
from .direct_funding import SolanaRpcFundingCollector, _native_inbound_transfers
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import DexScreenerLaunchCollector, FundingSource
from .live_collectors import _fresh
from .risk import EntityLink, FundingEvidence, LaunchEvidence, RiskDimension


_ORIGINAL_HYDRATE_MINT_LAUNCH_CONTEXT = bridge._hydrate_mint_launch_context


def _launch_contexts(self: DexScreenerLaunchCollector) -> dict[str, dict[str, Any]]:
    value = getattr(self, "_roi_launch_coverage_contexts", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_launch_coverage_contexts", value)
    return value


def _funding_contexts(self: SolanaRpcFundingCollector) -> dict[str, bool]:
    value = getattr(self, "_roi_funding_coverage_contexts", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_funding_coverage_contexts", value)
    return value


def _seed_launch_coverage_context(
    self: DexScreenerLaunchCollector,
    mint: str,
    *,
    created_at: datetime,
    observed_at: datetime,
    complete: bool,
) -> None:
    self.seed_created_at(mint, created_at)
    _launch_contexts(self)[mint] = {
        "created_at": created_at,
        "observed_at": observed_at,
        "complete": bool(complete),
    }


def _seed_funding_coverage_context(
    self: SolanaRpcFundingCollector,
    mint: str,
    *,
    complete: bool,
) -> None:
    _funding_contexts(self)[mint] = bool(complete)


def _queue_trigger_received_at(self: Any, signature: str) -> datetime | None:
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT trigger_received_at FROM direct_solana_hydration_queue WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        if row is None:
            return None
        value = row["trigger_received_at"] if hasattr(row, "keys") else row[0]
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _seed_runtime_collectors(
    self: Any,
    *,
    mint: str,
    created_at: datetime,
    observed_at: datetime,
    complete: bool,
) -> bool:
    raw = bridge._raw_collectors(self)
    seeded = False
    launch = getattr(raw, "launch", None)
    launch_seed = getattr(launch, "seed_coverage_context", None)
    if callable(launch_seed):
        launch_seed(
            mint,
            created_at=created_at,
            observed_at=observed_at,
            complete=complete,
        )
        seeded = True
    funding = getattr(raw, "funding", None)
    funding_seed = getattr(funding, "seed_coverage_context", None)
    if callable(funding_seed):
        funding_seed(mint, complete=complete)
        seeded = True
    return seeded


async def _hydrate_mint_launch_context_with_attestation(
    self: Any,
    *,
    mint: str,
    source: str,
    launch_signature: str,
    created_at: datetime,
) -> tuple[int, bool, int]:
    persisted, complete, candidate_count = await _ORIGINAL_HYDRATE_MINT_LAUNCH_CONTEXT(
        self,
        mint=mint,
        source=source,
        launch_signature=launch_signature,
        created_at=created_at,
    )

    # If almost the entire bounded signature page is inside the launch window, the
    # one-page mint query cannot prove it reached the older edge of that window.
    # Keep that rare high-volume case fail-closed rather than overstating coverage.
    signature_window_bounded = candidate_count < max(1, bridge.LAUNCH_CONTEXT_SIGNATURE_LIMIT - 1)
    complete = bool(complete and signature_window_bounded)
    if not signature_window_bounded:
        bridge._increment(self, "signature_window_truncated")

    observed_at = _queue_trigger_received_at(self, launch_signature)
    if observed_at is not None and _seed_runtime_collectors(
        self,
        mint=mint,
        created_at=created_at,
        observed_at=observed_at,
        complete=complete,
    ):
        bridge._increment(self, "coverage_context_attested")
        if complete:
            bridge._increment(self, "coverage_context_complete")
        else:
            bridge._increment(self, "coverage_context_incomplete_attested")
    else:
        # Missing the live receipt timestamp means the 3-second near-creation
        # claim cannot be reconstructed from chain time alone. Preserve old
        # fail-closed collector semantics in that case.
        bridge._increment(self, "coverage_context_unattested")
    return persisted, complete, candidate_count


async def _launch_collect_with_coverage_semantics(
    self: DexScreenerLaunchCollector,
    mint: str,
    at: datetime,
) -> bool:
    if _fresh(self.risk, mint, RiskDimension.LAUNCH, at):
        return True
    created_at = await self._created_at(mint)
    if created_at is None:
        return False
    if at < created_at + timedelta(seconds=self.policy.launch_window_seconds):
        return False

    rows = self._early_rows(
        mint,
        start=created_at - timedelta(seconds=1),
        end=created_at + timedelta(seconds=self.policy.launch_window_seconds),
        decision_at=at,
    )
    buys = [row for row in rows if row["side"] == "buy"]
    buyers = {str(row["wallet"]) for row in buys}

    context = _launch_contexts(self).get(mint)
    if isinstance(context, dict):
        observed_at = context.get("observed_at")
        if not isinstance(observed_at, datetime):
            lag_seconds = None
        else:
            lag_seconds = abs((observed_at - created_at).total_seconds())
        near_creation = (
            lag_seconds is not None
            and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        )
        # Completeness now means that the immutable launch-window acquisition was
        # proved complete. The number of buyers is market behavior, not evidence
        # that the observer did or did not cover the window.
        early_complete = bool(context.get("complete"))
        source = "program-wide-swaps+confirmed-launch-context:launch-window-v2"
    else:
        earliest = min(
            (datetime.fromisoformat(str(row["observed_at"])) for row in rows),
            default=None,
        )
        lag_seconds = abs((earliest - created_at).total_seconds()) if earliest is not None else None
        near_creation = (
            lag_seconds is not None
            and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        )
        # Preserve legacy/offline behavior unless a live bridge attestation exists.
        early_complete = (
            len(buys) >= self.policy.min_launch_buys
            and len(buyers) >= self.policy.min_launch_buyers
        )
        source = "program-wide-swaps+dexscreener:launch-window-v1"

    if hasattr(self.store, "record_program_coverage"):
        self.store.record_program_coverage(
            token_mint=mint,
            pair_created_at=created_at.isoformat(),
            assessed_at=at.isoformat(),
            launch_lag_ms=lag_seconds * 1000.0 if lag_seconds is not None else None,
            launch_near_creation=near_creation,
            early_buy_count=len(buys),
            early_buyer_count=len(buyers),
            early_buyers_complete=early_complete,
        )
    if not early_complete or not near_creation:
        return False

    slot_buyers: dict[int, set[str]] = {}
    buyer_sol: dict[str, float] = {}
    total_sol = 0.0
    for row in buys:
        slot_buyers.setdefault(int(row["slot"]), set()).add(str(row["wallet"]))
        amount = float(row["native_amount_sol"])
        total_sol += amount
        buyer_sol[str(row["wallet"])] = buyer_sol.get(str(row["wallet"]), 0.0) + amount
    bundled = max((len(value) for value in slot_buyers.values()), default=0) >= self.policy.bundled_same_slot_buyers
    top_two = sum(sorted(buyer_sol.values(), reverse=True)[:2])
    sniper_heavy = total_sol > 0 and top_two / total_sol >= self.policy.sniper_top_two_buy_share
    self.risk.record_launch(
        mint,
        LaunchEvidence(bundled_launch=bundled, sniper_heavy=sniper_heavy),
        observed_at=at,
        received_at=at,
        source=source,
    )
    return True


def _early_buyers_in_launch_window(
    self: SolanaRpcFundingCollector,
    mint: str,
    at: datetime,
) -> list[tuple[str, datetime]]:
    start: datetime | None = None
    end: datetime | None = None
    try:
        with self.store._lock:
            coverage = self.store.db.execute(
                "SELECT pair_created_at FROM program_coverage_observations WHERE token_mint=? LIMIT 1",
                (mint,),
            ).fetchone()
        if coverage is not None:
            raw = coverage["pair_created_at"] if hasattr(coverage, "keys") else coverage[0]
            created = datetime.fromisoformat(str(raw))
            start = created - timedelta(seconds=1)
            end = created + timedelta(seconds=self.policy.launch_window_seconds)
    except Exception:
        start = None
        end = None

    sql = (
        "SELECT wallet, observed_at, received_at FROM normalized_swaps "
        "WHERE token_mint=? AND side='buy' AND received_at<=?"
    )
    args: list[Any] = [mint, at.isoformat()]
    if start is not None and end is not None:
        sql += " AND observed_at>=? AND observed_at<=?"
        args.extend((start.isoformat(), end.isoformat()))
    sql += " ORDER BY observed_at, id LIMIT 200"
    with self.store._lock:
        rows = self.store.db.execute(sql, tuple(args)).fetchall()

    seen: set[str] = set()
    result: list[tuple[str, datetime]] = []
    for row in rows:
        wallet = str(row["wallet"])
        if wallet in seen:
            continue
        seen.add(wallet)
        result.append((wallet, datetime.fromisoformat(str(row["observed_at"]))))
        if len(result) >= self.policy.funding_early_buyer_count:
            break
    return result


async def _funding_source_result(
    self: SolanaRpcFundingCollector,
    wallet: str,
    before_at: datetime,
) -> tuple[FundingSource | None, bool]:
    """Return the latest qualifying recent funder and whether provenance is complete.

    Pages and transactions are examined newest-first. Once a qualifying inbound
    transfer is found it is necessarily the latest one in the policy lookback, so
    older history is irrelevant. If no transfer is found, completeness is proved
    only by reaching the lookback/history boundary within the unchanged page cap.
    """

    start_at = before_at - timedelta(days=self.policy.funding_lookback_days)
    before_signature: str | None = None
    threshold_lamports = int(self.policy.min_funding_transfer_sol * 1_000_000_000)

    for _page_index in range(self.policy.max_history_pages):
        rows, _provider, _latency = await self.rpc.get_signatures_for_address(
            wallet,
            before=before_signature,
            limit=1000,
            hedge=True,
        )
        if not rows:
            return None, True

        boundary_reached = False
        for row in rows:
            try:
                block_time = int(row.get("blockTime") or 0)
            except (TypeError, ValueError):
                continue
            if block_time <= 0:
                continue
            observed = datetime.fromtimestamp(block_time, tz=timezone.utc)
            if observed >= before_at:
                continue
            if observed < start_at:
                boundary_reached = True
                break
            if row.get("err") is not None:
                continue
            signature = str(row.get("signature") or "")
            if not signature:
                continue
            tx, _tx_provider, _tx_latency = await self.rpc.get_transaction(
                signature,
                hedge=True,
            )
            if not isinstance(tx, dict):
                continue
            try:
                tx_block_time = int(tx.get("blockTime") or block_time)
            except (TypeError, ValueError):
                tx_block_time = block_time
            transfer_at = datetime.fromtimestamp(tx_block_time, tz=timezone.utc)
            candidates = [
                (source, lamports)
                for source, lamports in _native_inbound_transfers(tx, wallet)
                if lamports >= threshold_lamports
            ]
            if candidates:
                source, lamports = max(candidates, key=lambda item: item[1])
                return (
                    FundingSource(
                        wallet,
                        source,
                        lamports / 1_000_000_000,
                        transfer_at,
                    ),
                    True,
                )

        if boundary_reached or len(rows) < 1000:
            return None, True
        before_signature = str(rows[-1].get("signature") or "")
        if not before_signature:
            return None, False

    return None, False


async def _funding_source_compat(
    self: SolanaRpcFundingCollector,
    wallet: str,
    before_at: datetime,
) -> FundingSource | None:
    source, complete = await _funding_source_result(self, wallet, before_at)
    return source if complete else None


async def _funding_collect_with_coverage_semantics(
    self: SolanaRpcFundingCollector,
    mint: str,
    at: datetime,
) -> bool:
    if _fresh(self.risk, mint, RiskDimension.FUNDING, at):
        return True
    buyers = self._early_buyers(mint, at)
    context_complete = _funding_contexts(self).get(mint)
    if context_complete is not True and len(buyers) < 3:
        # Legacy/offline behavior is unchanged without a live complete-window
        # attestation from the launch bridge.
        return False

    sources: list[FundingSource] = []
    for wallet, buy_at in buyers:
        source, complete = await self._source_result(wallet, min(at, buy_at))
        if not complete:
            return False
        if source is not None:
            sources.append(source)

    by_funder: dict[str, list[FundingSource]] = {}
    for source in sources:
        by_funder.setdefault(source.funder, []).append(source)
    for funder, group in by_funder.items():
        if len(group) < 2:
            continue
        ordered = sorted(group, key=lambda item: item.transfer_at)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1:]:
                gap = abs((right.transfer_at - left.transfer_at).total_seconds())
                denom = max(left.amount_sol, right.amount_sol)
                amount_gap = abs(left.amount_sol - right.amount_sol) / denom if denom else 1.0
                if (
                    gap <= self.policy.common_funder_max_gap_seconds
                    and amount_gap <= self.policy.common_funder_amount_tolerance
                ):
                    self.risk.entity_resolver.record_link(
                        EntityLink(
                            wallet_a=left.wallet,
                            wallet_b=right.wallet,
                            relationship=f"common_recent_native_funder:{funder}",
                            confidence=self.policy.common_funder_link_confidence,
                            observed_at=max(left.transfer_at, right.transfer_at),
                            received_at=at,
                            source="solana-standard-rpc:funding-provenance-v2",
                        )
                    )

    self.risk.record_funding(
        mint,
        FundingEvidence(tuple(wallet for wallet, _buy_at in buyers)),
        observed_at=at,
        received_at=at,
        source="solana-standard-rpc:complete-early-buyer-provenance-v2",
    )
    if hasattr(self.store, "mark_program_coverage_funding_complete"):
        self.store.mark_program_coverage_funding_complete(mint, assessed_at=at.isoformat())
    return True


def _status_with_coverage_completeness(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "coverage_semantics": "live-arrival+immutable-window-acquisition-v2",
                    "near_creation_uses_live_launch_receipt": True,
                    "early_buyer_completeness_is_acquisition_completeness": True,
                    "funding_scans_newest_first_until_latest_source_or_proven_boundary": True,
                    "funding_rpc_hedging": True,
                    "coverage_context_attested": int(getattr(self, "_roi_launch_bridge_coverage_context_attested", 0) or 0),
                    "coverage_context_complete": int(getattr(self, "_roi_launch_bridge_coverage_context_complete", 0) or 0),
                    "coverage_context_incomplete_attested": int(getattr(self, "_roi_launch_bridge_coverage_context_incomplete_attested", 0) or 0),
                    "coverage_context_unattested": int(getattr(self, "_roi_launch_bridge_coverage_context_unattested", 0) or 0),
                    "signature_window_truncated": int(getattr(self, "_roi_launch_bridge_signature_window_truncated", 0) or 0),
                    "candidate_activation_from_launch_bridge": False,
                    "latency_samples_from_launch_bridge": False,
                    "quote_samples_from_launch_bridge": False,
                    "paper_authority": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_coverage_completeness_repair", True)
    return status


def install_coverage_completeness_repair() -> None:
    DexScreenerLaunchCollector.seed_coverage_context = _seed_launch_coverage_context  # type: ignore[attr-defined]
    DexScreenerLaunchCollector.collect = _launch_collect_with_coverage_semantics  # type: ignore[method-assign]
    SolanaRpcFundingCollector.seed_coverage_context = _seed_funding_coverage_context  # type: ignore[attr-defined]
    SolanaRpcFundingCollector._early_buyers = _early_buyers_in_launch_window  # type: ignore[method-assign]
    SolanaRpcFundingCollector._source_result = _funding_source_result  # type: ignore[attr-defined]
    SolanaRpcFundingCollector._source = _funding_source_compat  # type: ignore[method-assign]
    SolanaRpcFundingCollector.collect = _funding_collect_with_coverage_semantics  # type: ignore[method-assign]

    # The already-installed launch hydrator resolves this module global at run
    # time, so no second queue or worker is introduced.
    bridge._hydrate_mint_launch_context = _hydrate_mint_launch_context_with_attestation  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_coverage_completeness_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_coverage_completeness(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_coverage_completeness_repair",
    "_funding_source_result",
    "_hydrate_mint_launch_context_with_attestation",
    "_launch_collect_with_coverage_semantics",
]
