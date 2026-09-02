from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from typing import Any, Callable

from . import direct_solana as direct_solana_module
from . import target_stream_fanout as fanout
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint
from .stream_resilience import STREAM_RECONNECT_INITIAL_SECONDS, STREAM_RECONNECT_MAX_SECONDS, _error_parts, _subscription_key


GAP_ERROR = (
    "prospective full-scope target coverage gap; historical RPC backfill cannot restore "
    "live arrival-time evidence"
)


def _all_target_keys(self: Any) -> set[str]:
    return {fanout._target_key(target) for target in self.watch_targets}


def _covered_keys(provider_targets: dict[str, set[str]]) -> set[str]:
    covered: set[str] = set()
    for rows in provider_targets.values():
        covered.update(rows)
    return covered


def _quarantine_gap_backfill(self: Any, *, reason: str) -> int:
    now = direct_solana_module.utcnow().isoformat()
    with self.store._lock, self.store.db:
        cur = self.store.db.execute(
            "UPDATE direct_solana_hydration_queue SET status='failed', last_error=?, updated_at=? "
            "WHERE status IN ('pending','processing') AND reason='gap_backfill'",
            (reason, now),
        )
    return int(cur.rowcount or 0)


async def _reject_historical_gap_recovery(self: Any, _outage_started_at: Any) -> None:
    """Never turn a prospective stream outage into a bulk hydration workload.

    Exact-release latency/chronology evidence cannot be recreated after the fact.
    Historical RPC recovery therefore cannot make a release certifiable and must
    not consume the candidate fast lane or flood the background queue.
    """

    _quarantine_gap_backfill(self, reason=GAP_ERROR)
    self.journal.close_outage(complete=False, error=GAP_ERROR)
    self._recovering = False


setattr(_reject_historical_gap_recovery, "_roi_no_bulk_gap_backfill", True)


async def _quorum_set_target_state(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    *,
    connected: bool,
    error_type: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    """Track provider health per target and derive continuity from provider union.

    A single target stream is independently authoritative once its own
    ``logsSubscribe`` acknowledgement succeeds. Requiring one public provider to
    have all ten targets simultaneously created false global outages even when the
    redundant provider pair collectively covered every frozen target.
    """

    lock, provider_targets, ready_events, states = fanout._state_maps(self)
    provider = endpoint.name
    key = fanout._target_key(target)
    all_keys = _all_target_keys(self)
    provider_event = ready_events.setdefault(provider, asyncio.Event())

    provider_to_full = False
    provider_from_full = False
    global_to_full = False
    global_from_full = False

    async with lock:
        before_covered = _covered_keys(provider_targets)
        before_global = before_covered == all_keys

        live = provider_targets.setdefault(provider, set())
        before_provider_full = live == all_keys
        if connected:
            live.add(key)
        else:
            live.discard(key)
        after_provider_full = live == all_keys
        if after_provider_full:
            provider_event.set()
        else:
            provider_event.clear()

        after_covered = _covered_keys(provider_targets)
        after_global = after_covered == all_keys
        provider_to_full = after_provider_full and not before_provider_full
        provider_from_full = before_provider_full and not after_provider_full
        global_to_full = after_global and not before_global
        global_from_full = before_global and not after_global

        provider_state = states.setdefault(provider, {})
        previous = provider_state.get(key) if isinstance(provider_state.get(key), dict) else {}
        reconnects = int(previous.get("reconnect_count") or 0)
        if connected and not bool(previous.get("connected")):
            reconnects += 1
        provider_state[key] = {
            "connected": bool(connected),
            "kind": target.kind,
            "address": target.address,
            "source_hint": target.source_hint,
            "reconnect_count": reconnects,
            "last_change_at": direct_solana_module.utcnow().isoformat(),
            "last_error_type": error_type,
            "last_error_code": error_code,
            "last_error_message": error_message,
        }

        setup = getattr(self, "_roi_subscription_setup", None)
        if not isinstance(setup, dict):
            setup = {}
            setattr(self, "_roi_subscription_setup", setup)
        setup[provider] = {
            "ready": after_provider_full,
            "phase": "live" if after_provider_full else ("partial" if live else "connecting"),
            "target_count": len(all_keys),
            "acknowledged_count": len(live),
            "current_target": None,
            "current_target_kind": None,
            "attempt": None,
            "error_code": error_code,
            "error_message": error_message,
            "error_type": error_type,
            "topology": "one-logsSubscribe-per-websocket",
        }

        setattr(self, "_roi_full_scope_target_coverage_ok", after_global)
        setattr(self, "_roi_full_scope_target_coverage_count", len(after_covered & all_keys))
        setattr(self, "_roi_full_scope_target_count", len(all_keys))

    # Provider-level telemetry remains strict: a provider is 'connected' only at
    # 10/10. This does not control full-scope continuity anymore; the target union
    # below does.
    if provider_to_full:
        self.journal.set_provider(provider, connected=True, error_type=None)
    elif provider_from_full:
        self.journal.set_provider(provider, connected=False, error_type=error_type)
    elif not connected and not bool(getattr(self, "_roi_provider_ever_full", {}).get(provider, False)):
        self.journal.set_provider(provider, connected=False, error_type=error_type)

    ever_full = getattr(self, "_roi_provider_ever_full", None)
    if not isinstance(ever_full, dict):
        ever_full = {}
        setattr(self, "_roi_provider_ever_full", ever_full)
    if provider_to_full:
        ever_full[provider] = True

    observed = bool(getattr(self, "_roi_global_coverage_observed", False))
    if global_to_full and not observed:
        setattr(self, "_roi_global_coverage_observed", True)
        # Exact-release startup is not a recovery event. The release-epoch logic
        # has already preserved any prior gap before target streams start.
        return

    if observed and global_from_full:
        self.journal.mark_outage(direct_solana_module.utcnow())
        return

    if observed and global_to_full:
        outage = self.journal.outage_started_at()
        if outage is not None:
            _quarantine_gap_backfill(self, reason=GAP_ERROR)
            self.journal.close_outage(complete=False, error=GAP_ERROR)


async def _quorum_single_target_stream(
    self: Any,
    endpoint: RpcEndpoint,
    target: WatchTarget,
    stop: asyncio.Event,
) -> None:
    """Consume one acknowledged target independently of sibling target health."""

    backoff = STREAM_RECONNECT_INITIAL_SECONDS
    while not stop.is_set():
        declared = False
        try:
            async with direct_solana_module.websockets.connect(
                endpoint.ws_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=2,
                max_queue=fanout.TARGET_WS_MAX_QUEUE,
                max_size=fanout.TARGET_WS_MAX_SIZE_BYTES,
                compression=None,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                        }
                    )
                )
                deadline = asyncio.get_running_loop().time() + fanout.TARGET_ACK_TIMEOUT_SECONDS
                external_subscription: str | None = None
                while not stop.is_set() and asyncio.get_running_loop().time() < deadline:
                    remaining = max(0.05, deadline - asyncio.get_running_loop().time())
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    message = json.loads(raw)
                    if not isinstance(message, dict) or message.get("id") not in (1, "1"):
                        continue
                    if message.get("error") is not None:
                        code, provider_message = _error_parts(message.get("error"))
                        await _quorum_set_target_state(
                            self,
                            endpoint,
                            target,
                            connected=False,
                            error_type="SubscriptionRejected",
                            error_code=code,
                            error_message=provider_message,
                        )
                        raise RuntimeError(f"logsSubscribe rejected code={code}: {provider_message}")
                    external_subscription = _subscription_key(message.get("result"))
                    break
                if not external_subscription:
                    raise TimeoutError("single-target Solana logsSubscribe acknowledgement timed out")

                await _quorum_set_target_state(self, endpoint, target, connected=True)
                declared = True
                backoff = STREAM_RECONNECT_INITIAL_SECONDS
                subscription_targets = {1: target}

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=5.0)
                        continue
                    message = json.loads(raw)
                    if not isinstance(message, dict) or message.get("method") != "logsNotification":
                        continue
                    params = message.get("params")
                    if not isinstance(params, dict):
                        continue
                    try:
                        if _subscription_key(params.get("subscription")) != external_subscription:
                            continue
                    except Exception:
                        continue
                    mapped = dict(message)
                    mapped_params = dict(params)
                    mapped_params["subscription"] = 1
                    mapped["params"] = mapped_params
                    # Evidence from this acknowledged target remains valid even if
                    # a sibling target on the same provider is reconnecting. The
                    # global continuity gate independently requires every one of
                    # the ten targets to have at least one live provider.
                    await self._handle_notification(endpoint.name, subscription_targets, mapped)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _quorum_set_target_state(
                self,
                endpoint,
                target,
                connected=False,
                error_type=type(exc).__name__,
            )
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(STREAM_RECONNECT_MAX_SECONDS, backoff * 2.0)
        else:
            if declared:
                await _quorum_set_target_state(self, endpoint, target, connected=False)


setattr(_quorum_single_target_stream, "_roi_target_quorum_stream", True)


def _status_with_quorum(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        all_keys = _all_target_keys(self)
        _lock, provider_targets, _events, _states = fanout._state_maps(self)
        covered = _covered_keys(provider_targets) & all_keys
        coverage_ok = bool(all_keys) and covered == all_keys
        unresolved = bool(payload.get("unresolved_gap", True))
        payload["continuity_ok"] = bool(coverage_ok and not unresolved)
        payload["full_scope_target_quorum"] = {
            "model": "per-target-provider-union",
            "covered_target_count": len(covered),
            "target_count": len(all_keys),
            "coverage_ok": coverage_ok,
            "minimum_live_provider_count_per_target": min(
                (
                    sum(1 for rows in provider_targets.values() if key in rows)
                    for key in all_keys
                ),
                default=0,
            ),
            "historical_backfill_can_restore_prospective_continuity": False,
        }
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "continuity_model": "all-ten-targets-covered-by-provider-union",
                    "acknowledged_partial_provider_target_evidence_recorded": True,
                    "partial_provider_evidence_recorded": True,
                    "bulk_gap_backfill_for_certification": False,
                    "gap_backfill_candidate_lane_allowed": False,
                }
            )
        return payload

    setattr(status, "_roi_target_quorum", True)
    setattr(status, "_roi_memory_bounded", True)
    return status


def install_target_quorum() -> None:
    """Install target-level redundancy and remove false provider-level outages."""

    fanout._set_target_state = _quorum_set_target_state  # type: ignore[assignment]
    fanout._single_target_stream = _quorum_single_target_stream  # type: ignore[assignment]

    DirectSolanaIngestionPlane._recover_gap = _reject_historical_gap_recovery  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_target_quorum", False)):
        DirectSolanaIngestionPlane.status = _status_with_quorum(current_status)  # type: ignore[method-assign]


__all__ = ["GAP_ERROR", "install_target_quorum"]
