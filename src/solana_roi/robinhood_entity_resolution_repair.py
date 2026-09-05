from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Callable

from .robinhood_chain_core import (
    BLOCKSCOUT_API,
    KNOWN_NON_ACTORS,
    ROBINHOOD_CHAIN_ID,
    ROBINHOOD_STOCK_ASSETS_API,
    _clean_address,
    _finite,
)


REPAIR_VERSION = "robinhood-entity-resolution-funnel-v1"
ENTITY_POSITIVE_TTL_SECONDS = 6 * 3600.0
ENTITY_BACKOFF_INITIAL_SECONDS = 5.0
ENTITY_BACKOFF_MAX_SECONDS = 300.0
ENTITY_RATE_LIMIT_BACKOFF_SECONDS = 60.0
RWA_REFRESH_TTL_SECONDS = 3600.0
RWA_RETRY_SECONDS = 60.0

_ORIGINAL_V5_CHOOSE: Callable[..., Any] | None = None
_ORIGINAL_V5_INSERT: Callable[..., Any] | None = None
_ORIGINAL_MAYBE_OPEN_V2: Callable[..., Any] | None = None
_ORIGINAL_MAYBE_OPEN_V3: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., Any] | None = None


def _stats(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_entity_resolution_stats", None)
    if not isinstance(state, dict):
        state = {
            "http_requests": 0,
            "positive_cache_hits": 0,
            "negative_cache_hits": 0,
            "inflight_dedup_hits": 0,
            "resolved_funding_anchors": 0,
            "resolved_singletons": 0,
            "external_failures": 0,
            "rate_limit_failures": 0,
            "partial_contexts": 0,
            "decision_ready_contexts": 0,
            "blocked_trigger_unresolved": 0,
            "blocked_deployer_unresolved": 0,
            "last_error_type": None,
            "last_error_status": None,
        }
        setattr(self, "_roi_entity_resolution_stats", state)
    return state


def _funnel(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_robinhood_decision_funnel", None)
    if not isinstance(state, dict):
        state = {
            "candidate_checks": 0,
            "v2_candidate_checks": 0,
            "v3_candidate_checks": 0,
            "not_caught_up": 0,
            "already_open": 0,
            "flow_evaluations": 0,
            "entity_decision_ready": 0,
            "entity_blocked": 0,
            "entity_partial_contexts": 0,
            "lane_evaluations": 0,
            "lane_selected": 0,
            "lane_rejected": 0,
            "paper_trials_opened": 0,
            "v2_no_trial_after_policy": 0,
            "v3_no_trial_after_policy": 0,
            "last_rejection_stage": None,
        }
        setattr(self, "_roi_robinhood_decision_funnel", state)
    return state


def _negative_cache(self: Any) -> dict[str, tuple[float, str, int, int | None]]:
    state = getattr(self, "_roi_entity_negative_cache", None)
    if not isinstance(state, dict):
        state = {}
        setattr(self, "_roi_entity_negative_cache", state)
    return state


def _inflight(self: Any) -> dict[str, asyncio.Task[str | None]]:
    state = getattr(self, "_roi_entity_resolution_inflight", None)
    if not isinstance(state, dict):
        state = {}
        setattr(self, "_roi_entity_resolution_inflight", state)
    return state


def _entity_semaphore(self: Any) -> asyncio.Semaphore:
    semaphore = getattr(self, "_roi_entity_resolution_semaphore", None)
    if isinstance(semaphore, asyncio.Semaphore):
        return semaphore
    raw = os.getenv("ROBINHOOD_ENTITY_RESOLUTION_MAX_CONCURRENCY", "3")
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        limit = 3
    limit = max(1, min(8, limit))
    semaphore = asyncio.Semaphore(limit)
    setattr(self, "_roi_entity_resolution_semaphore", semaphore)
    setattr(self, "_roi_entity_resolution_max_concurrency", limit)
    return semaphore


def _failure_backoff(exc: BaseException, count: int) -> tuple[float, int | None]:
    response = getattr(exc, "response", None)
    raw_status = getattr(response, "status_code", None)
    try:
        status = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        status = None
    delay = min(
        ENTITY_BACKOFF_MAX_SECONDS,
        ENTITY_BACKOFF_INITIAL_SECONDS * (2 ** min(max(0, count - 1), 6)),
    )
    if status == 429:
        delay = max(delay, ENTITY_RATE_LIMIT_BACKOFF_SECONDS)
    elif status is not None and status >= 500:
        delay = max(delay, 15.0)
    return delay, status


async def _entity_anchor_fetch(self: Any, actor: str) -> str | None:
    stats = _stats(self)
    negative = _negative_cache(self)
    prior = negative.get(actor)
    prior_count = int(prior[2]) if prior is not None else 0
    required = os.getenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true").strip().lower() not in {
        "0",
        "false",
        "no",
    }
    try:
        async with _entity_semaphore(self):
            params: dict[str, Any] = {"filter": "to"}
            oldest: tuple[int, str] | None = None
            for _ in range(3):
                stats["http_requests"] = int(stats["http_requests"]) + 1
                response = await self.rpc.client.get(
                    f"{BLOCKSCOUT_API}/addresses/{actor}/transactions",
                    params=params,
                    timeout=2.5,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("items") if isinstance(payload, dict) else None
                if not isinstance(items, list):
                    items = []
                for tx in items:
                    if not isinstance(tx, dict):
                        continue
                    to_raw = tx.get("to")
                    from_raw = tx.get("from")
                    to_addr = _clean_address(
                        (to_raw if isinstance(to_raw, dict) else {}).get("hash")
                    )
                    from_addr = _clean_address(
                        (from_raw if isinstance(from_raw, dict) else {}).get("hash")
                    )
                    try:
                        value = int(str(tx.get("value") or "0"))
                        block = int(tx.get("block_number") or 0)
                    except (TypeError, ValueError):
                        continue
                    if (
                        to_addr != actor
                        or not from_addr
                        or from_addr in KNOWN_NON_ACTORS
                        or value <= 0
                    ):
                        continue
                    if oldest is None or block < oldest[0]:
                        oldest = (block, from_addr)
                next_params = payload.get("next_page_params") if isinstance(payload, dict) else None
                if not isinstance(next_params, dict) or not next_params:
                    break
                params = {"filter": "to", **next_params}

        anchor = oldest[1] if oldest is not None else actor
        self._entity_cache[actor] = (anchor, time.monotonic())
        negative.pop(actor, None)
        stats["last_error_type"] = None
        stats["last_error_status"] = None
        if oldest is None:
            stats["resolved_singletons"] = int(stats["resolved_singletons"]) + 1
        else:
            stats["resolved_funding_anchors"] = int(stats["resolved_funding_anchors"]) + 1
        return anchor
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self._entity_resolution_failures += 1
        stats["external_failures"] = int(stats["external_failures"]) + 1
        delay, status = _failure_backoff(exc, prior_count + 1)
        if status == 429:
            stats["rate_limit_failures"] = int(stats["rate_limit_failures"]) + 1
        stats["last_error_type"] = type(exc).__name__
        stats["last_error_status"] = status
        negative[actor] = (
            time.monotonic() + delay,
            type(exc).__name__,
            prior_count + 1,
            status,
        )
        if required:
            return None
        self._entity_cache[actor] = (actor, time.monotonic())
        return actor


async def _entity_anchor(self: Any, actor: str) -> str | None:
    """Resolve an economic entity with deduplication and bounded failure backoff.

    A failed lookup never turns a raw address into independent evidence while
    resolution is required. Successful "no funder found" lookups still preserve the
    historical singleton semantics because the provider explicitly answered.
    """

    actor = _clean_address(actor)
    if not actor:
        return None
    now = time.monotonic()
    cached = self._entity_cache.get(actor)
    if cached is not None and now - float(cached[1]) < ENTITY_POSITIVE_TTL_SECONDS:
        stats = _stats(self)
        stats["positive_cache_hits"] = int(stats["positive_cache_hits"]) + 1
        return str(cached[0])

    negative = _negative_cache(self)
    failed = negative.get(actor)
    if failed is not None and now < float(failed[0]):
        stats = _stats(self)
        stats["negative_cache_hits"] = int(stats["negative_cache_hits"]) + 1
        return None

    inflight = _inflight(self)
    existing = inflight.get(actor)
    if existing is not None and not existing.done():
        stats = _stats(self)
        stats["inflight_dedup_hits"] = int(stats["inflight_dedup_hits"]) + 1
        return await asyncio.shield(existing)

    task = asyncio.create_task(
        _entity_anchor_fetch(self, actor),
        name=f"robinhood-entity-resolve:{actor[:10]}",
    )
    inflight[actor] = task

    def cleanup(done: asyncio.Task[str | None]) -> None:
        if inflight.get(actor) is done:
            inflight.pop(actor, None)

    task.add_done_callback(cleanup)
    return await asyncio.shield(task)


async def _refresh_rwa_registry(self: Any) -> bool:
    """Refresh the documented Robinhood Stock Token registry without retry storms."""

    now = time.monotonic()
    if self._rwa_registry_available and now - self._rwa_registry_last_refresh < RWA_REFRESH_TTL_SECONDS:
        return True
    next_retry = float(getattr(self, "_roi_rwa_registry_next_retry", 0.0) or 0.0)
    if now < next_retry:
        return bool(self._rwa_registry_available)

    explicit: set[str] = set()
    raw = os.getenv("ROBINHOOD_RWA_TOKEN_ADDRESSES_JSON", "").strip()
    if raw:
        try:
            values = json.loads(raw)
            if isinstance(values, list):
                explicit = {_clean_address(str(value)) for value in values}
                explicit.discard("")
        except Exception:
            pass

    try:
        response = await self.rpc.client.get(
            ROBINHOOD_STOCK_ASSETS_API,
            timeout=8.0,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
        candidates: list[Any] = []
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            for key in ("assets", "quotes", "results", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    candidates = value
                    break
        if not isinstance(candidates, list):
            raise RuntimeError("official Robinhood stock-token registry returned no asset list")

        discovered = set(explicit)
        for item in candidates:
            if not isinstance(item, dict):
                continue
            deployments = item.get("deployments") or []
            if not isinstance(deployments, list):
                continue
            for deployment in deployments:
                if not isinstance(deployment, dict):
                    continue
                try:
                    chain_id = int(deployment.get("chainId"))
                except (TypeError, ValueError):
                    continue
                if chain_id != ROBINHOOD_CHAIN_ID:
                    continue
                address = _clean_address(str(deployment.get("contractAddress") or ""))
                if address:
                    discovered.add(address)

        self._rwa_tokens = discovered
        self._rwa_registry_available = True
        self._rwa_registry_last_refresh = now
        self._rwa_registry_error = None
        setattr(self, "_roi_rwa_registry_next_retry", 0.0)
        setattr(self, "_roi_rwa_registry_refresh_successes", int(getattr(self, "_roi_rwa_registry_refresh_successes", 0) or 0) + 1)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self._rwa_tokens.update(explicit)
        self._rwa_registry_error = f"{type(exc).__name__}: official stock-token registry unavailable"
        setattr(self, "_roi_rwa_registry_next_retry", now + RWA_RETRY_SECONDS)
        setattr(self, "_roi_rwa_registry_refresh_failures", int(getattr(self, "_roi_rwa_registry_refresh_failures", 0) or 0) + 1)
        return bool(self._rwa_registry_available)


def _incomplete_metrics(
    self: Any,
    *,
    reason: str,
    unresolved_count: int,
    buy_count: int,
    sell_count: int,
) -> dict[str, Any]:
    stats = _stats(self)
    funnel = _funnel(self)
    funnel["entity_blocked"] = int(funnel["entity_blocked"]) + 1
    funnel["last_rejection_stage"] = reason
    if reason == "trigger_entity_unresolved":
        stats["blocked_trigger_unresolved"] = int(stats["blocked_trigger_unresolved"]) + 1
    elif reason == "deployer_entity_unresolved":
        stats["blocked_deployer_unresolved"] = int(stats["blocked_deployer_unresolved"]) + 1
    return {
        "state": "entity_resolution_incomplete",
        "entity_resolution_complete": False,
        "entity_resolution_partial": unresolved_count > 0,
        "entity_resolution_blocker": reason,
        "unresolved_buy_actor_count": unresolved_count,
        "trigger_actor": "",
        "trigger_entity": "",
        "independent_entities_60s": 0,
        "buy_count_60s": buy_count,
        "sell_count_60s": sell_count,
        "buy_sell_quote_ratio": 0.0,
        "buy_count_acceleration": 0.0,
        "buy_quote_wei": 0,
        "sell_quote_wei": 0,
        "creator_sell_quote_wei": 0,
        "creator_sell_pressure": 0.0,
    }


async def _v5_flow_metrics(self: Any, swaps: Any, *, deployer: str = "") -> dict[str, Any]:
    """Build flow evidence only from actors whose entity identity is authoritative.

    Unresolved non-trigger actors are excluded rather than counted as raw independent
    wallets. A paper decision still fails closed when the triggering buyer or the
    deployer identity cannot be resolved. This prevents one unrelated transient
    lookup failure from zeroing an otherwise fully resolved cohort without weakening
    the anti-sybil/entity boundary.
    """

    funnel = _funnel(self)
    funnel["flow_evaluations"] = int(funnel["flow_evaluations"]) + 1
    now_ts = time.time()
    current = [s for s in swaps if now_ts - float(s.get("observed_ts") or 0.0) <= 60.0]
    prior = [s for s in swaps if 60.0 < now_ts - float(s.get("observed_ts") or 0.0) <= 120.0]
    buys = [s for s in current if s.get("side") == "buy"]
    sells = [s for s in current if s.get("side") == "sell"]
    prior_buys = [s for s in prior if s.get("side") == "buy"]

    actors: list[str] = []
    for swap in buys:
        actor = _clean_address(str(swap.get("actor") or ""))
        if actor and actor not in KNOWN_NON_ACTORS and actor not in actors:
            actors.append(actor)
    actors = actors[-12:]
    anchors = await asyncio.gather(*(self._entity_anchor(actor) for actor in actors)) if actors else []
    mapping = {
        actor: str(anchor)
        for actor, anchor in zip(actors, anchors)
        if anchor
    }
    unresolved = [actor for actor in actors if actor not in mapping]

    trigger_actor = _clean_address(str(buys[-1].get("actor") or "")) if buys else ""
    if trigger_actor and trigger_actor not in KNOWN_NON_ACTORS and trigger_actor not in mapping:
        return _incomplete_metrics(
            self,
            reason="trigger_entity_unresolved",
            unresolved_count=len(unresolved),
            buy_count=len(buys),
            sell_count=len(sells),
        )

    deployer = _clean_address(deployer)
    deployer_anchor = await self._entity_anchor(deployer) if deployer else None
    if deployer and not deployer_anchor:
        return _incomplete_metrics(
            self,
            reason="deployer_entity_unresolved",
            unresolved_count=len(unresolved),
            buy_count=len(buys),
            sell_count=len(sells),
        )

    if unresolved:
        stats = _stats(self)
        stats["partial_contexts"] = int(stats["partial_contexts"]) + 1
        funnel["entity_partial_contexts"] = int(funnel["entity_partial_contexts"]) + 1

    resolved_buys = [
        s
        for s in buys
        if _clean_address(str(s.get("actor") or "")) in mapping
    ]
    trigger_entity = mapping.get(trigger_actor, trigger_actor)
    independent = {
        anchor
        for anchor in mapping.values()
        if anchor and anchor != deployer_anchor
    }
    buy_quote = sum(int(s.get("quote_amount_wei") or 0) for s in resolved_buys)
    sell_quote = sum(int(s.get("quote_amount_wei") or 0) for s in sells)
    creator_sell_quote = sum(
        int(s.get("quote_amount_wei") or 0)
        for s in sells
        if deployer and _clean_address(str(s.get("actor") or "")) == deployer
    )
    ratio = buy_quote / max(1, sell_quote)
    acceleration = len(resolved_buys) / max(1, len(prior_buys))
    prices = [
        float(s["price_eth"])
        for s in current
        if _finite(s.get("price_eth")) not in (None, 0.0)
    ]
    price_change = (
        prices[-1] / prices[0] - 1.0
        if len(prices) >= 2 and prices[0] > 0
        else 0.0
    )

    if (
        len(resolved_buys) >= 4
        and len(independent) >= 3
        and ratio >= 1.5
        and acceleration >= 1.25
        and 0.01 <= price_change <= 0.40
    ):
        state = "active_fomo"
    elif (
        len(resolved_buys) >= 3
        and len(independent) >= 2
        and ratio >= 1.15
        and price_change <= 0.40
    ):
        state = "pre_fomo"
    elif len(independent) >= 2 and buy_quote > sell_quote:
        state = "entity_accumulation"
    elif sells and sell_quote > buy_quote:
        state = "exhaustion"
    else:
        state = "neutral"

    stats = _stats(self)
    stats["decision_ready_contexts"] = int(stats["decision_ready_contexts"]) + 1
    funnel["entity_decision_ready"] = int(funnel["entity_decision_ready"]) + 1
    funnel["last_rejection_stage"] = None
    return {
        "state": state,
        "entity_resolution_complete": True,
        "entity_resolution_partial": bool(unresolved),
        "all_current_buy_entities_resolved": not unresolved,
        "unresolved_buy_actor_count": len(unresolved),
        "unresolved_buy_actors": unresolved,
        "trigger_actor": trigger_actor,
        "trigger_entity": trigger_entity,
        "trigger_is_creator": bool(deployer_anchor and trigger_entity == deployer_anchor),
        "deployer_entity": deployer_anchor or "",
        "independent_entities_60s": len(independent),
        "buy_count_60s": len(resolved_buys),
        "raw_buy_count_60s": len(buys),
        "sell_count_60s": len(sells),
        "buy_sell_quote_ratio": ratio,
        "buy_count_acceleration": acceleration,
        "price_change_60s": price_change,
        "buy_quote_wei": buy_quote,
        "sell_quote_wei": sell_quote,
        "creator_sell_quote_wei": creator_sell_quote,
        "creator_sell_pressure": creator_sell_quote / max(1, buy_quote),
        "unresolved_addresses_count_as_independent": False,
        "unresolved_buy_flow_counts_toward_signal": False,
    }


def _v5_choose_lane_fraction(self: Any, *args: Any, **kwargs: Any) -> Any:
    assert _ORIGINAL_V5_CHOOSE is not None
    funnel = _funnel(self)
    funnel["lane_evaluations"] = int(funnel["lane_evaluations"]) + 1
    result = _ORIGINAL_V5_CHOOSE(self, *args, **kwargs)
    lane, fraction, _profiles = result
    if lane and float(fraction or 0.0) > 0.0:
        funnel["lane_selected"] = int(funnel["lane_selected"]) + 1
    else:
        funnel["lane_rejected"] = int(funnel["lane_rejected"]) + 1
        funnel["last_rejection_stage"] = "lane_or_position_rejected"
    return result


def _v5_insert_trial(self: Any, *args: Any, **kwargs: Any) -> Any:
    assert _ORIGINAL_V5_INSERT is not None
    result = _ORIGINAL_V5_INSERT(self, *args, **kwargs)
    funnel = _funnel(self)
    funnel["paper_trials_opened"] = int(funnel["paper_trials_opened"]) + 1
    funnel["last_rejection_stage"] = None
    return result


async def _maybe_open_v2(self: Any, curve: Any) -> None:
    assert _ORIGINAL_MAYBE_OPEN_V2 is not None
    funnel = _funnel(self)
    funnel["candidate_checks"] = int(funnel["candidate_checks"]) + 1
    funnel["v2_candidate_checks"] = int(funnel["v2_candidate_checks"]) + 1
    if not self._caught_up:
        funnel["not_caught_up"] = int(funnel["not_caught_up"]) + 1
        funnel["last_rejection_stage"] = "not_caught_up"
        return
    if self._token_open(curve.token):
        funnel["already_open"] = int(funnel["already_open"]) + 1
        funnel["last_rejection_stage"] = "already_open"
        return
    before = int(funnel["paper_trials_opened"])
    await _ORIGINAL_MAYBE_OPEN_V2(self, curve)
    if int(funnel["paper_trials_opened"]) == before:
        funnel["v2_no_trial_after_policy"] = int(funnel["v2_no_trial_after_policy"]) + 1
        if funnel.get("last_rejection_stage") is None:
            funnel["last_rejection_stage"] = "v2_policy_or_executable_quote_rejected"


async def _maybe_open_v3(self: Any, pool: Any, *, current_block: int) -> None:
    assert _ORIGINAL_MAYBE_OPEN_V3 is not None
    funnel = _funnel(self)
    funnel["candidate_checks"] = int(funnel["candidate_checks"]) + 1
    funnel["v3_candidate_checks"] = int(funnel["v3_candidate_checks"]) + 1
    if not self._caught_up:
        funnel["not_caught_up"] = int(funnel["not_caught_up"]) + 1
        funnel["last_rejection_stage"] = "not_caught_up"
        return
    if self._token_open(pool.token):
        funnel["already_open"] = int(funnel["already_open"]) + 1
        funnel["last_rejection_stage"] = "already_open"
        return
    before = int(funnel["paper_trials_opened"])
    await _ORIGINAL_MAYBE_OPEN_V3(self, pool, current_block=current_block)
    if int(funnel["paper_trials_opened"]) == before:
        funnel["v3_no_trial_after_policy"] = int(funnel["v3_no_trial_after_policy"]) + 1
        if funnel.get("last_rejection_stage") is None:
            funnel["last_rejection_stage"] = "v3_policy_or_executable_quote_rejected"


def _status(self: Any) -> dict[str, Any]:
    assert _ORIGINAL_STATUS is not None
    payload = _ORIGINAL_STATUS(self)
    now = time.monotonic()
    negative = _negative_cache(self)
    stats = dict(_stats(self))
    active_negative = sum(1 for row in negative.values() if now < float(row[0]))
    entity = payload.setdefault("entity_resolution", {})
    if isinstance(entity, dict):
        entity.update(
            {
                "repair_version": REPAIR_VERSION,
                "method": "blockscout_native_funding_anchor_with_dedup_backoff_and_resolved_subset",
                "max_concurrency": int(getattr(self, "_roi_entity_resolution_max_concurrency", 3) or 3),
                "negative_cached_entities": active_negative,
                "http_requests": int(stats.get("http_requests") or 0),
                "positive_cache_hits": int(stats.get("positive_cache_hits") or 0),
                "negative_cache_hits": int(stats.get("negative_cache_hits") or 0),
                "inflight_dedup_hits": int(stats.get("inflight_dedup_hits") or 0),
                "resolved_funding_anchors": int(stats.get("resolved_funding_anchors") or 0),
                "resolved_singletons": int(stats.get("resolved_singletons") or 0),
                "external_failures_session": int(stats.get("external_failures") or 0),
                "rate_limit_failures_session": int(stats.get("rate_limit_failures") or 0),
                "partial_contexts_session": int(stats.get("partial_contexts") or 0),
                "decision_ready_contexts_session": int(stats.get("decision_ready_contexts") or 0),
                "blocked_trigger_unresolved_session": int(stats.get("blocked_trigger_unresolved") or 0),
                "blocked_deployer_unresolved_session": int(stats.get("blocked_deployer_unresolved") or 0),
                "last_error_type": stats.get("last_error_type"),
                "last_error_status": stats.get("last_error_status"),
                "raw_addresses_count_as_independent_when_resolution_fails": False,
                "unresolved_nontrigger_flow_counts_toward_signal": False,
                "decision_critical_trigger_and_deployer_resolution_required": True,
            }
        )

    rwa = payload.setdefault("rwa_filter", {})
    if isinstance(rwa, dict):
        retry_at = float(getattr(self, "_roi_rwa_registry_next_retry", 0.0) or 0.0)
        rwa.update(
            {
                "official_endpoint": ROBINHOOD_STOCK_ASSETS_API,
                "official_endpoint_verified_contract": "documented_read_only_assets_registry",
                "failure_retry_backoff_seconds": RWA_RETRY_SECONDS,
                "retry_backoff_active": now < retry_at,
                "refresh_successes_session": int(getattr(self, "_roi_rwa_registry_refresh_successes", 0) or 0),
                "refresh_failures_session": int(getattr(self, "_roi_rwa_registry_refresh_failures", 0) or 0),
                "fail_closed_when_registry_unavailable": True,
            }
        )

    funnel = dict(_funnel(self))
    funnel.update(
        {
            "considered_count": int(funnel.get("flow_evaluations") or 0),
            "paper_entry_count": int(funnel.get("paper_trials_opened") or 0),
            "raw_unresolved_addresses_can_authorize_entry": False,
            "strategy_thresholds_changed": False,
            "position_limits_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    )
    payload["decision_funnel"] = funnel
    payload["entity_resolution_repair"] = {
        "repair_version": REPAIR_VERSION,
        "enabled": True,
        "positive_cache_ttl_seconds": ENTITY_POSITIVE_TTL_SECONDS,
        "failure_backoff_initial_seconds": ENTITY_BACKOFF_INITIAL_SECONDS,
        "failure_backoff_max_seconds": ENTITY_BACKOFF_MAX_SECONDS,
        "raw_addresses_count_as_independent": False,
        "trigger_must_resolve": True,
        "deployer_must_resolve_when_present": True,
        "unresolved_other_buyers_excluded_from_signal": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


def install_robinhood_entity_resolution_repair(plane_cls: type[Any]) -> None:
    global _ORIGINAL_V5_CHOOSE
    global _ORIGINAL_V5_INSERT
    global _ORIGINAL_MAYBE_OPEN_V2
    global _ORIGINAL_MAYBE_OPEN_V3
    global _ORIGINAL_STATUS

    if bool(getattr(plane_cls, "_roi_entity_resolution_repair_installed", False)):
        return

    _ORIGINAL_V5_CHOOSE = plane_cls._v5_choose_lane_fraction
    _ORIGINAL_V5_INSERT = plane_cls._v5_insert_trial
    _ORIGINAL_MAYBE_OPEN_V2 = plane_cls._maybe_open_v2
    _ORIGINAL_MAYBE_OPEN_V3 = plane_cls._maybe_open_v3
    _ORIGINAL_STATUS = plane_cls.status

    plane_cls._entity_anchor = _entity_anchor
    plane_cls._refresh_rwa_registry = _refresh_rwa_registry
    plane_cls._v5_flow_metrics = _v5_flow_metrics
    plane_cls._v5_choose_lane_fraction = _v5_choose_lane_fraction
    plane_cls._v5_insert_trial = _v5_insert_trial
    plane_cls._maybe_open_v2 = _maybe_open_v2
    plane_cls._maybe_open_v3 = _maybe_open_v3
    plane_cls.status = _status
    setattr(plane_cls, "_roi_entity_resolution_repair_installed", True)
    setattr(plane_cls, "_roi_entity_resolution_repair_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "install_robinhood_entity_resolution_repair",
]
