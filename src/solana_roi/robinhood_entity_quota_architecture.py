from __future__ import annotations

import asyncio
import contextvars
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import continuation_market_recalibration as continuation
from . import robinhood_blockscout_pro_repair as blockscout
from . import robinhood_entity_resolution_repair as entity_repair
from .robinhood_chain_core import KNOWN_NON_ACTORS, ROBINHOOD_CHAIN_ID, _clean_address, _finite


REPAIR_VERSION = "robinhood-entity-quota-architecture-v1"
PROOF_VERSION = "blockscout-pro-native-funder-proof-v1"
DEFAULT_DAILY_CREDIT_BUDGET = 100_000
DEFAULT_CREDIT_RESERVE = 20_000
DEFAULT_ASSUMED_CREDITS_PER_REQUEST = 20
_ENTITY_PRIORITY: contextvars.ContextVar[str] = contextvars.ContextVar(
    "robinhood_entity_resolution_priority",
    default="critical",
)
_ORIGINAL_STATUS: Callable[..., Any] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _quota_stats(self: Any) -> dict[str, Any]:
    state = getattr(self, "_roi_entity_quota_stats", None)
    if not isinstance(state, dict):
        state = {
            "durable_cache_hits": 0,
            "durable_cache_writes": 0,
            "durable_cache_errors": 0,
            "provider_requests": 0,
            "critical_provider_requests": 0,
            "noncritical_provider_requests": 0,
            "noncritical_reserve_skips": 0,
            "local_pre_gate_skips": 0,
            "progressive_contexts": 0,
            "progressive_nontrigger_attempts": 0,
            "progressive_nontrigger_resolved": 0,
            "progressive_nontrigger_unresolved": 0,
            "external_requests_avoided": 0,
            "provider_credits_remaining": None,
            "provider_ratelimit_remaining": None,
            "last_provider_credit_header_at": None,
            "last_error_type": None,
        }
        setattr(self, "_roi_entity_quota_stats", state)
    return state


def _store(self: Any) -> Any | None:
    store = getattr(self, "store", None)
    if store is None or not hasattr(store, "db") or not hasattr(store, "_lock"):
        return None
    return store


def _ensure_schema(self: Any) -> bool:
    if bool(getattr(self, "_roi_entity_quota_schema_ready", False)):
        return True
    store = _store(self)
    if store is None:
        return False
    try:
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_entity_proofs ("
                "chain_id INTEGER NOT NULL, actor TEXT NOT NULL, funding_anchor TEXT NOT NULL, "
                "proof_kind TEXT NOT NULL, proof_block INTEGER, proof_tx TEXT, resolver_version TEXT NOT NULL, "
                "resolved_at TEXT NOT NULL, PRIMARY KEY(chain_id,actor,resolver_version))"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_robinhood_entity_proofs_anchor "
                "ON robinhood_entity_proofs(chain_id,funding_anchor,resolver_version)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_entity_credit_usage ("
                "day_utc TEXT PRIMARY KEY, provider_requests INTEGER NOT NULL, "
                "assumed_credits INTEGER NOT NULL, provider_credits_remaining INTEGER, "
                "provider_ratelimit_remaining INTEGER, updated_at TEXT NOT NULL)"
            )
        setattr(self, "_roi_entity_quota_schema_ready", True)
        return True
    except Exception:
        stats = _quota_stats(self)
        stats["durable_cache_errors"] = int(stats["durable_cache_errors"]) + 1
        return False


def _proof_lookup(self: Any, actor: str) -> str | None:
    if not _ensure_schema(self):
        return None
    store = _store(self)
    assert store is not None
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT funding_anchor FROM robinhood_entity_proofs "
                "WHERE chain_id=? AND actor=? AND resolver_version=?",
                (ROBINHOOD_CHAIN_ID, actor, PROOF_VERSION),
            ).fetchone()
        if row is None:
            return None
        anchor = _clean_address(
            str(row["funding_anchor"] if hasattr(row, "keys") else row[0])
        )
        if not anchor:
            return None
        stats = _quota_stats(self)
        stats["durable_cache_hits"] = int(stats["durable_cache_hits"]) + 1
        stats["external_requests_avoided"] = int(stats["external_requests_avoided"]) + 1
        return anchor
    except Exception:
        stats = _quota_stats(self)
        stats["durable_cache_errors"] = int(stats["durable_cache_errors"]) + 1
        return None


def _proof_write(
    self: Any,
    *,
    actor: str,
    anchor: str,
    proof_kind: str,
    proof_block: int | None,
    proof_tx: str | None,
) -> None:
    if not _ensure_schema(self):
        return
    store = _store(self)
    assert store is not None
    try:
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO robinhood_entity_proofs("
                "chain_id,actor,funding_anchor,proof_kind,proof_block,proof_tx,resolver_version,resolved_at"
                ") VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(chain_id,actor,resolver_version) DO UPDATE SET "
                "funding_anchor=excluded.funding_anchor,proof_kind=excluded.proof_kind,"
                "proof_block=excluded.proof_block,proof_tx=excluded.proof_tx,resolved_at=excluded.resolved_at",
                (
                    ROBINHOOD_CHAIN_ID,
                    actor,
                    anchor,
                    proof_kind,
                    proof_block,
                    proof_tx,
                    PROOF_VERSION,
                    _utcnow(),
                ),
            )
        stats = _quota_stats(self)
        stats["durable_cache_writes"] = int(stats["durable_cache_writes"]) + 1
    except Exception:
        stats = _quota_stats(self)
        stats["durable_cache_errors"] = int(stats["durable_cache_errors"]) + 1


def _usage_row(self: Any) -> dict[str, int | None]:
    budget = _int_env(
        "ROBINHOOD_BLOCKSCOUT_DAILY_CREDIT_BUDGET",
        DEFAULT_DAILY_CREDIT_BUDGET,
        minimum=1,
    )
    default = {
        "provider_requests": 0,
        "assumed_credits": 0,
        "provider_credits_remaining": budget,
        "provider_ratelimit_remaining": None,
    }
    if not _ensure_schema(self):
        stats = _quota_stats(self)
        remaining = stats.get("provider_credits_remaining")
        if isinstance(remaining, int):
            default["provider_credits_remaining"] = remaining
        return default
    store = _store(self)
    assert store is not None
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT provider_requests,assumed_credits,provider_credits_remaining,"
                "provider_ratelimit_remaining FROM robinhood_entity_credit_usage WHERE day_utc=?",
                (_utc_day(),),
            ).fetchone()
        if row is None:
            return default
        values = dict(row) if hasattr(row, "keys") else {
            "provider_requests": row[0],
            "assumed_credits": row[1],
            "provider_credits_remaining": row[2],
            "provider_ratelimit_remaining": row[3],
        }
        if values.get("provider_credits_remaining") is None:
            values["provider_credits_remaining"] = max(
                0,
                budget - int(values.get("assumed_credits") or 0),
            )
        return values
    except Exception:
        return default


def _record_provider_usage(self: Any, response: Any) -> None:
    stats = _quota_stats(self)
    assumed = _int_env(
        "ROBINHOOD_BLOCKSCOUT_ASSUMED_CREDITS_PER_REQUEST",
        DEFAULT_ASSUMED_CREDITS_PER_REQUEST,
        minimum=1,
    )
    credits_remaining: int | None = None
    ratelimit_remaining: int | None = None
    headers = getattr(response, "headers", None)
    if headers is not None:
        for name, target in (
            ("x-credits-remaining", "credits"),
            ("x-ratelimit-remaining", "rate"),
        ):
            try:
                raw = headers.get(name)
                parsed = int(str(raw)) if raw is not None else None
            except (TypeError, ValueError, AttributeError):
                parsed = None
            if target == "credits":
                credits_remaining = parsed
            else:
                ratelimit_remaining = parsed
    if credits_remaining is not None:
        stats["provider_credits_remaining"] = credits_remaining
        stats["last_provider_credit_header_at"] = _utcnow()
    if ratelimit_remaining is not None:
        stats["provider_ratelimit_remaining"] = ratelimit_remaining

    if not _ensure_schema(self):
        return
    store = _store(self)
    assert store is not None
    try:
        with store._lock, store.db:
            row = store.db.execute(
                "SELECT provider_requests,assumed_credits,provider_credits_remaining "
                "FROM robinhood_entity_credit_usage WHERE day_utc=?",
                (_utc_day(),),
            ).fetchone()
            requests = int(row[0]) if row is not None else 0
            used = int(row[1]) if row is not None else 0
            previous_remaining = int(row[2]) if row is not None and row[2] is not None else None
            remaining = credits_remaining if credits_remaining is not None else previous_remaining
            store.db.execute(
                "INSERT INTO robinhood_entity_credit_usage("
                "day_utc,provider_requests,assumed_credits,provider_credits_remaining,"
                "provider_ratelimit_remaining,updated_at) VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(day_utc) DO UPDATE SET "
                "provider_requests=excluded.provider_requests,"
                "assumed_credits=excluded.assumed_credits,"
                "provider_credits_remaining=excluded.provider_credits_remaining,"
                "provider_ratelimit_remaining=excluded.provider_ratelimit_remaining,"
                "updated_at=excluded.updated_at",
                (
                    _utc_day(),
                    requests + 1,
                    used + assumed,
                    remaining,
                    ratelimit_remaining,
                    _utcnow(),
                ),
            )
    except Exception:
        stats["durable_cache_errors"] = int(stats["durable_cache_errors"]) + 1


def _provider_request_allowed(self: Any, *, priority: str) -> bool:
    usage = _usage_row(self)
    remaining = int(usage.get("provider_credits_remaining") or 0)
    assumed = _int_env(
        "ROBINHOOD_BLOCKSCOUT_ASSUMED_CREDITS_PER_REQUEST",
        DEFAULT_ASSUMED_CREDITS_PER_REQUEST,
        minimum=1,
    )
    if priority == "critical":
        return remaining >= assumed
    reserve = _int_env(
        "ROBINHOOD_BLOCKSCOUT_CREDIT_RESERVE",
        DEFAULT_CREDIT_RESERVE,
        minimum=0,
    )
    return remaining - assumed >= reserve


async def _entity_anchor_fetch_quota(self: Any, actor: str) -> str | None:
    """Read-through durable entity proof with credit-aware Blockscout fallback."""

    actor = _clean_address(actor)
    if not actor:
        return None
    stats = entity_repair._stats(self)
    quota = _quota_stats(self)
    cached = _proof_lookup(self, actor)
    if cached:
        self._entity_cache[actor] = (cached, time.monotonic())
        stats["last_error_type"] = None
        stats["last_error_status"] = None
        return cached

    negative = entity_repair._negative_cache(self)
    prior = negative.get(actor)
    prior_count = int(prior[2]) if prior is not None else 0
    key = blockscout._api_key()
    required = blockscout._required()
    if not key:
        self._entity_resolution_failures += 1
        stats.setdefault("missing_api_key_failures", 0)
        stats["missing_api_key_failures"] = int(stats["missing_api_key_failures"]) + 1
        stats["last_error_type"] = "BlockscoutApiKeyMissing"
        stats["last_error_status"] = None
        negative[actor] = (
            time.monotonic() + blockscout.MISSING_KEY_BACKOFF_SECONDS,
            "BlockscoutApiKeyMissing",
            prior_count + 1,
            None,
        )
        if required:
            return None
        self._entity_cache[actor] = (actor, time.monotonic())
        return actor

    priority = _ENTITY_PRIORITY.get()
    if not _provider_request_allowed(self, priority=priority):
        quota["last_error_type"] = "EntityCreditReserveProtected"
        if priority != "critical":
            quota["noncritical_reserve_skips"] = int(quota["noncritical_reserve_skips"]) + 1
            return None
        self._entity_resolution_failures += 1
        stats["last_error_type"] = "BlockscoutCreditsExhausted"
        stats["last_error_status"] = None
        negative[actor] = (
            time.monotonic() + 60.0,
            "BlockscoutCreditsExhausted",
            prior_count + 1,
            None,
        )
        return None

    params: dict[str, Any] = {
        "chain_id": ROBINHOOD_CHAIN_ID,
        "module": "account",
        "action": "txlist",
        "address": actor,
        "startblock": 0,
        "endblock": 999999999,
        "page": 1,
        "offset": blockscout.TXLIST_OFFSET,
        "sort": "asc",
        "filterby": "to",
        "apikey": key,
    }
    try:
        async with entity_repair._entity_semaphore(self):
            stats["http_requests"] = int(stats["http_requests"]) + 1
            stats.setdefault("pro_api_requests", 0)
            stats["pro_api_requests"] = int(stats["pro_api_requests"]) + 1
            quota["provider_requests"] = int(quota["provider_requests"]) + 1
            if priority == "critical":
                quota["critical_provider_requests"] = int(quota["critical_provider_requests"]) + 1
            else:
                quota["noncritical_provider_requests"] = int(quota["noncritical_provider_requests"]) + 1
            response = await self.rpc.client.get(
                blockscout._api_url(),
                params=params,
                timeout=3.5,
                headers={"Accept": "application/json"},
            )
            _record_provider_usage(self, response)
            response.raise_for_status()
            payload = response.json()

        result = payload.get("result") if isinstance(payload, dict) else None
        message = str(payload.get("message") or "") if isinstance(payload, dict) else ""
        if isinstance(result, str):
            lowered = f"{message} {result}".lower()
            if "no transactions" in lowered:
                result = []
            else:
                stats.setdefault("pro_api_parse_failures", 0)
                stats["pro_api_parse_failures"] = int(stats["pro_api_parse_failures"]) + 1
                raise RuntimeError("Blockscout Pro txlist returned a non-list result")
        if not isinstance(result, list):
            stats.setdefault("pro_api_parse_failures", 0)
            stats["pro_api_parse_failures"] = int(stats["pro_api_parse_failures"]) + 1
            raise RuntimeError("Blockscout Pro txlist returned no transaction list")

        anchor: str | None = None
        proof_block: int | None = None
        proof_tx: str | None = None
        for tx in result:
            if not isinstance(tx, dict):
                continue
            to_addr = _clean_address(str(tx.get("to") or ""))
            from_addr = _clean_address(str(tx.get("from") or ""))
            try:
                value = int(str(tx.get("value") or "0"))
            except (TypeError, ValueError):
                continue
            if (
                to_addr == actor
                and from_addr
                and from_addr not in KNOWN_NON_ACTORS
                and value > 0
            ):
                anchor = from_addr
                try:
                    proof_block = int(str(tx.get("blockNumber") or tx.get("block_number") or "0")) or None
                except (TypeError, ValueError):
                    proof_block = None
                raw_hash = str(tx.get("hash") or tx.get("transaction_hash") or "")
                proof_tx = raw_hash if raw_hash.startswith("0x") else None
                break

        resolved = anchor or actor
        self._entity_cache[actor] = (resolved, time.monotonic())
        negative.pop(actor, None)
        stats["last_error_type"] = None
        stats["last_error_status"] = None
        if anchor is None:
            stats["resolved_singletons"] = int(stats["resolved_singletons"]) + 1
            proof_kind = "authoritative_no_prior_native_funder"
        else:
            stats["resolved_funding_anchors"] = int(stats["resolved_funding_anchors"]) + 1
            proof_kind = "earliest_inbound_native_funder"
        _proof_write(
            self,
            actor=actor,
            anchor=resolved,
            proof_kind=proof_kind,
            proof_block=proof_block,
            proof_tx=proof_tx,
        )
        return resolved
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self._entity_resolution_failures += 1
        stats["external_failures"] = int(stats["external_failures"]) + 1
        delay, http_status = entity_repair._failure_backoff(exc, prior_count + 1)
        if http_status == 429:
            stats["rate_limit_failures"] = int(stats["rate_limit_failures"]) + 1
        stats["last_error_type"] = type(exc).__name__
        stats["last_error_status"] = http_status
        negative[actor] = (
            time.monotonic() + delay,
            type(exc).__name__,
            prior_count + 1,
            http_status,
        )
        if required:
            return None
        self._entity_cache[actor] = (actor, time.monotonic())
        return actor


async def _resolve_with_priority(self: Any, actor: str, *, priority: str) -> str | None:
    token = _ENTITY_PRIORITY.set(priority)
    try:
        return await self._entity_anchor(actor)
    finally:
        _ENTITY_PRIORITY.reset(token)


def _incomplete(
    self: Any,
    *,
    reason: str,
    unresolved_count: int,
    buy_count: int,
    sell_count: int,
) -> dict[str, Any]:
    return entity_repair._incomplete_metrics(
        self,
        reason=reason,
        unresolved_count=unresolved_count,
        buy_count=buy_count,
        sell_count=sell_count,
    )


async def _v5_flow_metrics_quota(self: Any, swaps: Any, *, deployer: str = "") -> dict[str, Any]:
    """Resolve critical identities first, then enrich non-triggers progressively.

    No token, venue, lifecycle, or candidate universe is removed. Trigger and deployer
    identities remain fail-closed. Other buyers are best-effort evidence: known/durable
    identities are free, while new provider lookups yield before the protected credit
    reserve and their unresolved flow cannot count toward a signal.
    """

    funnel = entity_repair._funnel(self)
    funnel["flow_evaluations"] = int(funnel["flow_evaluations"]) + 1
    quota = _quota_stats(self)
    now_ts = time.time()
    current = [s for s in swaps if now_ts - float(s.get("observed_ts") or 0.0) <= 60.0]
    prior = [s for s in swaps if 60.0 < now_ts - float(s.get("observed_ts") or 0.0) <= 120.0]
    buys = [s for s in current if s.get("side") == "buy"]
    sells = [s for s in current if s.get("side") == "sell"]
    prior_buys = [s for s in prior if s.get("side") == "buy"]

    trigger_actor = _clean_address(str(buys[-1].get("actor") or "")) if buys else ""
    if not trigger_actor or trigger_actor in KNOWN_NON_ACTORS:
        quota["local_pre_gate_skips"] = int(quota["local_pre_gate_skips"]) + 1
        funnel["last_rejection_stage"] = "no_decision_critical_buy_trigger"
        return {
            "state": "neutral",
            "entity_resolution_complete": True,
            "entity_resolution_partial": False,
            "all_current_buy_entities_resolved": True,
            "unresolved_buy_actor_count": 0,
            "unresolved_buy_actors": [],
            "trigger_actor": "",
            "trigger_entity": "",
            "trigger_is_creator": False,
            "deployer_entity": "",
            "independent_entities_60s": 0,
            "buy_count_60s": 0,
            "raw_buy_count_60s": len(buys),
            "sell_count_60s": len(sells),
            "buy_sell_quote_ratio": 0.0,
            "buy_count_acceleration": 0.0,
            "price_change_60s": 0.0,
            "buy_quote_wei": 0,
            "sell_quote_wei": sum(int(s.get("quote_amount_wei") or 0) for s in sells),
            "creator_sell_quote_wei": 0,
            "creator_sell_pressure": 0.0,
            "unresolved_addresses_count_as_independent": False,
            "unresolved_buy_flow_counts_toward_signal": False,
            "entity_resolution_pre_gate": "no_valid_buy_trigger",
        }

    actors: list[str] = []
    for swap in buys:
        actor = _clean_address(str(swap.get("actor") or ""))
        if actor and actor not in KNOWN_NON_ACTORS and actor not in actors:
            actors.append(actor)
    actors = actors[-12:]
    if trigger_actor not in actors:
        actors.append(trigger_actor)

    mapping: dict[str, str] = {}
    trigger_anchor = await _resolve_with_priority(self, trigger_actor, priority="critical")
    if not trigger_anchor:
        unresolved = [actor for actor in actors if actor != trigger_actor]
        return _incomplete(
            self,
            reason="trigger_entity_unresolved",
            unresolved_count=1 + len(unresolved),
            buy_count=len(buys),
            sell_count=len(sells),
        )
    mapping[trigger_actor] = str(trigger_anchor)

    deployer = _clean_address(deployer)
    if deployer:
        deployer_anchor = await _resolve_with_priority(self, deployer, priority="critical")
        if not deployer_anchor:
            unresolved = [actor for actor in actors if actor not in mapping]
            return _incomplete(
                self,
                reason="deployer_entity_unresolved",
                unresolved_count=len(unresolved),
                buy_count=len(buys),
                sell_count=len(sells),
            )
    else:
        deployer_anchor = None

    remaining = [actor for actor in actors if actor not in mapping]
    if remaining:
        quota["progressive_contexts"] = int(quota["progressive_contexts"]) + 1

    for actor in remaining:
        quota["progressive_nontrigger_attempts"] = int(quota["progressive_nontrigger_attempts"]) + 1
        anchor = await _resolve_with_priority(self, actor, priority="noncritical")
        if anchor:
            mapping[actor] = str(anchor)
            quota["progressive_nontrigger_resolved"] = int(quota["progressive_nontrigger_resolved"]) + 1
        else:
            quota["progressive_nontrigger_unresolved"] = int(quota["progressive_nontrigger_unresolved"]) + 1
            if not _provider_request_allowed(self, priority="noncritical"):
                break

    unresolved = [actor for actor in actors if actor not in mapping]
    if unresolved:
        stats = entity_repair._stats(self)
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

    stats = entity_repair._stats(self)
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
        "entity_resolution_order": "trigger_then_deployer_then_nontrigger_progressive",
        "noncritical_enrichment_respects_credit_reserve": True,
    }


def _status_with_quota(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        quota = dict(_quota_stats(self))
        usage = _usage_row(self)
        proof_rows = 0
        if _ensure_schema(self):
            store = _store(self)
            assert store is not None
            try:
                with store._lock:
                    proof_rows = int(
                        store.db.execute(
                            "SELECT COUNT(*) FROM robinhood_entity_proofs "
                            "WHERE chain_id=? AND resolver_version=?",
                            (ROBINHOOD_CHAIN_ID, PROOF_VERSION),
                        ).fetchone()[0]
                    )
            except Exception:
                proof_rows = 0
        budget = _int_env(
            "ROBINHOOD_BLOCKSCOUT_DAILY_CREDIT_BUDGET",
            DEFAULT_DAILY_CREDIT_BUDGET,
            minimum=1,
        )
        reserve = _int_env(
            "ROBINHOOD_BLOCKSCOUT_CREDIT_RESERVE",
            DEFAULT_CREDIT_RESERVE,
            minimum=0,
        )
        entity = payload.setdefault("entity_resolution", {})
        if isinstance(entity, dict):
            entity.update(
                {
                    "durable_chain_actor_proof_cache": True,
                    "durable_proof_rows": proof_rows,
                    "durable_proof_version": PROOF_VERSION,
                    "durable_proofs_release_scoped": False,
                    "durable_proofs_expire_on_time": False,
                    "durable_cache_hits_session": int(quota.get("durable_cache_hits") or 0),
                    "external_requests_avoided_session": int(quota.get("external_requests_avoided") or 0),
                    "provider_requests_session": int(quota.get("provider_requests") or 0),
                    "critical_provider_requests_session": int(quota.get("critical_provider_requests") or 0),
                    "noncritical_provider_requests_session": int(quota.get("noncritical_provider_requests") or 0),
                    "noncritical_reserve_skips_session": int(quota.get("noncritical_reserve_skips") or 0),
                    "provider_credits_remaining": quota.get("provider_credits_remaining")
                    if quota.get("provider_credits_remaining") is not None
                    else usage.get("provider_credits_remaining"),
                    "provider_ratelimit_remaining": quota.get("provider_ratelimit_remaining"),
                    "daily_credit_budget": budget,
                    "protected_credit_reserve": reserve,
                    "daily_provider_requests_recorded": int(usage.get("provider_requests") or 0),
                    "daily_assumed_credits_recorded": int(usage.get("assumed_credits") or 0),
                    "progressive_resolution": True,
                    "resolution_order": "trigger_then_deployer_then_nontrigger",
                    "trigger_and_deployer_use_protected_critical_budget": True,
                    "nontrigger_enrichment_yields_at_credit_reserve": True,
                    "raw_unresolved_addresses_can_authorize_entry": False,
                }
            )
        payload["entity_quota_architecture"] = {
            "repair_version": REPAIR_VERSION,
            "enabled": True,
            "persistent_cache_scope": "robinhood_chain_x_actor_across_tokens_venues_regimes_releases",
            "provider_call_policy": "read_through_cache_miss_only",
            "local_pre_gate": "no_valid_buy_trigger_requires_zero_entity_calls",
            "progressive_resolution": "decision_critical_first_then_best_effort_nontrigger",
            "credit_aware": True,
            "credit_header": "x-credits-remaining",
            "daily_credit_budget": budget,
            "protected_credit_reserve": reserve,
            "universe_scope_reduced": False,
            "token_scope_reduced": False,
            "venue_scope_reduced": False,
            "strategy_thresholds_changed": False,
            "position_limits_changed": False,
            "trigger_resolution_required": True,
            "deployer_resolution_required_when_present": True,
            "unresolved_nontrigger_flow_counts_toward_signal": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_entity_quota_architecture", True)
    return status


def install_robinhood_entity_quota_architecture(plane_cls: type[Any]) -> None:
    """Install quota preservation beneath existing continuation-first policy authority."""

    global _ORIGINAL_STATUS
    if bool(getattr(plane_cls, "_roi_entity_quota_architecture_installed", False)):
        return

    entity_repair._entity_anchor_fetch = _entity_anchor_fetch_quota

    if bool(getattr(continuation, "_INSTALLED", False)):
        if continuation._ORIGINAL_RH_FLOW is None:
            raise RuntimeError("Robinhood continuation flow substrate is unavailable")
        continuation._ORIGINAL_RH_FLOW = _v5_flow_metrics_quota
        plane_cls._v5_flow_metrics = continuation._rh_flow_without_sniper_cap
    else:
        plane_cls._v5_flow_metrics = _v5_flow_metrics_quota

    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_entity_quota_architecture", False)):
        _ORIGINAL_STATUS = current_status
        plane_cls.status = _status_with_quota(current_status)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_entity_quota_architecture_installed", True)
    setattr(plane_cls, "_roi_entity_quota_architecture_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "PROOF_VERSION",
    "DEFAULT_DAILY_CREDIT_BUDGET",
    "DEFAULT_CREDIT_RESERVE",
    "_entity_anchor_fetch_quota",
    "_v5_flow_metrics_quota",
    "_resolve_with_priority",
    "install_robinhood_entity_quota_architecture",
]
