from __future__ import annotations

from .robinhood_chain_core import *


class RobinhoodIdentityMixin:
    async def _refresh_rwa_registry(self) -> bool:
        """Refresh the official Robinhood Stock Token deployment set for chain 4663.

        Direct Uniswap discovery is intentionally broad, but this strategy is a crypto/
        memecoin paper lane. Stock Tokens and tokenized ETFs are excluded using the
        official read-only Robinhood asset registry plus an optional explicit denylist.
        """
        now = time.monotonic()
        if self._rwa_registry_available and now - self._rwa_registry_last_refresh < 3600.0:
            return True
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
            response = await self.rpc.client.get(ROBINHOOD_STOCK_ASSETS_API)
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
            return True
        except Exception as exc:
            self._rwa_tokens.update(explicit)
            self._rwa_registry_error = f"{type(exc).__name__}: official stock-token registry unavailable"
            return bool(self._rwa_registry_available)

    async def _direct_v3_token_allowed(self, token: str) -> bool:
        required = os.getenv("ROBINHOOD_RWA_FILTER_REQUIRED", "true").strip().lower() not in {"0", "false", "no"}
        available = await self._refresh_rwa_registry()
        if required and not available:
            return False
        return _clean_address(token) not in self._rwa_tokens

    async def _entity_anchor(self, actor: str) -> str | None:
        """Resolve an effective economic-entity anchor without using tx.from as trader.

        Blockscout address history is used only to collapse obvious same-funder fleets.
        A successful lookup with no visible native funder keeps the actor as a singleton.
        A lookup failure returns None so paper entry can fail closed rather than count raw
        addresses as independent entities.
        """
        actor = _clean_address(actor)
        if not actor:
            return None
        cached = self._entity_cache.get(actor)
        now = time.monotonic()
        if cached is not None and now - cached[1] < 6 * 3600:
            return cached[0]
        required = os.getenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true").strip().lower() not in {"0", "false", "no"}
        try:
            params: dict[str, Any] = {"filter": "to"}
            oldest: tuple[int, str] | None = None
            for _ in range(3):
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
                    to_addr = _clean_address(((tx.get("to") or {}) if isinstance(tx.get("to"), dict) else {}).get("hash"))
                    from_addr = _clean_address(((tx.get("from") or {}) if isinstance(tx.get("from"), dict) else {}).get("hash"))
                    try:
                        value = int(str(tx.get("value") or "0"))
                        block = int(tx.get("block_number") or 0)
                    except (TypeError, ValueError):
                        continue
                    if to_addr != actor or not from_addr or from_addr in KNOWN_NON_ACTORS or value <= 0:
                        continue
                    if oldest is None or block < oldest[0]:
                        oldest = (block, from_addr)
                next_params = payload.get("next_page_params") if isinstance(payload, dict) else None
                if not isinstance(next_params, dict) or not next_params:
                    break
                params = {"filter": "to", **next_params}
            anchor = oldest[1] if oldest is not None else actor
            self._entity_cache[actor] = (anchor, now)
            return anchor
        except Exception:
            self._entity_resolution_failures += 1
            if required:
                return None
            self._entity_cache[actor] = (actor, now)
            return actor

    async def _resolved_metrics(self, swaps: deque[dict[str, Any]]) -> dict[str, Any]:
        now_ts = time.time()
        current = [s for s in swaps if now_ts - float(s["observed_ts"]) <= 60.0]
        actors = []
        for swap in current:
            if swap.get("side") != "buy":
                continue
            actor = _clean_address(str(swap.get("actor") or ""))
            if actor and actor not in KNOWN_NON_ACTORS and actor not in actors:
                actors.append(actor)
        actors = actors[-12:]
        anchors = await asyncio.gather(*(self._entity_anchor(actor) for actor in actors)) if actors else []
        if any(anchor is None for anchor in anchors):
            metrics = self._recent_metrics(swaps, now_ts=now_ts)
            metrics["state"] = "entity_resolution_incomplete"
            metrics["independent_entities_60s"] = 0
            metrics["entity_resolution_complete"] = False
            metrics["trigger_entity"] = ""
            return metrics
        mapping = {actor: str(anchor) for actor, anchor in zip(actors, anchors) if anchor}
        metrics = self._recent_metrics(swaps, now_ts=now_ts, entity_map=mapping)
        trigger_actor = _clean_address(str(metrics.get("trigger_actor") or ""))
        metrics["trigger_entity"] = mapping.get(trigger_actor, trigger_actor)
        metrics["entity_resolution_complete"] = True
        return metrics
