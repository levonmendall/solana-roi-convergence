from __future__ import annotations

from .robinhood_chain_core import *


class RobinhoodRuntimeMixin:
    async def _poll_once(self) -> None:
        self._last_poll_at = _utcnow()
        chain_id = await self.rpc.chain_id()
        if chain_id != ROBINHOOD_CHAIN_ID:
            raise RuntimeError(f"wrong Robinhood chain id: {chain_id}")
        latest = await self.rpc.block_number()
        self._latest_block = latest
        if self._cursor is None:
            lookback = max(10, int(os.getenv("ROBINHOOD_BOOTSTRAP_BLOCK_LOOKBACK", "1200")))
            self._cursor = max(PONS_V1_LEGACY_START_BLOCK, latest - lookback)
        if self._cursor >= latest:
            self._caught_up = True
            await self._settle_open_positions()
            self._last_success_at = _utcnow()
            return
        to_block = min(latest, self._cursor + MAX_BLOCKS_PER_POLL)
        live = latest - to_block <= LIVE_LAG_BLOCKS
        observed_at = _utcnow()
        factory_logs = await self.rpc.get_logs(
            from_block=self._cursor + 1,
            to_block=to_block,
            addresses=[
                UNISWAP_V3_FACTORY,
                PONS_V1_ACTIVE_FACTORY,
                PONS_V1_LEGACY_FACTORY,
                PONS_V2_FACTORY,
            ],
        )
        for log in factory_logs:
            await self._process_factory_log(log)

        pools = list(self.v3_pools.values())
        for index in range(0, len(pools), 32):
            batch = pools[index : index + 32]
            logs = await self.rpc.get_logs(
                from_block=self._cursor + 1,
                to_block=to_block,
                addresses=[pool.pool for pool in batch],
                topics=[V3_SWAP_TOPIC],
            )
            by_market = {pool.pool: pool for pool in batch}
            for log in logs:
                pool = by_market.get(_clean_address(log.get("address")))
                if pool is not None:
                    await self._process_v3_swap(pool, log, live=live, observed_at=observed_at)

        curves = list(self.v2_curves.values())
        for index in range(0, len(curves), 32):
            batch2 = curves[index : index + 32]
            logs = await self.rpc.get_logs(
                from_block=self._cursor + 1,
                to_block=to_block,
                addresses=[curve.curve for curve in batch2],
                topics=[[PONS_V2_CURVE_BUY_TOPIC, PONS_V2_CURVE_SELL_TOPIC]],
            )
            by_curve = {curve.curve: curve for curve in batch2}
            for log in logs:
                curve = by_curve.get(_clean_address(log.get("address")))
                if curve is not None:
                    await self._process_v2_curve_log(curve, log, live=live, observed_at=observed_at)

        self._set_cursor(to_block)
        self._caught_up = latest - to_block <= LIVE_LAG_BLOCKS
        if self._caught_up:
            await self._settle_open_positions()
        self._last_success_at = _utcnow()
        self._last_error = None

    async def run(self, stop: asyncio.Event) -> None:
        if not self.enabled:
            return
        while not stop.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._rpc_failures += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass

    def _profitability_segments(self) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT venue,lifecycle,net_return FROM robinhood_paper_outcomes "
                "WHERE release_commit=? ORDER BY id",
                (self.release_commit,),
            ).fetchall()
        grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
        for row in rows:
            grouped[(str(row["venue"]), str(row["lifecycle"]))].append(float(row["net_return"]))
        result = []
        for (venue, lifecycle), values in sorted(grouped.items()):
            result.append(
                {
                    "venue": venue,
                    "lifecycle": lifecycle,
                    "sample_count": len(values),
                    "mean_roi_pct": mean(values) * 100.0,
                    "median_roi_pct": median(values) * 100.0,
                    "trimmed_mean_roi_ex_best_1_pct": (
                        _trimmed_ex_best(values, 1) * 100.0
                        if _trimmed_ex_best(values, 1) is not None
                        else None
                    ),
                    "positive_rate_pct": sum(v > 0 for v in values) / len(values) * 100.0,
                }
            )
        return result

    def _wallet_contexts(self) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT trigger_actor,trigger_entity,venue,lifecycle,net_return FROM robinhood_paper_outcomes "
                "WHERE release_commit=? ORDER BY id",
                (self.release_commit,),
            ).fetchall()
        grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        for row in rows:
            grouped[(str(row["trigger_entity"]), str(row["venue"]), str(row["lifecycle"]))].append(float(row["net_return"]))
        contexts = []
        for (entity, venue, lifecycle), values in grouped.items():
            contexts.append(
                {
                    "wallet_or_effective_entity": entity,
                    "venue": venue,
                    "lifecycle": lifecycle,
                    "role": "momentum_alpha",
                    **classify_context_returns(values),
                    "cross_chain_success_transfer_allowed": False,
                    "cross_venue_success_transfer_allowed": False,
                }
            )
        contexts.sort(
            key=lambda row: (
                1 if row["state"] == "promoted_paper_context" else 0,
                row["trimmed_mean_roi_ex_best_1_pct"]
                if row["trimmed_mean_roi_ex_best_1_pct"] is not None
                else float("-inf"),
                row["sample_count"],
            ),
            reverse=True,
        )
        return contexts[:50]

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            trials = self.store.db.execute(
                "SELECT COUNT(*) AS n FROM robinhood_paper_trials WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()
            outcomes = self.store.db.execute(
                "SELECT COUNT(*) AS n FROM robinhood_paper_outcomes WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()
            swaps = self.store.db.execute(
                "SELECT COUNT(*) AS n FROM robinhood_swaps WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()
        return {
            "enabled": self.enabled,
            "chain": "ROBINHOOD_CHAIN",
            "chain_id": ROBINHOOD_CHAIN_ID,
            "strategy_version": ROBINHOOD_CHAIN_PAPER_VERSION,
            "paper_only": True,
            "paper_trading_authority": True,
            "shadow_only": False,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "portfolio_mode": "isolated_robinhood_chain_paper_sleeve",
            "starting_paper_nav_usd": self.starting_nav_usd,
            "paper_nav_usd": self._paper_nav_usd(),
            "open_exposure_fraction": self._open_exposure(),
            "max_open_exposure_fraction": MAX_OPEN_EXPOSURE_FRACTION,
            "max_position_fraction": MAX_POSITION_FRACTION,
            "bootstrap_position_fraction": BOOTSTRAP_PAPER_FRACTION,
            "cursor_block": self._cursor,
            "latest_block": self._latest_block,
            "caught_up_for_paper_decisions": self._caught_up,
            "last_poll_at": self._last_poll_at,
            "last_success_at": self._last_success_at,
            "last_error": self._last_error,
            "rpc_failures": self._rpc_failures,
            "rwa_filter": {
                "required": os.getenv("ROBINHOOD_RWA_FILTER_REQUIRED", "true").strip().lower() not in {"0", "false", "no"},
                "official_registry_available": self._rwa_registry_available,
                "excluded_stock_token_count": len(self._rwa_tokens),
                "last_error": self._rwa_registry_error,
                "direct_v3_stock_tokens_paper_eligible": False,
            },
            "entity_resolution": {
                "required": os.getenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true").strip().lower() not in {"0", "false", "no"},
                "method": "blockscout_native_funding_anchor_plus_effective_event_actor",
                "cached_entities": len(self._entity_cache),
                "failures": self._entity_resolution_failures,
                "raw_addresses_count_as_independent_when_resolution_fails": False,
            },
            "rpc_endpoint_kind": "configured_provider" if os.getenv("ROBINHOOD_RPC_URL") else "official_public_rate_limited",
            "tracked_v3_pools": len(self.v3_pools),
            "tracked_pons_v2_curves": len(self.v2_curves),
            "swap_observations": int(swaps["n"] if swaps is not None else 0),
            "paper_trials": int(trials["n"] if trials is not None else 0),
            "paper_outcomes": int(outcomes["n"] if outcomes is not None else 0),
            "profitability_by_venue_lifecycle": self._profitability_segments(),
            "wallet_contexts": self._wallet_contexts(),
            "wallet_authority_key": "chain_x_economic_entity_x_venue_x_lifecycle_x_role",
            "cross_chain_success_transfer_allowed": False,
            "cross_venue_success_transfer_allowed": False,
            "historical_evidence_promotion_authority": False,
            "actor_attribution": {
                "uniswap_v3": "swap_recipient_not_tx_from",
                "pons_v2_curve": "curve_buyer_or_seller",
                "erc4337_bundler_tx_from_never_used_as_independent_confirmation": True,
            },
            "venues": {
                "uniswap_v3_direct": {
                    "paper_authority": True,
                    "discovery": "chain_wide_weth_pool_created",
                    "quote": "amount_specific_uniswap_v3_quoter_v2_eth_call",
                },
                "pons_v1_uniswap_v3": {
                    "paper_authority": True,
                    "launch_status": "historical_or_if_factory_resumes",
                    "launch_protection_respected": True,
                },
                "pons_v2_curve": {
                    "paper_authority": True,
                    "quote": "deterministic_onchain_reserves_fee_creator_tax_and_snipe_tax",
                    "snipe_tax_max_for_entry_bps": 500,
                    "near_graduation_entry_cutoff": 0.85,
                },
                "pons_v2_uniswap_v4": {
                    "paper_authority": "exit_routing_for_curve_positions",
                    "new_entry_authority": False,
                    "quote": "uniswap_v4_quoter_eth_call",
                },
            },
            "risk_boundaries": {
                "max_chase_fraction": MAX_CHASE_FRACTION,
                "max_immediate_round_trip_cost_fraction": MAX_IMMEDIATE_ROUND_TRIP_COST,
                "stop_loss_fraction": STOP_LOSS_FRACTION,
                "harvest_fraction": HARVEST_FRACTION,
                "max_hold_seconds": MAX_HOLD_SECONDS,
                "requires_real_forward_chain_observation": True,
                "requires_exact_executable_buy_and_sell_quote": True,
                "paper_trading_is_forward_validation": True,
            },
        }
