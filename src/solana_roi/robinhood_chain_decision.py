from __future__ import annotations

from .robinhood_chain_core import *
from .v51_atomic_paper_capital import cancel_paper_capital, reserve_paper_capital


class RobinhoodDecisionMixin:
    def _paper_decision_transport_ready(self) -> bool:
        """Separate current live-entry readiness from historical backfill completeness."""
        if bool(getattr(self, "_roi_live_epoch_suppress_entries", False)):
            return False
        if getattr(self, "_roi_live_epoch_cursor", None) is not None:
            return bool(getattr(self, "_roi_live_epoch_ready", False))
        return bool(self._caught_up)

    async def _quote_v3_round_trip(self, pool: V3Pool, fraction: float) -> dict[str, Any] | None:
        eth_usd = await self._eth_usd()
        if eth_usd is None or eth_usd <= 0:
            return None
        nav = self._paper_nav_usd()
        usd = nav * fraction
        amount_in = int((usd / eth_usd) * 1e18)
        if amount_in <= 0:
            return None
        token_out, entry_gas = await self.rpc.v3_quote_exact_input(
            token_in=WETH,
            token_out=pool.token,
            fee=pool.fee,
            amount_in=amount_in,
        )
        if token_out <= 0:
            return None
        exit_out, exit_gas = await self.rpc.v3_quote_exact_input(
            token_in=pool.token,
            token_out=WETH,
            fee=pool.fee,
            amount_in=token_out,
        )
        gas_price = await self.rpc.gas_price()
        entry_gas_wei = (entry_gas + 80_000) * gas_price
        exit_gas_wei = (exit_gas + 80_000) * gas_price
        total_cost = amount_in + entry_gas_wei
        immediate_net = max(0, exit_out - exit_gas_wei)
        round_trip = 1.0 - immediate_net / max(1, total_cost)
        return {
            "amount_in_wei": amount_in,
            "token_out": token_out,
            "entry_gas_wei": entry_gas_wei,
            "exit_gas_wei": exit_gas_wei,
            "entry_total_cost_wei": total_cost,
            "immediate_exit_wei": immediate_net,
            "round_trip_cost_fraction": round_trip,
            "entry_price_eth": (amount_in / 1e18) / (token_out / (10 ** pool.token_decimals)),
        }

    async def _maybe_open_v3(self, pool: V3Pool, *, current_block: int) -> None:
        if not self._paper_decision_transport_ready() or self._token_open(pool.token):
            return
        if pool.restrictions_end_block and current_block <= pool.restrictions_end_block:
            return
        if pool.venue == "UNISWAP_V3_DIRECT" and not await self._direct_v3_token_allowed(pool.token):
            return
        raw_metrics = self._recent_metrics(pool.recent_swaps, now_ts=time.time())
        if raw_metrics["state"] not in {"pre_fomo", "active_fomo"}:
            return
        metrics = await self._resolved_metrics(pool.recent_swaps)
        if metrics["state"] not in {"pre_fomo", "active_fomo"}:
            return
        if pool.first_price_eth and pool.recent_swaps:
            latest_price = _finite(pool.recent_swaps[-1].get("price_eth"))
            if latest_price is not None and pool.first_price_eth > 0:
                chase = latest_price / pool.first_price_eth - 1.0
                if chase > MAX_CHASE_FRACTION:
                    return
        actor = _clean_address(metrics["trigger_actor"])
        entity = _clean_address(metrics.get("trigger_entity"))
        if not actor or not entity or actor in KNOWN_NON_ACTORS or actor == pool.deployer:
            return
        lifecycle = "post_protection_v3" if pool.venue == "PONS_V1_UNISWAP_V3" else "new_weth_pool"
        fraction, profile = self._position_fraction(entity, pool.venue, lifecycle)
        if fraction <= 0:
            return
        if self._open_exposure() + fraction > MAX_OPEN_EXPOSURE_FRACTION:
            return
        try:
            quote = await self._quote_v3_round_trip(pool, fraction)
        except Exception:
            return
        if quote is None or quote["round_trip_cost_fraction"] > MAX_IMMEDIATE_ROUND_TRIP_COST:
            return
        self._insert_trial(
            token=pool.token,
            market=pool.pool,
            venue=pool.venue,
            lifecycle=lifecycle,
            trigger_actor=actor,
            trigger_entity=entity,
            fomo_state=str(metrics["state"]),
            context_state=str(profile["state"]),
            fraction=fraction,
            quote=quote,
            decision_reason="real_chain_flow_plus_exact_v3_round_trip_quote",
        )

    async def _maybe_open_v2(self, curve: V2Curve) -> None:
        if not self._paper_decision_transport_ready() or self._token_open(curve.token):
            return
        raw_metrics = self._recent_metrics(curve.recent_swaps, now_ts=time.time())
        if raw_metrics["state"] not in {"pre_fomo", "active_fomo"}:
            return
        metrics = await self._resolved_metrics(curve.recent_swaps)
        if metrics["state"] not in {"pre_fomo", "active_fomo"}:
            return
        actor = _clean_address(metrics["trigger_actor"])
        entity = _clean_address(metrics.get("trigger_entity"))
        if not actor or not entity or actor in KNOWN_NON_ACTORS or actor == curve.deployer:
            return
        fraction, profile = self._position_fraction(entity, "PONS_V2_CURVE", "bonding_curve")
        if fraction <= 0 or self._open_exposure() + fraction > MAX_OPEN_EXPOSURE_FRACTION:
            return
        eth_usd = await self._eth_usd()
        if eth_usd is None or eth_usd <= 0:
            return
        amount_in = int((self._paper_nav_usd() * fraction / eth_usd) * 1e18)
        if amount_in <= 0:
            return
        try:
            state = await self.rpc.pons_v2_launch_state(curve.token)
            if int(state["phase"]) != 0:
                return
            real_quote = await self.rpc.call_uint(curve.curve, "realQuoteReserve()")
            threshold = max(1, int(state["graduation_threshold"] or curve.graduation_threshold))
            progress = real_quote / threshold
            if progress >= 0.85:
                return
            buy = await self.rpc.pons_v2_curve_quote(
                curve=curve.curve,
                quote_in=amount_in,
                recipient=self.paper_recipient,
            )
            if buy["snipe_tax_bps"] > 500 or buy["tokens_out"] <= 0:
                return
            exit_out = await self.rpc.pons_v2_curve_sell_quote(
                curve=curve.curve,
                tokens_in=buy["tokens_out"],
            )
            gas_price = await self.rpc.gas_price()
            entry_gas_wei = 220_000 * gas_price
            exit_gas_wei = 220_000 * gas_price
            total_cost = buy["spent"] + entry_gas_wei
            immediate_net = max(0, exit_out - exit_gas_wei)
            round_trip = 1.0 - immediate_net / max(1, total_cost)
            if round_trip > MAX_IMMEDIATE_ROUND_TRIP_COST:
                return
            quote = {
                "amount_in_wei": buy["spent"],
                "token_out": buy["tokens_out"],
                "entry_gas_wei": entry_gas_wei,
                "exit_gas_wei": exit_gas_wei,
                "entry_total_cost_wei": total_cost,
                "immediate_exit_wei": immediate_net,
                "round_trip_cost_fraction": round_trip,
                "entry_price_eth": (buy["spent"] / 1e18) / (buy["tokens_out"] / 1e18),
            }
        except Exception:
            return
        self._insert_trial(
            token=curve.token,
            market=curve.curve,
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            trigger_actor=actor,
            trigger_entity=entity,
            fomo_state=str(metrics["state"]),
            context_state=str(profile["state"]),
            fraction=fraction,
            quote=quote,
            decision_reason="real_chain_flow_plus_exact_pons_v2_curve_quote",
        )

    def _insert_trial(
        self,
        *,
        token: str,
        market: str,
        venue: str,
        lifecycle: str,
        trigger_actor: str,
        trigger_entity: str,
        fomo_state: str,
        context_state: str,
        fraction: float,
        quote: dict[str, Any],
        decision_reason: str,
    ) -> bool:
        reservation_id = f"robinhood:{_clean_address(token)}:{_clean_address(market)}"
        reservation = reserve_paper_capital(
            self.store,
            release_commit=self.release_commit,
            reservation_id=reservation_id,
            lane="robinhood",
            candidate_id=f"{_clean_address(token)}:{_clean_address(market)}",
            requested_fraction=float(fraction),
            capacity_fraction=MAX_OPEN_EXPOSURE_FRACTION,
            allow_downsize=False,
            minimum_fraction=float(fraction),
        )
        if str(reservation.get("status")) != "active" or float(reservation.get("reserved_fraction") or 0.0) + 1e-12 < float(fraction):
            return False
        try:
            with self.store._lock, self.store.db:
                cursor = self.store.db.execute(
                    "INSERT OR IGNORE INTO robinhood_paper_trials("
                    "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
                    "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,"
                    "entry_price_eth,entry_round_trip_cost_fraction,opened_at,decision_reason,capital_reservation_id,"
                    "paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                    (
                        self.release_commit,
                        ROBINHOOD_CHAIN_PAPER_VERSION,
                        token,
                        market,
                        venue,
                        lifecycle,
                        trigger_actor,
                        trigger_entity,
                        fomo_state,
                        context_state,
                        float(fraction),
                        str(int(quote["amount_in_wei"])),
                        str(int(quote["token_out"])),
                        str(int(quote["entry_gas_wei"])),
                        str(int(quote["entry_total_cost_wei"])),
                        float(quote["entry_price_eth"]),
                        float(quote["round_trip_cost_fraction"]),
                        _utcnow(),
                        decision_reason,
                        reservation_id,
                    ),
                )
            return int(cursor.rowcount or 0) == 1
        except Exception:
            cancel_paper_capital(
                self.store,
                release_commit=self.release_commit,
                reservation_id=reservation_id,
                reason="robinhood_trial_insert_failed",
            )
            raise
