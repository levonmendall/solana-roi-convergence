from __future__ import annotations

from .robinhood_chain_core import *
from .v51_atomic_paper_capital import settle_paper_capital


class RobinhoodSettlementMixin:
    async def _settle_open_positions(self) -> None:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT t.* FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
                "WHERE t.release_commit=? AND o.id IS NULL ORDER BY t.id",
                (self.release_commit,),
            ).fetchall()
        for raw in rows:
            row = dict(raw)
            try:
                await self._settle_one(row)
            except Exception:
                continue

    async def _settle_one(self, trial: dict[str, Any]) -> None:
        opened = datetime.fromisoformat(str(trial["opened_at"]))
        elapsed = max(0.0, (datetime.now(timezone.utc) - opened).total_seconds())
        token = _clean_address(trial["token"])
        market = _clean_address(trial["market"])
        token_amount = int(trial["entry_token_raw"])
        total_cost = int(trial["entry_total_cost_wei"])
        venue = str(trial["venue"])
        gas_price = await self.rpc.gas_price()
        exit_out: int | None = None
        exit_gas_wei = 0
        fomo_state = str(trial["fomo_state"])

        if venue in {"PONS_V1_UNISWAP_V3", "UNISWAP_V3_DIRECT"}:
            pool = self.v3_pools.get(market)
            if pool is None:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                exit_out = 0
            else:
                try:
                    raw_out, gas_estimate = await self.rpc.v3_quote_exact_input(
                        token_in=token,
                        token_out=WETH,
                        fee=pool.fee,
                        amount_in=token_amount,
                    )
                    exit_gas_wei = (gas_estimate + 80_000) * gas_price
                    exit_out = max(0, raw_out - exit_gas_wei)
                except Exception:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
                metrics = await self._resolved_metrics(pool.recent_swaps)
                fomo_state = str(metrics["state"])
        elif venue == "PONS_V2_CURVE":
            curve = self.v2_curves.get(market)
            if curve is None:
                return
            state = await self.rpc.pons_v2_launch_state(token)
            if int(state["phase"]) == 0:
                try:
                    raw_out = await self.rpc.pons_v2_curve_sell_quote(curve=market, tokens_in=token_amount)
                    exit_gas_wei = 220_000 * gas_price
                    exit_out = max(0, raw_out - exit_gas_wei)
                except RuntimeError:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
            elif int(state["phase"]) == 1:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                exit_out = 0
            elif int(state["phase"]) == 2:
                pair = _clean_address(state["pair_token"])
                currency_a = "0x0000000000000000000000000000000000000000" if pair in {"", "0x0000000000000000000000000000000000000000"} else pair
                currency0, currency1 = sorted([currency_a, token], key=lambda item: int(item, 16))
                zero_for_one = token == currency0
                try:
                    raw_out, gas_estimate = await self.rpc.v4_quote_exact_input(
                        currency0=currency0,
                        currency1=currency1,
                        fee=int(state["pool_fee"]),
                        tick_spacing=int(state["tick_spacing"]),
                        hooks=PONS_V2_MEME_HOOK,
                        zero_for_one=zero_for_one,
                        amount_in=token_amount,
                    )
                    exit_gas_wei = (gas_estimate + 120_000) * gas_price
                    exit_out = max(0, raw_out - exit_gas_wei)
                except Exception:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
            else:
                exit_out = 0
            metrics = await self._resolved_metrics(curve.recent_swaps)
            fomo_state = str(metrics["state"])
        else:
            return

        if exit_out is None:
            return
        net_return = exit_out / max(1, total_cost) - 1.0
        if net_return <= STOP_LOSS_FRACTION:
            reason = "stop_loss"
        elif net_return >= HARVEST_FRACTION:
            reason = "harvest"
        elif fomo_state == "exhaustion" and elapsed >= 30:
            reason = "fomo_exhaustion"
        elif elapsed >= MAX_HOLD_SECONDS:
            reason = "max_hold"
        else:
            return
        multiplier = max(0.0, 1.0 + float(trial["position_fraction"]) * net_return)
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_paper_outcomes("
                "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
                "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,"
                "paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    self.release_commit,
                    int(trial["id"]),
                    token,
                    market,
                    venue,
                    str(trial["lifecycle"]),
                    str(trial["trigger_actor"]),
                    str(trial["trigger_entity"]),
                    fomo_state,
                    float(trial["position_fraction"]),
                    float(net_return),
                    float(multiplier),
                    str(int(exit_out)),
                    str(int(exit_gas_wei)),
                    reason,
                    _utcnow(),
                ),
            )
        reservation_id = str(trial.get("capital_reservation_id") or "")
        if reservation_id:
            settle_paper_capital(
                self.store,
                release_commit=self.release_commit,
                reservation_id=reservation_id,
                settlement_id=f"robinhood-trial:{int(trial['id'])}",
                net_return=float(net_return),
            )
