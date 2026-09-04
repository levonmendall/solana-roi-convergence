from __future__ import annotations

from .robinhood_chain_core import *


class RobinhoodMetricsMixin:
    def _paper_nav_usd(self) -> float:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT paper_nav_multiplier FROM robinhood_paper_outcomes "
                "WHERE release_commit=? ORDER BY id",
                (self.release_commit,),
            ).fetchall()
        multiplier = 1.0
        for row in rows:
            multiplier *= max(0.0, float(row["paper_nav_multiplier"]))
        return self.starting_nav_usd * multiplier

    def _open_exposure(self) -> float:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT COALESCE(SUM(t.position_fraction),0) AS total "
                "FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
                "WHERE t.release_commit=? AND o.id IS NULL",
                (self.release_commit,),
            ).fetchone()
        return min(1.0, max(0.0, float(row["total"] or 0.0))) if row is not None else 0.0

    def _token_open(self, token: str) -> bool:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT 1 FROM robinhood_paper_trials t LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
                "WHERE t.release_commit=? AND t.token=? AND o.id IS NULL LIMIT 1",
                (self.release_commit, token),
            ).fetchone()
        return row is not None

    def _context_returns(self, entity: str, venue: str, lifecycle: str) -> list[float]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.net_return FROM robinhood_paper_outcomes o "
                "WHERE o.release_commit=? AND o.trigger_entity=? AND o.venue=? AND o.lifecycle=? ORDER BY o.id",
                (self.release_commit, entity, venue, lifecycle),
            ).fetchall()
        return [float(row["net_return"]) for row in rows]

    def _position_fraction(self, entity: str, venue: str, lifecycle: str) -> tuple[float, dict[str, Any]]:
        profile = classify_context_returns(self._context_returns(entity, venue, lifecycle))
        if profile["state"] == "demoted_paper_context":
            return 0.0, profile
        if profile["state"] == "promoted_paper_context":
            return float(profile["best_paper_position_fraction"]), profile
        return BOOTSTRAP_PAPER_FRACTION, profile

    def _recent_metrics(
        self,
        swaps: deque[dict[str, Any]],
        *,
        now_ts: float,
        entity_map: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        current = [s for s in swaps if now_ts - float(s["observed_ts"]) <= 60.0]
        prior = [s for s in swaps if 60.0 < now_ts - float(s["observed_ts"]) <= 120.0]
        buys = [s for s in current if s["side"] == "buy"]
        sells = [s for s in current if s["side"] == "sell"]
        prior_buys = [s for s in prior if s["side"] == "buy"]
        independent = {
            (entity_map or {}).get(_clean_address(str(s.get("actor") or "")), _clean_address(str(s.get("actor") or "")))
            for s in buys
            if _clean_address(str(s.get("actor") or "")) and _clean_address(str(s.get("actor") or "")) not in KNOWN_NON_ACTORS
        }
        independent.discard("")
        buy_quote = sum(int(s.get("quote_amount_wei") or 0) for s in buys)
        sell_quote = sum(int(s.get("quote_amount_wei") or 0) for s in sells)
        ratio = buy_quote / max(1, sell_quote)
        acceleration = len(buys) / max(1, len(prior_buys))
        prices = [float(s["price_eth"]) for s in current if _finite(s.get("price_eth")) not in (None, 0.0)]
        price_change = prices[-1] / prices[0] - 1.0 if len(prices) >= 2 and prices[0] > 0 else 0.0
        if len(buys) >= 4 and len(independent) >= 3 and ratio >= 1.5 and acceleration >= 1.25 and 0.01 <= price_change <= MAX_CHASE_FRACTION:
            state = "active_fomo"
        elif len(buys) >= 3 and len(independent) >= 3 and ratio >= 1.2 and price_change <= MAX_CHASE_FRACTION:
            state = "pre_fomo"
        elif sells and sell_quote > buy_quote:
            state = "exhaustion"
        else:
            state = "neutral"
        return {
            "state": state,
            "buy_count_60s": len(buys),
            "sell_count_60s": len(sells),
            "independent_buyers_60s": len(independent),
            "independent_entities_60s": len(independent),
            "buy_sell_quote_ratio": ratio,
            "buy_count_acceleration": acceleration,
            "price_change_60s": price_change,
            "trigger_actor": str(buys[-1].get("actor") or "") if buys else "",
        }

    def _record_swap(
        self,
        *,
        venue: str,
        lifecycle: str,
        token: str,
        market: str,
        tx_hash: str,
        log_index: int,
        block_number: int,
        actor: str,
        actor_source: str,
        side: str,
        quote_amount_wei: int,
        token_amount_raw: int,
        price_eth: float | None,
        fee_or_tax_wei: int = 0,
        observed_at: str,
    ) -> bool:
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_swaps("
                "release_commit,venue,lifecycle,token,market,tx_hash,log_index,block_number,actor,actor_source,"
                "side,quote_amount_wei,token_amount_raw,price_eth,fee_or_tax_wei,observed_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.release_commit,
                    venue,
                    lifecycle,
                    token,
                    market,
                    tx_hash,
                    int(log_index),
                    int(block_number),
                    actor,
                    actor_source,
                    side,
                    str(int(quote_amount_wei)),
                    str(int(token_amount_raw)),
                    price_eth,
                    str(int(fee_or_tax_wei)),
                    observed_at,
                ),
            )
        return bool(cursor.rowcount)

    def _persist_launch(
        self,
        *,
        protocol: str,
        venue: str,
        lifecycle: str,
        token: str,
        pool: str = "",
        curve: str = "",
        deployer: str = "",
        pair_token: str = "",
        fee: int | None = None,
        tick_spacing: int | None = None,
        launch_block: int,
        restrictions_end_block: int = 0,
        graduation_threshold: int | None = None,
        paper_eligible: bool,
        source_tx: str = "",
    ) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_launches("
                "release_commit,protocol,venue,lifecycle,token,pool,curve,deployer,pair_token,fee,tick_spacing,"
                "launch_block,restrictions_end_block,graduation_threshold,paper_eligible,source_tx,observed_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.release_commit,
                    protocol,
                    venue,
                    lifecycle,
                    token,
                    pool or None,
                    curve or None,
                    deployer or None,
                    pair_token or None,
                    fee,
                    tick_spacing,
                    int(launch_block),
                    int(restrictions_end_block),
                    str(graduation_threshold) if graduation_threshold is not None else None,
                    1 if paper_eligible else 0,
                    source_tx or None,
                    _utcnow(),
                ),
            )
