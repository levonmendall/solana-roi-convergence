from __future__ import annotations

from .robinhood_chain_core import *


class RobinhoodIngestMixin:
    async def _register_v3_pool(
        self,
        *,
        token0: str,
        token1: str,
        fee: int,
        pool: str,
        launch_block: int,
        venue: str = "UNISWAP_V3_DIRECT",
        lifecycle: str = "new_weth_pool",
        deployer: str = "",
        restrictions_end_block: int = 0,
        protocol: str = "uniswap_v3",
        source_tx: str = "",
        authoritative: bool = False,
    ) -> None:
        token0, token1, pool = _clean_address(token0), _clean_address(token1), _clean_address(pool)
        if not pool or WETH not in {token0, token1}:
            return
        token = token1 if token0 == WETH else token0
        if not token or token == WETH:
            return
        paper_eligible = True
        if not protocol.startswith("pons"):
            paper_eligible = await self._direct_v3_token_allowed(token)
        existing = self.v3_pools.get(pool)
        if existing is not None and not authoritative:
            return
        decimals = 18 if protocol.startswith("pons") else await self.rpc.token_decimals(token)
        metadata = V3Pool(
            token=token,
            pool=pool,
            token0=token0,
            token1=token1,
            fee=int(fee),
            token_decimals=decimals,
            venue=venue,
            lifecycle=lifecycle,
            deployer=_clean_address(deployer),
            launch_block=int(launch_block),
            restrictions_end_block=int(restrictions_end_block),
        )
        self.v3_pools[pool] = metadata
        self._persist_launch(
            protocol=protocol,
            venue=venue,
            lifecycle=lifecycle,
            token=token,
            pool=pool,
            deployer=deployer,
            pair_token=WETH,
            fee=fee,
            launch_block=launch_block,
            restrictions_end_block=restrictions_end_block,
            paper_eligible=paper_eligible,
            source_tx=source_tx,
        )
        self._trim_tracking()

    async def _process_factory_log(self, log: dict[str, Any]) -> None:
        address = _clean_address(log.get("address"))
        topics = [str(t).lower() for t in (log.get("topics") or [])]
        data = _words(str(log.get("data") or ""))
        block = int(str(log.get("blockNumber") or "0x0"), 16)
        tx_hash = str(log.get("transactionHash") or "")
        if address == UNISWAP_V3_FACTORY and topics and topics[0] == V3_POOL_CREATED_TOPIC and len(topics) >= 4 and len(data) >= 2:
            token0 = _topic_address(topics[1])
            token1 = _topic_address(topics[2])
            fee = _uint(topics[3])
            pool = _word_address(data[1])
            await self._register_v3_pool(
                token0=token0,
                token1=token1,
                fee=fee,
                pool=pool,
                launch_block=block,
                source_tx=tx_hash,
            )
            return

        if (
            address in {PONS_V1_ACTIVE_FACTORY, PONS_V1_LEGACY_FACTORY}
            and topics
            and topics[0] == PONS_V1_TOKEN_LAUNCHED_TOPIC
            and len(topics) >= 4
            and len(data) >= 7
        ):
            token = _topic_address(topics[1])
            deployer = _topic_address(topics[2])
            pair_token = _word_address(data[0])
            pool = _word_address(data[1])
            restrictions_end = _uint(data[5])
            if pair_token == WETH:
                token0, token1 = sorted([token, WETH], key=lambda item: int(item, 16))
                await self._register_v3_pool(
                    token0=token0,
                    token1=token1,
                    fee=10_000,
                    pool=pool,
                    launch_block=block,
                    venue="PONS_V1_UNISWAP_V3",
                    lifecycle="launch_protected_v3",
                    deployer=deployer,
                    restrictions_end_block=restrictions_end,
                    protocol="pons_v1",
                    source_tx=tx_hash,
                    authoritative=True,
                )
            return

        if address == PONS_V2_FACTORY and topics and topics[0] == PONS_V2_TOKEN_LAUNCHED_TOPIC and len(topics) >= 4 and len(data) >= 3:
            token = _topic_address(topics[1])
            curve = _topic_address(topics[2])
            deployer = _topic_address(topics[3])
            pair_token = _word_address(data[0])
            launch_config_id = _uint(data[1])
            threshold = _uint(data[2])
            metadata = V2Curve(
                token=token,
                curve=curve,
                deployer=deployer,
                pair_token=pair_token,
                launch_config_id=launch_config_id,
                graduation_threshold=threshold,
                launch_block=block,
            )
            self.v2_curves[curve] = metadata
            self._persist_launch(
                protocol="pons_v2",
                venue="PONS_V2_CURVE",
                lifecycle="bonding_curve",
                token=token,
                curve=curve,
                deployer=deployer,
                pair_token=pair_token,
                launch_block=block,
                graduation_threshold=threshold,
                paper_eligible=pair_token in {"", "0x0000000000000000000000000000000000000000", WETH},
                source_tx=tx_hash,
            )
            self._trim_tracking()

    async def _process_v3_swap(self, pool: V3Pool, log: dict[str, Any], *, live: bool, observed_at: str) -> None:
        topics = [str(t).lower() for t in (log.get("topics") or [])]
        if not topics or topics[0] != V3_SWAP_TOPIC or len(topics) < 3:
            return
        words = _words(str(log.get("data") or ""))
        if len(words) < 5:
            return
        amount0 = _signed(words[0])
        amount1 = _signed(words[1])
        sqrt_price_x96 = _uint(words[2])
        if amount0 == 0 or amount1 == 0 or sqrt_price_x96 <= 0:
            return
        quote_amount = amount0 if pool.weth_is_token0 else amount1
        token_amount = amount1 if pool.weth_is_token0 else amount0
        if quote_amount > 0 and token_amount < 0:
            side = "buy"
        elif quote_amount < 0 and token_amount > 0:
            side = "sell"
        else:
            return
        raw_ratio = (sqrt_price_x96 * sqrt_price_x96) / float(1 << 192)
        if raw_ratio <= 0:
            return
        if pool.weth_is_token0:
            human_token_per_weth = raw_ratio * (10 ** (18 - pool.token_decimals))
            price_eth = 1.0 / human_token_per_weth if human_token_per_weth > 0 else None
        else:
            price_eth = raw_ratio * (10 ** (pool.token_decimals - 18))
        recipient = _topic_address(topics[2])
        tx_hash = str(log.get("transactionHash") or "")
        block = int(str(log.get("blockNumber") or "0x0"), 16)
        log_index = int(str(log.get("logIndex") or "0x0"), 16)
        lifecycle = (
            "post_protection_v3"
            if pool.restrictions_end_block and block > pool.restrictions_end_block
            else pool.lifecycle
        )
        inserted = self._record_swap(
            venue=pool.venue,
            lifecycle=lifecycle,
            token=pool.token,
            market=pool.pool,
            tx_hash=tx_hash,
            log_index=log_index,
            block_number=block,
            actor=recipient,
            actor_source="swap_recipient_not_tx_from",
            side=side,
            quote_amount_wei=abs(quote_amount),
            token_amount_raw=abs(token_amount),
            price_eth=price_eth,
            observed_at=observed_at,
        )
        if not inserted or not live:
            return
        swap = {
            "side": side,
            "actor": recipient,
            "quote_amount_wei": abs(quote_amount),
            "token_amount_raw": abs(token_amount),
            "price_eth": price_eth,
            "observed_ts": time.time(),
        }
        pool.recent_swaps.append(swap)
        if pool.first_price_eth is None and price_eth is not None:
            pool.first_price_eth = price_eth
            pool.first_live_observed_at = observed_at
        await self._maybe_open_v3(pool, current_block=block)

    async def _process_v2_curve_log(self, curve: V2Curve, log: dict[str, Any], *, live: bool, observed_at: str) -> None:
        topics = [str(t).lower() for t in (log.get("topics") or [])]
        if len(topics) < 3 or not topics:
            return
        side: str
        if topics[0] == PONS_V2_CURVE_BUY_TOPIC:
            side = "buy"
        elif topics[0] == PONS_V2_CURVE_SELL_TOPIC:
            side = "sell"
        else:
            return
        words = _words(str(log.get("data") or ""))
        if len(words) < 4:
            return
        actor = _topic_address(topics[1])
        if side == "buy":
            quote_amount, token_amount, fee, tax = map(_uint, words[:4])
        else:
            token_amount, quote_amount, fee, tax = map(_uint, words[:4])
        pair = curve.pair_token
        if pair not in {"", "0x0000000000000000000000000000000000000000", WETH}:
            return
        price_eth = (quote_amount / 1e18) / (token_amount / 1e18) if token_amount else None
        tx_hash = str(log.get("transactionHash") or "")
        block = int(str(log.get("blockNumber") or "0x0"), 16)
        log_index = int(str(log.get("logIndex") or "0x0"), 16)
        inserted = self._record_swap(
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            token=curve.token,
            market=curve.curve,
            tx_hash=tx_hash,
            log_index=log_index,
            block_number=block,
            actor=actor,
            actor_source="curve_buyer_or_seller",
            side=side,
            quote_amount_wei=quote_amount,
            token_amount_raw=token_amount,
            price_eth=price_eth,
            fee_or_tax_wei=fee + tax,
            observed_at=observed_at,
        )
        if not inserted or not live:
            return
        curve.recent_swaps.append(
            {
                "side": side,
                "actor": actor,
                "quote_amount_wei": quote_amount,
                "token_amount_raw": token_amount,
                "price_eth": price_eth,
                "observed_ts": time.time(),
            }
        )
        await self._maybe_open_v2(curve)
