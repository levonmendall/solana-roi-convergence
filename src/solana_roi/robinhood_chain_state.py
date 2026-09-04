from __future__ import annotations

from .robinhood_chain_core import *


class RobinhoodStateMixin:
    def __init__(self, store: ObservationEventStore, *, release_commit: str | None = None) -> None:
        self.store = store
        self.release_commit = (
            release_commit
            or os.getenv("RENDER_GIT_COMMIT")
            or os.getenv("GITHUB_SHA")
            or "local"
        )
        self.enabled = os.getenv("ROBINHOOD_CHAIN_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
        self.rpc = RobinhoodRpc(timeout_seconds=float(os.getenv("ROBINHOOD_RPC_TIMEOUT_SECONDS", "4.0")))
        self.poll_seconds = max(1.0, float(os.getenv("ROBINHOOD_POLL_SECONDS", str(POLL_SECONDS))))
        self.paper_recipient = _clean_address(
            os.getenv("ROBINHOOD_PAPER_RECIPIENT_ADDRESS", "0x000000000000000000000000000000000000dead")
        )
        self.starting_nav_usd = max(1.0, float(os.getenv("ROBINHOOD_PAPER_STARTING_NAV_USD", "500")))
        self.v3_pools: dict[str, V3Pool] = {}
        self.v2_curves: dict[str, V2Curve] = {}
        self._cursor: int | None = None
        self._latest_block: int | None = None
        self._caught_up = False
        self._last_error: str | None = None
        self._last_success_at: str | None = None
        self._last_poll_at: str | None = None
        self._rpc_failures = 0
        self._eth_usd_cache: tuple[float, float] | None = None
        self._rwa_tokens: set[str] = set()
        self._rwa_registry_last_refresh = 0.0
        self._rwa_registry_available = False
        self._rwa_registry_error: str | None = None
        self._entity_cache: dict[str, tuple[str, float]] = {}
        self._entity_resolution_failures = 0
        self._schema()
        self._restore()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_chain_state ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_launches ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, protocol TEXT NOT NULL, "
                "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, token TEXT NOT NULL, pool TEXT, curve TEXT, "
                "deployer TEXT, pair_token TEXT, fee INTEGER, tick_spacing INTEGER, launch_block INTEGER NOT NULL, "
                "restrictions_end_block INTEGER NOT NULL DEFAULT 0, graduation_threshold TEXT, "
                "paper_eligible INTEGER NOT NULL, source_tx TEXT, observed_at TEXT NOT NULL, "
                "UNIQUE(release_commit, protocol, token))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_swaps ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, venue TEXT NOT NULL, "
                "lifecycle TEXT NOT NULL, token TEXT NOT NULL, market TEXT NOT NULL, tx_hash TEXT NOT NULL, "
                "log_index INTEGER NOT NULL, block_number INTEGER NOT NULL, actor TEXT, actor_source TEXT NOT NULL, "
                "side TEXT NOT NULL, quote_amount_wei TEXT NOT NULL, token_amount_raw TEXT NOT NULL, "
                "price_eth REAL, fee_or_tax_wei TEXT, observed_at TEXT NOT NULL, "
                "UNIQUE(release_commit, tx_hash, log_index))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_robinhood_swaps_market_time "
                "ON robinhood_swaps(release_commit, market, id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_paper_trials ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
                "token TEXT NOT NULL, market TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
                "trigger_actor TEXT NOT NULL, trigger_entity TEXT NOT NULL, fomo_state TEXT NOT NULL, context_state TEXT NOT NULL, "
                "position_fraction REAL NOT NULL, entry_quote_in_wei TEXT NOT NULL, entry_token_raw TEXT NOT NULL, "
                "entry_gas_wei TEXT NOT NULL, entry_total_cost_wei TEXT NOT NULL, entry_price_eth REAL NOT NULL, "
                "entry_round_trip_cost_fraction REAL NOT NULL, opened_at TEXT NOT NULL, decision_reason TEXT NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_paper_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, trial_id INTEGER NOT NULL UNIQUE, "
                "token TEXT NOT NULL, market TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
                "trigger_actor TEXT NOT NULL, trigger_entity TEXT NOT NULL, fomo_state TEXT NOT NULL, position_fraction REAL NOT NULL, "
                "net_return REAL NOT NULL, paper_nav_multiplier REAL NOT NULL, exit_quote_out_wei TEXT NOT NULL, "
                "exit_gas_wei TEXT NOT NULL, exit_reason TEXT NOT NULL, settled_at TEXT NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )

    def _restore(self) -> None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT value FROM robinhood_chain_state WHERE key='cursor_block'"
            ).fetchone()
            self._cursor = int(row["value"]) if row is not None else None
            launches = self.store.db.execute(
                "SELECT * FROM robinhood_launches WHERE release_commit=? ORDER BY id DESC LIMIT 256",
                (self.release_commit,),
            ).fetchall()
        for raw in reversed(launches):
            row = dict(raw)
            pool = _clean_address(row.get("pool"))
            curve = _clean_address(row.get("curve"))
            token = _clean_address(row.get("token"))
            if pool and token and str(row.get("venue", "")).startswith(("PONS_V1", "UNISWAP_V3")):
                self.v3_pools[pool] = V3Pool(
                    token=token,
                    pool=pool,
                    token0=sorted([WETH, token], key=lambda item: int(item, 16))[0],
                    token1=sorted([WETH, token], key=lambda item: int(item, 16))[1],
                    fee=int(row.get("fee") or 10_000),
                    token_decimals=18,
                    venue=str(row.get("venue") or "UNISWAP_V3_DIRECT"),
                    lifecycle=str(row.get("lifecycle") or "new_weth_pool"),
                    deployer=_clean_address(row.get("deployer")),
                    launch_block=int(row.get("launch_block") or 0),
                    restrictions_end_block=int(row.get("restrictions_end_block") or 0),
                )
            if curve and token and str(row.get("protocol")) == "pons_v2":
                self.v2_curves[curve] = V2Curve(
                    token=token,
                    curve=curve,
                    deployer=_clean_address(row.get("deployer")),
                    pair_token=_clean_address(row.get("pair_token")),
                    launch_config_id=0,
                    graduation_threshold=int(row.get("graduation_threshold") or 0),
                    launch_block=int(row.get("launch_block") or 0),
                )
        self._trim_tracking()

    def _set_cursor(self, value: int) -> None:
        now = _utcnow()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO robinhood_chain_state(key,value,updated_at) VALUES ('cursor_block',?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (str(int(value)), now),
            )
        self._cursor = int(value)

    def _trim_tracking(self) -> None:
        if len(self.v3_pools) > MAX_TRACKED_V3_POOLS:
            keep = sorted(self.v3_pools.values(), key=lambda p: p.launch_block, reverse=True)[:MAX_TRACKED_V3_POOLS]
            self.v3_pools = {p.pool: p for p in keep}
        if len(self.v2_curves) > MAX_TRACKED_V2_CURVES:
            keep2 = sorted(self.v2_curves.values(), key=lambda p: p.launch_block, reverse=True)[:MAX_TRACKED_V2_CURVES]
            self.v2_curves = {p.curve: p for p in keep2}

    async def close(self) -> None:
        await self.rpc.close()

    async def _eth_usd(self) -> float | None:
        fixed = _finite(os.getenv("ROBINHOOD_ETH_USD_FIXED"))
        if fixed is not None and fixed > 0:
            return fixed
        now = time.monotonic()
        if self._eth_usd_cache is not None and now - self._eth_usd_cache[1] < 60:
            return self._eth_usd_cache[0]
        try:
            response = await self.rpc.client.get("https://api.coinbase.com/v2/prices/ETH-USD/spot")
            response.raise_for_status()
            value = float(response.json()["data"]["amount"])
            if value > 0 and math.isfinite(value):
                self._eth_usd_cache = (value, now)
                return value
        except Exception:
            pass
        return self._eth_usd_cache[0] if self._eth_usd_cache else None
