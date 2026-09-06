from __future__ import annotations

import asyncio
import base64
import sqlite3
import threading
from collections import deque
from types import SimpleNamespace

import pytest

import solana_roi.v51_exit_execution_integrity as exit_integrity
from solana_roi.strategy_v51_authority import authority


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class Response:
    def __init__(self, payload: dict[str, object]):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return dict(self.payload)


class Client:
    def __init__(self, payloads: list[dict[str, object]]):
        self.payloads = deque(payloads)
        self.calls: list[dict[str, object]] = []

    async def get(self, url: str, *, params: dict[str, object], headers: dict[str, object]) -> Response:
        self.calls.append({"url": url, "params": dict(params), "headers": dict(headers)})
        payload = self.payloads.popleft() if self.payloads else {}
        return Response(payload)


class Rpc:
    def __init__(self, values: list[dict[str, object]]):
        self.values = deque(values)
        self.calls: list[tuple[str, list[object]]] = []

    async def call(self, method: str, params: list[object]) -> dict[str, object]:
        self.calls.append((method, params))
        return self.values.popleft() if self.values else {"value": {"err": None}}


class Adapter:
    def __init__(self, *, client: Client, rpc: Rpc):
        self.store = Store()
        self.release_commit = "a" * 40
        self._http = client
        self.discovery = SimpleNamespace(rpc=rpc)

    def _client(self) -> Client:
        return self._http


def _order(*, amount: int, out_amount: int = 2_000_000) -> dict[str, object]:
    return {
        "router": "jupiter-test-router",
        "inAmount": str(amount),
        "outAmount": str(out_amount),
        "otherAmountThreshold": str(out_amount - 10_000),
        "priceImpactPct": "0.75",
        "routePlan": [
            {"swapInfo": {"ammKey": "amm-1", "label": "Pump AMM"}},
            {"swapInfo": {"ammKey": "amm-2", "label": "Raydium"}},
        ],
        "tokenAccountRequirements": {"input": "source-ata", "output": "wsol-ata"},
        "signatureFeeLamports": 5_000,
        "prioritizationFeeLamports": 2_000,
        "rentFeeLamports": 1_000,
        "lastValidBlockHeight": 123456,
        "transaction": base64.b64encode(b"unsigned-exact-exit-transaction").decode(),
    }


def _ctx(adapter: Adapter, *, amount: int, due: str = "exit-sig-1") -> exit_integrity.ExitContext:
    return exit_integrity.ExitContext(
        adapter=adapter,
        row={
            "signature": due,
            "token_mint": "token-mint",
            "observed_at": "2026-09-06T12:00:00+00:00",
        },
        positions_by_amount={amount: deque(["entry-sig-1"])},
        first_due_by_source={"entry-sig-1": "2026-09-06T12:00:00+00:00"},
    )


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUPITER_API_KEY", "test-key")
    monkeypatch.setenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "11111111111111111111111111111111")
    monkeypatch.setattr(exit_integrity, "EXIT_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0))


def test_109_exit_quote_uses_exact_actual_held_raw_amount() -> None:
    amount = 987_654_321
    client = Client([_order(amount=amount)])
    rpc = Rpc([{"context": {"slot": 10}, "value": {"err": None, "unitsConsumed": 88_000, "logs": []}}])
    adapter = Adapter(client=client, rpc=rpc)

    result = _run(exit_integrity._exact_exit_route(
        adapter, _ctx(adapter, amount=amount), "token-mint", exit_integrity.WSOL_MINT, amount
    ))

    assert result == {"out_amount": 2_000_000, "fee_lamports": 8_000}
    assert client.calls[0]["params"]["amount"] == str(amount)
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT actual_position_raw,exit_quote_amount_raw,exact_amount_match FROM v51_exact_exit_attempts"
        ).fetchone()
    assert row["actual_position_raw"] == amount
    assert row["exit_quote_amount_raw"] == amount
    assert row["exact_amount_match"] == 1


def test_110_actual_unsigned_exit_order_metadata_is_persisted_and_never_submitted() -> None:
    amount = 111_222_333
    client = Client([_order(amount=amount)])
    rpc = Rpc([{"context": {"slot": 99}, "value": {"err": None, "unitsConsumed": 144_000, "logs": ["ok"]}}])
    adapter = Adapter(client=client, rpc=rpc)

    _run(exit_integrity._exact_exit_route(
        adapter, _ctx(adapter, amount=amount), "token-mint", exit_integrity.WSOL_MINT, amount
    ))

    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT router,expected_output_raw,minimum_output_raw,route_hops_json,price_impact_fraction,"
            "token_account_requirements_json,transaction_built,transaction_sha256,transaction_size_bytes,"
            "last_valid_block_height FROM v51_exact_exit_attempts"
        ).fetchone()
    assert row["router"] == "jupiter-test-router"
    assert row["expected_output_raw"] == 2_000_000
    assert row["minimum_output_raw"] == 1_990_000
    assert "amm-1" in row["route_hops_json"] and "amm-2" in row["route_hops_json"]
    assert row["price_impact_fraction"] == pytest.approx(0.75)
    assert "source-ata" in row["token_account_requirements_json"]
    assert row["transaction_built"] == 1
    assert row["transaction_sha256"]
    assert row["transaction_size_bytes"] > 0
    assert row["last_valid_block_height"] == 123456
    assert [name for name, _ in rpc.calls] == ["simulateTransaction"]
    assert rpc.calls[0][1][1]["sigVerify"] is False
    assert rpc.calls[0][1][1]["replaceRecentBlockhash"] is True


def test_111_exit_simulation_failure_is_independent_and_classified() -> None:
    amount = 444_555
    failure = {"InstructionError": [0, "InsufficientFunds"]}
    client = Client([_order(amount=amount) for _ in range(3)])
    rpc = Rpc([
        {"context": {"slot": 1}, "value": {"err": failure, "unitsConsumed": 20_000, "logs": []}},
        {"context": {"slot": 2}, "value": {"err": failure, "unitsConsumed": 21_000, "logs": []}},
        {"context": {"slot": 3}, "value": {"err": failure, "unitsConsumed": 22_000, "logs": []}},
    ])
    adapter = Adapter(client=client, rpc=rpc)

    result = _run(exit_integrity._exact_exit_route(
        adapter, _ctx(adapter, amount=amount), "token-mint", exit_integrity.WSOL_MINT, amount
    ))

    assert result is None
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT simulation_ok,simulation_error_class,account_failure,units_consumed,status "
            "FROM v51_exact_exit_attempts ORDER BY attempt_number"
        ).fetchall()
    assert len(rows) == 3
    assert all(row["simulation_ok"] == 0 for row in rows)
    assert all(row["simulation_error_class"] == "account_failure" for row in rows)
    assert all(row["account_failure"] == 1 for row in rows)
    assert [row["units_consumed"] for row in rows] == [20_000, 21_000, 22_000]
    assert all(row["status"] == "paper_exit_execution_failed" for row in rows)


def test_112_unexitable_position_persists_failure_retries_and_no_synthetic_fill() -> None:
    amount = 777_888
    client = Client([_order(amount=amount) for _ in range(3)])
    rpc = Rpc([
        {"value": {"err": {"InstructionError": [1, "Custom"]}}},
        {"value": {"err": {"InstructionError": [1, "Custom"]}}},
        {"value": {"err": {"InstructionError": [1, "Custom"]}}},
    ])
    adapter = Adapter(client=client, rpc=rpc)

    assert _run(exit_integrity._exact_exit_route(
        adapter, _ctx(adapter, amount=amount), "token-mint", exit_integrity.WSOL_MINT, amount
    )) is None

    with adapter.store._lock:
        state = adapter.store.db.execute("SELECT * FROM v51_exact_exit_state").fetchone()
        outcome_table = adapter.store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='profit_first_final_outcomes'"
        ).fetchone()
    assert state["first_exit_due_at"] == "2026-09-06T12:00:00+00:00"
    assert state["retry_attempts"] == 3
    assert state["route_ever_available"] == 1
    assert state["last_status"] == "paper_exit_execution_failed"
    assert state["eventual_exit_at"] is None
    assert state["terminal_liquidation_assumption"] == exit_integrity.TERMINAL_LIQUIDATION_ASSUMPTION
    assert outcome_table is None


def test_112_later_fresh_route_can_realize_eventual_exit_after_prior_failure() -> None:
    amount = 246_810
    failed_client = Client([_order(amount=amount) for _ in range(3)])
    failed_rpc = Rpc([{"value": {"err": "blocked"}} for _ in range(3)])
    adapter = Adapter(client=failed_client, rpc=failed_rpc)
    _run(exit_integrity._exact_exit_route(
        adapter, _ctx(adapter, amount=amount), "token-mint", exit_integrity.WSOL_MINT, amount
    ))

    adapter._http = Client([_order(amount=amount, out_amount=3_000_000)])
    adapter.discovery.rpc = Rpc([{"context": {"slot": 55}, "value": {"err": None, "unitsConsumed": 50_000, "logs": []}}])
    later = _ctx(adapter, amount=amount, due="exit-sig-2")
    later.first_due_by_source["entry-sig-1"] = "2026-09-06T12:00:00+00:00"
    result = _run(exit_integrity._exact_exit_route(
        adapter, later, "token-mint", exit_integrity.WSOL_MINT, amount
    ))
    assert result is not None
    with adapter.store._lock:
        state = adapter.store.db.execute("SELECT * FROM v51_exact_exit_state").fetchone()
    assert state["retry_attempts"] == 4
    assert state["last_due_exit_signature"] == "exit-sig-2"
    assert state["last_status"] == "paper_exit_executed_exact"
    assert state["eventual_exit_at"] is not None
    assert state["eventual_exit_output_raw"] == 3_000_000
    assert state["terminal_liquidation_assumption"] is None


def test_113_old_execution_epoch_records_remain_auditable_but_cannot_promote(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_release_compatibility(release_commit TEXT PRIMARY KEY,execution_model_epoch TEXT)"
        )
        store.db.executemany(
            "INSERT INTO v51_release_compatibility(release_commit,execution_model_epoch) VALUES (?,?)",
            [
                ("old-release", "v51-execution-model-20260905-1"),
                ("new-release", exit_integrity.EXECUTION_MODEL_EPOCH),
            ],
        )
    old_original = exit_integrity._ORIGINAL_ANALYTICS_PROMOTION_RECORDS
    monkeypatch.setattr(
        exit_integrity,
        "_ORIGINAL_ANALYTICS_PROMOTION_RECORDS",
        lambda _store: [
            {"release_commit": "old-release", "surface": "SOLANA_ALPHA", "source_signature": "old", "net_return": 9.0},
            {"release_commit": "new-release", "surface": "SOLANA_ALPHA", "source_signature": "new", "net_return": 0.2},
            {"release_commit": "old-release", "surface": "ROBINHOOD_CHAIN", "source_signature": "rh", "net_return": 0.1},
        ],
    )
    selected = exit_integrity._current_epoch_promotion_records(store)
    assert {row["source_signature"] for row in selected} == {"new", "rh"}
    assert selected[0]["execution_model_epoch"] == exit_integrity.EXECUTION_MODEL_EPOCH
    monkeypatch.setattr(exit_integrity, "_ORIGINAL_ANALYTICS_PROMOTION_RECORDS", old_original)


def test_113_execution_epoch_fingerprint_and_safety_boundary() -> None:
    payload = exit_integrity.status()
    spec = authority()
    assert payload["execution_model_epoch"] == "v51-execution-model-exact-exit-v2"
    assert payload["historical_execution_epoch_promotion_authority"] is False
    assert payload["synthetic_fixed_drag_exit_fill_authority"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert spec["execution"]["latency_hard_max_seconds"] == 20.0
    assert spec["paper_only"] is True
    assert spec["live_money_authority"] is False
