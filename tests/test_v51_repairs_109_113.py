from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation import WSOL_MINT
from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from solana_roi import v51_exact_exit_execution as exact


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Http:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def get(self, url, *, params, headers):
        self.calls.append((url, dict(params), dict(headers)))
        return _Response(self.payload)


class _Rpc:
    def __init__(self, value):
        self.value = value
        self.calls = []

    async def call(self, method, params):
        self.calls.append((method, params))
        return self.value


class _Execution:
    def __init__(self, http):
        self.http = http

    def _client(self):
        return self.http


def _adapter(http, rpc):
    return SimpleNamespace(
        execution=_Execution(http),
        discovery=SimpleNamespace(rpc=rpc),
    )


def _order_payload(**overrides):
    payload = {
        "router": "jupiter-test",
        "outAmount": "100000000",
        "otherAmountThreshold": "95000000",
        "priceImpactPct": "0.031",
        "routePlan": [{"swapInfo": {"label": "PumpSwap"}}],
        "lastValidBlockHeight": 12345,
        "signatureFeeLamports": 5000,
        "prioritizationFeeLamports": 7000,
        "rentFeeLamports": 3000,
        "transaction": base64.b64encode(b"unsigned-exit-transaction").decode(),
    }
    payload.update(overrides)
    return payload


def test_109_exit_quote_amount_is_exact_raw_held_size(monkeypatch):
    monkeypatch.setenv("JUPITER_API_KEY", "jup-key")
    monkeypatch.setenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "shadow-wallet")
    http = _Http(_order_payload())
    rpc = _Rpc({"context": {"slot": 9}, "value": {"err": None, "logs": ["ok"], "unitsConsumed": 321}})
    held_raw = 12_345_678

    result = asyncio.run(exact.observe_exact_exit_order(_adapter(http, rpc), token_mint="mint-a", actual_position_raw=held_raw))

    assert http.calls[0][1]["inputMint"] == "mint-a"
    assert http.calls[0][1]["outputMint"] == WSOL_MINT
    assert http.calls[0][1]["amount"] == str(held_raw)
    assert result["actual_position_raw"] == held_raw
    assert result["quote_input_raw"] == held_raw
    assert result["amount_match"] is True
    assert result["execution_model_epoch"] == exact.EXACT_EXIT_EXECUTION_MODEL_EPOCH


def test_110_actual_unsigned_exit_order_records_route_and_construction_state(monkeypatch):
    monkeypatch.setenv("JUPITER_API_KEY", "jup-key")
    monkeypatch.setenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "shadow-wallet")
    http = _Http(_order_payload())
    rpc = _Rpc({"context": {"slot": 22}, "value": {"err": None, "logs": [], "unitsConsumed": 456}})

    result = asyncio.run(exact.observe_exact_exit_order(_adapter(http, rpc), token_mint="mint-a", actual_position_raw=1_000_000))

    assert result["router"] == "jupiter-test"
    assert result["expected_output_lamports"] == 100_000_000
    assert result["minimum_output_lamports"] == 95_000_000
    assert result["route_hops"] == [{"swapInfo": {"label": "PumpSwap"}}]
    assert result["price_impact_pct"] == 0.031
    assert result["last_valid_block_height"] == 12345
    assert result["transaction_built"] is True
    assert result["transaction_sha256"]
    assert result["transaction_size_bytes"] == len(b"unsigned-exit-transaction")
    assert result["token_account_requirements"]["input_mint"] == "mint-a"
    assert result["signing_available"] is False
    assert result["transaction_submission_available"] is False


def test_111_exit_simulation_is_independent_and_classifies_transfer_restriction(monkeypatch):
    monkeypatch.setenv("JUPITER_API_KEY", "jup-key")
    monkeypatch.setenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "shadow-wallet")
    http = _Http(_order_payload())
    rpc = _Rpc(
        {
            "context": {"slot": 30},
            "value": {
                "err": {"InstructionError": [3, "Custom"]},
                "logs": ["Program log: transfer failed: account frozen"],
                "unitsConsumed": 999,
            },
        }
    )

    result = asyncio.run(exact.observe_exact_exit_order(_adapter(http, rpc), token_mint="mint-a", actual_position_raw=1_000_000))

    assert result["transaction_built"] is True
    assert result["route_valid"] is True
    assert result["simulation_ok"] is False
    assert result["simulation_error_class"] == "token_restriction"
    assert result["token_restriction"] is True
    assert result["transfer_failure"] is True
    assert result["exit_net_sol"] is None
    assert rpc.calls[0][0] == "simulateTransaction"
    simulation_options = rpc.calls[0][1][1]
    assert simulation_options["sigVerify"] is False
    assert simulation_options["replaceRecentBlockhash"] is True


def test_112_retry_policy_never_fabricates_fixed_drag_and_terminates_conservatively():
    first_due = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    expected = [10, 30, 60, 120, 300]
    for attempt_number, seconds in enumerate(expected, start=1):
        assert exact._retry_at(first_due, attempt_number) == first_due + timedelta(seconds=seconds)
    assert exact._retry_at(first_due, len(exact.EXIT_RETRY_ELAPSED_SECONDS)) is None
    assert exact.TERMINAL_LIQUIDATION_ASSUMPTION == "total_loss_after_300s_without_executable_exact_exit"
    assert "0.975" not in exact.TERMINAL_LIQUIDATION_ASSUMPTION


def test_112_failed_exit_state_is_persisted_with_retry_and_exact_amount(monkeypatch, tmp_path):
    store = ObservationEventStore(tmp_path / "exact-exit.sqlite3")
    adapter = SimpleNamespace(store=store, epoch_id="epoch-a", release_commit="release-a")
    exact._ensure_schema(adapter)
    first_due = datetime.now(timezone.utc)
    liquidation = exact._upsert_liquidation(
        adapter,
        {
            "position_scope": "final",
            "source_signature": "entry-a",
            "token_mint": "mint-a",
            "actual_position_raw": 777,
            "entry_cost_sol": 0.25,
            "position_fraction": 0.05,
            "exit_signal_signature": "sell-a",
            "exit_reason": "trigger_wallet_exit_baseline",
            "exit_features_json": "{}",
            "first_exit_due_at": first_due.isoformat(),
        },
    )
    evidence = {
        "attempted_at": first_due.isoformat(),
        "quote_input_raw": 777,
        "amount_match": True,
        "route_hops": [],
        "quote_age_ms": 12.0,
        "token_account_requirements": {},
        "transaction_built": False,
        "simulation_ok": False,
        "route_valid": False,
        "logs_count": 0,
        "signature_fee_lamports": 0,
        "prioritization_fee_lamports": 0,
        "rent_fee_lamports": 0,
        "total_fee_lamports": 0,
        "error": "no route",
    }
    next_retry = first_due + timedelta(seconds=10)
    exact._record_attempt(
        adapter,
        liquidation,
        evidence,
        attempt_number=1,
        next_retry_at=next_retry,
        status="paper_exit_execution_failed",
    )
    with store._lock:
        row = store.db.execute(
            "SELECT actual_position_raw,quote_input_raw,amount_match,status,next_retry_at,error "
            "FROM profit_first_final_exit_execution_attempts WHERE epoch_id='epoch-a'"
        ).fetchone()
    assert row["actual_position_raw"] == 777
    assert row["quote_input_raw"] == 777
    assert row["amount_match"] == 1
    assert row["status"] == "paper_exit_execution_failed"
    assert row["next_retry_at"] == next_retry.isoformat()
    assert row["error"] == "no route"


def test_109_fomo_uses_its_own_scaled_held_size_not_canonical_exit_size(tmp_path):
    store = ObservationEventStore(tmp_path / "fomo-held-size.sqlite3")
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE fomo_paper_trials (release_commit TEXT,source_signature TEXT,decision TEXT,position_fraction REAL)"
        )
        store.db.execute(
            "INSERT INTO fomo_paper_trials VALUES ('release-a','entry-a','paper_enter_bootstrap_probe',0.02)"
        )
    adapter = SimpleNamespace(store=store, release_commit="release-a")
    final_trial = {
        "source_signature": "entry-a",
        "assigned_position_fraction": 0.10,
        "entry_token_raw": 10_000,
        "quote_input_lamports": 1_000_000_000,
        "entry_fee_lamports": 10_000,
    }
    base = {
        "source_signature": "entry-a",
        "token_mint": "mint-a",
        "exit_signal_signature": "sell-a",
        "exit_reason": "trigger_wallet_exit_baseline",
        "exit_features_json": "{}",
        "first_exit_due_at": datetime.now(timezone.utc).isoformat(),
    }
    payload = exact._fomo_liquidation_payload(adapter, final_trial, base)
    assert payload is not None
    assert payload["position_scope"] == "fomo"
    assert payload["actual_position_raw"] == 2_000
    assert payload["actual_position_raw"] != final_trial["entry_token_raw"]


def test_113_new_execution_model_epoch_is_explicit_and_old_epoch_is_incompatible(tmp_path):
    assert exact.EXACT_EXIT_EXECUTION_MODEL_EPOCH == "v51-execution-model-exact-exit-v2"
    assert exact.EXACT_EXIT_EXECUTION_MODEL_EPOCH != exact.LEGACY_EXIT_EXECUTION_MODEL_EPOCH
    assert exact.PAPER_ONLY is True
    assert exact.LIVE_MONEY_AUTHORITY is False
    assert exact.SIGNING_AVAILABLE is False
    assert exact.TRANSACTION_SUBMISSION_AVAILABLE is False

    store = ObservationEventStore(tmp_path / "epoch.sqlite3")
    adapter = SimpleNamespace(store=store, epoch_id="epoch-a", release_commit="release-a")
    exact._ensure_schema(adapter)
    with store._lock:
        columns = {row[1] for row in store.db.execute("PRAGMA table_info(profit_first_final_exit_execution_attempts)").fetchall()}
    assert "execution_model_epoch" in columns
    assert "actual_position_raw" in columns
    assert "quote_input_raw" in columns


def test_113_production_composition_has_exact_exit_installed():
    assert bool(getattr(FinalProfitFirstResearchAdapter._sell, "_roi_exact_exit_execution_v2", False))
