from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from solana_roi import canonical_production_architecture as canonical
from solana_roi.execution_transfer_certification import V4ExecutionTransferCertification
from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import UNIFIED_LANE
from solana_roi import render_runtime_bootstrap_repair as bootstrap


def test_canonical_readiness_removes_legacy_webhook_authority(monkeypatch):
    monkeypatch.delenv(canonical.LEGACY_HELIUS_COMPAT_ENV, raising=False)
    monkeypatch.setattr(
        canonical,
        "_ORIGINAL_BASE_READINESS",
        lambda _self: {
            "requirements": {
                "direct_full_scope_stream_continuity": True,
                "amount_specific_quote_certified": True,
                "durable_webhook_queue_drained": False,
            },
            "passed": False,
            "webhook_queue": {"pending": 99},
        },
    )
    status = canonical._canonical_base_readiness(object())
    assert status["passed"] is True
    assert "durable_webhook_queue_drained" not in status["requirements"]
    assert status["canonical_data_plane"] == "direct-standard-solana"
    assert status["legacy_webhook_queue_has_readiness_authority"] is False
    assert status["legacy_helius_compat_enabled"] is False


class _Worker:
    def __init__(self):
        self.calls = 0

    async def run(self, stop):
        self.calls += 1
        await stop.wait()


async def _exercise_workers(monkeypatch, *, legacy: bool):
    if legacy:
        monkeypatch.setenv(canonical.LEGACY_HELIUS_COMPAT_ENV, "true")
    else:
        monkeypatch.delenv(canonical.LEGACY_HELIUS_COMPAT_ENV, raising=False)
    webhook = _Worker()
    direct = _Worker()
    wallet = _Worker()
    runtime = SimpleNamespace(
        webhook_worker=webhook,
        direct_ingestion=direct,
        wallet_discovery=wallet,
        price_clock=_Worker(),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(bootstrap._run_runtime_workers(runtime, stop))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    stop.set()
    await task
    return webhook.calls, direct.calls, wallet.calls


def test_legacy_helius_worker_is_off_by_default(monkeypatch):
    webhook, direct, wallet = asyncio.run(_exercise_workers(monkeypatch, legacy=False))
    assert webhook == 0
    assert direct == 1
    assert wallet == 1


def test_legacy_helius_worker_remains_explicit_opt_in_compatibility(monkeypatch):
    webhook, direct, wallet = asyncio.run(_exercise_workers(monkeypatch, legacy=True))
    assert webhook == 1
    assert direct == 1
    assert wallet == 1


def _schema(store):
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE profit_first_final_epochs (epoch_id TEXT PRIMARY KEY, started_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_trials ("
            "epoch_id TEXT NOT NULL,source_signature TEXT NOT NULL,lane TEXT NOT NULL,received_at TEXT NOT NULL,"
            "regime TEXT NOT NULL,decision_json TEXT NOT NULL,opportunity_json TEXT NOT NULL,"
            "quote_input_lamports INTEGER,entry_fee_lamports INTEGER,entry_all_in_price_sol REAL,"
            "round_trip_cost_fraction REAL,quote_latency_ms REAL,entry_executable INTEGER,exit_executable INTEGER)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,epoch_id TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "token_mint TEXT NOT NULL,trigger_wallet TEXT NOT NULL,lane TEXT NOT NULL,entry_observed_at TEXT NOT NULL,"
            "exit_observed_at TEXT NOT NULL,signal_to_entry_seconds REAL NOT NULL,position_fraction REAL NOT NULL,"
            "entry_cost_sol REAL NOT NULL,exit_net_sol REAL NOT NULL,net_return REAL NOT NULL,exit_reason TEXT NOT NULL,"
            "evidence_phase TEXT NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO profit_first_final_epochs(epoch_id,started_at) VALUES ('epoch-a','2026-09-03T00:00:00+00:00')"
        )


def _insert_episode(store, index: int, *, lane: str, selected: bool = True):
    signature = f"sig-{index}"
    token = f"mint-{index}"
    wallet = f"wallet-{index}"
    entity = f"entity-{index}"
    decision = "paper_enter" if selected else "shadow"
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO profit_first_final_trials("
            "epoch_id,source_signature,lane,received_at,regime,decision_json,opportunity_json,quote_input_lamports,"
            "entry_fee_lamports,entry_all_in_price_sol,round_trip_cost_fraction,quote_latency_ms,entry_executable,exit_executable) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "epoch-a",signature,lane,"2026-09-03T00:00:01+00:00","neutral",
                json.dumps({"decision": decision}),json.dumps({"trigger_entity": entity}),1_000_000_000,
                10_000,1.0,0.02,100.0,1,1,
            ),
        )
        store.db.execute(
            "INSERT INTO profit_first_final_outcomes("
            "epoch_id,source_signature,token_mint,trigger_wallet,lane,entry_observed_at,exit_observed_at,"
            "signal_to_entry_seconds,position_fraction,entry_cost_sol,exit_net_sol,net_return,exit_reason,evidence_phase) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "epoch-a",signature,token,wallet,lane,"2026-09-03T00:00:00+00:00","2026-09-03T00:01:00+00:00",
                2.0,0.01,1.00001,1.100011,0.10,"test_exit","forward",
            ),
        )


def test_parallel_lane_rows_cannot_fake_the_300_episode_gate(tmp_path):
    store = ObservationEventStore(tmp_path / "five-lane.sqlite3")
    _schema(store)
    lanes = [UNIFIED_LANE, "clean_scout_alpha", "elite_wallet_continuation", "creator_insider_continuation", "entity_flow_momentum"]
    # 60 real market episodes x five rows = 300 outcome rows. Certification must
    # still see only 60 independent policy-selected unified episodes.
    for index in range(60):
        for lane in lanes:
            _insert_episode(store, index, lane=lane)
    status = V4ExecutionTransferCertification(store).status("epoch-a")
    assert status["research_forward_outcome_rows"] == 60
    assert status["policy_selected_closed_episodes"] == 60
    assert status["certified"] is False
    assert "minimum_300_policy_selected_closed_episodes_not_met" in status["blockers"]


def test_300_independent_policy_selected_stressed_profitable_episodes_can_certify(tmp_path):
    store = ObservationEventStore(tmp_path / "certified.sqlite3")
    _schema(store)
    for index in range(300):
        _insert_episode(store, index, lane=UNIFIED_LANE)
    status = V4ExecutionTransferCertification(store).status("epoch-a")
    assert status["policy_selected_closed_episodes"] == 300
    assert status["unique_tokens"] == 300
    assert status["execution_transfer"]["complete_samples"] == 300
    assert status["execution_transfer"]["execution_stressed_net_pnl_usd"] > 0
    assert status["robustness"]["pnl_ex_top_5_percent_winners_usd"] > 0
    assert status["blockers"] == []
    assert status["certified"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
