from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.v52_wallet_forward_alpha import STATUS_INCOMPLETE, STATUS_MATERIAL
from solana_roi.v52_wallet_forward_alpha_integration import _no_wallet_pre
from solana_roi.v52_wallet_forward_alpha_runtime import WalletForwardAlphaRuntime
from solana_roi.v52_wallet_forward_alpha_strict_validation import install_strict_wallet_forward_alpha_validation


def _runtime(tmp_path, *, started_at=None):
    install_strict_wallet_forward_alpha_validation()
    store = ObservationEventStore(tmp_path / "wfa-runtime.sqlite3")
    owner = SimpleNamespace(store=store, wallet_discovery=None)
    return store, WalletForwardAlphaRuntime(store, owner, started_at=started_at)


def test_wallet_neutral_control_removes_only_explicit_wallet_lane():
    pre = {
        "wallet": "wallet-a",
        "lanes": (
            "elite_wallet_continuation",
            "graduation_continuation",
            "raydium_cross_venue_persistence",
            "hazard_continuation",
        ),
        "venue": "PUMP_FUN",
        "lifecycle": "bonding_curve",
        "regime": "neutral",
    }
    neutral = _no_wallet_pre(pre)
    assert neutral["wallet"] == ""
    assert "elite_wallet_continuation" not in neutral["lanes"]
    assert neutral["lanes"] == (
        "graduation_continuation",
        "raydium_cross_venue_persistence",
        "hazard_continuation",
    )
    assert pre["wallet"] == "wallet-a"
    assert "elite_wallet_continuation" in pre["lanes"]


def test_real_replay_cannot_shortcut_prospective_24h_7d_30d_windows(tmp_path):
    now = datetime(2026, 9, 14, tzinfo=timezone.utc)
    _store, runtime = _runtime(tmp_path, started_at=now)
    report = runtime.run_real_validation(as_of=now, persist_if_complete=False)
    assert report["acceptance_decision"] == STATUS_INCOMPLETE
    assert report["strategy_influence_enabled"] is False
    reasons = set(report["validation"]["reasons"])
    assert "24h_prospective_runtime_window_not_complete" in reasons
    assert "7d_prospective_runtime_window_not_complete" in reasons
    assert "30d_prospective_runtime_window_not_complete" in reasons
    assert report["portfolio"]["24h"]["paired_same_stream_rows"] == 0
    assert report["reference_portfolio_usd"] == 500.0


def test_same_stream_shadow_outcome_uses_one_realized_outcome_for_all_variants(tmp_path):
    now = datetime(2026, 9, 14, tzinfo=timezone.utc)
    store, runtime = _runtime(tmp_path, started_at=now - timedelta(days=31))
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v52_profit_signal_events("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, source_signature TEXT NOT NULL, lane TEXT NOT NULL, "
            "closed_at TEXT, realized_net_return REAL, realized_mfe REAL, realized_mae REAL, position_fraction REAL)"
        )
    adapter = SimpleNamespace(store=store, release_commit="test-release")
    for index in range(30):
        at = now - timedelta(minutes=60 - index)
        candidate = f"sig-{index}"
        runtime.record_shadow_decision(
            adapter=adapter,
            pre={
                "source_signature": candidate,
                "token": f"mint-{index}",
                "wallet": "wallet-a",
                "received_at": at.isoformat(),
            },
            lane="elite_wallet_continuation",
            current_target=0.05,
            no_wallet_lane=None,
            no_wallet_target=0.0,
            context_key="elite_wallet_continuation|PUMP_FUN|bonding_curve|neutral|clean",
        )
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO v52_profit_signal_events(source_signature,lane,closed_at,realized_net_return,realized_mfe,realized_mae,position_fraction) "
                "VALUES (?,?,?,?,?,?,?)",
                (candidate, "elite_wallet_continuation", (at + timedelta(minutes=1)).isoformat(), 0.10, 0.15, 0.02, 0.0125),
            )
    assert runtime._reconcile_shadow_outcomes(limit=100) == 30
    report = runtime.run_real_validation(as_of=now, persist_if_complete=False)
    rows = report["portfolio"]["24h"]
    assert rows["paired_same_stream_rows"] == 30
    assert rows["execution_realistic_rows"] == 30
    assert rows["baseline_v52_no_wallet"]["trades"] == 30
    assert rows["current_v52_wallet"]["trades"] == 30
    assert rows["wallet_forward_alpha"]["trades"] == 30
    assert rows["baseline_v52_no_wallet"]["net_pnl_usd"] == 0.0
    assert rows["current_v52_wallet"]["net_pnl_usd"] > 0.0
    # With no prior forward-alpha evidence, WFA is neutral to current v5.2 and
    # therefore must NOT be promoted as incremental strategy value.
    assert abs(rows["wallet_forward_alpha"]["net_pnl_usd"] - rows["current_v52_wallet"]["net_pnl_usd"]) < 1e-9
    assert report["acceptance_decision"] != STATUS_MATERIAL
    assert report["strategy_influence_enabled"] is False
    assert "no_statistically_positive_incremental_value_vs_current_wallet_intelligence" in report["validation"]["reasons"]


def test_runtime_schema_is_paper_only_and_has_no_live_authority(tmp_path):
    _store, runtime = _runtime(tmp_path)
    status = runtime.status()
    assert status["installed"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert status["automatic_point_in_time_capture"] is True
    assert status["historical_hindsight_backfill_allowed"] is False
