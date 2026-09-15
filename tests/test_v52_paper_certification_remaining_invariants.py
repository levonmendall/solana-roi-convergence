from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v52_authoritative_strategy as strategy
from solana_roi import v52_robinhood_canonical_capital_bridge as bridge
from solana_roi import v52_robinhood_shared_capital_repair as shared
from solana_roi.v51_atomic_paper_capital import capital_reconciliation, reserve_paper_capital
from solana_roi.v52_wallet_alpha_refinement import (
    WalletAlphaRefinementLedger,
    WalletMarginalAlphaObservation,
)


TOKEN = "0x" + "1" * 40
MARKET = "0x" + "2" * 40
EVIDENCE = "e" * 64


class _Owner:
    def __init__(self, store: ObservationEventStore, release: str) -> None:
        self.store = store
        self.release_commit = release
        self.starting_nav_usd = 500.0


def _robinhood_schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE robinhood_paper_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,token TEXT NOT NULL,market TEXT NOT NULL,"
            "position_fraction REAL NOT NULL,capital_reservation_id TEXT)"
        )
        store.db.execute(
            "CREATE TABLE robinhood_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,trial_id INTEGER NOT NULL,paper_nav_multiplier REAL NOT NULL,"
            "paper_only INTEGER NOT NULL DEFAULT 1)"
        )
        store.db.execute(
            "CREATE TABLE v52_robinhood_position_lots ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,position_id INTEGER NOT NULL,trial_id INTEGER NOT NULL UNIQUE,"
            "entry_fraction REAL NOT NULL,remaining_fraction REAL NOT NULL,entry_total_cost_wei TEXT NOT NULL,"
            "remaining_entry_cost_wei TEXT NOT NULL,realized_exit_net_wei TEXT NOT NULL,"
            "capital_reservation_id TEXT,evidence_fingerprint TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE v52_robinhood_position_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,position_id INTEGER NOT NULL,paper_nav_multiplier REAL)"
        )


def test_wallet_alpha_score_excludes_future_observation_at_decision_time(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "wallet-pit.sqlite3")
    ledger = WalletAlphaRefinementLedger(store)
    decision_at = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

    def observation(candidate_id: str, observed_at: datetime, wallet_return: float):
        return WalletMarginalAlphaObservation(
            wallet="wallet-a",
            context_key="pump_fun|bonding_curve|neutral|clean",
            candidate_id=candidate_id,
            observed_at=observed_at,
            wallet_policy_return=wallet_return,
            matched_control_return=0.01,
            executable_mfe=0.20,
            executable_mae=0.02,
            copyable=True,
        )

    assert ledger.record_paired(observation("past", decision_at - timedelta(seconds=1), 0.05)) is True
    assert ledger.record_paired(observation("future", decision_at + timedelta(seconds=1), 10.0)) is True

    at_decision = ledger.score(
        "wallet-a",
        "pump_fun|bonding_curve|neutral|clean",
        as_of=decision_at,
    )
    later = ledger.score(
        "wallet-a",
        "pump_fun|bonding_curve|neutral|clean",
        as_of=decision_at + timedelta(seconds=2),
    )

    assert at_decision.paired_forward_episodes == 1
    assert at_decision.decayed_marginal_alpha < 0.10
    assert later.paired_forward_episodes == 2
    assert later.decayed_marginal_alpha > 1.0
    store.close()


def test_robinhood_restart_recovers_durable_unlinked_lot_without_duplicate_capital(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "rh-crash-recovery.sqlite3")
    _robinhood_schema(store)
    origin = _Owner(store, "release-a")
    restarted = _Owner(store, "release-b")
    shared._ensure_schema(origin)
    bridge.install_v52_robinhood_canonical_capital_bridge()

    reservation_id = f"robinhood-v52:{TOKEN}:{MARKET}:{EVIDENCE}"
    reservation = reserve_paper_capital(
        store,
        release_commit="release-a",
        reservation_id=reservation_id,
        lane="robinhood",
        candidate_id=EVIDENCE,
        requested_fraction=0.20,
        allow_downsize=False,
        minimum_fraction=0.20,
    )
    assert reservation["status"] == "active"

    with store._lock, store.db:
        trial = store.db.execute(
            "INSERT INTO robinhood_paper_trials(release_commit,token,market,position_fraction,capital_reservation_id) "
            "VALUES (?,?,?,?,NULL)",
            ("release-a", TOKEN, MARKET, 0.20),
        )
        trial_id = int(trial.lastrowid)
        store.db.execute(
            "INSERT INTO v52_robinhood_position_lots("
            "position_id,trial_id,entry_fraction,remaining_fraction,entry_total_cost_wei,remaining_entry_cost_wei,"
            "realized_exit_net_wei,capital_reservation_id,evidence_fingerprint) VALUES (?,?,?,?,?,?,?,NULL,?)",
            (1, trial_id, 0.20, 0.20, "1000", "1000", "0", EVIDENCE),
        )

    payload = {"token": TOKEN, "market": MARKET, "fraction": 0.20}
    assert shared._recover_entry_link(
        restarted,
        payload=payload,
        evidence=EVIDENCE,
        reservation_id=reservation_id,
    ) is True
    assert shared._recover_entry_link(
        restarted,
        payload=payload,
        evidence=EVIDENCE,
        reservation_id=reservation_id,
    ) is True

    with store._lock:
        trial_row = store.db.execute(
            "SELECT capital_reservation_id FROM robinhood_paper_trials WHERE id=?",
            (trial_id,),
        ).fetchone()
        lot_row = store.db.execute(
            "SELECT capital_reservation_id FROM v52_robinhood_position_lots WHERE trial_id=?",
            (trial_id,),
        ).fetchone()
        line_count = store.db.execute(
            "SELECT COUNT(*) FROM v52_robinhood_capital_lineage WHERE trial_id=?",
            (trial_id,),
        ).fetchone()[0]
        active_count = store.db.execute(
            "SELECT COUNT(*) FROM v51_paper_capital_reservations WHERE status='active'"
        ).fetchone()[0]

    assert trial_row["capital_reservation_id"] == reservation_id
    assert lot_row["capital_reservation_id"] == reservation_id
    assert line_count == 1
    assert active_count == 1
    state = capital_reconciliation(store, release_commit="release-b")
    assert state["active_reserved_fraction"] == pytest.approx(0.20)
    store.close()


def test_pump_fun_to_pumpswap_is_same_asset_scale_not_second_starter(monkeypatch) -> None:
    prior = [{
        "position_fraction": 0.02,
        "entry_all_in_price_sol": 1.0,
        "venue": "PUMP_FUN",
        "lifecycle": "pump_bonding_curve",
        "lane": "elite_wallet_continuation",
        "risk_severity": 0.0,
        "opportunity_json": "{}",
    }]
    monkeypatch.setattr(strategy, "_ensure_v52_epoch", lambda owner: None)
    monkeypatch.setattr(
        strategy,
        "_v52_solana_target",
        lambda adapter, pre, chase, latency: (
            "graduation_continuation",
            0.08,
            {"graduation_continuation": {}},
        ),
    )
    monkeypatch.setattr(strategy, "_open_solana_rows", lambda adapter, token: prior)
    monkeypatch.setattr(strategy, "_current_entry_price", lambda adapter, pre, chase: 1.10)
    monkeypatch.setattr(strategy, "_record_candidate_state", lambda *args, **kwargs: None)

    lane, fraction, profiles = strategy._v52_solana_choose(
        object(),
        {
            "token": "same-mint",
            "venue": "PUMP_AMM",
            "lifecycle": "early_post_graduation",
            "independent_count": 1,
            "risk": {"risk_severity": 0.0},
        },
        chase=0.10,
        latency=4.0,
    )

    meta = profiles["graduation_continuation"]["v52_authority"]
    assert lane == "graduation_continuation"
    assert fraction == pytest.approx(0.02)
    assert meta["capture_stage"] == "scale"
    assert meta["open_fraction_before"] == pytest.approx(0.02)
    assert meta["target_fraction"] == pytest.approx(0.08)
    assert meta["reason"] == "v52_scale_new_forward_evidence"
