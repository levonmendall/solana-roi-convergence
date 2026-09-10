from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi import v52_learning_governance as governance
from solana_roi import v52_learning_governance_hardening as hardening


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.events: list[tuple[str, str, dict]] = []

    def append(self, kind: str, observed_at: str, payload: dict) -> None:
        self.events.append((kind, observed_at, dict(payload)))


def test_auto_challenger_id_is_restart_stable() -> None:
    first = hardening._stable_id("chase_optimization", "pump_fun", "deferred_chase_limit")
    second = hardening._stable_id("chase_optimization", "pump_fun", "deferred_chase_limit")
    other = hardening._stable_id("chase_optimization", "pump_fun", "another_reason")
    assert first == second
    assert first != other
    assert first.startswith("auto_chase_optimization_pump_fun_")


def test_heuristic_challenger_outcome_never_counts_as_exact_execution() -> None:
    stream = {
        "net_return": 0.50,
        "position_fraction": 0.02,
        "entered": True,
        "reason": "qualified",
        "chase_fraction": 0.10,
    }
    incumbent_return, incumbent_complete = hardening._strict_policy_stream_return(
        governance.INCUMBENT_ID, governance.INCUMBENT_ID, stream
    )
    challenger_return, challenger_complete = hardening._strict_policy_stream_return(
        "aggressive_sizing", "aggressive_sizing", stream
    )
    assert incumbent_return == pytest.approx(0.01)
    assert incumbent_complete is True
    assert challenger_return > incumbent_return
    assert challenger_complete is False


def test_incumbent_no_entry_is_a_completed_zero_return_decision() -> None:
    value, complete = hardening._strict_policy_stream_return(
        governance.INCUMBENT_ID,
        governance.INCUMBENT_ID,
        {
            "net_return": 0.75,
            "position_fraction": 0.01,
            "entered": False,
            "reason": "deferred_chase_limit",
            "chase_fraction": 0.42,
        },
    )
    assert value == 0.0
    assert complete is True


def test_exact_challenger_evidence_upgrades_only_paired_stream() -> None:
    store = Store()
    governance.ensure_named_challengers(store)
    hardening._exact_schema(store)
    observed = datetime.now(timezone.utc).isoformat()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
            "VALUES (?,?,?,?,0.0,0.0,1,1,1)",
            (governance.INCUMBENT_ID, "stream-1", "elite_wallet_continuation", observed),
        )
        store.db.execute(
            "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
            "VALUES (?,?,?,?,0.05,0.02,0,1,1)",
            ("aggressive_sizing", "stream-1", "elite_wallet_continuation", observed),
        )
    inserted = hardening.record_exact_challenger_outcome(
        store,
        challenger_id="aggressive_sizing",
        stream_id="stream-1",
        lane="elite_wallet_continuation",
        observed_at=observed,
        net_return=0.04,
        drawdown=0.01,
        evidence_ref="exact-paper-fill:stream-1:aggressive_sizing",
    )
    assert inserted is True
    with store._lock:
        row = store.db.execute(
            "SELECT net_return,drawdown,execution_complete,same_stream,prospective FROM v52_tournament_outcomes "
            "WHERE challenger_id='aggressive_sizing' AND stream_id='stream-1'"
        ).fetchone()
        evidence = store.db.execute(
            "SELECT evidence_ref FROM v52_tournament_exact_evidence "
            "WHERE challenger_id='aggressive_sizing' AND stream_id='stream-1'"
        ).fetchone()
    assert row["net_return"] == pytest.approx(0.04)
    assert row["drawdown"] == pytest.approx(0.01)
    assert row["execution_complete"] == 1
    assert row["same_stream"] == 1
    assert row["prospective"] == 1
    assert evidence["evidence_ref"] == "exact-paper-fill:stream-1:aggressive_sizing"


def test_exact_challenger_evidence_requires_incumbent_pair() -> None:
    store = Store()
    governance.ensure_named_challengers(store)
    with pytest.raises(ValueError, match="same-stream incumbent outcome"):
        hardening.record_exact_challenger_outcome(
            store,
            challenger_id="aggressive_sizing",
            stream_id="missing",
            lane="elite_wallet_continuation",
            observed_at=datetime.now(timezone.utc).isoformat(),
            net_return=0.10,
            drawdown=0.01,
            evidence_ref="exact:missing",
        )


def test_fresh_tournament_ignores_pre_epoch_outcomes() -> None:
    store = Store()
    governance._schema(store)
    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_governed_challengers(challenger_id,family,trigger_reason,config_json,created_at,status,analytical_only,paper_only,live_money_authority,evidence_json) "
            "VALUES ('only','aggressive_sizing','test','{}',?,'active',1,1,0,'{}')",
            (created_at,),
        )
        for index in range(40):
            stream = f"old-{index}"
            observed = (now - timedelta(days=1)).isoformat()
            store.db.execute(
                "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
                "VALUES (?,?,?,?,?,0.01,1,1,1)",
                (governance.INCUMBENT_ID, stream, "pump_fun", observed, 0.001),
            )
            store.db.execute(
                "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
                "VALUES (?,?,?,?,?,0.01,1,1,1)",
                ("only", stream, "pump_fun", observed, 0.20),
            )
    decision, posterior = hardening.evaluate_fresh_tournament(store)
    assert decision.eligible is False
    assert "insufficient_paired_forward_episodes" in decision.blockers
    assert posterior["episodes"] == 0


def test_heuristic_only_tournament_cannot_promote() -> None:
    store = Store()
    governance.ensure_named_challengers(store)
    now = datetime.now(timezone.utc) + timedelta(seconds=1)
    with store._lock, store.db:
        for index in range(40):
            stream = f"h-{index}"
            observed = (now + timedelta(seconds=index)).isoformat()
            store.db.execute(
                "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
                "VALUES (?,?,?,?,0.001,0.01,1,1,1)",
                (governance.INCUMBENT_ID, stream, "pump_fun", observed),
            )
            for challenger in governance.FIXED_CHALLENGERS:
                store.db.execute(
                    "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
                    "VALUES (?,?,?,?,0.20,0.01,0,1,1)",
                    (challenger, stream, "pump_fun", observed),
                )
    decision, posterior = hardening.evaluate_fresh_tournament(store)
    assert decision.eligible is False
    assert posterior["episodes"] == 0
    assert any("exact" in blocker or "completion" in blocker for blocker in decision.blockers)


def test_demotion_requires_latest_action_to_be_active_promotion() -> None:
    store = Store()
    governance._schema(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_strategy_governance_history(action,challenger_id,observed_at,from_fingerprint,to_fingerprint,changes_json,rollback_json,evidence_json,paper_only,live_money_authority) "
            "VALUES ('promote','aggressive_sizing',?,'a','b','{}','{}','{}',1,0)",
            (datetime.now(timezone.utc).isoformat(),),
        )
        store.db.execute(
            "INSERT INTO v52_strategy_governance_history(action,challenger_id,observed_at,from_fingerprint,to_fingerprint,changes_json,rollback_json,evidence_json,paper_only,live_money_authority) "
            "VALUES ('demote','aggressive_sizing',?,'b','a','{}','{}','{}',1,0)",
            ((datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),),
        )
    result = hardening.demote_once_with_fresh_evidence(store)
    assert result == {"action": "hold", "reason": "no_active_promotion"}


def test_hardening_status_preserves_authority_boundary() -> None:
    payload = hardening.status()
    assert payload["stable_auto_challenger_ids"] is True
    assert payload["fresh_same_stream_epoch_after_promotion"] is True
    assert payload["old_forward_evidence_reuse_for_next_promotion"] is False
    assert payload["heuristic_challenger_estimates_may_promote"] is False
    assert payload["exact_executable_challenger_evidence_required_for_promotion"] is True
    assert payload["exact_post_promotion_evidence_required_for_demotion"] is True
    assert payload["repeat_demotion_of_same_promotion_prevented"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
