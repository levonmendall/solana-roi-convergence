from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi.strategy_v52_authority import authority
from solana_roi import v52_learning_governance as governance


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.events: list[tuple[str, str, dict]] = []

    def append(self, kind: str, observed_at: str, payload: dict) -> None:
        self.events.append((kind, observed_at, dict(payload)))


def _create_wallet_lead_table(store: Store) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v52_wallet_lead_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, context_key TEXT NOT NULL, lane TEXT NOT NULL, "
            "net_return REAL NOT NULL, executable_mfe REAL NOT NULL, executable_mae REAL NOT NULL, capture_ratio REAL, "
            "lead_seconds REAL, alpha_life_seconds REAL, created_at TEXT NOT NULL)"
        )


def test_authority_enables_only_governed_forward_automatic_evolution() -> None:
    policy = authority()
    g = policy["governance"]
    assert g["historical_promotion_authority"] is False
    assert g["automatic_parameter_mutation_authority"] is True
    assert g["automatic_signal_promotion_authority"] is True
    assert g["automatic_forward_promotion_authority"] is True
    assert g["automatic_forward_demotion_authority"] is True
    assert g["promotion_requires_same_stream_forward_evidence"] is True
    assert g["automatic_challengers_are_analytical_only_until_promotion"] is True
    assert g["automatic_evolution_min_paired_forward_episodes"] >= 30
    assert g["automatic_evolution_min_posterior_probability"] >= 0.95
    assert policy["paper_only"] is True
    assert policy["live_money_authority"] is False
    assert policy["signing_available"] is False
    assert policy["transaction_submission_available"] is False


def test_lane_decay_uses_distinct_priors_and_learns_from_forward_alpha_life() -> None:
    store = Store()
    governance._schema(store)
    pump = governance.lane_decay_profile(store, "elite_wallet_continuation")
    robinhood = governance.lane_decay_profile(store, "robinhood")
    assert pump["half_life_hours"] != robinhood["half_life_hours"]
    _create_wallet_lead_table(store)
    now = datetime.now(timezone.utc)
    with store._lock, store.db:
        for i in range(12):
            store.db.execute(
                "INSERT INTO v52_wallet_lead_outcomes(wallet,context_key,lane,net_return,executable_mfe,executable_mae,capture_ratio,lead_seconds,alpha_life_seconds,created_at) "
                "VALUES ('w','ctx','elite_wallet_continuation',0.20,0.30,0.05,0.66,10,14400,?)",
                ((now - timedelta(minutes=i)).isoformat(),),
            )
    learned = governance.lane_decay_profile(store, "elite_wallet_continuation")
    assert learned["learned"] is True
    assert learned["sample_count"] == 12
    assert 2.0 <= learned["half_life_hours"] <= 168.0


def test_bayesian_posterior_requires_credible_forward_edge() -> None:
    store = Store()
    governance._schema(store)
    _create_wallet_lead_table(store)
    now = datetime.now(timezone.utc)
    with store._lock, store.db:
        for i in range(45):
            store.db.execute(
                "INSERT INTO v52_wallet_lead_outcomes(wallet,context_key,lane,net_return,executable_mfe,executable_mae,capture_ratio,lead_seconds,alpha_life_seconds,created_at) "
                "VALUES ('lead','ctx','elite_wallet_continuation',0.40,0.50,0.04,0.80,8,7200,?)",
                ((now - timedelta(minutes=i)).isoformat(),),
            )
    posterior = governance.bayesian_wallet_posterior(store, "lead", "ctx", "elite_wallet_continuation")
    assert posterior["samples"] >= 30
    assert posterior["posterior_probability_positive"] > 0.95
    assert posterior["posterior_return_lower_90"] > 0.0
    assert posterior["credible_positive_edge"] is True
    assert posterior["lane_half_life_hours"] != 72.0


def test_wallet_distribution_reversal_detects_trigger_departure_and_coordinated_sells() -> None:
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations (token_mint TEXT, wallet TEXT, side TEXT, received_at TEXT)"
        )
    now = datetime.now(timezone.utc)
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO wallet_discovery_forward_observations(token_mint,wallet,side,received_at) VALUES ('token',?,?,?)",
            [
                ("lead", "sell", (now - timedelta(seconds=10)).isoformat()),
                ("peer", "sell", (now - timedelta(seconds=8)).isoformat()),
                ("buyer", "buy", (now - timedelta(seconds=20)).isoformat()),
            ],
        )
    signal = governance.wallet_distribution_reversal(store, "token", "lead", now)
    assert signal["triggered"] is True
    assert signal["trigger_wallet_departed"] is True
    assert signal["distinct_sellers_90s"] == 2


def test_six_named_challengers_are_concurrent_analytical_only() -> None:
    store = Store()
    governance.ensure_named_challengers(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT challenger_id,status,analytical_only,paper_only,live_money_authority FROM v52_governed_challengers ORDER BY challenger_id"
        ).fetchall()
    assert len(rows) == 6
    assert {row["challenger_id"] for row in rows} == set(governance.FIXED_CHALLENGERS)
    assert all(row["status"] == "active" for row in rows)
    assert all(row["analytical_only"] == 1 for row in rows)
    assert all(row["paper_only"] == 1 for row in rows)
    assert all(row["live_money_authority"] == 0 for row in rows)


def test_tournament_promotion_requires_same_stream_posterior_superiority() -> None:
    store = Store()
    governance.ensure_named_challengers(store)
    now = datetime.now(timezone.utc)
    policies = [governance.INCUMBENT_ID, *governance.FIXED_CHALLENGERS]
    with store._lock, store.db:
        for i in range(36):
            stream = f"stream-{i}"
            for policy in policies:
                if policy == governance.INCUMBENT_ID:
                    ret = 0.005
                elif policy == "aggressive_sizing":
                    ret = 0.20
                else:
                    ret = 0.006
                store.db.execute(
                    "INSERT INTO v52_tournament_outcomes(challenger_id,stream_id,lane,observed_at,net_return,drawdown,execution_complete,same_stream,prospective) "
                    "VALUES (?,?,?,?,?,0.05,1,1,1)",
                    (policy, stream, "elite_wallet_continuation", (now + timedelta(seconds=i)).isoformat(), ret),
                )
    decision, posterior = governance.evaluate_tournament(store)
    assert decision.eligible is True
    assert decision.winner == "aggressive_sizing"
    assert posterior["episodes"] >= 30
    assert posterior["probability_positive"] >= governance.POSTERIOR_PROMOTION_PROBABILITY
    assert posterior["lower_90"] > 0.0


def test_automatic_challenger_generation_is_bounded_to_evolvable_families() -> None:
    store = Store()
    governance._schema(store)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v52_counterfactual_decisions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, source_signature TEXT, lane TEXT, observed_at TEXT, resolved_at TEXT, "
            "hypothetical_fraction REAL, net_return REAL, executable_mae REAL, chase_fraction REAL, reason TEXT, "
            "opportunity_cost_usd REAL, avoided_loss_usd REAL)"
        )
        now = datetime.now(timezone.utc).isoformat()
        for i in range(10):
            store.db.execute(
                "INSERT INTO v52_counterfactual_decisions(source_signature,lane,observed_at,resolved_at,hypothetical_fraction,net_return,executable_mae,chase_fraction,reason,opportunity_cost_usd,avoided_loss_usd) "
                "VALUES (?,?,?, ?,0.01,0.40,0.05,0.42,'deferred_chase_limit',5.0,0.1)",
                (f"s{i}", "elite_wallet_continuation", now, now),
            )
    created = governance.generate_challengers_from_missed_opportunities(store)
    assert len(created) == 1
    with store._lock:
        row = store.db.execute(
            "SELECT family,analytical_only,paper_only,live_money_authority FROM v52_governed_challengers WHERE challenger_id=?",
            (created[0],),
        ).fetchone()
    assert row["family"] == "chase_optimization"
    assert row["analytical_only"] == 1
    assert row["paper_only"] == 1
    assert row["live_money_authority"] == 0


def test_learning_governance_status_reports_all_six_completed_pieces() -> None:
    payload = governance.status()
    assert payload["bayesian_posterior_confidence"] is True
    assert payload["wallet_distribution_reversal_primary_exit_signal"] is True
    assert payload["lane_specific_learned_decay"] is True
    assert payload["automatic_challenger_generation"] is True
    assert payload["concurrent_named_same_stream_tournament"] is True
    assert payload["automatic_forward_promotion"] is True
    assert payload["automatic_forward_demotion"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
