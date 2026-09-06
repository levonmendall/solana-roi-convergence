from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi.v51_roadmap_59_64_research import (
    build_fomo_signal_half_life_from_rows,
    build_roadmap_59_64_research,
    build_venue_execution_decay_from_rows,
    fomo_latency_band,
    refresh_token_lifecycle_research,
)


NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _row(
    *,
    signature: str,
    venue: str,
    lifecycle: str,
    latency: float,
    net_return: float,
    chase: float | None = 0.05,
    cost: float | None = 0.02,
    risk: str = "clean",
    token: str = "",
) -> dict:
    return {
        "source_signature": signature,
        "independent_event_id": signature,
        "surface": "FOMO" if venue == "FOMO" else "SOLANA",
        "venue": venue,
        "lifecycle": lifecycle,
        "regime": "neutral",
        "risk_class": risk,
        "latency_seconds": latency,
        "chase_fraction": chase,
        "round_trip_cost_fraction": cost,
        "net_return": net_return,
        "token_mint": token,
    }


def test_fomo_latency_band_has_exact_sub_20_boundaries() -> None:
    assert fomo_latency_band(0.0) == "fomo_0_2s"
    assert fomo_latency_band(2.0) == "fomo_0_2s"
    assert fomo_latency_band(2.0001) == "fomo_2_5s"
    assert fomo_latency_band(5.0) == "fomo_2_5s"
    assert fomo_latency_band(10.0) == "fomo_5_10s"
    assert fomo_latency_band(20.0) == "fomo_10_20s"
    assert fomo_latency_band(20.0001) == "fomo_gt_20s"
    assert fomo_latency_band(None) == "unknown"


def test_fomo_half_life_keeps_clean_and_hazard_separate() -> None:
    rows = [
        _row(signature="c1", venue="FOMO", lifecycle="active_fomo", latency=1.0, net_return=0.20, risk="clean"),
        _row(signature="c2", venue="FOMO", lifecycle="active_fomo", latency=1.5, net_return=0.16, risk="clean"),
        _row(signature="c3", venue="FOMO", lifecycle="active_fomo", latency=4.0, net_return=0.12, risk="clean"),
        _row(signature="c4", venue="FOMO", lifecycle="active_fomo", latency=8.0, net_return=0.07, risk="clean"),
        _row(signature="c5", venue="FOMO", lifecycle="active_fomo", latency=15.0, net_return=0.04, risk="clean"),
        _row(signature="h1", venue="FOMO", lifecycle="active_fomo", latency=1.0, net_return=-0.05, risk="hazard"),
        _row(signature="h2", venue="FOMO", lifecycle="active_fomo", latency=6.0, net_return=-0.10, risk="hazard"),
    ]
    proof = build_fomo_signal_half_life_from_rows(rows)

    clean = proof["cohorts"]["clean"]
    hazard = proof["cohorts"]["hazard"]
    assert clean["bands"]["fomo_0_2s"]["sample_count"] == 2
    assert clean["bands"]["fomo_5_10s"]["sample_count"] == 1
    assert clean["half_life"]["state"] == "empirical_bucket_crossing_observed"
    assert clean["half_life"]["first_half_edge_band"] == "fomo_5_10s"
    assert clean["half_life"]["half_life_lower_bound_seconds"] == 5.0
    assert hazard["bands"]["fomo_0_2s"]["sample_count"] == 1
    assert hazard["bands"]["fomo_5_10s"]["sample_count"] == 1
    assert proof["current_authority_changed"] is False
    assert proof["above_20s_paper_entry_authority"] is False


def test_execution_decay_never_pools_pump_amm_raydium_or_fomo_risk() -> None:
    rows = [
        _row(
            signature="pa",
            venue="PUMP_AMM",
            lifecycle="pump_amm_early_post_graduation_30_120s",
            latency=12.0,
            net_return=0.15,
            chase=0.10,
            cost=0.03,
        ),
        _row(
            signature="ray",
            venue="RAYDIUM",
            lifecycle="raydium_post_pump_migration_evidence",
            latency=12.0,
            net_return=-0.02,
            chase=0.10,
            cost=0.03,
        ),
        _row(signature="fc", venue="FOMO", lifecycle="active_fomo", latency=4.0, net_return=0.12, risk="clean"),
        _row(signature="fh", venue="FOMO", lifecycle="active_fomo", latency=4.0, net_return=-0.12, risk="hazard"),
    ]
    proof = build_venue_execution_decay_from_rows(rows)
    keys = {(row["venue"], row["lifecycle"], row["risk_slice"]) for row in proof["segments"]}

    assert ("PUMP_AMM", "pump_amm_early_post_graduation_30_120s", "all") in keys
    assert ("RAYDIUM", "raydium_post_pump_migration_evidence", "all") in keys
    assert ("FOMO", "active_fomo", "clean") in keys
    assert ("FOMO", "active_fomo", "hazard") in keys
    assert proof["pump_amm_and_raydium_pooling_allowed"] is False
    assert proof["fomo_clean_and_hazard_pooling_allowed"] is False
    assert proof["selection_authority"] is False
    assert proof["promotion_authority"] is False


def _seed_lifecycle(store: Store) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,signature TEXT UNIQUE NOT NULL,"
            "token_mint TEXT NOT NULL,received_at TEXT NOT NULL,source TEXT NOT NULL)"
        )
        events = [
            ("pf-1", "token-a", 0, "solana-direct:PUMP_FUN:buy"),
            ("pf-2", "token-a", 10, "solana-direct:PUMP_FUN:buy"),
            ("pa-1", "token-a", 15, "solana-direct:PUMP_AMM:buy"),
            ("pa-2", "token-a", 25, "solana-direct:PUMP_AMM:buy"),
            ("pa-3", "token-a", 75, "solana-direct:PUMP_AMM:buy"),
            ("pa-4", "token-a", 215, "solana-direct:PUMP_AMM:buy"),
            ("ray-1", "token-a", 30, "solana-direct:RAYDIUM:buy"),
            ("pf-b", "token-b", 0, "solana-direct:PUMP_FUN:buy"),
            ("ray-b", "token-b", 5, "solana-direct:RAYDIUM:buy"),
        ]
        for signature, token, seconds, source in events:
            store.db.execute(
                "INSERT INTO wallet_discovery_forward_observations(signature,token_mint,received_at,source) VALUES (?,?,?,?)",
                (signature, token, (NOW + timedelta(seconds=seconds)).isoformat(), source),
            )


def test_pump_fun_to_pump_amm_transition_becomes_persistent_event_without_fabricated_timestamp() -> None:
    store = Store()
    _seed_lifecycle(store)
    execution_rows = [
        _row(
            signature="pa-outcome",
            venue="PUMP_AMM",
            lifecycle="pump_amm_immediate_graduation_0_30s",
            latency=8.0,
            net_return=0.14,
            chase=0.08,
            cost=0.025,
            token="token-a",
        )
    ]
    proof = refresh_token_lifecycle_research(store, execution_rows)

    assert proof["event_count"] == 1
    event = proof["graduation_events"][0]
    assert event["token_mint"] == "token-a"
    assert event["last_pump_fun_observed_at"] == (NOW + timedelta(seconds=10)).isoformat()
    assert event["first_pump_amm_observed_at"] == (NOW + timedelta(seconds=15)).isoformat()
    assert event["graduation_timestamp"] is None
    assert event["graduation_timestamp_source"] == "inferred_transition_window_only"
    assert event["first_raydium_observed_at"] == (NOW + timedelta(seconds=30)).isoformat()
    assert event["raydium_evidence_pooling_allowed"] is False
    assert event["pump_amm_post_transition_observation_counts"] == {
        "0_30s": 2,
        "30_120s": 1,
        "120_300s": 1,
        "gt_300s": 0,
    }
    assert event["post_transition_execution_profile"]["sample_count"] == 1
    assert event["post_transition_execution_profile"]["mean_net_return"] == 0.14
    assert event["post_transition_execution_profile"]["mean_round_trip_cost_fraction"] == 0.025
    assert event["forward_price_horizon_measurement_available"] is True

    with store._lock:
        persisted = store.db.execute(
            "SELECT token_mint,graduation_timestamp_source,paper_only,live_money_authority "
            "FROM v51_token_lifecycle_research_events"
        ).fetchall()
    assert len(persisted) == 1
    assert persisted[0]["token_mint"] == "token-a"
    assert persisted[0]["graduation_timestamp_source"] == "inferred_transition_window_only"
    assert persisted[0]["paper_only"] == 1
    assert persisted[0]["live_money_authority"] == 0


def test_explicit_graduation_marker_can_supply_exact_timestamp() -> None:
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,signature TEXT UNIQUE NOT NULL,"
            "token_mint TEXT NOT NULL,received_at TEXT NOT NULL,source TEXT NOT NULL)"
        )
        for signature, seconds, source in (
            ("pf", 0, "solana-direct:PUMP_FUN:buy"),
            ("grad", 9, "solana-direct:PUMP_FUN:GRADUATION"),
            ("pa", 10, "solana-direct:PUMP_AMM:buy"),
        ):
            store.db.execute(
                "INSERT INTO wallet_discovery_forward_observations(signature,token_mint,received_at,source) VALUES (?,?,?,?)",
                (signature, "token-g", (NOW + timedelta(seconds=seconds)).isoformat(), source),
            )
    proof = refresh_token_lifecycle_research(store)
    event = proof["graduation_events"][0]
    assert event["graduation_timestamp"] == (NOW + timedelta(seconds=9)).isoformat()
    assert event["graduation_timestamp_source"] == "explicit_observation"


def test_combined_59_64_plane_preserves_v51_authority() -> None:
    store = Store()
    _seed_lifecycle(store)
    proof = build_roadmap_59_64_research(store)

    assert set(proof["items"]) == {
        "59_fomo_signal_half_life",
        "60_pump_fun_first_slot_policy",
        "61_cross_venue_token_lifecycle",
        "62_graduation_event_cluster",
        "63_raydium_pumpswap_isolation",
        "64_venue_specific_execution_decay",
    }
    assert proof["items"]["60_pump_fun_first_slot_policy"]["first_slot_promotion_authority"] is False
    assert proof["items"]["63_raydium_pumpswap_isolation"]["pump_amm_and_raydium_pooling_allowed"] is False
    assert proof["current_authority_changed"] is False
    assert proof["latency_hard_max_seconds_changed"] is False
    assert proof["above_20s_paper_entry_authority"] is False
    assert proof["selection_authority"] is False
    assert proof["sizing_authority"] is False
    assert proof["exit_authority"] is False
    assert proof["promotion_authority"] is False
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
    assert proof["signing_available"] is False
    assert proof["transaction_submission_available"] is False
