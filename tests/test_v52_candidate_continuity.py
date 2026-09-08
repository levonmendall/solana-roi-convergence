from __future__ import annotations

import sqlite3

import pytest

from solana_roi import v52_candidate_continuity as continuity


T0 = "2026-09-08T20:00:00+00:00"
T1 = "2026-09-08T20:00:01+00:00"
T2 = "2026-09-08T20:00:02+00:00"
T3 = "2026-09-08T20:00:03+00:00"
T4 = "2026-09-08T20:00:04+00:00"


def _candidate() -> continuity.CandidateLifecycle:
    return continuity.new_candidate(
        candidate_id="candidate:mint-v52",
        asset_id="mint-v52",
        surface="PUMP_FUN",
        observed_at=T0,
        source_signature="sig-discovery",
        route_id="pump-fun-curve",
    )


def _quote() -> continuity.ExecutableQuoteSnapshot:
    return continuity.ExecutableQuoteSnapshot(
        entry_price=1.05,
        exit_price=1.03,
        quote_timestamp=T1,
        entry_exact=True,
        exit_exact=True,
        structurally_exitable=True,
    )


def _advance(candidate, *states):
    current = candidate
    for index, state in enumerate(states, start=1):
        current = continuity.transition(
            current,
            state,
            observed_at=f"2026-09-08T20:01:{index:02d}+00:00",
        )
    return current


def test_batch1_safety_manifest_preserves_incumbent_and_zero_authority() -> None:
    manifest = continuity.safety_manifest()

    assert manifest["batch_version"] == "v52-batch1-candidate-continuity-1"
    assert manifest["challenger_version"] == "roi-convergence-v5.2-continuation-capture-1"
    assert manifest["challenger_epoch"] == "v52-weekend-review-20260908"
    assert manifest["incumbent_version"] == "roi-convergence-v5.1-context-exactness-1"
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["research_only"] is True
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False
    assert manifest["production_composition_hook"] is False


def test_full_canonical_lifecycle_survives_exit_and_reentry_watch() -> None:
    candidate = _advance(
        _candidate(),
        "developing",
        "pre_breakout",
        "actionable",
        "entered",
        "scaling",
        "partial_exit",
        "runner",
        "exited",
        "reentry_watch",
        "actionable",
    )

    assert candidate.candidate_id == "candidate:mint-v52"
    assert candidate.asset_id == "mint-v52"
    assert candidate.state == "actionable"
    assert candidate.last_valid_state == "actionable"
    assert candidate.lifecycle_sequence == 10
    assert candidate.entry_authority is False
    assert candidate.research_only is True


def test_candidate_identity_survives_pumpfun_graduation_pumpswap_and_secondary_pool() -> None:
    candidate = _candidate()
    candidate = continuity.record_surface_transition(
        candidate,
        surface="PUMPSWAP",
        event_type="graduated",
        observed_at=T1,
        route_id="pump-amm-pool",
        source_signature="sig-graduation",
    )
    candidate = continuity.record_surface_transition(
        candidate,
        surface="RAYDIUM",
        event_type="secondary_pool_route_available",
        observed_at=T2,
        route_id="raydium-pool",
        source_signature="sig-secondary",
    )

    assert candidate.candidate_id == "candidate:mint-v52"
    assert candidate.asset_id == "mint-v52"
    assert candidate.current_surface == "RAYDIUM"
    assert [row.surface for row in candidate.surface_history] == [
        "PUMP_FUN",
        "PUMPSWAP",
        "RAYDIUM",
    ]
    assert candidate.source_signatures == (
        "sig-discovery",
        "sig-graduation",
        "sig-secondary",
    )


def test_temporary_rejection_retains_all_required_context_and_does_not_reset_state() -> None:
    candidate = _advance(_candidate(), "developing", "pre_breakout")
    rejected = continuity.reject_temporarily(
        candidate,
        blocker_reason="liquidity_temporarily_below_actionable_floor",
        latest_exact_executable_quote=_quote(),
        distance_to_actionable=0.17,
        active_event_subscriptions=("liquidity_changed", "quote_refreshed"),
        rejected_at=T2,
    )

    assert rejected.state == "pre_breakout"
    assert rejected.last_valid_state == "pre_breakout"
    assert rejected.temporarily_rejected is True
    assert rejected.rejection is not None
    assert rejected.rejection.last_valid_state == "pre_breakout"
    assert rejected.rejection.blocker_reason == "liquidity_temporarily_below_actionable_floor"
    assert rejected.rejection.latest_exact_executable_quote == _quote().validated()
    assert rejected.rejection.quote_timestamp == _quote().validated().quote_timestamp
    assert rejected.rejection.distance_to_actionable == pytest.approx(0.17)
    assert rejected.rejection.active_event_subscriptions == (
        "liquidity_changed",
        "quote_refreshed",
    )


def test_temporary_rejection_requires_exact_executable_quote_and_subscriptions() -> None:
    candidate = _candidate()
    bad_quote = continuity.ExecutableQuoteSnapshot(
        entry_price=1.0,
        exit_price=0.99,
        quote_timestamp=T1,
        entry_exact=True,
        exit_exact=False,
        structurally_exitable=True,
    )

    with pytest.raises(ValueError, match="exact_exit_quote_required"):
        continuity.reject_temporarily(
            candidate,
            blocker_reason="quote_blocker",
            latest_exact_executable_quote=bad_quote,
            distance_to_actionable=0.1,
            active_event_subscriptions=("quote_refreshed",),
            rejected_at=T2,
        )

    with pytest.raises(ValueError, match="temporary_rejection_event_subscriptions_required"):
        continuity.reject_temporarily(
            candidate,
            blocker_reason="liquidity_blocker",
            latest_exact_executable_quote=_quote(),
            distance_to_actionable=0.1,
            active_event_subscriptions=(),
            rejected_at=T2,
        )


def test_unsubscribed_event_preserves_temporary_rejection() -> None:
    rejected = continuity.reject_temporarily(
        _advance(_candidate(), "developing"),
        blocker_reason="waiting_for_liquidity",
        latest_exact_executable_quote=_quote(),
        distance_to_actionable=0.2,
        active_event_subscriptions=("liquidity_changed",),
        rejected_at=T2,
    )
    result = continuity.process_event(
        rejected,
        continuity.LifecycleEvent(
            candidate_id=rejected.candidate_id,
            event_type="wallet_cascade_changed",
            observed_at=T3,
            blocker_resolved=True,
        ),
    )

    assert result.reevaluation_triggered is False
    assert result.rejection_cleared is False
    assert result.candidate.temporarily_rejected is True
    assert result.candidate.state == "developing"
    assert result.reason == "event_not_subscribed_for_reevaluation"


def test_subscribed_event_triggers_reevaluation_without_resetting_when_blocker_remains() -> None:
    rejected = continuity.reject_temporarily(
        _advance(_candidate(), "developing"),
        blocker_reason="waiting_for_liquidity",
        latest_exact_executable_quote=_quote(),
        distance_to_actionable=0.2,
        active_event_subscriptions=("liquidity_changed",),
        rejected_at=T2,
    )
    result = continuity.process_event(
        rejected,
        continuity.LifecycleEvent(
            candidate_id=rejected.candidate_id,
            event_type="liquidity_changed",
            observed_at=T3,
            blocker_resolved=False,
        ),
    )

    assert result.reevaluation_triggered is True
    assert result.rejection_cleared is False
    assert result.candidate.state == "developing"
    assert result.candidate.temporarily_rejected is True


def test_subscribed_event_reactivates_and_can_advance_retained_candidate() -> None:
    rejected = continuity.reject_temporarily(
        _advance(_candidate(), "developing"),
        blocker_reason="waiting_for_pre_breakout_evidence",
        latest_exact_executable_quote=_quote(),
        distance_to_actionable=0.08,
        active_event_subscriptions=("buyer_acceleration_changed",),
        rejected_at=T2,
    )
    result = continuity.process_event(
        rejected,
        continuity.LifecycleEvent(
            candidate_id=rejected.candidate_id,
            event_type="buyer_acceleration_changed",
            observed_at=T3,
            target_state="pre_breakout",
            blocker_resolved=True,
            source_signature="sig-reactivation",
        ),
    )

    assert result.reevaluation_triggered is True
    assert result.rejection_cleared is True
    assert result.state_changed is True
    assert result.candidate.temporarily_rejected is False
    assert result.candidate.state == "pre_breakout"
    assert result.candidate.candidate_id == rejected.candidate_id
    assert "sig-reactivation" in result.candidate.source_signatures


def test_permanent_rejection_is_terminal_and_clears_subscriptions() -> None:
    rejected = continuity.reject_permanently(
        _advance(_candidate(), "developing"),
        blocker_reason="permanent_structural_exit_failure",
        rejected_at=T2,
    )

    assert rejected.permanently_rejected is True
    assert rejected.state == "developing"
    assert rejected.rejection is not None
    assert rejected.rejection.active_event_subscriptions == ()

    result = continuity.process_event(
        rejected,
        continuity.LifecycleEvent(
            candidate_id=rejected.candidate_id,
            event_type="blocker_resolved",
            observed_at=T3,
            target_state="pre_breakout",
            blocker_resolved=True,
        ),
    )
    assert result.event_consumed is False
    assert result.candidate == rejected
    assert result.reason == "permanent_rejection_terminal"

    with pytest.raises(ValueError, match="permanent_rejection_terminal"):
        continuity.transition(rejected, "pre_breakout", observed_at=T4)


def test_invalid_transition_and_identity_mismatch_fail_closed() -> None:
    candidate = _candidate()
    with pytest.raises(ValueError, match="invalid_lifecycle_transition"):
        continuity.transition(candidate, "actionable", observed_at=T1)

    with pytest.raises(ValueError, match="event_candidate_identity_mismatch"):
        continuity.process_event(
            candidate,
            continuity.LifecycleEvent(
                candidate_id="other-candidate",
                event_type="curve_progress_changed",
                observed_at=T1,
            ),
        )


def test_sqlite_store_persists_canonical_identity_rejection_and_event_history() -> None:
    db = sqlite3.connect(":memory:")
    store = continuity.CandidateContinuityStore(db)
    candidate = store.create(_advance(_candidate(), "developing"))
    rejected = continuity.reject_temporarily(
        candidate,
        blocker_reason="waiting_for_graduation",
        latest_exact_executable_quote=_quote(),
        distance_to_actionable=0.12,
        active_event_subscriptions=("graduated",),
        rejected_at=T2,
    )
    store.put(rejected)

    result = store.process(
        continuity.LifecycleEvent(
            candidate_id=candidate.candidate_id,
            event_type="graduated",
            observed_at=T3,
            surface="PUMPSWAP",
            route_id="pump-amm-pool",
            source_signature="sig-graduation",
            target_state="pre_breakout",
            blocker_resolved=True,
        )
    )
    loaded = store.get(candidate.candidate_id)
    by_asset = store.get_by_asset(candidate.asset_id)

    assert loaded == result.candidate
    assert by_asset == result.candidate
    assert loaded is not None
    assert loaded.state == "pre_breakout"
    assert loaded.current_surface == "PUMPSWAP"
    assert loaded.rejection is None
    assert loaded.candidate_id == "candidate:mint-v52"
    assert loaded.asset_id == "mint-v52"
    assert store.event_count(candidate.candidate_id) == 1

    with pytest.raises(ValueError, match="asset_id_already_has_canonical_candidate"):
        store.create(
            continuity.new_candidate(
                candidate_id="duplicate-candidate-id-for-same-mint",
                asset_id="mint-v52",
                surface="PUMP_FUN",
                observed_at=T4,
                source_signature="sig-duplicate",
            )
        )
