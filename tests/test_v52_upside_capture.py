from __future__ import annotations

from solana_roi.v52_lossless_candidate_accounting import CANONICAL_LANES
from solana_roi.v52_upside_capture import (
    CapturePosition,
    ForwardEvidence,
    QuoteSnapshot,
    SecondLegEvidence,
    UpsideCapturePolicy,
    plan_position_management,
    plan_scale_in,
    plan_second_leg_reentry,
    plan_starter,
    safety_manifest,
)


def _policy() -> UpsideCapturePolicy:
    return UpsideCapturePolicy(
        starter_fraction_of_target=0.25,
        max_scale_fraction_of_target=0.25,
        first_derisk_fraction_of_position=0.25,
        second_derisk_fraction_of_position=0.50,
        runner_fraction_of_target=0.10,
        minimum_exit_depth_coverage_ratio=2.0,
    )


def _quote(
    *,
    chase: float = 0.20,
    buy_notional: float = 2_000.0,
    sell_notional: float = 2_000.0,
    sell_depth: float = 2_000.0,
    exact_buy: bool = True,
    exact_sell: bool = True,
    exitable: bool = True,
    latency: float = 2.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        latency_seconds=latency,
        chase_fraction=chase,
        exact_buy_quote_available=exact_buy,
        exact_sell_quote_available=exact_sell,
        structurally_exitable=exitable,
        buy_quote_notional=buy_notional,
        sell_quote_notional=sell_notional,
        executable_sell_depth_notional=sell_depth,
    )


def _position(
    *,
    lane: str = "pump_fun",
    position_notional: float = 150.0,
    last_add_price: float = 1.0,
    lifecycle_state: str = "entered",
    derisk_stage: int = 0,
    impulse_id: str = "impulse-1",
) -> CapturePosition:
    return CapturePosition(
        candidate_id=f"batch4-{lane}",
        lane=lane,
        target_notional=1_000.0,
        position_notional=position_notional,
        last_add_price=last_add_price,
        lifecycle_state=lifecycle_state,
        derisk_stage=derisk_stage,
        impulse_id=impulse_id,
    )


def test_starter_is_fractional_liquidity_adjusted_and_available_to_every_lane() -> None:
    policy = _policy()
    quote = _quote(buy_notional=500.0, sell_notional=500.0, sell_depth=300.0)

    for lane in CANONICAL_LANES:
        action = plan_starter(
            candidate_id=f"batch4-{lane}",
            lane=lane,
            target_notional=1_000.0,
            mark_price=1.0,
            impulse_id=f"{lane}-impulse-1",
            quote=quote,
            policy=policy,
        )
        assert action.action == "starter"
        assert action.resulting_position_notional == 150.0
        assert action.resulting_position_notional < 1_000.0
        assert action.liquidity_cap_notional == 150.0
        assert action.entry_authority is False
        assert action.paper_only is True
        assert action.research_only is True


def test_scale_requires_new_forward_strength_and_never_averages_down() -> None:
    policy = _policy()
    quote = _quote(sell_depth=800.0)

    no_new_evidence = plan_scale_in(
        _position(),
        quote=quote,
        evidence=ForwardEvidence(
            mark_price=1.10,
            new_forward_evidence=False,
            liquidity_improving=True,
        ),
        policy=policy,
    )
    assert no_new_evidence.action == "scale_blocked"
    assert "no_new_forward_evidence" in no_new_evidence.reasons

    average_down = plan_scale_in(
        _position(last_add_price=1.0),
        quote=quote,
        evidence=ForwardEvidence(
            mark_price=0.90,
            new_forward_evidence=True,
            independent_buying_accelerating=True,
        ),
        policy=policy,
    )
    assert average_down.action == "scale_blocked"
    assert "averaging_down_prohibited" in average_down.reasons


def test_scale_requotes_both_sides_and_is_capped_by_exit_depth() -> None:
    policy = _policy()
    position = _position(position_notional=150.0)

    missing_exit_requote = plan_scale_in(
        position,
        quote=_quote(exact_sell=False, sell_depth=800.0),
        evidence=ForwardEvidence(
            mark_price=1.10,
            new_forward_evidence=True,
            independent_buying_accelerating=True,
        ),
        policy=policy,
    )
    assert missing_exit_requote.action == "scale_blocked"
    assert "exact_sell_quote_missing" in missing_exit_requote.reasons

    action = plan_scale_in(
        position,
        quote=_quote(
            buy_notional=500.0,
            sell_notional=500.0,
            sell_depth=800.0,
        ),
        evidence=ForwardEvidence(
            mark_price=1.10,
            new_forward_evidence=True,
            independent_buying_accelerating=True,
            liquidity_improving=True,
            sell_depth_improving=True,
        ),
        policy=policy,
    )
    assert action.action == "scale_in"
    assert action.notional_change == 250.0
    assert action.resulting_position_notional == 400.0
    assert action.liquidity_cap_notional == 400.0
    assert "two_sided_requote_confirmed" in action.reasons


def test_staged_derisk_can_reduce_to_runner_then_exit_runner_on_deterioration() -> None:
    policy = _policy()
    quote = _quote(sell_depth=5_000.0, sell_notional=5_000.0)
    deterioration = ForwardEvidence(
        mark_price=2.0,
        attention_decay=True,
        weakening_price_structure=True,
    )

    first = plan_position_management(
        _position(position_notional=800.0),
        quote=quote,
        evidence=deterioration,
        policy=policy,
    )
    assert first.action == "stage_derisk_1"
    assert first.notional_change == -200.0
    assert first.resulting_position_notional == 600.0
    assert first.resulting_derisk_stage == 1

    second = plan_position_management(
        _position(
            position_notional=600.0,
            lifecycle_state="de_risking",
            derisk_stage=1,
        ),
        quote=quote,
        evidence=deterioration,
        policy=policy,
    )
    assert second.action == "stage_derisk_2"
    assert second.notional_change == -300.0
    assert second.resulting_position_notional == 300.0
    assert second.resulting_derisk_stage == 2

    runner = plan_position_management(
        _position(
            position_notional=300.0,
            lifecycle_state="de_risking",
            derisk_stage=2,
        ),
        quote=quote,
        evidence=ForwardEvidence(
            mark_price=2.1,
            continuation_healthy=True,
            independent_participation_persists=True,
            seller_pressure_controlled=True,
            hazards_acceptable=True,
        ),
        policy=policy,
    )
    assert runner.action == "enter_runner"
    assert runner.resulting_lifecycle_state == "runner"
    assert runner.resulting_position_notional == 100.0
    assert runner.notional_change == -200.0

    exit_runner = plan_position_management(
        _position(
            position_notional=100.0,
            lifecycle_state="runner",
            derisk_stage=2,
        ),
        quote=quote,
        evidence=ForwardEvidence(
            mark_price=1.8,
            continuation_healthy=False,
            seller_pressure_controlled=False,
        ),
        policy=policy,
    )
    assert exit_runner.action == "exit_runner"
    assert exit_runner.resulting_position_notional == 0.0
    assert exit_runner.resulting_lifecycle_state == "reentry_watch"


def test_sell_actions_fail_closed_without_exact_executable_exit_quote() -> None:
    action = plan_position_management(
        _position(position_notional=400.0),
        quote=_quote(exact_sell=False),
        evidence=ForwardEvidence(
            mark_price=0.8,
            structural_hard_stop=True,
        ),
        policy=_policy(),
    )
    assert action.action == "exit_blocked"
    assert action.resulting_position_notional == 400.0
    assert "exact_sell_quote_missing" in action.reasons


def test_second_leg_reentry_requires_fresh_sequence_and_gt_40pct_remains_observe_only() -> None:
    policy = _policy()
    exited = _position(
        position_notional=0.0,
        lifecycle_state="reentry_watch",
        derisk_stage=2,
        impulse_id="impulse-1",
    )
    second_leg = SecondLegEvidence(
        new_impulse_id="impulse-2",
        consolidation_confirmed=True,
        new_independent_buyers=True,
        liquidity_expanding=True,
        renewed_acceleration=True,
    )
    evidence = ForwardEvidence(mark_price=1.5)

    high_chase = plan_second_leg_reentry(
        exited,
        quote=_quote(chase=0.41),
        evidence=evidence,
        second_leg=second_leg,
        policy=policy,
    )
    assert high_chase.action == "observe_only"
    assert high_chase.resulting_position_notional == 0.0
    assert high_chase.reasons == ("gt_40pct_chase_observe_only",)

    reentry = plan_second_leg_reentry(
        exited,
        quote=_quote(chase=0.20),
        evidence=evidence,
        second_leg=second_leg,
        policy=policy,
    )
    assert reentry.action == "reentry_starter"
    assert reentry.resulting_position_notional == 250.0
    assert reentry.resulting_lifecycle_state == "entered"

    reused_impulse = plan_second_leg_reentry(
        exited,
        quote=_quote(chase=0.20),
        evidence=evidence,
        second_leg=SecondLegEvidence(
            new_impulse_id="impulse-1",
            consolidation_confirmed=True,
            new_independent_buyers=True,
            liquidity_expanding=True,
            renewed_acceleration=True,
        ),
        policy=policy,
    )
    assert reused_impulse.action == "reentry_blocked"
    assert "second_leg_must_use_new_impulse" in reused_impulse.reasons


def test_batch4_safety_manifest_preserves_locked_authority_boundary() -> None:
    manifest = safety_manifest()

    assert manifest["batch_version"] == "v52-batch4-upside-capture-1"
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["research_only"] is True
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False
    assert manifest["production_composition_hook"] is False
    assert manifest["production_economic_parameters_frozen_by_batch4"] is False
    assert manifest["starter_positions_supported"] is True
    assert manifest["scale_requires_new_forward_evidence"] is True
    assert manifest["averaging_down_prohibited"] is True
    assert manifest["scale_requires_fresh_two_sided_amount_specific_quotes"] is True
    assert manifest["liquidity_adjusted_position_cap"] is True
    assert manifest["staged_derisk_supported"] is True
    assert manifest["persistent_runner_supported"] is True
    assert manifest["second_leg_reentry_supported"] is True
    assert manifest["gt_40pct_chase_observe_only"] is True
