from __future__ import annotations

from pathlib import Path

from solana_roi import robinhood_chain_runtime as runtime
from solana_roi import robinhood_production_provider_finalizer as finalizer
from solana_roi import robinhood_production_ws_transport as transport
from solana_roi import robinhood_usage_bounded_transport as bounded


def test_factory_discovery_contracts_remain_complete_under_provider_budgeting() -> None:
    # Market subscription membership is now research-promoted and is covered by the
    # provider-budget regressions. The permanent discovery set itself must remain
    # complete regardless of that live shortlist.
    assert set(bounded.DISCOVERY_ADDRESSES) == {
        runtime.UNISWAP_V3_FACTORY,
        runtime.PONS_V1_ACTIVE_FACTORY,
        runtime.PONS_V1_LEGACY_FACTORY,
        runtime.PONS_V2_FACTORY,
    }
    assert set(bounded.DISCOVERY_TOPICS) == {
        runtime.V3_POOL_CREATED_TOPIC.lower(),
        runtime.PONS_V1_TOKEN_LAUNCHED_TOPIC.lower(),
        runtime.PONS_V2_TOKEN_LAUNCHED_TOPIC.lower(),
    }


def test_bounded_transport_replaces_global_reader_before_final_install() -> None:
    # Installation is idempotent and deliberately occurs before the production
    # transport class wrapper is installed by the finalizer.
    if not bounded.status()["installed"] and not bool(getattr(transport, "_INSTALLED", False)):
        bounded.install_robinhood_usage_bounded_transport()

    assert transport.TRANSPORT_VERSION == bounded.USAGE_BOUNDED_VERSION
    assert transport._reader_async is bounded._reader_async
    assert transport._reader_ready is bounded._reader_ready
    assert transport._production_ws_run is bounded._production_ws_run


def test_usage_bounded_source_has_no_global_newheads_subscription() -> None:
    source = Path(bounded.__file__).read_text(encoding="utf-8")
    assert '"newHeads"' not in source
    assert '"address": addresses' in source
    assert '"topics": [topics]' in source
    assert "live_authority=False" in source
    assert "new_target_gap_backfill_research_only" in source


def test_provider_budgeting_does_not_narrow_factory_candidate_discovery() -> None:
    status = bounded.status()
    assert status["global_newheads_subscription"] is False
    assert status["chain_wide_log_subscription"] is False
    assert status["candidate_discovery_constrained_by_active_subscription_cap"] is False
    assert len(status["factory_discovery_addresses"]) == 4
    assert status["target_gap_backfill_authority"] == "research_only_no_paper_entry"


def test_v51_and_paper_only_authority_remain_unchanged() -> None:
    assert bounded.status()["canonical_latency_hard_max_seconds"] == 20.0
    assert bounded.status()["legacy_two_block_gate_has_production_authority"] is False
    assert bounded.status()["paper_only"] is True
    assert bounded.status()["live_money_authority"] is False
    assert bounded.status()["signing_available"] is False
    assert bounded.status()["transaction_submission_available"] is False

    source = Path(bounded.__file__).read_text(encoding="utf-8")
    assert "eth_sendRawTransaction" not in source
    assert "eth_sendTransaction" not in source


def test_finalizer_installs_bounded_transport_before_provider_wrapper() -> None:
    source = Path(finalizer.__file__).read_text(encoding="utf-8")
    bounded_install = source.index("install_robinhood_usage_bounded_transport()")
    provider_install = source.index("production_transport.install_robinhood_production_ws_transport(plane_cls)")
    assert bounded_install < provider_install
