from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.wallet_evidence_rpc_repair import (
    _copyability_reasons,
    _point_in_time_risk_flags,
)


def test_copyability_diagnostics_preserve_thresholds_and_name_exact_blockers():
    reasons = _copyability_reasons(
        {
            "copyable": 0,
            "copyable_price_sol": 1.20,
            "chase_fraction": 0.18,
            "observation_lag_ms": 3_000.0,
            "processing_delay_ms": 5_000.0,
        },
        max_chase_fraction=0.15,
        max_observation_lag_seconds=20.0,
        max_mark_delay_seconds=20.0,
    )
    assert reasons == ("chase_above_ceiling",)

    missing = _copyability_reasons(
        {
            "copyable": 0,
            "copyable_price_sol": None,
            "chase_fraction": None,
            "observation_lag_ms": 25_000.0,
            "processing_delay_ms": 21_000.0,
        },
        max_chase_fraction=0.15,
        max_observation_lag_seconds=20.0,
        max_mark_delay_seconds=20.0,
    )
    assert set(missing) == {
        "mark_unavailable",
        "chase_unavailable",
        "observation_lag_above_limit",
        "mark_delay_above_limit",
    }


class _EntityResolver:
    def entity_id_for(self, wallet, *, fallback_entity_id, as_of):
        return f"graph:{wallet}"

    def component(self, wallet, *, as_of):
        return {wallet}


class _MissingRisk:
    async def snapshot(self, *args, **kwargs):
        return None


class _Collectors:
    def __init__(self):
        self.at = []

    async def refresh_candidate(self, mint, at, *, current_swap=None):
        self.at.append(("candidate", at))

    async def refresh_coverage(self, mint, at, *, current_swap=None):
        self.at.append(("coverage", at))


def test_missing_point_in_time_risk_prewarms_future_only_at_actual_collection_time():
    boundary = datetime.now(timezone.utc) - timedelta(seconds=2)
    received = datetime.now(timezone.utc) - timedelta(milliseconds=50)
    collectors = _Collectors()
    discovery = SimpleNamespace(
        entity_resolver=_EntityResolver(),
        risk=_MissingRisk(),
        risk_collectors=collectors,
        store=SimpleNamespace(append=lambda *args, **kwargs: None),
        _roi_wallet_evidence_repair_started_at=boundary,
        _roi_risk_prewarm_locks={},
        _roi_risk_prewarm_next_at={},
        _roi_risk_prewarm_attempts=0,
        _roi_risk_prewarm_errors=0,
        _roi_risk_point_in_time_hits=0,
        _roi_risk_point_in_time_misses=0,
    )
    swap = SimpleNamespace(
        side="buy",
        wallet="wallet-a",
        token_mint="mint-a",
        received_at=received,
    )

    complete, manipulation, side_wallet = asyncio.run(
        _point_in_time_risk_flags(discovery, swap)
    )

    assert complete is False
    assert manipulation is True
    assert side_wallet is True
    assert discovery._roi_risk_prewarm_attempts == 1
    assert [name for name, _at in collectors.at] == ["candidate", "coverage"]
    assert all(at >= received for _name, at in collectors.at)


def test_pre_repair_observation_cannot_be_retroactively_completed_or_refreshed():
    boundary = datetime.now(timezone.utc)
    collectors = _Collectors()
    discovery = SimpleNamespace(
        entity_resolver=_EntityResolver(),
        risk=_MissingRisk(),
        risk_collectors=collectors,
        store=SimpleNamespace(append=lambda *args, **kwargs: None),
        _roi_wallet_evidence_repair_started_at=boundary,
        _roi_risk_prewarm_locks={},
        _roi_risk_prewarm_next_at={},
        _roi_risk_prewarm_attempts=0,
        _roi_risk_prewarm_errors=0,
        _roi_risk_point_in_time_hits=0,
        _roi_risk_point_in_time_misses=0,
    )
    swap = SimpleNamespace(
        side="buy",
        wallet="wallet-a",
        token_mint="mint-a",
        received_at=boundary - timedelta(seconds=1),
    )

    result = asyncio.run(_point_in_time_risk_flags(discovery, swap))
    assert result == (False, True, True)
    assert collectors.at == []
    assert discovery._roi_risk_prewarm_attempts == 0
