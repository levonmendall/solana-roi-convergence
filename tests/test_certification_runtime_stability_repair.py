from __future__ import annotations

import inspect

from solana_roi import production_capacity_repair as capacity
from solana_roi import v51_forward_certification as forward
from solana_roi import v51_evidence_analytics as analytics


def test_forward_certification_get_is_snapshot_read_only() -> None:
    source = inspect.getsource(forward.install_forward_certification)
    assert "_cached_production_proof" in source
    assert "http_request_executes_evidence_materialization" in source
    endpoint_body = source.split("def forward_certification_status", 1)[1]
    assert "build_forward_certification(" not in endpoint_body.split("app.add_api_route", 1)[0]


def test_high_volume_upserts_skip_unchanged_rows() -> None:
    cost = inspect.getsource(analytics.refresh_execution_cost_ledger)
    rejected = inspect.getsource(analytics.refresh_rejected_counterfactuals)
    assert "WHERE v51_execution_cost_ledger.family IS NOT excluded.family" in cost
    assert "WHERE v51_rejected_counterfactuals.release_commit IS NOT excluded.release_commit" in rejected
    assert "live_money_authority IS NOT 0" in cost
    assert "live_money_authority IS NOT 0" in rejected


def test_capacity_endpoint_root_owns_terminal_failure() -> None:
    source = inspect.getsource(capacity._capacity_call_endpoint)
    assert "_own_capacity_endpoint_task()" in source
    assert capacity._ROI_CAPACITY_TASK_OWNERSHIP_VERSION == "capacity-endpoint-root-terminal-ownership-v1"


def test_capacity_repair_does_not_change_failure_semantics() -> None:
    status_source = inspect.getsource(capacity._capacity_status)
    assert '"failure_semantics_changed": False' in status_source
    assert '"read_only": True' in status_source
