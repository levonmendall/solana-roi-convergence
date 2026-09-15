from __future__ import annotations

from types import SimpleNamespace

from solana_roi import cgroup_oom_forensics as forensics


def _thread(name: str, *, daemon: bool = True) -> SimpleNamespace:
    return SimpleNamespace(name=name, daemon=daemon)


def test_thread_census_groups_generated_thread_ordinals(monkeypatch) -> None:
    threads = [
        _thread(f"Thread-{index} (_risk_worker_no_lookahead)")
        for index in range(1500)
    ] + [
        _thread(f"asyncio_{index}") for index in range(8)
    ] + [
        _thread("MainThread", daemon=False),
        _thread("robinhood-chain-paper-isolated"),
    ]
    monkeypatch.setattr(forensics.threading, "enumerate", lambda: list(threads))

    payload = forensics._thread_census()

    assert payload["python_active_count"] == 1510
    assert payload["daemon_count"] == 1509
    assert payload["non_daemon_count"] == 1
    normalized = {row["name"]: row["count"] for row in payload["top_normalized_names"]}
    assert normalized["Thread-* (_risk_worker_no_lookahead)"] == 1500
    assert normalized["asyncio_*"] == 8
    assert payload["read_only"] is True


def test_thread_census_payload_is_bounded(monkeypatch) -> None:
    threads = [_thread(f"owner-{index}-thread") for index in range(200)]
    monkeypatch.setattr(forensics.threading, "enumerate", lambda: list(threads))

    payload = forensics._thread_census()

    assert len(payload["top_exact_names"]) == forensics.THREAD_CENSUS_MAX_NAMES
    assert len(payload["top_normalized_names"]) <= forensics.THREAD_CENSUS_MAX_NAMES
    assert payload["exact_names_truncated"] is True


def test_status_preserves_zero_authority_contract() -> None:
    payload = forensics.status()
    assert payload["thread_census_bounded"] is True
    assert payload["certification_thresholds_changed"] is False
    assert payload["economic_thresholds_changed"] is False
    assert payload["canonical_evidence_reset"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
