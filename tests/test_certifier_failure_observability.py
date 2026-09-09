from __future__ import annotations

import threading

from solana_roi import certifier_service


def test_safe_error_message_redacts_shared_credential_and_named_secrets(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "private-shared-value")
    exc = RuntimeError(
        "child failed token=other-secret Authorization:BearerThing private-shared-value"
    )

    message = certifier_service._safe_error_message(exc)

    assert "private-shared-value" not in message
    assert "other-secret" not in message
    assert "BearerThing" not in message
    assert "[redacted]" in message
    assert len(message) <= 1600


def test_failed_cycle_persists_safe_failure_state_without_authority_change(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "private-shared-value")
    monkeypatch.setattr(
        certifier_service,
        "_download_snapshot",
        lambda destination: (_ for _ in ()).throw(RuntimeError("snapshot failed private-shared-value")),
    )

    with certifier_service._LOCK:
        certifier_service._STATE.update(
            {
                "cycles": 0,
                "successes": 0,
                "failures": 0,
                "last_error_type": "PriorFailure",
                "last_published_surfaces": ["e2e"],
                "active_child_pid": None,
            }
        )

    certifier_service._run_cycle_sync(threading.Event())
    payload = certifier_service.health()

    assert payload["state"]["last_error_type"] == "RuntimeError"
    assert payload["state"]["last_published_surfaces"] == []
    assert payload["state"]["failures"] == 1
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
