from __future__ import annotations

import subprocess
import sys
import textwrap


# The production hardening module intentionally imports the final Robinhood/v5.1
# composition graph. Importing it at pytest collection time would mutate global
# production singletons before later regression modules have even been collected.
# Exercise its low-level contracts in a fresh interpreter so the repository-wide
# suite retains the same collection/import order as canonical production.
_SCRIPT = r'''
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import e2e_production_hardening_repair as repair


def scout_row(age_seconds):
    return {
        "signature": "sig-1",
        "priority": 0,
        "reason": "frozen_scout_processed_trigger",
        "trigger_received_at": (
            datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        ).isoformat(),
    }


# A queue claim count cannot terminate a still-open frozen 20-second candidate.
calls = []
def finish(_self, signature, *, error=None, retry=False):
    calls.append({"signature": signature, "error": error, "retry": retry})
repair._ORIGINAL_JOURNAL_FINISH = finish
token = repair._CURRENT_SCOUT_ROW.set(scout_row(2.0))
try:
    repair._journal_finish_with_absolute_candidate_deadline(
        SimpleNamespace(),
        "sig-1",
        error="confirmed transaction not yet available",
        retry=False,
    )
finally:
    repair._CURRENT_SCOUT_ROW.reset(token)
assert calls == [{
    "signature": "sig-1",
    "error": "confirmed transaction not yet available",
    "retry": True,
}]
assert repair.ENTRY_WINDOW_SECONDS == 20.0

# The same failure is terminal after the original absolute entry window.
calls.clear()
token = repair._CURRENT_SCOUT_ROW.set(scout_row(25.0))
try:
    repair._journal_finish_with_absolute_candidate_deadline(
        SimpleNamespace(),
        "sig-1",
        error="confirmed transaction not yet available",
        retry=False,
    )
finally:
    repair._CURRENT_SCOUT_ROW.reset(token)
assert calls[0]["retry"] is False

# Duplicate funding transaction reads collapse to one underlying request.
transaction_calls = 0
async def transaction_underlying(_self, signature):
    global transaction_calls
    transaction_calls += 1
    await asyncio.sleep(0.01)
    return {"signature": signature}
repair._ORIGINAL_FUNDING_TX_CACHED = transaction_underlying
collector = SimpleNamespace()
async def transaction_scenario():
    return await asyncio.gather(
        repair._funding_transaction_singleflight(collector, "same-signature"),
        repair._funding_transaction_singleflight(collector, "same-signature"),
    )
values = asyncio.run(transaction_scenario())
assert transaction_calls == 1
assert values == [{"signature": "same-signature"}, {"signature": "same-signature"}]
assert getattr(collector, "_roi_e2e_hardening_funding_tx_singleflight_joins") == 1
assert getattr(collector, "_roi_e2e_funding_tx_inflight") == {}

# Duplicate funding signature-history pages also collapse to one request.
signature_calls = 0
async def signature_underlying(_self, wallet, *, before):
    global signature_calls
    signature_calls += 1
    await asyncio.sleep(0.01)
    return [{"wallet": wallet, "before": before}]
repair._ORIGINAL_SIGNATURE_PAGE = signature_underlying
collector = SimpleNamespace()
async def signature_scenario():
    return await asyncio.gather(
        repair._funding_signature_page_singleflight(collector, "wallet", before="cursor"),
        repair._funding_signature_page_singleflight(collector, "wallet", before="cursor"),
    )
values = asyncio.run(signature_scenario())
assert signature_calls == 1
assert values[0] == values[1]
assert getattr(collector, "_roi_e2e_hardening_funding_signature_singleflight_joins") == 1
assert getattr(collector, "_roi_e2e_funding_signature_inflight") == {}

# Near-creation timing failures retain their exact reason instead of one aggregate.
store = SimpleNamespace()
def frontier_underlying(_store, *, signature, created_at, max_age_seconds):
    assert signature == "launch-sig"
    assert max_age_seconds == 3.0
    return None, "missing_recent_preexisting_websocket_frontier"
repair._ORIGINAL_FRONTIER_LAG = frontier_underlying
value = repair._frontier_lag_with_reason_accounting(
    store,
    signature="launch-sig",
    created_at=datetime.now(timezone.utc),
    max_age_seconds=3.0,
)
assert value == (None, "missing_recent_preexisting_websocket_frontier")
assert store._roi_e2e_frontier_reason_counts == {
    "missing_recent_preexisting_websocket_frontier": 1
}

# Robinhood supervision cannot change the paper-only authority boundary.
repair._ORIGINAL_ROBINHOOD_METADATA = lambda **_kwargs: {
    "paper_only": True,
    "live_money_authority": False,
    "signing_available": False,
    "transaction_submission_available": False,
}
repair.robinhood_runtime._STATE["supervisor_restart_count"] = 3
repair.robinhood_runtime._STATE["supervisor_generation"] = 4
payload = repair._robinhood_supervision_metadata(store_path="/tmp/rh.sqlite3")
assert payload["supervised_restart_enabled"] is True
assert payload["restart_count"] == 3
assert payload["worker_generation"] == 4
assert payload["paper_only"] is True
assert payload["live_money_authority"] is False
assert payload["signing_available"] is False
assert payload["transaction_submission_available"] is False

manifest = repair.status()
assert manifest["candidate_absolute_entry_deadline_seconds"] == 20.0
assert manifest["strategy_thresholds_changed"] is False
assert manifest["certification_thresholds_changed"] is False
assert manifest["paper_only"] is True
assert manifest["live_money_authority"] is False
assert manifest["signing_available"] is False
assert manifest["transaction_submission_available"] is False
'''


def test_e2e_production_hardening_contracts_in_fresh_interpreter() -> None:
    completed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, (
        "isolated E2E hardening contract failed\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
