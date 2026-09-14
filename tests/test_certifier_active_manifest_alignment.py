from __future__ import annotations

import json
import os
import subprocess
import sys


def _probe(active: bool) -> dict[str, object]:
    env = dict(os.environ)
    if active:
        env["SOLANA_ROI_ACTIVE_STORAGE_ENABLED"] = "1"
    else:
        env.pop("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", None)
    script = r'''
import json
from solana_roi import certifier_service
from solana_roi import certifier_cleanup_service as cleanup
from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap_client as logical
from solana_roi import certification_replica_client as replica
print(json.dumps({
    "installed": cleanup._ACTIVE_MANIFEST_INSTALLED,
    "service_version": cleanup.SERVICE_VERSION,
    "replication": replication.REPLICATION_VERSION,
    "replica": replica.REPLICATION_VERSION,
    "logical": logical.REPLICATION_VERSION,
    "state_installed": cleanup._STATE["active_storage_replication_manifest"],
    "state_version": cleanup._STATE["replication_version"],
    "paper_only": certifier_service.PAPER_ONLY,
    "live_money_authority": certifier_service.LIVE_MONEY_AUTHORITY,
}, sort_keys=True))
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_certifier_active_storage_installs_same_positive_replication_manifest() -> None:
    payload = _probe(True)
    expected = "+positive-active-manifest-v1"
    assert payload["installed"] is True
    assert payload["state_installed"] is True
    assert str(payload["replication"]).endswith(expected)
    assert payload["replication"] == payload["replica"] == payload["logical"] == payload["state_version"]
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False


def test_certifier_legacy_mode_keeps_legacy_replication_identity() -> None:
    payload = _probe(False)
    assert payload["installed"] is False
    assert payload["state_installed"] is False
    assert "+positive-active-manifest-v1" not in str(payload["replication"])
    assert payload["replication"] == payload["replica"] == payload["logical"] == payload["state_version"]
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
