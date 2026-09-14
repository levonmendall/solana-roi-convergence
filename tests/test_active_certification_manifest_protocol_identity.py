from __future__ import annotations

import subprocess
import sys


def test_active_manifest_preserves_replication_protocol_and_filters_scope() -> None:
    script = r'''
import sqlite3

from solana_roi import certification_incremental_replication as replication
from solana_roi.certification_active_manifest import (
    ACTIVE_MANIFEST_VERSION,
    install_active_certification_manifest,
)

base_version = replication.REPLICATION_VERSION
assert ACTIVE_MANIFEST_VERSION == "positive-active-manifest-v1"

connection = sqlite3.connect(":memory:")
connection.execute("CREATE TABLE system_current(state_key TEXT PRIMARY KEY, payload_json TEXT, payload_hash TEXT, updated_at TEXT)")
connection.execute("CREATE TABLE definitely_legacy(id INTEGER PRIMARY KEY, payload TEXT)")
connection.commit()

install_active_certification_manifest()
assert replication.REPLICATION_VERSION == base_version

names = {str(row["name"]) for row in replication._ordinary_tables(connection)}
assert "system_current" in names
assert "definitely_legacy" not in names

schema = replication._schema_objects(connection)
assert any(kind == "table" and name == "system_current" for kind, name, _table, _sql in schema)
assert not any(name == "definitely_legacy" for _kind, name, _table, _sql in schema)

# Installation is idempotent and must never mutate the transport version.
install_active_certification_manifest()
assert replication.REPLICATION_VERSION == base_version
connection.close()
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
