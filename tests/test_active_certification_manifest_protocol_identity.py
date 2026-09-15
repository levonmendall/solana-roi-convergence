from __future__ import annotations

import subprocess
import sys


def test_active_manifest_marks_replication_protocol_and_filters_scope() -> None:
    script = r'''
import sqlite3
from solana_roi import certification_incremental_replication as replication
from solana_roi.certification_active_manifest import (
    ACTIVE_MANIFEST_VERSION,
    install_active_certification_manifest,
)

base_version = replication.REPLICATION_VERSION
expected_version = base_version + "+positive-active-manifest-v1"
assert ACTIVE_MANIFEST_VERSION == "positive-active-manifest-v1"

connection = sqlite3.connect(":memory:")
connection.execute("CREATE TABLE allowed_table(id INTEGER PRIMARY KEY, value TEXT)")
connection.execute("CREATE TABLE denied_table(id INTEGER PRIMARY KEY, value TEXT)")
connection.execute("CREATE INDEX allowed_idx ON allowed_table(value)")
connection.execute("CREATE INDEX denied_idx ON denied_table(value)")
connection.commit()

import solana_roi.certification_active_manifest as active
active.certification_table_allowlist = lambda: ("allowed_table",)
install_active_certification_manifest()

assert replication.REPLICATION_VERSION == expected_version
assert [row["name"] for row in replication._ordinary_tables(connection)] == ["allowed_table"]
schema = replication._schema_objects(connection)
assert all(row[2] == "allowed_table" or row[1] == "allowed_table" for row in schema)
assert not any(row[1] == "denied_table" or row[2] == "denied_table" for row in schema)

# Installation is idempotent and must never duplicate the protocol suffix.
install_active_certification_manifest()
assert replication.REPLICATION_VERSION == expected_version
connection.close()
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
