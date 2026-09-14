from __future__ import annotations

from typing import Any

# Register persistence added by the current v5.2 market-validation and
# wallet-forward runtimes before the positive certification allowlist is read.
from . import storage_current_v52_reconciliation as _current_v52_reconciliation  # noqa: F401
from .storage_manifest import certification_table_allowlist

ACTIVE_MANIFEST_VERSION = "positive-active-manifest-v1"
_INSTALLED = False


def install_active_certification_manifest() -> None:
    """Replace discovery-based certification scope with the positive manifest.

    This is installed only by active-storage runtime composition. Legacy mode
    is left behaviorally unchanged until cutover. Logical bootstrap calls the
    same replication helpers, so one scope controls both trigger installation,
    bounded deltas, and bootstrap tables/schema objects.

    The replication version describes the wire/protocol contract, not the set
    of admitted persistent datasets. Active-storage scope is already fail-closed
    through the positive allowlist plus the replication epoch/schema fingerprint.
    Changing the protocol version merely because the allowed table set changes
    incorrectly makes an otherwise compatible certifier reject the active store.
    """
    global _INSTALLED
    if _INSTALLED:
        return
    from . import certification_incremental_replication as replication

    allowed = frozenset(certification_table_allowlist())
    original_tables = replication._ordinary_tables
    original_schema_objects = replication._schema_objects

    def _manifest_tables(connection: Any) -> list[dict[str, Any]]:
        return [row for row in original_tables(connection) if str(row.get("name")) in allowed]

    def _manifest_schema_objects(connection: Any) -> list[tuple[str, str, str, str]]:
        result: list[tuple[str, str, str, str]] = []
        for kind, name, table_name, sql in original_schema_objects(connection):
            if kind == "table":
                if name in allowed:
                    result.append((kind, name, table_name, sql))
                continue
            # Indexes and triggers belong only to approved tables. Views are
            # omitted unless their reported table identity is approved.
            if table_name in allowed:
                result.append((kind, name, table_name, sql))
        return result

    replication._ordinary_tables = _manifest_tables
    replication._schema_objects = _manifest_schema_objects
    # Intentionally preserve replication.REPLICATION_VERSION. The positive
    # manifest changes certification data scope, not the bounded replication
    # wire protocol. Scope identity remains exact through the allowlist-derived
    # schema fingerprint and replication epoch.
    _INSTALLED = True
