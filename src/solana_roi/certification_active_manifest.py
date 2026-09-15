from __future__ import annotations

from typing import Any

# Register persistence added by the current v5.2 market-validation and
# wallet-forward runtimes before the positive certification allowlist is read.
from . import storage_current_v52_reconciliation as _current_v52_reconciliation  # noqa: F401
from .certification_replication_protocol import (
    ACTIVE_MANIFEST_PROTOCOL_SUFFIX,
    ACTIVE_MANIFEST_VERSION,
)
from .storage_manifest import certification_table_allowlist

_INSTALLED = False


def install_active_certification_manifest() -> None:
    """Replace discovery-based certification scope with the positive manifest.

    This is installed only by active-storage runtime composition. Legacy mode
    is left behaviorally unchanged until cutover. Logical bootstrap calls the
    same replication helpers, so one scope controls both trigger installation,
    bounded deltas, and bootstrap tables/schema objects.

    The active manifest is deliberately carried in the replication protocol
    identity. That prevents a certifier from silently mixing legacy all-history
    scope with the bounded positive allowlist under one bootstrap/checkpoint/
    delta stream.
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
    if not str(replication.REPLICATION_VERSION).endswith(ACTIVE_MANIFEST_PROTOCOL_SUFFIX):
        replication.REPLICATION_VERSION = str(replication.REPLICATION_VERSION) + ACTIVE_MANIFEST_PROTOCOL_SUFFIX
    _INSTALLED = True
