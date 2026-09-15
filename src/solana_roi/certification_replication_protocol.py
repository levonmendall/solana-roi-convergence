from __future__ import annotations

"""Stable certification replication protocol identities.

The positive active-storage manifest intentionally marks its bounded replication
surface with a protocol suffix. Certifier clients must accept only the legacy
base protocol or that exact active-manifest protocol, and they must pin whichever
identity was selected for the lifetime of a bootstrap/checkpoint/delta stream.

The incremental-replication module's exported version can be mutated during
active-manifest installation. Normalize it here so imports remain correct whether
this module is loaded before or after that installation.
"""

from .certification_incremental_replication import REPLICATION_VERSION as _RUNTIME_REPLICATION_VERSION

ACTIVE_MANIFEST_VERSION = "positive-active-manifest-v1"
ACTIVE_MANIFEST_PROTOCOL_SUFFIX = f"+{ACTIVE_MANIFEST_VERSION}"


def _normalized_base_version(value: str) -> str:
    normalized = str(value)
    while normalized.endswith(ACTIVE_MANIFEST_PROTOCOL_SUFFIX):
        normalized = normalized[: -len(ACTIVE_MANIFEST_PROTOCOL_SUFFIX)]
    return normalized


BASE_REPLICATION_VERSION = _normalized_base_version(str(_RUNTIME_REPLICATION_VERSION))
ACTIVE_MANIFEST_REPLICATION_VERSION = BASE_REPLICATION_VERSION + ACTIVE_MANIFEST_PROTOCOL_SUFFIX
SUPPORTED_REPLICATION_VERSIONS = frozenset(
    {
        BASE_REPLICATION_VERSION,
        ACTIVE_MANIFEST_REPLICATION_VERSION,
    }
)


def is_supported_replication_version(value: object) -> bool:
    return str(value or "") in SUPPORTED_REPLICATION_VERSIONS


__all__ = [
    "ACTIVE_MANIFEST_PROTOCOL_SUFFIX",
    "ACTIVE_MANIFEST_REPLICATION_VERSION",
    "ACTIVE_MANIFEST_VERSION",
    "BASE_REPLICATION_VERSION",
    "SUPPORTED_REPLICATION_VERSIONS",
    "is_supported_replication_version",
]
