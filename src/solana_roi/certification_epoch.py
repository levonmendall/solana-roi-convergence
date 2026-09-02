from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any


def release_commit_from_env() -> str | None:
    for name in ("SOLANA_ROI_RELEASE_COMMIT", "RENDER_GIT_COMMIT", "GITHUB_SHA"):
        value = os.getenv(name, "").strip().lower()
        if re.fullmatch(r"[0-9a-f]{40}", value):
            return value
    return None


def ensure_release_certification_epoch(store: Any, *, now: datetime | None = None) -> datetime:
    """Return a persistent prospective evidence boundary for the exact release.

    A restart of the same release reuses its original boundary. A new release
    gets a new boundary, preventing pre-release evidence from certifying a
    changed runtime.
    """

    started_at = now or datetime.now(timezone.utc)
    release_commit = release_commit_from_env()
    if release_commit is None:
        return started_at
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS certification_release_epochs ("
            "release_commit TEXT PRIMARY KEY, started_at TEXT NOT NULL)"
        )
        store.db.execute(
            "INSERT OR IGNORE INTO certification_release_epochs(release_commit, started_at) VALUES (?, ?)",
            (release_commit, started_at.isoformat()),
        )
        row = store.db.execute(
            "SELECT started_at FROM certification_release_epochs WHERE release_commit=?",
            (release_commit,),
        ).fetchone()
    if row is None:
        raise RuntimeError("release certification epoch could not be persisted")
    return datetime.fromisoformat(str(row[0]))
