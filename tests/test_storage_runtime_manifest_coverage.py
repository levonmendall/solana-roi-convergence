from __future__ import annotations


def test_build_runtime_creates_only_positive_manifest_tables(tmp_path, monkeypatch):
    """Production constructor reachability may not create unclassified persistence.

    This intentionally builds the real runtime composition without starting any
    network workers. If a future constructor adds a durable table, storage
    cutover must classify it before the change can pass CI.
    """
    database = tmp_path / "constructor-footprint.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(database))
    monkeypatch.setenv("RENDER_GIT_COMMIT", "a" * 40)
    monkeypatch.delenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", raising=False)
    monkeypatch.delenv("SOLANA_ROI_ACTIVE_STORAGE_SHADOW", raising=False)
    monkeypatch.delenv("SOLANA_ROI_ACTIVE_DB_PATH", raising=False)
    monkeypatch.delenv("SOLANA_ROI_WALLET_PROFILES_JSON", raising=False)
    monkeypatch.delenv("JUPITER_API_KEY", raising=False)

    # Importing production first applies the same compatibility composition used
    # by the launched service; build_runtime itself still owns storage selection.
    import solana_roi.production  # noqa: F401
    from solana_roi.runtime import build_runtime
    from solana_roi.storage_manifest import RETENTION_REGISTRY

    runtime = build_runtime()
    try:
        with runtime.store._lock:
            tables = {
                str(row[0])
                for row in runtime.store.db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }
        unknown = sorted(tables - set(RETENTION_REGISTRY))
        assert not unknown, f"production runtime created unclassified persistent tables: {unknown}"
    finally:
        runtime.store.close()
