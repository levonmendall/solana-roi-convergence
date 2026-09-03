from __future__ import annotations

from solana_roi import production  # noqa: F401 - installs the exact Render composition
from solana_roi import runtime as runtime_module


def test_same_release_can_reopen_the_same_persistent_store(monkeypatch, tmp_path):
    db_path = tmp_path / "solana-roi.sqlite3"
    release = "a" * 40
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(db_path))
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setenv("SOLANA_ROI_WALLET_PROFILES_JSON", "[]")
    monkeypatch.setenv("SOLANA_ROI_WALLET_DISCOVERY_ENABLED", "true")
    monkeypatch.setenv("SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE", "true")
    monkeypatch.setenv(
        "SOLANA_ROI_RPC_ENDPOINTS_JSON",
        '[{"name":"test-a","http":"https://rpc-a.invalid","ws":"wss://rpc-a.invalid"},'
        '{"name":"test-b","http":"https://rpc-b.invalid","ws":"wss://rpc-b.invalid"}]',
    )

    first = runtime_module.build_runtime()
    try:
        assert first.certification_epoch is not None
    finally:
        first.store.close()

    second = runtime_module.build_runtime()
    try:
        assert second.certification_epoch == first.certification_epoch
        with second.store._lock:
            row = second.store.db.execute(
                "SELECT started_at FROM certification_release_epochs WHERE release_commit=?",
                (release,),
            ).fetchone()
        assert row is not None
    finally:
        second.store.close()
