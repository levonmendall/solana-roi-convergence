from __future__ import annotations

import asyncio
import logging

from solana_roi import render_runtime_bootstrap_repair as bootstrap


def test_runtime_storage_inventory_logging_is_disabled_by_default(monkeypatch) -> None:
    calls: list[tuple[object, int]] = []
    monkeypatch.delenv("SOLANA_ROI_STORAGE_INVENTORY_LOG_ONCE", raising=False)
    monkeypatch.setattr(bootstrap, "_STORAGE_INVENTORY_LOG_EMITTED", False)
    monkeypatch.setattr(
        bootstrap,
        "inventory_storage",
        lambda root, *, top_n: calls.append((root, top_n)) or {"status": "ok"},
    )

    asyncio.run(bootstrap._emit_storage_inventory_once_if_enabled())

    assert calls == []
    assert bootstrap._STORAGE_INVENTORY_LOG_EMITTED is False


def test_runtime_storage_inventory_logging_emits_exactly_once(monkeypatch, caplog) -> None:
    calls: list[tuple[object, int]] = []
    monkeypatch.setenv("SOLANA_ROI_STORAGE_INVENTORY_LOG_ONCE", "true")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "test-release")
    monkeypatch.setattr(bootstrap, "_STORAGE_INVENTORY_LOG_EMITTED", False)
    monkeypatch.setattr(bootstrap, "_STORAGE_INVENTORY_START_DELAY_SECONDS", 0.0)

    def fake_inventory(root, *, top_n):
        calls.append((root, top_n))
        return {
            "status": "ok",
            "read_only": True,
            "file_contents_read": False,
            "sqlite_opened": False,
            "retention_changed": False,
            "cleanup_performed": False,
            "total_bytes": 123,
            "top_level": [{"name": "solana-roi.sqlite3", "bytes": 123}],
            "largest_files": [{"path": "solana-roi.sqlite3", "bytes": 123}],
        }

    monkeypatch.setattr(bootstrap, "inventory_storage", fake_inventory)
    caplog.set_level(logging.WARNING, logger=bootstrap.__name__)

    asyncio.run(bootstrap._emit_storage_inventory_once_if_enabled())
    asyncio.run(bootstrap._emit_storage_inventory_once_if_enabled())

    assert calls == [("/var/data", 20)]
    records = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("SOLANA_ROI_STORAGE_INVENTORY ")
    ]
    assert len(records) == 1
    assert '"release_commit":"test-release"' in records[0]
    assert '"diagnostic":"read_only_storage_inventory"' in records[0]
    assert '"file_contents_read":false' in records[0]
    assert '"sqlite_opened":false' in records[0]
    assert '"retention_changed":false' in records[0]
    assert '"cleanup_performed":false' in records[0]


def test_lifespan_starts_inventory_even_when_runtime_bootstrap_never_becomes_ready(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_inventory() -> None:
        calls.append("inventory")

    async def fake_bootstrap(stop: asyncio.Event) -> None:
        calls.append("bootstrap")
        await stop.wait()

    monkeypatch.setattr(bootstrap, "_emit_storage_inventory_once_if_enabled", fake_inventory)
    monkeypatch.setattr(bootstrap, "_bootstrap_and_run", fake_bootstrap)

    async def exercise() -> None:
        async with bootstrap._render_handoff_lifespan(None):
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert "inventory" in calls
            assert "bootstrap" in calls

    asyncio.run(exercise())

    assert calls.count("inventory") == 1
    assert calls.count("bootstrap") == 1
