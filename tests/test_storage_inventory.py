from __future__ import annotations

import os
import runpy
from pathlib import Path

_inventory_module = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts" / "storage_inventory.py")
)
inventory_storage = _inventory_module["inventory_storage"]


def test_inventory_reports_sizes_and_top_level_consumers(tmp_path: Path) -> None:
    alpha = tmp_path / "alpha"
    beta = tmp_path / "beta"
    alpha.mkdir()
    beta.mkdir()
    (alpha / "small.db").write_bytes(b"a" * 11)
    (alpha / "large.db-wal").write_bytes(b"b" * 29)
    (beta / "state.json").write_bytes(b"c" * 7)

    payload = inventory_storage(tmp_path, top_n=2)

    assert payload["status"] == "ok"
    assert payload["read_only"] is True
    assert payload["file_contents_read"] is False
    assert payload["symlinks_followed"] is False
    assert payload["total_bytes"] == 47
    assert payload["file_count"] == 3
    assert payload["directory_count"] == 2
    assert payload["error_count"] == 0
    assert payload["top_level"][0]["name"] == "alpha"
    assert payload["top_level"][0]["bytes"] == 40
    assert payload["largest_files"] == [
        {"path": "alpha/large.db-wal", "bytes": 29},
        {"path": "alpha/small.db", "bytes": 11},
    ]


def test_inventory_never_follows_symlinks(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (outside / "outside.bin").write_bytes(b"x" * 101)
    (tmp_path / "inside.bin").write_bytes(b"y" * 5)
    os.symlink(outside, tmp_path / "outside-link")

    payload = inventory_storage(tmp_path)

    assert payload["status"] == "ok"
    assert payload["total_bytes"] == 5
    assert payload["file_count"] == 1
    assert payload["symlink_count"] == 1
    assert payload["symlinks_followed"] is False


def test_inventory_refuses_symlink_root(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    os.symlink(real, link)

    payload = inventory_storage(link)

    assert payload["status"] == "refused_symlink_root"
    assert payload["total_bytes"] == 0


def test_inventory_handles_missing_root_without_creating_it(tmp_path: Path) -> None:
    missing = tmp_path / "missing"

    payload = inventory_storage(missing)

    assert payload["status"] == "missing"
    assert not missing.exists()


def test_inventory_deduplicates_hardlinks(tmp_path: Path) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"z" * 13)
    os.link(first, second)

    payload = inventory_storage(tmp_path)

    assert payload["total_bytes"] == 13
    assert payload["file_count"] == 1
    assert payload["hardlink_duplicates"] == 1
