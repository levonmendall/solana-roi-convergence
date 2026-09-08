from __future__ import annotations

import heapq
import os
import stat
from collections import defaultdict
from pathlib import Path
from typing import Any

DEFAULT_ROOT = Path("/var/data")
DEFAULT_TOP_N = 20


def _empty_bucket() -> dict[str, int]:
    return {
        "bytes": 0,
        "files": 0,
        "directories": 0,
        "symlinks": 0,
        "other": 0,
        "errors": 0,
    }


def inventory_storage(
    root: str | os.PathLike[str] = DEFAULT_ROOT,
    *,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Inventory filesystem metadata beneath root without reading file contents.

    Symlinks are counted but never followed. Regular-file byte totals are deduplicated
    by device/inode so hard links do not inflate the reported storage consumer.
    The function performs no writes, opens no SQLite connection, and has no cleanup
    or retention authority.
    """
    root_path = Path(root)
    result: dict[str, Any] = {
        "root": str(root_path),
        "read_only": True,
        "file_contents_read": False,
        "sqlite_opened": False,
        "retention_changed": False,
        "cleanup_performed": False,
        "symlinks_followed": False,
        "status": "ok",
        "total_bytes": 0,
        "file_count": 0,
        "directory_count": 0,
        "symlink_count": 0,
        "other_count": 0,
        "hardlink_duplicates": 0,
        "error_count": 0,
        "top_level": [],
        "largest_files": [],
    }

    try:
        root_stat = os.lstat(root_path)
    except FileNotFoundError:
        result["status"] = "missing"
        result["error"] = "root does not exist"
        return result
    except OSError as exc:
        result["status"] = "unavailable"
        result["error"] = f"{type(exc).__name__}: root metadata unavailable"
        return result

    if stat.S_ISLNK(root_stat.st_mode):
        result["status"] = "refused_symlink_root"
        result["error"] = "root is a symlink; refusing to traverse"
        return result
    if not stat.S_ISDIR(root_stat.st_mode):
        result["status"] = "not_directory"
        result["error"] = "root is not a directory"
        return result

    buckets: defaultdict[str, dict[str, int]] = defaultdict(_empty_bucket)
    seen_regular_files: set[tuple[int, int]] = set()
    largest: list[tuple[int, str]] = []
    limit = max(0, int(top_n))

    stack: list[tuple[Path, str | None]] = [(root_path, None)]
    while stack:
        directory, inherited_bucket = stack.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    relative = Path(entry.path).relative_to(root_path)
                    bucket_name = inherited_bucket or relative.parts[0]
                    bucket = buckets[bucket_name]
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except OSError:
                        result["error_count"] += 1
                        bucket["errors"] += 1
                        continue

                    mode = entry_stat.st_mode
                    if stat.S_ISLNK(mode):
                        result["symlink_count"] += 1
                        bucket["symlinks"] += 1
                        continue
                    if stat.S_ISDIR(mode):
                        result["directory_count"] += 1
                        bucket["directories"] += 1
                        stack.append((Path(entry.path), bucket_name))
                        continue
                    if stat.S_ISREG(mode):
                        inode_key = (int(entry_stat.st_dev), int(entry_stat.st_ino))
                        if inode_key in seen_regular_files:
                            result["hardlink_duplicates"] += 1
                            continue
                        seen_regular_files.add(inode_key)
                        size = int(entry_stat.st_size)
                        result["total_bytes"] += size
                        result["file_count"] += 1
                        bucket["bytes"] += size
                        bucket["files"] += 1
                        if limit:
                            item = (size, relative.as_posix())
                            if len(largest) < limit:
                                heapq.heappush(largest, item)
                            elif item > largest[0]:
                                heapq.heapreplace(largest, item)
                        continue

                    result["other_count"] += 1
                    bucket["other"] += 1
        except OSError:
            result["error_count"] += 1
            if inherited_bucket is not None:
                buckets[inherited_bucket]["errors"] += 1

    result["top_level"] = [
        {"name": name, **values}
        for name, values in sorted(
            buckets.items(), key=lambda item: (-item[1]["bytes"], item[0])
        )
    ]
    result["largest_files"] = [
        {"path": path, "bytes": size}
        for size, path in sorted(largest, key=lambda item: (-item[0], item[1]))
    ]
    return result


__all__ = ["DEFAULT_ROOT", "DEFAULT_TOP_N", "inventory_storage"]
