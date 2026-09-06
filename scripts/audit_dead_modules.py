from __future__ import annotations

import argparse
import ast
import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
PACKAGE_ROOT = SRC_ROOT / "solana_roi"
TEST_ROOT = ROOT / "tests"

# Explicit application entrypoints/public surfaces that need not have an inbound
# Python import to be live. Everything else must be referenced by source/tests or be
# deliberately added here with a concrete external-entrypoint reason.
EXTERNAL_ROOTS = {
    "solana_roi",
    "solana_roi.production",
    "solana_roi.api",
    "solana_roi.cli",
}


def module_for(path: Path) -> str:
    rel = path.relative_to(SRC_ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def source_modules() -> dict[str, Path]:
    return {
        module_for(path): path
        for path in PACKAGE_ROOT.rglob("*.py")
        if "__pycache__" not in path.parts
    }


def _current_package(module: str, path: Path) -> list[str]:
    parts = module.split(".")
    return parts if path.name == "__init__.py" else parts[:-1]


def _add_existing_prefix(target: str, modules: set[str], found: set[str]) -> None:
    parts = target.split(".")
    for end in range(len(parts), 0, -1):
        candidate = ".".join(parts[:end])
        if candidate in modules:
            found.add(candidate)
            return


def imports_from(path: Path, module: str, modules: set[str]) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return set()

    found: set[str] = set()
    current_package = _current_package(module, path)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "solana_roi" or alias.name.startswith("solana_roi."):
                    _add_existing_prefix(alias.name, modules, found)

        elif isinstance(node, ast.ImportFrom):
            if node.level:
                trim = max(0, node.level - 1)
                base = current_package[: len(current_package) - trim] if trim else list(current_package)
                if node.module:
                    base += node.module.split(".")
                base_name = ".".join(base)
            else:
                base_name = node.module or ""

            if base_name == "solana_roi" or base_name.startswith("solana_roi."):
                _add_existing_prefix(base_name, modules, found)
                for alias in node.names:
                    if alias.name != "*":
                        _add_existing_prefix(f"{base_name}.{alias.name}", modules, found)

        elif isinstance(node, ast.Call):
            dynamic_name: str | None = None
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "import_module"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                dynamic_name = node.args[0].value
            elif (
                isinstance(node.func, ast.Name)
                and node.func.id == "__import__"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                dynamic_name = node.args[0].value
            if dynamic_name and (dynamic_name == "solana_roi" or dynamic_name.startswith("solana_roi.")):
                _add_existing_prefix(dynamic_name, modules, found)

    found.discard(module)
    return found


def test_imports(modules: set[str]) -> set[str]:
    found: set[str] = set()
    if not TEST_ROOT.exists():
        return found
    for path in TEST_ROOT.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "solana_roi" or alias.name.startswith("solana_roi."):
                        _add_existing_prefix(alias.name, modules, found)
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if base == "solana_roi" or base.startswith("solana_roi."):
                    _add_existing_prefix(base, modules, found)
                    for alias in node.names:
                        if alias.name != "*":
                            _add_existing_prefix(f"{base}.{alias.name}", modules, found)
    return found


def has_main_guard(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return '__name__ == "__main__"' in text or "__name__ == '__main__'" in text


def inventory() -> dict[str, object]:
    module_paths = source_modules()
    modules = set(module_paths)
    edges: dict[str, set[str]] = {}
    inbound: dict[str, set[str]] = defaultdict(set)

    for module, path in module_paths.items():
        targets = imports_from(path, module, modules)
        edges[module] = targets
        for target in targets:
            inbound[target].add(module)

    tests = test_imports(modules)
    for target in tests:
        inbound[target].add("<tests>")

    external_roots = set(EXTERNAL_ROOTS)
    external_roots.update(module for module, path in module_paths.items() if has_main_guard(path))

    reachable: set[str] = set()
    queue = deque(sorted(root for root in external_roots if root in modules))
    while queue:
        current = queue.popleft()
        if current in reachable:
            continue
        reachable.add(current)
        for target in sorted(edges.get(current, ())):
            if target not in reachable:
                queue.append(target)

    orphan_modules = sorted(
        module
        for module, path in module_paths.items()
        if path.name != "__init__.py"
        and module not in external_roots
        and not inbound.get(module)
    )
    source_unreachable = sorted(
        module
        for module, path in module_paths.items()
        if path.name != "__init__.py"
        and module not in reachable
    )

    return {
        "module_count": len(modules),
        "external_roots": sorted(external_roots),
        "orphan_modules": orphan_modules,
        "orphan_count": len(orphan_modules),
        "source_unreachable_from_application_roots": source_unreachable,
        "source_unreachable_count": len(source_unreachable),
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit solana_roi modules for static dead-code candidates")
    parser.add_argument("--strict", action="store_true", help="fail when an entirely unreferenced module remains")
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = inventory()
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.strict and int(report["orphan_count"]) > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
