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
POLICY_PATH = ROOT / "module_reachability_policy.json"


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


def load_policy() -> dict[str, object]:
    payload = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("module reachability policy must be a JSON object")
    return payload


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
            # Current migration root has finite string registries. Treat both as
            # static dependency edges so the report reflects the actual graph, but
            # strict mode separately rejects the installer registry for Repair 126.
            elif (
                module == "solana_roi.production_system"
                and isinstance(node.func, ast.Name)
                and node.func.id in {"_adapter", "_component"}
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                dynamic_name = node.args[1].value
            if dynamic_name and (dynamic_name == "solana_roi" or dynamic_name.startswith("solana_roi.")):
                _add_existing_prefix(dynamic_name, modules, found)

    found.discard(module)
    return found


def _test_sources() -> list[tuple[Path, str]]:
    sources: list[tuple[Path, str]] = []
    if not TEST_ROOT.exists():
        return sources
    for path in TEST_ROOT.rglob("*.py"):
        try:
            sources.append((path, path.read_text(encoding="utf-8")))
        except OSError:
            continue
    return sources


def test_imports(modules: set[str], sources: list[tuple[Path, str]]) -> set[str]:
    found: set[str] = set()
    for path, text in sources:
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
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


def test_file_references(module_paths: dict[str, Path], sources: list[tuple[Path, str]]) -> set[str]:
    referenced: set[str] = set()
    file_access_markers = ("read_text", "read_bytes", ".exists()", "is_file()")
    for module, path in module_paths.items():
        basename = path.name
        for _test_path, text in sources:
            if basename in text and any(marker in text for marker in file_access_markers):
                referenced.add(module)
                break
    return referenced


def has_main_guard(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return '__name__ == "__main__"' in text or "__name__ == '__main__'" in text


def package_import_installer_calls() -> list[str]:
    path = PACKAGE_ROOT / "__init__.py"
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return ["unparseable_package_initializer"]
    calls: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id.startswith("install_"):
            calls.append(node.func.id)
    return sorted(set(calls))


def production_root_installer_debt() -> dict[str, list[str]]:
    """Detect installer activation in the two canonical composition source files."""
    result: dict[str, list[str]] = {}
    for module, path in (
        ("solana_roi.production", PACKAGE_ROOT / "production.py"),
        ("solana_roi.production_system", PACKAGE_ROOT / "production_system.py"),
    ):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            result[module] = ["unparseable"]
            continue
        debt: set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id.startswith("install_"):
                debt.add(node.func.id)
            if isinstance(node.func, ast.Name) and node.func.id == "_adapter":
                debt.add("compatibility_adapter_registry")
            if isinstance(node.func, ast.Attribute) and node.func.attr == "activate":
                debt.add("compatibility_adapter_activation")
        if debt:
            result[module] = sorted(debt)
    return result


def _reachable_from(root: str, modules: set[str], edges: dict[str, set[str]]) -> set[str]:
    reachable: set[str] = set()
    queue = deque([root] if root in modules else [])
    while queue:
        current = queue.popleft()
        if current in reachable:
            continue
        reachable.add(current)
        for target in sorted(edges.get(current, ())):
            if target not in reachable:
                queue.append(target)
    return reachable


def inventory() -> dict[str, object]:
    policy = load_policy()
    module_paths = source_modules()
    modules = set(module_paths)
    edges: dict[str, set[str]] = {}
    inbound_source: dict[str, set[str]] = defaultdict(set)

    for module, path in module_paths.items():
        targets = imports_from(path, module, modules)
        edges[module] = targets
        for target in targets:
            inbound_source[target].add(module)

    sources = _test_sources()
    imported_by_tests = test_imports(modules, sources)
    file_referenced_by_tests = test_file_references(module_paths, sources)
    test_referenced = imported_by_tests | file_referenced_by_tests

    production_root = str(policy.get("production_root") or "solana_roi.production")
    production_reachable = _reachable_from(production_root, modules, edges)

    declared_test_only = {str(item) for item in policy.get("test_only", [])}
    declared_migration_only = {str(item) for item in policy.get("migration_only", [])}
    unknown_policy_modules = sorted((declared_test_only | declared_migration_only) - modules)
    classification_overlap = sorted(declared_test_only & declared_migration_only)
    production_policy_conflicts = sorted(production_reachable & (declared_test_only | declared_migration_only))

    # Standalone CLI/main-guard modules are still source, but they do not gain
    # production authority merely because they can be invoked manually.
    main_guard_modules = sorted(module for module, path in module_paths.items() if has_main_guard(path))
    test_only_observed = sorted((test_referenced - production_reachable) & declared_test_only)
    migration_only_observed = sorted(declared_migration_only - production_reachable)

    classified = production_reachable | declared_test_only | declared_migration_only
    # Package __init__ is a passive namespace and is classified as infrastructure.
    classified.add("solana_roi")
    unclassified_unreachable = sorted(
        module
        for module, path in module_paths.items()
        if path.name != "__init__.py" and module not in classified
    )
    test_referenced_unclassified = sorted((test_referenced - production_reachable) - declared_test_only - declared_migration_only)

    orphan_modules = sorted(
        module
        for module, path in module_paths.items()
        if path.name != "__init__.py"
        and module not in production_reachable
        and not inbound_source.get(module)
        and module not in declared_test_only
        and module not in declared_migration_only
    )

    package_installers = package_import_installer_calls()
    production_installer_debt = production_root_installer_debt()

    return {
        "policy_version": policy.get("policy_version"),
        "module_count": len(modules),
        "production_root": production_root,
        "production_reachable_modules": sorted(production_reachable),
        "production_reachable_count": len(production_reachable),
        "test_only_declared": sorted(declared_test_only),
        "test_only_observed": test_only_observed,
        "migration_only_declared": sorted(declared_migration_only),
        "migration_only_observed": migration_only_observed,
        "main_guard_modules_without_implicit_production_authority": main_guard_modules,
        "test_referenced_but_not_production_reachable": sorted(test_referenced - production_reachable),
        "test_referenced_unclassified": test_referenced_unclassified,
        "unclassified_unreachable_modules": unclassified_unreachable,
        "unclassified_unreachable_count": len(unclassified_unreachable),
        "unknown_policy_modules": unknown_policy_modules,
        "classification_overlap": classification_overlap,
        "production_policy_conflicts": production_policy_conflicts,
        "orphan_modules": orphan_modules,
        "orphan_count": len(orphan_modules),
        "package_import_installer_calls": package_installers,
        "package_import_installer_call_count": len(package_installers),
        "package_import_is_side_effect_free": len(package_installers) == 0,
        "production_root_installer_debt": production_installer_debt,
        "production_root_has_installer_debt": bool(production_installer_debt),
        "tests_grant_production_reachability": False,
        "unreachable_modules_fail_ci": True,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit solana_roi production reachability and explicit non-production classifications")
    parser.add_argument("--strict", action="store_true", help="fail on architecture/reachability ambiguity")
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = inventory()
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.strict and (
        int(report["unclassified_unreachable_count"]) > 0
        or int(report["package_import_installer_call_count"]) > 0
        or bool(report["production_root_has_installer_debt"])
        or bool(report["unknown_policy_modules"])
        or bool(report["classification_overlap"])
        or bool(report["production_policy_conflicts"])
        or bool(report["test_referenced_unclassified"])
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
