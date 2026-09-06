from __future__ import annotations

import ast
import json
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "solana_roi"


@dataclass(frozen=True)
class Installer:
    module: str
    name: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    imports: dict[str, tuple[str, str | None]]

    @property
    def key(self) -> str:
        return f"{self.module}:{self.name}"


def module_name(path: Path) -> str:
    rel = path.relative_to(ROOT / "src").with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def import_map(tree: ast.AST, current_module: str) -> dict[str, tuple[str, str | None]]:
    result: dict[str, tuple[str, str | None]] = {}
    package_parts = current_module.split(".")[:-1]
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                base = package_parts[: max(0, len(package_parts) - (node.level - 1))]
                if node.module:
                    base.extend(node.module.split("."))
                module = ".".join(base)
            else:
                module = node.module or ""
            for alias in node.names:
                if alias.name == "*":
                    continue
                result[alias.asname or alias.name] = (module, alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".")[0]
                result[local] = (alias.name, None)
    return result


def load_installers() -> tuple[dict[str, Installer], dict[str, list[str]], dict[str, ast.AST]]:
    installers: dict[str, Installer] = {}
    by_name: dict[str, list[str]] = {}
    trees: dict[str, ast.AST] = {}
    for path in SRC.rglob("*.py"):
        module = module_name(path)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        trees[module] = tree
        imports = import_map(tree, module)
        for node in getattr(tree, "body", []):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("install_"):
                installer = Installer(module, node.name, node, imports)
                installers[installer.key] = installer
                by_name.setdefault(node.name, []).append(installer.key)
    return installers, by_name, trees


def resolve_call(installer: Installer, call: ast.Call, installers: dict[str, Installer], by_name: dict[str, list[str]]) -> str | None:
    if isinstance(call.func, ast.Name):
        name = call.func.id
        if not name.startswith("install_"):
            return None
        imported = installer.imports.get(name)
        if imported and imported[1] is not None:
            key = f"{imported[0]}:{imported[1]}"
            return key if key in installers else None
        local = f"{installer.module}:{name}"
        if local in installers:
            return local
        matches = by_name.get(name, [])
        return matches[0] if len(matches) == 1 else None
    if isinstance(call.func, ast.Attribute) and call.func.attr.startswith("install_"):
        if isinstance(call.func.value, ast.Name):
            imported = installer.imports.get(call.func.value.id)
            if imported and imported[1] is None:
                key = f"{imported[0]}:{call.func.attr}"
                return key if key in installers else None
    return None


def _expr_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _expr_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    if isinstance(node, ast.Subscript):
        return _expr_name(node.value) + "[...]"
    return type(node).__name__


def mutation_targets(node: ast.AST) -> list[str]:
    targets: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            raw_targets: list[ast.AST]
            if isinstance(child, ast.Assign):
                raw_targets = list(child.targets)
            else:
                raw_targets = [child.target]
            for target in raw_targets:
                if isinstance(target, ast.Attribute):
                    targets.add(_expr_name(target))
        elif isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id == "setattr":
            if len(child.args) >= 2 and isinstance(child.args[1], ast.Constant) and isinstance(child.args[1].value, str):
                targets.add(f"{_expr_name(child.args[0])}.{child.args[1].value}")
    return sorted(targets)


def root_installers(trees: dict[str, ast.AST], installers: dict[str, Installer], by_name: dict[str, list[str]]) -> list[str]:
    roots: set[str] = set()
    module = "solana_roi.production_system"
    tree = trees[module]
    imports = import_map(tree, module)
    pseudo = Installer(module, "<production-root>", ast.FunctionDef(name="x", args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]), body=[], decorator_list=[]), imports)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_adapter":
            if len(node.args) >= 3 and all(isinstance(node.args[index], ast.Constant) for index in (1, 2)):
                target_module = str(node.args[1].value)
                target_name = str(node.args[2].value)
                key = f"{target_module}:{target_name}"
                if key in installers:
                    roots.add(key)
        elif isinstance(node, ast.Call):
            key = resolve_call(pseudo, node, installers, by_name)
            if key:
                roots.add(key)
    return sorted(roots)


def build_inventory() -> dict[str, Any]:
    installers, by_name, trees = load_installers()
    roots = root_installers(trees, installers, by_name)
    reached: set[str] = set()
    queue = deque(roots)
    edges: dict[str, list[str]] = {}
    unresolved_calls: dict[str, list[str]] = {}

    while queue:
        key = queue.popleft()
        if key in reached or key not in installers:
            continue
        reached.add(key)
        installer = installers[key]
        children: set[str] = set()
        unresolved: set[str] = set()
        for node in ast.walk(installer.node):
            if not isinstance(node, ast.Call):
                continue
            called_name = None
            if isinstance(node.func, ast.Name) and node.func.id.startswith("install_"):
                called_name = node.func.id
            elif isinstance(node.func, ast.Attribute) and node.func.attr.startswith("install_"):
                called_name = node.func.attr
            if not called_name:
                continue
            resolved = resolve_call(installer, node, installers, by_name)
            if resolved:
                children.add(resolved)
            else:
                unresolved.add(called_name)
        edges[key] = sorted(children)
        if unresolved:
            unresolved_calls[key] = sorted(unresolved)
        queue.extend(sorted(children))

    records: list[dict[str, Any]] = []
    unique_targets: set[str] = set()
    for key in sorted(reached):
        installer = installers[key]
        targets = mutation_targets(installer.node)
        unique_targets.update(targets)
        records.append(
            {
                "installer": key,
                "mutation_targets": targets,
                "calls_installers": edges.get(key, []),
                "unresolved_installer_calls": unresolved_calls.get(key, []),
            }
        )

    return {
        "root_installer_count": len(roots),
        "root_installers": roots,
        "transitive_installer_count": len(reached),
        "unique_mutation_target_count": len(unique_targets),
        "unique_mutation_targets": sorted(unique_targets),
        "installers": records,
        "unresolved_installer_calls": unresolved_calls,
        "paper_only_boundary_expected": True,
        "purpose": "repair_126_migration_inventory_not_runtime_authority",
    }


def main() -> int:
    print(json.dumps(build_inventory(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
