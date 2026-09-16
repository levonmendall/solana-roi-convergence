"""Build a detached, source-pinned v5.2 domain package. Never imports production.

Run from any directory: python extraction/v52/build.py --output /tmp/new-dist
All emitted economic definitions are verbatim source slices. Only their import
boundary changes. The single-position exit plan is a documented AST extraction.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
import textwrap
from pathlib import Path

BASE_RELEASE = 'b8d7ee2e672b52ffe314f8b285d00892cf171387'
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
SOURCE = REPO / 'src' / 'solana_roi'
COMMON = '''from __future__ import annotations
import json, math, statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable, Iterable, Mapping, Sequence
from contextvars import ContextVar
'''
POLICY_IMPORT = '''from .strategy_v52_authority import (
    AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, LIVE_MONEY_AUTHORITY, PAPER_ONLY,
    SIGNING_AVAILABLE, STRATEGY_VERSION, TRANSACTION_SUBMISSION_AVAILABLE,
    authority_fingerprint, execution_policy, position_policy, target_sizing_policy,
    detection_policy)
'''
WHOLE = ('v52_lane_contract', 'v52_continuous_evolution', 'v52_strategy_promotion',
         'strategy_v52_authority', 'v52_wallet_alpha_refinement')
SLICES = {
    'risk_conditioned_alpha_v5': (
        ('_v5_pre_context', '_context_key', 'robust_return_profile', '_lane_cap', '_regime_multiplier'), ''),
    'profit_first_entity_final': (('MarketRegime', 'ExitFeatures', 'ExitSignal', 'ExitAlphaModel'), ''),
    'profit_first_entity_strategy': (('EntityGraph', 'EntityLink'), ''),
    'v52_authoritative_strategy': (('_v52_solana_choose',), POLICY_IMPORT +
        'from . import risk_conditioned_alpha_v5 as solana_strategy\n'),
    'v52_adaptive_continuation_refinement': (('_solana_choose', '_contextual_position_policy'), POLICY_IMPORT +
        'from . import v52_authoritative_strategy as authoritative\n'
        'from .strategy_v52_authority import authority as strategy_authority, position_policy as canonical_position_policy\n'
        'from .v52_wallet_alpha_refinement import ContextualWalletScore, WalletAlphaRefinementLedger\n'),
    'v52_profit_confidence_completion': (('_completed_solana_choose', '_exit_features', '_exit_policy', '_learned_ttl_seconds'), POLICY_IMPORT +
        'from . import risk_conditioned_alpha_v5 as solana_strategy\n'
        'from . import v52_adaptive_continuation_refinement as adaptive\n'
        'from .profit_first_entity_final import ExitFeatures, ExitSignal\n'),
    'v52_profit_confidence_finalization': (('_final_solana_choose',), POLICY_IMPORT +
        'from . import v52_profit_confidence_completion as completion\n'
        'from . import v52_authoritative_strategy as authoritative\n'),
}
METHODS = {
    'source_adapter': ('profit_first_entity_final_research', 'FinalProfitFirstResearchAdapter',
        ('_confirmation_context', '_creator_flow_state', '_market_regime', '_seller_entity', '_flow_reversed')),
    'source_execution': ('profit_first_entity_research', 'ProfitFirstResearchAdapter',
        ('_deployer', '_entity_graph', '_confirmations')),
}
POLICIES = ('strategy_v52_authority.json', 'strategy_v52_profit_confidence_completion.json')


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def segment(source: str, node: ast.AST) -> str:
    start = min([node.lineno] + [d.lineno for d in getattr(node, 'decorator_list', [])])
    return '\n'.join(source.splitlines()[start - 1:node.end_lineno])


def closure(source: str, roots: tuple[str, ...]) -> tuple[str, list[str]]:
    """Resolve only in-module definition/constant dependencies, not legacy imports."""
    nodes = ast.parse(source).body
    names = {}
    for node in nodes:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    names[target.id] = node
    selected = set()
    todo = list(roots)
    while todo:
        name = todo.pop()
        if name in selected or name not in names:
            continue
        selected.add(name)
        node = names[name]
        todo.extend(n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load))
    selected_nodes = {id(names[name]) for name in selected}
    body = '\n\n'.join(segment(source, node) for node in nodes if id(node) in selected_nodes)
    # No source installer is permitted into the detached package.
    if any(n.startswith(('install_', 'configure_')) for n in selected):
        raise ValueError('unexpected installer in economic dependency closure')
    return body, sorted(selected)


def exit_plan(source: str) -> str:
    """Preserve the production per-position exit decision, not its persistence.

    Select statements from `features = ...` through the amount/stage decision.
    Remove only `_record_exit_signal` (the caller journals the returned decision).
    `continue` becomes `return None` because this function handles one position.
    Quotes, durable fills and settlement belong to the new transaction owner.
    """
    tree = ast.parse(source)
    sell = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_completed_solana_sell')
    loop = next(n for n in sell.body if isinstance(n, ast.For))
    start = next(i for i, n in enumerate(loop.body) if isinstance(n, ast.Assign) and
                 isinstance(n.targets[0], ast.Name) and n.targets[0].id == 'features')
    end = next(i for i, n in enumerate(loop.body) if isinstance(n, ast.Assign) and
               isinstance(n.targets[0], ast.Name) and n.targets[0].id == 'filled')
    body = [n for n in loop.body[start:end] if not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and
            isinstance(n.value.func, ast.Name) and n.value.func.id == '_record_exit_signal')]
    class OnePosition(ast.NodeTransformer):
        def visit_Continue(self, node):
            return ast.copy_location(ast.Return(value=ast.Constant(value=None)), node)
    body = [OnePosition().visit(n) for n in body]
    body += ast.parse("return {'token_raw': slice_raw, 'stage': next_stage, 'reason': reason, 'features': asdict(features), 'signal': asdict(base_signal)}").body
    fn = ast.FunctionDef(name='plan_exit', args=ast.arguments(posonlyargs=[], args=[ast.arg(arg=n) for n in
        ('self', 'item', 'lifecycle', 'row', 'lane')], kwonlyargs=[], kw_defaults=[], defaults=[]), body=body, decorator_list=[])
    return ast.unparse(ast.fix_missing_locations(fn)) + '\n'


def build(output: Path) -> dict:
    pins = json.loads((HERE / 'source-pins.json').read_text())
    for rel, expected in pins['sources'].items():
        if digest((REPO / rel).read_bytes()) != expected:
            raise ValueError(f'pinned source changed: {rel}')
    if output.exists():
        raise ValueError('output must be a new directory; never overwrite an existing runtime')
    output.mkdir(parents=True)
    package = output / 'roi_extracted'
    domain = package / 'domain'
    domain.mkdir(parents=True)
    (package / '__init__.py').write_text('"""Offline, isolated v5.2 first-lifecycle extraction."""\n')
    (domain / '__init__.py').write_text('"""Build-time extracted domain only; no production installers."""\n')
    emitted = {}
    for name in WHOLE:
        text = (SOURCE / f'{name}.py').read_text()
        (domain / f'{name}.py').write_text(text)
        emitted[name] = {'mode': 'verbatim_module', 'source': name + '.py'}
    for name, (roots, imports) in SLICES.items():
        source = (SOURCE / f'{name}.py').read_text()
        body, definitions = closure(source, roots)
        text = COMMON + imports + '\n' + body + '\n'
        (domain / f'{name}.py').write_text(text)
        emitted[name] = {'mode': 'verbatim_definitions', 'definitions': definitions}
    for dest, (src, cls, methods) in METHODS.items():
        text = (SOURCE / f'{src}.py').read_text()
        source_cls = next(n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == cls)
        nodes = [n for n in source_cls.body if isinstance(n, ast.FunctionDef) and n.name in methods]
        body = '\n\n'.join(segment(text, n) for n in nodes)
        (domain / f'{dest}.py').write_text(COMMON +
            'from .profit_first_entity_final import MarketRegime\n'
            'from .profit_first_entity_strategy import EntityGraph, EntityLink\n\n'
            'class SourceMethods:\n' + body + '\n')
        emitted[dest] = {'source': src, 'class': cls, 'methods': methods}
    plan = COMMON + 'from .v52_profit_confidence_completion import _exit_features, _learned_ttl_seconds, _parse_time, _exit_policy\n' + exit_plan((SOURCE / 'v52_profit_confidence_completion.py').read_text())
    (domain / 'exit_plan.py').write_text(plan)
    for name in POLICIES:
        shutil.copyfile(REPO / name, output / name)
    for name in ('kernel.py', 'runtime.py', 'demo.py'):
        shutil.copyfile(HERE / name, package / name)
    manifest = {
        'schema': 'isolated-v52-extraction-1', 'base_release': BASE_RELEASE,
        'policy_scope': 'checked-in v5.2 baseline plus adaptive/completion/finalization; no live strategy epoch imported',
        'paper_only': True, 'live_money_authority': False,
        'sources': pins['sources'], 'extracted': emitted,
        'files': {str(p.relative_to(output)): digest(p.read_bytes()) for p in sorted(output.rglob('*')) if p.is_file()},
    }
    (output / 'extraction-manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.output.resolve()), indent=2))
