"""Explicit v5.2 economic boundary over an ephemeral point-in-time evidence view.

No legacy package import, runtime installer, external I/O or durable portfolio
write is reachable here. Actual upstream economic definitions are extracted at
build time. This first milestone deliberately supports cold-start Pump.fun
starters and their exits, not policy promotion, scale-ins or all-market operation.
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from .domain import risk_conditioned_alpha_v5 as risk
from .domain import strategy_v52_authority as policy
from .domain import v52_authoritative_strategy as sizing
from .domain import v52_adaptive_continuation_refinement as adaptive
from .domain import v52_profit_confidence_completion as completion
from .domain import v52_profit_confidence_finalization as finalization
from .domain import v52_wallet_alpha_refinement as wallet
from .domain.exit_plan import plan_exit
from .domain.source_adapter import SourceMethods as AdapterMethods
from .domain.source_execution import SourceMethods as ExecutionMethods
from .domain.profit_first_entity_final import ExitAlphaModel
from .domain.profit_first_entity_strategy import EntityGraph, EntityLink

BASE_RELEASE = 'b8d7ee2e672b52ffe314f8b285d00892cf171387'
NATIVE = 1_000_000_000
USD = 1_000_000_000
MAX_EVIDENCE_ROWS = 256  # Admission bound, never truncate evidence to manufacture eligibility.
_EVALUATION = ContextVar('isolated_v52_evaluation')


class EvidenceError(ValueError):
    pass


def timestamp(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        raise EvidenceError('timezone-aware evidence required')
    return value.astimezone(timezone.utc)


def positive_int(value: Any) -> int:
    if type(value) is not int or value <= 0:
        raise EvidenceError('positive integer amount required')
    return value


def finite(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise EvidenceError('nonfinite evidence')
    return result


class AtDecisionClock(datetime):
    @classmethod
    def now(cls, tz=None):
        at = _EVALUATION.get().at
        return at.astimezone(tz) if tz else at.replace(tzinfo=None)


class WalletPort:
    def score(self, address, context_key):
        scope = _EVALUATION.get()
        return scope.wallet.score(address, context_key, as_of=scope.at)


# This is the complete, fixed call graph for the retained Solana selector.
# No installer discovers predecessors, no class method is patched, and no global
# in solana_roi is imported or modified. Per-decision state uses ContextVar.
sizing.position_policy = adaptive._contextual_position_policy
adaptive._BASE_SOLANA_CHOOSE = sizing._v52_solana_choose
adaptive._WALLET_ALPHA = WalletPort()
completion._BASE_SOLANA_CHOOSE = adaptive._solana_choose
finalization._BASE_SOLANA_CHOOSE = completion._completed_solana_choose
# Replace only the external wall-clock dependency, never an economic expression.
completion.datetime = AtDecisionClock
sizing.datetime = AtDecisionClock


class EvidenceView:
    def __init__(self, risk_snapshot):
        self._lock = threading.RLock()
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.risk_snapshot = risk_snapshot
        self.db.executescript('''
        CREATE TABLE wallet_discovery_forward_observations (
            id INTEGER PRIMARY KEY, signature TEXT UNIQUE, wallet TEXT, token_mint TEXT,
            side TEXT, token_amount REAL, observed_at TEXT, received_at TEXT,
            wallet_price_sol REAL, copyable_price_sol REAL, chase_fraction REAL,
            copyable INTEGER, observation_lag_ms REAL, risk_complete INTEGER,
            manipulation_flag INTEGER, side_wallet_flag INTEGER, source TEXT);
        CREATE TABLE risk_conditioned_alpha_v5_trials (
            id INTEGER PRIMARY KEY, release_commit TEXT, strategy_version TEXT,
            source_signature TEXT, token_mint TEXT, position_fraction REAL, lane TEXT,
            venue TEXT, lifecycle TEXT, regime TEXT, flow_state TEXT, risk_severity REAL,
            selected INTEGER, decision TEXT);
        CREATE TABLE risk_conditioned_alpha_v5_outcomes (
            id INTEGER PRIMARY KEY, release_commit TEXT, strategy_version TEXT,
            source_signature TEXT, lane TEXT, context_key TEXT, net_return REAL);
        CREATE TABLE profit_first_final_trials (
            id INTEGER PRIMARY KEY, epoch_id TEXT, source_signature TEXT, lane TEXT,
            entry_all_in_price_sol REAL, opportunity_json TEXT);
        ''')
        completion._schema(self)

    def append(self, *_args, **_kwargs):
        # Upstream wallet ledger's generic research-event mirror is ephemeral.
        # The canonical receipt records the supplied evidence and actual score once.
        return None

    def latest_risk_evidence(self, token, kind, *, as_of_received_at):
        value = self.risk_snapshot
        if token == value['token_mint'] and kind == 'deployer' and timestamp(value['available_at']) <= timestamp(as_of_received_at):
            return {'payload': {'deployer_wallet': value['deployer_wallet']}}
        return None

    def close(self):
        self.db.close()


class Resolver:
    def __init__(self, links):
        self.links = links

    def component(self, address, *, as_of):
        known = [link for link in self.links if timestamp(link['available_at']) <= as_of]
        graph = EntityGraph(EntityLink(x['left'], x['right'], 'supplied_point_in_time_link') for x in known)
        addresses = {address} | {x[k] for x in known for k in ('left', 'right')}
        entity = graph.entity_id(address)
        return {x for x in addresses if graph.entity_id(x) == entity}


class Execution(ExecutionMethods):
    def __init__(self, store, discovery):
        self.store = store
        self.discovery = discovery


class Adapter(AdapterMethods):
    def __init__(self, store, links):
        self.store = store
        self.release_commit = BASE_RELEASE
        self.epoch_id = 'isolated-v52-first-lifecycle'
        self.discovery = SimpleNamespace(entity_resolver=Resolver(links))
        self.execution = Execution(store, self.discovery)
        self.strategy = SimpleNamespace(exit_model=ExitAlphaModel())


def _eligible_evidence(event, state):
    at = timestamp(event['decision_at'])
    row = dict(event['observation'])
    for key in ('observed_at', 'received_at'):
        row[key] = timestamp(row[key]).isoformat()
    if row['signature'] != event['id'] or row['side'] not in ('buy', 'sell'):
        raise EvidenceError('observation identity mismatch')
    if not timestamp(row['observed_at']) <= timestamp(row['received_at']) <= at:
        raise EvidenceError('observation is from the future or reverses time')
    # This milestone accepts exactly-at-decision evidence, avoiding invented quote
    # freshness/latency policies. Later delayed-provider integration is out of scope.
    if timestamp(row['received_at']) != at:
        raise EvidenceError('scope_requires_decision_at_receipt')
    if row['source'] != 'solana-direct:PUMP_FUN:' + row['side']:
        raise EvidenceError('scope_unsupported_surface')
    if not str(row['token_mint']) or not str(row['wallet']):
        raise EvidenceError('missing asset/wallet identity')
    if not row.get('risk_complete') or not row.get('copyable'):
        raise EvidenceError('incomplete_or_uncopyable_observation')
    if finite(row['wallet_price_sol']) <= 0 or finite(row['token_amount']) <= 0:
        raise EvidenceError('invalid price or amount')
    snapshot = event['risk']
    if snapshot['token_mint'] != row['token_mint'] or not snapshot.get('complete'):
        raise EvidenceError('missing complete risk snapshot')
    if not timestamp(snapshot['observed_at']) <= timestamp(snapshot['available_at']) <= at:
        raise EvidenceError('risk evidence not available at decision')
    if timestamp(snapshot['available_at']) != at:
        raise EvidenceError('scope_requires_current_risk_snapshot')
    raw_history = list(state['observations'].values()) + list(event.get('history', [])) + [row]
    raw_episodes = list(state['wallet_episodes'].values()) + list(event.get('wallet_episodes', []))
    raw_links = list(state['entity_links'].values()) + list(event.get('entity_links', []))
    if any(len(group) > MAX_EVIDENCE_ROWS for group in (raw_history, raw_episodes, raw_links)):
        raise EvidenceError('scope_evidence_budget_exceeded_no_truncation')
    excluded = 0
    observations, episodes, links = {}, {}, {}
    for original in raw_history:
        item = dict(original)
        for key in ('observed_at', 'received_at'):
            item[key] = timestamp(item[key]).isoformat()
        if timestamp(item['observed_at']) > timestamp(item['received_at']):
            raise EvidenceError('history timestamp inversion')
        if timestamp(item['received_at']) > at:
            excluded += 1
            continue
        key = item['signature']
        if key in observations and observations[key] != item:
            raise EvidenceError('conflicting historical observation')
        observations[key] = item
    for original in raw_episodes:
        item = dict(original)
        for key in ('observed_at', 'available_at'):
            item[key] = timestamp(item[key]).isoformat()
        if timestamp(item['observed_at']) > timestamp(item['available_at']):
            raise EvidenceError('wallet outcome timestamp inversion')
        if timestamp(item['available_at']) > at:
            excluded += 1
            continue
        if not item.get('prospective') or not item.get('paired_same_candidate_stream'):
            raise EvidenceError('wallet evidence lacks prospective matched-control provenance')
        key = '|'.join((item['wallet'], item['context_key'], item['candidate_id']))
        if key in episodes and episodes[key] != item:
            raise EvidenceError('conflicting wallet evidence')
        episodes[key] = item
    for original in raw_links:
        item = dict(original)
        for key in ('observed_at', 'available_at'):
            item[key] = timestamp(item[key]).isoformat()
        if timestamp(item['observed_at']) > timestamp(item['available_at']):
            raise EvidenceError('entity link timestamp inversion')
        if timestamp(item['available_at']) > at:
            excluded += 1
            continue
        key = '|'.join(sorted((item['left'], item['right'])))
        if key in links and links[key] != item:
            raise EvidenceError('conflicting entity evidence')
        links[key] = item
    return at, row, snapshot, observations, episodes, links, excluded


@contextmanager
def evaluation(event, state):
    at, row, snapshot, observations, episodes, links, excluded = _eligible_evidence(event, state)
    view = EvidenceView(snapshot)
    scope = SimpleNamespace(at=at, wallet=None)
    context = _EVALUATION.set(scope)
    try:
        adapter = Adapter(view, list(links.values()))
        columns = ('signature,wallet,token_mint,side,token_amount,observed_at,received_at,'
                   'wallet_price_sol,copyable_price_sol,chase_fraction,copyable,observation_lag_ms,'
                   'risk_complete,manipulation_flag,side_wallet_flag,source').split(',')
        with view.db:
            for item in sorted(observations.values(), key=lambda x: (x['received_at'], x['signature'])):
                view.db.execute('INSERT INTO wallet_discovery_forward_observations (' + ','.join(columns) + ') VALUES (' + ','.join('?' for _ in columns) + ')', [item.get(k) for k in columns])
        scope.wallet = wallet.WalletAlphaRefinementLedger(view)
        for item in sorted(episodes.values(), key=lambda x: (x['observed_at'], x['candidate_id'])):
            fields = {k: item[k] for k in ('wallet','context_key','candidate_id','wallet_policy_return','matched_control_return','executable_mfe','executable_mae','copyable')}
            fields['observed_at'] = timestamp(item['observed_at'])
            scope.wallet.record_paired(wallet.WalletMarginalAlphaObservation(**fields))
        # Current ownership is explicitly projected from the NEW portfolio only.
        # No live production checkpoint, predecessor, raw history or registry opens.
        for position in state['positions'].values():
            if position['remaining_raw']:
                data = position['signal_event']
                cols = list(data)
                view.db.execute('INSERT INTO v52_profit_signal_events (' + ','.join(cols) + ') VALUES (' + ','.join('?' for _ in cols) + ')', [data[k] for k in cols])
        view.db.commit()
        yield SimpleNamespace(adapter=adapter, row=row, at=at, risk=snapshot, wallet=scope.wallet,
                              observations=observations, episodes=episodes, links=links, excluded=excluded)
    finally:
        _EVALUATION.reset(context)
        view.close()


def _quote(event, kind, amount, token):
    matches = [q for q in event.get('quotes', []) if q.get('kind') == kind and q.get('input_raw') == amount and q.get('token_mint') == token]
    if len(matches) != 1:
        raise EvidenceError(f'exact_amount_{kind}_quote_missing_or_ambiguous')
    q = dict(matches[0])
    positive_int(q['input_raw']); positive_int(q['out_raw'])
    if type(q['fee_lamports']) is not int or q['fee_lamports'] < 0:
        raise EvidenceError('invalid quote fees')
    if not timestamp(q['observed_at']) <= timestamp(q['available_at']) <= timestamp(event['decision_at']):
        raise EvidenceError('future quote')
    if timestamp(q['available_at']) != timestamp(event['decision_at']):
        raise EvidenceError('scope_requires_exact_at_decision_quote')
    if q.get('source') != 'synthetic-exact-quote-fixture' or not q.get('quote_id'):
        raise EvidenceError('this milestone accepts isolated synthetic quote fixtures only')
    if kind == 'entry' and q['slot'] <= event['observation']['slot']:
        raise EvidenceError('first_slot_sniping_not_permitted')
    return q


def decide(event, state):
    current = policy.canonical_strategy_evolution().current
    baseline = policy.new_strategy_evolution().current
    if current.sequence != 1 or dict(current.config) != dict(baseline.config):
        raise EvidenceError('scope_promoted_policy_requires_explicit_import_and_validation')
    with evaluation(event, state) as e:
        row, adapter = e.row, e.adapter
        basis = {'evidence_excluded_future': e.excluded,
                 'observations': e.observations, 'wallet_episodes': e.episodes, 'entity_links': e.links}
        token = row['token_mint']
        if row['side'] == 'sell':
            opened = [p for p in state['positions'].values() if p['token'] == token and p['remaining_raw']]
            if not opened:
                return {**basis, 'action': 'observe', 'reason': 'no_open_position'}
            p = opened[0]
            item = {'source_signature': p['entry_id'], 'token_mint': token, 'trigger_wallet': p['wallet'],
                    'opportunity_json': json.dumps(p['opportunity'])}
            lifecycle = {'remaining_token_raw': p['remaining_raw'], 'total_token_raw': p['total_raw'],
                         'derisk_stage': p['stage'], 'entry_observed_at': p['opened_at']}
            plan = plan_exit(adapter, item, lifecycle, row, p['lane'])
            if plan is None:
                mark = _quote(event, 'mark', p['remaining_raw'], token)
                return {**basis, 'action': 'hold', 'reason': 'v52_exit_conditions_not_met', 'position_id': p['entry_id'], 'mark': mark}
            quote = _quote(event, 'exit', plan['token_raw'], token)
            if quote['out_raw'] <= quote['fee_lamports']:
                return {**basis, 'action': 'hold', 'reason': 'exit_net_quote_not_positive'}
            return {**basis, 'action': 'exit', 'position_id': p['entry_id'], 'plan': plan, 'quote': quote}
        pre = risk._v5_pre_context(adapter, row, hard=e.risk['hard_flags'], soft=e.risk['soft_flags'], early_exit=finite(e.risk['early_exit_fraction']))
        if not pre['risk']['structurally_tradeable']:
            return {**basis, 'action': 'reject', 'reason': 'reject_mechanical_hard_stop', 'risk': pre['risk']}
        if state['settlements']:
            raise EvidenceError('scope_post_settlement_learning_not_implemented')
        if any(p['token'] == token for p in state['positions'].values()):
            raise EvidenceError('scope_scale_or_reentry_not_implemented')
        # Zero outcomes in this NEW prospective cold-start experiment. Importing
        # evolved/promoted production policy or historical trade priors is not supported.
        latency = (e.at - timestamp(row['observed_at'])).total_seconds()
        lane, fraction, profiles = finalization._final_solana_choose(adapter, pre, chase=0.0, latency=latency)
        if not lane or fraction <= 0:
            return {**basis, 'action': 'reject', 'reason': 'v52_qualification_failed', 'profiles': profiles}
        sol_usd_nano = positive_int(event['sol_usd_nano'])
        result = None
        for _ in range(int(policy.execution_policy()['maximum_sizing_requotes']) + 1):
            amount = max(1, int(round(500.0 * fraction / (sol_usd_nano / USD) * NATIVE)))
            buy = _quote(event, 'entry', amount, token)
            qty = buy['out_raw']
            sell = _quote(event, 'exit', qty, token)
            depth = _quote(event, 'depth', int(round(qty * float(policy.position_policy()['minimum_exit_depth_coverage_ratio']))), token)
            if sell['out_raw'] <= sell['fee_lamports'] or depth['out_raw'] <= depth['fee_lamports']:
                raise EvidenceError('two_sided_exit_depth_not_executable')
            decimals = event['token_decimals']
            if type(decimals) is not int or not 0 <= decimals <= 18:
                raise EvidenceError('invalid token decimals')
            cost = amount + buy['fee_lamports']
            entry_price = cost / NATIVE / (qty / 10 ** decimals)
            chase = max(0.0, entry_price / finite(row['wallet_price_sol']) - 1.0)
            next_lane, next_fraction, next_profiles = finalization._final_solana_choose(adapter, pre, chase=chase, latency=latency)
            if not next_lane or next_fraction <= 0:
                return {**basis, 'action': 'reject', 'reason': 'v52_exact_quote_qualification_failed', 'profiles': next_profiles}
            if next_lane == lane and abs(next_fraction - fraction) <= 1e-9:
                result = (buy, sell, depth, cost, entry_price, chase, next_profiles)
                break
            lane, fraction, profiles = next_lane, next_fraction, next_profiles
        if result is None:
            raise EvidenceError('exact_sizing_requotes_did_not_converge')
        buy, sell, depth, cost, entry_price, chase, profiles = result
        context_key = profiles[lane]['context_key']
        score = e.wallet.score(row['wallet'], context_key, as_of=e.at)
        op = {'creator_entity': pre['creator_entity'], 'early_buyer_exit_fraction': pre['early_exit'],
              'independent_confirmation_count': pre['independent_count']}
        return {**basis, 'action': 'entry', 'lane': lane, 'fraction': fraction, 'profiles': profiles,
                'wallet_score': asdict(score), 'buy': buy, 'mark': sell, 'depth': depth,
                'cost_lamports': cost, 'entry_price_sol': entry_price, 'chase_fraction': chase,
                'opportunity': op, 'lifecycle': pre['lifecycle'], 'regime': pre['regime'], 'pre': {k:v for k,v in pre.items() if k != 'at'}}
