"""Single-writer, restart-safe offline paper ledger for the extracted v5.2 slice.

This is a new explicitly labelled synthetic experiment, never a migration or
successor to production. Only this module writes its SQLite ledger. Economic
functions work on an ephemeral evidence view; they cannot commit portfolio state.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Callable

from .kernel import BASE_RELEASE, USD, NATIVE, EvidenceError, decide, timestamp, positive_int

MAGIC = 'isolated-v52-synthetic-paper-lifecycle-v1'
MAX_EVENT_BYTES = 128 * 1024
MAX_EVENTS = 512  # This acceptance harness never becomes an unbounded history engine.
MAX_LEDGER_BYTES = 32 * 1024 * 1024
ROOT = Path(__file__).resolve().parents[1]


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def artifact_identity() -> str:
    manifest = json.loads((ROOT / 'extraction-manifest.json').read_text())
    if manifest['base_release'] != BASE_RELEASE:
        raise EvidenceError('extraction base release mismatch')
    for name, expected in manifest['files'].items():
        path = (ROOT / name).resolve()
        if not path.is_relative_to(ROOT.resolve()) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise EvidenceError('detached artifact integrity mismatch: ' + name)
    return digest(manifest)


def money(lamports: int, sol_usd_nano: int, *, debit=False) -> int:
    numerator = lamports * sol_usd_nano
    return (numerator + NATIVE - 1) // NATIVE if debit else numerator // NATIVE


def initial_state(run_id: str, identity: str) -> dict:
    return {'run_id': run_id, 'artifact_identity': identity, 'base_release': BASE_RELEASE,
            'mode': 'synthetic_offline_paper', 'live_money_authority': False,
            'initial_cash_nano': 500 * USD, 'cash_nano': 500 * USD,
            'realized_pnl_nano': 0, 'clock': None, 'sequence': 0,
            'observations': {}, 'wallet_episodes': {}, 'entity_links': {},
            'positions': {}, 'settlements': {}, 'head_hash': '0' * 64}


def state_hash(state: dict) -> str:
    return digest({k: v for k, v in state.items() if k != 'head_hash'})


def summary(state: dict) -> dict:
    open_positions = [p for p in state['positions'].values() if p['remaining_raw'] > 0]
    marked = all(p['mark_nano'] is not None for p in open_positions)
    basis = sum(p['remaining_basis_nano'] for p in open_positions)
    marks = sum(p['mark_nano'] or 0 for p in open_positions)
    return {'run_id': state['run_id'], 'sequence': state['sequence'],
            'cash_nano': state['cash_nano'], 'reserved_basis_nano': basis,
            'unrealized_pnl_nano': marks - basis if marked else None,
            'realized_pnl_nano': state['realized_pnl_nano'],
            'nav_nano': state['cash_nano'] + marks if marked else None,
            'open_positions': len(open_positions), 'settlements': len(state['settlements']),
            'paper_only': True, 'live_money_authority': False,
            'signing_available': False, 'transaction_submission_available': False,
            'evidence_mode': 'synthetic_offline_paper', 'production_certified': False}


class Runtime:
    def __init__(self, path: Path):
        self.path = Path(path).resolve()
        if self.path.is_relative_to(Path('/var/data')):
            raise EvidenceError('production disk paths forbidden')
        if not self.path.is_file():
            raise EvidenceError('explicit new experiment initialization required')
        # Check ownership read-only BEFORE opening a writer or setting WAL.
        with closing(sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)) as db:
            try:
                meta = dict(db.execute('SELECT key,value FROM metadata'))
            except sqlite3.Error as exc:
                raise EvidenceError('not an extraction-owned ledger') from exc
        self.identity = artifact_identity()
        if meta.get('magic') != MAGIC or meta.get('artifact_identity') != self.identity:
            raise EvidenceError('experiment ownership/policy identity mismatch')
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA busy_timeout=5000')
        self.verify()

    @classmethod
    def create(cls, path: Path, run_id: str):
        path = Path(path).resolve()
        if not run_id or path.is_relative_to(Path('/var/data')):
            raise EvidenceError('new named offline experiment outside production is required')
        identity = artifact_identity()
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        state = initial_state(run_id, identity)
        with closing(sqlite3.connect(path)) as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            db.executescript('''
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE current_state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE journal (
                seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
                event_json TEXT NOT NULL, event_hash TEXT NOT NULL, receipt_json TEXT NOT NULL,
                state_hash TEXT NOT NULL, previous_hash TEXT NOT NULL, chain_hash TEXT NOT NULL);
            CREATE TRIGGER journal_no_update BEFORE UPDATE ON journal BEGIN SELECT RAISE(ABORT,'append-only journal'); END;
            CREATE TRIGGER journal_no_delete BEFORE DELETE ON journal BEGIN SELECT RAISE(ABORT,'append-only journal'); END;
            CREATE TRIGGER metadata_no_update BEFORE UPDATE ON metadata BEGIN SELECT RAISE(ABORT,'immutable experiment identity'); END;
            CREATE TRIGGER metadata_no_delete BEFORE DELETE ON metadata BEGIN SELECT RAISE(ABORT,'immutable experiment identity'); END;
            ''')
            db.executemany('INSERT INTO metadata VALUES (?,?)', [
                ('magic', MAGIC), ('run_id', run_id), ('artifact_identity', identity), ('base_release', BASE_RELEASE)])
            db.execute('INSERT INTO current_state VALUES (1,?,?)', (canonical(state), state_hash(state)))
            db.commit()
        return cls(path)

    def close(self):
        self.db.close()

    def _state(self):
        row = self.db.execute('SELECT payload,digest FROM current_state WHERE id=1').fetchone()
        if row is None:
            raise EvidenceError('state unavailable')
        state = json.loads(row['payload'])
        if state_hash(state) != row['digest'] or state['artifact_identity'] != self.identity:
            raise EvidenceError('state integrity/policy mismatch')
        return state

    def verify(self):
        # First-milestone ledger has a fixed admission bound. Stream rather than
        # materializing its history, and never invoke any legacy verification.
        if self.path.stat().st_size > MAX_LEDGER_BYTES:
            raise EvidenceError('offline ledger exceeds acceptance-harness bound')
        self.db.execute('BEGIN')
        try:
            state = self._state()
            previous, count, last_state_hash = '0' * 64, 0, None
            for row in self.db.execute('SELECT * FROM journal ORDER BY seq'):
                count += 1
                if count > MAX_EVENTS or row['seq'] != count:
                    raise EvidenceError('journal bound/sequence mismatch')
                event = json.loads(row['event_json'])
                receipt = json.loads(row['receipt_json'])
                chain = digest({'seq': count, 'event_hash': row['event_hash'], 'receipt': receipt,
                                'state_hash': row['state_hash'], 'previous_hash': previous})
                if digest(event) != row['event_hash'] or row['event_id'] != event['id'] or row['previous_hash'] != previous or row['chain_hash'] != chain:
                    raise EvidenceError('journal integrity mismatch')
                previous, last_state_hash = chain, row['state_hash']
            if state['sequence'] != count or state['head_hash'] != previous or (count and state_hash(state) != last_state_hash):
                raise EvidenceError('state/journal reconciliation mismatch')
            self._reconcile(state)
            return summary(state)
        finally:
            self.db.execute('ROLLBACK')

    @staticmethod
    def _reconcile(state):
        basis = sum(p['remaining_basis_nano'] for p in state['positions'].values())
        if state['cash_nano'] < 0 or basis < 0:
            raise EvidenceError('negative shared capital')
        if state['cash_nano'] + basis != state['initial_cash_nano'] + state['realized_pnl_nano']:
            raise EvidenceError('cash/cost-basis/realized-PnL do not reconcile')
        if any(p['remaining_raw'] < 0 or p['remaining_basis_nano'] < 0 for p in state['positions'].values()):
            raise EvidenceError('negative position state')

    def apply(self, event: dict, fault_hook: Callable[[str], None] | None = None) -> dict:
        body = canonical(event)
        if len(body.encode()) > MAX_EVENT_BYTES or event.get('mode') != 'synthetic_offline_paper':
            raise EvidenceError('explicit bounded synthetic event required')
        if not event.get('id') or event.get('sol_usd_nano') is None:
            raise EvidenceError('missing event identity or valuation')
        timestamp(event['decision_at'])
        positive_int(event['sol_usd_nano'])
        event_hash = digest(event)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            prior = self.db.execute('SELECT event_hash,receipt_json FROM journal WHERE event_id=?', (event['id'],)).fetchone()
            if prior is not None:
                if prior['event_hash'] != event_hash:
                    raise EvidenceError('same event id with different evidence')
                self.db.execute('ROLLBACK')
                return json.loads(prior['receipt_json'])
            state = self._state()
            if state['sequence'] >= MAX_EVENTS:
                raise EvidenceError('acceptance-harness event budget reached')
            if state['clock'] and timestamp(event['decision_at']) < timestamp(state['clock']):
                raise EvidenceError('out-of-order decision rejected')
            try:
                outcome = decide(event, state)
            except EvidenceError as exc:
                outcome = {'action': 'reject', 'reason': str(exc), 'scope_or_evidence_rejection': True}
            for key in ('observations', 'wallet_episodes', 'entity_links'):
                if key in outcome:
                    state[key] = outcome.pop(key)
            action = outcome['action']
            token = event['observation']['token_mint']
            now = event['decision_at']
            if action == 'entry':
                cost = money(outcome['cost_lamports'], event['sol_usd_nano'], debit=True)
                if cost > state['cash_nano']:
                    outcome.update(action='reject', reason='shared_cash_including_fees_unavailable')
                else:
                    p = {'entry_id': event['id'], 'token': token, 'wallet': event['observation']['wallet'],
                         'lane': outcome['lane'], 'fraction': outcome['fraction'], 'opened_at': now,
                         'total_raw': outcome['buy']['out_raw'], 'remaining_raw': outcome['buy']['out_raw'],
                         'remaining_basis_nano': cost, 'initial_basis_nano': cost, 'cost_lamports': outcome['cost_lamports'],
                         'sol_usd_nano': event['sol_usd_nano'], 'stage': 0, 'realized_pnl_nano': 0,
                         'opportunity': outcome['opportunity'], 'entry_price_sol': outcome['entry_price_sol'],
                         'mark_nano': money(outcome['mark']['out_raw'] - outcome['mark']['fee_lamports'], event['sol_usd_nano']),
                         'signal_event': {
                             'release_commit': BASE_RELEASE, 'source_signature': event['id'], 'token_mint': token,
                             'wallet': event['observation']['wallet'], 'lane': outcome['lane'], 'venue': 'PUMP_FUN',
                             'lifecycle': outcome['lifecycle'], 'regime': outcome['regime'],
                             'context_key': outcome['profiles'][outcome['lane']]['context_key'], 'observed_at': now,
                             'priority_score': outcome['profiles'][outcome['lane']]['v52_authority']['portfolio_priority_score'],
                             'target_fraction': outcome['profiles'][outcome['lane']]['v52_authority']['target_fraction'],
                             'position_fraction': outcome['fraction'], 'decision': 'paper_enter_v52_profit_confidence',
                             'reason': 'extracted_exact_v52_selector', 'paper_only': 1, 'live_money_authority': 0}}
                    state['cash_nano'] -= cost
                    state['positions'][event['id']] = p
                    outcome['reserved_cost_nano'] = cost
            elif action == 'hold' and 'mark' in outcome:
                p = state['positions'][outcome['position_id']]
                p['mark_nano'] = money(outcome['mark']['out_raw'] - outcome['mark']['fee_lamports'], event['sol_usd_nano'])
            elif action == 'exit':
                p = state['positions'][outcome['position_id']]
                quantity = outcome['plan']['token_raw']
                if not 0 < quantity <= p['remaining_raw']:
                    raise EvidenceError('exit quantity exceeds actual position')
                # Allocate integer basis; the last fill consumes every remainder.
                basis = p['remaining_basis_nano'] if quantity == p['remaining_raw'] else p['remaining_basis_nano'] * quantity // p['remaining_raw']
                proceeds = money(outcome['quote']['out_raw'] - outcome['quote']['fee_lamports'], event['sol_usd_nano'])
                pnl = proceeds - basis
                p['remaining_raw'] -= quantity
                p['remaining_basis_nano'] -= basis
                p['realized_pnl_nano'] += pnl
                p['stage'] = outcome['plan']['stage']
                p['mark_nano'] = None if p['remaining_raw'] else 0
                state['cash_nano'] += proceeds
                state['realized_pnl_nano'] += pnl
                outcome.update(proceeds_nano=proceeds, allocated_basis_nano=basis, realized_pnl_nano=pnl)
                if not p['remaining_raw']:
                    settlement = {'position_id': p['entry_id'], 'exit_event_id': event['id'], 'settled_at': now,
                                  'realized_pnl_nano': p['realized_pnl_nano'], 'initial_basis_nano': p['initial_basis_nano'],
                                  'remaining_raw': 0, 'capital_released': True, 'paper_only': True}
                    state['settlements'][p['entry_id']] = settlement
                    outcome['settlement'] = settlement
            state['clock'] = now
            state['sequence'] += 1
            self._reconcile(state)
            receipt = {'event_id': event['id'], 'decision_at': now, 'decision': outcome,
                       'portfolio': summary(state), 'artifact_identity': self.identity,
                       'synthetic_evidence_only': True, 'production_certification': False}
            # The returned acknowledgement is byte-semantically identical to its
            # persisted replay (upstream economic profiles can contain tuples).
            receipt = json.loads(canonical(receipt))
            new_state_hash = state_hash(state)
            chain = digest({'seq': state['sequence'], 'event_hash': event_hash, 'receipt': receipt,
                            'state_hash': new_state_hash, 'previous_hash': state['head_hash']})
            self.db.execute('INSERT INTO journal VALUES (?,?,?,?,?,?,?,?)',
                            (state['sequence'], event['id'], body, event_hash, canonical(receipt), new_state_hash, state['head_hash'], chain))
            state['head_hash'] = chain
            self.db.execute('UPDATE current_state SET payload=?,digest=? WHERE id=1', (canonical(state), new_state_hash))
            page_count = self.db.execute('PRAGMA page_count').fetchone()[0]
            page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
            if page_count * page_size > MAX_LEDGER_BYTES:
                raise EvidenceError('offline ledger growth exceeds acceptance-harness bound')
            if fault_hook:
                fault_hook('before_commit')
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            raise
        if fault_hook:
            fault_hook('after_commit')
        return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True, type=Path)
    parser.add_argument('--init', metavar='NEW_EXPERIMENT_ID')
    parser.add_argument('--event', type=Path)
    args = parser.parse_args()
    runtime = Runtime.create(args.db, args.init) if args.init else Runtime(args.db)
    try:
        if args.event:
            print(canonical(runtime.apply(json.loads(args.event.read_text()))))
        else:
            print(canonical(runtime.verify()))
    finally:
        runtime.close()
