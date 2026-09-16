"""A synthetic vertical slice, NOT a profitability backtest or a live provider."""
from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .kernel import USD

T0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
TOKEN = 'fixture-pump-token'
WALLET = 'fixture-leader-wallet'


def row(signature, side, seconds, *, wallet=WALLET):
    at = T0 + timedelta(seconds=seconds)
    return {'signature': signature, 'wallet': wallet, 'token_mint': TOKEN, 'side': side,
            'slot': 100 + seconds, 'token_amount': 25.0,
            'observed_at': (at - timedelta(milliseconds=200)).isoformat(),
            'received_at': at.isoformat(), 'wallet_price_sol': 0.001,
            'copyable_price_sol': 0.001, 'chase_fraction': 0.0, 'copyable': 1,
            'observation_lag_ms': 200.0, 'risk_complete': 1,
            'manipulation_flag': 0, 'side_wallet_flag': 0, 'source': 'solana-direct:PUMP_FUN:' + side}


def quote(kind, amount, out, seconds, *, fee=5000):
    at = (T0 + timedelta(seconds=seconds)).isoformat()
    return {'quote_id': f'{kind}-{seconds}-{amount}', 'source': 'synthetic-exact-quote-fixture',
            'kind': kind, 'token_mint': TOKEN, 'input_raw': amount, 'out_raw': out,
            'fee_lamports': fee, 'slot': 101 + seconds, 'observed_at': at, 'available_at': at}


def event(signature, side, seconds, *, wallet=WALLET):
    at = (T0 + timedelta(seconds=seconds)).isoformat()
    return {'mode': 'synthetic_offline_paper', 'id': signature, 'decision_at': at,
            'observation': row(signature, side, seconds, wallet=wallet),
            'sol_usd_nano': 100 * USD, 'token_decimals': 6,
            'risk': {'token_mint': TOKEN, 'complete': True, 'observed_at': at, 'available_at': at,
                     'hard_flags': [], 'soft_flags': [], 'early_exit_fraction': 0.0,
                     'deployer_wallet': 'fixture-creator-wallet'},
            'history': [], 'wallet_episodes': [], 'entity_links': [], 'quotes': []}


def tape():
    buy = event('entry-1', 'buy', 0)
    buy['quotes'] = [quote('entry', 25_000_000, 25_000_000, 0),
                     quote('exit', 25_000_000, 24_905_000, 0),
                     quote('depth', 50_000_000, 49_605_000, 0)]
    hold = event('hold-1', 'sell', 5, wallet='unrelated-holder')
    hold['quotes'] = [quote('mark', 25_000_000, 25_505_000, 5)]
    sell = event('exit-1', 'sell', 10)
    sell['quotes'] = [quote('exit', 25_000_000, 26_255_000, 10)]
    rejected = event('reject-1', 'buy', 20)
    rejected['observation']['token_mint'] = 'fixture-blocked-token'
    rejected['risk']['token_mint'] = 'fixture-blocked-token'
    rejected['risk']['hard_flags'] = ['liquidity_unexitable']
    return buy, hold, sell, rejected


def run(directory: Path):
    directory.mkdir(parents=True, exist_ok=False)
    db = directory / 'paper.sqlite3'
    outcomes = []
    def process(*args):
        completed = subprocess.run([sys.executable, '-B', '-S', '-m', 'roi_extracted.runtime', '--db', str(db), *args],
                                   text=True, capture_output=True, timeout=30)
        if completed.returncode:
            raise RuntimeError(completed.stderr)
        return json.loads(completed.stdout)
    outcomes.append(process('--init', 'synthetic-restart-lifecycle-1'))
    for event_data in tape():
        path = directory / (event_data['id'] + '.json')
        path.write_text(json.dumps(event_data))
        outcomes.append(process('--event', str(path)))
        outcomes.append(process())  # Separate interpreter, durable recovery.
    outcomes.append(process('--event', str(directory / 'exit-1.json')))
    final = process()
    assert final['open_positions'] == 0 and final['settlements'] == 1
    assert final['cash_nano'] == 500_124_500_000
    assert final['realized_pnl_nano'] == 124_500_000 and final['reserved_basis_nano'] == 0
    assert outcomes[1]['decision']['action'] == 'entry'
    assert outcomes[3]['decision']['action'] == 'hold'
    assert outcomes[5]['decision']['action'] == 'exit'
    assert outcomes[7]['decision']['action'] == 'reject'
    report = {'mode': 'synthetic_offline_paper', 'profitability_evidence': False,
              'production_changed': False, 'lifecycle_passed': True, 'final': final,
              'fresh_interpreter_boundaries': 11, 'steps': outcomes}
    (directory / 'acceptance.json').write_text(json.dumps(report, indent=2, sort_keys=True))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.directory.resolve()), indent=2, sort_keys=True))
