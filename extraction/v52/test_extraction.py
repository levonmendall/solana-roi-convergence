"""Acceptance tests include independent interpreters and forced process death.

This suite uses synthetic observations/quotes only. Its test-only oracle imports
original functions in a separate subprocess; the detached runtime never does.
"""
from __future__ import annotations

import ast
import copy
import importlib.util
import json
import os
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


@pytest.fixture(scope='session')
def dist(tmp_path_factory):
    path = tmp_path_factory.mktemp('build-parent') / 'detached'
    result = subprocess.run([sys.executable, str(HERE / 'build.py'), '--output', str(path)],
                            text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    sys.path.insert(0, str(path))
    return path


def run_python(dist, source, *args, oracle=False, timeout=30):
    env = {k: v for k, v in os.environ.items() if not k.startswith(('SOLANA_ROI_', 'RENDER_', 'ROBINHOOD_'))}
    env['PYTHONPATH'] = str(dist) + (os.pathsep + str(REPO / 'src') if oracle else '')
    flags = ['-B'] if oracle else ['-B', '-S']
    return subprocess.run([sys.executable, *flags, '-c', source, *map(str, args)],
                          env=env, cwd=str(dist), text=True, capture_output=True, timeout=timeout)


def new_runtime(dist, tmp_path):
    from roi_extracted.runtime import Runtime
    return Runtime.create(tmp_path / 'paper.sqlite3', 'test-experiment')


def test_complete_lifecycle_in_fresh_interpreters(dist, tmp_path):
    result = run_python(dist, 'from pathlib import Path; from roi_extracted.demo import run; import sys,json; print(json.dumps(run(Path(sys.argv[1]))))', tmp_path / 'demo')
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report['lifecycle_passed'] and report['fresh_interpreter_boundaries'] == 11
    final = report['final']
    assert final['cash_nano'] == 500_124_500_000
    assert final['realized_pnl_nano'] == 124_500_000
    assert final['open_positions'] == final['reserved_basis_nano'] == 0
    assert final['settlements'] == 1
    entry = report['steps'][1]
    assert entry['portfolio']['cash_nano'] == 497_499_500_000
    assert entry['decision']['fraction'] == .005  # Original completion's $2.50 minimum, NOT .25% basic starter.
    assert entry['decision']['wallet_score']['paired_forward_episodes'] == 0
    assert not report['profitability_evidence']


def test_detached_runtime_has_no_legacy_import_or_external_io(dist, tmp_path):
    source = r'''
import importlib.abc, sys, socket, json
from pathlib import Path
class NoLegacy(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'solana_roi' or fullname.startswith('solana_roi.') or fullname.split('.')[0] in {'httpx','requests','fastapi','uvicorn','websockets'}:
            raise AssertionError('forbidden import: '+fullname)
sys.meta_path.insert(0, NoLegacy())
def forbid(*args, **kwargs): raise AssertionError('network attempted')
socket.socket = forbid
socket.create_connection = forbid
from roi_extracted.runtime import Runtime
from roi_extracted.demo import tape
r=Runtime.create(Path(sys.argv[1]), 'offline')
for e in tape(): r.apply(e)
print(json.dumps({'state':r.verify(),'modules':[n for n in sys.modules if n.startswith('solana_roi')]}))
r.close()
'''
    result = run_python(dist, source, tmp_path / 'offline.sqlite')
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload['modules'] == [] and payload['state']['settlements'] == 1


@pytest.mark.parametrize('point,expected_sequence', [('before_commit', 0), ('after_commit', 1)])
@pytest.mark.parametrize('operation', ['entry', 'exit'])
def test_sigkill_transaction_and_lost_ack_recovery(dist, tmp_path, point, expected_sequence, operation):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import Runtime
    buy, hold, sell, _ = tape()
    r = new_runtime(dist, tmp_path)
    baseline = 0
    if operation == 'exit':
        r.apply(buy)
        baseline = 1
    r.close()
    event = sell if operation == 'exit' else buy
    event_path = tmp_path / 'event.json'; event_path.write_text(json.dumps(event))
    code = r'''
import os,signal,sys,json
from pathlib import Path
from roi_extracted.runtime import Runtime
r=Runtime(Path(sys.argv[1]))
def crash(phase):
    if phase==sys.argv[3]: os.kill(os.getpid(),signal.SIGKILL)
r.apply(json.loads(Path(sys.argv[2]).read_text()),fault_hook=crash)
'''
    result = run_python(dist, code, tmp_path / 'paper.sqlite3', event_path, point)
    assert result.returncode == -signal.SIGKILL, result.stderr
    r = Runtime(tmp_path / 'paper.sqlite3')
    assert r.verify()['sequence'] == baseline + expected_sequence
    receipt = r.apply(event)
    assert r.apply(event) == receipt
    state = r.verify()
    assert state['sequence'] == baseline + 1
    if operation == 'exit':
        assert state['settlements'] == 1 and state['cash_nano'] == 500_124_500_000
    else:
        assert state['open_positions'] == 1 and state['cash_nano'] == 497_499_500_000
    r.close()


def test_two_processes_cannot_duplicate_capital(dist, tmp_path):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import Runtime
    r = new_runtime(dist, tmp_path); r.close()
    file = tmp_path / 'event.json'; file.write_text(json.dumps(tape()[0]))
    env = dict(os.environ, PYTHONPATH=str(dist))
    cmd = [sys.executable, '-B', '-S', '-m', 'roi_extracted.runtime', '--db', str(tmp_path / 'paper.sqlite3'), '--event', str(file)]
    children = [subprocess.Popen(cmd, env=env, cwd=dist, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
    responses = []
    for child in children:
        out, err = child.communicate(timeout=30)
        assert child.returncode == 0, err
        responses.append(json.loads(out))
    assert responses[0] == responses[1]
    r = Runtime(tmp_path / 'paper.sqlite3')
    assert r.verify()['sequence'] == 1 and r.verify()['cash_nano'] == 497_499_500_000
    r.close()


@pytest.mark.parametrize('failure', ['hard_stop', 'missing_exit', 'wrong_amount', 'future_quote', 'first_slot', 'stale_latency', 'copyability', 'risk_missing', 'unsupported_surface', 'high_chase'])
def test_legitimate_rejection_never_spends_capital(dist, tmp_path, failure):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import USD
    r = new_runtime(dist, tmp_path)
    event = tape()[0]
    if failure == 'hard_stop': event['risk']['hard_flags'] = ['liquidity_unexitable']
    if failure == 'missing_exit': event['quotes'] = [q for q in event['quotes'] if q['kind'] != 'exit']
    if failure == 'wrong_amount': event['quotes'][0]['input_raw'] += 1
    if failure == 'future_quote': event['quotes'][0]['available_at'] = '2026-09-01T12:00:01+00:00'
    if failure == 'first_slot': event['quotes'][0]['slot'] = event['observation']['slot']
    if failure == 'stale_latency': event['observation']['observed_at'] = '2026-09-01T11:59:30+00:00'
    if failure == 'copyability': event['observation']['copyable'] = 0
    if failure == 'risk_missing': event['risk']['complete'] = False
    if failure == 'unsupported_surface': event['observation']['source'] = 'solana-direct:RAYDIUM:buy'
    if failure == 'high_chase': event['observation']['wallet_price_sol'] = .0001
    result = r.apply(event)
    assert result['decision']['action'] == 'reject'
    assert r.verify()['cash_nano'] == 500 * USD and r.verify()['open_positions'] == 0
    r.close()


def wallet_episodes(context, n, *, future=False):
    return [{'wallet':'fixture-leader-wallet','context_key':context,'candidate_id':f'paired-{i}',
             'observed_at':'2026-08-31T12:00:00+00:00',
             'available_at':'2026-09-02T12:00:00+00:00' if future else '2026-08-31T12:00:01+00:00',
             'wallet_policy_return':.45, 'matched_control_return':.1,
             'executable_mfe':.6, 'executable_mae':.1, 'copyable':True,
             'prospective':True,'paired_same_candidate_stream':True} for i in range(n)]


def test_real_wallet_score_and_future_evidence_exclusion(dist, tmp_path):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import Runtime
    from roi_extracted.kernel import decide
    r = new_runtime(dist, tmp_path)
    event = tape()[0]
    base = decide(event, r._state())
    context = base['profiles'][base['lane']]['context_key']
    event['wallet_episodes'] = wallet_episodes(context, 30)
    scored = decide(event, r._state())
    assert scored['wallet_score']['paired_forward_episodes'] == 30
    assert scored['wallet_score']['eligible_for_strategy_influence']
    auth = scored['profiles'][scored['lane']]['v52_authority']
    assert auth['wallet_target_utilization_multiplier'] > 1
    future = copy.deepcopy(event); future['wallet_episodes'] = wallet_episodes(context, 30, future=True)
    unseen = decide(future, r._state())
    assert unseen['wallet_score'] == base['wallet_score'] and unseen['evidence_excluded_future'] == 30
    assert unseen['profiles'] == base['profiles']
    r.apply(event); r.close()
    reopened = Runtime(tmp_path / 'paper.sqlite3')
    assert len(reopened._state()['wallet_episodes']) == 30
    assert reopened.verify()['open_positions'] == 1
    reopened.close()


def test_entity_deduplication_uses_real_graph(dist, tmp_path):
    from roi_extracted.demo import tape, row
    from roi_extracted.kernel import decide
    r = new_runtime(dist, tmp_path)
    event = tape()[0]
    event['history'] = [row('prior-a','buy',-4,wallet='side-a'), row('prior-b','buy',-3,wallet='side-b')]
    # Decision-only test: evidence and selector, no invented fills for resized orders.
    from roi_extracted.kernel import evaluation, risk
    with evaluation(event, r._state()) as e:
        pre = risk._v5_pre_context(e.adapter,e.row,hard=[],soft=[],early_exit=0)
        assert pre['independent_count'] == 2
    event['entity_links'] = [{'left':'side-a','right':'side-b','observed_at':'2026-08-31T12:00:00+00:00','available_at':'2026-08-31T12:00:01+00:00'}]
    with evaluation(event, r._state()) as e:
        pre = risk._v5_pre_context(e.adapter,e.row,hard=[],soft=[],early_exit=0)
        assert pre['independent_count'] == 1
    r.close()


@pytest.mark.parametrize('chase,latency', [(0,.2),(.04,2),(.4,20),(.40001,2),(.8,2),(.80001,2),(0,20.01)])
def test_selector_matches_original_pinned_chain(dist, chase, latency):
    code = r'''
import json,sys
from roi_extracted import kernel as k
from roi_extracted.runtime import initial_state
from roi_extracted.demo import tape
from solana_roi import v52_authoritative_strategy as a
from solana_roi import v52_adaptive_continuation_refinement as b
from solana_roi import v52_profit_confidence_completion as c
from solana_roi import v52_profit_confidence_finalization as d
# Explicit dependencies match the original installation order. No decision function
# or economic result is stubbed; the only replacement is time/evidence ports.
a.position_policy=b._contextual_position_policy
b._BASE_SOLANA_CHOOSE=a._v52_solana_choose
b._WALLET_ALPHA=k.WalletPort()
c._BASE_SOLANA_CHOOSE=b._solana_choose
d._BASE_SOLANA_CHOOSE=c._completed_solana_choose
c.datetime=k.AtDecisionClock
a.datetime=k.AtDecisionClock
with k.evaluation(tape()[0],initial_state('oracle','unused')) as e:
    pre=k.risk._v5_pre_context(e.adapter,e.row,hard=[],soft=[],early_exit=0)
    actual=k.finalization._final_solana_choose(e.adapter,pre,chase=float(sys.argv[1]),latency=float(sys.argv[2]))
    expected=d._final_solana_choose(e.adapter,pre,chase=float(sys.argv[1]),latency=float(sys.argv[2]))
    assert actual==expected, (actual,expected)
    print(json.dumps({'matched':True,'lane':actual[0],'fraction':actual[1]}))
'''
    result = run_python(dist, code, chase, latency, oracle=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['matched']


@pytest.mark.parametrize('seller', ['fixture-leader-wallet', 'fixture-creator-wallet', 'unrelated-holder'])
def test_exit_matches_original_execution_boundary(dist, tmp_path, seller):
    code = r'''
import asyncio,json,sys
from roi_extracted import kernel as k
from roi_extracted.demo import tape
from roi_extracted.runtime import initial_state
from solana_roi import v52_profit_confidence_completion as original
state=initial_state('exit-oracle','unused')
buy, hold, sell, _=tape()
state['observations']={buy['id']:buy['observation']}
event=hold
event['observation']['wallet']=sys.argv[1]
item={'source_signature':'entry-1','token_mint':event['observation']['token_mint'], 'trigger_wallet':'fixture-leader-wallet','lane':'elite_wallet_continuation','opportunity_json':'{}'}
lifecycle={'source_signature':'entry-1','remaining_token_raw':25000000,'total_token_raw':25000000,'derisk_stage':0,'entry_observed_at':buy['decision_at']}
seen=[]
async def no_op(*a,**kw): pass
async def intercept_quote(_self,_life,_row,qty,stage,reason):
    seen.append({'token_raw':qty,'stage':stage,'reason':reason});return None
with k.evaluation(event,state) as e:
    # Stub only persistence/read-projection/quote ports at the original method.
    # The original exit features/model/high-urgency/stage arithmetic remain real.
    original._BASE_SOLANA_SELL=no_op
    original._resolve_counterfactuals=no_op
    original._open_selected_positions=lambda *_:[item]
    original._effective_exit_lane=lambda *_:'elite_wallet_continuation'
    original._ensure_lifecycle=lambda *_:lifecycle
    original._record_exit_signal=lambda *_:None
    original._exact_stage_fill=intercept_quote
    actual=k.plan_exit(e.adapter,item,lifecycle,e.row,'elite_wallet_continuation')
    asyncio.run(original._completed_solana_sell(e.adapter,e.row))
    expected=seen[0] if seen else None
    assert (None if actual is None else {x:actual[x] for x in expected})==expected,(actual,expected)
print(json.dumps({'matched':True}))
'''
    result = run_python(dist, code, seller, oracle=True)
    assert result.returncode == 0, result.stderr


def test_old_database_is_never_modified(dist, tmp_path):
    from roi_extracted.runtime import Runtime, EvidenceError
    old = tmp_path / 'old-production.sqlite'
    db = sqlite3.connect(old); db.execute('CREATE TABLE protected_data (value TEXT)');db.execute("INSERT INTO protected_data VALUES ('keep')");db.commit();db.close()
    original = old.read_bytes()
    with pytest.raises(EvidenceError): Runtime(old)
    assert old.read_bytes() == original
    with pytest.raises(FileExistsError): Runtime.create(old, 'do-not-overwrite')
    assert old.read_bytes() == original
    with pytest.raises(EvidenceError): Runtime(Path('/var/data/solana-roi-active.sqlite3'))


def test_journal_idempotency_and_integrity(dist, tmp_path):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import Runtime, EvidenceError
    r = new_runtime(dist, tmp_path); event=tape()[0]
    receipt=r.apply(event)
    altered=copy.deepcopy(event);altered['quotes'][0]['fee_lamports']+=1
    with pytest.raises(EvidenceError):r.apply(altered)
    assert r.apply(event)==receipt
    with pytest.raises(sqlite3.IntegrityError):r.db.execute('DELETE FROM journal')
    r.db.execute("UPDATE current_state SET payload='{}'")
    with pytest.raises((EvidenceError,KeyError)):r.verify()
    r.close()


def test_artifact_tamper_fails_before_opening_ledger(dist, tmp_path):
    import shutil
    copy_root=tmp_path/'tampered';shutil.copytree(dist,copy_root)
    f=copy_root/'strategy_v52_authority.json';s=json.loads(f.read_text());s['paper_only']=False;f.write_text(json.dumps(s))
    result=run_python(copy_root, 'from pathlib import Path; from roi_extracted.runtime import Runtime; import sys; Runtime.create(Path(sys.argv[1]),"tampered")',tmp_path/'must-not-exist.sqlite')
    assert result.returncode != 0 and not (tmp_path/'must-not-exist.sqlite').exists()


def test_no_scale_in_or_policy_evolution_silently_enabled(dist, tmp_path):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import Runtime
    from roi_extracted.kernel import policy
    r=new_runtime(dist,tmp_path);r.apply(tape()[0])
    event=tape()[0];event['id']='entry-2';event['observation']['signature']='entry-2'
    assert r.apply(event)['decision']['reason']=='scope_scale_or_reentry_not_implemented'
    policy.canonical_strategy_evolution().current.config['absolute_latency_ceiling_seconds']=99
    try:
        event['id']='entry-3';event['observation']['signature']='entry-3'
        result=r.apply(event)
        assert result['decision']['reason']=='scope_promoted_policy_requires_explicit_import_and_validation'
    finally:policy._CANONICAL_EVOLUTION=None
    assert r.verify()['open_positions']==1
    r.close()


def test_verbatim_source_and_config_extraction(dist):
    import hashlib
    spec=importlib.util.spec_from_file_location('extraction_builder',HERE/'build.py')
    builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
    manifest=json.loads((dist/'extraction-manifest.json').read_text())
    for name in builder.WHOLE:
        assert (dist/'roi_extracted/domain'/f'{name}.py').read_bytes()==(REPO/'src/solana_roi'/f'{name}.py').read_bytes()
    for name in builder.POLICIES:
        assert (dist/name).read_bytes()==(REPO/name).read_bytes()
    def definitions(text):
        result={}
        for n in ast.parse(text).body:
            if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
                result[n.name]=ast.dump(n,include_attributes=False)
            if isinstance(n,(ast.Assign,ast.AnnAssign)):
                for target in n.targets if isinstance(n,ast.Assign) else [n.target]:
                    if isinstance(target,ast.Name): result[target.id]=ast.dump(n,include_attributes=False)
        return result
    for name in builder.SLICES:
        original=definitions((REPO/'src/solana_roi'/f'{name}.py').read_text())
        extracted=definitions((dist/'roi_extracted/domain'/f'{name}.py').read_text())
        for key in manifest['extracted'][name]['definitions']:
            assert extracted[key]==original[key],(name,key)
    for dest,(src,cls,methods) in builder.METHODS.items():
        a=next(n for n in ast.parse((REPO/'src/solana_roi'/f'{src}.py').read_text()).body if isinstance(n,ast.ClassDef) and n.name==cls)
        b=next(n for n in ast.parse((dist/'roi_extracted/domain'/f'{dest}.py').read_text()).body if isinstance(n,ast.ClassDef))
        for method in methods:
            old=next(n for n in a.body if isinstance(n,ast.FunctionDef) and n.name==method)
            new=next(n for n in b.body if isinstance(n,ast.FunctionDef) and n.name==method)
            assert ast.dump(old,include_attributes=False)==ast.dump(new,include_attributes=False)
    for rel,expected in manifest['sources'].items():
        assert hashlib.sha256((REPO/rel).read_bytes()).hexdigest()==expected


def test_independent_build_is_reproducible_and_nonoverwriting(dist,tmp_path):
    output=tmp_path/'repeat'
    command=[sys.executable,str(HERE/'build.py'),'--output',str(output)]
    result=subprocess.run(command,text=True,capture_output=True,timeout=30)
    assert result.returncode==0,result.stderr
    assert (output/'extraction-manifest.json').read_bytes()==(dist/'extraction-manifest.json').read_bytes()
    result=subprocess.run(command,text=True,capture_output=True,timeout=30)
    assert result.returncode!=0 and 'never overwrite' in result.stderr


def test_out_of_order_oversize_and_evidence_bounds_are_not_silent_truncation(dist,tmp_path):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import EvidenceError,MAX_EVENT_BYTES
    r=new_runtime(dist,tmp_path)
    oversized=tape()[0];oversized['padding']='x'*(MAX_EVENT_BYTES+1)
    with pytest.raises(EvidenceError):r.apply(oversized)
    assert r.verify()['sequence']==0
    e=tape()[0];e['history']=[dict(e['observation'],signature=f'h{i}') for i in range(257)]
    result=r.apply(e)
    assert result['decision']['reason']=='scope_evidence_budget_exceeded_no_truncation'
    assert r.verify()['cash_nano']==500_000_000_000
    older=tape()[0];older['id']='older';older['observation']['signature']='older';older['decision_at']='2026-09-01T11:00:00+00:00'
    with pytest.raises(EvidenceError):r.apply(older)
    r.close()


def test_journal_corruption_detected_across_process_restart(dist,tmp_path):
    from roi_extracted.demo import tape
    from roi_extracted.runtime import Runtime,EvidenceError
    r=new_runtime(dist,tmp_path);r.apply(tape()[0]);r.close()
    db=sqlite3.connect(tmp_path/'paper.sqlite3')
    db.execute('DROP TRIGGER journal_no_update')
    db.execute("UPDATE journal SET event_hash='tampered'");db.commit();db.close()
    with pytest.raises(EvidenceError,match='journal integrity'):
        Runtime(tmp_path/'paper.sqlite3')


def test_resource_envelope_of_detached_lifecycle(dist,tmp_path):
    code=r'''
import json,resource,sys
from pathlib import Path
resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
from roi_extracted.runtime import Runtime
from roi_extracted.demo import tape
r=Runtime.create(Path(sys.argv[1]),'resource-fixture')
for event in tape(): r.apply(event)
state=r.verify();r.close()
print(json.dumps({'maxrss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'state':state}))
'''
    result=run_python(dist,code,tmp_path/'bounded.sqlite')
    assert result.returncode==0,result.stderr
    payload=json.loads(result.stdout)
    assert payload['state']['settlements']==1
    assert payload['maxrss_kib'] < 256*1024
    assert (tmp_path/'bounded.sqlite').stat().st_size < 1024*1024
