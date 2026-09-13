"""Validate operator-supplied SANITIZED fixtures; never export production state."""
import argparse, hashlib, json
from pathlib import Path
REQUIRED={'wallet','direct_solana','hydration','forward_evidence','events_checkpoints',
    'lifecycle_portfolio','certification_replication','shadow_price_tokens'}

def validate(root):
    root=root.resolve();manifest=json.loads((root/'manifest.json').read_text())
    assert manifest.get('sanitized') is True,'Sanitization proof required'
    assert manifest.get('snapshot_consistent') is True,'Consistent DB/WAL/SHM snapshot proof required'
    assert manifest.get('source_release')=='92c0c1620f78116e7ecbeade039e9aedaf3a51a9'
    assert REQUIRED<=set(manifest['dimensions']),'Missing represented state categories'
    for key in REQUIRED:
        row=manifest['dimensions'][key]
        assert row.get('tables') and 'production_count' in row and 'fixture_count' in row
        assert row.get('history_depth') and row.get('fidelity_gap') is not None
    assert manifest.get('providers'),'Explicit provider coverage/replay manifest required'
    for p in manifest['providers']:
        for key in ('name','transport','cadence','sizes','latency','timeouts','retries','failover','concurrency','cancellation','fidelity_gap'):
            assert key in p,f'Missing provider fidelity field: {key}'
    assert 'state/solana-roi.sqlite3' in manifest['files']
    for path in (root/'state').rglob('*'):
        assert not path.is_symlink(),'Symlinks are not accepted in disposable state'
        if path.is_file():assert str(path.relative_to(root)) in manifest['files'],'Unmanifested state file'
    for relative,sha in manifest['files'].items():
        path=(root/relative).resolve()
        assert path.is_relative_to(root) and path.is_file(),'Unsafe or missing fixture path'
        digest=hashlib.sha256()
        with path.open('rb') as f:
            for block in iter(lambda:f.read(1024*1024),b''):digest.update(block)
        assert digest.hexdigest()==sha,f'Fixture digest mismatch: {relative}'
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('fixtures',type=Path);a=p.parse_args()
    m=validate(a.fixtures);print(json.dumps({'valid_contract':True,'dimensions':sorted(m['dimensions']),
        'provider_count':len(m['providers']),'note':'Manifest validation does not establish production fidelity.'},indent=2))
