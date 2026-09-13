"""Disposable-only isolation gate. No application import before host launch marker."""
import json, os, time
from pathlib import Path

CGROUP = Path('/sys/fs/cgroup')
CONTROL = Path('/control')
EXPECTED = 2147483648

def verify(cgroup=CGROUP, state=Path('/state')):
    if not (cgroup/'cgroup.controllers').is_file():
        raise RuntimeError('cgroup v2 is required')
    maximum = int((cgroup/'memory.max').read_text())
    members = sorted(set(int(p) for p in (cgroup/'cgroup.procs').read_text().split()))
    if maximum != EXPECTED:
        raise RuntimeError(f'Expected exclusive 2 GiB, got {maximum}')
    if members != [os.getpid()]:
        raise RuntimeError(f'Initial exclusive membership not proven: {members}')
    if any(p.name != '.' for p in cgroup.iterdir() if p.is_dir()):
        raise RuntimeError('Unexpected child cgroups before application launch')
    if list(state.iterdir()):
        raise RuntimeError('State must initially be empty; load only after isolation proof')
    return dict(pid=os.getpid(),memory_max=maximum,
        memory_current=int((cgroup/'memory.current').read_text()),
        memory_stat=(cgroup/'memory.stat').read_text(),members=members,
        proc_cgroup=Path('/proc/self/cgroup').read_text(),
        proc_status=Path('/proc/self/status').read_text(),
        initial_state_empty=True,application_imported=False)

if __name__ == '__main__':
    evidence=verify()
    CONTROL.mkdir(exist_ok=True)
    (CONTROL/'isolation.json').write_text(json.dumps(evidence,indent=2))
    print('ISOLATION_GATE_READY',flush=True)
    deadline=time.monotonic()+1800
    while not (CONTROL/'launch').exists():
        if time.monotonic()>deadline:raise RuntimeError('Fixture launch deadline exceeded')
        time.sleep(.25)
    if not Path(os.environ['SOLANA_ROI_DB_PATH']).is_file():
        raise RuntimeError('Representative disposable database missing')
    # Same production application entrypoint and normal worker startup sequence.
    os.execvp('uvicorn',['uvicorn','solana_roi.production:app','--host','0.0.0.0','--port','10000'])
