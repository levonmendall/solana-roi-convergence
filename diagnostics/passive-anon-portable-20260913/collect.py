"""Low-overhead collector run on the Linux host OUTSIDE the application cgroup.
Reads status/rollup/io and cached publication state; never reads full smaps or SQL.
Requires host proc/cgroup access. Does not start, stop, or reconfigure the container.
"""
import argparse, json, os, subprocess, time, urllib.request
from pathlib import Path

def fields(path):
    try:
        out={}
        for line in path.read_text().splitlines():
            parts=line.replace(':',' ').split()
            if len(parts)>1 and parts[1].isdigit():out[parts[0]]=int(parts[1])
        return out
    except OSError:return None

def scalar(path):
    try:return int(path.read_text().strip())
    except (OSError,ValueError):return None

def cgroup_for(pid):
    lines=Path(f'/proc/{pid}/cgroup').read_text().splitlines()
    name=next(line[3:] for line in lines if line.startswith('0::'))
    return (Path('/sys/fs/cgroup')/name.lstrip('/')).resolve()

def capture(cgroup, init_pid):
    groups=[cgroup,*[p.parent for p in cgroup.rglob('cgroup.procs') if p.parent!=cgroup]]
    members=set()
    for group in groups:
        members.update(map(int,(group/'cgroup.procs').read_text().split()))
    processes=[]
    for pid in sorted(members):
        proc=Path(f'/proc/{pid}')
        try:
            status=fields(proc/'status');rollup=fields(proc/'smaps_rollup')
            processes.append(dict(pid=pid,status_kib=status,smaps_rollup_kib=rollup,
                io_bytes=fields(proc/'io'),stat=(proc/'stat').read_text(),
                comm=(proc/'comm').read_text().strip(),
                fd_count=len(list((proc/'fd').iterdir()))))
        except OSError as exc:processes.append(dict(pid=pid,error=type(exc).__name__))
    root=Path(f'/proc/{init_pid}/root/state')
    sizes={}
    for name in ['solana-roi.sqlite3','solana-roi.sqlite3-wal','solana-roi.sqlite3-shm']:
        try:sizes[name]=(root/name).stat().st_size
        except OSError:sizes[name]=None
    return dict(timestamp=time.time(),host_uptime_seconds=float(Path('/proc/uptime').read_text().split()[0]),
        clock_ticks=os.sysconf('SC_CLK_TCK'),memory_current=scalar(cgroup/'memory.current'),
        memory_max=scalar(cgroup/'memory.max'),memory_stat=fields(cgroup/'memory.stat'),
        memory_events=fields(cgroup/'memory.events'),pids_current=scalar(cgroup/'pids.current'),
        members=sorted(members),processes=processes,state_sizes=sizes)

def main():
    p=argparse.ArgumentParser();p.add_argument('container');p.add_argument('--output',type=Path,required=True)
    p.add_argument('--seconds',type=int,default=900);p.add_argument('--interval',type=int,default=60)
    p.add_argument('--publication-url',default='http://127.0.0.1:18768/v1/strategy/forward-certification/cache')
    a=p.parse_args()
    if a.interval<30:raise SystemExit('Use a coarse cadence of at least 30 seconds')
    inspection=json.loads(subprocess.check_output(['docker','inspect',a.container]))[0]
    if inspection['HostConfig']['Memory']!=2147483648:raise SystemExit('Docker memory maximum mismatch')
    pid=int(inspection['State']['Pid']);cg=cgroup_for(pid)
    if scalar(cg/'memory.max')!=2147483648:raise SystemExit('Kernel memory maximum mismatch')
    if cgroup_for(os.getpid())==cg:raise SystemExit('Collector must be outside application cgroup')
    start=time.monotonic()
    with a.output.open('x') as f:
        while True:
            try:row=capture(cg,pid)
            except OSError as exc:
                f.write(json.dumps({'timestamp':time.time(),'container_disappeared':type(exc).__name__})+'\n');break
            try:
                with urllib.request.urlopen(a.publication_url,timeout=5) as r:row['publication']=json.load(r)
            except Exception as exc:row['publication_unavailable']=type(exc).__name__
            f.write(json.dumps(row)+'\n');f.flush()
            if time.monotonic()-start>=a.seconds:break
            time.sleep(min(a.interval,max(0,a.seconds-(time.monotonic()-start))))

if __name__=='__main__':main()
