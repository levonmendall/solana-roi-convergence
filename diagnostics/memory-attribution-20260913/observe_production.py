import json,time,urllib.request
from pathlib import Path
out=Path('/workspace/scratch/46b2fc0f9965/production-observations.jsonl')
for i in range(8):
    r={'sample':i,'observed_at':time.time()}
    for name,path in [('composition','/v1/operations/production-composition'),('forward','/v1/strategy/forward-certification/cache')]:
        try:
            with urllib.request.urlopen('https://solana-roi-convergence.onrender.com'+path,timeout=15) as response:
                x=json.load(response)
                r[name]=x if name=='forward' else {'memory':x.get('durable_bootstrap_memory',{}).get('cgroup_memory'),
                    'lifecycle':x.get('paper_execution_lifecycle'), 'rpc':x.get('rpc_task_ownership')}
        except Exception as e:r[name]={'error':str(e)}
    with out.open('a') as f:f.write(json.dumps(r)+'\n')
    if i<7:time.sleep(20)
