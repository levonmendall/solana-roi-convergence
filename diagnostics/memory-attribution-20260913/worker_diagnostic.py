"""Run real uvicorn production entrypoint with measurement hooks on disposable state.
No production files, credentials, workers, guards or authority settings are modified.
This is an activation/SQL locator, not by itself a causal proof.
"""
import asyncio, json, os, sqlite3, sys, threading, time, traceback
from pathlib import Path
from response_diagnostic import sample

ROOT=Path('/workspace/scratch/46b2fc0f9965')
os.environ.update(json.loads((ROOT/'composition-env.json').read_text()))
DB=Path(os.environ['SOLANA_ROI_DB_PATH'])
assert DB.resolve().is_relative_to((ROOT/'disposable').resolve())
LOG=(ROOT/'worker-events.jsonl').open('w',buffering=1)
lock=threading.RLock();stats={};pending={};closed=False
def emit(event,**kw):
    if closed:return
    with lock:LOG.write(json.dumps(dict(event=event,**kw),default=str)+'\n')
def resource(phase):return sample(phase,DB)
def caller():
    return [f'{f.filename}:{f.lineno}:{f.name}' for f in traceback.extract_stack(limit=14) if '/solana_roi/' in f.filename][-7:]
def measure(q,operation,fn,connection=None,params=()):
    before=resource('before');error=None
    try:return fn()
    except BaseException as e:error=type(e).__name__;raise
    finally:
        after=resource('after');elapsed=after['monotonic']-before['monotonic']
        key=q
        with lock:
            if key not in stats:
                plan=None
                # Read-only EXPLAIN after the measured operation, once per SQL shape.
                # Plan overhead is outside this operation's reported elapsed time.
                if connection and q.lstrip().upper().startswith('SELECT'):
                    try:plan=[list(r) for r in sqlite3.Connection.execute(connection,'EXPLAIN QUERY PLAN '+q,params).fetchall()]
                    except Exception as e:plan={'unavailable':type(e).__name__}
                stats[key]={'sql':q,'caller':caller(),'plan':plan,'count':0,'seconds':0.,'max_seconds':0.}
            s=stats[key];s['count']+=1;s['seconds']+=elapsed;s['max_seconds']=max(s['max_seconds'],elapsed)
        delta={k:after[k]-before[k] for k in ('anon','clean','dirty','writeback','rchar','read_bytes','threads','pids') if before[k] is not None and after[k] is not None}
        if elapsed>.1 or any(abs(delta.get(k,0))>=2*1048576 for k in ('anon','clean','dirty','writeback','read_bytes')):
            emit('sql_material_interval',sql=q,operation=operation,caller=caller(),thread=threading.current_thread().name,
                before=before,after=after,delta=delta,elapsed=elapsed,error=error,
                causal_proof=False)

class Cursor(sqlite3.Cursor):
    query=''
    def execute(self,q,p=()):
        self.query=q
        return measure(q,'execute',lambda:super(Cursor,self).execute(q,p),self.connection,p)
    def executemany(self,q,p):
        self.query=q
        return measure(q,'executemany',lambda:super(Cursor,self).executemany(q,p))
    def fetchall(self):return measure(self.query,'fetchall',lambda:super(Cursor,self).fetchall())
    def fetchone(self):return super().fetchone()
    def fetchmany(self,size=None):
        return measure(self.query,'fetchmany',lambda:super(Cursor,self).fetchmany() if size is None else super(Cursor,self).fetchmany(size))
class Connection(sqlite3.Connection):
    def cursor(self,factory=None):return super().cursor(factory or Cursor)
    def execute(self,q,p=()):return self.cursor().execute(q,p)
    def executemany(self,q,p):return self.cursor().executemany(q,p)
connect=sqlite3.connect
def observed_connect(*a,**kw):
    if len(a)<6 and 'factory' not in kw:kw['factory']=Connection
    return connect(*a,**kw)
sqlite3.connect=observed_connect

create_task=asyncio.BaseEventLoop.create_task
def observed_task(loop,coro,*a,**kw):
    frame=getattr(coro,'cr_frame',None)
    code=getattr(coro,'cr_code',None)
    if frame and code and '/solana_roi/' in code.co_filename:
        pending[id(frame)]={'name':kw.get('name') or code.co_qualname,'file':code.co_filename,'function':code.co_qualname}
    return create_task(loop,coro,*a,**kw)
asyncio.BaseEventLoop.create_task=observed_task
def profile(frame,event,arg):
    row=pending.get(id(frame))
    if row:
        if event=='call' and 'before' not in row:
            row['before']=resource('before_worker_first_slice')
            emit('worker_activation_begin',**row)
        elif event=='return' and 'before' in row:
            emit('worker_activation_first_suspension',**row,after=resource('after_worker_first_slice'))
            pending.pop(id(frame),None)
sys.setprofile(profile);threading.setprofile(profile)
start=threading.Thread.start
def thread_start(self,*a,**kw):
    b=resource('before_thread_start');result=start(self,*a,**kw)
    emit('thread_start',name=self.name,before=b,after=resource('after_thread_start'),caller=caller(),causal_proof=False)
    return result
threading.Thread.start=thread_start

import uvicorn
server=uvicorn.Server(uvicorn.Config('solana_roi.production:app',host='127.0.0.1',port=8768,log_level='warning',loop='asyncio'))
async def monitor():
    for i in range(18):
        emit('steady_state',sample=i,resources=resource('steady_state'))
        await asyncio.sleep(5)
    server.should_exit=True
async def main():
    emit('before_production_import',resources=resource('before_production_import'))
    watcher=asyncio.create_task(monitor())
    try:await server.serve()
    finally:watcher.cancel()
try:asyncio.run(main())
finally:
    emit('sql_summary',statements=list(stats.values()))
    closed=True
    sys.setprofile(None);threading.setprofile(None)
    LOG.close()
