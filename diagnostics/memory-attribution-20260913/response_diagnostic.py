"""Measurement-only probe; all database writes are restricted to new scratch stores.

Run with the canonical src on PYTHONPATH and requirements.lock installed. This
isolates the canonical endpoint, not the complete production worker composition.
"""
import argparse, asyncio, gc, hashlib, json, os, sqlite3, sys, threading, time, weakref
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

def sample(phase, path, **extra):
    def fields(file):
        try:
            return {p[0].rstrip(':'): int(p[1]) for l in Path(file).read_text().splitlines()
                    if len(p := l.split()) >= 2 and p[1].isdigit()}
        except OSError: return {}
    s=fields('/proc/self/status'); c=fields('/sys/fs/cgroup/memory.stat'); io=fields('/proc/self/io')
    def scalar(file):
        try: return int(Path(file).read_text())
        except (OSError,ValueError): return None
    return dict(phase=phase,timestamp=time.time(),monotonic=time.monotonic(),
        rss=s.get('VmRSS',0)*1024,anon=s.get('RssAnon',0)*1024,cgroup_anon=c.get('anon'),
        clean=max(0,c.get('file',0)-c.get('shmem',0)-c.get('file_dirty',0)-c.get('file_writeback',0)),
        dirty=c.get('file_dirty'),writeback=c.get('file_writeback'),
        rchar=io.get('rchar'),read_bytes=io.get('read_bytes'),threads=s.get('Threads'),
        pids=scalar('/sys/fs/cgroup/pids.current'),
        sizes={suffix:Path(str(path)+suffix).stat().st_size if Path(str(path)+suffix).exists() else 0
               for suffix in ('','-wal','-shm')},**extra)

def seed(path, rows):
    assert not path.exists(), 'Refusing to overwrite any existing database'
    from solana_roi import certification_incremental_replication as rep
    db=sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE anonymous_candidate_latency_failures(id INTEGER PRIMARY KEY AUTOINCREMENT,failed_at TEXT NOT NULL,reason TEXT NOT NULL,outcome TEXT NOT NULL,count INTEGER NOT NULL,max_age_ms REAL NOT NULL)')
    db.execute('CREATE INDEX ix_anonymous_candidate_latency_failures_failed_at ON anonymous_candidate_latency_failures(failed_at)')
    payload='x'*24576
    for start in range(0,rows,1000):
        db.executemany('INSERT INTO anonymous_candidate_latency_failures(failed_at,reason,outcome,count,max_age_ms) VALUES(?,?,?,?,?)',
            (('2026-09-11T12:00:00+00:00',payload,'expired_before_entry',1,float(i)) for i in range(start,min(start+1000,rows))))
        db.commit()
    store=SimpleNamespace(path=path,db=db,_lock=threading.RLock())
    rep.prepare_bootstrap(store)
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)');db.close()
    print(json.dumps({'seed_rows':rows,'bytes':path.stat().st_size}),flush=True)

def probe(path, output, skip_hook=False, natural=False):
    import fastapi, starlette
    from fastapi import FastAPI
    from starlette.responses import Response
    from solana_roi import certification_logical_bootstrap as logical
    from solana_roi import certification_incremental_replication as rep
    from solana_roi import durable_bootstrap_memory_repair as memory
    from solana_roi import logical_bootstrap_page_cache_repair as gate
    assert fastapi.__version__=='0.141.1' and starlette.__version__=='1.6.0'
    os.environ['SOLANA_ROI_CERTIFICATION_SHARED_TOKEN']='disposable-diagnostic-token'
    os.environ['SOLANA_ROI_RELEASE_COMMIT']='92c0c1620f78116e7ecbeade039e9aedaf3a51a9'
    store=SimpleNamespace(path=path,db=sqlite3.connect(path,check_same_thread=False),_lock=threading.RLock())
    identity=rep._meta(store.db)
    app=FastAPI();logical.install_certification_logical_bootstrap(app,lambda:SimpleNamespace(store=store))
    gate.configure_logical_bootstrap_page_cache_repair(app)
    events=[]; sql=[]; state={'page':-1,'vm_steps':0,'rows_fetched':0,'hook_calls':0}
    row_tail={}
    def mark(phase,**kw):
        event=sample(phase,path,page=state['page'],vm_steps=state['vm_steps'],rows_fetched=state['rows_fetched'],**kw)
        if phase in ('after_sqlite_fetch','after_python_row_materialization') and kw.get('row',0)>1:
            row_tail[phase]=event
        else:events.append(event)
    original_reader=logical._pinned_reader
    class Cursor:
        def __init__(self,c,target):self.c=c;self.target=target
        def fetchone(self):
            r=self.c.fetchone()
            if self.target and r is not None:
                state['rows_fetched']+=1
                # Streaming fetch/materialization interleave. Keep every row's
                # measurement to avoid inventing an all-rows-fetch boundary.
                mark('after_sqlite_fetch',row=state['rows_fetched'])
            return r
        def __getattr__(self,n):return getattr(self.c,n)
    class Reader:
        def __init__(self,c):self.c=c
        def execute(self,q,p=()):
            target=q.startswith('SELECT rowid,')
            sql.append({'page':state['page'],'sql':q,'params':list(p),'caller':'certification_logical_bootstrap._page',
                'plan':[list(x) for x in self.c.execute('EXPLAIN QUERY PLAN '+q,p)] if q.lstrip().upper().startswith('SELECT') else None})
            if target:mark('before_sqlite_page_fetch')
            return Cursor(self.c.execute(q,p),target)
        def __getattr__(self,n):return getattr(self.c,n)
    def reader(s):
        c=original_reader(s)
        def progress():state['vm_steps']+=1;return 0
        c.set_progress_handler(progress,1)
        return Reader(c)
    logical._pinned_reader=reader
    stream=logical._stream_page_records
    def wrapped_stream(c,**kw):
        convert=kw['row_to_record']
        def convert_record(row):
            r=convert(row);mark('after_python_row_materialization',row=state['rows_fetched']);return r
        kw['row_to_record']=convert_record
        result=stream(c,**kw)
        events.extend(sorted(row_tail.values(),key=lambda e:e['monotonic']));row_tail.clear()
        mark('after_page_materialization',returned_rows=len(result[0]));return result
    logical._stream_page_records=wrapped_stream
    init=Response.__init__;call=Response.__call__;trim=memory._trim_process_heap
    refs=[]
    def response_init(self,*a,**kw):
        init(self,*a,**kw)
        if kw.get('media_type')=='application/json' and len(self.body)>1000:
            state['hook_calls']+=1
            refs.append(weakref.ref(self))
            weakref.finalize(self,mark,'response_object_destroyed')
            mark('after_response_serialization',response_type=f'{type(self).__module__}.{type(self).__qualname__}',body_size=len(self.body),object_id=id(self))
    async def response_call(self,scope,receive,send):
        async def observe(message):
            if message['type']=='http.response.body':
                mark('before_first_asgi_body_send',body_size=len(message.get('body',b'')))
                await send(message)
                if not message.get('more_body',False):mark('after_final_asgi_body_send')
            else:await send(message)
        await call(self,scope,receive,observe)
        mark('after_Response_call_returns_object_alive',body_size=len(self.body))
    def trim_probe():
        mark('before_production_allocator_trim_response_alive',response_alive=bool(refs and refs[-1]() is not None))
        result=trim();mark('after_production_allocator_trim_response_alive');return result
    if not skip_hook:Response.__init__=response_init;Response.__call__=response_call
    memory._trim_process_heap=trim_probe
    async def scenario():
        for page in range(20 if natural else 5):
            state.update(page=page,vm_steps=0,rows_fetched=0)
            mark('before_next_page_enters')
            params=dict(table='anonymous_candidate_latency_failures',epoch=identity['epoch'],schema_fingerprint=identity['schema_fingerprint'],limit=250)
            scope={'type':'http','asgi':{'version':'3.0'},'http_version':'1.1','method':'GET','scheme':'http','path':gate.PAGE_PATH,
                'raw_path':gate.PAGE_PATH.encode(),'query_string':urlencode(params).encode(),
                'headers':[(b'x-certification-token',b'disposable-diagnostic-token')], 'client':('127.0.0.1',1),'server':('test',80),'root_path':''}
            digest=hashlib.sha256();count=0;status=None
            async def receive():return {'type':'http.request','body':b'','more_body':False}
            async def send(m):
                nonlocal count,status
                if m['type']=='http.response.start':status=m['status']
                if m['type']=='http.response.body':digest.update(m.get('body',b''));count+=len(m.get('body',b''))
            await app(scope,receive,send)
            mark('after_response_body_references_released',response_alive=bool(refs and refs[-1]() is not None),body_size=count,status=status,sha256=digest.hexdigest())
            assert status==200,status
            assert state['hook_calls']==page+1, 'Concrete production Response hook did not execute'
            assert refs[-1]() is None,'Response reference retained after ASGI app returned'
            if not natural:
                gc.collect();trim();logical.split._drop_file_cache(path)
                mark('after_diagnostic_allocator_trim_cache_cleanup')
            await asyncio.sleep(.1)
    try:
        memory._trim_process_heap();logical.split._drop_file_cache(path)
        asyncio.run(scenario())
        required={'before_next_page_enters','before_sqlite_page_fetch','after_sqlite_fetch','after_python_row_materialization',
            'after_response_serialization','before_first_asgi_body_send','after_final_asgi_body_send',
            'before_production_allocator_trim_response_alive','after_Response_call_returns_object_alive',
            'after_response_body_references_released','after_diagnostic_allocator_trim_cache_cleanup'}
        if natural:required.remove('after_diagnostic_allocator_trim_cache_cleanup')
        for page in range(20 if natural else 5):assert required <= {x['phase'] for x in events if x['page']==page}
        assert state['hook_calls']==(20 if natural else 5), 'Concrete production Response hook did not execute'
        output.write_text(json.dumps({'python':sys.version,'fastapi':fastapi.__version__,'starlette':starlette.__version__,
            'events':events,'sql':sql,'hook_calls':state['hook_calls'],
            'limitations':['endpoint isolation, not full production composition','shared 14 GiB cgroup, not exclusive 2 GiB',
            'rows_fetched is returned cursor rows, not SQLite scanstatus NVISIT','VM steps include EXPLAIN QUERY PLAN instrumentation',
            'synthetic schema-shaped history, no production row-distribution proof','same logical rows; epoch differs across stores']},indent=2))
    finally:
        Response.__init__=init;Response.__call__=call;store.db.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['seed','probe']);p.add_argument('path',type=Path);p.add_argument('--rows',type=int);p.add_argument('--output',type=Path);p.add_argument('--skip-hook',action='store_true');p.add_argument('--natural',action='store_true');a=p.parse_args()
    # Existing paths only permitted under this turn's disposable area.
    root=Path('/workspace/scratch/46b2fc0f9965/disposable').resolve()
    assert a.path.resolve().is_relative_to(root)
    root.mkdir(exist_ok=True)
    if a.action=='seed':seed(a.path,a.rows)
    else:probe(a.path,a.output,a.skip_hook,a.natural)
