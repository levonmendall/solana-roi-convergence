from __future__ import annotations

import json, math, statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence
from .strategy_v52_authority import target_sizing_policy

FORWARD_ALPHA_VERSION = "v52-wallet-forward-alpha-v1"
HORIZON_SECONDS = {"15s":15,"30s":30,"60s":60,"2m":120,"5m":300,"graduation":None,"post_graduation":None,"v52_exit":None}
VALIDATION_WINDOWS = ("24h","7d","30d")
VALIDATION_VARIANTS = ("baseline_v52","current_wallet","wallet_forward_alpha")
STATUS_MATERIAL = "WALLET FORWARD ALPHA VALIDATED — MATERIAL STRATEGY VALUE PROVEN"
STATUS_RISK_ONLY = "WALLET FORWARD ALPHA VALIDATED — RISK VALUE ONLY"
STATUS_NO_VALUE = "WALLET FORWARD ALPHA VALIDATED — NO MATERIAL VALUE"
STATUS_INCOMPLETE = "VALIDATION INCOMPLETE — MORE EVIDENCE REQUIRED"

def _utc(x: datetime) -> datetime:
    if x.tzinfo is None: raise ValueError("timestamps must be timezone-aware")
    return x.astimezone(timezone.utc)
def _finite(x: float, name: str) -> float:
    y=float(x)
    if not math.isfinite(y): raise ValueError(f"{name} must be finite")
    return y
def _json(x: Any) -> str: return json.dumps(x,sort_keys=True,separators=(",",":"),default=str)
def _wmedian(values: Sequence[float], weights: Sequence[float]) -> float:
    if not values: return 0.0
    pairs=sorted(zip(values,weights)); total=sum(max(0.0,w) for _,w in pairs); acc=0.0
    if total<=0: return float(statistics.median(values))
    for v,w in pairs:
        acc += max(0.0,w)
        if acc >= total/2: return float(v)
    return float(pairs[-1][0])

@dataclass(frozen=True, slots=True)
class WalletPointInTimeObservation:
    wallet:str; context_key:str; candidate_id:str; token_mint:str; transaction_signature:str
    chain_timestamp:datetime; first_observable_at:datetime; detected_at:datetime
    lifecycle:str; graduation_state:str; observed_price:float; earliest_executable_price:float
    liquidity_usd:float; slippage_fraction:float; fee_fraction:float; market_impact_fraction:float; max_executable_usd:float
    entity_id:str|None=None; relationships:Mapping[str,Any]|None=None; integrity_known:Mapping[str,Any]|None=None
    wallet_statistics_known:Mapping[str,Any]|None=None; v52_candidate_state:str="unknown"; v52_decision_state:str="unknown"
    could_enter:bool=False; entry_blocker:str|None=None
    @property
    def detection_latency_seconds(self)->float: return max(0.0,(_utc(self.detected_at)-_utc(self.first_observable_at)).total_seconds())

@dataclass(frozen=True, slots=True)
class WalletForwardOutcome:
    wallet:str; context_key:str; candidate_id:str; horizon:str; available_at:datetime
    gross_return:float; net_executable_return:float; matched_control_net_return:float
    exit_price:float; exit_liquidity_usd:float; exit_capacity_usd:float
    max_favorable_excursion:float; max_adverse_excursion:float; copyable:bool=True
    @property
    def marginal_alpha(self)->float: return self.net_executable_return-self.matched_control_net_return

@dataclass(frozen=True, slots=True)
class WalletIntegritySnapshot:
    wallet:str; observed_at:datetime; integrity_score:float; suspicious:bool
    creator_associated:bool=False; common_funder_cluster:str|None=None; reasons:tuple[str,...]=()

@dataclass(frozen=True, slots=True)
class WalletForwardScore:
    wallet:str; context_key:str; horizon:str; as_of:datetime; observations:int; effective_weight:float
    raw_expected_marginal_alpha:float; shrunk_expected_marginal_alpha:float; median_marginal_alpha:float
    lower_confidence_bound:float; upper_confidence_bound:float; downside_mean:float; max_adverse_excursion:float
    win_rate:float; copyability_rate:float; capacity_coverage_for_500:float; tier:str; role:str; confidence:float
    integrity_score:float|None; integrity_suspicious:bool|None; eligible_for_strategy_influence:bool
    blockers:tuple[str,...]; alpha_decay:Mapping[str,float|None]

@dataclass(frozen=True, slots=True)
class ReplayComparisonObservation:
    window:str; candidate_id:str; baseline_v52_return:float; current_wallet_return:float; wallet_forward_alpha_return:float
    baseline_drawdown:float=0.0; current_wallet_drawdown:float=0.0; wallet_forward_alpha_drawdown:float=0.0
    lookahead_free:bool=True; execution_realistic:bool=True

@dataclass(frozen=True, slots=True)
class ValidationWindowResult:
    window:str; observations:int; baseline_mean_return:float; current_wallet_mean_return:float; forward_alpha_mean_return:float
    forward_vs_baseline_mean:float; forward_vs_current_mean:float; forward_vs_baseline_lower_95:float; forward_vs_current_lower_95:float
    baseline_max_drawdown:float; current_wallet_max_drawdown:float; forward_alpha_max_drawdown:float
    leakage_failures:int; realism_failures:int; accepted:bool

@dataclass(frozen=True, slots=True)
class WalletForwardValidationReport:
    status:str; windows:tuple[ValidationWindowResult,...]; strategy_influence_enabled:bool; influence_scope:tuple[str,...]; reasons:tuple[str,...]

@dataclass(frozen=True, slots=True)
class GraduationBuyerEvidence:
    wallet:str; entity_id:str; integrity_suspicious:bool; creator_associated:bool; common_funder_cluster:str|None
    forward_alpha_score:float; forward_alpha_confidence:float; alpha_exhausted:bool

@dataclass(frozen=True, slots=True)
class GraduationQuality:
    unique_wallets:int; independent_entities:int; suspicious_entities:int; creator_associated_entities:int
    high_forward_alpha_entities:int; exhausted_alpha_entities:int; entity_breadth_score:float; wallet_alpha_score:float
    integrity_score:float; quality_score:float; correlated_signal_counted_once:bool

class WalletForwardAlphaEngine:
    def __init__(self, store:Any, *, half_life_hours:float=72.0):
        if half_life_hours<=0: raise ValueError("half_life_hours must be positive")
        self.store=store; self.half_life_hours=float(half_life_hours); self._schema()
    def _schema(self)->None:
        sql=(
        "CREATE TABLE IF NOT EXISTS v52_wallet_point_in_time_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,wallet TEXT NOT NULL,context_key TEXT NOT NULL,candidate_id TEXT NOT NULL,token_mint TEXT NOT NULL,transaction_signature TEXT NOT NULL,chain_timestamp TEXT NOT NULL,first_observable_at TEXT NOT NULL,detected_at TEXT NOT NULL,detection_latency_seconds REAL NOT NULL,lifecycle TEXT NOT NULL,graduation_state TEXT NOT NULL,observed_price REAL NOT NULL,earliest_executable_price REAL NOT NULL,liquidity_usd REAL NOT NULL,slippage_fraction REAL NOT NULL,fee_fraction REAL NOT NULL,market_impact_fraction REAL NOT NULL,max_executable_usd REAL NOT NULL,entity_id TEXT,relationships_json TEXT NOT NULL,integrity_known_json TEXT NOT NULL,wallet_statistics_known_json TEXT NOT NULL,v52_candidate_state TEXT NOT NULL,v52_decision_state TEXT NOT NULL,could_enter INTEGER NOT NULL,entry_blocker TEXT,UNIQUE(wallet,context_key,candidate_id))",
        "CREATE INDEX IF NOT EXISTS ix_v52_wallet_pit_score ON v52_wallet_point_in_time_observations(wallet,context_key,detected_at)",
        "CREATE TABLE IF NOT EXISTS v52_wallet_forward_outcomes(id INTEGER PRIMARY KEY AUTOINCREMENT,wallet TEXT NOT NULL,context_key TEXT NOT NULL,candidate_id TEXT NOT NULL,horizon TEXT NOT NULL,available_at TEXT NOT NULL,gross_return REAL NOT NULL,net_executable_return REAL NOT NULL,matched_control_net_return REAL NOT NULL,marginal_alpha REAL NOT NULL,exit_price REAL NOT NULL,exit_liquidity_usd REAL NOT NULL,exit_capacity_usd REAL NOT NULL,max_favorable_excursion REAL NOT NULL,max_adverse_excursion REAL NOT NULL,copyable INTEGER NOT NULL,UNIQUE(wallet,context_key,candidate_id,horizon))",
        "CREATE INDEX IF NOT EXISTS ix_v52_wallet_forward_score ON v52_wallet_forward_outcomes(wallet,context_key,horizon,available_at)",
        "CREATE TABLE IF NOT EXISTS v52_wallet_integrity_snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT,wallet TEXT NOT NULL,observed_at TEXT NOT NULL,integrity_score REAL NOT NULL,suspicious INTEGER NOT NULL,creator_associated INTEGER NOT NULL,common_funder_cluster TEXT,reasons_json TEXT NOT NULL,UNIQUE(wallet,observed_at))",
        "CREATE INDEX IF NOT EXISTS ix_v52_wallet_integrity_time ON v52_wallet_integrity_snapshots(wallet,observed_at)",
        "CREATE TABLE IF NOT EXISTS v52_wallet_forward_validation(id INTEGER PRIMARY KEY AUTOINCREMENT,evaluated_at TEXT NOT NULL,status TEXT NOT NULL,strategy_influence_enabled INTEGER NOT NULL,influence_scope_json TEXT NOT NULL,reasons_json TEXT NOT NULL,windows_json TEXT NOT NULL,paper_only INTEGER NOT NULL,UNIQUE(evaluated_at))")
        with self.store._lock,self.store.db:
            for stmt in sql: self.store.db.execute(stmt)
    def record_observation(self,o:WalletPointInTimeObservation)->bool:
        if not o.wallet or not o.context_key or not o.candidate_id or not o.token_mint or not o.transaction_signature: raise ValueError("observation identity fields required")
        chain,first,detected=_utc(o.chain_timestamp),_utc(o.first_observable_at),_utc(o.detected_at)
        if first<chain or detected<first: raise ValueError("point-in-time timestamp order invalid")
        nums=[_finite(getattr(o,k),k) for k in ("observed_price","earliest_executable_price","liquidity_usd","slippage_fraction","fee_fraction","market_impact_fraction","max_executable_usd")]
        if nums[0]<=0 or nums[1]<=0 or min(nums[2:])<0: raise ValueError("price/cost/capacity values invalid")
        with self.store._lock,self.store.db:
            cur=self.store.db.execute("INSERT OR IGNORE INTO v52_wallet_point_in_time_observations(wallet,context_key,candidate_id,token_mint,transaction_signature,chain_timestamp,first_observable_at,detected_at,detection_latency_seconds,lifecycle,graduation_state,observed_price,earliest_executable_price,liquidity_usd,slippage_fraction,fee_fraction,market_impact_fraction,max_executable_usd,entity_id,relationships_json,integrity_known_json,wallet_statistics_known_json,v52_candidate_state,v52_decision_state,could_enter,entry_blocker) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(o.wallet,o.context_key,o.candidate_id,o.token_mint,o.transaction_signature,chain.isoformat(),first.isoformat(),detected.isoformat(),o.detection_latency_seconds,o.lifecycle,o.graduation_state,*nums,o.entity_id,_json(o.relationships or {}),_json(o.integrity_known or {}),_json(o.wallet_statistics_known or {}),o.v52_candidate_state,o.v52_decision_state,1 if o.could_enter else 0,o.entry_blocker))
        if cur.rowcount==1:
            self.store.append("v52_wallet_point_in_time_observation",detected.isoformat(),{"wallet":o.wallet,"context_key":o.context_key,"candidate_id":o.candidate_id,"future_information_used":False,"paper_only":True}); return True
        return False
    def record_forward_outcome(self,o:WalletForwardOutcome)->bool:
        if o.horizon not in HORIZON_SECONDS: raise ValueError(f"unsupported horizon:{o.horizon}")
        available=_utc(o.available_at)
        vals=[_finite(getattr(o,k),k) for k in ("gross_return","net_executable_return","matched_control_net_return","exit_price","exit_liquidity_usd","exit_capacity_usd","max_favorable_excursion","max_adverse_excursion")]
        if vals[1]<=-1 or vals[2]<=-1 or vals[3]<=0 or min(vals[4:])<0: raise ValueError("forward outcome values invalid")
        with self.store._lock:
            src=self.store.db.execute("SELECT detected_at FROM v52_wallet_point_in_time_observations WHERE wallet=? AND context_key=? AND candidate_id=?",(o.wallet,o.context_key,o.candidate_id)).fetchone()
        if src is None or available<datetime.fromisoformat(str(src["detected_at"])): raise ValueError("valid prior observation required")
        with self.store._lock,self.store.db:
            cur=self.store.db.execute("INSERT OR IGNORE INTO v52_wallet_forward_outcomes(wallet,context_key,candidate_id,horizon,available_at,gross_return,net_executable_return,matched_control_net_return,marginal_alpha,exit_price,exit_liquidity_usd,exit_capacity_usd,max_favorable_excursion,max_adverse_excursion,copyable) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(o.wallet,o.context_key,o.candidate_id,o.horizon,available.isoformat(),*vals[:3],o.marginal_alpha,*vals[3:],1 if o.copyable else 0))
        if cur.rowcount==1: self.store.append("v52_wallet_forward_outcome",available.isoformat(),{"wallet":o.wallet,"candidate_id":o.candidate_id,"horizon":o.horizon,"marginal_alpha":o.marginal_alpha,"paper_only":True}); return True
        return False
    def record_integrity(self,s:WalletIntegritySnapshot)->bool:
        score=_finite(s.integrity_score,"integrity_score"); observed=_utc(s.observed_at)
        if not s.wallet or not 0<=score<=1: raise ValueError("wallet/integrity invalid")
        with self.store._lock,self.store.db:
            cur=self.store.db.execute("INSERT OR IGNORE INTO v52_wallet_integrity_snapshots(wallet,observed_at,integrity_score,suspicious,creator_associated,common_funder_cluster,reasons_json) VALUES (?,?,?,?,?,?,?)",(s.wallet,observed.isoformat(),score,1 if s.suspicious else 0,1 if s.creator_associated else 0,s.common_funder_cluster,_json(s.reasons)))
        if cur.rowcount==1: self.store.append("v52_wallet_integrity_snapshot",observed.isoformat(),{"wallet":s.wallet,"integrity_score":score,"suspicious":s.suspicious,"paper_only":True}); return True
        return False
    def _integrity(self,wallet:str,as_of:datetime)->tuple[float|None,bool|None]:
        with self.store._lock: r=self.store.db.execute("SELECT integrity_score,suspicious FROM v52_wallet_integrity_snapshots WHERE wallet=? AND observed_at<=? ORDER BY observed_at DESC,id DESC LIMIT 1",(wallet,_utc(as_of).isoformat())).fetchone()
        return (None,None) if r is None else (float(r["integrity_score"]),bool(r["suspicious"]))
    def _rows(self,wallet:str,context:str,horizon:str,as_of:datetime)->list[dict[str,Any]]:
        with self.store._lock:
            rows=self.store.db.execute("SELECT o.available_at,o.marginal_alpha,o.max_adverse_excursion,o.copyable,o.exit_capacity_usd,p.max_executable_usd FROM v52_wallet_forward_outcomes o JOIN v52_wallet_point_in_time_observations p ON p.wallet=o.wallet AND p.context_key=o.context_key AND p.candidate_id=o.candidate_id WHERE o.wallet=? AND o.context_key=? AND o.horizon=? AND o.available_at<=? ORDER BY o.available_at,o.id",(wallet,context,horizon,_utc(as_of).isoformat())).fetchall()
        return [dict(r) for r in rows]
    def _curve(self,wallet:str,context:str,as_of:datetime)->dict[str,float|None]:
        out={}
        for h in HORIZON_SECONDS:
            rows=self._rows(wallet,context,h,as_of); out[h]=statistics.fmean(float(r["marginal_alpha"]) for r in rows) if rows else None
        return out
    @staticmethod
    def _role(c:Mapping[str,float|None])->str:
        p=lambda h:c.get(h) is not None and float(c[h])>0
        if p("15s"): return "discovery"
        if p("60s") or p("2m"): return "confirmation"
        if p("graduation"): return "graduation_confirmation"
        if p("post_graduation"): return "post_graduation"
        if p("v52_exit"): return "re_entry_or_exit_context"
        if any(v is not None and v<0 for v in c.values()): return "risk_avoidance"
        return "retrospective_only"
    def score(self,wallet:str,context_key:str,*,horizon:str="60s",as_of:datetime|None=None,reference_capital_usd:float=500.0)->WalletForwardScore:
        if horizon not in HORIZON_SECONDS: raise ValueError(f"unsupported horizon:{horizon}")
        now=_utc(as_of or datetime.now(timezone.utc)); capital=_finite(reference_capital_usd,"reference_capital_usd")
        if capital<=0: raise ValueError("reference_capital_usd must be positive")
        rows=self._rows(wallet,context_key,horizon,now); alpha=[float(r["marginal_alpha"]) for r in rows]
        weights=[2**(-max(0,(now-datetime.fromisoformat(str(r["available_at"]))).total_seconds()/3600)/self.half_life_hours) for r in rows]
        tw=sum(weights); raw=sum(w*x for w,x in zip(weights,alpha))/tw if tw else 0.0; minimum=int(target_sizing_policy()["minimum_forward_samples"]); prior=max(1.0,minimum/2)
        shrunk=raw*tw/(tw+prior) if tw else 0.0; median=_wmedian(alpha,weights)
        variance=sum(w*(x-raw)**2 for w,x in zip(weights,alpha))/tw if tw else 0.0; en=(tw*tw/max(1e-12,sum(w*w for w in weights))) if tw else 0.0; se=math.sqrt(max(0,variance)/max(1.0,en)); lower,upper=shrunk-1.96*se,shrunk+1.96*se
        copy=sum(bool(r["copyable"]) for r in rows)/len(rows) if rows else 0.0; cap=statistics.fmean(min(1.0,min(float(r["max_executable_usd"]),float(r["exit_capacity_usd"]))/capital) for r in rows) if rows else 0.0
        integrity,suspicious=self._integrity(wallet,now); blockers=[]
        if len(rows)<minimum: blockers.append("insufficient_point_in_time_forward_samples")
        if lower<=0: blockers.append("forward_alpha_lower_confidence_bound_not_positive")
        if copy<.80: blockers.append("copyability_rate_below_minimum")
        if cap<.80: blockers.append("reference_capital_capacity_below_minimum")
        if suspicious is True: blockers.append("wallet_integrity_suspicious")
        if integrity is None: blockers.append("wallet_integrity_evidence_missing")
        tier="unclassified" if len(rows)<max(3,minimum//2) else "D" if shrunk<=0 else "C" if lower<=0 else "A" if len(rows)>=minimum*2 and copy>=.90 and cap>=.90 else "B"
        conf=min(1.0,len(rows)/max(1,minimum*2))*copy*cap*(1.0 if lower>0 else max(0.0,min(1.0,.5+shrunk/max(1e-9,2*(se or 1.0)))))
        curve=self._curve(wallet,context_key,now)
        return WalletForwardScore(wallet,context_key,horizon,now,len(rows),tw,raw,shrunk,median,lower,upper,statistics.fmean(min(0.0,x) for x in alpha) if alpha else 0.0,max((float(r["max_adverse_excursion"]) for r in rows),default=0.0),sum(x>0 for x in alpha)/len(alpha) if alpha else 0.0,copy,cap,tier,self._role(curve),max(0.0,min(1.0,conf)),integrity,suspicious,not blockers,tuple(blockers),curve)
    @staticmethod
    def graduation_quality(buyers:Iterable[GraduationBuyerEvidence])->GraduationQuality:
        by_entity={}; wallets=set()
        for b in buyers:
            wallets.add(b.wallet); old=by_entity.get(b.entity_id)
            if old is None or b.forward_alpha_confidence>old.forward_alpha_confidence: by_entity[b.entity_id]=b
        xs=list(by_entity.values()); n=len(xs); suspicious=sum(x.integrity_suspicious for x in xs); creator=sum(x.creator_associated or bool(x.common_funder_cluster) for x in xs); high=sum(x.forward_alpha_score>0 and x.forward_alpha_confidence>=.5 and not x.alpha_exhausted for x in xs); exhausted=sum(x.alpha_exhausted for x in xs)
        breadth=1-math.exp(-n/6) if n else 0; alpha=high/n if n else 0; integrity=max(0.0,1-(suspicious+.5*creator)/max(1,n)); quality=max(0.0,min(1.0,.45*breadth+.30*integrity+.25*alpha))
        return GraduationQuality(len(wallets),n,suspicious,creator,high,exhausted,breadth,alpha,integrity,quality,True)
    @staticmethod
    def _paired(values:Sequence[float])->tuple[float,float]:
        if not values:return 0.0,0.0
        mean=statistics.fmean(values)
        if len(values)<2:return mean,mean
        return mean,mean-1.96*statistics.stdev(values)/math.sqrt(len(values))
    @classmethod
    def evaluate_replay(cls,rows:Sequence[ReplayComparisonObservation])->WalletForwardValidationReport:
        minimum=int(target_sizing_policy()["minimum_forward_samples"]); results=[]; reasons=[]
        for window in VALIDATION_WINDOWS:
            s=[r for r in rows if r.window==window]
            if not s: reasons.append(f"missing_{window}_comparison"); continue
            b=[r.baseline_v52_return for r in s]; c=[r.current_wallet_return for r in s]; f=[r.wallet_forward_alpha_return for r in s]; mb,lb=cls._paired([x-y for x,y in zip(f,b)]); mc,lc=cls._paired([x-y for x,y in zip(f,c)]); leak=sum(not r.lookahead_free for r in s); realism=sum(not r.execution_realistic for r in s); bdd=max((r.baseline_drawdown for r in s),default=0); cdd=max((r.current_wallet_drawdown for r in s),default=0); fdd=max((r.wallet_forward_alpha_drawdown for r in s),default=0); ceiling=max(bdd,cdd)*1.10+1e-12
            accepted=len(s)>=minimum and not leak and not realism and mb>0 and mc>=0 and lb>0 and lc>=0 and fdd<=ceiling
            if len(s)<minimum: reasons.append(f"{window}_insufficient_paired_samples")
            if leak: reasons.append(f"{window}_lookahead_leakage")
            if realism: reasons.append(f"{window}_execution_realism_failure")
            if fdd>ceiling: reasons.append(f"{window}_drawdown_worse_than_tolerance")
            results.append(ValidationWindowResult(window,len(s),statistics.fmean(b),statistics.fmean(c),statistics.fmean(f),mb,mc,lb,lc,bdd,cdd,fdd,leak,realism,accepted))
        complete=len(results)==3 and all(r.observations>=minimum for r in results); material=complete and all(r.accepted for r in results)
        if material: status,enabled,scope=STATUS_MATERIAL,True,("candidate_ranking","graduation_confidence","entry_confidence","bounded_sizing")
        elif complete and all(not r.leakage_failures and not r.realism_failures for r in results):
            risk=all(r.forward_alpha_max_drawdown<=r.baseline_max_drawdown+1e-12 for r in results); status,enabled,scope=(STATUS_RISK_ONLY,False,("negative_risk_filter",)) if risk else (STATUS_NO_VALUE,False,())
        else: status,enabled,scope=STATUS_INCOMPLETE,False,()
        return WalletForwardValidationReport(status,tuple(results),enabled,scope,tuple(dict.fromkeys(reasons)))
    def persist_validation(self,report:WalletForwardValidationReport,*,evaluated_at:datetime|None=None)->None:
        now=_utc(evaluated_at or datetime.now(timezone.utc))
        with self.store._lock,self.store.db: self.store.db.execute("INSERT INTO v52_wallet_forward_validation(evaluated_at,status,strategy_influence_enabled,influence_scope_json,reasons_json,windows_json,paper_only) VALUES (?,?,?,?,?,?,1)",(now.isoformat(),report.status,1 if report.strategy_influence_enabled else 0,_json(report.influence_scope),_json(report.reasons),_json([asdict(x) for x in report.windows])))
        self.store.append("v52_wallet_forward_validation",now.isoformat(),{"status":report.status,"strategy_influence_enabled":report.strategy_influence_enabled,"paper_only":True})
    def latest_validation(self)->WalletForwardValidationReport:
        with self.store._lock:r=self.store.db.execute("SELECT status,strategy_influence_enabled,influence_scope_json,reasons_json,windows_json FROM v52_wallet_forward_validation ORDER BY id DESC LIMIT 1").fetchone()
        if r is None:return WalletForwardValidationReport(STATUS_INCOMPLETE,(),False,(),("no_persisted_point_in_time_validation",))
        windows=tuple(ValidationWindowResult(**x) for x in json.loads(str(r["windows_json"])))
        return WalletForwardValidationReport(str(r["status"]),windows,bool(r["strategy_influence_enabled"]),tuple(json.loads(str(r["influence_scope_json"]))),tuple(json.loads(str(r["reasons_json"]))))
    def strategy_profile(self,*,wallet:str,context_key:str,as_of:datetime|None=None,reference_capital_usd:float=500.0)->dict[str,Any]:
        score=self.score(wallet,context_key,as_of=as_of,reference_capital_usd=reference_capital_usd); validation=self.latest_validation(); enabled=validation.strategy_influence_enabled and score.eligible_for_strategy_influence; mult=1.0+max(-.20,min(.10,score.shrunk_expected_marginal_alpha*score.confidence)) if enabled else 1.0
        return {"version":FORWARD_ALPHA_VERSION,"wallet_forward_alpha":asdict(score),"validation_status":validation.status,"strategy_influence_enabled":enabled,"sizing_multiplier":mult,"may_create_eligibility":False,"may_bypass_risk_or_execution":False,"independent_trade_authority":False,"paper_only":True}
    def status(self)->dict[str,Any]:
        with self.store._lock:o=self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_point_in_time_observations").fetchone()[0]; f=self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_forward_outcomes").fetchone()[0]; i=self.store.db.execute("SELECT COUNT(*) FROM v52_wallet_integrity_snapshots").fetchone()[0]
        v=self.latest_validation(); return {"version":FORWARD_ALPHA_VERSION,"point_in_time_observations":int(o),"forward_outcomes":int(f),"integrity_snapshots":int(i),"forward_horizons":tuple(HORIZON_SECONDS),"validation_windows":VALIDATION_WINDOWS,"validation_variants":VALIDATION_VARIANTS,"validation_status":v.status,"strategy_influence_enabled":v.strategy_influence_enabled,"reference_portfolio_usd":500.0,"alpha_integrity_separated":True,"dynamic_tiers":("A","B","C","D","unclassified"),"hard_coded_wallet_whitelist":False,"future_outcomes_excluded_until_available_at":True,"paper_only":True,"live_money_authority":False,"signing_available":False,"transaction_submission_available":False}

def wallet_forward_shadow_profile(store:Any,*,wallet:str,context_key:str,as_of:datetime|None=None,reference_capital_usd:float=500.0)->dict[str,Any]:
    neutral={"version":FORWARD_ALPHA_VERSION,"available":False,"strategy_influence_enabled":False,"sizing_multiplier":1.0,"may_create_eligibility":False,"may_bypass_risk_or_execution":False,"independent_trade_authority":False,"paper_only":True}
    if not wallet or not context_key:return {**neutral,"reason":"wallet_or_context_missing"}
    required={"v52_wallet_point_in_time_observations","v52_wallet_forward_outcomes","v52_wallet_integrity_snapshots","v52_wallet_forward_validation"}
    try:
        with store._lock: names={str(r[0]) for r in store.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?)",tuple(sorted(required))).fetchall()}
        if names!=required:return {**neutral,"reason":"forward_alpha_schema_not_installed"}
        p=WalletForwardAlphaEngine(store).strategy_profile(wallet=wallet,context_key=context_key,as_of=as_of,reference_capital_usd=reference_capital_usd); p["available"]=True; return p
    except Exception as exc:return {**neutral,"reason":f"fail_closed:{type(exc).__name__}"}

__all__=["FORWARD_ALPHA_VERSION","GraduationBuyerEvidence","GraduationQuality","HORIZON_SECONDS","ReplayComparisonObservation","STATUS_INCOMPLETE","STATUS_MATERIAL","STATUS_NO_VALUE","STATUS_RISK_ONLY","VALIDATION_VARIANTS","VALIDATION_WINDOWS","ValidationWindowResult","WalletForwardAlphaEngine","WalletForwardOutcome","WalletForwardScore","WalletForwardValidationReport","WalletIntegritySnapshot","WalletPointInTimeObservation","wallet_forward_shadow_profile"]
