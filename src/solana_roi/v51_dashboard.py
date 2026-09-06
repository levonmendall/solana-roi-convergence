from __future__ import annotations

from html import escape
from typing import Any

DASHBOARD_VERSION = "v51-economic-dashboard-v2-phase10-proof-confidence"


def _num(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "—"


def _pct(value: Any, digits: int = 2) -> str:
    try:
        return f"{100.0 * float(value):.{digits}f}%"
    except (TypeError, ValueError):
        return "—"


def _state(family: dict[str, Any]) -> str:
    profile = family.get("promotion_kill_profile") or {}
    return str(profile.get("state") or "unproven")


def _sensitivity(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "—"
    parts: list[str] = []
    for name, raw in value.items():
        row = raw if isinstance(raw, dict) else {}
        parts.append(f"{name}:{_pct(row.get('mean_return'))}")
    return " · ".join(parts)


def _stress(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "—"
    parts: list[str] = []
    for name, raw in value.items():
        row = raw if isinstance(raw, dict) else {}
        parts.append(f"{name}:{_num(row.get('best_expected_log_growth'), 6)}")
    return " · ".join(parts)


def render_economic_dashboard(
    certification: dict[str, Any],
    coverage: dict[str, Any],
    mechanism_stress: dict[str, Any] | None = None,
    *,
    funnel: dict[str, Any] | None = None,
    proof_confidence: dict[str, Any] | None = None,
) -> str:
    families = certification.get("families") or {}
    ranking = list(certification.get("research_family_ranking") or families.keys())
    weights = certification.get("paper_allocation_weights") or {}
    confidence = proof_confidence or {}
    rows: list[str] = []
    for family in ranking:
        data = families.get(family) or {}
        robust = data.get("robust_profile") or {}
        proof = confidence.get(family) or {
            "raw_n": data.get("closed_outcome_count"),
            "independent_n": data.get("independent_event_count"),
            "holdout_n": 0,
            "net_roi": data.get("net_roi_sum"),
            "compounded_nav": data.get("compounded_nav_multiple"),
            "expected_log_growth": robust.get("best_expected_log_growth"),
            "lcb_expected_log_growth": robust.get("expected_log_growth_ci95_lower"),
            "es20": robust.get("expected_shortfall_20"),
            "max_drawdown": robust.get("max_drawdown_at_best_fraction"),
            "winner_concentration": robust.get("winner_concentration"),
            "top_1_removed": robust.get("leave_best_trade_out_mean"),
            "top_3_removed": robust.get("remove_top_3_mean"),
            "latency_sensitivity": data.get("latency_sensitivity"),
            "cost_sensitivity": data.get("execution_cost_sensitivity"),
            "stress_performance": data.get("execution_stress"),
            "promotion_state": _state(data),
        }
        rows.append(
            "<tr>"
            f"<td><strong>{escape(str(family))}</strong></td>"
            f"<td>{int(proof.get('raw_n') or 0)}</td>"
            f"<td>{int(proof.get('independent_n') or 0)}</td>"
            f"<td>{int(proof.get('holdout_n') or 0)}</td>"
            f"<td>{_pct(proof.get('net_roi'))}</td>"
            f"<td>{_num(proof.get('compounded_nav'))}</td>"
            f"<td>{_num(proof.get('expected_log_growth'), 6)}</td>"
            f"<td>{_num(proof.get('lcb_expected_log_growth'), 6)}</td>"
            f"<td>{_pct(proof.get('es20'))}</td>"
            f"<td>{_pct(proof.get('max_drawdown'))}</td>"
            f"<td>{_pct(proof.get('winner_concentration'))}</td>"
            f"<td>{_pct(proof.get('top_1_removed'))}</td>"
            f"<td>{_pct(proof.get('top_3_removed'))}</td>"
            f"<td class='wide'>{escape(_sensitivity(proof.get('latency_sensitivity')))}</td>"
            f"<td class='wide'>{escape(_sensitivity(proof.get('cost_sensitivity')))}</td>"
            f"<td class='wide'>{escape(_stress(proof.get('stress_performance')))}</td>"
            f"<td>{escape(str(proof.get('promotion_state') or 'unproven'))}</td>"
            f"<td>{_pct(weights.get(family, 0.0))}</td>"
            "</tr>"
        )

    stress_summary: list[str] = []
    if mechanism_stress:
        mechanisms = mechanism_stress.get("mechanisms") or []
        for mechanism in mechanisms:
            stress_summary.append(f"<span class='chip'>{escape(str(mechanism))}</span>")

    coverage_complete = bool(coverage.get("coverage_complete"))
    coverage_class = "ok" if coverage_complete else "warn"
    coverage_text = "complete" if coverage_complete else "debt present"
    closed = int(certification.get("closed_outcome_count") or 0)
    cash = _pct(certification.get("paper_cash_weight", 1.0))
    debt = int(coverage.get("coverage_debt_count") or 0)
    authority = escape(str(certification.get("authority_id") or "unknown"))
    epoch = escape(str(certification.get("economic_freeze_epoch") or "unknown"))
    funnel = funnel or {}
    funnel_cards = "".join(
        f"<div class='card'><div class='k'>{escape(label)}</div><div class='v'>{int(funnel.get(key) or 0)}</div></div>"
        for key, label in (
            ("detected_opportunities", "Detected opportunities"),
            ("evaluated_opportunities", "Evaluated opportunities"),
            ("coverage_debt", "Coverage debt"),
            ("paper_entries", "Paper entries"),
            ("settled_trades", "Settled trades"),
            ("promoted_trades", "Promoted trades"),
            ("research_probes", "Research probes"),
            ("missed_opportunities", "Missed opportunities"),
        )
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ROI Convergence v5.1 System Proof</title>
<style>
:root{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111;background:#f6f7f8}}
body{{margin:0;padding:24px}} main{{max-width:1500px;margin:auto}}
h1{{margin:0 0 6px;font-size:28px}} .sub{{color:#5b6470;margin-bottom:22px}}
.badge{{display:inline-block;border:1px solid #222;border-radius:999px;padding:4px 9px;font-size:12px;font-weight:700;margin-right:6px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:18px 0}}
.card{{background:white;border:1px solid #dde1e5;border-radius:12px;padding:16px}} .k{{font-size:12px;color:#6b7280}} .v{{font-size:24px;font-weight:700;margin-top:5px}}
.ok{{color:#08783e}} .warn{{color:#a15c00}} table{{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden}}
th,td{{padding:10px 9px;border-bottom:1px solid #eceff2;text-align:right;font-size:12px;white-space:nowrap}} th:first-child,td:first-child{{text-align:left}} th{{background:#fafafa;color:#4b5563}}
td.wide{{max-width:360px;white-space:normal;text-align:left}}
section{{margin-top:22px}} .chip{{display:inline-block;background:white;border:1px solid #d7dce1;border-radius:999px;padding:5px 9px;margin:4px 4px 0 0;font-size:12px}}
.note{{font-size:12px;color:#6b7280;line-height:1.5}} .scroll{{overflow-x:auto}}
</style>
</head>
<body><main>
<h1>v5.1 System Proof Dashboard</h1>
<div class="sub"><span class="badge">PAPER ONLY</span><span class="badge">NO LIVE-MONEY AUTHORITY</span>Dashboard {escape(DASHBOARD_VERSION)}</div>
<div class="note">Authority <strong>{authority}</strong><br>Frozen evidence epoch <strong>{epoch}</strong></div>
<div class="grid">
<div class="card"><div class="k">Closed outcomes</div><div class="v">{closed}</div></div>
<div class="card"><div class="k">Paper cash weight</div><div class="v">{cash}</div></div>
<div class="card"><div class="k">Candidate coverage</div><div class="v {coverage_class}">{coverage_text}</div></div>
<div class="card"><div class="k">Coverage debt</div><div class="v">{debt}</div></div>
</div>
<section><h2>Opportunity funnel</h2><div class="grid">{funnel_cards}</div>
<p class="note">Detected → evaluated → paper entry → settlement are operational stages. Research probes are rejected candidates kept for counterfactual learning; missed opportunities are rejected probes later resolving positive. Promoted trades are settled outcomes in families with a currently valid promotion claim.</p></section>
<section><h2>Proof confidence by family</h2><div class="scroll"><table>
<thead><tr><th>Family</th><th>Raw N</th><th>Independent N</th><th>Holdout N</th><th>Net ROI</th><th>Compounded NAV</th><th>Expected log growth</th><th>LCB log growth</th><th>ES20</th><th>Max DD</th><th>Winner concentration</th><th>Top-1 removed</th><th>Top-3 removed</th><th>Latency sensitivity</th><th>Cost sensitivity</th><th>Stress performance</th><th>Promotion state</th><th>Allocation</th></tr></thead>
<tbody>{''.join(rows) if rows else '<tr><td colspan="18">No frozen-epoch outcomes yet.</td></tr>'}</tbody>
</table></div></section>
<section><h2>Execution mechanism diagnostics</h2>{''.join(stress_summary) if stress_summary else '<span class="note">No mechanism diagnostics available.</span>'}
<p class="note">Mechanism shocks are paper-to-live sensitivity diagnostics only. They do not change the frozen strategy, authorize signing, or imply live fills.</p></section>
</main></body></html>"""


__all__ = ["DASHBOARD_VERSION", "render_economic_dashboard"]
