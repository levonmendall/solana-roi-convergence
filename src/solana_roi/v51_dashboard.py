from __future__ import annotations

from html import escape
from typing import Any

DASHBOARD_VERSION = "v51-economic-dashboard-v1"


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


def render_economic_dashboard(
    certification: dict[str, Any],
    coverage: dict[str, Any],
    mechanism_stress: dict[str, Any] | None = None,
) -> str:
    families = certification.get("families") or {}
    ranking = list(certification.get("research_family_ranking") or families.keys())
    weights = certification.get("paper_allocation_weights") or {}
    rows: list[str] = []
    for family in ranking:
        data = families.get(family) or {}
        robust = data.get("robust_profile") or {}
        rows.append(
            "<tr>"
            f"<td><strong>{escape(str(family))}</strong></td>"
            f"<td>{int(data.get('closed_outcome_count') or 0)}</td>"
            f"<td>{int(data.get('independent_event_count') or 0)}</td>"
            f"<td>{_num(data.get('compounded_nav_multiple'))}</td>"
            f"<td>{_num(robust.get('best_expected_log_growth'), 6)}</td>"
            f"<td>{_pct(robust.get('expected_shortfall_20'))}</td>"
            f"<td>{_pct(robust.get('max_drawdown_at_best_fraction'))}</td>"
            f"<td>{_pct(weights.get(family, 0.0))}</td>"
            f"<td>{escape(_state(data))}</td>"
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

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ROI Convergence v5.1 Economic Certification</title>
<style>
:root{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111;background:#f6f7f8}}
body{{margin:0;padding:24px}} main{{max-width:1180px;margin:auto}}
h1{{margin:0 0 6px;font-size:28px}} .sub{{color:#5b6470;margin-bottom:22px}}
.badge{{display:inline-block;border:1px solid #222;border-radius:999px;padding:4px 9px;font-size:12px;font-weight:700;margin-right:6px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:18px 0}}
.card{{background:white;border:1px solid #dde1e5;border-radius:12px;padding:16px}} .k{{font-size:12px;color:#6b7280}} .v{{font-size:24px;font-weight:700;margin-top:5px}}
.ok{{color:#08783e}} .warn{{color:#a15c00}} table{{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden}}
th,td{{padding:10px 9px;border-bottom:1px solid #eceff2;text-align:right;font-size:13px}} th:first-child,td:first-child{{text-align:left}} th{{background:#fafafa;color:#4b5563}}
section{{margin-top:22px}} .chip{{display:inline-block;background:white;border:1px solid #d7dce1;border-radius:999px;padding:5px 9px;margin:4px 4px 0 0;font-size:12px}}
.note{{font-size:12px;color:#6b7280;line-height:1.5}} .scroll{{overflow-x:auto}}
</style>
</head>
<body><main>
<h1>v5.1 Economic Certification</h1>
<div class="sub"><span class="badge">PAPER ONLY</span><span class="badge">NO LIVE-MONEY AUTHORITY</span>Dashboard {escape(DASHBOARD_VERSION)}</div>
<div class="note">Authority <strong>{authority}</strong><br>Frozen evidence epoch <strong>{epoch}</strong></div>
<div class="grid">
<div class="card"><div class="k">Closed outcomes</div><div class="v">{closed}</div></div>
<div class="card"><div class="k">Paper cash weight</div><div class="v">{cash}</div></div>
<div class="card"><div class="k">Candidate coverage</div><div class="v {coverage_class}">{coverage_text}</div></div>
<div class="card"><div class="k">Coverage debt</div><div class="v">{debt}</div></div>
</div>
<section><h2>Forward capital efficiency by family</h2><div class="scroll"><table>
<thead><tr><th>Family</th><th>Closed N</th><th>Independent N</th><th>NAV multiple</th><th>Expected log growth</th><th>ES20</th><th>Drawdown</th><th>Allocation</th><th>State</th></tr></thead>
<tbody>{''.join(rows) if rows else '<tr><td colspan="9">No frozen-epoch outcomes yet.</td></tr>'}</tbody>
</table></div></section>
<section><h2>Execution mechanism diagnostics</h2>{''.join(stress_summary) if stress_summary else '<span class="note">No mechanism diagnostics available.</span>'}
<p class="note">Mechanism shocks are paper-to-live sensitivity diagnostics only. They do not change the frozen strategy, authorize signing, or imply live fills.</p></section>
</main></body></html>"""


__all__ = ["DASHBOARD_VERSION", "render_economic_dashboard"]
