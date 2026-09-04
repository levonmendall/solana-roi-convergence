from __future__ import annotations

import bisect
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .certification import wilson_lower
from .config import BASELINE
from .profit_first_entity_final import STARTING_PAPER_NAV_USD, UNIFIED_LANE


DELAY_STRESS_SECONDS = (1, 2, 5, 10, 20)
PRICE_MARK_TOLERANCE_SECONDS = 2.5


def _parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json(value: Any) -> dict[str, Any]:
    try:
        row = json.loads(str(value or "{}"))
        return row if isinstance(row, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _decision_value(raw: Any) -> str:
    value = _json(raw).get("decision")
    text = str(value or "").lower()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text


def _latency_band(seconds: float) -> str:
    if seconds <= 2.0:
        return "<=2s"
    if seconds <= 5.0:
        return "2-5s"
    if seconds <= 10.0:
        return "5-10s"
    if seconds <= 20.0:
        return "10-20s"
    return ">20s"


def _profit_factor(values: list[float]) -> float:
    gains = sum(max(0.0, value) for value in values)
    losses = -sum(min(0.0, value) for value in values)
    if losses > 0.0:
        return gains / losses
    return math.inf if gains > 0.0 else 0.0


def _geometric_growth(rows: list[dict[str, Any]], *, return_key: str) -> float:
    growth = 1.0
    for row in rows:
        fraction = max(0.0, float(row.get("position_fraction") or 0.0))
        result = float(row.get(return_key) or 0.0)
        terminal = 1.0 + fraction * result
        if terminal <= 0.0:
            return -1.0
        growth *= terminal
    return growth - 1.0


def _pnl_usd(row: dict[str, Any], *, return_key: str) -> float:
    return STARTING_PAPER_NAV_USD * max(0.0, float(row.get("position_fraction") or 0.0)) * float(
        row.get(return_key) or 0.0
    )


def _sum_without_best_group(rows: list[dict[str, Any]], key: str, *, pnl_key: str) -> float:
    by_group: dict[str, float] = defaultdict(float)
    for row in rows:
        by_group[str(row.get(key) or "unknown")] += float(row.get(pnl_key) or 0.0)
    total = sum(by_group.values())
    best = max(by_group.values(), default=0.0)
    return total - max(0.0, best)


def _remove_top_winners(rows: list[dict[str, Any]], fraction: float, *, pnl_key: str) -> float:
    if not rows:
        return 0.0
    ordered = sorted((float(row.get(pnl_key) or 0.0) for row in rows), reverse=True)
    remove = max(1, math.ceil(len(ordered) * fraction))
    return sum(ordered[remove:])


class V4ExecutionTransferCertification:
    """Read-only certification over release-bound final-v4 prospective evidence.

    No transaction is signed or submitted. Transfer stress uses only evidence the
    paper system actually recorded: amount-specific entry cost, observed Jupiter
    fees, closed forward outcome, measured signal-to-entry latency and later price
    marks. Delayed marks are diagnostic price-path stress, not asserted live fills.
    """

    def __init__(self, store: Any):
        self.store = store

    def _latest_epoch(self) -> str | None:
        try:
            with self.store._lock:
                row = self.store.db.execute(
                    "SELECT epoch_id FROM profit_first_final_epochs ORDER BY started_at DESC, rowid DESC LIMIT 1"
                ).fetchone()
        except Exception:
            return None
        return str(row[0]) if row is not None else None

    def _rows(self, epoch_id: str) -> list[dict[str, Any]]:
        try:
            with self.store._lock:
                rows = self.store.db.execute(
                    "SELECT o.source_signature,o.token_mint,o.trigger_wallet,o.lane,o.entry_observed_at,o.exit_observed_at,"
                    "o.signal_to_entry_seconds,o.position_fraction,o.entry_cost_sol,o.exit_net_sol,o.net_return,o.exit_reason,"
                    "t.received_at,t.regime,t.decision_json,t.opportunity_json,t.quote_input_lamports,t.entry_fee_lamports,"
                    "t.entry_all_in_price_sol,t.round_trip_cost_fraction,t.quote_latency_ms,t.entry_executable,t.exit_executable "
                    "FROM profit_first_final_outcomes o JOIN profit_first_final_trials t ON "
                    "t.epoch_id=o.epoch_id AND t.source_signature=o.source_signature AND t.lane=o.lane "
                    "WHERE o.epoch_id=? AND o.evidence_phase='forward' ORDER BY o.id",
                    (epoch_id,),
                ).fetchall()
        except Exception:
            return []
        result: list[dict[str, Any]] = []
        for source in rows:
            row = dict(source)
            opportunity = _json(row.get("opportunity_json"))
            row["trigger_entity"] = str(opportunity.get("trigger_entity") or row.get("trigger_wallet") or "unknown")
            row["selected_by_policy"] = _decision_value(row.get("decision_json")) == "paper_enter"
            row["latency_band"] = _latency_band(float(row.get("signal_to_entry_seconds") or 0.0))
            result.append(row)
        return result

    def _marks_for_token(self, token: str, start: datetime, end: datetime) -> tuple[list[datetime], list[float]]:
        try:
            with self.store._lock:
                rows = self.store.db.execute(
                    "SELECT received_at,price_sol FROM price_marks WHERE token_mint=? AND received_at>=? AND received_at<=? "
                    "ORDER BY received_at,id",
                    (token, start.isoformat(), end.isoformat()),
                ).fetchall()
        except Exception:
            return [], []
        times: list[datetime] = []
        prices: list[float] = []
        for row in rows:
            try:
                price = float(row["price_sol"])
                if price <= 0:
                    continue
                times.append(_parse_dt(row["received_at"]))
                prices.append(price)
            except Exception:
                continue
        return times, prices

    def _attach_transfer_stress(self, rows: list[dict[str, Any]]) -> None:
        windows: dict[str, tuple[datetime, datetime]] = {}
        for row in rows:
            try:
                start = _parse_dt(row["received_at"])
            except Exception:
                continue
            end = start + timedelta(seconds=max(DELAY_STRESS_SECONDS) + PRICE_MARK_TOLERANCE_SECONDS)
            token = str(row["token_mint"])
            current = windows.get(token)
            if current is None:
                windows[token] = (start, end)
            else:
                windows[token] = (min(current[0], start), max(current[1], end))
        marks = {token: self._marks_for_token(token, start, end) for token, (start, end) in windows.items()}

        for row in rows:
            entry_cost = float(row.get("entry_cost_sol") or 0.0)
            fee_lamports = int(row.get("entry_fee_lamports") or 0)
            fee_sol = fee_lamports / 1_000_000_000.0
            failed_attempt_fraction = fee_sol / entry_cost if entry_cost > 0.0 else 0.0
            observed_return = float(row.get("net_return") or 0.0)
            row["failed_attempt_fee_fraction"] = max(0.0, failed_attempt_fraction)
            row["execution_stressed_return"] = observed_return - max(0.0, failed_attempt_fraction)
            row["stress_input_complete"] = bool(
                entry_cost > 0.0
                and int(row.get("quote_input_lamports") or 0) > 0
                and row.get("entry_all_in_price_sol") is not None
                and bool(row.get("entry_executable"))
                and bool(row.get("exit_executable"))
                and float(row.get("signal_to_entry_seconds") or 0.0) <= 20.0
            )
            row["observed_pnl_usd"] = _pnl_usd(row, return_key="net_return")
            row["execution_stressed_pnl_usd"] = _pnl_usd(row, return_key="execution_stressed_return")

            delayed: dict[str, Any] = {}
            try:
                base_time = _parse_dt(row["received_at"])
                base_price = float(row.get("entry_all_in_price_sol") or 0.0)
            except Exception:
                base_time, base_price = datetime.min.replace(tzinfo=timezone.utc), 0.0
            token_times, token_prices = marks.get(str(row.get("token_mint") or ""), ([], []))
            for delay in DELAY_STRESS_SECONDS:
                key = f"{delay}s"
                target = base_time + timedelta(seconds=delay)
                index = bisect.bisect_left(token_times, target)
                if index >= len(token_times) or token_times[index] > target + timedelta(seconds=PRICE_MARK_TOLERANCE_SECONDS) or base_price <= 0.0:
                    delayed[key] = {"available": False, "promotion_authority": False}
                    continue
                later_price = token_prices[index]
                adverse_multiplier = max(1.0, later_price / base_price)
                stressed = (1.0 + observed_return) / adverse_multiplier - 1.0 - max(0.0, failed_attempt_fraction)
                delayed[key] = {
                    "available": True,
                    "mark_received_at": token_times[index].isoformat(),
                    "price_sol": later_price,
                    "adverse_entry_multiplier": adverse_multiplier,
                    "stressed_return": stressed,
                    "promotion_authority": False,
                    "interpretation": "diagnostic price-path landing stress; not an asserted executable fill",
                }
            row["delayed_entry_stress"] = delayed

    @staticmethod
    def _group_report(rows: list[dict[str, Any]], key: str, *, return_key: str) -> dict[str, Any]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[str(row.get(key) or "unknown")].append(row)
        return {
            group: {
                "samples": len(items),
                "net_pnl_usd": sum(_pnl_usd(item, return_key=return_key) for item in items),
                "mean_return": sum(float(item.get(return_key) or 0.0) for item in items) / len(items),
                "profit_factor": _profit_factor([_pnl_usd(item, return_key=return_key) for item in items]),
            }
            for group, items in sorted(groups.items())
        }

    def status(self, epoch_id: str | None = None) -> dict[str, Any]:
        epoch = epoch_id or self._latest_epoch()
        if epoch is None:
            return {
                "status": "collecting_or_failed",
                "certified": False,
                "blockers": ["no_release_bound_v4_epoch"],
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }

        all_rows = self._rows(epoch)
        self._attach_transfer_stress(all_rows)
        unified = [row for row in all_rows if str(row.get("lane")) == UNIFIED_LANE]
        selected = [row for row in unified if bool(row.get("selected_by_policy"))]
        valid_stress = [row for row in selected if bool(row.get("stress_input_complete"))]

        observed_pnls = [float(row["observed_pnl_usd"]) for row in selected]
        stressed_pnls = [float(row["execution_stressed_pnl_usd"]) for row in valid_stress]
        total_pnl = sum(observed_pnls)
        stressed_total = sum(stressed_pnls)
        hits = sum(float(row.get("net_return") or 0.0) > 0.0 for row in selected)
        lower = wilson_lower(hits, len(selected))
        unique_tokens = len({str(row.get("token_mint")) for row in selected})
        best_trade = max(observed_pnls, default=0.0)
        pnl_ex_best_trade = total_pnl - max(0.0, best_trade)
        pnl_ex_best_wallet = _sum_without_best_group(selected, "trigger_wallet", pnl_key="observed_pnl_usd")
        pnl_ex_best_entity = _sum_without_best_group(selected, "trigger_entity", pnl_key="observed_pnl_usd")
        pnl_ex_best_token = _sum_without_best_group(selected, "token_mint", pnl_key="observed_pnl_usd")
        pnl_ex_top_1 = _remove_top_winners(selected, 0.01, pnl_key="observed_pnl_usd")
        pnl_ex_top_5 = _remove_top_winners(selected, 0.05, pnl_key="observed_pnl_usd")
        pnl_ex_top_10 = _remove_top_winners(selected, 0.10, pnl_key="observed_pnl_usd")
        profit_factor = _profit_factor(observed_pnls)
        stressed_profit_factor = _profit_factor(stressed_pnls)
        growth = _geometric_growth(selected, return_key="net_return")
        stressed_growth = _geometric_growth(valid_stress, return_key="execution_stressed_return")

        blockers: list[str] = []
        if len(selected) < BASELINE.certification_min_closed_trades:
            blockers.append("minimum_300_policy_selected_closed_episodes_not_met")
        if unique_tokens < BASELINE.certification_min_closed_trades:
            blockers.append("minimum_300_independent_tokens_not_met")
        if total_pnl <= 0.0:
            blockers.append("aggregate_net_pnl_not_positive")
        if growth <= 0.0:
            blockers.append("geometric_growth_not_positive")
        if profit_factor <= 1.0:
            blockers.append("profit_factor_not_above_one")
        if lower <= BASELINE.certification_break_even_hit_rate:
            blockers.append("wilson_lower_hit_rate_not_above_break_even")
        if pnl_ex_best_trade <= 0.0:
            blockers.append("profitability_depends_on_best_trade")
        if pnl_ex_best_wallet <= 0.0:
            blockers.append("profitability_depends_on_best_wallet")
        if pnl_ex_best_entity <= 0.0:
            blockers.append("profitability_depends_on_best_entity")
        if pnl_ex_best_token <= 0.0:
            blockers.append("profitability_depends_on_best_token")
        if pnl_ex_top_5 <= 0.0:
            blockers.append("profitability_depends_on_top_five_percent_winners")
        if selected and len(valid_stress) != len(selected):
            blockers.append("execution_transfer_evidence_incomplete")
        if stressed_total <= 0.0:
            blockers.append("execution_stressed_net_pnl_not_positive")
        if stressed_growth <= 0.0:
            blockers.append("execution_stressed_geometric_growth_not_positive")
        if stressed_profit_factor <= 1.0:
            blockers.append("execution_stressed_profit_factor_not_above_one")
        if any(float(row.get("signal_to_entry_seconds") or 0.0) > 20.0 for row in selected):
            blockers.append("policy_selected_episode_exceeds_20_second_entry_ceiling")

        delayed: dict[str, Any] = {}
        for delay in DELAY_STRESS_SECONDS:
            key = f"{delay}s"
            values = [
                float(row["delayed_entry_stress"][key]["stressed_return"])
                for row in selected
                if bool(row.get("delayed_entry_stress", {}).get(key, {}).get("available"))
            ]
            delayed[key] = {
                "sample_count": len(values),
                "coverage_fraction": len(values) / len(selected) if selected else 0.0,
                "mean_stressed_return": sum(values) / len(values) if values else None,
                "positive_fraction": sum(value > 0.0 for value in values) / len(values) if values else None,
                "hard_certification_authority": False,
            }

        return {
            "status": "certified" if not blockers else "collecting_or_failed",
            "certified": not blockers,
            "evidence_epoch_id": epoch,
            "strategy_lane": UNIFIED_LANE,
            "research_forward_outcome_rows": len(unified),
            "policy_selected_closed_episodes": len(selected),
            "required_closed_episodes": BASELINE.certification_min_closed_trades,
            "unique_tokens": unique_tokens,
            "hit_rate": hits / len(selected) if selected else 0.0,
            "hit_rate_wilson_lower": lower,
            "break_even_hit_rate": BASELINE.certification_break_even_hit_rate,
            "net_pnl_usd": total_pnl,
            "return_on_starting_500_usd": total_pnl / STARTING_PAPER_NAV_USD,
            "geometric_growth": growth,
            "profit_factor": profit_factor,
            "execution_transfer": {
                "complete_samples": len(valid_stress),
                "coverage_fraction": len(valid_stress) / len(selected) if selected else 0.0,
                "stress_model": "observed_forward_return_plus_one_additional_observed_entry-fee-equivalent_failed-attempt",
                "execution_stressed_net_pnl_usd": stressed_total,
                "execution_stressed_geometric_growth": stressed_growth,
                "execution_stressed_profit_factor": stressed_profit_factor,
                "delayed_entry_price_path_stress": delayed,
                "delayed_marks_have_promotion_authority": False,
                "unsigned_simulation_required_by_entry_trial": True,
                "live_fill_claimed": False,
            },
            "robustness": {
                "pnl_ex_best_trade_usd": pnl_ex_best_trade,
                "pnl_ex_best_wallet_usd": pnl_ex_best_wallet,
                "pnl_ex_best_entity_usd": pnl_ex_best_entity,
                "pnl_ex_best_token_usd": pnl_ex_best_token,
                "pnl_ex_top_1_percent_winners_usd": pnl_ex_top_1,
                "pnl_ex_top_5_percent_winners_usd": pnl_ex_top_5,
                "pnl_ex_top_10_percent_winners_usd": pnl_ex_top_10,
            },
            "performance_by_latency_band": self._group_report(selected, "latency_band", return_key="net_return"),
            "performance_by_regime": self._group_report(selected, "regime", return_key="net_return"),
            "performance_by_trigger_entity": self._group_report(selected, "trigger_entity", return_key="net_return"),
            "blockers": blockers,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "historical_promotion_authority": False,
            "source_wallet_fill_credit": False,
        }


__all__ = [
    "DELAY_STRESS_SECONDS",
    "PRICE_MARK_TOLERANCE_SECONDS",
    "V4ExecutionTransferCertification",
]
