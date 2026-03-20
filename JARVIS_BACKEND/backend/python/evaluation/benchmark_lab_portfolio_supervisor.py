from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_from_ts(value: float) -> str:
    try:
        numeric = float(value)
    except Exception:  # noqa: BLE001
        return ""
    if numeric <= 0:
        return ""
    return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()


class DesktopBenchmarkLabPortfolioSupervisor:
    def __init__(
        self,
        *,
        state_path: str = "data/desktop_benchmark_portfolio_supervisor.json",
        enabled: bool = False,
        interval_s: float = 300.0,
        max_portfolios: int = 2,
        max_waves_per_portfolio: int = 2,
        max_programs_per_portfolio: int = 3,
        max_campaigns_per_program: int = 3,
        max_sweeps_per_campaign: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        adaptive_budgeting: bool = False,
        adaptive_goal: str = "",
        portfolio_status: str = "",
        pack: str = "",
        app_name: str = "",
    ) -> None:
        self._store = LocalStore(state_path)
        self._lock = threading.RLock()
        self._wakeup = threading.Event()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._execute_callback: Optional[Callable[..., Dict[str, Any]]] = None
        self._config = self._default_config(
            enabled=enabled,
            interval_s=interval_s,
            max_portfolios=max_portfolios,
            max_waves_per_portfolio=max_waves_per_portfolio,
            max_programs_per_portfolio=max_programs_per_portfolio,
            max_campaigns_per_program=max_campaigns_per_program,
            max_sweeps_per_campaign=max_sweeps_per_campaign,
            max_sessions=max_sessions,
            max_replays_per_session=max_replays_per_session,
            history_limit=history_limit,
            adaptive_budgeting=adaptive_budgeting,
            adaptive_goal=adaptive_goal,
            portfolio_status=portfolio_status,
            pack=pack,
            app_name=app_name,
        )
        self._runtime = self._default_runtime()
        self._history: list[Dict[str, Any]] = []
        self._load()

    def start(self, execute_callback: Callable[..., Dict[str, Any]]) -> None:
        with self._lock:
            self._execute_callback = execute_callback
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._wakeup.clear()
            self._thread = threading.Thread(
                target=self._worker,
                name="desktop-benchmark-portfolio-supervisor",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
        if thread is None:
            return
        self._stop_event.set()
        self._wakeup.set()
        thread.join(timeout=5)
        with self._lock:
            self._thread = None

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return self._public_status_locked()

    def history(
        self,
        *,
        limit: int = 12,
        status: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            normalized_limit = self._coerce_int(limit, minimum=1, maximum=128, default=12)
            normalized_status = str(status or "").strip().lower()
            normalized_source = str(source or "").strip().lower()
            items = [
                copy.deepcopy(item)
                for item in self._history
                if isinstance(item, dict)
                and (
                    not normalized_status
                    or str(item.get("status", "") or "").strip().lower() == normalized_status
                )
                and (
                    not normalized_source
                    or str(item.get("source", "") or "").strip().lower() == normalized_source
                )
            ]
            limited = items[-normalized_limit:]
            latest = dict(limited[-1]) if limited else {}
            status_counts: Dict[str, int] = {}
            source_counts: Dict[str, int] = {}
            trend_direction_counts: Dict[str, int] = {}
            wave_stop_reason_counts: Dict[str, int] = {}
            budget_profile_counts: Dict[str, int] = {}
            executed_portfolio_total = 0
            executed_wave_total = 0
            executed_program_total = 0
            executed_campaign_total = 0
            executed_sweep_total = 0
            stable_portfolio_total = 0
            stable_campaign_total = 0
            regression_campaign_total = 0
            adaptive_portfolio_total = 0
            planned_wave_budget_total = 0
            planned_program_budget_total = 0
            campaign_stop_reason_counts: Dict[str, int] = {}
            for item in items:
                self._increment_count(status_counts, str(item.get("status", "") or "unknown"))
                self._increment_count(source_counts, str(item.get("source", "") or "unknown"))
                executed_portfolio_total += self._coerce_int(
                    item.get("executed_portfolio_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                executed_wave_total += self._coerce_int(
                    item.get("executed_wave_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                executed_program_total += self._coerce_int(
                    item.get("executed_program_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                executed_campaign_total += self._coerce_int(
                    item.get("executed_campaign_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                executed_sweep_total += self._coerce_int(
                    item.get("executed_sweep_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                stable_portfolio_total += self._coerce_int(
                    item.get("stable_portfolio_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                stable_campaign_total += self._coerce_int(
                    item.get("stable_campaign_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                regression_campaign_total += self._coerce_int(
                    item.get("regression_campaign_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                adaptive_portfolio_total += self._coerce_int(
                    item.get("adaptive_portfolio_count", 0), minimum=0, maximum=1_000_000, default=0
                )
                planned_wave_budget_total += self._coerce_int(
                    item.get("planned_wave_budget_total", 0), minimum=0, maximum=1_000_000, default=0
                )
                planned_program_budget_total += self._coerce_int(
                    item.get("planned_program_budget_total", 0), minimum=0, maximum=1_000_000, default=0
                )
                trend_counts = item.get("trend_direction_counts", {})
                if isinstance(trend_counts, dict):
                    for key, value in trend_counts.items():
                        clean = str(key or "").strip().lower() or "unknown"
                        trend_direction_counts[clean] = int(trend_direction_counts.get(clean, 0)) + self._coerce_int(
                            value, minimum=0, maximum=1_000_000, default=0
                        )
                stop_counts = item.get("wave_stop_reason_counts", {})
                if isinstance(stop_counts, dict):
                    for key, value in stop_counts.items():
                        clean = str(key or "").strip().lower() or "unknown"
                        wave_stop_reason_counts[clean] = int(wave_stop_reason_counts.get(clean, 0)) + self._coerce_int(
                            value, minimum=0, maximum=1_000_000, default=0
                        )
                campaign_stop_counts = item.get("campaign_stop_reason_counts", {})
                if isinstance(campaign_stop_counts, dict):
                    for key, value in campaign_stop_counts.items():
                        clean = str(key or "").strip().lower() or "unknown"
                        campaign_stop_reason_counts[clean] = int(
                            campaign_stop_reason_counts.get(clean, 0)
                        ) + self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                profile_counts = item.get("budget_profile_counts", {})
                if isinstance(profile_counts, dict):
                    for key, value in profile_counts.items():
                        clean = str(key or "").strip().lower() or "unknown"
                        budget_profile_counts[clean] = int(budget_profile_counts.get(clean, 0)) + self._coerce_int(
                            value, minimum=0, maximum=1_000_000, default=0
                        )
            return {
                "status": "success",
                "count": len(limited),
                "total": len(items),
                "limit": normalized_limit,
                "filters": {
                    "status": normalized_status,
                    "source": normalized_source,
                },
                "items": limited,
                "latest_run": latest,
                "summary": {
                    "status_counts": self._sorted_count_map(status_counts),
                    "source_counts": self._sorted_count_map(source_counts),
                    "executed_portfolio_total": executed_portfolio_total,
                    "executed_wave_total": executed_wave_total,
                    "executed_program_total": executed_program_total,
                    "executed_campaign_total": executed_campaign_total,
                    "executed_sweep_total": executed_sweep_total,
                    "stable_portfolio_total": stable_portfolio_total,
                    "stable_campaign_total": stable_campaign_total,
                    "regression_campaign_total": regression_campaign_total,
                    "adaptive_portfolio_total": adaptive_portfolio_total,
                    "planned_wave_budget_total": planned_wave_budget_total,
                    "planned_program_budget_total": planned_program_budget_total,
                    "budget_profile_counts": self._sorted_count_map(budget_profile_counts),
                    "trend_direction_counts": self._sorted_count_map(trend_direction_counts),
                    "campaign_stop_reason_counts": self._sorted_count_map(campaign_stop_reason_counts),
                    "wave_stop_reason_counts": self._sorted_count_map(wave_stop_reason_counts),
                },
            }

    def reset_history(
        self,
        *,
        status: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            normalized_status = str(status or "").strip().lower()
            normalized_source = str(source or "").strip().lower()
            before = len(self._history)
            if normalized_status or normalized_source:
                self._history = [
                    item
                    for item in self._history
                    if not (
                        isinstance(item, dict)
                        and (
                            not normalized_status
                            or str(item.get("status", "") or "").strip().lower() == normalized_status
                        )
                        and (
                            not normalized_source
                            or str(item.get("source", "") or "").strip().lower() == normalized_source
                        )
                    )
                ]
            else:
                self._history = []
            removed = max(0, before - len(self._history))
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            return {
                "status": "success",
                "removed_count": removed,
                "remaining_count": len(self._history),
                "filters": {
                    "status": normalized_status,
                    "source": normalized_source,
                },
                "latest_run": copy.deepcopy(self._history[-1]) if self._history else {},
            }

    def configure(
        self,
        *,
        enabled: Optional[bool] = None,
        interval_s: Optional[float] = None,
        max_portfolios: Optional[int] = None,
        max_waves_per_portfolio: Optional[int] = None,
        max_programs_per_portfolio: Optional[int] = None,
        max_campaigns_per_program: Optional[int] = None,
        max_sweeps_per_campaign: Optional[int] = None,
        max_sessions: Optional[int] = None,
        max_replays_per_session: Optional[int] = None,
        history_limit: Optional[int] = None,
        adaptive_budgeting: Optional[bool] = None,
        adaptive_goal: Optional[str] = None,
        portfolio_status: Optional[str] = None,
        pack: Optional[str] = None,
        app_name: Optional[str] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            if enabled is not None:
                self._config["enabled"] = bool(enabled)
            if interval_s is not None:
                self._config["interval_s"] = self._coerce_float(interval_s, minimum=5.0, maximum=3600.0, default=300.0)
            if max_portfolios is not None:
                self._config["max_portfolios"] = self._coerce_int(max_portfolios, minimum=1, maximum=32, default=2)
            if max_waves_per_portfolio is not None:
                self._config["max_waves_per_portfolio"] = self._coerce_int(
                    max_waves_per_portfolio, minimum=1, maximum=8, default=2
                )
            if max_programs_per_portfolio is not None:
                self._config["max_programs_per_portfolio"] = self._coerce_int(
                    max_programs_per_portfolio, minimum=1, maximum=8, default=3
                )
            if max_campaigns_per_program is not None:
                self._config["max_campaigns_per_program"] = self._coerce_int(
                    max_campaigns_per_program, minimum=1, maximum=8, default=3
                )
            if max_sweeps_per_campaign is not None:
                self._config["max_sweeps_per_campaign"] = self._coerce_int(
                    max_sweeps_per_campaign, minimum=1, maximum=8, default=2
                )
            if max_sessions is not None:
                self._config["max_sessions"] = self._coerce_int(max_sessions, minimum=1, maximum=8, default=3)
            if max_replays_per_session is not None:
                self._config["max_replays_per_session"] = self._coerce_int(
                    max_replays_per_session, minimum=1, maximum=8, default=2
                )
            if history_limit is not None:
                self._config["history_limit"] = self._coerce_int(history_limit, minimum=1, maximum=64, default=8)
            if adaptive_budgeting is not None:
                self._config["adaptive_budgeting"] = bool(adaptive_budgeting)
            if adaptive_goal is not None:
                self._config["adaptive_goal"] = str(adaptive_goal or "").strip().lower()
            if portfolio_status is not None:
                self._config["portfolio_status"] = str(portfolio_status or "").strip()
            if pack is not None:
                self._config["pack"] = str(pack or "").strip()
            if app_name is not None:
                self._config["app_name"] = str(app_name or "").strip()
            self._runtime["last_config_source"] = str(source or "manual").strip().lower() or "manual"
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            status = self._public_status_locked()
        self._wakeup.set()
        return status

    def trigger_now(
        self,
        *,
        source: str = "manual",
        max_portfolios: Optional[int] = None,
        max_waves_per_portfolio: Optional[int] = None,
        max_programs_per_portfolio: Optional[int] = None,
        max_campaigns_per_program: Optional[int] = None,
        max_sweeps_per_campaign: Optional[int] = None,
        max_sessions: Optional[int] = None,
        max_replays_per_session: Optional[int] = None,
        history_limit: Optional[int] = None,
        adaptive_budgeting: Optional[bool] = None,
        adaptive_goal: Optional[str] = None,
        portfolio_status: Optional[str] = None,
        pack: Optional[str] = None,
        app_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        overrides = {
            "max_portfolios": max_portfolios,
            "max_waves_per_portfolio": max_waves_per_portfolio,
            "max_programs_per_portfolio": max_programs_per_portfolio,
            "max_campaigns_per_program": max_campaigns_per_program,
            "max_sweeps_per_campaign": max_sweeps_per_campaign,
            "max_sessions": max_sessions,
            "max_replays_per_session": max_replays_per_session,
            "history_limit": history_limit,
            "adaptive_budgeting": adaptive_budgeting,
            "adaptive_goal": adaptive_goal,
            "portfolio_status": portfolio_status,
            "pack": pack,
            "app_name": app_name,
        }
        return self._execute_once(source=str(source or "manual").strip().lower() or "manual", overrides=overrides)

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            wait_s = self._compute_wait_s()
            if self._wakeup.wait(timeout=wait_s):
                self._wakeup.clear()
                continue
            if self._stop_event.is_set():
                break
            with self._lock:
                if not bool(self._config.get("enabled", False)):
                    continue
            self._execute_once(source="daemon")

    def _compute_wait_s(self) -> float:
        with self._lock:
            if not bool(self._config.get("enabled", False)):
                return 1.0
            interval_s = self._coerce_float(self._config.get("interval_s", 300.0), minimum=5.0, maximum=3600.0, default=300.0)
            last_tick_ts = float(self._runtime.get("last_tick_ts", 0.0) or 0.0)
            now = time.time()
            if last_tick_ts <= 0:
                return 0.0
            return max(0.0, (last_tick_ts + interval_s) - now)

    def _execute_once(self, *, source: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            if bool(self._runtime.get("inflight", False)):
                return {
                    "status": "busy",
                    "message": "benchmark portfolio daemon tick already in progress",
                    "supervisor": self._public_status_locked(),
                }
            callback = self._execute_callback
            if callback is None:
                return {
                    "status": "unavailable",
                    "message": "benchmark portfolio daemon callback unavailable",
                    "supervisor": self._public_status_locked(),
                }
            self._runtime["inflight"] = True
            self._runtime["last_trigger_source"] = str(source or "manual").strip().lower() or "manual"
            self._runtime["last_trigger_at"] = _utc_now_iso()
            self._persist_locked()
            config = dict(self._config)
        effective = self._apply_overrides(config, overrides or {})
        start_ts = time.time()
        try:
            result = callback(
                max_portfolios=self._coerce_int(effective.get("max_portfolios", 2), minimum=1, maximum=32, default=2),
                max_waves_per_portfolio=self._coerce_int(
                    effective.get("max_waves_per_portfolio", 2), minimum=1, maximum=8, default=2
                ),
                max_programs_per_portfolio=self._coerce_int(
                    effective.get("max_programs_per_portfolio", 3), minimum=1, maximum=8, default=3
                ),
                max_campaigns_per_program=self._coerce_int(
                    effective.get("max_campaigns_per_program", 3), minimum=1, maximum=8, default=3
                ),
                max_sweeps_per_campaign=self._coerce_int(
                    effective.get("max_sweeps_per_campaign", 2), minimum=1, maximum=8, default=2
                ),
                max_sessions=self._coerce_int(effective.get("max_sessions", 3), minimum=1, maximum=8, default=3),
                max_replays_per_session=self._coerce_int(
                    effective.get("max_replays_per_session", 2), minimum=1, maximum=8, default=2
                ),
                history_limit=self._coerce_int(effective.get("history_limit", 8), minimum=1, maximum=64, default=8),
                adaptive_budgeting=bool(effective.get("adaptive_budgeting", False)),
                adaptive_goal=str(effective.get("adaptive_goal", "") or "").strip().lower(),
                portfolio_status=str(effective.get("portfolio_status", "") or "").strip(),
                pack=str(effective.get("pack", "") or "").strip(),
                app_name=str(effective.get("app_name", "") or "").strip(),
                trigger_source=str(source or "manual").strip().lower() or "manual",
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "error", "message": str(exc)}
        duration_ms = max(0.0, (time.time() - start_ts) * 1000.0)
        result_payload = dict(result) if isinstance(result, dict) else {"status": "error", "message": "invalid benchmark portfolio supervisor result"}
        with self._lock:
            self._runtime["inflight"] = False
            self._runtime["last_tick_ts"] = time.time()
            self._runtime["last_tick_at"] = _iso_from_ts(self._runtime["last_tick_ts"])
            self._runtime["last_duration_ms"] = round(duration_ms, 2)
            self._runtime["last_result_status"] = str(result_payload.get("status", "") or "").strip().lower()
            self._runtime["last_result_message"] = str(result_payload.get("message", "") or "").strip()
            self._runtime["last_summary"] = self._result_summary(result_payload)
            self._runtime["run_count"] = self._coerce_int(self._runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
            counter_key = "auto_trigger_count" if source == "daemon" else "manual_trigger_count"
            self._runtime[counter_key] = self._coerce_int(
                self._runtime.get(counter_key, 0), minimum=0, maximum=1_000_000, default=0
            ) + 1
            if str(result_payload.get("status", "") or "").strip().lower() == "error":
                self._runtime["last_error_at"] = self._runtime["last_tick_at"]
                self._runtime["consecutive_error_count"] = self._coerce_int(
                    self._runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0
                ) + 1
            else:
                self._runtime["last_success_at"] = self._runtime["last_tick_at"]
                self._runtime["consecutive_error_count"] = 0
            self._append_history_locked(
                source=source,
                duration_ms=duration_ms,
                effective=effective,
                result_payload=result_payload,
            )
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            status = self._public_status_locked()
        return {
            "status": str(result_payload.get("status", "") or "").strip().lower() or "success",
            "message": str(result_payload.get("message", "") or "").strip(),
            "result": copy.deepcopy(result_payload),
            "supervisor": status,
        }

    def _public_status_locked(self) -> Dict[str, Any]:
        interval_s = self._coerce_float(self._config.get("interval_s", 300.0), minimum=5.0, maximum=3600.0, default=300.0)
        last_tick_ts = float(self._runtime.get("last_tick_ts", 0.0) or 0.0)
        next_due_at = _iso_from_ts(last_tick_ts + interval_s) if bool(self._config.get("enabled", False)) and last_tick_ts > 0 else ""
        latest_history_run = copy.deepcopy(self._history[-1]) if self._history else {}
        return {
            "status": "success",
            "active": bool(self._thread and self._thread.is_alive()),
            "enabled": bool(self._config.get("enabled", False)),
            "inflight": bool(self._runtime.get("inflight", False)),
            "interval_s": interval_s,
            "max_portfolios": self._coerce_int(self._config.get("max_portfolios", 2), minimum=1, maximum=32, default=2),
            "max_waves_per_portfolio": self._coerce_int(
                self._config.get("max_waves_per_portfolio", 2), minimum=1, maximum=8, default=2
            ),
            "max_programs_per_portfolio": self._coerce_int(
                self._config.get("max_programs_per_portfolio", 3), minimum=1, maximum=8, default=3
            ),
            "max_campaigns_per_program": self._coerce_int(
                self._config.get("max_campaigns_per_program", 3), minimum=1, maximum=8, default=3
            ),
            "max_sweeps_per_campaign": self._coerce_int(
                self._config.get("max_sweeps_per_campaign", 2), minimum=1, maximum=8, default=2
            ),
            "max_sessions": self._coerce_int(self._config.get("max_sessions", 3), minimum=1, maximum=8, default=3),
            "max_replays_per_session": self._coerce_int(
                self._config.get("max_replays_per_session", 2), minimum=1, maximum=8, default=2
            ),
            "history_limit": self._coerce_int(self._config.get("history_limit", 8), minimum=1, maximum=64, default=8),
            "adaptive_budgeting": bool(self._config.get("adaptive_budgeting", False)),
            "adaptive_goal": str(self._config.get("adaptive_goal", "") or "").strip(),
            "portfolio_status": str(self._config.get("portfolio_status", "") or "").strip(),
            "pack": str(self._config.get("pack", "") or "").strip(),
            "app_name": str(self._config.get("app_name", "") or "").strip(),
            "last_tick_at": str(self._runtime.get("last_tick_at", "") or "").strip(),
            "last_success_at": str(self._runtime.get("last_success_at", "") or "").strip(),
            "last_error_at": str(self._runtime.get("last_error_at", "") or "").strip(),
            "last_duration_ms": float(self._runtime.get("last_duration_ms", 0.0) or 0.0),
            "last_result_status": str(self._runtime.get("last_result_status", "") or "").strip(),
            "last_result_message": str(self._runtime.get("last_result_message", "") or "").strip(),
            "last_trigger_source": str(self._runtime.get("last_trigger_source", "") or "").strip(),
            "last_trigger_at": str(self._runtime.get("last_trigger_at", "") or "").strip(),
            "last_config_source": str(self._runtime.get("last_config_source", "") or "").strip(),
            "next_due_at": next_due_at,
            "run_count": self._coerce_int(self._runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "manual_trigger_count": self._coerce_int(self._runtime.get("manual_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "auto_trigger_count": self._coerce_int(self._runtime.get("auto_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "consecutive_error_count": self._coerce_int(self._runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0),
            "last_summary": copy.deepcopy(self._runtime.get("last_summary", {})) if isinstance(self._runtime.get("last_summary", {}), dict) else {},
            "history_count": len(self._history),
            "latest_history_run": latest_history_run,
            "updated_at": str(self._runtime.get("updated_at", "") or "").strip(),
        }

    def _load(self) -> None:
        config = self._store.get("config", {})
        runtime = self._store.get("runtime", {})
        history = self._store.get("history", [])
        if isinstance(config, dict):
            self._config.update(self._apply_overrides(self._config, config))
        if isinstance(runtime, dict):
            self._runtime.update(runtime)
        if isinstance(history, list):
            self._history = [
                copy.deepcopy(item)
                for item in history
                if isinstance(item, dict)
            ][-self._coerce_int(self._config.get("history_limit", 8), minimum=1, maximum=64, default=8):]

    def _persist_locked(self) -> None:
        self._store.set("config", self._config)
        self._store.set("runtime", self._runtime)
        self._store.set("history", self._history)

    @staticmethod
    def _result_summary(result_payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = result_payload if isinstance(result_payload, dict) else {}
        return {
            "status": str(payload.get("status", "") or "").strip().lower(),
            "message": str(payload.get("message", "") or "").strip(),
            "targeted_portfolio_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("targeted_portfolio_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_portfolio_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("executed_portfolio_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_wave_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("executed_wave_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_program_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("executed_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_campaign_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("executed_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_sweep_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("executed_sweep_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stable_portfolio_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("stable_portfolio_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "regression_portfolio_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("regression_portfolio_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stable_campaign_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("stable_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "regression_campaign_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("regression_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_program_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("pending_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "attention_program_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("attention_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_campaign_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("pending_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_session_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_app_target_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "long_horizon_pending_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "error_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("error_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "latest_portfolio_label": str(payload.get("latest_portfolio_label", "") or "").strip(),
            "auto_created_portfolio_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("auto_created_portfolio_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "adaptive_budgeting": bool(payload.get("adaptive_budgeting", False)),
            "adaptive_goal": str(payload.get("adaptive_goal", "") or "").strip().lower(),
            "adaptive_portfolio_count": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("adaptive_portfolio_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "planned_wave_budget_total": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("planned_wave_budget_total", 0), minimum=0, maximum=100_000, default=0
            ),
            "planned_program_budget_total": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                payload.get("planned_program_budget_total", 0), minimum=0, maximum=100_000, default=0
            ),
            "budget_profile_counts": copy.deepcopy(payload.get("budget_profile_counts", {}))
            if isinstance(payload.get("budget_profile_counts", {}), dict)
            else {},
            "wave_stop_reason_counts": copy.deepcopy(payload.get("wave_stop_reason_counts", {}))
            if isinstance(payload.get("wave_stop_reason_counts", {}), dict)
            else {},
            "campaign_stop_reason_counts": copy.deepcopy(payload.get("campaign_stop_reason_counts", {}))
            if isinstance(payload.get("campaign_stop_reason_counts", {}), dict)
            else {},
            "trend_direction_counts": copy.deepcopy(payload.get("trend_direction_counts", {}))
            if isinstance(payload.get("trend_direction_counts", {}), dict)
            else {},
        }

    @staticmethod
    def _default_config(
        *,
        enabled: bool,
        interval_s: float,
        max_portfolios: int,
        max_waves_per_portfolio: int,
        max_programs_per_portfolio: int,
        max_campaigns_per_program: int,
        max_sweeps_per_campaign: int,
        max_sessions: int,
        max_replays_per_session: int,
        history_limit: int,
        adaptive_budgeting: bool,
        adaptive_goal: str,
        portfolio_status: str,
        pack: str,
        app_name: str,
    ) -> Dict[str, Any]:
        return {
            "enabled": bool(enabled),
            "interval_s": DesktopBenchmarkLabPortfolioSupervisor._coerce_float(interval_s, minimum=5.0, maximum=3600.0, default=300.0),
            "max_portfolios": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(max_portfolios, minimum=1, maximum=32, default=2),
            "max_waves_per_portfolio": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                max_waves_per_portfolio, minimum=1, maximum=8, default=2
            ),
            "max_programs_per_portfolio": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                max_programs_per_portfolio, minimum=1, maximum=8, default=3
            ),
            "max_campaigns_per_program": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                max_campaigns_per_program, minimum=1, maximum=8, default=3
            ),
            "max_sweeps_per_campaign": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                max_sweeps_per_campaign, minimum=1, maximum=8, default=2
            ),
            "max_sessions": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(max_sessions, minimum=1, maximum=8, default=3),
            "max_replays_per_session": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(
                max_replays_per_session, minimum=1, maximum=8, default=2
            ),
            "history_limit": DesktopBenchmarkLabPortfolioSupervisor._coerce_int(history_limit, minimum=1, maximum=64, default=8),
            "adaptive_budgeting": bool(adaptive_budgeting),
            "adaptive_goal": str(adaptive_goal or "").strip().lower(),
            "portfolio_status": str(portfolio_status or "").strip(),
            "pack": str(pack or "").strip(),
            "app_name": str(app_name or "").strip(),
        }

    @staticmethod
    def _default_runtime() -> Dict[str, Any]:
        return {
            "inflight": False,
            "last_tick_ts": 0.0,
            "last_tick_at": "",
            "last_success_at": "",
            "last_error_at": "",
            "last_duration_ms": 0.0,
            "last_result_status": "",
            "last_result_message": "",
            "last_trigger_source": "",
            "last_trigger_at": "",
            "last_config_source": "",
            "run_count": 0,
            "manual_trigger_count": 0,
            "auto_trigger_count": 0,
            "consecutive_error_count": 0,
            "last_summary": {},
            "updated_at": "",
        }

    def _append_history_locked(
        self,
        *,
        source: str,
        duration_ms: float,
        effective: Dict[str, Any],
        result_payload: Dict[str, Any],
    ) -> None:
        summary = self._result_summary(result_payload)
        entry = {
            "recorded_at": str(self._runtime.get("last_tick_at", "") or "").strip() or _utc_now_iso(),
            "source": str(source or "manual").strip().lower() or "manual",
            "status": str(summary.get("status", "") or "").strip().lower(),
            "message": str(summary.get("message", "") or "").strip(),
            "duration_ms": round(float(duration_ms or 0.0), 2),
            "filters": {
                "portfolio_status": str(effective.get("portfolio_status", "") or "").strip(),
                "pack": str(effective.get("pack", "") or "").strip(),
                "app_name": str(effective.get("app_name", "") or "").strip(),
                "max_waves_per_portfolio": self._coerce_int(
                    effective.get("max_waves_per_portfolio", 2), minimum=1, maximum=8, default=2
                ),
                "max_programs_per_portfolio": self._coerce_int(
                    effective.get("max_programs_per_portfolio", 3), minimum=1, maximum=8, default=3
                ),
                "max_campaigns_per_program": self._coerce_int(
                    effective.get("max_campaigns_per_program", 3), minimum=1, maximum=8, default=3
                ),
                "max_sweeps_per_campaign": self._coerce_int(
                    effective.get("max_sweeps_per_campaign", 2), minimum=1, maximum=8, default=2
                ),
                "adaptive_budgeting": bool(effective.get("adaptive_budgeting", False)),
                "adaptive_goal": str(effective.get("adaptive_goal", "") or "").strip().lower(),
            },
            **summary,
        }
        self._history.append(entry)
        history_limit = self._coerce_int(self._config.get("history_limit", 8), minimum=1, maximum=64, default=8)
        self._history = self._history[-history_limit:]

    @staticmethod
    def _apply_overrides(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(base)
        if not isinstance(overrides, dict):
            return payload
        for key in (
            "enabled",
            "interval_s",
            "max_portfolios",
            "max_waves_per_portfolio",
            "max_programs_per_portfolio",
            "max_campaigns_per_program",
            "max_sweeps_per_campaign",
            "max_sessions",
            "max_replays_per_session",
            "history_limit",
            "adaptive_budgeting",
            "adaptive_goal",
            "portfolio_status",
            "pack",
            "app_name",
        ):
            if key in overrides and overrides[key] is not None:
                payload[key] = overrides[key]
        return payload

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            result = int(value)
        except Exception:  # noqa: BLE001
            return default
        return max(minimum, min(maximum, result))

    @staticmethod
    def _increment_count(counts: Dict[str, int], key: str) -> None:
        clean = str(key or "").strip().lower() or "unknown"
        counts[clean] = int(counts.get(clean, 0)) + 1

    @staticmethod
    def _sorted_count_map(source: Dict[str, int]) -> Dict[str, int]:
        items = sorted(source.items(), key=lambda item: (-int(item[1]), item[0]))
        return {key: int(value) for key, value in items}

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            result = float(value)
        except Exception:  # noqa: BLE001
            return default
        return max(minimum, min(maximum, result))


