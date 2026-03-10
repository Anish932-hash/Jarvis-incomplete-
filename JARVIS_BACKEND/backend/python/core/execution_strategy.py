from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List

from .contracts import ActionResult


class ExecutionStrategyController:
    """
    Cross-mission execution strategy controller.

    Learns lightweight per-task-class runtime strategy hints:
    - parallel execution budget
    - verification strictness
    - external mutation branch policy
    - external mutation simulation toggle
    """

    _MODES = ("strict", "balanced", "throughput")

    def __init__(
        self,
        *,
        store_path: str = "data/execution_strategy.json",
        max_task_classes: int = 1400,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_task_classes = self._coerce_int(max_task_classes, minimum=40, maximum=50_000, default=1400)
        self.ema_alpha = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_EMA_ALPHA", "0.34"),
            minimum=0.05,
            maximum=0.95,
            default=0.34,
        )
        self.min_pulls_for_confident_mode = self._coerce_int(
            os.getenv("JARVIS_EXECUTION_STRATEGY_MIN_PULLS", "4"),
            minimum=1,
            maximum=1000,
            default=4,
        )
        self.mode_hysteresis_margin = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_MODE_HYSTERESIS", "0.08"),
            minimum=0.01,
            maximum=0.4,
            default=0.08,
        )
        self.global_parallel_cap = self._coerce_int(
            os.getenv("JARVIS_EXECUTION_STRATEGY_PARALLEL_CAP", "3"),
            minimum=1,
            maximum=6,
            default=3,
        )
        self.strict_bias = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_STRICT_BIAS", "0.0"),
            minimum=-0.5,
            maximum=0.8,
            default=0.0,
        )
        self.throughput_bias = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_THROUGHPUT_BIAS", "0.0"),
            minimum=-0.5,
            maximum=0.8,
            default=0.0,
        )
        self.family_bias_alpha = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_FAMILY_BIAS_ALPHA", "0.26"),
            minimum=0.05,
            maximum=0.95,
            default=0.26,
        )
        self.family_bias_limit = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_FAMILY_BIAS_LIMIT", "0.52"),
            minimum=0.05,
            maximum=0.95,
            default=0.52,
        )
        self.family_hotspot_weight = self._coerce_float(
            os.getenv("JARVIS_EXECUTION_STRATEGY_FAMILY_HOTSPOT_WEIGHT", "0.34"),
            minimum=0.0,
            maximum=1.0,
            default=0.34,
        )
        self.max_task_families = self._coerce_int(
            os.getenv("JARVIS_EXECUTION_STRATEGY_MAX_TASK_FAMILIES", "64"),
            minimum=6,
            maximum=400,
            default=64,
        )

        self._lock = RLock()
        self._rows: Dict[str, Dict[str, Any]] = {}
        self._task_family_bias: Dict[str, Dict[str, Any]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._last_tune: Dict[str, Any] = {
            "status": "idle",
            "last_run_at": "",
            "changed": False,
            "dry_run": False,
            "reason": "",
            "mode": "",
        }
        self._load()

    def recommend(
        self,
        *,
        task_class: str,
        source_name: str = "",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_task_class = self._normalize_task_class(task_class)
        source = str(source_name or "").strip().lower()
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        with self._lock:
            row = dict(self._rows.get(clean_task_class, {}))
            task_family = self._task_family_from_class(task_class)
            family_row = dict(self._task_family_bias.get(task_family, {}))

        pulls = self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0)
        mode = str(row.get("mode", "")).strip().lower()
        if mode not in self._MODES:
            mode = self._default_mode_for_source(source)
        family_strict_bias = self._coerce_float(
            family_row.get("strict_bias", 0.0),
            minimum=-self.family_bias_limit,
            maximum=self.family_bias_limit,
            default=0.0,
        )
        family_throughput_bias = self._coerce_float(
            family_row.get("throughput_bias", 0.0),
            minimum=-self.family_bias_limit,
            maximum=self.family_bias_limit,
            default=0.0,
        )
        confidence = self._coerce_float(
            min(1.0, float(pulls) / max(1.0, float(self.min_pulls_for_confident_mode * 4))),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mode = self._apply_family_mode_adjustment(
            mode=mode,
            confidence=confidence,
            strict_bias=family_strict_bias,
            throughput_bias=family_throughput_bias,
        )
        strategy = self._mode_strategy(
            mode=mode,
            source_name=source,
            confidence=confidence,
            metadata=runtime_meta,
            task_class=clean_task_class,
            task_family=task_family,
            family_strict_bias=family_strict_bias,
            family_throughput_bias=family_throughput_bias,
        )
        return {
            "status": "success",
            "task_class": clean_task_class,
            "task_family": task_family,
            "mode": mode,
            "confidence": round(confidence, 6),
            "pulls": pulls,
            "strategy": strategy,
            "signals": {
                "ema_failure_pressure": self._coerce_float(
                    row.get("ema_failure_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "ema_blocked_pressure": self._coerce_float(
                    row.get("ema_blocked_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "ema_retry_pressure": self._coerce_float(
                    row.get("ema_retry_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "ema_duration_pressure": self._coerce_float(
                    row.get("ema_duration_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "family_strict_bias": round(family_strict_bias, 6),
                "family_throughput_bias": round(family_throughput_bias, 6),
                "family_risk_ema": self._coerce_float(
                    family_row.get("risk_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
            },
        }

    def record_outcome(
        self,
        *,
        task_class: str,
        outcome: str,
        results: List[ActionResult] | None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_task_class = self._normalize_task_class(task_class)
        normalized_outcome = str(outcome or "").strip().lower() or "unknown"
        rows = [row for row in (results or []) if isinstance(row, ActionResult)]
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        now_iso = datetime.now(timezone.utc).isoformat()

        total_steps = len(rows)
        failed_steps = sum(1 for row in rows if str(row.status or "").strip().lower() == "failed")
        blocked_steps = sum(1 for row in rows if str(row.status or "").strip().lower() == "blocked")
        success_steps = sum(1 for row in rows if str(row.status or "").strip().lower() == "success")
        avg_attempt = (
            sum(max(1, int(row.attempt or 1)) for row in rows) / float(max(1, total_steps))
            if rows
            else 1.0
        )
        avg_duration_ms = (
            sum(max(0, int(row.duration_ms or 0)) for row in rows) / float(max(1, total_steps))
            if rows
            else 0.0
        )

        failed_ratio = float(failed_steps) / max(1.0, float(total_steps))
        blocked_ratio = float(blocked_steps) / max(1.0, float(total_steps))
        retry_pressure = self._clamp((avg_attempt - 1.0) / 3.0, minimum=0.0, maximum=1.0)
        duration_pressure = self._clamp(avg_duration_ms / 2800.0, minimum=0.0, maximum=1.0)
        outcome_failure_boost = 0.0
        if normalized_outcome in {"failed", "blocked"}:
            outcome_failure_boost = 0.22
        elif normalized_outcome == "cancelled":
            outcome_failure_boost = 0.12
        failure_pressure = self._clamp(
            (failed_ratio * 0.7) + (blocked_ratio * 0.22) + outcome_failure_boost,
            minimum=0.0,
            maximum=1.0,
        )

        source_name = str(runtime_meta.get("source", "")).strip().lower()
        task_family = self._task_family_from_class(task_class)
        with self._lock:
            previous = dict(self._rows.get(clean_task_class, {}))
            previous_family = dict(self._task_family_bias.get(task_family, {}))
            pulls = self._coerce_int(previous.get("pulls", 0), minimum=0, maximum=100_000_000, default=0) + 1
            successes = self._coerce_int(previous.get("successes", 0), minimum=0, maximum=100_000_000, default=0)
            failures = self._coerce_int(previous.get("failures", 0), minimum=0, maximum=100_000_000, default=0)
            blocked = self._coerce_int(previous.get("blocked", 0), minimum=0, maximum=100_000_000, default=0)
            cancelled = self._coerce_int(previous.get("cancelled", 0), minimum=0, maximum=100_000_000, default=0)
            if normalized_outcome == "completed":
                successes += 1
            elif normalized_outcome == "blocked":
                blocked += 1
                failures += 1
            elif normalized_outcome == "cancelled":
                cancelled += 1
            elif normalized_outcome == "failed":
                failures += 1

            ema_failure = self._ema(
                previous=previous.get("ema_failure_pressure", 0.0),
                incoming=failure_pressure,
            )
            ema_blocked = self._ema(
                previous=previous.get("ema_blocked_pressure", 0.0),
                incoming=blocked_ratio,
            )
            ema_retry = self._ema(
                previous=previous.get("ema_retry_pressure", 0.0),
                incoming=retry_pressure,
            )
            ema_duration = self._ema(
                previous=previous.get("ema_duration_pressure", 0.0),
                incoming=duration_pressure,
            )

            old_mode = str(previous.get("mode", "")).strip().lower()
            if old_mode not in self._MODES:
                old_mode = self._default_mode_for_source(source_name)
            family_strict_bias = self._coerce_float(
                previous_family.get("strict_bias", 0.0),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
                default=0.0,
            )
            family_throughput_bias = self._coerce_float(
                previous_family.get("throughput_bias", 0.0),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
                default=0.0,
            )
            mode = self._select_mode(
                previous_mode=old_mode,
                pulls=pulls,
                failure_pressure=ema_failure,
                blocked_pressure=ema_blocked,
                retry_pressure=ema_retry,
                duration_pressure=ema_duration,
                success_ratio=(float(successes) / max(1.0, float(pulls))),
                family_strict_bias=family_strict_bias,
                family_throughput_bias=family_throughput_bias,
            )
            mode = self._apply_family_mode_adjustment(
                mode=mode,
                confidence=min(1.0, float(pulls) / max(1.0, float(self.min_pulls_for_confident_mode * 4))),
                strict_bias=family_strict_bias,
                throughput_bias=family_throughput_bias,
            )

            updated = {
                "task_class": clean_task_class,
                "mode": mode,
                "pulls": pulls,
                "successes": successes,
                "failures": failures,
                "blocked": blocked,
                "cancelled": cancelled,
                "ema_failure_pressure": round(ema_failure, 8),
                "ema_blocked_pressure": round(ema_blocked, 8),
                "ema_retry_pressure": round(ema_retry, 8),
                "ema_duration_pressure": round(ema_duration, 8),
                "last_outcome": normalized_outcome,
                "last_goal_id": str(runtime_meta.get("goal_id", "")).strip(),
                "last_updated_at": now_iso,
            }
            self._rows[clean_task_class] = updated
            family_risk_ema = self._ema(
                previous=previous_family.get("risk_ema", 0.0),
                incoming=failure_pressure,
            )
            family_retry_ema = self._ema(
                previous=previous_family.get("retry_ema", 0.0),
                incoming=retry_pressure,
            )
            family_success_ema = self._ema(
                previous=previous_family.get("success_ema", 0.0),
                incoming=(1.0 - failure_pressure),
            )
            family_samples = self._coerce_int(
                previous_family.get("samples", 0),
                minimum=0,
                maximum=100_000_000,
                default=0,
            ) + 1
            family_strict_target = self._clamp(
                (family_risk_ema * 0.44) + (ema_blocked * 0.18) + (family_retry_ema * 0.16) - 0.22,
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            family_throughput_target = self._clamp(
                (family_success_ema * 0.42) + ((1.0 - family_retry_ema) * 0.22) - 0.42,
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            alpha = self._coerce_float(
                self.family_bias_alpha,
                minimum=0.05,
                maximum=0.95,
                default=0.26,
            )
            next_family_strict_bias = self._clamp(
                ((family_strict_bias * (1.0 - alpha)) + (family_strict_target * alpha)),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            next_family_throughput_bias = self._clamp(
                ((family_throughput_bias * (1.0 - alpha)) + (family_throughput_target * alpha)),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            self._task_family_bias[task_family] = {
                "task_family": task_family,
                "strict_bias": round(next_family_strict_bias, 8),
                "throughput_bias": round(next_family_throughput_bias, 8),
                "risk_ema": round(family_risk_ema, 8),
                "retry_ema": round(family_retry_ema, 8),
                "success_ema": round(family_success_ema, 8),
                "samples": family_samples,
                "last_updated_at": now_iso,
            }
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=False)

        return {
            "status": "success",
            "task_class": clean_task_class,
            "task_family": task_family,
            "mode": mode,
            "mode_changed": mode != old_mode,
            "pulls": pulls,
            "signals": {
                "ema_failure_pressure": round(ema_failure, 6),
                "ema_blocked_pressure": round(ema_blocked, 6),
                "ema_retry_pressure": round(ema_retry, 6),
                "ema_duration_pressure": round(ema_duration, 6),
                "family_strict_bias": round(next_family_strict_bias, 6),
                "family_throughput_bias": round(next_family_throughput_bias, 6),
                "family_risk_ema": round(family_risk_ema, 6),
            },
        }

    def tune_from_operational_signals(
        self,
        *,
        autonomy_report: Dict[str, Any] | None = None,
        mission_summary: Dict[str, Any] | None = None,
        dry_run: bool = False,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        report = autonomy_report if isinstance(autonomy_report, dict) else {}
        missions = mission_summary if isinstance(mission_summary, dict) else {}
        pressures = report.get("pressures", {}) if isinstance(report.get("pressures", {}), dict) else {}
        scores = report.get("scores", {}) if isinstance(report.get("scores", {}), dict) else {}
        trend = missions.get("trend", {}) if isinstance(missions.get("trend", {}), dict) else {}
        recommendation = str(missions.get("recommendation", "")).strip().lower()

        failure_pressure = self._coerce_float(pressures.get("failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        breaker_pressure = self._coerce_float(pressures.get("open_breaker_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        reliability = self._coerce_float(scores.get("reliability", 0.0), minimum=0.0, maximum=100.0, default=0.0) / 100.0
        autonomy = self._coerce_float(scores.get("autonomy", 0.0), minimum=0.0, maximum=100.0, default=0.0) / 100.0
        trend_pressure = self._coerce_float(trend.get("pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)

        strict_target = self._clamp(
            (failure_pressure * 0.44)
            + (breaker_pressure * 0.24)
            + (trend_pressure * 0.18)
            + ((1.0 - reliability) * 0.14),
            minimum=-0.5,
            maximum=0.8,
        )
        throughput_target = self._clamp(
            (reliability * 0.34)
            + (autonomy * 0.26)
            + ((1.0 - failure_pressure) * 0.22)
            + ((1.0 - breaker_pressure) * 0.18),
            minimum=-0.5,
            maximum=0.8,
        ) - 0.42

        if recommendation == "stability":
            strict_target = self._clamp(strict_target + 0.08, minimum=-0.5, maximum=0.8)
            throughput_target = self._clamp(throughput_target - 0.05, minimum=-0.5, maximum=0.8)
        elif recommendation == "throughput":
            strict_target = self._clamp(strict_target - 0.06, minimum=-0.5, maximum=0.8)
            throughput_target = self._clamp(throughput_target + 0.09, minimum=-0.5, maximum=0.8)

        parallel_cap_target = 3
        if strict_target >= 0.4 or failure_pressure >= 0.55:
            parallel_cap_target = 2
        if strict_target >= 0.58 or breaker_pressure >= 0.48:
            parallel_cap_target = 1
        if throughput_target >= 0.24 and failure_pressure <= 0.25 and breaker_pressure <= 0.12:
            parallel_cap_target = 4
        parallel_cap_target = self._coerce_int(parallel_cap_target, minimum=1, maximum=6, default=3)

        next_strict_bias = self._clamp(((self.strict_bias * 0.72) + (strict_target * 0.28)), minimum=-0.5, maximum=0.8)
        next_throughput_bias = self._clamp(((self.throughput_bias * 0.72) + (throughput_target * 0.28)), minimum=-0.5, maximum=0.8)
        next_parallel_cap = self._coerce_int(
            round((self.global_parallel_cap * 0.65) + (parallel_cap_target * 0.35)),
            minimum=1,
            maximum=6,
            default=self.global_parallel_cap,
        )

        changes: Dict[str, Dict[str, Any]] = {}
        if round(next_strict_bias, 6) != round(self.strict_bias, 6):
            changes["strict_bias"] = {"from": round(self.strict_bias, 6), "to": round(next_strict_bias, 6)}
        if round(next_throughput_bias, 6) != round(self.throughput_bias, 6):
            changes["throughput_bias"] = {"from": round(self.throughput_bias, 6), "to": round(next_throughput_bias, 6)}
        if int(next_parallel_cap) != int(self.global_parallel_cap):
            changes["global_parallel_cap"] = {"from": int(self.global_parallel_cap), "to": int(next_parallel_cap)}

        family_targets = self._family_targets_from_operational_signals(
            autonomy_report=report,
            mission_summary=missions,
            strict_target=strict_target,
            throughput_target=throughput_target,
        )
        family_changes: Dict[str, Dict[str, Any]] = {}
        task_mode_changes: List[Dict[str, Any]] = []
        with self._lock:
            for family, target_row in family_targets.items():
                previous = dict(self._task_family_bias.get(family, {}))
                previous_strict = self._coerce_float(
                    previous.get("strict_bias", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                )
                previous_throughput = self._coerce_float(
                    previous.get("throughput_bias", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                )
                target_strict = self._coerce_float(
                    target_row.get("strict_target", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                )
                target_throughput = self._coerce_float(
                    target_row.get("throughput_target", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                )
                confidence = self._coerce_float(
                    target_row.get("confidence", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                effective_alpha = self._coerce_float(
                    self.family_bias_alpha * (0.72 + (confidence * 0.4)),
                    minimum=0.05,
                    maximum=0.95,
                    default=self.family_bias_alpha,
                )
                next_family_strict = self._clamp(
                    ((previous_strict * (1.0 - effective_alpha)) + (target_strict * effective_alpha)),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                )
                next_family_throughput = self._clamp(
                    ((previous_throughput * (1.0 - effective_alpha)) + (target_throughput * effective_alpha)),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                )
                pressure = self._coerce_float(
                    target_row.get("pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                samples = self._coerce_int(
                    previous.get("samples", 0),
                    minimum=0,
                    maximum=100_000_000,
                    default=0,
                )
                samples += self._coerce_int(
                    round(confidence * 8.0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                if round(next_family_strict, 6) != round(previous_strict, 6) or round(next_family_throughput, 6) != round(previous_throughput, 6):
                    family_changes[family] = {
                        "strict_bias": {"from": round(previous_strict, 6), "to": round(next_family_strict, 6)},
                        "throughput_bias": {"from": round(previous_throughput, 6), "to": round(next_family_throughput, 6)},
                        "pressure": round(pressure, 6),
                        "confidence": round(confidence, 6),
                    }
                if not dry_run:
                    self._task_family_bias[family] = {
                        "task_family": family,
                        "strict_bias": round(next_family_strict, 8),
                        "throughput_bias": round(next_family_throughput, 8),
                        "risk_ema": round(
                            self._ema(previous=previous.get("risk_ema", pressure), incoming=pressure),
                            8,
                        ),
                        "retry_ema": round(
                            self._ema(previous=previous.get("retry_ema", pressure * 0.7), incoming=pressure * 0.7),
                            8,
                        ),
                        "success_ema": round(
                            self._ema(previous=previous.get("success_ema", max(0.0, 1.0 - pressure)), incoming=max(0.0, 1.0 - pressure)),
                            8,
                        ),
                        "samples": samples,
                        "last_updated_at": datetime.now(timezone.utc).isoformat(),
                    }

            if family_targets:
                task_mode_changes = self._retune_task_modes_from_family_targets_locked(
                    targets=family_targets,
                    dry_run=dry_run,
                )
                if task_mode_changes and not dry_run:
                    self._updates_since_save += 1

            if not dry_run:
                self._trim_locked()

        if family_changes:
            changes["task_family_bias"] = {
                "changed_families": len(family_changes),
                "families": family_changes,
            }
        if task_mode_changes:
            changes["task_mode_alignment"] = {
                "changed_task_classes": len(task_mode_changes),
                "items": task_mode_changes[:24],
            }

        if changes and not dry_run:
            self.strict_bias = next_strict_bias
            self.throughput_bias = next_throughput_bias
            self.global_parallel_cap = next_parallel_cap
            with self._lock:
                self._maybe_save_locked(force=True)

        payload = {
            "status": "success",
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "changed": bool(changes),
            "dry_run": bool(dry_run),
            "reason": str(reason or "").strip() or "manual",
            "mode": "stability" if strict_target >= throughput_target else "throughput",
            "changes": changes,
            "targets": {
                "strict_target": round(strict_target, 6),
                "throughput_target": round(throughput_target, 6),
                "parallel_cap_target": int(parallel_cap_target),
                "task_family_targets": family_targets,
            },
            "state": {
                "strict_bias": round(next_strict_bias if dry_run else self.strict_bias, 6),
                "throughput_bias": round(next_throughput_bias if dry_run else self.throughput_bias, 6),
                "global_parallel_cap": int(next_parallel_cap if dry_run else self.global_parallel_cap),
            },
        }
        self._last_tune = payload
        return payload

    def snapshot(self, *, task_class: str = "", limit: int = 200) -> Dict[str, Any]:
        clean_task = self._normalize_task_class(task_class) if str(task_class or "").strip() else ""
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        with self._lock:
            rows = dict(self._rows)
            family_rows = dict(self._task_family_bias)

        items: List[Dict[str, Any]] = []
        for class_name, row in rows.items():
            if clean_task and class_name != clean_task:
                continue
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["task_class"] = class_name
            items.append(item)
        items.sort(
            key=lambda item: (
                -self._coerce_int(item.get("pulls", 0), minimum=0, maximum=100_000_000, default=0),
                str(item.get("task_class", "")),
            )
        )
        selected = items[:bounded]
        return {
            "status": "success",
            "task_class": clean_task,
            "count": len(selected),
            "total": len(items),
            "tracked_task_classes": len(rows),
            "tracked_task_families": len(family_rows),
            "config": {
                "ema_alpha": self.ema_alpha,
                "min_pulls_for_confident_mode": self.min_pulls_for_confident_mode,
                "mode_hysteresis_margin": self.mode_hysteresis_margin,
                "strict_bias": self.strict_bias,
                "throughput_bias": self.throughput_bias,
                "global_parallel_cap": self.global_parallel_cap,
                "family_bias_alpha": self.family_bias_alpha,
                "family_bias_limit": self.family_bias_limit,
                "family_hotspot_weight": self.family_hotspot_weight,
                "last_tune": dict(self._last_tune),
            },
            "task_family_bias": self._public_task_family_bias_rows(family_rows),
            "items": selected,
        }

    def reset(self, *, task_class: str = "") -> Dict[str, Any]:
        clean_task = self._normalize_task_class(task_class) if str(task_class or "").strip() else ""
        removed = 0
        with self._lock:
            if clean_task:
                if clean_task in self._rows:
                    self._rows.pop(clean_task, None)
                    removed = 1
            else:
                removed = len(self._rows)
                self._rows = {}
            self._maybe_save_locked(force=True)
        return {"status": "success", "task_class": clean_task, "removed": removed}

    def _mode_strategy(
        self,
        *,
        mode: str,
        source_name: str,
        confidence: float,
        metadata: Dict[str, Any],
        task_class: str = "",
        task_family: str = "",
        family_strict_bias: float = 0.0,
        family_throughput_bias: float = 0.0,
    ) -> Dict[str, Any]:
        clean_mode = str(mode or "").strip().lower()
        source = str(source_name or "").strip().lower()
        if clean_mode not in self._MODES:
            clean_mode = self._default_mode_for_source(source)

        strategy: Dict[str, Any]
        if clean_mode == "strict":
            strategy = {
                "execution_allow_parallel": False,
                "execution_max_parallel_steps": 1,
                "verification_strictness": "strict",
                "external_branch_strategy": "enforce",
                "external_mutation_simulation_enabled": True,
            }
        elif clean_mode == "throughput":
            strategy = {
                "execution_allow_parallel": True,
                "execution_max_parallel_steps": 4,
                "verification_strictness": "standard",
                "external_branch_strategy": "warn",
                "external_mutation_simulation_enabled": True,
            }
        else:
            strategy = {
                "execution_allow_parallel": True,
                "execution_max_parallel_steps": 2,
                "verification_strictness": "standard",
                "external_branch_strategy": "warn",
                "external_mutation_simulation_enabled": True,
            }

        if source in {"desktop-schedule", "desktop-trigger"} and clean_mode != "throughput":
            strategy["execution_allow_parallel"] = False
            strategy["execution_max_parallel_steps"] = 1
            strategy["verification_strictness"] = "strict"
            strategy["external_branch_strategy"] = "enforce"

        if confidence < 0.2:
            strategy["verification_strictness"] = "standard"
            strategy["external_branch_strategy"] = "warn"

        strategy["execution_max_parallel_steps"] = self._coerce_int(
            min(
                int(strategy.get("execution_max_parallel_steps", 2) or 2),
                int(self.global_parallel_cap),
            ),
            minimum=1,
            maximum=6,
            default=2,
        )
        family_gap = self._coerce_float(
            family_throughput_bias - family_strict_bias,
            minimum=-1.5,
            maximum=1.5,
            default=0.0,
        )
        if family_gap <= -0.16:
            strategy["execution_max_parallel_steps"] = max(
                1,
                int(strategy.get("execution_max_parallel_steps", 2)) - 1,
            )
            strategy["verification_strictness"] = "strict"
            if clean_mode != "throughput":
                strategy["external_branch_strategy"] = "enforce"
        elif family_gap >= 0.2 and clean_mode == "throughput":
            strategy["execution_max_parallel_steps"] = min(
                int(self.global_parallel_cap),
                int(strategy.get("execution_max_parallel_steps", 2)) + 1,
            )
        if task_class:
            strategy["task_class"] = str(task_class).strip().lower()
        if task_family:
            strategy["task_family"] = str(task_family).strip().lower()
        return strategy

    def _select_mode(
        self,
        *,
        previous_mode: str,
        pulls: int,
        failure_pressure: float,
        blocked_pressure: float,
        retry_pressure: float,
        duration_pressure: float,
        success_ratio: float,
        family_strict_bias: float = 0.0,
        family_throughput_bias: float = 0.0,
    ) -> str:
        if pulls < self.min_pulls_for_confident_mode:
            return previous_mode if previous_mode in self._MODES else "balanced"

        strict_score = self._clamp(
            (failure_pressure * 0.46)
            + (blocked_pressure * 0.2)
            + (retry_pressure * 0.2)
            + (duration_pressure * 0.14)
            + self.strict_bias,
            minimum=0.0,
            maximum=1.2,
        )
        strict_score = self._clamp(
            strict_score + (family_strict_bias * 0.35),
            minimum=0.0,
            maximum=1.2,
        )
        throughput_score = self._clamp(
            ((1.0 - failure_pressure) * 0.34)
            + ((1.0 - blocked_pressure) * 0.16)
            + ((1.0 - retry_pressure) * 0.2)
            + ((1.0 - duration_pressure) * 0.1)
            + (success_ratio * 0.2)
            + self.throughput_bias,
            minimum=0.0,
            maximum=1.2,
        )
        throughput_score = self._clamp(
            throughput_score + (family_throughput_bias * 0.35),
            minimum=0.0,
            maximum=1.2,
        )
        balanced_score = self._clamp(
            1.0
            - abs(strict_score - throughput_score)
            - (0.26 * max(strict_score, throughput_score))
            - (failure_pressure * 0.42)
            - (blocked_pressure * 0.18),
            minimum=0.0,
            maximum=1.2,
        )
        scores = {
            "strict": strict_score,
            "balanced": balanced_score,
            "throughput": throughput_score,
        }
        ranked = sorted(scores.items(), key=lambda item: (-float(item[1]), item[0]))
        selected = str(ranked[0][0]).strip().lower() if ranked else "balanced"
        prior = previous_mode if previous_mode in self._MODES else "balanced"
        if selected != prior:
            selected_score = float(scores.get(selected, 0.0))
            prior_score = float(scores.get(prior, 0.0))
            if (selected_score - prior_score) < self.mode_hysteresis_margin:
                return prior
        return selected

    @staticmethod
    def _default_mode_for_source(source_name: str) -> str:
        source = str(source_name or "").strip().lower()
        if source in {"desktop-schedule", "desktop-trigger"}:
            return "strict"
        if source in {"desktop-mission", "desktop-macro"}:
            return "throughput"
        return "balanced"

    def _ema(self, *, previous: Any, incoming: float) -> float:
        prev = self._coerce_float(previous, minimum=0.0, maximum=1.0, default=0.0)
        alpha = self._coerce_float(self.ema_alpha, minimum=0.05, maximum=0.95, default=0.34)
        return self._clamp(((1.0 - alpha) * prev) + (alpha * incoming), minimum=0.0, maximum=1.0)

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        try:
            payload = json.loads(self.store_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        config = payload.get("config", {})
        if isinstance(config, dict):
            self.strict_bias = self._coerce_float(config.get("strict_bias", self.strict_bias), minimum=-0.5, maximum=0.8, default=self.strict_bias)
            self.throughput_bias = self._coerce_float(
                config.get("throughput_bias", self.throughput_bias),
                minimum=-0.5,
                maximum=0.8,
                default=self.throughput_bias,
            )
            self.global_parallel_cap = self._coerce_int(
                config.get("global_parallel_cap", self.global_parallel_cap),
                minimum=1,
                maximum=6,
                default=self.global_parallel_cap,
            )
            self.family_bias_alpha = self._coerce_float(
                config.get("family_bias_alpha", self.family_bias_alpha),
                minimum=0.05,
                maximum=0.95,
                default=self.family_bias_alpha,
            )
            self.family_bias_limit = self._coerce_float(
                config.get("family_bias_limit", self.family_bias_limit),
                minimum=0.05,
                maximum=0.95,
                default=self.family_bias_limit,
            )
            self.family_hotspot_weight = self._coerce_float(
                config.get("family_hotspot_weight", self.family_hotspot_weight),
                minimum=0.0,
                maximum=1.0,
                default=self.family_hotspot_weight,
            )
            last_tune = config.get("last_tune", {})
            if isinstance(last_tune, dict):
                self._last_tune.update(
                    {
                        "status": str(last_tune.get("status", self._last_tune.get("status", ""))).strip(),
                        "last_run_at": str(last_tune.get("last_run_at", self._last_tune.get("last_run_at", ""))).strip(),
                        "changed": bool(last_tune.get("changed", self._last_tune.get("changed", False))),
                        "dry_run": bool(last_tune.get("dry_run", self._last_tune.get("dry_run", False))),
                        "reason": str(last_tune.get("reason", self._last_tune.get("reason", ""))).strip(),
                        "mode": str(last_tune.get("mode", self._last_tune.get("mode", ""))).strip(),
                    }
                )
        items = payload.get("items", [])
        if not isinstance(items, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            task_class = self._normalize_task_class(row.get("task_class", ""))
            if not task_class:
                continue
            loaded[task_class] = {
                "task_class": task_class,
                "mode": str(row.get("mode", "balanced")).strip().lower() or "balanced",
                "pulls": self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0),
                "successes": self._coerce_int(row.get("successes", 0), minimum=0, maximum=100_000_000, default=0),
                "failures": self._coerce_int(row.get("failures", 0), minimum=0, maximum=100_000_000, default=0),
                "blocked": self._coerce_int(row.get("blocked", 0), minimum=0, maximum=100_000_000, default=0),
                "cancelled": self._coerce_int(row.get("cancelled", 0), minimum=0, maximum=100_000_000, default=0),
                "ema_failure_pressure": self._coerce_float(row.get("ema_failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "ema_blocked_pressure": self._coerce_float(row.get("ema_blocked_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "ema_retry_pressure": self._coerce_float(row.get("ema_retry_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "ema_duration_pressure": self._coerce_float(row.get("ema_duration_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "last_outcome": str(row.get("last_outcome", "")).strip().lower(),
                "last_goal_id": str(row.get("last_goal_id", "")).strip(),
                "last_updated_at": str(row.get("last_updated_at", "")).strip(),
            }
        family_raw = payload.get("task_family_bias", [])
        family_items = []
        if isinstance(family_raw, list):
            family_items = family_raw
        elif isinstance(family_raw, dict):
            family_items = list(family_raw.values())
        loaded_families: Dict[str, Dict[str, Any]] = {}
        for row in family_items:
            if not isinstance(row, dict):
                continue
            family = self._normalize_task_family(row.get("task_family", ""))
            if not family:
                continue
            loaded_families[family] = {
                "task_family": family,
                "strict_bias": self._coerce_float(
                    row.get("strict_bias", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                ),
                "throughput_bias": self._coerce_float(
                    row.get("throughput_bias", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                ),
                "risk_ema": self._coerce_float(row.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "retry_ema": self._coerce_float(row.get("retry_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "success_ema": self._coerce_float(row.get("success_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=100_000_000, default=0),
                "last_updated_at": str(row.get("last_updated_at", "")).strip(),
            }
        with self._lock:
            self._rows = loaded
            self._task_family_bias = loaded_families
            self._trim_locked()

    def _trim_locked(self) -> None:
        if len(self._rows) <= self.max_task_classes:
            if len(self._task_family_bias) > self.max_task_families:
                ordered_families = sorted(
                    self._task_family_bias.items(),
                    key=lambda item: (
                        str(item[1].get("last_updated_at", "")),
                        self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=100_000_000, default=0),
                        item[0],
                    ),
                    reverse=True,
                )
                self._task_family_bias = {
                    name: dict(row)
                    for name, row in ordered_families[: self.max_task_families]
                }
            return
        ordered = sorted(
            self._rows.items(),
            key=lambda item: (
                str(item[1].get("last_updated_at", "")),
                item[0],
            ),
            reverse=True,
        )
        self._rows = {name: dict(row) for name, row in ordered[: self.max_task_classes]}
        if len(self._task_family_bias) > self.max_task_families:
            ordered_families = sorted(
                self._task_family_bias.items(),
                key=lambda item: (
                    str(item[1].get("last_updated_at", "")),
                    self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=100_000_000, default=0),
                    item[0],
                ),
                reverse=True,
            )
            self._task_family_bias = {
                name: dict(row)
                for name, row in ordered_families[: self.max_task_families]
            }

    def _maybe_save_locked(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force:
            if self._updates_since_save < 12 and (now - self._last_save_monotonic) < 16.0:
                return
        items: List[Dict[str, Any]] = []
        for task_class, row in self._rows.items():
            if not isinstance(row, dict):
                continue
            payload = dict(row)
            payload["task_class"] = task_class
            items.append(payload)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "ema_alpha": self.ema_alpha,
                "min_pulls_for_confident_mode": self.min_pulls_for_confident_mode,
                "mode_hysteresis_margin": self.mode_hysteresis_margin,
                "strict_bias": self.strict_bias,
                "throughput_bias": self.throughput_bias,
                "global_parallel_cap": self.global_parallel_cap,
                "family_bias_alpha": self.family_bias_alpha,
                "family_bias_limit": self.family_bias_limit,
                "family_hotspot_weight": self.family_hotspot_weight,
                "last_tune": dict(self._last_tune),
            },
            "task_family_bias": self._public_task_family_bias_rows(self._task_family_bias),
            "items": items,
        }
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            self.store_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            self._updates_since_save = 0
            self._last_save_monotonic = now
        except Exception:
            return

    @classmethod
    def _task_family_from_class(cls, task_class: str) -> str:
        raw_task = str(task_class or "").strip().lower()
        if ":" in raw_task:
            parts = [part for part in raw_task.split(":") if part]
            if len(parts) >= 2:
                return cls._normalize_task_family(parts[1])
        clean_task = cls._normalize_task_class(task_class)
        normalized = clean_task.replace(":", "_").replace("-", "_").replace("/", "_")
        parts = [part for part in normalized.split("_") if part]
        for part in parts:
            family = cls._normalize_task_family(part)
            if family != "generic":
                return family
        return "generic"

    @staticmethod
    def _normalize_task_family(value: object) -> str:
        clean = str(value or "").strip().lower().replace("-", "_").replace("/", "_")
        if not clean:
            return "generic"
        allowed = {"external", "desktop", "filesystem", "automation", "browser", "query", "generic"}
        if clean in allowed:
            return clean
        if clean.startswith("external"):
            return "external"
        if clean.startswith("desktop"):
            return "desktop"
        if clean.startswith("file"):
            return "filesystem"
        if clean.startswith("auto"):
            return "automation"
        if clean.startswith("brows"):
            return "browser"
        if clean.startswith("quer"):
            return "query"
        return "generic"

    @classmethod
    def _task_family_from_action(cls, action: str) -> str:
        clean_action = str(action or "").strip().lower()
        if not clean_action:
            return "generic"
        if clean_action.startswith("external_") or clean_action.startswith("oauth_"):
            return "external"
        if clean_action.startswith("computer_") or clean_action.startswith("mouse_") or clean_action.startswith("keyboard_") or clean_action.startswith("accessibility_"):
            return "desktop"
        if clean_action in {"read_file", "write_file", "copy_file", "backup_file", "scan_directory", "list_folder", "list_files", "search_files", "search_text", "create_folder", "folder_size", "explorer_open_path", "explorer_select_file"}:
            return "filesystem"
        if clean_action.startswith("browser_") or clean_action == "open_url":
            return "browser"
        if clean_action.startswith("media_") or clean_action in {"run_trusted_script", "run_whitelisted_app", "open_app"}:
            return "automation"
        if clean_action in {"time_now", "defender_status", "system_snapshot"}:
            return "query"
        return "generic"

    @classmethod
    def _apply_family_mode_adjustment(
        cls,
        *,
        mode: str,
        confidence: float,
        strict_bias: float,
        throughput_bias: float,
    ) -> str:
        clean_mode = str(mode or "").strip().lower()
        gap = float(throughput_bias) - float(strict_bias)
        if confidence < 0.15:
            if gap >= 0.22:
                return "throughput"
            if gap <= -0.22:
                return "strict"
        if clean_mode == "balanced":
            if gap >= 0.28 and confidence <= 0.6:
                return "throughput"
            if gap <= -0.26 and confidence <= 0.75:
                return "strict"
        if clean_mode == "throughput" and gap <= -0.3:
            return "balanced"
        if clean_mode == "strict" and gap >= 0.34:
            return "balanced"
        return clean_mode if clean_mode in cls._MODES else "balanced"

    def _family_targets_from_operational_signals(
        self,
        *,
        autonomy_report: Dict[str, Any],
        mission_summary: Dict[str, Any],
        strict_target: float,
        throughput_target: float,
    ) -> Dict[str, Dict[str, Any]]:
        report = autonomy_report if isinstance(autonomy_report, dict) else {}
        summary = mission_summary if isinstance(mission_summary, dict) else {}
        hotspots_raw = report.get("action_hotspots", [])
        hotspots = hotspots_raw if isinstance(hotspots_raw, list) else []
        trend = summary.get("trend", {}) if isinstance(summary.get("trend", {}), dict) else {}
        recommendation = str(summary.get("recommendation", "")).strip().lower()
        trend_pressure = self._coerce_float(trend.get("pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)

        family_aggregate: Dict[str, Dict[str, float]] = {}
        total_weight = 0.0
        for row in hotspots[:20]:
            if not isinstance(row, dict):
                continue
            action = str(row.get("action", "")).strip().lower()
            if not action:
                continue
            family = self._task_family_from_action(action)
            runs = self._coerce_int(row.get("runs", 0), minimum=0, maximum=1_000_000, default=0)
            failure_rate = self._coerce_float(row.get("failure_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            failures = self._coerce_int(row.get("failures", 0), minimum=0, maximum=1_000_000, default=0)
            weight = max(0.2, min(3.5, (float(runs) / 10.0) + (float(failures) * 0.12)))
            bucket = family_aggregate.setdefault(
                family,
                {
                    "weight": 0.0,
                    "failure_weighted": 0.0,
                },
            )
            bucket["weight"] += weight
            bucket["failure_weighted"] += failure_rate * weight
            total_weight += weight

        targets: Dict[str, Dict[str, Any]] = {}
        baseline_families = set(self._task_family_bias.keys()) | {"external", "desktop", "filesystem", "browser", "automation", "query"}
        for family in baseline_families:
            hotspot = family_aggregate.get(family, {})
            weight = float(hotspot.get("weight", 0.0))
            failure_pressure = 0.0
            if weight > 0.0:
                failure_pressure = float(hotspot.get("failure_weighted", 0.0)) / max(1e-6, weight)
            confidence = self._coerce_float(weight / max(1.0, total_weight), minimum=0.0, maximum=1.0, default=0.0)
            hotspot_component = self._clamp(
                ((failure_pressure - 0.38) * 0.75) * (0.45 + (self.family_hotspot_weight * 0.55)),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            strict_family_target = self._clamp(
                (strict_target * 0.78)
                + (hotspot_component * 0.42)
                + (trend_pressure * 0.08),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            throughput_family_target = self._clamp(
                (throughput_target * 0.78)
                - (hotspot_component * 0.34)
                - (trend_pressure * 0.06),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
            )
            if recommendation == "stability":
                strict_family_target = self._clamp(strict_family_target + 0.04, minimum=-self.family_bias_limit, maximum=self.family_bias_limit)
                throughput_family_target = self._clamp(throughput_family_target - 0.03, minimum=-self.family_bias_limit, maximum=self.family_bias_limit)
            elif recommendation == "throughput":
                strict_family_target = self._clamp(strict_family_target - 0.03, minimum=-self.family_bias_limit, maximum=self.family_bias_limit)
                throughput_family_target = self._clamp(throughput_family_target + 0.05, minimum=-self.family_bias_limit, maximum=self.family_bias_limit)
            targets[family] = {
                "strict_target": round(strict_family_target, 6),
                "throughput_target": round(throughput_family_target, 6),
                "pressure": round(self._coerce_float(failure_pressure, minimum=0.0, maximum=1.0, default=0.0), 6),
                "confidence": round(confidence, 6),
            }
        return targets

    def _retune_task_modes_from_family_targets_locked(
        self,
        *,
        targets: Dict[str, Dict[str, Any]],
        dry_run: bool,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for task_class, row in self._rows.items():
            if not isinstance(row, dict):
                continue
            family = self._task_family_from_class(task_class)
            target = targets.get(family)
            if not isinstance(target, dict):
                continue
            pulls = self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0)
            if pulls < self.min_pulls_for_confident_mode:
                continue
            strict_target = self._coerce_float(
                target.get("strict_target", 0.0),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
                default=0.0,
            )
            throughput_target = self._coerce_float(
                target.get("throughput_target", 0.0),
                minimum=-self.family_bias_limit,
                maximum=self.family_bias_limit,
                default=0.0,
            )
            pressure = self._coerce_float(target.get("pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            old_mode = str(row.get("mode", "")).strip().lower()
            if old_mode not in self._MODES:
                old_mode = "balanced"
            desired = old_mode
            if strict_target - throughput_target >= 0.22 and pressure >= 0.28:
                desired = "strict"
            elif throughput_target - strict_target >= 0.24 and pressure <= 0.42:
                desired = "throughput"
            elif abs(strict_target - throughput_target) <= 0.1:
                desired = "balanced"
            if desired == old_mode:
                continue
            rows.append(
                {
                    "task_class": task_class,
                    "task_family": family,
                    "from": old_mode,
                    "to": desired,
                    "pressure": round(pressure, 6),
                }
            )
            if not dry_run:
                row["mode"] = desired
                row["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                self._rows[task_class] = row
        return rows[:120]

    def _public_task_family_bias_rows(self, rows: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for family, row in rows.items():
            if not isinstance(row, dict):
                continue
            item = {
                "task_family": self._normalize_task_family(family),
                "strict_bias": self._coerce_float(
                    row.get("strict_bias", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                ),
                "throughput_bias": self._coerce_float(
                    row.get("throughput_bias", 0.0),
                    minimum=-self.family_bias_limit,
                    maximum=self.family_bias_limit,
                    default=0.0,
                ),
                "risk_ema": self._coerce_float(row.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "retry_ema": self._coerce_float(row.get("retry_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "success_ema": self._coerce_float(row.get("success_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=100_000_000, default=0),
                "last_updated_at": str(row.get("last_updated_at", "")).strip(),
            }
            items.append(item)
        items.sort(
            key=lambda row: (
                str(row.get("last_updated_at", "")),
                self._coerce_int(row.get("samples", 0), minimum=0, maximum=100_000_000, default=0),
                str(row.get("task_family", "")),
            ),
            reverse=True,
        )
        return items[: self.max_task_families]

    @staticmethod
    def _normalize_task_class(value: object) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return "default"
        collapsed = "_".join(part for part in raw.replace("/", "_").replace(":", "_").split() if part)
        return collapsed[:140] if collapsed else "default"

    @staticmethod
    def _coerce_int(value: object, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: object, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _clamp(value: float, *, minimum: float, maximum: float) -> float:
        return max(minimum, min(maximum, float(value)))
