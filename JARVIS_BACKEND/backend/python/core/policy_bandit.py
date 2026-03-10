from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class MissionPolicyBandit:
    """
    Mission-level policy profile selector using lightweight UCB scoring.

    Tracks reward outcomes for (task_class, policy_profile) pairs and returns
    profile recommendations that balance exploration and exploitation.
    """

    def __init__(
        self,
        *,
        store_path: str = "data/policy_bandit.json",
        max_task_classes: int = 1200,
        max_profiles_per_class: int = 24,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_task_classes = self._coerce_int(max_task_classes, minimum=40, maximum=50_000, default=1200)
        self.max_profiles_per_class = self._coerce_int(max_profiles_per_class, minimum=2, maximum=200, default=24)
        self.exploration_weight = self._coerce_float(
            os.getenv("JARVIS_POLICY_BANDIT_EXPLORATION", "0.26"),
            minimum=0.02,
            maximum=2.5,
            default=0.26,
        )
        self.cold_start_bonus = self._coerce_float(
            os.getenv("JARVIS_POLICY_BANDIT_COLD_START_BONUS", "0.16"),
            minimum=0.0,
            maximum=1.0,
            default=0.16,
        )
        self.min_trials_per_profile = self._coerce_int(
            os.getenv("JARVIS_POLICY_BANDIT_MIN_TRIALS", "2"),
            minimum=0,
            maximum=100,
            default=2,
        )
        self.decay = self._coerce_float(
            os.getenv("JARVIS_POLICY_BANDIT_DECAY", "0.985"),
            minimum=0.8,
            maximum=1.0,
            default=0.985,
        )
        self._adaptive_alpha = self._coerce_float(
            os.getenv("JARVIS_POLICY_BANDIT_ADAPTIVE_ALPHA", "0.34"),
            minimum=0.05,
            maximum=0.95,
            default=0.34,
        )
        self._mode_hysteresis_margin = self._coerce_float(
            os.getenv("JARVIS_POLICY_BANDIT_MODE_HYSTERESIS", "0.09"),
            minimum=0.01,
            maximum=0.45,
            default=0.09,
        )

        self._lock = RLock()
        self._classes: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._adaptive_state: Dict[str, Any] = {
            "ema_failure_pressure": 0.0,
            "ema_open_breaker_pressure": 0.0,
            "ema_failed_ratio": 0.0,
            "ema_blocked_ratio": 0.0,
            "ema_quality": 0.0,
            "ema_risk": 0.0,
            "ema_reliability": 0.0,
            "ema_autonomy": 0.0,
            "ema_guardrail_pressure": 0.0,
            "ema_hotspot_pressure": 0.0,
            "last_mode": "balanced",
            "last_mode_reason": "initial",
            "tune_runs": 0,
            "last_updated_at": "",
        }
        self._last_tune: Dict[str, Any] = {
            "status": "idle",
            "last_run_at": "",
            "mode": "",
            "changed": False,
            "dry_run": False,
            "reason": "",
        }
        self._load()

    def choose_profile(
        self,
        *,
        task_class: str,
        candidate_profiles: List[str],
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_task_class = self._normalize_task_class(task_class)
        profiles = self._normalize_profiles(candidate_profiles)
        if not profiles:
            return {"status": "skip", "task_class": clean_task_class, "message": "No candidate profiles."}

        runtime_meta = metadata if isinstance(metadata, dict) else {}
        source_name = str(runtime_meta.get("source", "")).strip().lower()
        with self._lock:
            class_rows = self._classes.get(clean_task_class, {})
            total_pulls = 0
            for row in class_rows.values():
                if not isinstance(row, dict):
                    continue
                total_pulls += self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0)
            exploration_threshold = max(1, len(profiles) * max(1, self.min_trials_per_profile))
            mature_phase = total_pulls >= exploration_threshold

            scored: List[Dict[str, Any]] = []
            for profile in profiles:
                state = class_rows.get(profile, {})
                pulls = self._coerce_int(state.get("pulls", 0), minimum=0, maximum=100_000_000, default=0)
                reward_mean = self._coerce_float(state.get("reward_mean", 0.52), minimum=0.0, maximum=1.0, default=0.52)
                confidence = min(1.0, float(pulls) / 12.0)
                prior = self._profile_prior(profile=profile, task_class=clean_task_class, source_name=source_name)
                exploration_scale = 0.52 if mature_phase else 1.0
                exploration = self.exploration_weight * exploration_scale * math.sqrt(
                    math.log(float(total_pulls) + 2.0) / (float(pulls) + 1.0)
                )
                cold_start = self.cold_start_bonus if (pulls < self.min_trials_per_profile and not mature_phase) else 0.0
                score = (reward_mean * (0.65 + (0.35 * confidence))) + exploration + cold_start + prior
                scored.append(
                    {
                        "profile": profile,
                        "score": round(score, 6),
                        "reward_mean": round(reward_mean, 6),
                        "pulls": pulls,
                        "exploration": round(exploration, 6),
                        "cold_start": round(cold_start, 6),
                        "prior": round(prior, 6),
                    }
                )

        scored.sort(key=lambda row: (-float(row.get("score", 0.0)), str(row.get("profile", ""))))
        selected = str(scored[0].get("profile", "")).strip() if scored else ""
        return {
            "status": "success",
            "task_class": clean_task_class,
            "selected_profile": selected,
            "candidate_count": len(scored),
            "candidates": scored[: min(len(scored), 8)],
            "config": {
                "exploration_weight": round(self.exploration_weight, 6),
                "cold_start_bonus": round(self.cold_start_bonus, 6),
                "min_trials_per_profile": int(self.min_trials_per_profile),
                "decay": round(self.decay, 6),
            },
        }

    def record_outcome(
        self,
        *,
        task_class: str,
        profile: str,
        reward: float,
        outcome: str,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_task_class = self._normalize_task_class(task_class)
        clean_profile = str(profile or "").strip().lower()
        if not clean_profile:
            return {"status": "error", "message": "profile is required"}

        normalized_outcome = str(outcome or "").strip().lower() or "unknown"
        bounded_reward = self._coerce_float(reward, minimum=0.0, maximum=1.0, default=0.0)
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        now_iso = datetime.now(timezone.utc).isoformat()

        with self._lock:
            class_rows = self._classes.get(clean_task_class, {})
            row = dict(class_rows.get(clean_profile, {}))
            pulls = self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0) + 1
            reward_sum = self._coerce_float(row.get("reward_sum", 0.0), minimum=0.0, maximum=100_000_000.0, default=0.0)
            reward_sum = (reward_sum * self.decay) + bounded_reward
            reward_mean = reward_sum / max(1.0, float(pulls))
            successes = self._coerce_int(row.get("successes", 0), minimum=0, maximum=100_000_000, default=0)
            failures = self._coerce_int(row.get("failures", 0), minimum=0, maximum=100_000_000, default=0)
            blocked = self._coerce_int(row.get("blocked", 0), minimum=0, maximum=100_000_000, default=0)
            if normalized_outcome == "completed":
                successes += 1
            elif normalized_outcome == "blocked":
                blocked += 1
                failures += 1
            elif normalized_outcome in {"failed", "cancelled"}:
                failures += 1

            updated = {
                "task_class": clean_task_class,
                "profile": clean_profile,
                "pulls": pulls,
                "reward_sum": round(reward_sum, 8),
                "reward_mean": round(max(0.0, min(1.0, reward_mean)), 8),
                "successes": successes,
                "failures": failures,
                "blocked": blocked,
                "last_reward": round(bounded_reward, 6),
                "last_outcome": normalized_outcome,
                "last_goal_id": str(runtime_meta.get("goal_id", "")).strip(),
                "updated_at": now_iso,
            }
            class_rows[clean_profile] = updated
            self._classes[clean_task_class] = class_rows
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=False)

        return {"status": "success", "task_class": clean_task_class, "profile": clean_profile}

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
        recommendation = str(missions.get("recommendation", "")).strip().lower()
        if recommendation not in {"stability", "throughput", "balanced"}:
            recommendation = "balanced"
        features = self._extract_operational_features(report=report, missions=missions)

        with self._lock:
            previous_adaptive = dict(self._adaptive_state)
        adaptive = self._advance_adaptive_state(previous=previous_adaptive, features=features)

        mode_scores = self._score_modes(features=features, adaptive=adaptive, recommendation=recommendation)
        mode, mode_reason = self._select_mode(
            mode_scores=mode_scores,
            recommendation=recommendation,
            previous_mode=str(previous_adaptive.get("last_mode", "balanced")).strip().lower() or "balanced",
            features=features,
        )
        target = self._derive_target_config(mode=mode, features=features, adaptive=adaptive)

        current = {
            "exploration_weight": float(self.exploration_weight),
            "cold_start_bonus": float(self.cold_start_bonus),
            "min_trials_per_profile": int(self.min_trials_per_profile),
            "decay": float(self.decay),
        }

        changes: Dict[str, Dict[str, Any]] = {}
        for key in ("exploration_weight", "cold_start_bonus", "min_trials_per_profile", "decay"):
            if target.get(key) != current.get(key):
                changes[key] = {"from": current.get(key), "to": target.get(key)}

        updated_adaptive = dict(adaptive)
        updated_adaptive["last_mode"] = mode
        updated_adaptive["last_mode_reason"] = mode_reason
        updated_adaptive["last_updated_at"] = datetime.now(timezone.utc).isoformat()
        updated_adaptive["tune_runs"] = self._coerce_int(
            adaptive.get("tune_runs", 0),
            minimum=0,
            maximum=1_000_000_000,
            default=0,
        ) + 1
        adaptive_changed = updated_adaptive != previous_adaptive

        applied = False
        if (changes or adaptive_changed) and not dry_run:
            self.exploration_weight = self._coerce_float(
                target["exploration_weight"],
                minimum=0.02,
                maximum=2.5,
                default=self.exploration_weight,
            )
            self.cold_start_bonus = self._coerce_float(
                target["cold_start_bonus"],
                minimum=0.0,
                maximum=1.0,
                default=self.cold_start_bonus,
            )
            self.min_trials_per_profile = self._coerce_int(
                target["min_trials_per_profile"],
                minimum=0,
                maximum=100,
                default=self.min_trials_per_profile,
            )
            self.decay = self._coerce_float(
                target["decay"],
                minimum=0.8,
                maximum=1.0,
                default=self.decay,
            )
            with self._lock:
                self._adaptive_state = dict(updated_adaptive)
                self._maybe_save_locked(force=True)
            applied = True

        state = {
            "status": "success",
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "changed": bool(changes),
            "applied": applied,
            "dry_run": bool(dry_run),
            "reason": str(reason or "").strip() or "manual",
            "changes": changes,
            "mode_reason": mode_reason,
            "mode_scores": mode_scores,
            "signals": features,
            "adaptive_state": updated_adaptive if dry_run else dict(self._adaptive_state),
        }
        self._last_tune = state
        return dict(state)

    def snapshot(self, *, task_class: str = "", limit: int = 200) -> Dict[str, Any]:
        clean_task_class = self._normalize_task_class(task_class) if str(task_class or "").strip() else ""
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        with self._lock:
            classes = dict(self._classes)

        rows: List[Dict[str, Any]] = []
        for class_name, profiles in classes.items():
            if clean_task_class and class_name != clean_task_class:
                continue
            if not isinstance(profiles, dict):
                continue
            for profile_name, row in profiles.items():
                if not isinstance(row, dict):
                    continue
                row_payload = dict(row)
                row_payload["task_class"] = class_name
                row_payload["profile"] = profile_name
                rows.append(row_payload)

        rows.sort(
            key=lambda row: (
                -self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0),
                -self._coerce_float(row.get("reward_mean", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(row.get("task_class", "")),
                str(row.get("profile", "")),
            )
        )
        items = rows[:bounded]
        return {
            "status": "success",
            "task_class": clean_task_class,
            "count": len(items),
            "total": len(rows),
            "tracked_task_classes": len(classes),
            "config": {
                "exploration_weight": self.exploration_weight,
                "cold_start_bonus": self.cold_start_bonus,
                "min_trials_per_profile": self.min_trials_per_profile,
                "decay": self.decay,
                "adaptive_alpha": self._adaptive_alpha,
                "mode_hysteresis_margin": self._mode_hysteresis_margin,
                "adaptive_state": dict(self._adaptive_state),
            },
            "last_tune": dict(self._last_tune),
            "items": items,
        }

    def reset(self, *, task_class: str = "") -> Dict[str, Any]:
        clean_task_class = self._normalize_task_class(task_class) if str(task_class or "").strip() else ""
        removed = 0
        with self._lock:
            if clean_task_class:
                rows = self._classes.pop(clean_task_class, {})
                if isinstance(rows, dict):
                    removed = len(rows)
            else:
                removed = sum(len(rows) for rows in self._classes.values() if isinstance(rows, dict))
                self._classes = {}
            self._maybe_save_locked(force=True)
        return {"status": "success", "task_class": clean_task_class, "removed": removed}

    @staticmethod
    def _normalize_task_class(value: object) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return "default"
        collapsed = "_".join(part for part in raw.replace("/", "_").replace(":", "_").split() if part)
        return collapsed[:120] if collapsed else "default"

    @staticmethod
    def _normalize_profiles(raw_profiles: List[str]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for item in raw_profiles:
            profile = str(item or "").strip().lower()
            if not profile or profile in seen:
                continue
            seen.add(profile)
            out.append(profile)
        return out

    @staticmethod
    def _profile_prior(*, profile: str, task_class: str, source_name: str) -> float:
        clean_profile = str(profile or "").strip().lower()
        clean_task = str(task_class or "").strip().lower()
        clean_source = str(source_name or "").strip().lower()
        prior = 0.0
        if clean_profile == "automation_safe":
            if any(token in clean_task for token in ("schedule", "trigger", "automation")):
                prior += 0.08
            if clean_source in {"desktop-schedule", "desktop-trigger"}:
                prior += 0.06
        elif clean_profile == "interactive":
            if any(token in clean_task for token in ("chat", "query", "info", "summarize")):
                prior += 0.06
        elif clean_profile == "automation_power":
            if any(token in clean_task for token in ("compose", "workflow", "multi_step", "desktop")):
                prior += 0.04
        return prior

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
            self.exploration_weight = self._coerce_float(
                config.get("exploration_weight", self.exploration_weight),
                minimum=0.02,
                maximum=2.5,
                default=self.exploration_weight,
            )
            self.cold_start_bonus = self._coerce_float(
                config.get("cold_start_bonus", self.cold_start_bonus),
                minimum=0.0,
                maximum=1.0,
                default=self.cold_start_bonus,
            )
            self.min_trials_per_profile = self._coerce_int(
                config.get("min_trials_per_profile", self.min_trials_per_profile),
                minimum=0,
                maximum=100,
                default=self.min_trials_per_profile,
            )
            self.decay = self._coerce_float(
                config.get("decay", self.decay),
                minimum=0.8,
                maximum=1.0,
                default=self.decay,
            )
            tune_state = config.get("last_tune", {})
            if isinstance(tune_state, dict):
                self._last_tune.update(
                    {
                        "status": str(tune_state.get("status", self._last_tune.get("status", ""))).strip(),
                        "last_run_at": str(tune_state.get("last_run_at", self._last_tune.get("last_run_at", ""))).strip(),
                        "mode": str(tune_state.get("mode", self._last_tune.get("mode", ""))).strip(),
                        "changed": bool(tune_state.get("changed", self._last_tune.get("changed", False))),
                        "dry_run": bool(tune_state.get("dry_run", self._last_tune.get("dry_run", False))),
                        "reason": str(tune_state.get("reason", self._last_tune.get("reason", ""))).strip(),
                    }
                )
            adaptive_state = config.get("adaptive_state", {})
            if isinstance(adaptive_state, dict):
                self._adaptive_state.update(
                    {
                        "ema_failure_pressure": self._coerce_float(
                            adaptive_state.get("ema_failure_pressure", self._adaptive_state.get("ema_failure_pressure", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_open_breaker_pressure": self._coerce_float(
                            adaptive_state.get("ema_open_breaker_pressure", self._adaptive_state.get("ema_open_breaker_pressure", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_failed_ratio": self._coerce_float(
                            adaptive_state.get("ema_failed_ratio", self._adaptive_state.get("ema_failed_ratio", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_blocked_ratio": self._coerce_float(
                            adaptive_state.get("ema_blocked_ratio", self._adaptive_state.get("ema_blocked_ratio", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_quality": self._coerce_float(
                            adaptive_state.get("ema_quality", self._adaptive_state.get("ema_quality", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_risk": self._coerce_float(
                            adaptive_state.get("ema_risk", self._adaptive_state.get("ema_risk", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_reliability": self._coerce_float(
                            adaptive_state.get("ema_reliability", self._adaptive_state.get("ema_reliability", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_autonomy": self._coerce_float(
                            adaptive_state.get("ema_autonomy", self._adaptive_state.get("ema_autonomy", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_guardrail_pressure": self._coerce_float(
                            adaptive_state.get("ema_guardrail_pressure", self._adaptive_state.get("ema_guardrail_pressure", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "ema_hotspot_pressure": self._coerce_float(
                            adaptive_state.get("ema_hotspot_pressure", self._adaptive_state.get("ema_hotspot_pressure", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "last_mode": str(adaptive_state.get("last_mode", self._adaptive_state.get("last_mode", "balanced"))).strip().lower() or "balanced",
                        "last_mode_reason": str(adaptive_state.get("last_mode_reason", self._adaptive_state.get("last_mode_reason", "initial"))).strip() or "initial",
                        "tune_runs": self._coerce_int(
                            adaptive_state.get("tune_runs", self._adaptive_state.get("tune_runs", 0)),
                            minimum=0,
                            maximum=1_000_000_000,
                            default=0,
                        ),
                        "last_updated_at": str(adaptive_state.get("last_updated_at", self._adaptive_state.get("last_updated_at", ""))).strip(),
                    }
                )

        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return
        loaded: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            task_class = self._normalize_task_class(row.get("task_class", ""))
            profile = str(row.get("profile", "")).strip().lower()
            if not task_class or not profile:
                continue
            class_rows = loaded.setdefault(task_class, {})
            class_rows[profile] = {
                "task_class": task_class,
                "profile": profile,
                "pulls": self._coerce_int(row.get("pulls", 0), minimum=0, maximum=100_000_000, default=0),
                "reward_sum": self._coerce_float(row.get("reward_sum", 0.0), minimum=0.0, maximum=100_000_000.0, default=0.0),
                "reward_mean": self._coerce_float(row.get("reward_mean", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "successes": self._coerce_int(row.get("successes", 0), minimum=0, maximum=100_000_000, default=0),
                "failures": self._coerce_int(row.get("failures", 0), minimum=0, maximum=100_000_000, default=0),
                "blocked": self._coerce_int(row.get("blocked", 0), minimum=0, maximum=100_000_000, default=0),
                "last_reward": self._coerce_float(row.get("last_reward", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "last_outcome": str(row.get("last_outcome", "")).strip().lower(),
                "last_goal_id": str(row.get("last_goal_id", "")).strip(),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        with self._lock:
            self._classes = loaded
            self._trim_locked()

    def _trim_locked(self) -> None:
        if len(self._classes) > self.max_task_classes:
            ordered_classes = sorted(
                self._classes.items(),
                key=lambda item: (
                    max(
                        (
                            str(profile.get("updated_at", ""))
                            for profile in item[1].values()
                            if isinstance(profile, dict)
                        ),
                        default="",
                    ),
                    item[0],
                ),
                reverse=True,
            )
            keep = ordered_classes[: self.max_task_classes]
            self._classes = {key: dict(value) for key, value in keep}

        for task_class, profiles in list(self._classes.items()):
            if not isinstance(profiles, dict):
                self._classes.pop(task_class, None)
                continue
            if len(profiles) <= self.max_profiles_per_class:
                continue
            rows = sorted(
                profiles.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("pulls", 0), minimum=0, maximum=100_000_000, default=0),
                    str(item[1].get("updated_at", "")),
                    item[0],
                ),
                reverse=True,
            )
            self._classes[task_class] = {name: dict(row) for name, row in rows[: self.max_profiles_per_class]}

    def _maybe_save_locked(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force:
            if self._updates_since_save < 14 and (now - self._last_save_monotonic) < 18.0:
                return
        items: List[Dict[str, Any]] = []
        for task_class, profiles in self._classes.items():
            if not isinstance(profiles, dict):
                continue
            for profile, row in profiles.items():
                if not isinstance(row, dict):
                    continue
                payload = dict(row)
                payload["task_class"] = task_class
                payload["profile"] = profile
                items.append(payload)

        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "exploration_weight": self.exploration_weight,
                "cold_start_bonus": self.cold_start_bonus,
                "min_trials_per_profile": self.min_trials_per_profile,
                "decay": self.decay,
                "adaptive_alpha": self._adaptive_alpha,
                "mode_hysteresis_margin": self._mode_hysteresis_margin,
                "last_tune": dict(self._last_tune),
                "adaptive_state": dict(self._adaptive_state),
            },
            "items": items,
        }
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            self.store_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            self._updates_since_save = 0
            self._last_save_monotonic = now
        except Exception:
            return

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

    def _extract_operational_features(self, *, report: Dict[str, Any], missions: Dict[str, Any]) -> Dict[str, float]:
        pressures = report.get("pressures", {}) if isinstance(report.get("pressures", {}), dict) else {}
        scores = report.get("scores", {}) if isinstance(report.get("scores", {}), dict) else {}
        breakers = report.get("circuit_breakers", {}) if isinstance(report.get("circuit_breakers", {}), dict) else {}
        guardrails = report.get("policy_guardrails", {}) if isinstance(report.get("policy_guardrails", {}), dict) else {}
        risk_payload = missions.get("risk", {}) if isinstance(missions.get("risk", {}), dict) else {}
        quality_payload = missions.get("quality", {}) if isinstance(missions.get("quality", {}), dict) else {}
        hotspot_payload = missions.get("hotspots", {}) if isinstance(missions.get("hotspots", {}), dict) else {}

        mission_count = self._coerce_int(missions.get("count", 0), minimum=0, maximum=5_000_000, default=0)
        failure_pressure = self._coerce_float(pressures.get("failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        open_breaker_pressure = self._coerce_float(
            pressures.get("open_breaker_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_failed_ratio = self._coerce_float(missions.get("failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        mission_blocked_ratio = self._coerce_float(missions.get("blocked_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        mission_risk_score = self._coerce_float(risk_payload.get("avg_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        mission_quality_score = self._coerce_float(quality_payload.get("avg_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)

        reliability_norm = self._coerce_float(
            self._coerce_float(scores.get("reliability", 0.0), minimum=0.0, maximum=100.0, default=0.0) / 100.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        autonomy_norm = self._coerce_float(
            self._coerce_float(scores.get("autonomy", 0.0), minimum=0.0, maximum=100.0, default=0.0) / 100.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        critical_guardrails = self._coerce_int(guardrails.get("critical_count", 0), minimum=0, maximum=2_000_000, default=0)
        unstable_guardrails = self._coerce_int(guardrails.get("unstable_count", 0), minimum=0, maximum=2_000_000, default=0)
        open_count = self._coerce_int(breakers.get("open_count", 0), minimum=0, maximum=2_000_000, default=0)
        hotspot_retry_total = self._coerce_int(hotspot_payload.get("retry_total", 0), minimum=0, maximum=2_000_000, default=0)
        hotspot_failure_total = self._coerce_int(
            hotspot_payload.get("failure_total", 0),
            minimum=0,
            maximum=2_000_000,
            default=0,
        )

        guardrail_pressure = self._clamp(
            ((float(critical_guardrails) * 1.2) + (float(unstable_guardrails) * 0.35)) / max(4.0, float(mission_count) * 1.4),
            minimum=0.0,
            maximum=1.0,
        )
        hotspot_pressure = self._clamp(
            ((float(hotspot_failure_total) * 1.0) + (float(hotspot_retry_total) * 0.55)) / max(8.0, float(mission_count) * 4.0),
            minimum=0.0,
            maximum=1.0,
        )
        novelty_pressure = self._clamp(1.0 - (float(mission_count) / 160.0), minimum=0.0, maximum=1.0)

        return {
            "mission_count": float(mission_count),
            "failure_pressure": failure_pressure,
            "open_breaker_pressure": open_breaker_pressure,
            "mission_failed_ratio": mission_failed_ratio,
            "mission_blocked_ratio": mission_blocked_ratio,
            "mission_risk_score": mission_risk_score,
            "mission_quality_score": mission_quality_score,
            "reliability_norm": reliability_norm,
            "autonomy_norm": autonomy_norm,
            "guardrail_pressure": guardrail_pressure,
            "hotspot_pressure": hotspot_pressure,
            "novelty_pressure": novelty_pressure,
            "open_breaker_count": float(open_count),
            "critical_guardrails": float(critical_guardrails),
            "unstable_guardrails": float(unstable_guardrails),
            "hotspot_retry_total": float(hotspot_retry_total),
            "hotspot_failure_total": float(hotspot_failure_total),
        }

    def _advance_adaptive_state(self, *, previous: Dict[str, Any], features: Dict[str, float]) -> Dict[str, Any]:
        alpha = self._adaptive_alpha
        updated = dict(previous)
        ema_keys = {
            "ema_failure_pressure": "failure_pressure",
            "ema_open_breaker_pressure": "open_breaker_pressure",
            "ema_failed_ratio": "mission_failed_ratio",
            "ema_blocked_ratio": "mission_blocked_ratio",
            "ema_quality": "mission_quality_score",
            "ema_risk": "mission_risk_score",
            "ema_reliability": "reliability_norm",
            "ema_autonomy": "autonomy_norm",
            "ema_guardrail_pressure": "guardrail_pressure",
            "ema_hotspot_pressure": "hotspot_pressure",
        }
        for ema_name, feature_name in ema_keys.items():
            previous_value = self._coerce_float(previous.get(ema_name, 0.0), minimum=0.0, maximum=1.0, default=0.0)
            incoming = self._coerce_float(features.get(feature_name, 0.0), minimum=0.0, maximum=1.0, default=0.0)
            blended = ((1.0 - alpha) * previous_value) + (alpha * incoming)
            updated[ema_name] = round(self._clamp(blended, minimum=0.0, maximum=1.0), 6)
        return updated

    def _score_modes(
        self,
        *,
        features: Dict[str, float],
        adaptive: Dict[str, Any],
        recommendation: str,
    ) -> Dict[str, float]:
        blended_failure = (0.65 * features.get("failure_pressure", 0.0)) + (0.35 * self._coerce_float(adaptive.get("ema_failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_breakers = (0.6 * features.get("open_breaker_pressure", 0.0)) + (0.4 * self._coerce_float(adaptive.get("ema_open_breaker_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_failed_ratio = (0.6 * features.get("mission_failed_ratio", 0.0)) + (0.4 * self._coerce_float(adaptive.get("ema_failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_quality = (0.55 * features.get("mission_quality_score", 0.0)) + (0.45 * self._coerce_float(adaptive.get("ema_quality", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_reliability = (0.55 * features.get("reliability_norm", 0.0)) + (0.45 * self._coerce_float(adaptive.get("ema_reliability", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_autonomy = (0.55 * features.get("autonomy_norm", 0.0)) + (0.45 * self._coerce_float(adaptive.get("ema_autonomy", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_guardrail = (0.58 * features.get("guardrail_pressure", 0.0)) + (0.42 * self._coerce_float(adaptive.get("ema_guardrail_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0))
        blended_hotspot = (0.58 * features.get("hotspot_pressure", 0.0)) + (0.42 * self._coerce_float(adaptive.get("ema_hotspot_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0))

        recommendation_bias = 0.06
        stability_score = self._clamp(
            (0.32 * blended_failure)
            + (0.16 * blended_breakers)
            + (0.18 * blended_failed_ratio)
            + (0.12 * (1.0 - blended_quality))
            + (0.1 * features.get("mission_risk_score", 0.0))
            + (0.07 * blended_guardrail)
            + (0.05 * blended_hotspot),
            minimum=0.0,
            maximum=1.0,
        )
        throughput_score = self._clamp(
            (0.28 * blended_reliability)
            + (0.2 * blended_autonomy)
            + (0.22 * blended_quality)
            + (0.12 * (1.0 - blended_failure))
            + (0.08 * (1.0 - blended_breakers))
            + (0.06 * (1.0 - blended_guardrail))
            + (0.04 * (1.0 - blended_hotspot)),
            minimum=0.0,
            maximum=1.0,
        )
        if recommendation == "stability":
            stability_score = self._clamp(stability_score + recommendation_bias, minimum=0.0, maximum=1.0)
        elif recommendation == "throughput":
            throughput_score = self._clamp(throughput_score + recommendation_bias, minimum=0.0, maximum=1.0)

        balance_score = self._clamp(
            1.0
            - abs(stability_score - throughput_score)
            - (0.32 * max(stability_score, throughput_score)),
            minimum=0.0,
            maximum=1.0,
        )
        return {
            "stability": round(stability_score, 6),
            "throughput": round(throughput_score, 6),
            "balanced": round(balance_score, 6),
        }

    def _select_mode(
        self,
        *,
        mode_scores: Dict[str, float],
        recommendation: str,
        previous_mode: str,
        features: Dict[str, float],
    ) -> tuple[str, str]:
        ranked = sorted(
            (("stability", float(mode_scores.get("stability", 0.0))), ("throughput", float(mode_scores.get("throughput", 0.0))), ("balanced", float(mode_scores.get("balanced", 0.0)))),
            key=lambda item: (-item[1], item[0]),
        )
        mode = ranked[0][0]
        top_score = ranked[0][1]
        runner_up = ranked[1][1] if len(ranked) > 1 else 0.0
        reason = "score_peak"

        if (
            features.get("failure_pressure", 0.0) >= 0.42
            or features.get("open_breaker_pressure", 0.0) >= 0.24
            or features.get("mission_failed_ratio", 0.0) >= 0.35
        ):
            mode = "stability"
            reason = "safety_override"
        elif (
            features.get("reliability_norm", 0.0) >= 0.9
            and features.get("autonomy_norm", 0.0) >= 0.84
            and features.get("mission_quality_score", 0.0) >= 0.82
            and features.get("mission_failed_ratio", 0.0) <= 0.08
            and features.get("open_breaker_pressure", 0.0) <= 0.05
            and features.get("guardrail_pressure", 0.0) <= 0.08
        ):
            mode = "throughput"
            reason = "throughput_override"
        elif recommendation in {"stability", "throughput"} and mode != recommendation and abs(top_score - float(mode_scores.get(recommendation, 0.0))) <= 0.07:
            mode = recommendation
            reason = "mission_recommendation_bias"

        clean_previous = previous_mode if previous_mode in {"stability", "throughput", "balanced"} else "balanced"
        if mode != clean_previous and (top_score - runner_up) < self._mode_hysteresis_margin:
            mode = clean_previous
            reason = "hysteresis_hold"
        return mode, reason

    def _derive_target_config(
        self,
        *,
        mode: str,
        features: Dict[str, float],
        adaptive: Dict[str, Any],
    ) -> Dict[str, Any]:
        failure_trend = self._clamp(
            (0.55 * max(features.get("failure_pressure", 0.0), features.get("mission_failed_ratio", 0.0)))
            + (0.45 * self._coerce_float(adaptive.get("ema_failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)),
            minimum=0.0,
            maximum=1.0,
        )
        guardrail_trend = self._clamp(
            (0.6 * features.get("guardrail_pressure", 0.0))
            + (0.4 * self._coerce_float(adaptive.get("ema_guardrail_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)),
            minimum=0.0,
            maximum=1.0,
        )
        throughput_trend = self._clamp(
            (0.6 * max(features.get("reliability_norm", 0.0), features.get("autonomy_norm", 0.0)))
            + (0.4 * self._coerce_float(adaptive.get("ema_reliability", 0.0), minimum=0.0, maximum=1.0, default=0.0)),
            minimum=0.0,
            maximum=1.0,
        )
        novelty = self._coerce_float(features.get("novelty_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        quality_gap = self._clamp(1.0 - features.get("mission_quality_score", 0.0), minimum=0.0, maximum=1.0)

        if mode == "stability":
            exploration_weight = self._clamp(0.11 + (0.08 * novelty) - (0.1 * failure_trend), minimum=0.08, maximum=0.25)
            cold_start_bonus = self._clamp(0.07 + (0.11 * novelty) - (0.06 * failure_trend), minimum=0.04, maximum=0.2)
            min_trials = int(round(self._clamp(3.0 + (3.2 * failure_trend) + (1.8 * guardrail_trend) - (0.8 * novelty), minimum=2.0, maximum=8.0)))
            decay = self._clamp(0.987 + (0.008 * failure_trend) + (0.003 * quality_gap), minimum=0.97, maximum=0.998)
        elif mode == "throughput":
            exploration_weight = self._clamp(
                0.24 + (0.2 * throughput_trend) + (0.08 * novelty) - (0.06 * failure_trend),
                minimum=0.2,
                maximum=0.65,
            )
            cold_start_bonus = self._clamp(
                0.16 + (0.17 * novelty) + (0.05 * throughput_trend) - (0.05 * guardrail_trend),
                minimum=0.12,
                maximum=0.45,
            )
            min_trials = int(round(self._clamp(1.4 + (2.0 * failure_trend) + (1.2 * guardrail_trend), minimum=1.0, maximum=5.0)))
            decay = self._clamp(0.953 + (0.018 * (1.0 - throughput_trend)) + (0.01 * failure_trend), minimum=0.93, maximum=0.989)
        else:
            exploration_weight = self._clamp(
                0.18 + (0.1 * novelty) + (0.08 * throughput_trend) - (0.08 * failure_trend),
                minimum=0.14,
                maximum=0.42,
            )
            cold_start_bonus = self._clamp(
                0.12 + (0.11 * novelty) + (0.04 * throughput_trend) - (0.04 * failure_trend),
                minimum=0.09,
                maximum=0.3,
            )
            min_trials = int(
                round(
                    self._clamp(
                        2.1 + (2.0 * failure_trend) + (1.2 * guardrail_trend) - (1.0 * throughput_trend),
                        minimum=1.0,
                        maximum=7.0,
                    )
                )
            )
            decay = self._clamp(0.972 + (0.011 * failure_trend) + (0.004 * guardrail_trend), minimum=0.955, maximum=0.995)

        return {
            "exploration_weight": round(exploration_weight, 6),
            "cold_start_bonus": round(cold_start_bonus, 6),
            "min_trials_per_profile": int(min_trials),
            "decay": round(decay, 6),
        }
