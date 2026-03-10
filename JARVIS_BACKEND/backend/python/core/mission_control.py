from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

from .contracts import ActionResult, ExecutionPlan, PlanStep


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class MissionStepRecord:
    sequence: int
    event: str
    step_id: str
    action: str
    status: str
    attempt: int
    completed_at: str
    goal_id: str = ""
    plan_id: str = ""
    args_fingerprint: str = ""
    error: str = ""
    duration_ms: int = 0
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MissionRecord:
    mission_id: str
    root_goal_id: str
    latest_goal_id: str
    text: str
    source: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "running"
    created_at: str = field(default_factory=lambda: _iso(_utc_now()))
    updated_at: str = field(default_factory=lambda: _iso(_utc_now()))
    last_checkpoint_at: str = ""
    last_error: str = ""
    resume_count: int = 0
    active_step_id: str = ""
    active_plan_id: str = ""
    active_goal_id: str = ""
    last_step_sequence: int = 0
    step_status: Dict[str, str] = field(default_factory=dict)
    step_attempts: Dict[str, int] = field(default_factory=dict)
    resume_cursor: Dict[str, Any] = field(default_factory=dict)
    plan: Dict[str, Any] = field(default_factory=dict)
    checkpoints: List[MissionStepRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["checkpoints"] = [item.to_dict() for item in self.checkpoints]
        return payload


class MissionControl:
    """
    Long-running mission lifecycle storage with checkpoint and resume support.
    """

    TERMINAL_STATUSES = {"completed", "failed", "blocked", "cancelled"}

    def __init__(self, *, store_path: str = "data/missions.json", max_records: int = 5000, max_checkpoints: int = 1200) -> None:
        self.store_path = Path(store_path)
        self.max_records = max(300, min(int(max_records), 100_000))
        self.max_checkpoints = max(10, min(int(max_checkpoints), 20_000))
        self._records: Dict[str, MissionRecord] = {}
        self._goal_to_mission: Dict[str, str] = {}
        self._lock = RLock()
        self._load_locked()

    def create_for_goal(
        self,
        *,
        goal_id: str,
        text: str,
        source: str,
        metadata: Optional[Dict[str, Any]] = None,
        mission_id: str = "",
    ) -> MissionRecord:
        clean_goal = str(goal_id or "").strip()
        clean_text = str(text or "").strip()
        clean_source = str(source or "").strip() or "desktop-ui"
        if not clean_goal:
            raise ValueError("goal_id is required")
        if not clean_text:
            raise ValueError("text is required")

        requested = str(mission_id or "").strip()
        if not requested and isinstance(metadata, dict):
            requested = str(metadata.get("__jarvis_mission_id", "")).strip()

        with self._lock:
            if requested and requested in self._records:
                record = self._records[requested]
                record.latest_goal_id = clean_goal
                record.active_goal_id = clean_goal
                record.updated_at = _iso(_utc_now())
                record.status = "running"
                self._goal_to_mission[clean_goal] = record.mission_id
                self._save_locked()
                return record

            now_iso = _iso(_utc_now())
            clean_metadata = dict(metadata or {})
            record = MissionRecord(
                mission_id=str(uuid.uuid4()),
                root_goal_id=clean_goal,
                latest_goal_id=clean_goal,
                text=clean_text,
                source=clean_source,
                metadata=clean_metadata,
                status="running",
                created_at=now_iso,
                updated_at=now_iso,
                active_goal_id=clean_goal,
            )
            self._records[record.mission_id] = record
            self._goal_to_mission[clean_goal] = record.mission_id
            self._trim_locked()
            self._save_locked()
            return record

    def mission_for_goal(self, goal_id: str) -> str:
        with self._lock:
            return str(self._goal_to_mission.get(str(goal_id or "").strip(), ""))

    def bind_goal(self, mission_id: str, goal_id: str) -> bool:
        clean_mission = str(mission_id or "").strip()
        clean_goal = str(goal_id or "").strip()
        if not clean_mission or not clean_goal:
            return False
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return False
            record.latest_goal_id = clean_goal
            record.active_goal_id = clean_goal
            record.updated_at = _iso(_utc_now())
            self._goal_to_mission[clean_goal] = clean_mission
            self._save_locked()
            return True

    def set_plan(self, mission_id: str, plan: ExecutionPlan | Dict[str, Any]) -> None:
        clean_mission = str(mission_id or "").strip()
        if not clean_mission:
            return
        serialized = self._serialize_plan(plan)
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return
            record.plan = serialized
            plan_id = str(serialized.get("plan_id", "")).strip()
            if plan_id:
                record.active_plan_id = plan_id
            record.updated_at = _iso(_utc_now())
            self._save_locked()

    def checkpoint_step_started(
        self,
        mission_id: str,
        *,
        goal_id: str,
        plan_id: str,
        step: PlanStep,
        attempt: int = 1,
    ) -> None:
        clean_mission = str(mission_id or "").strip()
        if not clean_mission:
            return
        if not isinstance(step, PlanStep):
            return

        step_id = str(step.step_id or "").strip()
        if not step_id:
            step_id = f"{step.action}:{max(1, int(attempt))}"
        args_payload = step.args if isinstance(step.args, dict) else {}
        row = MissionStepRecord(
            sequence=0,
            event="started",
            step_id=step_id,
            action=str(step.action or "").strip(),
            status="running",
            attempt=max(1, int(attempt)),
            completed_at=_iso(_utc_now()),
            goal_id=str(goal_id or "").strip(),
            plan_id=str(plan_id or "").strip(),
            args_fingerprint=self._args_fingerprint(args_payload),
            evidence={
                "step_id": step_id,
                "request": {
                    "action": str(step.action or "").strip(),
                    "args": args_payload,
                },
            },
        )
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return
            self._append_checkpoint_locked(record, row)

    def checkpoint_step_finished(
        self,
        mission_id: str,
        result: ActionResult,
        *,
        goal_id: str = "",
        plan_id: str = "",
        step_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        clean_mission = str(mission_id or "").strip()
        if not clean_mission:
            return
        if not isinstance(result, ActionResult):
            return

        evidence = result.evidence if isinstance(result.evidence, dict) else {}
        step_id = str(evidence.get("step_id", "")).strip()
        if not step_id:
            step_id = f"{result.action}:{max(1, int(result.attempt))}"
        fingerprint_payload: Dict[str, Any] = {}
        if isinstance(step_args, dict):
            fingerprint_payload = step_args
        else:
            request_payload = evidence.get("request")
            if isinstance(request_payload, dict):
                candidate = request_payload.get("args")
                if isinstance(candidate, dict):
                    fingerprint_payload = candidate
        row = MissionStepRecord(
            sequence=0,
            event="finished",
            step_id=step_id,
            action=str(result.action or "").strip(),
            status=self._normalize_step_status(result.status),
            attempt=max(1, int(result.attempt)),
            completed_at=str(result.completed_at or _iso(_utc_now())),
            goal_id=str(goal_id or "").strip(),
            plan_id=str(plan_id or "").strip(),
            args_fingerprint=self._args_fingerprint(fingerprint_payload),
            error=str(result.error or "").strip(),
            duration_ms=max(0, int(result.duration_ms)),
            evidence=self._sanitize_evidence(evidence),
        )
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return
            self._append_checkpoint_locked(record, row)

    def checkpoint_step(self, mission_id: str, result: ActionResult) -> None:
        # Backward-compatible alias used by existing call sites/tests.
        self.checkpoint_step_finished(mission_id, result)

    def mark_finished(self, mission_id: str, *, status: str, error: str = "") -> None:
        clean_mission = str(mission_id or "").strip()
        if not clean_mission:
            return
        clean_status = str(status or "").strip().lower() or "failed"
        if clean_status not in {"running", "completed", "failed", "blocked", "cancelled"}:
            clean_status = "failed"
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return
            now_iso = _iso(_utc_now())
            record.status = clean_status
            if clean_status in {"failed", "blocked"} and error:
                record.last_error = str(error).strip()
            elif clean_status == "cancelled" and error:
                record.last_error = str(error).strip()
            elif clean_status == "completed":
                record.last_error = ""
                record.active_step_id = ""
                record.active_goal_id = ""
                record.active_plan_id = ""
                record.resume_cursor = {}
            elif clean_status in {"failed", "blocked", "cancelled"}:
                record.active_step_id = ""
            record.updated_at = now_iso
            self._save_locked()

    def mark_resumed(self, mission_id: str, *, new_goal_id: str) -> None:
        clean_mission = str(mission_id or "").strip()
        clean_goal = str(new_goal_id or "").strip()
        if not clean_mission or not clean_goal:
            return
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return
            record.latest_goal_id = clean_goal
            record.status = "running"
            record.resume_count = max(0, int(record.resume_count)) + 1
            record.active_goal_id = clean_goal
            record.updated_at = _iso(_utc_now())
            self._goal_to_mission[clean_goal] = clean_mission
            self._save_locked()

    def get(self, mission_id: str) -> Optional[Dict[str, Any]]:
        clean_mission = str(mission_id or "").strip()
        with self._lock:
            record = self._records.get(clean_mission)
            return record.to_dict() if record else None

    def list(self, *, status: str = "", limit: int = 100) -> Dict[str, Any]:
        normalized = str(status or "").strip().lower()
        bounded = max(1, min(int(limit), 2000))
        with self._lock:
            rows = list(self._records.values())
        if normalized:
            rows = [item for item in rows if item.status == normalized]
        rows.sort(key=lambda item: item.updated_at, reverse=True)
        items = [item.to_dict() for item in rows[:bounded]]
        return {"status": "success", "items": items, "count": len(items), "total": len(rows)}

    def timeline(
        self,
        mission_id: str,
        *,
        limit: int = 200,
        event: str = "",
        step_id: str = "",
        status: str = "",
        descending: bool = True,
    ) -> Dict[str, Any]:
        clean_mission = str(mission_id or "").strip()
        if not clean_mission:
            return {"status": "error", "message": "mission id is required"}

        normalized_event = str(event or "").strip().lower()
        normalized_step = str(step_id or "").strip()
        normalized_status = self._normalize_step_status(status) if str(status or "").strip() else ""
        bounded_limit = max(1, min(int(limit), 5000))

        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return {"status": "error", "message": "mission not found"}
            rows = list(record.checkpoints)
            active_step_id = str(record.active_step_id or "").strip()
            active_goal_id = str(record.active_goal_id or "").strip()
            active_plan_id = str(record.active_plan_id or "").strip()
            checkpoint_sequence = max(0, int(record.last_step_sequence))

        if normalized_event:
            rows = [item for item in rows if str(item.event or "").strip().lower() == normalized_event]
        if normalized_step:
            rows = [item for item in rows if str(item.step_id or "").strip() == normalized_step]
        if normalized_status:
            rows = [item for item in rows if self._normalize_step_status(item.status) == normalized_status]

        rows.sort(key=lambda item: (max(0, int(item.sequence or 0)), str(item.completed_at or "")), reverse=bool(descending))
        items = [item.to_dict() for item in rows[:bounded_limit]]
        return {
            "status": "success",
            "mission_id": clean_mission,
            "items": items,
            "count": len(items),
            "total": len(rows),
            "active_step_id": active_step_id,
            "active_goal_id": active_goal_id,
            "active_plan_id": active_plan_id,
            "checkpoint_sequence": checkpoint_sequence,
        }

    def resume_preview(self, mission_id: str) -> Dict[str, Any]:
        return self.build_resume_payload(mission_id)

    def diagnostics(self, mission_id: str, *, hotspot_limit: int = 8) -> Dict[str, Any]:
        clean_mission = str(mission_id or "").strip()
        if not clean_mission:
            return {"status": "error", "message": "mission id is required"}
        bounded_hotspots = max(1, min(int(hotspot_limit), 50))

        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return {"status": "error", "message": "mission not found"}
            mission_snapshot = record.to_dict()
            step_status = dict(record.step_status)
            step_attempts = dict(record.step_attempts)
            checkpoints = list(record.checkpoints)
            plan_snapshot = json.loads(json.dumps(record.plan)) if isinstance(record.plan, dict) else {}
            mission_status = str(record.status or "").strip().lower()
            active_step_id = str(record.active_step_id or "").strip()
            last_checkpoint_at = str(record.last_checkpoint_at or "").strip()
            last_error = str(record.last_error or "").strip()

        steps_raw = plan_snapshot.get("steps", [])
        steps = [item for item in steps_raw if isinstance(item, dict)] if isinstance(steps_raw, list) else []
        merged_status = self._merge_step_status(step_status, checkpoints)
        known_step_ids = {
            str(item.get("step_id", "")).strip()
            for item in steps
            if isinstance(item, dict) and str(item.get("step_id", "")).strip()
        }

        status_counts: Dict[str, int] = {key: 0 for key in ("pending", "running", "success", "failed", "blocked", "skipped")}
        dependency_issues: List[Dict[str, Any]] = []
        dependency_edges = 0
        unresolved_edges = 0
        missing_edges = 0
        step_order: Dict[str, int] = {}

        for index, step in enumerate(steps):
            step_id = str(step.get("step_id", "")).strip()
            if step_id:
                step_order[step_id] = index + 1
            status_value = self._normalize_step_status(merged_status.get(step_id, "pending"))
            status_counts[status_value] = status_counts.get(status_value, 0) + 1

            deps_raw = step.get("depends_on", [])
            deps = [str(dep).strip() for dep in deps_raw if str(dep).strip()] if isinstance(deps_raw, list) else []
            dependency_edges += len(deps)
            missing = [dep for dep in deps if dep not in known_step_ids]
            unresolved = [dep for dep in deps if self._normalize_step_status(merged_status.get(dep, "pending")) != "success"]
            unresolved_non_missing = [dep for dep in unresolved if dep not in missing]
            missing_edges += len(missing)
            unresolved_edges += len(unresolved_non_missing)

            if status_value != "success" and (missing or unresolved_non_missing):
                dependency_issues.append(
                    {
                        "step_id": step_id,
                        "status": status_value,
                        "depends_on": deps,
                        "missing_dependencies": missing,
                        "unresolved_dependencies": unresolved_non_missing,
                    }
                )

        if not steps and merged_status:
            for value in merged_status.values():
                normalized = self._normalize_step_status(value)
                status_counts[normalized] = status_counts.get(normalized, 0) + 1

        checkpoints_sorted = sorted(
            checkpoints,
            key=lambda item: (max(0, int(getattr(item, "sequence", 0))), str(getattr(item, "completed_at", ""))),
        )
        latest_finished_by_step: Dict[str, MissionStepRecord] = {}
        duration_by_step: Dict[str, List[int]] = {}
        for row in checkpoints_sorted:
            if str(row.event or "").strip().lower() != "finished":
                continue
            step_id = str(row.step_id or "").strip()
            if not step_id:
                continue
            latest_finished_by_step[step_id] = row
            duration = max(0, int(row.duration_ms or 0))
            if duration > 0:
                duration_by_step.setdefault(step_id, []).append(duration)

        retry_hotspots_all: List[Dict[str, Any]] = []
        for step_id, attempts in step_attempts.items():
            clean_id = str(step_id or "").strip()
            if not clean_id:
                continue
            attempt_count = max(1, int(attempts or 1))
            if attempt_count <= 1:
                continue
            latest = latest_finished_by_step.get(clean_id)
            retry_hotspots_all.append(
                {
                    "step_id": clean_id,
                    "attempts": attempt_count,
                    "status": self._normalize_step_status(merged_status.get(clean_id, "pending")),
                    "action": str(latest.action or "").strip() if latest else "",
                    "last_error": str(latest.error or "").strip() if latest else "",
                    "sequence": max(0, int(latest.sequence or 0)) if latest else 0,
                    "plan_index": int(step_order.get(clean_id, 0)),
                }
            )
        retry_hotspots_all.sort(
            key=lambda item: (
                -int(item.get("attempts", 0)),
                -int(item.get("sequence", 0)),
                str(item.get("step_id", "")),
            )
        )

        slow_hotspots_all: List[Dict[str, Any]] = []
        for step_id, values in duration_by_step.items():
            if not values:
                continue
            latest = latest_finished_by_step.get(step_id)
            count = len(values)
            average_ms = int(round(sum(values) / max(1, count)))
            max_ms = max(values)
            slow_hotspots_all.append(
                {
                    "step_id": step_id,
                    "samples": count,
                    "avg_duration_ms": average_ms,
                    "max_duration_ms": max_ms,
                    "status": self._normalize_step_status(merged_status.get(step_id, "pending")),
                    "action": str(latest.action or "").strip() if latest else "",
                    "plan_index": int(step_order.get(step_id, 0)),
                }
            )
        slow_hotspots_all.sort(
            key=lambda item: (
                -int(item.get("max_duration_ms", 0)),
                -int(item.get("avg_duration_ms", 0)),
                str(item.get("step_id", "")),
            )
        )

        failure_hotspots_all: List[Dict[str, Any]] = []
        for step_id, row in latest_finished_by_step.items():
            status_value = self._normalize_step_status(row.status)
            if status_value not in {"failed", "blocked"}:
                continue
            failure_hotspots_all.append(
                {
                    "step_id": step_id,
                    "status": status_value,
                    "action": str(row.action or "").strip(),
                    "attempt": max(1, int(row.attempt or 1)),
                    "sequence": max(0, int(row.sequence or 0)),
                    "error": str(row.error or "").strip(),
                    "completed_at": str(row.completed_at or ""),
                    "plan_index": int(step_order.get(step_id, 0)),
                }
            )
        failure_hotspots_all.sort(
            key=lambda item: (
                -int(item.get("sequence", 0)),
                -int(item.get("attempt", 0)),
                str(item.get("step_id", "")),
            )
        )

        total_steps = len(steps) if steps else len([item for item in merged_status.keys() if str(item).strip()])
        failed_count = int(status_counts.get("failed", 0))
        blocked_count = int(status_counts.get("blocked", 0))
        running_count = int(status_counts.get("running", 0))
        retry_hotspot_count = len(retry_hotspots_all)
        dependency_issue_count = len(dependency_issues)

        resume_payload = self.build_resume_payload(clean_mission)
        resume_ready = str(resume_payload.get("status", "")).strip().lower() == "success"
        resume_remaining_steps = int(resume_payload.get("remaining_steps", 0) or 0) if resume_ready else 0
        resume_error = "" if resume_ready else str(resume_payload.get("message", "")).strip()

        if total_steps <= 0:
            if mission_status in {"failed", "blocked"}:
                risk_score = 0.72
            elif mission_status == "running":
                risk_score = 0.42
            elif mission_status == "cancelled":
                risk_score = 0.35
            else:
                risk_score = 0.16
        else:
            fail_ratio = (failed_count + blocked_count) / max(1, total_steps)
            unresolved_ratio = dependency_issue_count / max(1, total_steps)
            retry_ratio = min(1.0, retry_hotspot_count / max(1, total_steps))
            running_ratio = running_count / max(1, total_steps)
            risk_score = min(1.0, (fail_ratio * 0.58) + (unresolved_ratio * 0.24) + (retry_ratio * 0.12) + (running_ratio * 0.06))

        if mission_status in {"failed", "blocked"}:
            risk_score = max(risk_score, 0.58)
        if mission_status == "running" and (failed_count + blocked_count) > 0:
            risk_score = max(risk_score, 0.45)
        if mission_status == "completed":
            risk_score = min(risk_score, 0.2)
        if not resume_ready and mission_status in {"failed", "blocked", "running"}:
            risk_score = min(1.0, risk_score + 0.08)
        if last_error and mission_status != "completed":
            risk_score = min(1.0, risk_score + 0.04)

        if risk_score >= 0.72:
            risk_level = "high"
        elif risk_score >= 0.4:
            risk_level = "medium"
        else:
            risk_level = "low"

        risk_reasons: List[str] = []
        if failed_count > 0 or blocked_count > 0:
            risk_reasons.append(f"{failed_count + blocked_count} step(s) are failed or blocked.")
        if dependency_issue_count > 0:
            risk_reasons.append(f"{dependency_issue_count} step(s) have unresolved dependencies.")
        if retry_hotspot_count > 0:
            risk_reasons.append(f"{retry_hotspot_count} step(s) retried more than once.")
        if not resume_ready and mission_status in {"failed", "blocked", "running"}:
            risk_reasons.append("Mission resume payload is not currently buildable.")
        if not risk_reasons:
            risk_reasons.append("No critical mission risk signals detected.")

        success_count = int(status_counts.get("success", 0))
        success_ratio = (success_count / max(1, total_steps)) if total_steps > 0 else (1.0 if mission_status == "completed" else 0.0)
        retry_pressure = min(1.0, retry_hotspot_count / max(1, total_steps))
        verification_failures = 0
        confirm_policy_failures = 0
        for row in failure_hotspots_all:
            if not isinstance(row, dict):
                continue
            message = str(row.get("error", "")).strip().lower()
            if not message:
                continue
            if "verification" in message or "confirm" in message:
                verification_failures += 1
            if "confirm policy failed" in message:
                confirm_policy_failures += 1
        verification_pressure = min(1.0, verification_failures / max(1, total_steps))
        quality_score = (
            (success_ratio * 0.55)
            + ((1.0 - risk_score) * 0.25)
            + ((1.0 - retry_pressure) * 0.1)
            + ((1.0 - verification_pressure) * 0.1)
        )
        quality_score = max(0.0, min(1.0, quality_score))
        if mission_status in {"failed", "blocked"}:
            quality_score = min(quality_score, 0.45)
        elif mission_status == "completed":
            quality_score = max(quality_score, 0.62 if failed_count == 0 and blocked_count == 0 else 0.48)
        if quality_score >= 0.8:
            quality_level = "high"
        elif quality_score >= 0.58:
            quality_level = "medium"
        else:
            quality_level = "low"

        if risk_level == "high" or verification_pressure >= 0.35:
            recommended_recovery_profile = "safe"
            recommended_verification_strictness = "strict"
        elif risk_level == "medium" or retry_pressure >= 0.25:
            recommended_recovery_profile = "balanced"
            recommended_verification_strictness = "standard"
        else:
            recommended_recovery_profile = "aggressive" if success_ratio >= 0.9 and mission_status in {"completed", "running"} else "balanced"
            recommended_verification_strictness = "standard"

        recommendations: List[str] = []
        if dependency_issue_count > 0:
            recommendations.append("Repair missing or unresolved step dependencies before resume.")
        if failure_hotspots_all:
            recommendations.append("Inspect failed step errors and tighten verify checks for those actions.")
        if retry_hotspot_count > 0:
            recommendations.append("Tune retries/timeouts for hotspot steps and add stronger precondition checks.")
        if verification_failures > 0:
            recommendations.append("Increase verification strictness or add confirm checks for actions failing postcondition validation.")
        if confirm_policy_failures > 0:
            recommendations.append("Adjust confirm policy mode/min_success or strengthen UI targeting to reduce confirm-policy failures.")
        if mission_status in {"failed", "blocked"} and resume_ready:
            recommendations.append("Resume mission with a suitable recovery profile after confirming external preconditions.")
        if mission_status == "running" and failed_count + blocked_count > 0:
            recommendations.append("Consider cancelling current execution and resuming from the latest stable cursor.")
        if not recommendations:
            recommendations.append("Continue mission monitoring; no immediate intervention required.")

        return {
            "status": "success",
            "mission_id": clean_mission,
            "mission_status": mission_status,
            "mission": mission_snapshot,
            "plan": {
                "plan_id": str(plan_snapshot.get("plan_id", "")).strip(),
                "step_count": total_steps,
                "dependency_edges": dependency_edges,
                "unresolved_dependency_edges": unresolved_edges,
                "missing_dependency_edges": missing_edges,
            },
            "execution": {
                "checkpoint_count": len(checkpoints),
                "last_checkpoint_at": last_checkpoint_at,
                "active_step_id": active_step_id,
                "last_error": last_error,
            },
            "step_counts": status_counts,
            "dependency_issues": dependency_issues[:bounded_hotspots],
            "hotspots": {
                "retry": retry_hotspots_all[:bounded_hotspots],
                "slow": slow_hotspots_all[:bounded_hotspots],
                "failures": failure_hotspots_all[:bounded_hotspots],
            },
            "resume": {
                "ready": resume_ready,
                "remaining_steps": resume_remaining_steps,
                "error": resume_error,
            },
            "risk": {
                "score": round(risk_score, 4),
                "level": risk_level,
                "reasons": risk_reasons,
            },
            "quality": {
                "score": round(quality_score, 4),
                "level": quality_level,
                "success_ratio": round(success_ratio, 4),
                "retry_pressure": round(retry_pressure, 4),
                "verification_pressure": round(verification_pressure, 4),
                "verification_failures": verification_failures,
                "confirm_policy_failures": confirm_policy_failures,
                "recommended_recovery_profile": recommended_recovery_profile,
                "recommended_verification_strictness": recommended_verification_strictness,
            },
            "recommendations": recommendations,
        }

    def build_resume_payload(self, mission_id: str) -> Dict[str, Any]:
        clean_mission = str(mission_id or "").strip()
        with self._lock:
            record = self._records.get(clean_mission)
            if record is None:
                return {"status": "error", "message": "mission not found"}
            if not record.plan:
                return {"status": "error", "message": "mission has no plan snapshot"}
            if record.status == "completed":
                return {"status": "error", "message": "mission already completed"}
            plan_snapshot = json.loads(json.dumps(record.plan))
            mission_snapshot = record.to_dict()
            checkpoint_sequence = max(0, int(record.last_step_sequence))
            step_status = dict(record.step_status)
            step_attempts = dict(record.step_attempts)
            active_step_id = str(record.active_step_id or "").strip()
            active_plan_id = str(record.active_plan_id or "").strip()
            active_goal_id = str(record.active_goal_id or "").strip()
            checkpoints = list(record.checkpoints)

        steps = plan_snapshot.get("steps")
        if not isinstance(steps, list) or not steps:
            return {"status": "error", "message": "mission plan has no steps"}

        merged_status = self._merge_step_status(step_status, checkpoints)
        completed_ids = {step_id for step_id, status in merged_status.items() if status == "success"}
        cursor = self._resolve_resume_cursor(
            steps=steps,
            step_status=merged_status,
            completed_step_ids=completed_ids,
            active_step_id=active_step_id,
        )
        if cursor is None:
            return {"status": "error", "message": "mission has no remaining steps to resume"}

        remaining: List[Dict[str, Any]] = []
        for raw_step in steps[cursor["index"] :]:
            if not isinstance(raw_step, dict):
                continue
            step_id = str(raw_step.get("step_id", "")).strip()
            if step_id and step_id in completed_ids:
                continue
            step = dict(raw_step)
            deps = step.get("depends_on")
            if isinstance(deps, list):
                step["depends_on"] = [str(dep).strip() for dep in deps if str(dep).strip() and str(dep).strip() not in completed_ids]
            remaining.append(step)

        if not remaining:
            return {"status": "error", "message": "mission has no remaining steps to resume"}

        resumed_plan = dict(plan_snapshot)
        resumed_plan["plan_id"] = f"{plan_snapshot.get('plan_id', 'plan')}-resume-{uuid.uuid4().hex[:8]}"
        resumed_plan["steps"] = remaining
        context = resumed_plan.get("context", {})
        if not isinstance(context, dict):
            context = {}
        context["resume_mode"] = True
        context["resume_mission_id"] = clean_mission
        context["resume_completed_step_ids"] = sorted(completed_ids)
        context["resume_cursor_step_id"] = cursor["step_id"]
        context["resume_cursor_index"] = cursor["index"]
        context["resume_cursor_status"] = cursor["status"]
        context["resume_checkpoint_sequence"] = checkpoint_sequence
        context["resume_step_status"] = dict(merged_status)
        context["resume_step_attempts"] = dict(step_attempts)
        context["resume_active_step_id"] = active_step_id
        context["resume_active_plan_id"] = active_plan_id
        context["resume_active_goal_id"] = active_goal_id
        resumed_plan["context"] = context

        resume_cursor = {
            "step_id": cursor["step_id"],
            "index": int(cursor["index"]),
            "status": cursor["status"],
            "checkpoint_sequence": checkpoint_sequence,
            "active_step_id": active_step_id,
            "active_plan_id": active_plan_id,
            "active_goal_id": active_goal_id,
        }

        return {
            "status": "success",
            "mission": mission_snapshot,
            "resume_plan": resumed_plan,
            "completed_step_ids": sorted(completed_ids),
            "remaining_steps": len(remaining),
            "resume_cursor": resume_cursor,
            "step_status": merged_status,
        }

    def _append_checkpoint_locked(self, record: MissionRecord, row: MissionStepRecord) -> None:
        record.last_step_sequence = max(0, int(record.last_step_sequence)) + 1
        row.sequence = record.last_step_sequence
        if not row.completed_at:
            row.completed_at = _iso(_utc_now())

        record.checkpoints.append(row)
        if len(record.checkpoints) > self.max_checkpoints:
            overflow = len(record.checkpoints) - self.max_checkpoints
            if overflow > 0:
                record.checkpoints = record.checkpoints[overflow:]

        step_id = str(row.step_id or "").strip()
        if step_id:
            record.step_attempts[step_id] = max(max(1, int(row.attempt)), int(record.step_attempts.get(step_id, 0) or 0))
            record.step_status[step_id] = self._normalize_step_status(row.status)

        if row.event == "started":
            record.active_step_id = step_id
            if row.plan_id:
                record.active_plan_id = row.plan_id
            if row.goal_id:
                record.active_goal_id = row.goal_id
        elif step_id and record.active_step_id == step_id and self._normalize_step_status(row.status) in {
            "success",
            "failed",
            "blocked",
            "skipped",
        }:
            record.active_step_id = ""

        record.resume_cursor = {
            "step_id": step_id,
            "status": self._normalize_step_status(row.status),
            "checkpoint_sequence": row.sequence,
            "goal_id": str(row.goal_id or "").strip(),
            "plan_id": str(row.plan_id or "").strip(),
            "event": str(row.event or "").strip().lower(),
        }

        now_iso = _iso(_utc_now())
        record.last_checkpoint_at = now_iso
        record.updated_at = now_iso
        if row.status in {"failed", "blocked"}:
            record.last_error = row.error or f"{row.action} failed"
        self._save_locked()

    @classmethod
    def _resolve_resume_cursor(
        cls,
        *,
        steps: List[Dict[str, Any]],
        step_status: Dict[str, str],
        completed_step_ids: set[str],
        active_step_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not steps:
            return None

        active = str(active_step_id or "").strip()
        if active:
            for index, raw_step in enumerate(steps):
                if not isinstance(raw_step, dict):
                    continue
                if str(raw_step.get("step_id", "")).strip() == active:
                    status = cls._normalize_step_status(step_status.get(active, "pending"))
                    if status != "success":
                        return {"index": index, "step_id": active, "status": status}

        for index, raw_step in enumerate(steps):
            if not isinstance(raw_step, dict):
                continue
            step_id = str(raw_step.get("step_id", "")).strip()
            if not step_id:
                continue
            status = cls._normalize_step_status(step_status.get(step_id, "pending"))
            if status in {"failed", "blocked", "running"}:
                return {"index": index, "step_id": step_id, "status": status}

        for index, raw_step in enumerate(steps):
            if not isinstance(raw_step, dict):
                continue
            step_id = str(raw_step.get("step_id", "")).strip()
            if not step_id:
                continue
            status = cls._normalize_step_status(step_status.get(step_id, "pending"))
            if status == "success":
                continue
            deps = raw_step.get("depends_on", [])
            deps_list = [str(dep).strip() for dep in deps if str(dep).strip()] if isinstance(deps, list) else []
            unresolved = [dep for dep in deps_list if dep not in completed_step_ids]
            if not unresolved:
                return {"index": index, "step_id": step_id, "status": status}

        for index, raw_step in enumerate(steps):
            if not isinstance(raw_step, dict):
                continue
            step_id = str(raw_step.get("step_id", "")).strip()
            if not step_id:
                continue
            status = cls._normalize_step_status(step_status.get(step_id, "pending"))
            if status != "success":
                return {"index": index, "step_id": step_id, "status": status}
        return None

    @classmethod
    def _merge_step_status(
        cls,
        step_status: Dict[str, str],
        checkpoints: List[MissionStepRecord],
    ) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        for step_id, status in step_status.items():
            clean_id = str(step_id or "").strip()
            if not clean_id:
                continue
            merged[clean_id] = cls._normalize_step_status(status)

        rows = sorted(
            checkpoints,
            key=lambda item: (max(0, int(getattr(item, "sequence", 0))), str(getattr(item, "completed_at", ""))),
        )
        for row in rows:
            step_id = str(getattr(row, "step_id", "") or "").strip()
            if not step_id:
                continue
            status = cls._normalize_step_status(getattr(row, "status", "pending"))
            merged[step_id] = status
        return merged

    @staticmethod
    def _step_status_rank(value: str) -> int:
        normalized = MissionControl._normalize_step_status(value)
        ranks = {
            "pending": 0,
            "running": 1,
            "skipped": 2,
            "failed": 3,
            "blocked": 4,
            "success": 5,
        }
        return ranks.get(normalized, 0)

    @staticmethod
    def _normalize_step_status(value: Any) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"pending", "running", "success", "failed", "blocked", "skipped"}:
            return clean
        return "pending"

    @staticmethod
    def _args_fingerprint(args: Dict[str, Any]) -> str:
        if not isinstance(args, dict) or not args:
            return ""
        try:
            encoded = json.dumps(args, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        except Exception:  # noqa: BLE001
            return ""
        return hashlib.sha1(encoded.encode("utf-8", errors="ignore")).hexdigest()  # noqa: S324

    def _load_locked(self) -> None:
        with self._lock:
            if not self.store_path.exists():
                return
            try:
                payload = json.loads(self.store_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                return
            if not isinstance(payload, list):
                return
            for raw in payload:
                row = self._coerce_record(raw)
                if row is None:
                    continue
                if not row.step_status and row.checkpoints:
                    row.step_status = self._merge_step_status({}, row.checkpoints)
                if row.last_step_sequence <= 0 and row.checkpoints:
                    row.last_step_sequence = max(
                        max(0, int(getattr(item, "sequence", 0) or 0))
                        for item in row.checkpoints
                    )
                self._records[row.mission_id] = row
                if row.root_goal_id:
                    self._goal_to_mission[row.root_goal_id] = row.mission_id
                if row.latest_goal_id:
                    self._goal_to_mission[row.latest_goal_id] = row.mission_id
            self._trim_locked()

    def _coerce_record(self, raw: Any) -> Optional[MissionRecord]:
        if not isinstance(raw, dict):
            return None
        mission_id = str(raw.get("mission_id", "")).strip()
        root_goal_id = str(raw.get("root_goal_id", "")).strip()
        latest_goal_id = str(raw.get("latest_goal_id", "")).strip() or root_goal_id
        text = str(raw.get("text", "")).strip()
        source = str(raw.get("source", "desktop-ui")).strip() or "desktop-ui"
        if not mission_id or not root_goal_id or not text:
            return None

        checkpoints_raw = raw.get("checkpoints", [])
        checkpoints: List[MissionStepRecord] = []
        if isinstance(checkpoints_raw, list):
            for item in checkpoints_raw:
                if not isinstance(item, dict):
                    continue
                checkpoints.append(
                    MissionStepRecord(
                        sequence=max(0, int(item.get("sequence", 0) or 0)),
                        event=str(item.get("event", "finished") or "finished").strip().lower() or "finished",
                        step_id=str(item.get("step_id", "")).strip(),
                        action=str(item.get("action", "")).strip(),
                        status=self._normalize_step_status(item.get("status", "")),
                        attempt=max(1, int(item.get("attempt", 1))),
                        completed_at=str(item.get("completed_at", "")),
                        goal_id=str(item.get("goal_id", "")),
                        plan_id=str(item.get("plan_id", "")),
                        args_fingerprint=str(item.get("args_fingerprint", "")),
                        error=str(item.get("error", "")),
                        duration_ms=max(0, int(item.get("duration_ms", 0))),
                        evidence=item.get("evidence", {}) if isinstance(item.get("evidence"), dict) else {},
                    )
                )

        metadata = raw.get("metadata", {})
        plan = raw.get("plan", {})
        step_status_raw = raw.get("step_status", {})
        step_status: Dict[str, str] = {}
        if isinstance(step_status_raw, dict):
            for step_id, status in step_status_raw.items():
                clean_id = str(step_id or "").strip()
                if not clean_id:
                    continue
                step_status[clean_id] = self._normalize_step_status(status)

        step_attempts_raw = raw.get("step_attempts", {})
        step_attempts: Dict[str, int] = {}
        if isinstance(step_attempts_raw, dict):
            for step_id, attempt in step_attempts_raw.items():
                clean_id = str(step_id or "").strip()
                if not clean_id:
                    continue
                try:
                    parsed_attempt = int(attempt or 1)
                except Exception:  # noqa: BLE001
                    parsed_attempt = 1
                step_attempts[clean_id] = max(1, parsed_attempt)

        return MissionRecord(
            mission_id=mission_id,
            root_goal_id=root_goal_id,
            latest_goal_id=latest_goal_id,
            text=text,
            source=source,
            metadata=metadata if isinstance(metadata, dict) else {},
            status=str(raw.get("status", "running")).strip().lower() or "running",
            created_at=str(raw.get("created_at", _iso(_utc_now()))),
            updated_at=str(raw.get("updated_at", _iso(_utc_now()))),
            last_checkpoint_at=str(raw.get("last_checkpoint_at", "")),
            last_error=str(raw.get("last_error", "")),
            resume_count=max(0, int(raw.get("resume_count", 0))),
            active_step_id=str(raw.get("active_step_id", "")),
            active_plan_id=str(raw.get("active_plan_id", "")),
            active_goal_id=str(raw.get("active_goal_id", "")),
            last_step_sequence=max(0, int(raw.get("last_step_sequence", 0) or 0)),
            step_status=step_status,
            step_attempts=step_attempts,
            resume_cursor=raw.get("resume_cursor", {}) if isinstance(raw.get("resume_cursor"), dict) else {},
            plan=plan if isinstance(plan, dict) else {},
            checkpoints=checkpoints,
        )

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.to_dict() for item in self._records.values()]
        rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        self.store_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

    def _trim_locked(self) -> None:
        if len(self._records) <= self.max_records:
            return
        rows = sorted(self._records.values(), key=lambda item: item.updated_at)
        overflow = len(rows) - self.max_records
        for item in rows[:overflow]:
            self._records.pop(item.mission_id, None)
            for goal_id, mission_id in list(self._goal_to_mission.items()):
                if mission_id == item.mission_id:
                    self._goal_to_mission.pop(goal_id, None)

    @staticmethod
    def _serialize_plan(plan: ExecutionPlan | Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(plan, dict):
            return json.loads(json.dumps(plan))
        if not isinstance(plan, ExecutionPlan):
            return {}
        return {
            "plan_id": str(plan.plan_id),
            "goal_id": str(plan.goal_id),
            "intent": str(plan.intent),
            "created_at": str(plan.created_at),
            "context": plan.context if isinstance(plan.context, dict) else {},
            "steps": [MissionControl._serialize_step(step) for step in plan.steps],
        }

    @staticmethod
    def _serialize_step(step: PlanStep) -> Dict[str, Any]:
        return {
            "step_id": str(step.step_id),
            "action": str(step.action),
            "args": step.args if isinstance(step.args, dict) else {},
            "depends_on": list(step.depends_on) if isinstance(step.depends_on, list) else [],
            "verify": step.verify if isinstance(step.verify, dict) else {},
            "can_retry": bool(step.can_retry),
            "max_retries": max(0, int(step.max_retries)),
            "timeout_s": max(1, int(step.timeout_s)),
        }

    @staticmethod
    def _sanitize_evidence(evidence: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key, value in evidence.items():
            clean_key = str(key).strip()
            if not clean_key:
                continue
            if clean_key in {"raw_dom", "raw_html", "raw_bytes"}:
                continue
            out[clean_key] = value
        return out

    @staticmethod
    def _completed_step_ids(checkpoints: List[MissionStepRecord]) -> set[str]:
        success: set[str] = set()
        for row in checkpoints:
            step_id = str(row.step_id or "").strip()
            if not step_id:
                continue
            if str(row.status or "").strip().lower() == "success":
                success.add(step_id)
        return success
