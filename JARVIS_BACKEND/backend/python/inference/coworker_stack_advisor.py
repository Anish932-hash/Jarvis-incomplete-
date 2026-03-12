from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Dict, Iterable, List, Optional


LaunchTemplateExecutor = Callable[[str, str, str], Dict[str, Any]]


class CoworkerStackAdvisor:
    LOCAL_RUNTIME_TASKS = {"reasoning", "tts", "stt", "vision"}
    CORE_TASKS = {"reasoning", "vision", "embedding", "intent"}
    VOICE_TASKS = {"wakeword", "stt", "tts"}
    TASK_WEIGHTS: Dict[str, float] = {
        "reasoning": 3.0,
        "vision": 2.4,
        "stt": 1.8,
        "tts": 1.4,
        "wakeword": 1.2,
        "embedding": 1.2,
        "intent": 1.1,
    }
    CLOUD_PROVIDER_HINTS = {
        "groq": "Save and verify the Groq API key to unlock cloud reasoning fallback.",
        "nvidia": "Save and verify the NVIDIA API key to unlock cloud reasoning/vision fallback.",
        "elevenlabs": "Save and verify the ElevenLabs key and voice id to unlock cloud TTS.",
        "huggingface": "Configure a Hugging Face access token for gated model downloads.",
    }

    def build_status(
        self,
        *,
        stack_name: str,
        mission_profile: str,
        route_bundle: Dict[str, Any],
        runtime_supervisors: Dict[str, Any],
        active_runtimes: Dict[str, Any],
        provider_credentials: Dict[str, Any],
        setup_plan: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        route_items = route_bundle.get("items", []) if isinstance(route_bundle.get("items"), list) else []
        provider_rows = self._provider_rows(provider_credentials)
        missing_setup_by_task = self._missing_setup_by_task(setup_plan or {})

        task_rows: List[Dict[str, Any]] = []
        blockers: List[str] = []
        warnings: List[str] = []
        recommendations: List[str] = []
        ready_count = 0
        action_required_count = 0
        blocked_count = 0
        local_count = 0
        cloud_count = 0
        total_weight = 0.0
        earned_weight = 0.0

        for row in route_items:
            if not isinstance(row, dict):
                continue
            task_row = self._build_task_status(
                item=row,
                runtime_supervisors=runtime_supervisors,
                active_runtimes=active_runtimes,
                provider_rows=provider_rows,
                missing_setup_by_task=missing_setup_by_task,
            )
            task_rows.append(task_row)
            blockers.extend(task_row.get("blockers", []))
            warnings.extend(task_row.get("warnings", []))
            recommendations.extend(task_row.get("recommendations", []))

            task_name = str(task_row.get("task", "") or "").strip().lower()
            provider_name = str(task_row.get("provider", "") or "").strip().lower()
            readiness_status = str(task_row.get("status", "unknown") or "unknown").strip().lower()
            readiness_score = float(task_row.get("readiness_score", 0.0) or 0.0)
            weight = float(self.TASK_WEIGHTS.get(task_name, 1.0))
            total_weight += weight
            earned_weight += max(0.0, min(readiness_score, 1.0)) * weight

            if readiness_status == "ready":
                ready_count += 1
            elif readiness_status == "action_required":
                action_required_count += 1
            else:
                blocked_count += 1

            if provider_name == "local":
                local_count += 1
            elif provider_name:
                cloud_count += 1

        score = round((earned_weight / total_weight) * 100.0, 1) if total_weight > 0 else 0.0
        status_by_task = {
            str(item.get("task", "") or "").strip().lower(): str(item.get("status", "unknown") or "unknown").strip().lower()
            for item in task_rows
        }
        core_ready = all(status_by_task.get(task, "") == "ready" for task in self.CORE_TASKS if task in status_by_task)
        voice_ready = all(status_by_task.get(task, "") == "ready" for task in self.VOICE_TASKS if task in status_by_task)
        local_runtime_ready_count = sum(
            1
            for item in task_rows
            if str(item.get("provider", "") or "").strip().lower() == "local"
            and str(item.get("task", "") or "").strip().lower() in self.LOCAL_RUNTIME_TASKS
            and str(item.get("status", "") or "").strip().lower() == "ready"
        )
        local_runtime_actionable_count = sum(
            1
            for item in task_rows
            if str(item.get("provider", "") or "").strip().lower() == "local"
            and str(item.get("task", "") or "").strip().lower() in self.LOCAL_RUNTIME_TASKS
            and bool(item.get("auto_applyable", False))
            and not bool(item.get("already_active", False))
        )
        provider_blocker_count = sum(
            1
            for item in task_rows
            if bool(item.get("requires_credentials", False)) and str(item.get("status", "") or "").strip().lower() != "ready"
        )
        desktop_coworker_ready = core_ready and score >= 72.0 and blocked_count <= 1
        status = "success"
        if blocked_count > 0 and ready_count <= 0:
            status = "error"
        elif blocked_count > 0 or action_required_count > 0:
            status = "partial"

        return {
            "status": status,
            "stack_name": str(stack_name or "desktop_agent").strip().lower() or "desktop_agent",
            "mission_profile": str(mission_profile or "balanced").strip().lower() or "balanced",
            "summary": {
                "score": score,
                "task_count": len(task_rows),
                "ready_task_count": ready_count,
                "action_required_count": action_required_count,
                "blocked_task_count": blocked_count,
                "local_task_count": local_count,
                "cloud_task_count": cloud_count,
                "local_runtime_ready_count": local_runtime_ready_count,
                "local_runtime_actionable_count": local_runtime_actionable_count,
                "provider_blocker_count": provider_blocker_count,
                "core_ready": core_ready,
                "voice_ready": voice_ready,
                "desktop_coworker_ready": desktop_coworker_ready,
            },
            "tasks": task_rows,
            "blockers": self._dedupe_strings(blockers),
            "warnings": self._dedupe_strings(warnings),
            "recommendations": self._dedupe_strings(recommendations),
            "route_bundle": deepcopy(route_bundle) if isinstance(route_bundle, dict) else {},
            "runtime_supervisors": deepcopy(runtime_supervisors) if isinstance(runtime_supervisors, dict) else {},
            "active_runtimes": deepcopy(active_runtimes) if isinstance(active_runtimes, dict) else {},
            "provider_credentials": deepcopy(provider_credentials) if isinstance(provider_credentials, dict) else {},
            "setup_plan": deepcopy(setup_plan) if isinstance(setup_plan, dict) else {},
        }

    def apply_recommended(
        self,
        *,
        status_payload: Dict[str, Any],
        execute_launch_template: LaunchTemplateExecutor,
        selected_tasks: Optional[Iterable[str]] = None,
        force_reapply: bool = False,
        continue_on_error: bool = True,
    ) -> Dict[str, Any]:
        tasks_filter = {
            str(item or "").strip().lower()
            for item in (selected_tasks or [])
            if str(item or "").strip()
        }
        task_rows = status_payload.get("tasks", []) if isinstance(status_payload.get("tasks"), list) else []
        results: List[Dict[str, Any]] = []
        executed_count = 0
        skipped_count = 0
        error_count = 0

        for row in task_rows:
            if not isinstance(row, dict):
                continue
            task_name = str(row.get("task", "") or "").strip().lower()
            if tasks_filter and task_name not in tasks_filter:
                continue
            if str(row.get("action_kind", "") or "").strip().lower() != "execute_launch_template":
                skipped_count += 1
                continue
            if not bool(row.get("auto_applyable", False)):
                skipped_count += 1
                continue
            if bool(row.get("already_active", False)) and not bool(force_reapply):
                skipped_count += 1
                continue
            profile_id = str(row.get("profile_id", "") or "").strip().lower()
            template_id = str(row.get("template_id", "") or "").strip().lower()
            if not profile_id or not template_id:
                skipped_count += 1
                continue

            payload = execute_launch_template(task_name, profile_id, template_id)
            payload = deepcopy(payload) if isinstance(payload, dict) else {"status": "error", "message": "invalid launch response"}
            status_name = str(payload.get("status", "error") or "error").strip().lower()
            ok = status_name in {"success", "degraded", "partial", "accepted"}
            if ok:
                executed_count += 1
            else:
                error_count += 1
            results.append(
                {
                    "task": task_name,
                    "action_kind": "execute_launch_template",
                    "profile_id": profile_id,
                    "template_id": template_id,
                    "status": status_name,
                    "ok": ok,
                    "result": payload,
                }
            )
            if not ok and not continue_on_error:
                break

        status = "success"
        if executed_count <= 0 and error_count > 0:
            status = "error"
        elif error_count > 0:
            status = "partial"
        elif executed_count <= 0:
            status = "skipped"
        return {
            "status": status,
            "executed_count": executed_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "items": results,
            "requested_tasks": sorted(tasks_filter),
            "force_reapply": bool(force_reapply),
        }

    def _build_task_status(
        self,
        *,
        item: Dict[str, Any],
        runtime_supervisors: Dict[str, Any],
        active_runtimes: Dict[str, Any],
        provider_rows: Dict[str, Dict[str, Any]],
        missing_setup_by_task: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        task_name = str(item.get("task", "") or "").strip().lower()
        provider_name = str(item.get("provider", "") or "").strip().lower()
        model_name = str(item.get("model", "") or "").strip()
        base: Dict[str, Any] = {
            "task": task_name,
            "provider": provider_name,
            "model": model_name,
            "selected_path": str(item.get("selected_path", "") or "").strip(),
            "route_adjusted": bool(item.get("route_adjusted", False)),
            "route_blocked": bool(item.get("route_blocked", False)),
            "route_adjustment_reason": str(item.get("route_adjustment_reason", "") or "").strip().lower(),
            "readiness_score": 0.0,
            "status": "blocked",
            "blockers": [],
            "warnings": [],
            "recommendations": [],
            "auto_applyable": False,
            "already_active": False,
            "requires_credentials": False,
            "action_kind": "",
        }
        if str(item.get("status", "error") or "error").strip().lower() != "success":
            base["blockers"] = self._task_setup_blockers(task_name=task_name, missing_setup_by_task=missing_setup_by_task)
            message = str(item.get("message", "") or "").strip()
            if message:
                base["blockers"].insert(0, message)
            base["blockers"] = self._dedupe_strings(base["blockers"])
            return base

        route_policy = item.get("route_policy", {}) if isinstance(item.get("route_policy"), dict) else {}
        route_reason = str(route_policy.get("reason", "") or item.get("route_warning", "") or "").strip()
        if route_reason:
            base["warnings"].append(route_reason)

        if provider_name == "local" and task_name in self.LOCAL_RUNTIME_TASKS:
            return self._local_runtime_task_status(
                base=base,
                item=item,
                route_policy=route_policy,
                runtime_supervisors=runtime_supervisors,
                active_runtimes=active_runtimes,
                missing_setup_by_task=missing_setup_by_task,
            )
        if provider_name == "local":
            base["status"] = "ready" if base["selected_path"] else "blocked"
            base["readiness_score"] = 1.0 if base["selected_path"] else 0.0
            if not base["selected_path"]:
                base["blockers"] = self._task_setup_blockers(task_name=task_name, missing_setup_by_task=missing_setup_by_task)
            return base

        provider_row = provider_rows.get(provider_name, {})
        provider_ready = self._provider_ready(provider_row)
        base["requires_credentials"] = True
        base["provider_ready"] = provider_ready
        if provider_ready:
            base["status"] = "ready"
            base["readiness_score"] = 1.0
        else:
            base["status"] = "blocked"
            base["readiness_score"] = 0.0
            hint = self.CLOUD_PROVIDER_HINTS.get(provider_name, f"Configure credentials for provider '{provider_name}'.")
            base["blockers"].append(hint)
            status_text = str(provider_row.get("status", "") or "").strip()
            if status_text:
                base["warnings"].append(f"{provider_name} status: {status_text}")
            message = str(provider_row.get("message", "") or "").strip()
            if message:
                base["warnings"].append(message)
        if bool(base.get("route_adjusted", False)):
            requested_provider = str(item.get("requested_provider", "") or "").strip().lower()
            if requested_provider == "local":
                base["warnings"].append("Local runtime was rerouted to a cloud provider because the local launch policy was not safe enough.")
        base["warnings"] = self._dedupe_strings(base["warnings"])
        base["blockers"] = self._dedupe_strings(base["blockers"])
        return base

    def _local_runtime_task_status(
        self,
        *,
        base: Dict[str, Any],
        item: Dict[str, Any],
        route_policy: Dict[str, Any],
        runtime_supervisors: Dict[str, Any],
        active_runtimes: Dict[str, Any],
        missing_setup_by_task: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        task_name = str(base.get("task", "") or "").strip().lower()
        active = self._active_runtime_status(task_name=task_name, active_runtimes=active_runtimes, runtime_supervisors=runtime_supervisors)
        profile_id = str(route_policy.get("profile_id", "") or item.get("local_launch_profile_id", "") or "").strip().lower()
        template_id = str(route_policy.get("recommended_template_id", "") or item.get("local_launch_template_id", "") or "").strip().lower()
        matched = bool(route_policy.get("matched", False) or (profile_id and template_id))
        local_route_viable = bool(route_policy.get("local_route_viable", matched))
        blacklisted = bool(route_policy.get("blacklisted", False))
        review_required = bool(route_policy.get("review_required", False))
        active_profile_id = str(active.get("active_profile_id", "") or "").strip().lower()
        active_template_id = str(active.get("active_template_id", "") or "").strip().lower()
        ready = bool(active.get("ready", False))
        profile_match = bool(profile_id and active_profile_id and profile_id == active_profile_id)
        template_match = bool(template_id and active_template_id and template_id == active_template_id)
        if not template_id:
            template_match = True
        already_active = bool(ready and ((profile_id and profile_match) or not profile_id) and template_match)
        auto_applyable = bool(profile_id and template_id and matched and local_route_viable and not blacklisted and not review_required)

        base["profile_id"] = profile_id
        base["template_id"] = template_id
        base["current_profile_id"] = active_profile_id
        base["current_template_id"] = active_template_id
        base["runtime_ready"] = ready
        base["already_active"] = already_active
        base["auto_applyable"] = auto_applyable
        base["action_kind"] = "execute_launch_template" if auto_applyable else ""
        base["current_runtime"] = active

        if already_active:
            base["status"] = "ready"
            base["readiness_score"] = 1.0
            return base

        if auto_applyable:
            base["status"] = "action_required"
            base["readiness_score"] = 0.72
            title = profile_id or task_name
            if template_id:
                base["recommendations"].append(f"Apply the recommended {task_name} launch template '{template_id}' for profile '{title}'.")
            return base

        setup_blockers = self._task_setup_blockers(task_name=task_name, missing_setup_by_task=missing_setup_by_task)
        if setup_blockers:
            base["blockers"].extend(setup_blockers)
        if not matched:
            base["blockers"].append(f"No local {task_name} launch profile matched the selected route artifact.")
        if blacklisted:
            base["blockers"].append(f"The recommended local {task_name} launch template is blacklisted.")
        if review_required:
            base["blockers"].append(f"The recommended local {task_name} launch template requires review before autonomous use.")
        if not local_route_viable and not base["blockers"]:
            base["blockers"].append(f"The local {task_name} runtime is not currently viable for the selected route.")
        base["readiness_score"] = 0.18 if matched and not blacklisted else 0.0
        base["status"] = "blocked"
        base["blockers"] = self._dedupe_strings(base["blockers"])
        base["warnings"] = self._dedupe_strings(base["warnings"])
        return base

    def _active_runtime_status(
        self,
        *,
        task_name: str,
        active_runtimes: Dict[str, Any],
        runtime_supervisors: Dict[str, Any],
    ) -> Dict[str, Any]:
        payload = active_runtimes.get(task_name, {}) if isinstance(active_runtimes, dict) else {}
        row = dict(payload) if isinstance(payload, dict) else {}
        runtime_ready = False
        if task_name == "reasoning":
            reasoning = runtime_supervisors.get("reasoning", {}) if isinstance(runtime_supervisors.get("reasoning"), dict) else {}
            runtime_ready = bool(reasoning.get("runtime_ready", False) or row.get("ready", False))
            row.setdefault("active_profile_id", str(row.get("active_profile_id", "") or ""))
            row.setdefault("active_template_id", str(row.get("active_template_id", "") or ""))
        elif task_name == "vision":
            runtime_ready = bool(int(row.get("loaded_count", row.get("available", False) or 0) or 0) > 0 or row.get("available", False))
        elif task_name == "stt":
            runtime_ready = bool(row.get("available", False))
        elif task_name == "tts":
            runtime_ready = bool(row.get("ready", False))
        row["ready"] = runtime_ready
        row.setdefault("active_profile_id", str(row.get("active_profile_id", row.get("profile_id", "")) or ""))
        row.setdefault("active_template_id", str(row.get("active_template_id", row.get("template_id", "")) or ""))
        return row

    @staticmethod
    def _provider_rows(provider_credentials: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        providers = provider_credentials.get("providers", {}) if isinstance(provider_credentials.get("providers"), dict) else {}
        return {
            str(name or "").strip().lower(): dict(row)
            for name, row in providers.items()
            if str(name or "").strip() and isinstance(row, dict)
        }

    @staticmethod
    def _provider_ready(provider_row: Dict[str, Any]) -> bool:
        return bool(
            provider_row.get("ready", False)
            or provider_row.get("available", False)
            or provider_row.get("present", False)
            or str(provider_row.get("status", "") or "").strip().lower() in {"ready", "configured", "success"}
        )

    @staticmethod
    def _missing_setup_by_task(setup_plan: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        items = setup_plan.get("items", []) if isinstance(setup_plan.get("items"), list) else []
        rows: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            if bool(item.get("present", False)):
                continue
            task_name = str(item.get("task", "") or "").strip().lower()
            if not task_name:
                continue
            rows.setdefault(task_name, []).append(dict(item))
        return rows

    def _task_setup_blockers(self, *, task_name: str, missing_setup_by_task: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        rows = missing_setup_by_task.get(task_name, [])
        if not rows:
            return []
        automation_ready = any(bool(row.get("automation_ready", False)) for row in rows)
        if automation_ready:
            return [f"Install the missing local {task_name} artifact from the setup planner before using this route."]
        return [f"The local {task_name} route still needs a manual model pipeline or conversion step before it can be activated."]

    @staticmethod
    def _dedupe_strings(values: Iterable[Any]) -> List[str]:
        rows: List[str] = []
        seen: set[str] = set()
        for value in values:
            clean = str(value or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            rows.append(clean)
        return rows
