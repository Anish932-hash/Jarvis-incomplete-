from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional


RefreshRegistryFn = Callable[..., Dict[str, Any]]
StackStatusFn = Callable[..., Dict[str, Any]]
ApplyStackFn = Callable[..., Dict[str, Any]]
InventorySnapshotFn = Callable[..., Dict[str, Any]]


class CoworkerStackActivationOrchestrator:
    LOCAL_RUNTIME_TASKS = {"reasoning", "tts", "stt", "vision"}
    _ITEM_SUCCESS_STATUSES = {"success", "warning"}
    _ITEM_REUSE_STATUSES = {"skipped"}

    def activate(
        self,
        *,
        source: str,
        run_payload: Dict[str, Any],
        task: str = "",
        stack_name: str = "desktop_agent",
        mission_profile: str = "balanced",
        requires_offline: bool = False,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        cost_sensitive: bool = False,
        max_cost_units: Optional[float] = None,
        refresh_registry: Optional[RefreshRegistryFn] = None,
        stack_status: Optional[StackStatusFn] = None,
        apply_stack: Optional[ApplyStackFn] = None,
        inventory_snapshot: Optional[InventorySnapshotFn] = None,
    ) -> Dict[str, Any]:
        clean_source = str(source or "setup").strip().lower() or "setup"
        clean_task = str(task or "").strip().lower()
        clean_stack = str(stack_name or "desktop_agent").strip().lower() or "desktop_agent"
        clean_profile = str(mission_profile or "balanced").strip().lower() or "balanced"
        run_status = str(run_payload.get("status", "unknown") or "unknown").strip().lower()
        dry_run = bool(run_payload.get("dry_run", False))
        payload: Dict[str, Any] = {
            "status": "skipped",
            "source": clean_source,
            "task": clean_task,
            "stack_name": clean_stack,
            "mission_profile": clean_profile,
            "run_id": str(run_payload.get("run_id", "") or "").strip(),
            "run_status": run_status,
            "dry_run": dry_run,
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "latency_sensitive": bool(latency_sensitive),
            "cost_sensitive": bool(cost_sensitive),
            "max_cost_units": max_cost_units,
            "message": "",
            "refresh": {},
            "inventory": {},
            "before": {},
            "after": {},
            "apply": {},
            "affected_tasks": [],
            "runtime_tasks": [],
            "non_runtime_tasks": [],
            "activation_candidates": [],
            "warnings": [],
            "blockers": [],
            "summary": {
                "affected_task_count": 0,
                "runtime_task_count": 0,
                "non_runtime_task_count": 0,
                "activation_candidate_count": 0,
                "activated_task_count": 0,
                "already_active_count": 0,
                "blocked_runtime_count": 0,
                "ready_before_count": 0,
                "ready_after_count": 0,
                "action_required_before_count": 0,
                "action_required_after_count": 0,
            },
        }
        if not callable(stack_status):
            payload["status"] = "error"
            payload["message"] = "stack status callback unavailable"
            payload["blockers"] = ["Unable to inspect the coworker stack after setup completion."]
            return payload
        if not callable(apply_stack):
            payload["status"] = "error"
            payload["message"] = "stack apply callback unavailable"
            payload["blockers"] = ["Unable to auto-activate newly available runtimes."]
            return payload
        if dry_run:
            payload["message"] = "dry-run completed; activation was skipped"
            return payload

        affected_tasks = self._affected_tasks(run_payload=run_payload, task_hint=clean_task)
        payload["affected_tasks"] = list(affected_tasks)
        payload["summary"]["affected_task_count"] = len(affected_tasks)
        if not affected_tasks:
            payload["message"] = "no completed setup artifacts mapped to activation tasks"
            return payload

        runtime_tasks = [task_name for task_name in affected_tasks if task_name in self.LOCAL_RUNTIME_TASKS]
        non_runtime_tasks = [task_name for task_name in affected_tasks if task_name not in self.LOCAL_RUNTIME_TASKS]
        payload["runtime_tasks"] = list(runtime_tasks)
        payload["non_runtime_tasks"] = list(non_runtime_tasks)
        payload["summary"]["runtime_task_count"] = len(runtime_tasks)
        payload["summary"]["non_runtime_task_count"] = len(non_runtime_tasks)
        if non_runtime_tasks:
            payload["warnings"].append(
                "Some completed setup artifacts do not map to an auto-launchable local runtime: "
                + ", ".join(non_runtime_tasks)
            )

        refresh_payload: Dict[str, Any] = {}
        if callable(refresh_registry):
            try:
                refresh_payload = refresh_registry(force=True)
            except Exception as exc:  # noqa: BLE001
                refresh_payload = {"status": "error", "message": str(exc)}
        payload["refresh"] = deepcopy(refresh_payload) if isinstance(refresh_payload, dict) else {}
        if isinstance(refresh_payload, dict) and str(refresh_payload.get("status", "") or "").strip().lower() == "error":
            refresh_message = str(refresh_payload.get("message", "") or "").strip()
            if refresh_message:
                payload["warnings"].append(f"Registry refresh reported an issue: {refresh_message}")

        inventory_payload = self._build_inventory_payload(tasks=affected_tasks, inventory_snapshot=inventory_snapshot)
        payload["inventory"] = inventory_payload

        before_payload = stack_status(
            stack_name=clean_stack,
            requires_offline=bool(requires_offline),
            privacy_mode=bool(privacy_mode),
            latency_sensitive=bool(latency_sensitive),
            mission_profile=clean_profile,
            cost_sensitive=bool(cost_sensitive),
            max_cost_units=max_cost_units,
            refresh_provider_credentials=False,
        )
        if not isinstance(before_payload, dict):
            payload["status"] = "error"
            payload["message"] = "invalid coworker stack status payload"
            payload["blockers"] = ["Coworker stack status returned an invalid response."]
            return payload
        payload["before"] = self._filter_status_payload(before_payload, affected_tasks)
        before_task_rows = self._task_row_map(payload["before"])

        candidates: List[Dict[str, Any]] = []
        selected_apply_tasks: List[str] = []
        ready_before_count = 0
        action_required_before_count = 0
        already_active_count = 0
        blocked_runtime_count = 0
        for task_name in runtime_tasks:
            row = before_task_rows.get(task_name, {})
            before_status = str(row.get("status", "unknown") or "unknown").strip().lower()
            auto_applyable = bool(row.get("auto_applyable", False))
            already_active = bool(row.get("already_active", False))
            candidate = {
                "task": task_name,
                "provider": str(row.get("provider", "") or "").strip().lower(),
                "model": str(row.get("model", "") or "").strip(),
                "selected_path": str(row.get("selected_path", "") or "").strip(),
                "before_status": before_status,
                "after_status": "",
                "auto_applyable": auto_applyable,
                "already_active": already_active,
                "profile_id": str(row.get("profile_id", "") or "").strip().lower(),
                "template_id": str(row.get("template_id", "") or "").strip().lower(),
                "activated": False,
                "activation_status": "",
                "activation_ok": False,
                "blockers": self._string_list(row.get("blockers", [])),
                "warnings": self._string_list(row.get("warnings", [])),
            }
            if before_status == "ready":
                ready_before_count += 1
            elif before_status == "action_required":
                action_required_before_count += 1
            else:
                blocked_runtime_count += 1
            if already_active:
                already_active_count += 1
            if auto_applyable and not already_active:
                selected_apply_tasks.append(task_name)
            candidates.append(candidate)
        payload["summary"]["ready_before_count"] = ready_before_count
        payload["summary"]["action_required_before_count"] = action_required_before_count
        payload["summary"]["already_active_count"] = already_active_count
        payload["summary"]["blocked_runtime_count"] = blocked_runtime_count
        payload["summary"]["activation_candidate_count"] = len(selected_apply_tasks)

        apply_response: Dict[str, Any]
        if selected_apply_tasks:
            apply_response = apply_stack(
                stack_name=clean_stack,
                tasks=selected_apply_tasks,
                requires_offline=bool(requires_offline),
                privacy_mode=bool(privacy_mode),
                latency_sensitive=bool(latency_sensitive),
                mission_profile=clean_profile,
                cost_sensitive=bool(cost_sensitive),
                max_cost_units=max_cost_units,
                force_reapply=False,
                continue_on_error=True,
            )
            payload["apply"] = deepcopy(apply_response) if isinstance(apply_response, dict) else {}
        else:
            apply_response = {
                "status": "skipped",
                "message": "no auto-activatable coworker stack tasks were detected",
                "requested_tasks": [],
            }
            payload["apply"] = deepcopy(apply_response)

        after_payload = {}
        if isinstance(apply_response, dict) and isinstance(apply_response.get("after"), dict):
            after_payload = dict(apply_response.get("after", {}))
        if not after_payload:
            after_payload = stack_status(
                stack_name=clean_stack,
                requires_offline=bool(requires_offline),
                privacy_mode=bool(privacy_mode),
                latency_sensitive=bool(latency_sensitive),
                mission_profile=clean_profile,
                cost_sensitive=bool(cost_sensitive),
                max_cost_units=max_cost_units,
                refresh_provider_credentials=False,
            )
        payload["after"] = self._filter_status_payload(after_payload if isinstance(after_payload, dict) else {}, affected_tasks)
        after_task_rows = self._task_row_map(payload["after"])

        apply_items = self._apply_item_map(payload["apply"])
        ready_after_count = 0
        action_required_after_count = 0
        activated_task_count = 0
        for candidate in candidates:
            task_name = str(candidate.get("task", "") or "").strip().lower()
            after_row = after_task_rows.get(task_name, {})
            after_status = str(after_row.get("status", "unknown") or "unknown").strip().lower()
            candidate["after_status"] = after_status
            if after_status == "ready":
                ready_after_count += 1
            elif after_status == "action_required":
                action_required_after_count += 1
            apply_item = apply_items.get(task_name, {})
            activation_status = str(
                apply_item.get("status", (apply_item.get("result", {}) or {}).get("status", ""))
                or ""
            ).strip().lower()
            activation_ok = bool(
                apply_item.get("ok", False)
                or activation_status in {"success", "accepted", "partial", "degraded"}
            )
            if activation_status:
                candidate["activation_status"] = activation_status
            candidate["activation_ok"] = activation_ok
            candidate["activated"] = bool(candidate.get("auto_applyable", False) and activation_ok)
            if bool(candidate.get("activated", False)):
                activated_task_count += 1
        payload["activation_candidates"] = candidates
        payload["summary"]["activated_task_count"] = activated_task_count
        payload["summary"]["ready_after_count"] = ready_after_count
        payload["summary"]["action_required_after_count"] = action_required_after_count

        warnings = self._string_list(payload.get("warnings", []))
        blockers = self._string_list(payload.get("blockers", []))
        for candidate in candidates:
            warnings.extend(self._string_list(candidate.get("warnings", [])))
            if str(candidate.get("after_status", "") or "").strip().lower() == "blocked":
                blockers.extend(self._string_list(candidate.get("blockers", [])))
        payload["warnings"] = self._dedupe_strings(warnings)
        payload["blockers"] = self._dedupe_strings(blockers)

        if not runtime_tasks:
            payload["status"] = "skipped"
            payload["message"] = "completed setup artifacts were refreshed, but no local runtime tasks required activation"
            return payload
        if selected_apply_tasks:
            apply_section = payload["apply"].get("apply", payload["apply"]) if isinstance(payload.get("apply"), dict) else {}
            apply_status = str((apply_section or {}).get("status", payload["apply"].get("status", "")) or "").strip().lower()
            if activated_task_count >= len(selected_apply_tasks) and action_required_after_count <= 0:
                payload["status"] = "success"
                payload["message"] = f"Activated {activated_task_count} coworker runtime task(s)."
            elif activated_task_count > 0:
                payload["status"] = "partial"
                payload["message"] = (
                    f"Activated {activated_task_count} coworker runtime task(s), "
                    f"with {max(0, len(selected_apply_tasks) - activated_task_count)} still needing attention."
                )
            elif apply_status in {"error"}:
                payload["status"] = "error"
                payload["message"] = "Activation was attempted, but the coworker stack failed to apply the new runtimes."
            else:
                payload["status"] = "partial"
                payload["message"] = "Activation was attempted, but no new runtime task became ready."
            return payload

        if already_active_count >= len(runtime_tasks) and len(runtime_tasks) > 0:
            payload["status"] = "success"
            payload["message"] = "The affected coworker runtime tasks were already active."
            return payload

        if blocked_runtime_count > 0 or action_required_before_count > 0:
            payload["status"] = "partial"
            payload["message"] = "Artifacts were refreshed, but at least one runtime still needs review or a matching launch template."
            return payload

        payload["status"] = "skipped"
        payload["message"] = "No coworker runtime activation was required after refresh."
        return payload

    def _affected_tasks(self, *, run_payload: Dict[str, Any], task_hint: str) -> List[str]:
        tasks: List[str] = []
        items = run_payload.get("items", []) if isinstance(run_payload.get("items"), list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            task_name = str(item.get("task", "") or "").strip().lower()
            if not task_name:
                continue
            if self._item_materialized_artifact(item):
                tasks.append(task_name)
        if not tasks and task_hint:
            tasks.append(task_hint)
        return self._dedupe_strings(tasks)

    def _item_materialized_artifact(self, item: Dict[str, Any]) -> bool:
        status_name = str(item.get("status", "") or "").strip().lower()
        if status_name in self._ITEM_SUCCESS_STATUSES:
            return True
        artifact = item.get("artifact", {}) if isinstance(item.get("artifact"), dict) else {}
        if bool(artifact.get("exists", False)):
            return True
        if status_name not in self._ITEM_REUSE_STATUSES:
            return False
        path_text = str(item.get("path", "") or "").strip()
        if not path_text:
            return False
        try:
            return Path(path_text).expanduser().exists()
        except Exception:
            return False

    def _build_inventory_payload(
        self,
        *,
        tasks: Iterable[str],
        inventory_snapshot: Optional[InventorySnapshotFn],
    ) -> Dict[str, Any]:
        if not callable(inventory_snapshot):
            return {}
        payload: Dict[str, Any] = {"status": "success", "tasks": {}}
        for task_name in tasks:
            clean_task = str(task_name or "").strip().lower()
            if not clean_task:
                continue
            try:
                snapshot = inventory_snapshot(task=clean_task, limit=24)
            except TypeError:
                snapshot = inventory_snapshot(clean_task, 24)
            except Exception as exc:  # noqa: BLE001
                snapshot = {"status": "error", "message": str(exc)}
            task_payload = snapshot if isinstance(snapshot, dict) else {"status": "error", "message": "invalid inventory snapshot"}
            items = task_payload.get("items", []) if isinstance(task_payload.get("items"), list) else []
            payload["tasks"][clean_task] = {
                "status": str(task_payload.get("status", "unknown") or "unknown"),
                "present_count": int(task_payload.get("present_count", 0) or 0),
                "missing_count": int(task_payload.get("missing_count", 0) or 0),
                "declared_count": int(task_payload.get("declared_count", 0) or 0),
                "detected_count": int(task_payload.get("detected_count", 0) or 0),
                "items": [
                    {
                        "name": str(item.get("name", "") or "model"),
                        "path": str(item.get("path", "") or ""),
                        "present": bool(item.get("present", False)),
                        "missing": bool(item.get("missing", False)),
                        "declared": bool(item.get("declared", False)),
                    }
                    for item in items
                    if isinstance(item, dict)
                ][:8],
            }
        return payload

    def _filter_status_payload(self, payload: Dict[str, Any], tasks: Iterable[str]) -> Dict[str, Any]:
        clean_tasks = {
            str(task_name or "").strip().lower()
            for task_name in tasks
            if str(task_name or "").strip()
        }
        rows = payload.get("tasks", []) if isinstance(payload.get("tasks"), list) else []
        filtered_rows = [
            deepcopy(row)
            for row in rows
            if isinstance(row, dict) and str(row.get("task", "") or "").strip().lower() in clean_tasks
        ]
        return {
            "status": str(payload.get("status", "unknown") or "unknown"),
            "stack_name": str(payload.get("stack_name", "") or ""),
            "mission_profile": str(payload.get("mission_profile", "") or ""),
            "summary": deepcopy(payload.get("summary", {})) if isinstance(payload.get("summary"), dict) else {},
            "tasks": filtered_rows,
            "warnings": self._string_list(payload.get("warnings", [])),
            "blockers": self._string_list(payload.get("blockers", [])),
        }

    def _task_row_map(self, payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        rows = payload.get("tasks", []) if isinstance(payload.get("tasks"), list) else []
        mapping: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            task_name = str(row.get("task", "") or "").strip().lower()
            if task_name:
                mapping[task_name] = dict(row)
        return mapping

    def _apply_item_map(self, payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        if not isinstance(payload, dict):
            return {}
        apply_payload = payload.get("apply", payload) if isinstance(payload.get("apply", payload), dict) else {}
        rows = apply_payload.get("items", []) if isinstance(apply_payload.get("items"), list) else []
        mapping: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            task_name = str(row.get("task", "") or "").strip().lower()
            if task_name:
                mapping[task_name] = dict(row)
        return mapping

    @staticmethod
    def _string_list(values: Any) -> List[str]:
        if not isinstance(values, list):
            return []
        return [str(value).strip() for value in values if str(value).strip()]

    def _dedupe_strings(self, values: Iterable[str]) -> List[str]:
        rows: List[str] = []
        seen: set[str] = set()
        for value in values:
            clean_value = str(value or "").strip()
            if not clean_value or clean_value in seen:
                continue
            seen.add(clean_value)
            rows.append(clean_value)
        return rows
