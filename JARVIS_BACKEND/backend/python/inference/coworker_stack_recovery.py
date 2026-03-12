from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

from backend.python.inference.coworker_stack_advisor import CoworkerStackAdvisor


LaunchTemplateExecutor = Callable[[str, str, str], Dict[str, Any]]
SetupInstallLauncher = Callable[[str, Optional[List[str]]], Dict[str, Any]]
ProviderVerifier = Callable[[str, str, Optional[List[str]]], Dict[str, Any]]
ManualPipelineLauncher = Callable[[str, Optional[List[str]]], Dict[str, Any]]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CoworkerStackRecoveryPlanner:
    _ACTIVE_INSTALL_STATUSES = {"accepted", "queued", "running", "cancelling"}
    _AUTO_RUNNABLE_KINDS = {
        "apply_runtime_template",
        "launch_setup_install",
        "launch_manual_pipeline",
        "verify_provider_credentials",
    }
    _STAGE_ORDER = {
        "runtime": 0,
        "provider": 1,
        "setup": 2,
        "manual": 3,
    }

    def build_plan(
        self,
        *,
        status_payload: Dict[str, Any],
        install_runs_payload: Optional[Dict[str, Any]] = None,
        manual_pipeline_payload: Optional[Dict[str, Any]] = None,
        manual_runs_payload: Optional[Dict[str, Any]] = None,
        verification_stale_after_s: float = 21_600.0,
    ) -> Dict[str, Any]:
        status_row = status_payload if isinstance(status_payload, dict) else {}
        stack_name = str(status_row.get("stack_name", "desktop_agent") or "desktop_agent").strip().lower() or "desktop_agent"
        mission_profile = str(status_row.get("mission_profile", "balanced") or "balanced").strip().lower() or "balanced"
        stack_summary = status_row.get("summary", {}) if isinstance(status_row.get("summary"), dict) else {}
        task_rows = self._task_rows(status_row.get("tasks"))
        task_rows_by_name = self._task_rows_by_name(task_rows)
        setup_plan = status_row.get("setup_plan", {}) if isinstance(status_row.get("setup_plan"), dict) else {}
        provider_snapshot = (
            status_row.get("provider_credentials", {})
            if isinstance(status_row.get("provider_credentials"), dict)
            else {}
        )
        provider_rows = self._provider_rows(provider_snapshot)
        setup_provider_rows = self._setup_provider_rows(setup_plan)
        automation_setup_by_task, manual_setup_by_task, huggingface_item_keys = self._setup_items_by_task(setup_plan)
        active_install_runs = self._active_install_runs(install_runs_payload)
        manual_pipeline = manual_pipeline_payload if isinstance(manual_pipeline_payload, dict) else {}
        active_manual_runs = self._active_manual_runs(manual_runs_payload)
        manual_pipeline_by_task = self._manual_pipeline_by_task(manual_pipeline)
        upgrade_actions = self._upgrade_actions(manual_pipeline)

        actions: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        for task_row in task_rows:
            action = self._build_runtime_action(task_row)
            if action is None:
                continue
            action_id = str(action.get("id", "") or "").strip().lower()
            if not action_id or action_id in seen_ids:
                continue
            seen_ids.add(action_id)
            actions.append(action)

        for task_name, rows in task_rows_by_name.items():
            needs_local_recovery = any(
                str(row.get("status", "") or "").strip().lower() != "ready"
                or bool(row.get("route_adjusted", False))
                for row in rows
            )
            if not needs_local_recovery:
                continue

            auto_items = automation_setup_by_task.get(task_name, [])
            if auto_items:
                install_action = self._build_install_action(
                    task_name=task_name,
                    task_rows=rows,
                    setup_items=auto_items,
                    active_install_runs=active_install_runs,
                )
                action_id = str(install_action.get("id", "") or "").strip().lower()
                if action_id and action_id not in seen_ids:
                    seen_ids.add(action_id)
                    actions.append(install_action)

            manual_items = manual_setup_by_task.get(task_name, [])
            if manual_items:
                manual_action = self._build_manual_action(
                    task_name=task_name,
                    task_rows=rows,
                    setup_items=manual_items,
                    manual_pipeline_items=manual_pipeline_by_task.get(task_name, []),
                    active_manual_runs=active_manual_runs,
                    upgrade_actions=upgrade_actions,
                )
                action_id = str(manual_action.get("id", "") or "").strip().lower()
                if action_id and action_id not in seen_ids:
                    seen_ids.add(action_id)
                    actions.append(manual_action)

        provider_usage = self._provider_usage(
            task_rows=task_rows,
            provider_rows=provider_rows,
            setup_plan=setup_plan,
            huggingface_item_keys=huggingface_item_keys,
        )
        for provider_name, usage in provider_usage.items():
            action = self._build_provider_action(
                provider_name=provider_name,
                usage=usage,
                provider_row=provider_rows.get(provider_name, {}),
                setup_provider_row=setup_provider_rows.get(provider_name, {}),
                verification_stale_after_s=max(300.0, float(verification_stale_after_s)),
            )
            if action is None:
                continue
            action_id = str(action.get("id", "") or "").strip().lower()
            if not action_id or action_id in seen_ids:
                continue
            seen_ids.add(action_id)
            actions.append(action)

        actions.sort(key=self._action_sort_key)

        auto_runnable_count = sum(
            1
            for action in actions
            if bool(action.get("auto_runnable", False))
            and str(action.get("status", "") or "").strip().lower() == "ready"
        )
        in_progress_count = sum(
            1 for action in actions if str(action.get("status", "") or "").strip().lower() == "in_progress"
        )
        manual_count = sum(
            1
            for action in actions
            if not bool(action.get("auto_runnable", False))
            or str(action.get("status", "") or "").strip().lower() == "manual"
        )
        runtime_action_count = sum(1 for action in actions if str(action.get("kind", "") or "") == "apply_runtime_template")
        provider_action_count = sum(
            1
            for action in actions
            if str(action.get("kind", "") or "") in {"verify_provider_credentials", "configure_provider_credentials"}
        )
        setup_action_count = sum(
            1
            for action in actions
            if str(action.get("kind", "") or "") in {"launch_setup_install", "launch_manual_pipeline", "manual_pipeline_review"}
        )
        manual_run_action_count = sum(
            1 for action in actions if str(action.get("kind", "") or "") == "launch_manual_pipeline"
        )
        recoverable_task_names = {
            str(action.get("task", "") or "").strip().lower()
            for action in actions
            if str(action.get("task", "") or "").strip()
        }
        estimated_impact_score = round(
            sum(
                float(action.get("estimated_impact_score", 0.0) or 0.0)
                for action in actions
                if bool(action.get("auto_runnable", False))
                and str(action.get("status", "") or "").strip().lower() == "ready"
            ),
            2,
        )
        recommendations = self._build_recommendations(
            actions=actions,
            auto_runnable_count=auto_runnable_count,
            manual_count=manual_count,
            in_progress_count=in_progress_count,
        )
        warnings = self._dedupe_strings(
            warning
            for action in actions
            for warning in (
                action.get("warnings", [])
                if isinstance(action.get("warnings"), list)
                else []
            )
        )
        blockers = self._dedupe_strings(
            blocker
            for action in actions
            if str(action.get("status", "") or "").strip().lower() in {"manual", "blocked"}
            for blocker in (
                action.get("blockers", [])
                if isinstance(action.get("blockers"), list)
                else []
            )
        )

        return {
            "status": "success" if not actions else "partial",
            "generated_at": _utc_now_iso(),
            "stack_name": stack_name,
            "mission_profile": mission_profile,
            "stack_status": str(status_row.get("status", "unknown") or "unknown"),
            "stack_summary": deepcopy(stack_summary),
            "summary": {
                "current_stack_score": float(stack_summary.get("score", 0.0) or 0.0),
                "current_blocked_task_count": int(stack_summary.get("blocked_task_count", 0) or 0),
                "action_count": len(actions),
                "auto_runnable_count": auto_runnable_count,
                "manual_action_count": manual_count,
                "in_progress_count": in_progress_count,
                "runtime_action_count": runtime_action_count,
                "provider_action_count": provider_action_count,
                "setup_action_count": setup_action_count,
                "manual_run_action_count": manual_run_action_count,
                "recoverable_task_count": len(recoverable_task_names),
                "upgrade_action_count": len(upgrade_actions),
                "estimated_impact_score": estimated_impact_score,
                "safe_auto_recovery_available": auto_runnable_count > 0,
                "recovery_ready": len(actions) <= 0,
            },
            "actions": [deepcopy(action) for action in actions],
            "recommendations": recommendations,
            "warnings": warnings,
            "blockers": blockers,
            "upgrade_actions": deepcopy(upgrade_actions),
            "install_runs": {
                "active_count": len(active_install_runs),
                "items": [deepcopy(row) for row in active_install_runs[:8]],
            },
            "manual_runs": {
                "active_count": len(active_manual_runs),
                "items": [deepcopy(row) for row in active_manual_runs[:8]],
            },
            "manual_pipeline_summary": deepcopy(
                manual_pipeline.get("summary", {}) if isinstance(manual_pipeline.get("summary"), dict) else {}
            ),
        }

    def execute(
        self,
        *,
        plan_payload: Dict[str, Any],
        execute_launch_template: LaunchTemplateExecutor,
        launch_setup_install: SetupInstallLauncher,
        launch_manual_pipeline: ManualPipelineLauncher,
        verify_provider_credentials: ProviderVerifier,
        selected_action_ids: Optional[Iterable[str]] = None,
        continue_on_error: bool = True,
    ) -> Dict[str, Any]:
        selected_ids = {
            str(item or "").strip().lower()
            for item in (selected_action_ids or [])
            if str(item or "").strip()
        }
        actions = self._task_rows(plan_payload.get("actions"))
        actions.sort(key=self._action_sort_key)
        results: List[Dict[str, Any]] = []
        executed_count = 0
        skipped_count = 0
        error_count = 0
        accepted_install_runs: List[Dict[str, Any]] = []
        accepted_manual_runs: List[Dict[str, Any]] = []

        for action in actions:
            action_id = str(action.get("id", "") or "").strip().lower()
            if not action_id:
                continue
            if selected_ids and action_id not in selected_ids:
                continue

            clean_status = str(action.get("status", "") or "").strip().lower()
            kind = str(action.get("kind", "") or "").strip().lower()
            if kind not in self._AUTO_RUNNABLE_KINDS or not bool(action.get("auto_runnable", False)) or clean_status != "ready":
                skipped_count += 1
                results.append(
                    {
                        "action_id": action_id,
                        "kind": kind,
                        "status": "skipped",
                        "ok": False,
                        "reason": "action is not auto-runnable right now",
                        "action": deepcopy(action),
                    }
                )
                continue

            if kind == "apply_runtime_template":
                payload = execute_launch_template(
                    str(action.get("task", "") or "").strip().lower(),
                    str(action.get("profile_id", "") or "").strip().lower(),
                    str(action.get("template_id", "") or "").strip().lower(),
                )
            elif kind == "launch_setup_install":
                payload = launch_setup_install(
                    str(action.get("task", "") or "").strip().lower(),
                    [
                        str(item).strip().lower()
                        for item in (action.get("item_keys", []) if isinstance(action.get("item_keys"), list) else [])
                        if str(item).strip()
                    ]
                    or None,
                )
            elif kind == "launch_manual_pipeline":
                payload = launch_manual_pipeline(
                    str(action.get("task", "") or "").strip().lower(),
                    [
                        str(item).strip().lower()
                        for item in (action.get("item_keys", []) if isinstance(action.get("item_keys"), list) else [])
                        if str(item).strip()
                    ]
                    or None,
                )
            elif kind == "verify_provider_credentials":
                payload = verify_provider_credentials(
                    str(action.get("provider", "") or "").strip().lower(),
                    str(action.get("primary_task", "") or "").strip().lower(),
                    [
                        str(item).strip().lower()
                        for item in (action.get("item_keys", []) if isinstance(action.get("item_keys"), list) else [])
                        if str(item).strip()
                    ]
                    or None,
                )
            else:
                skipped_count += 1
                results.append(
                    {
                        "action_id": action_id,
                        "kind": kind,
                        "status": "skipped",
                        "ok": False,
                        "reason": "unknown recovery action",
                        "action": deepcopy(action),
                    }
                )
                continue

            normalized_payload = deepcopy(payload) if isinstance(payload, dict) else {"status": "error", "message": "invalid result"}
            ok = self._execution_ok(kind=kind, payload=normalized_payload)
            if ok:
                executed_count += 1
            else:
                error_count += 1
            if kind == "launch_setup_install":
                run_row = normalized_payload.get("run", {}) if isinstance(normalized_payload.get("run"), dict) else {}
                if run_row:
                    accepted_install_runs.append(deepcopy(run_row))
            elif kind == "launch_manual_pipeline":
                run_row = normalized_payload.get("run", {}) if isinstance(normalized_payload.get("run"), dict) else {}
                if run_row:
                    accepted_manual_runs.append(deepcopy(run_row))
            results.append(
                {
                    "action_id": action_id,
                    "kind": kind,
                    "status": str(normalized_payload.get("status", "error") or "error").strip().lower(),
                    "ok": ok,
                    "result": normalized_payload,
                    "action": deepcopy(action),
                }
            )
            if not ok and not bool(continue_on_error):
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
            "selected_action_ids": sorted(selected_ids),
            "continue_on_error": bool(continue_on_error),
            "accepted_install_run_count": len(accepted_install_runs),
            "accepted_install_runs": accepted_install_runs,
            "accepted_manual_run_count": len(accepted_manual_runs),
            "accepted_manual_runs": accepted_manual_runs,
            "items": results,
        }

    def _build_runtime_action(self, task_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        clean_status = str(task_row.get("status", "") or "").strip().lower()
        if clean_status != "action_required":
            return None
        if str(task_row.get("action_kind", "") or "").strip().lower() != "execute_launch_template":
            return None
        if not bool(task_row.get("auto_applyable", False)) or bool(task_row.get("already_active", False)):
            return None
        task_name = str(task_row.get("task", "") or "").strip().lower()
        profile_id = str(task_row.get("profile_id", "") or "").strip().lower()
        template_id = str(task_row.get("template_id", "") or "").strip().lower()
        if not task_name or not profile_id or not template_id:
            return None
        weight = float(CoworkerStackAdvisor.TASK_WEIGHTS.get(task_name, 1.0))
        return {
            "id": f"runtime-{task_name}-{profile_id}-{template_id}",
            "kind": "apply_runtime_template",
            "stage": "runtime",
            "status": "ready",
            "auto_runnable": True,
            "priority": round(110.0 + (weight * 10.0), 3),
            "estimated_impact_score": round(weight * 1.35, 3),
            "task": task_name,
            "task_scope": [task_name],
            "provider": str(task_row.get("provider", "local") or "local").strip().lower(),
            "profile_id": profile_id,
            "template_id": template_id,
            "title": f"Activate the {task_name} local runtime",
            "summary": f"Apply launch template '{template_id}' for profile '{profile_id}'.",
            "reasons": self._dedupe_strings(task_row.get("recommendations", [])),
            "warnings": self._dedupe_strings(task_row.get("warnings", [])),
            "blockers": [],
        }

    def _build_install_action(
        self,
        *,
        task_name: str,
        task_rows: List[Dict[str, Any]],
        setup_items: List[Dict[str, Any]],
        active_install_runs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        matching_runs = self._matching_install_runs(
            task_name=task_name,
            item_keys=[
                str(item.get("key", "") or "").strip().lower()
                for item in setup_items
                if str(item.get("key", "") or "").strip()
            ],
            install_runs=active_install_runs,
        )
        weight = float(CoworkerStackAdvisor.TASK_WEIGHTS.get(task_name, 1.0))
        in_progress = len(matching_runs) > 0
        status = "in_progress" if in_progress else "ready"
        strategies = sorted(
            {
                str(item.get("strategy", "") or "").strip().lower()
                for item in setup_items
                if str(item.get("strategy", "") or "").strip()
            }
        )
        reasons = [
            f"{len(setup_items)} automation-ready setup item(s) are missing for {task_name}."
        ]
        if any(bool(row.get("route_adjusted", False)) for row in task_rows):
            reasons.append(
                f"The current {task_name} route was adjusted away from its preferred local path, so restoring local assets can improve autonomy."
            )
        warnings: List[str] = []
        if in_progress:
            warnings.append("A model setup install run is already active for this task.")
        return {
            "id": f"install-{task_name}",
            "kind": "launch_setup_install",
            "stage": "setup",
            "status": status,
            "auto_runnable": not in_progress,
            "priority": round(90.0 + (weight * 10.0), 3),
            "estimated_impact_score": round(weight * max(1, len(task_rows)), 3),
            "task": task_name,
            "task_scope": [task_name],
            "item_count": len(setup_items),
            "item_keys": [
                str(item.get("key", "") or "").strip().lower()
                for item in setup_items
                if str(item.get("key", "") or "").strip()
            ],
            "items": [
                {
                    "key": str(item.get("key", "") or "").strip().lower(),
                    "name": str(item.get("name", "") or "").strip(),
                    "strategy": str(item.get("strategy", "") or "").strip().lower(),
                    "source_kind": str(item.get("source_kind", "") or "").strip().lower(),
                    "path": str(item.get("path", "") or "").strip(),
                }
                for item in setup_items
            ],
            "requires_network": True,
            "strategies": strategies,
            "active_runs": [deepcopy(row) for row in matching_runs],
            "title": f"Install missing {task_name} artifacts",
            "summary": f"Queue the automation-ready local {task_name} artifacts so the desktop coworker can recover that capability.",
            "reasons": self._dedupe_strings(reasons),
            "warnings": self._dedupe_strings(warnings),
            "blockers": [],
        }

    def _build_manual_action(
        self,
        *,
        task_name: str,
        task_rows: List[Dict[str, Any]],
        setup_items: List[Dict[str, Any]],
        manual_pipeline_items: List[Dict[str, Any]],
        active_manual_runs: List[Dict[str, Any]],
        upgrade_actions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        weight = float(CoworkerStackAdvisor.TASK_WEIGHTS.get(task_name, 1.0))
        item_keys = [
            str(item.get("key", "") or "").strip().lower()
            for item in setup_items
            if str(item.get("key", "") or "").strip()
        ]
        matching_runs = self._matching_manual_runs(
            task_name=task_name,
            item_keys=item_keys,
            manual_runs=active_manual_runs,
        )
        runnable_items = [
            dict(item)
            for item in manual_pipeline_items
            if self._manual_pipeline_item_runnable(item)
        ]
        recommended_next_actions = self._dedupe_strings(
            item.get("recommended_next_action", "")
            for item in manual_pipeline_items
        )
        reasons = [
            f"{len(setup_items)} local {task_name} artifact(s) still require manual selection or conversion."
        ]
        if any(bool(row.get("route_adjusted", False)) for row in task_rows):
            reasons.append(
                f"Completing the manual {task_name} pipeline will reduce dependency on cloud reroutes."
            )
        blockers = self._dedupe_strings(
            blocker
            for item in manual_pipeline_items
            for blocker in (
                item.get("blockers", [])
                if isinstance(item.get("blockers"), list)
                else []
            )
        )
        if not blockers:
            blockers = self._dedupe_strings(
                blocker
                for item in setup_items
                for blocker in (
                    item.get("blockers", [])
                    if isinstance(item.get("blockers"), list)
                    else []
                )
            )
        warnings = self._dedupe_strings(
            warning
            for item in manual_pipeline_items
            for warning in (
                item.get("warnings", [])
                if isinstance(item.get("warnings"), list)
                else []
            )
        )
        if matching_runs:
            kind = "launch_manual_pipeline"
            status = "in_progress"
            auto_runnable = False
            summary = f"Manual {task_name} conversion is already running in the background."
        elif runnable_items and not blockers:
            kind = "launch_manual_pipeline"
            status = "ready"
            auto_runnable = True
            summary = f"Launch the prepared manual {task_name} conversion pipeline so JARVIS can restore that local capability."
        else:
            kind = "manual_pipeline_review"
            status = "blocked" if blockers else "manual"
            auto_runnable = False
            summary = f"Review the manual pipeline or GGUF conversion plan for the remaining {task_name} artifacts."
        return {
            "id": f"manual-{task_name}",
            "kind": kind,
            "stage": "manual",
            "status": status,
            "auto_runnable": auto_runnable,
            "priority": round(50.0 + (weight * 8.0), 3),
            "estimated_impact_score": round(weight * 0.9, 3),
            "task": task_name,
            "task_scope": [task_name],
            "item_count": len(setup_items),
            "item_keys": item_keys,
            "runnable_item_count": len(runnable_items),
            "runnable_item_keys": [
                str(item.get("key", "") or "").strip().lower()
                for item in runnable_items
                if str(item.get("key", "") or "").strip()
            ],
            "requires_network": any(bool(item.get("auth_required", False)) for item in manual_pipeline_items),
            "active_runs": [deepcopy(row) for row in matching_runs],
            "manual_items": [
                {
                    "key": str(item.get("key", "") or "").strip().lower(),
                    "name": str(item.get("name", "") or "").strip(),
                    "status": str(item.get("status", "") or "").strip().lower(),
                    "convertible": bool(item.get("convertible", False)),
                    "pipeline_kind": str(item.get("pipeline_kind", "") or "").strip().lower(),
                    "recommended_next_action": str(item.get("recommended_next_action", "") or "").strip(),
                    "commands": [
                        str(command).strip()
                        for command in (item.get("commands", []) if isinstance(item.get("commands"), list) else [])
                        if str(command).strip()
                    ][:6],
                }
                for item in manual_pipeline_items
            ],
            "upgrade_actions": [
                {
                    "title": str(action.get("title", "") or "").strip(),
                    "status": str(action.get("status", "") or "").strip().lower(),
                    "commands": [
                        str(command).strip()
                        for command in (action.get("commands", []) if isinstance(action.get("commands"), list) else [])
                        if str(command).strip()
                    ][:4],
                }
                for action in upgrade_actions[:6]
                if isinstance(action, dict)
            ],
            "title": f"Finish the manual {task_name} pipeline",
            "summary": summary,
            "reasons": self._dedupe_strings(reasons + recommended_next_actions),
            "warnings": warnings,
            "blockers": blockers,
        }

    def _build_provider_action(
        self,
        *,
        provider_name: str,
        usage: Dict[str, Any],
        provider_row: Dict[str, Any],
        setup_provider_row: Dict[str, Any],
        verification_stale_after_s: float,
    ) -> Optional[Dict[str, Any]]:
        ready = bool(provider_row.get("ready", False))
        present = bool(provider_row.get("present", False))
        task_scope = [
            str(item).strip().lower()
            for item in usage.get("task_scope", [])
            if str(item).strip()
        ]
        item_keys = [
            str(item).strip().lower()
            for item in usage.get("item_keys", [])
            if str(item).strip()
        ]
        weight = sum(float(CoworkerStackAdvisor.TASK_WEIGHTS.get(task_name, 1.0)) for task_name in task_scope) or 1.0
        if not ready:
            missing_fields = ["api_key"] if not present else []
            missing_fields.extend(
                str(item).strip()
                for item in (provider_row.get("missing_requirements", []) if isinstance(provider_row.get("missing_requirements"), list) else [])
                if str(item).strip()
            )
            reasons = list(usage.get("reasons", [])) if isinstance(usage.get("reasons"), list) else []
            if not present:
                reasons.append(f"{provider_name} is not configured yet.")
            format_reason = str(provider_row.get("format_reason", "") or "").strip()
            if format_reason and format_reason.lower() != "ok":
                reasons.append(format_reason)
            title = f"Configure the {provider_name} provider"
            summary = f"Save the required {provider_name} credentials so the coworker stack can use that provider safely."
            fields = setup_provider_row.get("fields", []) if isinstance(setup_provider_row.get("fields"), list) else []
            return {
                "id": f"provider-config-{provider_name}",
                "kind": "configure_provider_credentials",
                "stage": "manual",
                "status": "manual",
                "auto_runnable": False,
                "priority": round(65.0 + (weight * 4.0), 3),
                "estimated_impact_score": round(weight * 0.8, 3),
                "provider": provider_name,
                "primary_task": task_scope[0] if task_scope else "",
                "task_scope": task_scope,
                "item_keys": item_keys,
                "required_fields": missing_fields,
                "fields": deepcopy(fields),
                "title": title,
                "summary": summary,
                "reasons": self._dedupe_strings(reasons),
                "warnings": self._dedupe_strings(
                    usage.get("warnings", [])
                    if isinstance(usage.get("warnings"), list)
                    else []
                ),
                "blockers": self._dedupe_strings(
                    [
                        f"Missing provider configuration for {provider_name}.",
                        *(
                            provider_row.get("missing_requirements", [])
                            if isinstance(provider_row.get("missing_requirements"), list)
                            else []
                        ),
                    ]
                ),
            }

        verification_state = self._verification_state(provider_row, stale_after_s=verification_stale_after_s)
        if not verification_state.get("needs_verification", False):
            return None
        reasons = list(usage.get("reasons", [])) if isinstance(usage.get("reasons"), list) else []
        reasons.append(str(verification_state.get("reason", "") or "").strip())
        warnings = list(usage.get("warnings", [])) if isinstance(usage.get("warnings"), list) else []
        warnings.append(str(provider_row.get("verification_summary", "") or "").strip())
        return {
            "id": f"provider-verify-{provider_name}",
            "kind": "verify_provider_credentials",
            "stage": "provider",
            "status": "ready",
            "auto_runnable": True,
            "priority": round(95.0 + (weight * 4.0), 3),
            "estimated_impact_score": round(weight * 1.1, 3),
            "provider": provider_name,
            "primary_task": task_scope[0] if task_scope else "",
            "task_scope": task_scope,
            "item_keys": item_keys,
            "requires_network": True,
            "title": f"Verify the {provider_name} provider",
            "summary": f"Run a live credential check for {provider_name} before the coworker stack depends on it.",
            "reasons": self._dedupe_strings(reasons),
            "warnings": self._dedupe_strings(warnings),
            "blockers": [],
        }

    def _provider_usage(
        self,
        *,
        task_rows: List[Dict[str, Any]],
        provider_rows: Dict[str, Dict[str, Any]],
        setup_plan: Dict[str, Any],
        huggingface_item_keys: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        usage: Dict[str, Dict[str, Any]] = {}
        for task_row in task_rows:
            provider_name = str(task_row.get("provider", "") or "").strip().lower()
            if not provider_name or provider_name == "local":
                continue
            row = usage.setdefault(provider_name, {"task_scope": [], "item_keys": [], "reasons": [], "warnings": []})
            task_name = str(task_row.get("task", "") or "").strip().lower()
            if task_name and task_name not in row["task_scope"]:
                row["task_scope"].append(task_name)
            task_status = str(task_row.get("status", "") or "").strip().lower()
            if task_status != "ready":
                row["reasons"].append(
                    f"The {task_name or provider_name} route is blocked until {provider_name} is ready."
                )
            if bool(task_row.get("route_adjusted", False)):
                row["warnings"].append(
                    f"The {task_name or provider_name} route is currently adjusted, so provider health matters for fallback execution."
                )

        setup_items = setup_plan.get("items", []) if isinstance(setup_plan.get("items"), list) else []
        if huggingface_item_keys:
            hf_keys = set(huggingface_item_keys)
            row = usage.setdefault("huggingface", {"task_scope": [], "item_keys": [], "reasons": [], "warnings": []})
            for item in setup_items:
                if not isinstance(item, dict):
                    continue
                clean_key = str(item.get("key", "") or "").strip().lower()
                if clean_key not in hf_keys:
                    continue
                task_name = str(item.get("task", "") or "").strip().lower()
                if task_name and task_name not in row["task_scope"]:
                    row["task_scope"].append(task_name)
                if clean_key and clean_key not in row["item_keys"]:
                    row["item_keys"].append(clean_key)
            row["reasons"].append(
                "Pending Hugging Face-backed model installs or manual pipelines still need verified hub access."
            )

        for provider_name, row in usage.items():
            provider_row = provider_rows.get(provider_name, {})
            if bool(provider_row.get("ready", False)) and not str(provider_row.get("verification_checked_at", "") or "").strip():
                row["reasons"].append(
                    f"{provider_name} is configured, but it has not been live-verified yet."
                )
        return {
            provider: {
                "task_scope": self._dedupe_strings(row.get("task_scope", [])),
                "item_keys": self._dedupe_strings(row.get("item_keys", [])),
                "reasons": self._dedupe_strings(row.get("reasons", [])),
                "warnings": self._dedupe_strings(row.get("warnings", [])),
            }
            for provider, row in usage.items()
        }

    @staticmethod
    def _verification_state(provider_row: Dict[str, Any], *, stale_after_s: float) -> Dict[str, Any]:
        verified = bool(provider_row.get("verification_verified", False))
        checked_at = str(provider_row.get("verification_checked_at", "") or "").strip()
        if not checked_at:
            return {"needs_verification": True, "reason": "No live provider verification has been recorded yet."}
        checked_epoch = CoworkerStackRecoveryPlanner._iso_to_epoch(checked_at)
        if checked_epoch is None:
            return {"needs_verification": True, "reason": "The latest provider verification timestamp is invalid."}
        age_s = max(0.0, datetime.now(timezone.utc).timestamp() - checked_epoch)
        if not verified:
            return {"needs_verification": True, "reason": "The latest provider verification did not pass."}
        if age_s > max(300.0, stale_after_s):
            hours = round(age_s / 3600.0, 1)
            return {"needs_verification": True, "reason": f"The last provider verification is stale ({hours}h old)."}
        return {"needs_verification": False, "reason": ""}

    @staticmethod
    def _execution_ok(*, kind: str, payload: Dict[str, Any]) -> bool:
        status_name = str(payload.get("status", "error") or "error").strip().lower()
        if kind == "verify_provider_credentials":
            verification = payload.get("verification", {}) if isinstance(payload.get("verification"), dict) else {}
            if verification:
                return bool(verification.get("verified", False)) and status_name in {"success", "partial"}
            return status_name == "success"
        if kind in {"launch_setup_install", "launch_manual_pipeline"}:
            return status_name in {"accepted", "success", "partial"}
        return status_name in {"success", "degraded", "partial", "accepted"}

    def _matching_install_runs(
        self,
        *,
        task_name: str,
        item_keys: List[str],
        install_runs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        requested_keys = {
            str(item).strip().lower()
            for item in item_keys
            if str(item).strip()
        }
        rows: List[Dict[str, Any]] = []
        for run in install_runs:
            if not isinstance(run, dict):
                continue
            run_task = str(run.get("task", "") or "").strip().lower()
            run_keys = {
                str(item).strip().lower()
                for item in (
                    run.get("selected_item_keys", [])
                    if isinstance(run.get("selected_item_keys"), list)
                    else []
                )
                if str(item).strip()
            }
            run_item_keys = {
                str(item.get("key", "") or "").strip().lower()
                for item in (
                    run.get("items", [])
                    if isinstance(run.get("items"), list)
                    else []
                )
                if isinstance(item, dict) and str(item.get("key", "") or "").strip()
            }
            if requested_keys and (requested_keys & (run_keys | run_item_keys)):
                rows.append(dict(run))
                continue
            if task_name and run_task == task_name:
                rows.append(dict(run))
        return rows

    def _matching_manual_runs(
        self,
        *,
        task_name: str,
        item_keys: List[str],
        manual_runs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        requested_keys = {
            str(item).strip().lower()
            for item in item_keys
            if str(item).strip()
        }
        rows: List[Dict[str, Any]] = []
        for run in manual_runs:
            if not isinstance(run, dict):
                continue
            run_task = str(run.get("task", "") or "").strip().lower()
            run_keys = {
                str(item).strip().lower()
                for item in (
                    run.get("selected_item_keys", [])
                    if isinstance(run.get("selected_item_keys"), list)
                    else []
                )
                if str(item).strip()
            }
            run_item_keys = {
                str(item.get("key", "") or "").strip().lower()
                for item in (
                    run.get("items", [])
                    if isinstance(run.get("items"), list)
                    else []
                )
                if isinstance(item, dict) and str(item.get("key", "") or "").strip()
            }
            if requested_keys and (requested_keys & (run_keys | run_item_keys)):
                rows.append(dict(run))
                continue
            if task_name and run_task == task_name:
                rows.append(dict(run))
        return rows

    @staticmethod
    def _active_install_runs(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = payload.get("items", []) if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
        return [
            dict(row)
            for row in rows
            if isinstance(row, dict)
            and str(row.get("status", "") or "").strip().lower() in CoworkerStackRecoveryPlanner._ACTIVE_INSTALL_STATUSES
        ]

    @staticmethod
    def _active_manual_runs(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = payload.get("items", []) if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
        return [
            dict(row)
            for row in rows
            if isinstance(row, dict)
            and str(row.get("status", "") or "").strip().lower() in CoworkerStackRecoveryPlanner._ACTIVE_INSTALL_STATUSES
        ]

    @staticmethod
    def _manual_pipeline_item_runnable(item: Dict[str, Any]) -> bool:
        status_name = str(item.get("status", "") or "").strip().lower()
        if status_name not in {"ready", "warning"}:
            return False
        if str(item.get("pipeline_kind", "") or "").strip().lower() == "unresolved_source":
            return False
        if any(
            str(blocker).strip()
            for blocker in (item.get("blockers", []) if isinstance(item.get("blockers"), list) else [])
        ):
            return False
        for step in item.get("steps", []) if isinstance(item.get("steps"), list) else []:
            if not isinstance(step, dict):
                continue
            step_status = str(step.get("status", "") or "").strip().lower()
            if step_status == "blocked":
                continue
            commands = step.get("commands", []) if isinstance(step.get("commands"), list) else []
            if any(str(command).strip() for command in commands):
                return True
        commands = item.get("commands", []) if isinstance(item.get("commands"), list) else []
        return any(str(command).strip() for command in commands)

    @staticmethod
    def _task_rows(value: Any) -> List[Dict[str, Any]]:
        return [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []

    @staticmethod
    def _task_rows_by_name(task_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        rows: Dict[str, List[Dict[str, Any]]] = {}
        for item in task_rows:
            task_name = str(item.get("task", "") or "").strip().lower()
            if not task_name:
                continue
            rows.setdefault(task_name, []).append(item)
        return rows

    @staticmethod
    def _provider_rows(provider_snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        providers = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers"), dict) else {}
        return {
            str(name or "").strip().lower(): dict(row)
            for name, row in providers.items()
            if str(name or "").strip() and isinstance(row, dict)
        }

    @staticmethod
    def _setup_provider_rows(setup_plan: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        rows = setup_plan.get("providers", []) if isinstance(setup_plan.get("providers"), list) else []
        return {
            str(row.get("provider", "") or "").strip().lower(): dict(row)
            for row in rows
            if isinstance(row, dict) and str(row.get("provider", "") or "").strip()
        }

    @staticmethod
    def _setup_items_by_task(setup_plan: Dict[str, Any]) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]], List[str]]:
        auto_rows: Dict[str, List[Dict[str, Any]]] = {}
        manual_rows: Dict[str, List[Dict[str, Any]]] = {}
        huggingface_item_keys: List[str] = []
        items = setup_plan.get("items", []) if isinstance(setup_plan.get("items"), list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            if bool(item.get("present", False)):
                continue
            task_name = str(item.get("task", "") or "").strip().lower()
            if not task_name:
                continue
            clean_key = str(item.get("key", "") or "").strip().lower()
            if str(item.get("source_kind", "") or "").strip().lower() == "huggingface" and clean_key:
                huggingface_item_keys.append(clean_key)
            target = auto_rows if bool(item.get("automation_ready", False)) else manual_rows
            target.setdefault(task_name, []).append(dict(item))
        return auto_rows, manual_rows, CoworkerStackRecoveryPlanner._dedupe_strings(huggingface_item_keys)

    @staticmethod
    def _manual_pipeline_by_task(payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        rows: Dict[str, List[Dict[str, Any]]] = {}
        for item in payload.get("items", []) if isinstance(payload.get("items"), list) else []:
            if not isinstance(item, dict):
                continue
            task_name = str(item.get("task", "") or "").strip().lower()
            if not task_name:
                continue
            rows.setdefault(task_name, []).append(dict(item))
        return rows

    @staticmethod
    def _upgrade_actions(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [dict(item) for item in payload.get("upgrade_actions", []) if isinstance(item, dict)] if isinstance(payload.get("upgrade_actions"), list) else []

    @classmethod
    def _build_recommendations(
        cls,
        *,
        actions: List[Dict[str, Any]],
        auto_runnable_count: int,
        manual_count: int,
        in_progress_count: int,
    ) -> List[str]:
        recommendations: List[str] = []
        if auto_runnable_count > 0:
            top_actions = [str(action.get("title", "") or "").strip() for action in actions if bool(action.get("auto_runnable", False))][:3]
            if top_actions:
                recommendations.append(
                    "Run safe recovery to execute: " + "; ".join(top_actions) + "."
                )
        if in_progress_count > 0:
            recommendations.append("Wait for the active model install runs to finish, then refresh the coworker recovery plan.")
        if manual_count > 0:
            recommendations.append("Review the manual pipeline and provider configuration actions to close the remaining local-model gaps.")
        return cls._dedupe_strings(recommendations)

    @staticmethod
    def _action_sort_key(action: Dict[str, Any]) -> tuple[float, int, str]:
        priority = -float(action.get("priority", 0.0) or 0.0)
        stage = CoworkerStackRecoveryPlanner._STAGE_ORDER.get(str(action.get("stage", "") or "").strip().lower(), 99)
        title = str(action.get("title", "") or "").strip().lower()
        return (priority, stage, title)

    @staticmethod
    def _iso_to_epoch(value: str) -> Optional[float]:
        clean = str(value or "").strip()
        if not clean:
            return None
        try:
            normalized = clean.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized).timestamp()
        except Exception:
            return None

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
