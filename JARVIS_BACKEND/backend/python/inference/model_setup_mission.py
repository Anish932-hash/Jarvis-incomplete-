from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional


WorkspaceScaffoldLauncher = Callable[[bool], Dict[str, Any]]
SetupInstallLauncher = Callable[[str, Optional[List[str]], bool], Dict[str, Any]]
ManualPipelineLauncher = Callable[[str, Optional[List[str]], bool], Dict[str, Any]]
ProviderVerificationLauncher = Callable[[str, str, Optional[List[str]]], Dict[str, Any]]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_model_setup_mission(
    *,
    workspace_payload: Dict[str, Any],
    setup_plan_payload: Dict[str, Any],
    preflight_payload: Optional[Dict[str, Any]] = None,
    manual_pipeline_payload: Optional[Dict[str, Any]] = None,
    install_runs_payload: Optional[Dict[str, Any]] = None,
    manual_runs_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    workspace = workspace_payload if isinstance(workspace_payload, dict) else {}
    setup_plan = setup_plan_payload if isinstance(setup_plan_payload, dict) else {}
    preflight = preflight_payload if isinstance(preflight_payload, dict) else {}
    manual_pipeline = manual_pipeline_payload if isinstance(manual_pipeline_payload, dict) else {}
    install_runs = install_runs_payload if isinstance(install_runs_payload, dict) else {}
    manual_runs = manual_runs_payload if isinstance(manual_runs_payload, dict) else {}
    preflight_items = _preflight_items(preflight)
    ready_install_items = [row for row in preflight_items if bool(row.get("launch_ready", False))]
    blocked_install_items = [row for row in preflight_items if not bool(row.get("launch_ready", False))]
    auth_missing_install_items = [
        row for row in blocked_install_items if _preflight_remote_credential_state(row) == "missing"
    ]
    access_blocked_install_items = [
        row for row in blocked_install_items if _preflight_remote_credential_state(row) == "access_denied"
    ]
    configured_install_items = [
        row for row in preflight_items if _preflight_remote_credential_state(row) == "configured"
    ]
    provider_item_keys = _preflight_provider_item_keys(preflight_items)
    provider_issue_rows = _preflight_provider_issue_rows(blocked_install_items)

    actions: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    def push(action: Optional[Dict[str, Any]]) -> None:
        if not isinstance(action, dict):
            return
        action_id = str(action.get("id", "") or "").strip().lower()
        if not action_id:
            return
        if action_id in seen_ids:
            for existing in actions:
                existing_id = str(existing.get("id", "") or "").strip().lower()
                if existing_id != action_id:
                    continue
                existing_item_keys = _string_list(existing.get("item_keys")) or []
                incoming_item_keys = _string_list(action.get("item_keys")) or []
                merged_item_keys = _dedupe_strings([*existing_item_keys, *incoming_item_keys])
                if merged_item_keys:
                    existing["item_keys"] = merged_item_keys
                    existing["item_count"] = len(merged_item_keys)
                existing["blockers"] = _dedupe_strings(
                    [
                        *(_string_list(existing.get("blockers")) or []),
                        *(_string_list(action.get("blockers")) or []),
                    ]
                )
                existing["warnings"] = _dedupe_strings(
                    [
                        *(_string_list(existing.get("warnings")) or []),
                        *(_string_list(action.get("warnings")) or []),
                    ]
                )
                if not str(existing.get("recommended_next_action", "") or "").strip():
                    existing["recommended_next_action"] = action.get("recommended_next_action", "")
                if not str(existing.get("verification_reason", "") or "").strip():
                    existing["verification_reason"] = action.get("verification_reason", "")
                existing["estimated_impact_score"] = max(
                    float(existing.get("estimated_impact_score", 0.0) or 0.0),
                    float(action.get("estimated_impact_score", 0.0) or 0.0),
                )
                return
            return
        seen_ids.add(action_id)
        actions.append(action)

    push(_workspace_action(workspace))

    required_providers = [
        dict(row)
        for row in workspace.get("required_providers", [])
        if isinstance(row, dict)
    ]
    for provider_row in required_providers:
        if bool(provider_row.get("ready", False)):
            provider_name = str(provider_row.get("provider", "") or "").strip().lower()
            if not provider_name:
                continue
            verification_action = _provider_verification_action(
                provider_row=provider_row,
                item_keys=provider_item_keys.get(provider_name, []),
                issue_rows=provider_issue_rows.get(provider_name, []),
            )
            push(verification_action)
            continue
        provider_name = str(provider_row.get("provider", "") or "").strip().lower()
        if not provider_name:
            continue
        blockers = [
            str(item).strip()
            for item in provider_row.get("missing_requirements", [])
            if str(item).strip()
        ] if isinstance(provider_row.get("missing_requirements", []), list) else []
        push(
            {
                "id": f"configure_provider:{provider_name}",
                "kind": "configure_provider_credentials",
                "stage": "provider",
                "title": f"Configure {provider_name}",
                "status": "manual",
                "auto_runnable": False,
                "provider": provider_name,
                "estimated_impact_score": 24.0,
                "blockers": blockers,
                "warnings": [],
                "recommended_next_action": "save_and_verify_provider_credentials",
            }
        )
    if (access_blocked_install_items or configured_install_items) and "huggingface" not in {
        str(row.get("provider", "") or "").strip().lower()
        for row in required_providers
        if isinstance(row, dict)
    }:
        push(
            {
                "id": "verify_provider:huggingface",
                "kind": "verify_provider_credentials",
                "stage": "provider",
                "title": "Verify Hugging Face repository access",
                "status": "ready",
                "auto_runnable": True,
                "provider": "huggingface",
                "task": "",
                "item_keys": _preflight_item_keys(access_blocked_install_items or configured_install_items),
                "item_count": len(access_blocked_install_items or configured_install_items),
                "estimated_impact_score": 26.0,
                "blockers": [],
                "warnings": _dedupe_strings(
                    [_first_issue(access_blocked_install_items, key="blockers")] if access_blocked_install_items else []
                ),
                "recommended_next_action": "verify_provider_credentials",
                "verification_reason": (
                    "Configured Hugging Face credentials should be live-verified against the pending repositories."
                ),
            }
        )
    if auth_missing_install_items:
        first_auth_blocker = _first_issue(auth_missing_install_items, key="blockers")
        push(
            {
                "id": "configure_provider:huggingface",
                "kind": "configure_provider_credentials",
                "stage": "acquisition",
                "title": "Configure Hugging Face access for gated sources",
                "status": "manual",
                "auto_runnable": False,
                "provider": "huggingface",
                "item_keys": _preflight_item_keys(auth_missing_install_items),
                "item_count": len(auth_missing_install_items),
                "estimated_impact_score": 28.0,
                "blockers": [first_auth_blocker] if first_auth_blocker else ["Gated Hugging Face sources need an access token."],
                "warnings": [],
                "recommended_next_action": "save_and_verify_provider_credentials",
            }
        )
    if access_blocked_install_items:
        first_access_blocker = _first_issue(access_blocked_install_items, key="blockers")
        push(
            {
                "id": "review_provider_access:huggingface",
                "kind": "review_provider_access",
                "stage": "acquisition",
                "title": "Review Hugging Face repository access",
                "status": "manual",
                "auto_runnable": False,
                "provider": "huggingface",
                "item_keys": _preflight_item_keys(access_blocked_install_items),
                "item_count": len(access_blocked_install_items),
                "estimated_impact_score": 21.0,
                "blockers": [first_access_blocker] if first_access_blocker else [],
                "warnings": [],
                "recommended_next_action": "verify_provider_credentials",
            }
        )

    preflight_summary = preflight.get("summary", {}) if isinstance(preflight.get("summary"), dict) else {}
    blocked_preflight_count = int(preflight_summary.get("blocked_count", 0) or 0)
    if blocked_preflight_count > 0:
        first_blocker = _first_issue(preflight.get("items", []), key="blockers")
        push(
            {
                "id": "review_preflight_blockers",
                "kind": "review_preflight_blockers",
                "stage": "preflight",
                "title": "Review setup preflight blockers",
                "status": "blocked",
                "auto_runnable": False,
                "estimated_impact_score": 18.0,
                "blockers": [first_blocker] if first_blocker else [],
                "warnings": [],
                "recommended_next_action": "fix_preflight_blockers",
            }
        )

    install_items = ready_install_items or [
        dict(row)
        for row in setup_plan.get("items", [])
        if isinstance(row, dict) and bool(row.get("automation_ready", False))
    ]
    active_install_count = int(install_runs.get("active_count", 0) or 0)
    if install_items or active_install_count > 0:
        install_status = "in_progress" if active_install_count > 0 else "ready"
        install_blockers = []
        install_warnings = []
        if blocked_install_items:
            install_warnings.append(
                f"{len(blocked_install_items)} automation-ready source"
                f"{'' if len(blocked_install_items) == 1 else 's'} remain blocked and will be skipped for now."
            )
        if blocked_preflight_count > len(blocked_install_items):
            install_warnings.append("Some setup items still need review before a full setup pass can complete.")
        push(
            {
                "id": "launch_setup_install:auto",
                "kind": "launch_setup_install",
                "stage": "setup",
                "title": "Run auto-installable model setup tasks",
                "status": install_status,
                "auto_runnable": install_status == "ready",
                "task": "",
                "item_keys": [
                    str(row.get("key", "")).strip()
                    for row in install_items
                    if str(row.get("key", "")).strip()
                ],
                "item_count": len(install_items),
                "estimated_impact_score": 38.0,
                "blockers": install_blockers,
                "warnings": install_warnings,
                "recommended_next_action": "launch_setup_install",
            }
        )
    if blocked_install_items:
        first_blocker = _first_issue(blocked_install_items, key="blockers")
        push(
            {
                "id": "review_setup_install_blockers",
                "kind": "review_setup_install_blockers",
                "stage": "acquisition",
                "title": "Review blocked auto-install sources",
                "status": "manual",
                "auto_runnable": False,
                "item_keys": _preflight_item_keys(blocked_install_items),
                "item_count": len(blocked_install_items),
                "estimated_impact_score": 19.0,
                "blockers": [first_blocker] if first_blocker else [],
                "warnings": [],
                "recommended_next_action": "review_setup_preflight",
            }
        )

    manual_items = [
        dict(row)
        for row in manual_pipeline.get("items", [])
        if isinstance(row, dict)
    ]
    runnable_manual_items = [
        row
        for row in manual_items
        if str(row.get("status", "") or "").strip().lower() in {"ready", "warning"}
        and not _has_strings(row.get("blockers"))
    ]
    blocked_manual_items = [
        row
        for row in manual_items
        if _has_strings(row.get("blockers"))
    ]
    active_manual_count = int(manual_runs.get("active_count", 0) or 0)
    if runnable_manual_items or active_manual_count > 0:
        manual_status = "in_progress" if active_manual_count > 0 else "ready"
        push(
            {
                "id": "launch_manual_pipeline:all",
                "kind": "launch_manual_pipeline",
                "stage": "manual",
                "title": "Run manual conversion pipeline tasks",
                "status": manual_status,
                "auto_runnable": manual_status == "ready",
                "task": "",
                "item_keys": [
                    str(row.get("key", "")).strip()
                    for row in runnable_manual_items
                    if str(row.get("key", "")).strip()
                ],
                "item_count": len(runnable_manual_items),
                "estimated_impact_score": 31.0,
                "blockers": [],
                "warnings": [],
                "recommended_next_action": "launch_manual_pipeline",
            }
        )
    if blocked_manual_items:
        first_blocker = _first_issue(blocked_manual_items, key="blockers")
        push(
            {
                "id": "review_manual_pipeline_blockers",
                "kind": "review_manual_pipeline_blockers",
                "stage": "manual_review",
                "title": "Review blocked manual model tasks",
                "status": "manual",
                "auto_runnable": False,
                "item_count": len(blocked_manual_items),
                "estimated_impact_score": 14.0,
                "blockers": [first_blocker] if first_blocker else [],
                "warnings": [],
                "recommended_next_action": "review_manual_pipeline",
            }
        )

    actions.sort(key=_action_sort_key)
    ready_action_count = sum(
        1 for action in actions if bool(action.get("auto_runnable", False)) and str(action.get("status", "")).strip().lower() == "ready"
    )
    manual_action_count = sum(
        1 for action in actions if not bool(action.get("auto_runnable", False)) and str(action.get("status", "")).strip().lower() in {"manual", "blocked"}
    )
    in_progress_count = sum(1 for action in actions if str(action.get("status", "")).strip().lower() == "in_progress")
    blocked_action_count = sum(1 for action in actions if str(action.get("status", "")).strip().lower() == "blocked")
    verification_action_count = sum(
        1 for action in actions if str(action.get("kind", "")).strip().lower() == "verify_provider_credentials"
    )
    mission_status = "ready" if ready_action_count > 0 else ("blocked" if actions else "complete")
    if in_progress_count > 0:
        mission_status = "in_progress"
    if ready_action_count <= 0 and manual_action_count > 0 and in_progress_count <= 0:
        mission_status = "manual"

    recommendations = _dedupe_strings(
        action.get("title", "")
        for action in actions
        if bool(action.get("recommended_next_action"))
    )

    return {
        "status": "success",
        "generated_at": _utc_now_iso(),
        "mission_status": mission_status,
        "summary": {
            "action_count": len(actions),
            "ready_action_count": ready_action_count,
            "manual_action_count": manual_action_count,
            "blocked_action_count": blocked_action_count,
            "in_progress_count": in_progress_count,
            "acquisition_ready_count": len(ready_install_items),
            "acquisition_blocked_count": len(blocked_install_items),
            "auth_missing_count": len(auth_missing_install_items),
            "access_blocked_count": len(access_blocked_install_items),
            "verification_action_count": verification_action_count,
            "launch_recommended": ready_action_count > 0,
            "workspace_ready": bool(_summary_value(workspace, "workspace_ready")),
            "stack_ready": bool(_summary_value(workspace, "stack_ready")),
            "readiness_score": int(_summary_value(workspace, "readiness_score", 0) or 0),
        },
        "actions": [deepcopy(action) for action in actions],
        "recommendations": recommendations,
        "workspace": deepcopy(workspace),
        "setup_plan": deepcopy(setup_plan),
        "preflight": deepcopy(preflight),
        "manual_pipeline": deepcopy(manual_pipeline),
        "install_runs": deepcopy(install_runs),
        "manual_runs": deepcopy(manual_runs),
    }


def execute_model_setup_mission(
    *,
    mission_payload: Dict[str, Any],
    execute_workspace_scaffold: WorkspaceScaffoldLauncher,
    launch_setup_install: SetupInstallLauncher,
    launch_manual_pipeline: ManualPipelineLauncher,
    verify_provider_credentials: Optional[ProviderVerificationLauncher] = None,
    selected_action_ids: Optional[Iterable[str]] = None,
    dry_run: bool = False,
    continue_on_error: bool = True,
) -> Dict[str, Any]:
    selected_ids = {
        str(item or "").strip().lower()
        for item in (selected_action_ids or [])
        if str(item or "").strip()
    }
    actions = [
        dict(row)
        for row in mission_payload.get("actions", [])
        if isinstance(row, dict)
    ]
    actions.sort(key=_action_sort_key)
    results: List[Dict[str, Any]] = []
    executed_count = 0
    skipped_count = 0
    error_count = 0

    for action in actions:
        action_id = str(action.get("id", "") or "").strip().lower()
        if not action_id:
            continue
        if selected_ids and action_id not in selected_ids:
            continue
        status_name = str(action.get("status", "") or "").strip().lower()
        kind = str(action.get("kind", "") or "").strip().lower()
        if not bool(action.get("auto_runnable", False)) or status_name != "ready":
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

        if dry_run:
            executed_count += 1
            results.append(
                {
                    "action_id": action_id,
                    "kind": kind,
                    "status": "planned",
                    "ok": True,
                    "action": deepcopy(action),
                }
            )
            continue

        if kind == "scaffold_workspace":
            payload = execute_workspace_scaffold(False)
        elif kind == "launch_setup_install":
            payload = launch_setup_install(
                str(action.get("task", "") or "").strip().lower(),
                _string_list(action.get("item_keys")),
                False,
            )
        elif kind == "launch_manual_pipeline":
            payload = launch_manual_pipeline(
                str(action.get("task", "") or "").strip().lower(),
                _string_list(action.get("item_keys")),
                False,
            )
        elif kind == "verify_provider_credentials":
            if not callable(verify_provider_credentials):
                payload = {"status": "error", "message": "provider verification launcher unavailable"}
            else:
                payload = verify_provider_credentials(
                    str(action.get("provider", "") or "").strip().lower(),
                    str(action.get("task", "") or "").strip().lower(),
                    _string_list(action.get("item_keys")),
                )
        else:
            payload = {"status": "error", "message": f"unsupported action kind: {kind}"}

        payload = deepcopy(payload) if isinstance(payload, dict) else {"status": "error", "message": "invalid execution payload"}
        payload_status = str(payload.get("status", "error") or "error").strip().lower()
        ok = _execution_ok(kind=kind, payload=payload)
        if ok:
            executed_count += 1
        else:
            error_count += 1
        results.append(
            {
                "action_id": action_id,
                "kind": kind,
                "status": payload_status,
                "ok": ok,
                "result": payload,
                "action": deepcopy(action),
            }
        )
        if not ok and not continue_on_error:
            break

    status = "success"
    if error_count > 0 and executed_count <= 0:
        status = "error"
    elif error_count > 0:
        status = "partial"
    elif dry_run:
        status = "planned"
    elif executed_count <= 0:
        status = "skipped"

    return {
        "status": status,
        "generated_at": _utc_now_iso(),
        "dry_run": bool(dry_run),
        "executed_count": executed_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "items": results,
        "selected_action_ids": sorted(selected_ids),
    }


def _workspace_action(workspace: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    missing_directory_count = int(_summary_value(workspace, "missing_directory_count", 0) or 0)
    if missing_directory_count <= 0:
        return None
    directory_actions = [
        dict(row)
        for row in workspace.get("directory_actions", [])
        if isinstance(row, dict)
    ]
    blocked = any(not bool(row.get("safe", False)) for row in directory_actions)
    return {
        "id": "scaffold_workspace",
        "kind": "scaffold_workspace",
        "stage": "workspace",
        "title": "Create missing manifest directories",
        "status": "blocked" if blocked else "ready",
        "auto_runnable": not blocked,
        "item_count": missing_directory_count,
        "estimated_impact_score": 16.0,
        "blockers": ["One or more target paths are outside the workspace root."] if blocked else [],
        "warnings": [],
        "recommended_next_action": "scaffold_workspace",
    }


def _summary_value(payload: Dict[str, Any], key: str, default: Any = None) -> Any:
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    return summary.get(key, default)


def _string_list(value: Any) -> Optional[List[str]]:
    if not isinstance(value, list):
        return None
    rows = [str(item).strip() for item in value if str(item).strip()]
    return rows or None


def _has_strings(value: Any) -> bool:
    return any(str(item).strip() for item in value) if isinstance(value, list) else False


def _first_issue(rows: Any, *, key: str) -> str:
    items = rows if isinstance(rows, list) else []
    for row in items:
        if not isinstance(row, dict):
            continue
        values = row.get(key, [])
        if not isinstance(values, list):
            continue
        for value in values:
            clean = str(value).strip()
            if clean:
                return clean
    return ""


def _dedupe_strings(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    rows: List[str] = []
    for value in values:
        clean = str(value or "").strip()
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
    return rows


def _action_sort_key(action: Dict[str, Any]) -> tuple[int, int, str]:
    stage_order = {
        "workspace": 0,
        "provider": 1,
        "acquisition": 2,
        "preflight": 3,
        "setup": 4,
        "manual": 5,
        "manual_review": 6,
    }
    status_order = {
        "ready": 0,
        "in_progress": 1,
        "manual": 2,
        "blocked": 3,
        "skipped": 4,
    }
    stage = str(action.get("stage", "") or "").strip().lower()
    status_name = str(action.get("status", "") or "").strip().lower()
    return (
        stage_order.get(stage, 99),
        status_order.get(status_name, 99),
        str(action.get("id", "") or "").strip().lower(),
    )


def _execution_ok(*, kind: str, payload: Dict[str, Any]) -> bool:
    status_name = str(payload.get("status", "error") or "error").strip().lower()
    if kind == "verify_provider_credentials":
        verification = payload.get("verification", {}) if isinstance(payload.get("verification", {}), dict) else {}
        if verification:
            return bool(verification.get("verified", False)) and status_name in {"success", "partial"}
        return status_name == "success"
    if kind in {"launch_setup_install", "launch_manual_pipeline"}:
        return status_name in {"accepted", "success", "partial"}
    return status_name in {"success", "degraded", "partial", "accepted"}


def _preflight_items(preflight_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = preflight_payload if isinstance(preflight_payload, dict) else {}
    return [
        dict(row)
        for row in payload.get("items", [])
        if isinstance(row, dict)
    ]


def _preflight_item_keys(rows: List[Dict[str, Any]]) -> List[str]:
    return [
        str(row.get("key", "") or "").strip()
        for row in rows
        if str(row.get("key", "") or "").strip()
    ]


def _preflight_remote_credential_state(row: Dict[str, Any]) -> str:
    remote_probe = row.get("remote_probe", {}) if isinstance(row.get("remote_probe", {}), dict) else {}
    remote_acquisition = row.get("remote_acquisition", {}) if isinstance(row.get("remote_acquisition", {}), dict) else {}
    return str(
        remote_acquisition.get("credential_state", "") or remote_probe.get("credential_state", "") or ""
    ).strip().lower()


def _preflight_provider_name(row: Dict[str, Any]) -> str:
    remote_probe = row.get("remote_probe", {}) if isinstance(row.get("remote_probe", {}), dict) else {}
    strategy = str(row.get("strategy", "") or "").strip().lower()
    if str(remote_probe.get("repo_id", "") or "").strip():
        return "huggingface"
    if "huggingface" in strategy:
        return "huggingface"
    return ""


def _preflight_provider_item_keys(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for row in rows:
        provider_name = _preflight_provider_name(row)
        if not provider_name:
            continue
        clean_key = str(row.get("key", "") or "").strip()
        if not clean_key:
            continue
        bucket = result.setdefault(provider_name, [])
        if clean_key not in bucket:
            bucket.append(clean_key)
    return result


def _preflight_provider_issue_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        provider_name = _preflight_provider_name(row)
        if not provider_name:
            continue
        result.setdefault(provider_name, []).append(row)
    return result


def _provider_verification_action(
    *,
    provider_row: Dict[str, Any],
    item_keys: List[str],
    issue_rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    provider_name = str(provider_row.get("provider", "") or "").strip().lower()
    if not provider_name or not bool(provider_row.get("ready", False)):
        return None
    verification_checked_at = str(provider_row.get("verification_checked_at", "") or "").strip()
    verification_verified = bool(provider_row.get("verification_verified", False))
    verification_status = str(provider_row.get("verification_status", "") or "").strip().lower()
    verification_summary = str(provider_row.get("verification_summary", "") or "").strip()
    if verification_checked_at and verification_verified and verification_status in {"", "success"} and not issue_rows:
        return None
    reason = "No live provider verification has been recorded yet."
    if verification_checked_at and not verification_verified:
        reason = "The latest provider verification did not pass."
    elif issue_rows:
        reason = _first_issue(issue_rows, key="blockers") or "Pending setup items still need a verified provider check."
    warnings = [verification_summary] if verification_summary else []
    return {
        "id": f"verify_provider:{provider_name}",
        "kind": "verify_provider_credentials",
        "stage": "provider",
        "title": f"Verify {provider_name} access",
        "status": "ready",
        "auto_runnable": True,
        "provider": provider_name,
        "task": "",
        "item_keys": item_keys or None,
        "item_count": len(item_keys),
        "estimated_impact_score": 26.0,
        "blockers": [],
        "warnings": _dedupe_strings(warnings),
        "recommended_next_action": "verify_provider_credentials",
        "verification_reason": reason,
    }
