from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional


def build_model_setup_workspace(
    *,
    manifest_payload: Dict[str, Any],
    provider_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest = manifest_payload if isinstance(manifest_payload, dict) else {}
    provider_state = provider_snapshot if isinstance(provider_snapshot, dict) else {}
    workspace_root = _resolve_workspace_root(manifest)
    directories = [
        dict(row)
        for row in manifest.get("directories", [])
        if isinstance(row, dict)
    ]
    models = [
        dict(row)
        for row in manifest.get("models", [])
        if isinstance(row, dict)
    ]
    provider_rows = _required_provider_rows(
        required_providers=manifest.get("providers", []),
        provider_snapshot=provider_state,
    )
    directory_actions = _directory_actions(
        directories=directories,
        workspace_root=workspace_root,
    )

    present_directory_count = sum(1 for row in directories if bool(row.get("present", False)))
    missing_directory_count = sum(1 for row in directories if bool(row.get("missing", False)))
    present_model_count = sum(1 for row in models if bool(row.get("present", False)))
    missing_model_count = sum(1 for row in models if bool(row.get("missing", False)))
    ready_required_provider_count = sum(1 for row in provider_rows if bool(row.get("ready", False)))
    missing_required_provider_count = sum(1 for row in provider_rows if not bool(row.get("ready", False)))

    directory_ready = missing_directory_count <= 0
    provider_ready = missing_required_provider_count <= 0
    artifact_ready = missing_model_count <= 0
    workspace_ready = directory_ready and provider_ready
    stack_ready = workspace_ready and artifact_ready
    readiness_score = _readiness_score(
        present_directory_count=present_directory_count,
        total_directory_count=len(directories),
        ready_required_provider_count=ready_required_provider_count,
        total_required_provider_count=len(provider_rows),
        present_model_count=present_model_count,
        total_model_count=len(models),
    )

    recommendations: List[str] = []
    if missing_directory_count > 0:
        recommendations.append(
            f"Create {missing_directory_count} missing manifest directory"
            f"{'' if missing_directory_count == 1 else 'ies'} before placing new local models."
        )
    if missing_required_provider_count > 0:
        recommendations.append(
            f"Configure {missing_required_provider_count} required provider credential"
            f"{'' if missing_required_provider_count == 1 else 's'} to unlock cloud-backed setup tasks."
        )
    if missing_model_count > 0:
        recommendations.append(
            f"{missing_model_count} manifest-declared model artifact"
            f"{'' if missing_model_count == 1 else 's'} are still missing."
        )
    if not recommendations:
        recommendations.append("Manifest directories, required providers, and declared model artifacts all look ready.")

    return {
        "status": "success",
        "workspace_root": str(workspace_root),
        "manifest_path": str(manifest.get("path", "") or ""),
        "directories": directories,
        "directory_actions": directory_actions,
        "required_providers": provider_rows,
        "recommendations": recommendations,
        "summary": {
            "directory_count": len(directories),
            "present_directory_count": present_directory_count,
            "missing_directory_count": missing_directory_count,
            "required_provider_count": len(provider_rows),
            "ready_required_provider_count": ready_required_provider_count,
            "missing_required_provider_count": missing_required_provider_count,
            "model_count": len(models),
            "present_model_count": present_model_count,
            "missing_model_count": missing_model_count,
            "directory_ready": directory_ready,
            "provider_ready": provider_ready,
            "workspace_ready": workspace_ready,
            "artifact_ready": artifact_ready,
            "stack_ready": stack_ready,
            "readiness_score": readiness_score,
        },
        "manifest": {
            "status": str(manifest.get("status", "unknown") or "unknown"),
            "path": str(manifest.get("path", "") or ""),
            "workspace_root": str(manifest.get("workspace_root", "") or str(workspace_root)),
            "model_count": int(manifest.get("model_count", len(models)) or len(models)),
            "directory_count": int(manifest.get("directory_count", len(directories)) or len(directories)),
            "provider_count": int(manifest.get("provider_count", len(provider_rows)) or len(provider_rows)),
            "providers": list(manifest.get("providers", [])) if isinstance(manifest.get("providers", []), list) else [],
            "directories": directories,
        },
    }


def scaffold_model_setup_workspace(
    *,
    manifest_payload: Dict[str, Any],
    provider_snapshot: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    workspace_payload = build_model_setup_workspace(
        manifest_payload=manifest_payload,
        provider_snapshot=provider_snapshot,
    )
    workspace_root = _resolve_workspace_root(manifest_payload)
    actions = [
        dict(row)
        for row in workspace_payload.get("directory_actions", [])
        if isinstance(row, dict)
    ]
    results: List[Dict[str, Any]] = []
    created_count = 0
    existing_count = 0
    blocked_count = 0
    error_count = 0

    for action in actions:
        target_path = Path(str(action.get("path", "") or "").strip())
        if not str(target_path).strip():
            blocked_count += 1
            results.append(
                {
                    **action,
                    "status": "blocked",
                    "message": "path is missing",
                }
            )
            continue
        if not _is_relative_to(target_path, workspace_root):
            blocked_count += 1
            results.append(
                {
                    **action,
                    "status": "blocked",
                    "message": "path is outside the workspace root",
                }
            )
            continue
        if target_path.exists() and not target_path.is_dir():
            blocked_count += 1
            results.append(
                {
                    **action,
                    "status": "blocked",
                    "message": "target exists as a file",
                }
            )
            continue
        if dry_run:
            results.append(
                {
                    **action,
                    "status": "planned",
                    "message": "directory would be created",
                }
            )
            continue
        try:
            existed_before = target_path.exists() and target_path.is_dir()
            target_path.mkdir(parents=True, exist_ok=True)
            if existed_before:
                existing_count += 1
            else:
                created_count += 1
            results.append(
                {
                    **action,
                    "status": "exists" if existed_before else "created",
                    "message": "directory already existed" if existed_before else "directory created",
                }
            )
        except Exception as exc:  # noqa: BLE001
            error_count += 1
            results.append(
                {
                    **action,
                    "status": "error",
                    "message": str(exc),
                }
            )

    return {
        "status": "success" if error_count <= 0 else "partial",
        "dry_run": bool(dry_run),
        "action_count": len(results),
        "created_count": created_count,
        "existing_count": existing_count,
        "blocked_count": blocked_count,
        "error_count": error_count,
        "actions": results,
        "workspace": workspace_payload,
    }


def _required_provider_rows(
    *,
    required_providers: Any,
    provider_snapshot: Dict[str, Any],
) -> List[Dict[str, Any]]:
    provider_names = [
        str(item or "").strip().lower()
        for item in (required_providers if isinstance(required_providers, list) else [])
        if str(item or "").strip()
    ]
    providers = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers", {}), dict) else {}
    rows: List[Dict[str, Any]] = []
    for provider_name in provider_names:
        provider_row = providers.get(provider_name, {})
        provider_data = provider_row if isinstance(provider_row, dict) else {}
        missing_requirements = [
            str(item).strip()
            for item in provider_data.get("missing_requirements", [])
            if str(item).strip()
        ] if isinstance(provider_data.get("missing_requirements", []), list) else []
        rows.append(
            {
                "provider": provider_name,
                "required_by_manifest": True,
                "present": bool(provider_data.get("present", False)),
                "ready": bool(provider_data.get("ready", False)),
                "source": str(provider_data.get("source", "none") or "none"),
                "credential_label": str(provider_data.get("credential_label", "API Key") or "API Key"),
                "missing_requirements": missing_requirements,
                "redacted": str(provider_data.get("redacted", "") or ""),
                "fingerprint": str(provider_data.get("fingerprint", "") or ""),
                "verification_status": str(provider_data.get("verification_status", "") or ""),
                "verification_verified": bool(provider_data.get("verification_verified", False)),
                "verification_checked_at": str(provider_data.get("verification_checked_at", "") or ""),
                "verification_summary": str(provider_data.get("verification_summary", "") or ""),
                "last_verification": (
                    dict(provider_data.get("last_verification", {}))
                    if isinstance(provider_data.get("last_verification", {}), dict)
                    else {}
                ),
            }
        )
    return rows


def _directory_actions(*, directories: List[Dict[str, Any]], workspace_root: Path) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    for row in directories:
        if bool(row.get("present", False)):
            continue
        target_path = Path(str(row.get("path", "") or "").strip())
        actions.append(
            {
                "kind": "create_directory",
                "name": str(row.get("name", "") or ""),
                "task": str(row.get("task", "") or ""),
                "path": str(target_path),
                "workspace_relative_path": _workspace_relative_path(target_path, workspace_root),
                "aliases": list(row.get("aliases", [])) if isinstance(row.get("aliases", []), list) else [],
                "safe": _is_relative_to(target_path, workspace_root),
                "present": bool(row.get("present", False)),
            }
        )
    return actions


def _workspace_relative_path(path: Path, workspace_root: Path) -> str:
    try:
        return str(path.relative_to(workspace_root)).replace("\\", "/")
    except Exception:
        return ""


def _resolve_workspace_root(manifest_payload: Dict[str, Any]) -> Path:
    workspace_root = Path(str(manifest_payload.get("workspace_root", "") or "").strip() or ".")
    try:
        return workspace_root.resolve(strict=False)
    except Exception:
        return workspace_root


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
        return True
    except Exception:
        return False


def _readiness_score(
    *,
    present_directory_count: int,
    total_directory_count: int,
    ready_required_provider_count: int,
    total_required_provider_count: int,
    present_model_count: int,
    total_model_count: int,
) -> int:
    directory_ratio = (
        1.0
        if total_directory_count <= 0
        else max(0.0, min(1.0, float(present_directory_count) / float(total_directory_count)))
    )
    provider_ratio = (
        1.0
        if total_required_provider_count <= 0
        else max(0.0, min(1.0, float(ready_required_provider_count) / float(total_required_provider_count)))
    )
    model_ratio = (
        1.0
        if total_model_count <= 0
        else max(0.0, min(1.0, float(present_model_count) / float(total_model_count)))
    )
    score = (directory_ratio * 25.0) + (provider_ratio * 25.0) + (model_ratio * 50.0)
    return int(round(max(0.0, min(100.0, score))))
