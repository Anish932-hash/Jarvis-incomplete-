from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional

from .contracts import ActionResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class RollbackEntry:
    rollback_id: str
    action: str
    source: str
    goal_id: str
    args: Dict[str, Any]
    operations: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "ready"
    created_at: str = field(default_factory=lambda: _iso(_utc_now()))
    updated_at: str = field(default_factory=lambda: _iso(_utc_now()))
    executed_at: str = ""
    error: str = ""
    execution_log: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RollbackManager:
    """
    Records compensating operations for reversible actions and executes rollback.
    """

    def __init__(
        self,
        *,
        store_path: str = "data/rollback_journal.json",
        backup_dir: str = "data/rollback_backups",
        max_entries: int = 20_000,
    ) -> None:
        self.store_path = Path(store_path)
        self.backup_dir = Path(backup_dir)
        self.max_entries = max(200, min(int(max_entries), 200_000))
        self._entries: Dict[str, RollbackEntry] = {}
        self._lock = RLock()
        self._load_locked()

    _LOCAL_REVERSIBLE_ACTIONS = {
        "write_file",
        "copy_file",
        "clipboard_write",
        "create_folder",
    }

    _EXTERNAL_HIGH_IMPACT_ACTIONS = {
        "external_email_send",
        "external_calendar_create_event",
        "external_calendar_update_event",
        "external_doc_create",
        "external_doc_update",
        "external_task_create",
        "external_task_update",
        "oauth_token_upsert",
        "oauth_token_revoke",
    }

    _EXTERNAL_MUTATION_ACTIONS = {
        "external_email_send",
        "external_calendar_create_event",
        "external_calendar_update_event",
        "external_doc_create",
        "external_doc_update",
        "external_task_create",
        "external_task_update",
        "oauth_token_upsert",
        "oauth_token_refresh",
        "oauth_token_maintain",
        "oauth_token_revoke",
    }

    def capture_pre_state(self, *, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
        normalized_action = str(action or "").strip().lower()
        payload = dict(args) if isinstance(args, dict) else {}
        if normalized_action == "write_file":
            return self._capture_write_file_state(payload)
        if normalized_action == "copy_file":
            return self._capture_copy_file_state(payload)
        if normalized_action == "clipboard_write":
            return self._capture_clipboard_state()
        if normalized_action == "create_folder":
            return self._capture_create_folder_state(payload)
        return {}

    def rollback_profile(self, *, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
        normalized_action = str(action or "").strip().lower()
        payload = dict(args) if isinstance(args, dict) else {}
        if not normalized_action:
            return {
                "action": "",
                "rollback_supported": False,
                "reversible": False,
                "reversibility_class": "unknown",
                "high_impact": False,
                "requires_branch": False,
                "branch_reason": "missing_action",
            }

        if normalized_action in self._LOCAL_REVERSIBLE_ACTIONS:
            return {
                "action": normalized_action,
                "rollback_supported": True,
                "reversible": True,
                "reversibility_class": "full",
                "high_impact": normalized_action in {"write_file", "copy_file"},
                "requires_branch": False,
                "branch_reason": "",
                "pre_state_capturable": True,
            }

        if normalized_action.startswith("external_") or normalized_action.startswith("oauth_token_"):
            high_impact = normalized_action in self._EXTERNAL_HIGH_IMPACT_ACTIONS
            is_mutation = normalized_action in self._EXTERNAL_MUTATION_ACTIONS
            provider = str(payload.get("provider", "")).strip().lower()
            return {
                "action": normalized_action,
                "provider": provider,
                "rollback_supported": False,
                "reversible": False,
                "reversibility_class": "none",
                "high_impact": bool(high_impact),
                "requires_branch": bool(high_impact or is_mutation),
                "branch_reason": "external_non_reversible_mutation" if (high_impact or is_mutation) else "",
                "pre_state_capturable": False,
            }

        return {
            "action": normalized_action,
            "rollback_supported": False,
            "reversible": False,
            "reversibility_class": "unknown",
            "high_impact": False,
            "requires_branch": False,
            "branch_reason": "unsupported_action",
            "pre_state_capturable": False,
        }

    def record_success(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        result: ActionResult,
        source: str,
        goal_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        pre_state: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if str(result.status or "").strip().lower() != "success":
            return None
        operations = self._build_operations(
            action=action,
            args=args if isinstance(args, dict) else {},
            pre_state=pre_state if isinstance(pre_state, dict) else {},
            result=result,
        )
        if not operations:
            return None

        now_iso = _iso(_utc_now())
        entry = RollbackEntry(
            rollback_id=str(uuid.uuid4()),
            action=str(action or "").strip(),
            source=str(source or "").strip() or "desktop-ui",
            goal_id=str(goal_id or "").strip(),
            args=self._sanitize_args(args if isinstance(args, dict) else {}),
            operations=operations,
            metadata={
                **dict(metadata or {}),
                "rollback_supported": True,
                "reversible": True,
                "reversibility_class": "full",
                "operation_count": len(operations),
            },
            status="ready",
            created_at=now_iso,
            updated_at=now_iso,
            execution_log=[],
        )
        with self._lock:
            self._entries[entry.rollback_id] = entry
            self._trim_locked()
            self._save_locked()
            return entry.to_dict()

    def list_entries(self, *, status: str = "", goal_id: str = "", limit: int = 200) -> Dict[str, Any]:
        normalized_status = str(status or "").strip().lower()
        normalized_goal = str(goal_id or "").strip()
        bounded = max(1, min(int(limit), 2000))
        with self._lock:
            rows = list(self._entries.values())
        if normalized_status:
            rows = [item for item in rows if item.status == normalized_status]
        if normalized_goal:
            rows = [item for item in rows if item.goal_id == normalized_goal]
        rows.sort(key=lambda item: item.created_at, reverse=True)
        items = [row.to_dict() for row in rows[:bounded]]
        return {"status": "success", "items": items, "count": len(items), "total": len(rows)}

    def get_entry(self, rollback_id: str) -> Optional[Dict[str, Any]]:
        clean = str(rollback_id or "").strip()
        with self._lock:
            row = self._entries.get(clean)
            return row.to_dict() if row else None

    def rollback_entry(self, rollback_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
        clean_id = str(rollback_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "rollback_id is required"}

        with self._lock:
            entry = self._entries.get(clean_id)
            if entry is None:
                return {"status": "error", "message": "rollback entry not found"}
            operations = list(entry.operations)
            if not operations:
                return {"status": "error", "message": "rollback entry has no operations"}
            run_log = self._execute_operations(operations=operations, dry_run=dry_run)
            success = all(item.get("status") == "success" for item in run_log)

            now_iso = _iso(_utc_now())
            entry.updated_at = now_iso
            entry.executed_at = now_iso
            entry.execution_log = run_log
            if success:
                entry.status = "rolled_back" if not dry_run else "dry_run_ok"
                entry.error = ""
            else:
                entry.status = "rollback_failed" if not dry_run else "dry_run_failed"
                errors = [str(item.get("message", "")) for item in run_log if item.get("status") != "success"]
                entry.error = "; ".join([item for item in errors if item][:6])
            self._save_locked()
            return {
                "status": "success" if success else "error",
                "rollback": entry.to_dict(),
                "execution_log": run_log,
                "dry_run": bool(dry_run),
            }

    def rollback_goal(self, goal_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
        clean_goal = str(goal_id or "").strip()
        if not clean_goal:
            return {"status": "error", "message": "goal_id is required"}

        with self._lock:
            rows = [item for item in self._entries.values() if item.goal_id == clean_goal and item.status in {"ready", "rollback_failed"}]
            rows.sort(key=lambda item: item.created_at, reverse=True)

        if not rows:
            return {"status": "error", "message": "no rollback entries for this goal"}

        results: List[Dict[str, Any]] = []
        failures = 0
        for row in rows:
            result = self.rollback_entry(row.rollback_id, dry_run=dry_run)
            results.append(result)
            if result.get("status") != "success":
                failures += 1

        return {
            "status": "success" if failures == 0 else "error",
            "goal_id": clean_goal,
            "rolled_back": len(rows) - failures,
            "failed": failures,
            "results": results,
            "dry_run": bool(dry_run),
        }

    def _build_operations(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        pre_state: Dict[str, Any],
        result: ActionResult,
    ) -> List[Dict[str, Any]]:
        normalized_action = str(action or "").strip().lower()
        if normalized_action == "write_file":
            return self._ops_for_write_file(args=args, pre_state=pre_state)
        if normalized_action == "copy_file":
            return self._ops_for_copy_file(args=args, pre_state=pre_state)
        if normalized_action == "clipboard_write":
            return self._ops_for_clipboard_write(pre_state=pre_state)
        if normalized_action == "create_folder":
            return self._ops_for_create_folder(args=args, pre_state=pre_state)
        return []

    def _ops_for_write_file(self, *, args: Dict[str, Any], pre_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        target = self._normalize_path(args.get("path"))
        if target is None:
            return []
        existed_before = bool(pre_state.get("file_existed", False))
        if existed_before:
            return [
                {
                    "type": "restore_text_file",
                    "path": str(target),
                    "content": str(pre_state.get("previous_content", "")),
                }
            ]
        return [{"type": "delete_file_if_exists", "path": str(target)}]

    def _ops_for_copy_file(self, *, args: Dict[str, Any], pre_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        destination = self._normalize_path(args.get("destination"))
        if destination is None:
            return []
        existed_before = bool(pre_state.get("destination_existed", False))
        if existed_before:
            backup_path = self._normalize_path(pre_state.get("destination_backup_path"))
            if backup_path is not None and backup_path.exists():
                return [
                    {
                        "type": "restore_file_from_backup",
                        "destination": str(destination),
                        "backup_path": str(backup_path),
                    }
                ]
            return []
        return [{"type": "delete_file_if_exists", "path": str(destination)}]

    @staticmethod
    def _ops_for_clipboard_write(*, pre_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "clipboard_text" not in pre_state:
            return []
        return [{"type": "clipboard_write", "text": str(pre_state.get("clipboard_text", ""))}]

    def _ops_for_create_folder(self, *, args: Dict[str, Any], pre_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        path = self._normalize_path(args.get("path"))
        if path is None:
            return []
        if bool(pre_state.get("path_existed", False)):
            return []
        return [{"type": "remove_dir_if_empty", "path": str(path)}]

    def _capture_write_file_state(self, args: Dict[str, Any]) -> Dict[str, Any]:
        target = self._normalize_path(args.get("path"))
        if target is None:
            return {}
        if not target.exists():
            return {"file_existed": False}
        if not target.is_file():
            return {"file_existed": False}
        content = ""
        try:
            content = target.read_text(encoding="utf-8")
        except Exception:
            content = ""
        if len(content) > 1_500_000:
            content = content[:1_500_000]
        return {"file_existed": True, "previous_content": content}

    def _capture_copy_file_state(self, args: Dict[str, Any]) -> Dict[str, Any]:
        destination = self._normalize_path(args.get("destination"))
        if destination is None:
            return {}
        if not destination.exists() or not destination.is_file():
            return {"destination_existed": False}
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        backup_name = f"{destination.name}.{uuid.uuid4().hex[:10]}.bak"
        backup_path = self.backup_dir / backup_name
        try:
            shutil.copy2(destination, backup_path)
        except Exception:
            return {"destination_existed": True}
        return {"destination_existed": True, "destination_backup_path": str(backup_path)}

    @staticmethod
    def _capture_clipboard_state() -> Dict[str, Any]:
        try:
            import pyperclip  # type: ignore

            return {"clipboard_text": str(pyperclip.paste())}
        except Exception:
            return {}

    def _capture_create_folder_state(self, args: Dict[str, Any]) -> Dict[str, Any]:
        target = self._normalize_path(args.get("path"))
        if target is None:
            return {}
        return {"path_existed": target.exists()}

    @staticmethod
    def _sanitize_args(args: Dict[str, Any]) -> Dict[str, Any]:
        redacted_keywords = ("token", "password", "secret", "api_key", "authorization")
        out: Dict[str, Any] = {}
        for key, value in args.items():
            clean_key = str(key or "").strip()
            if not clean_key:
                continue
            lowered = clean_key.lower()
            if any(marker in lowered for marker in redacted_keywords):
                out[clean_key] = "***redacted***"
                continue
            if isinstance(value, str) and len(value) > 2000:
                out[clean_key] = f"{value[:2000]}...(truncated)"
            else:
                out[clean_key] = value
        return out

    def _execute_operations(self, *, operations: List[Dict[str, Any]], dry_run: bool) -> List[Dict[str, Any]]:
        run_log: List[Dict[str, Any]] = []
        for operation in reversed(operations):
            op_type = str(operation.get("type", "")).strip()
            if not op_type:
                run_log.append({"status": "error", "type": "", "message": "operation type is missing"})
                continue
            if dry_run:
                run_log.append({"status": "success", "type": op_type, "message": "dry-run"})
                continue
            try:
                result = self._execute_operation(operation)
            except Exception as exc:  # noqa: BLE001
                result = {"status": "error", "type": op_type, "message": str(exc)}
            run_log.append(result)
        return run_log

    def _execute_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        op_type = str(operation.get("type", "")).strip()
        if op_type == "restore_text_file":
            target = self._normalize_path(operation.get("path"))
            if target is None:
                return {"status": "error", "type": op_type, "message": "path is required"}
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(str(operation.get("content", "")), encoding="utf-8")
            return {"status": "success", "type": op_type, "path": str(target)}

        if op_type == "delete_file_if_exists":
            target = self._normalize_path(operation.get("path"))
            if target is None:
                return {"status": "error", "type": op_type, "message": "path is required"}
            if target.exists() and target.is_file():
                target.unlink()
            return {"status": "success", "type": op_type, "path": str(target)}

        if op_type == "restore_file_from_backup":
            destination = self._normalize_path(operation.get("destination"))
            backup = self._normalize_path(operation.get("backup_path"))
            if destination is None or backup is None:
                return {"status": "error", "type": op_type, "message": "destination and backup_path are required"}
            if not backup.exists() or not backup.is_file():
                return {"status": "error", "type": op_type, "message": "backup file not found"}
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup, destination)
            return {"status": "success", "type": op_type, "destination": str(destination), "backup_path": str(backup)}

        if op_type == "remove_dir_if_empty":
            target = self._normalize_path(operation.get("path"))
            if target is None:
                return {"status": "error", "type": op_type, "message": "path is required"}
            if target.exists() and target.is_dir():
                try:
                    target.rmdir()
                except OSError:
                    return {"status": "error", "type": op_type, "message": "directory is not empty"}
            return {"status": "success", "type": op_type, "path": str(target)}

        if op_type == "clipboard_write":
            try:
                import pyperclip  # type: ignore

                pyperclip.copy(str(operation.get("text", "")))
                return {"status": "success", "type": op_type}
            except Exception as exc:  # noqa: BLE001
                return {"status": "error", "type": op_type, "message": str(exc)}

        return {"status": "error", "type": op_type, "message": f"unsupported operation type '{op_type}'"}

    @staticmethod
    def _normalize_path(raw_path: Any) -> Optional[Path]:
        text = str(raw_path or "").strip()
        if not text:
            return None
        try:
            return Path(text).expanduser().resolve()
        except Exception:  # noqa: BLE001
            return None

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
                row = self._coerce_entry(raw)
                if row is None:
                    continue
                self._entries[row.rollback_id] = row
            self._trim_locked()

    def _coerce_entry(self, raw: Any) -> Optional[RollbackEntry]:
        if not isinstance(raw, dict):
            return None
        rollback_id = str(raw.get("rollback_id", "")).strip()
        action = str(raw.get("action", "")).strip()
        source = str(raw.get("source", "desktop-ui")).strip() or "desktop-ui"
        goal_id = str(raw.get("goal_id", "")).strip()
        operations = raw.get("operations", [])
        if not rollback_id or not action or not isinstance(operations, list):
            return None
        args = raw.get("args", {})
        metadata = raw.get("metadata", {})
        execution_log = raw.get("execution_log", [])
        return RollbackEntry(
            rollback_id=rollback_id,
            action=action,
            source=source,
            goal_id=goal_id,
            args=args if isinstance(args, dict) else {},
            operations=[item for item in operations if isinstance(item, dict)],
            metadata=metadata if isinstance(metadata, dict) else {},
            status=str(raw.get("status", "ready")).strip().lower() or "ready",
            created_at=str(raw.get("created_at", _iso(_utc_now()))),
            updated_at=str(raw.get("updated_at", _iso(_utc_now()))),
            executed_at=str(raw.get("executed_at", "")),
            error=str(raw.get("error", "")),
            execution_log=[item for item in execution_log if isinstance(item, dict)],
        )

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.to_dict() for item in self._entries.values()]
        rows.sort(key=lambda row: str(row.get("created_at", "")), reverse=True)
        self.store_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

    def _trim_locked(self) -> None:
        if len(self._entries) <= self.max_entries:
            return
        rows = sorted(self._entries.values(), key=lambda item: item.created_at)
        overflow = len(rows) - self.max_entries
        for item in rows[:overflow]:
            self._entries.pop(item.rollback_id, None)
