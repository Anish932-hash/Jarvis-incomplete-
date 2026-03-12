from __future__ import annotations

import base64
import hashlib
import os
import shutil
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.database.local_store import LocalStore
from backend.python.inference.model_setup_integrity import build_remote_item_map, verify_installed_artifact


ProgressCallback = Callable[[Dict[str, Any]], None]


class ModelSetupCancelled(RuntimeError):
    pass


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _path_size(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        if path.is_file():
            return int(path.stat().st_size)
        return int(sum(item.stat().st_size for item in path.rglob("*") if item.is_file()))
    except Exception:
        return 0


def select_install_items(plan_payload: Dict[str, Any], item_keys: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    items = [
        dict(item)
        for item in plan_payload.get("items", [])
        if isinstance(item, dict)
    ]
    requested_keys = {
        str(item).strip().lower()
        for item in (item_keys or [])
        if str(item).strip()
    }
    selected: List[Dict[str, Any]] = []
    for item in items:
        clean_key = str(item.get("key", "") or "").strip().lower()
        if requested_keys:
            if clean_key in requested_keys:
                selected.append(item)
            continue
        if bool(item.get("automation_ready", False)):
            selected.append(item)
    return selected


class ModelSetupInstaller:
    def __init__(
        self,
        *,
        history_path: str = "data/model_setup_install_history.json",
        provider_credentials: Optional[ProviderCredentialManager] = None,
    ) -> None:
        self._history_path = self._resolve_path(history_path)
        self._store = LocalStore(self._history_path)
        self._provider_credentials = provider_credentials

    def history(self, *, limit: int = 20) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 200))
        runs = self._runs()
        return {
            "status": "success",
            "count": min(len(runs), bounded),
            "total": len(runs),
            "items": runs[:bounded],
            "history_path": self._history_path,
        }

    def install(
        self,
        *,
        plan_payload: Dict[str, Any],
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        run_id: str = "",
        progress_callback: Optional[ProgressCallback] = None,
        cancel_event: Optional[threading.Event] = None,
        remote_metadata: Optional[Dict[str, Any]] = None,
        verify_integrity: bool = True,
    ) -> Dict[str, Any]:
        items = select_install_items(plan_payload, item_keys=item_keys)
        remote_item_map = build_remote_item_map(remote_metadata)
        requested_keys = {
            str(item).strip().lower()
            for item in (item_keys or [])
            if str(item).strip()
        }
        run_started = time.time()
        clean_run_id = str(run_id or "").strip() or uuid.uuid4().hex
        payload: Dict[str, Any] = {
            "status": "success",
            "run_id": clean_run_id,
            "dry_run": bool(dry_run),
            "force": bool(force),
            "selected_count": len(items),
            "success_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "blocked_count": 0,
            "cancelled_count": 0,
            "verified_count": 0,
            "observed_count": 0,
            "verification_error_count": 0,
            "integrity_status_counts": {},
            "requested_item_keys": sorted(requested_keys),
            "started_at": _iso_now(),
            "completed_at": "",
            "duration_s": 0.0,
            "items": [],
            "history_path": self._history_path,
            "manifest_path": str((plan_payload.get("manifest", {}) or {}).get("path", "") or ""),
        }
        if callable(progress_callback):
            try:
                progress_callback(
                    {
                        "event": "run_started",
                        "run_id": clean_run_id,
                        "selected_count": len(items),
                        "dry_run": bool(dry_run),
                        "force": bool(force),
                    }
                )
            except Exception:
                pass
        if not items:
            payload["status"] = "error"
            payload["message"] = "no setup items selected"
            payload["completed_at"] = _iso_now()
            payload["duration_s"] = round(time.time() - run_started, 6)
            self._append_run(payload)
            return payload

        for index, item in enumerate(items, start=1):
            if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                result = {
                    "key": str(item.get("key", "") or ""),
                    "name": str(item.get("name", "") or Path(str(item.get("path", "") or "")).name or "model"),
                    "task": str(item.get("task", "") or "unknown"),
                    "path": str(item.get("path", "") or ""),
                    "strategy": str(item.get("strategy", "manual") or "manual").strip().lower(),
                    "status": "cancelled",
                    "message": "cancelled",
                    "bytes_written": 0,
                    "started_at": _iso_now(),
                    "completed_at": _iso_now(),
                    "duration_s": 0.0,
                }
            else:
                result = self._install_item(
                    item=item,
                    dry_run=bool(dry_run),
                    force=bool(force),
                    index=index,
                    total_items=len(items),
                    progress_callback=progress_callback,
                    cancel_event=cancel_event,
                    remote_item=remote_item_map.get(str(item.get("key", "") or "").strip().lower(), {}),
                    verify_integrity=bool(verify_integrity),
                )
            payload["items"].append(result)
            status_name = str(result.get("status", "error") or "error").strip().lower()
            if status_name in {"success", "planned"}:
                payload["success_count"] = int(payload.get("success_count", 0) or 0) + 1
            elif status_name == "skipped":
                payload["skipped_count"] = int(payload.get("skipped_count", 0) or 0) + 1
            elif status_name == "blocked":
                payload["blocked_count"] = int(payload.get("blocked_count", 0) or 0) + 1
            elif status_name == "cancelled":
                payload["cancelled_count"] = int(payload.get("cancelled_count", 0) or 0) + 1
            else:
                payload["error_count"] = int(payload.get("error_count", 0) or 0) + 1
            integrity_status = str(result.get("integrity_status", "") or "").strip().lower()
            integrity_counts = payload.get("integrity_status_counts") if isinstance(payload.get("integrity_status_counts"), dict) else {}
            if integrity_status:
                integrity_counts[integrity_status] = int(integrity_counts.get(integrity_status, 0) or 0) + 1
            payload["integrity_status_counts"] = integrity_counts
            if bool(result.get("verified", False)):
                payload["verified_count"] = int(payload.get("verified_count", 0) or 0) + 1
            elif integrity_status == "observed":
                payload["observed_count"] = int(payload.get("observed_count", 0) or 0) + 1
            if bool(result.get("verification_failed", False)):
                payload["verification_error_count"] = int(payload.get("verification_error_count", 0) or 0) + 1
            if callable(progress_callback):
                try:
                    progress_callback(
                        {
                            "event": "item_completed",
                            "run_id": clean_run_id,
                            "index": index,
                            "total_items": len(items),
                            "item": dict(result),
                        }
                    )
                except Exception:
                    pass
            if status_name == "cancelled":
                break

        if int(payload.get("error_count", 0) or 0) > 0:
            payload["status"] = "partial" if int(payload.get("success_count", 0) or 0) > 0 else "error"
        elif int(payload.get("cancelled_count", 0) or 0) > 0:
            payload["status"] = "partial" if int(payload.get("success_count", 0) or 0) > 0 else "cancelled"
        elif int(payload.get("blocked_count", 0) or 0) > 0:
            payload["status"] = "partial"
        payload["completed_at"] = _iso_now()
        payload["duration_s"] = round(time.time() - run_started, 6)
        if callable(progress_callback):
            try:
                progress_callback({"event": "run_completed", "run_id": clean_run_id, "payload": dict(payload)})
            except Exception:
                pass
        self._append_run(payload)
        return payload

    def _install_item(
        self,
        *,
        item: Dict[str, Any],
        dry_run: bool,
        force: bool,
        index: int,
        total_items: int,
        progress_callback: Optional[ProgressCallback] = None,
        cancel_event: Optional[threading.Event] = None,
        remote_item: Optional[Dict[str, Any]] = None,
        verify_integrity: bool = True,
    ) -> Dict[str, Any]:
        target_path = Path(str(item.get("path", "") or ""))
        strategy = str(item.get("strategy", "manual") or "manual").strip().lower()
        started = time.time()
        payload = {
            "key": str(item.get("key", "") or ""),
            "name": str(item.get("name", "") or target_path.name or "model"),
            "task": str(item.get("task", "") or "unknown"),
            "path": str(target_path),
            "strategy": strategy,
            "status": "error",
            "message": "",
            "bytes_written": 0,
            "started_at": _iso_now(),
            "completed_at": "",
            "duration_s": 0.0,
            "verified": False,
            "integrity_status": "",
            "verification_failed": False,
            "verification": {},
        }
        if callable(progress_callback):
            try:
                progress_callback(
                    {
                        "event": "item_started",
                        "index": index,
                        "total_items": total_items,
                        "item": dict(payload),
                    }
                )
            except Exception:
                pass
        if not bool(item.get("automation_ready", False)):
            payload["status"] = "blocked"
            payload["message"] = "item is not automation-ready"
            payload["completed_at"] = _iso_now()
            payload["duration_s"] = round(time.time() - started, 6)
            return payload
        if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
            payload["status"] = "cancelled"
            payload["message"] = "cancelled"
            payload["completed_at"] = _iso_now()
            payload["duration_s"] = round(time.time() - started, 6)
            return payload
        if target_path.exists() and not force:
            payload["status"] = "skipped"
            payload["message"] = "target already exists"
            payload["bytes_written"] = _path_size(target_path)
            payload["completed_at"] = _iso_now()
            payload["duration_s"] = round(time.time() - started, 6)
            return payload
        if dry_run:
            payload["status"] = "planned"
            payload["message"] = "dry-run only"
            payload["completed_at"] = _iso_now()
            payload["duration_s"] = round(time.time() - started, 6)
            return payload

        install_metadata: Dict[str, Any] = {}
        try:
            if strategy == "huggingface_snapshot":
                source_ref = str(item.get("source_ref", "") or "").strip()
                if not source_ref:
                    raise ValueError("source_ref is required")
                install_metadata = self._install_huggingface_snapshot(
                    repo_id=source_ref,
                    target_path=target_path,
                    cancel_event=cancel_event,
                    token=self._huggingface_token(),
                )
            elif strategy == "direct_url":
                source_url = str(item.get("source_url", "") or item.get("source_ref", "") or "").strip()
                if not source_url:
                    raise ValueError("source_url is required")
                install_metadata = self._install_direct_url(
                    url=source_url,
                    target_path=target_path,
                    cancel_event=cancel_event,
                )
            else:
                raise ValueError(f"unsupported install strategy: {strategy}")
            payload["status"] = "success"
            payload["message"] = "installed"
            payload["bytes_written"] = max(0, int(install_metadata.get("bytes_written", 0) or _path_size(target_path)))
            if bool(verify_integrity):
                try:
                    verification = verify_installed_artifact(
                        target_path=target_path,
                        item=item,
                        remote_item=remote_item if isinstance(remote_item, dict) else {},
                        install_metadata=install_metadata,
                    )
                except Exception as exc:  # noqa: BLE001
                    verification = {
                        "status": "error",
                        "verified": False,
                        "errors": [str(exc)],
                        "warnings": [],
                    }
                payload["verification"] = verification
                payload["verified"] = bool(verification.get("verified", False))
                payload["integrity_status"] = str(verification.get("status", "") or "").strip().lower()
                if payload["integrity_status"] in {"mismatch", "error"}:
                    _remove_artifact(target_path)
                    payload["status"] = "error"
                    payload["message"] = (
                        f"integrity verification failed: {', '.join(verification.get('errors', []))}"
                        if isinstance(verification.get("errors", []), list) and verification.get("errors", [])
                        else "integrity verification failed"
                    )
                    payload["verification_failed"] = True
                    payload["bytes_written"] = 0
                elif payload["verified"]:
                    payload["message"] = "installed and verified"
        except ModelSetupCancelled as exc:
            payload["status"] = "cancelled"
            payload["message"] = str(exc) or "cancelled"
        except Exception as exc:  # noqa: BLE001
            payload["status"] = "error"
            payload["message"] = str(exc)
            if payload["integrity_status"]:
                payload["verification_failed"] = True

        payload["completed_at"] = _iso_now()
        payload["duration_s"] = round(time.time() - started, 6)
        return payload

    @staticmethod
    def _install_huggingface_snapshot(
        *,
        repo_id: str,
        target_path: Path,
        cancel_event: Optional[threading.Event] = None,
        token: str = "",
    ) -> Dict[str, Any]:
        if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
            raise ModelSetupCancelled("cancelled before snapshot download")
        try:
            from huggingface_hub import snapshot_download
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("huggingface_hub is required for snapshot installs") from exc
        target_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_download(
            repo_id=repo_id,
            local_dir=str(target_path),
            local_dir_use_symlinks=False,
            resume_download=True,
            token=(token or None),
        )
        if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
            raise ModelSetupCancelled("cancelled after snapshot download")
        return {
            "repo_id": repo_id,
            "bytes_written": _path_size(target_path),
            "auth_used": bool(token),
        }

    @staticmethod
    def _install_direct_url(
        *,
        url: str,
        target_path: Path,
        cancel_event: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix(target_path.suffix + ".part")
        request = urllib.request.Request(url, headers={"User-Agent": "JARVIS-SetupInstaller/1.0"})
        sha256 = hashlib.sha256()
        md5 = hashlib.md5()
        bytes_written = 0
        try:
            with urllib.request.urlopen(request, timeout=300.0) as response, tmp_path.open("wb") as handle:
                final_url = str(response.geturl() or url)
                final_host = urlparse(final_url).netloc.lower()
                etag = str(response.headers.get("ETag", "") or "")
                content_length = _response_content_length(response)
                while True:
                    if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                        raise ModelSetupCancelled("cancelled during direct download")
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    bytes_written += len(chunk)
                    sha256.update(chunk)
                    md5.update(chunk)
            if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                raise ModelSetupCancelled("cancelled before file finalize")
            os.replace(tmp_path, target_path)
            md5_bytes = md5.digest()
            return {
                "bytes_written": bytes_written,
                "sha256_hex": sha256.hexdigest(),
                "md5_hex": md5.hexdigest(),
                "md5_base64": base64.b64encode(md5_bytes).decode("ascii"),
                "final_url": final_url,
                "final_host": final_host,
                "etag": etag,
                "content_length": content_length,
            }
        except Exception:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            raise

    def _runs(self) -> List[Dict[str, Any]]:
        rows = self._store.get("runs", [])
        if not isinstance(rows, list):
            return []
        return [dict(item) for item in rows if isinstance(item, dict)]

    def _append_run(self, payload: Dict[str, Any]) -> None:
        rows = self._runs()
        rows.insert(0, dict(payload))
        self._store.set("runs", rows[:60])

    @staticmethod
    def _resolve_path(raw_path: str) -> str:
        clean = str(raw_path or "").strip()
        if not clean:
            return str(Path.cwd())
        candidate = Path(clean)
        if candidate.is_absolute():
            return str(candidate)
        cwd = Path.cwd().resolve()
        for option in (cwd / clean, cwd.parent / clean, cwd.parent.parent / clean):
            if option.exists():
                return str(option)
        return str(cwd / clean)

    def _huggingface_token(self) -> str:
        manager = self._provider_credentials
        if manager is not None:
            try:
                token = str(manager.get_api_key("huggingface") or "").strip()
                if token:
                    return token
            except Exception:
                pass
        for env_name in ("HUGGINGFACE_HUB_TOKEN", "HF_TOKEN", "HUGGINGFACE_TOKEN"):
            token = str(os.getenv(env_name, "") or "").strip()
            if token:
                return token
        return ""


def _response_content_length(response: Any) -> int:
    try:
        content_length = str(response.headers.get("Content-Length", "") or "").strip()
        if content_length:
            return max(0, int(content_length))
    except Exception:
        return 0
    return 0


def _remove_artifact(target_path: Path) -> None:
    try:
        if target_path.is_dir():
            shutil.rmtree(target_path, ignore_errors=True)
        elif target_path.exists():
            target_path.unlink()
    except Exception:
        pass
