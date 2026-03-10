from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .file_tools import FileTools
from .system_tools import SystemTools
from .time_tools import TimeTools

class AutomationTools:
    """
    SAFE + EXTENDED SAFE automation engine.
    Supports workflow automation, file tasks, app workflows (whitelist),
    scheduled operations, resource triggers, and script execution from trusted directories.
    """

    WHITELISTED_APPS = {
        "notepad": r"C:\Windows\notepad.exe",
        "calc": r"C:\Windows\System32\calc.exe",
    }

    TRUSTED_SCRIPT_DIR = Path("trusted_scripts")
    TRUSTED_SCRIPT_MANIFEST = "manifest.json"
    ALLOWED_SCRIPT_SUFFIXES = {".py", ".bat", ".cmd", ".ps1"}

    @staticmethod
    def run_whitelisted_app(app_name: str):
        clean_name = str(app_name or "").strip().lower()
        if clean_name not in AutomationTools.WHITELISTED_APPS:
            raise PermissionError(f"App not whitelisted: {clean_name or app_name}")

        app_path = Path(AutomationTools.WHITELISTED_APPS[clean_name]).expanduser().resolve()
        if not app_path.exists() or not app_path.is_file():
            raise FileNotFoundError(f"Whitelisted app path not found: {app_path}")

        return subprocess.Popen([str(app_path)], shell=False, close_fds=False)

    @staticmethod
    def run_trusted_script(
        script_name: str,
        *,
        args: Optional[List[str]] = None,
        env_overrides: Optional[Dict[str, str]] = None,
    ):
        """
        Execute a script from trusted_scripts with strict containment and optional integrity checks.

        Security controls:
        - Canonical path containment inside TRUSTED_SCRIPT_DIR
        - Extension allow-list
        - Optional manifest-driven hash verification
        - No shell invocation
        """
        trusted_root = AutomationTools.TRUSTED_SCRIPT_DIR.expanduser().resolve()
        trusted_root.mkdir(parents=True, exist_ok=True)

        script_path = AutomationTools._resolve_trusted_script_path(script_name=script_name, trusted_root=trusted_root)
        suffix = script_path.suffix.lower()
        if suffix not in AutomationTools.ALLOWED_SCRIPT_SUFFIXES:
            allowed = ", ".join(sorted(AutomationTools.ALLOWED_SCRIPT_SUFFIXES))
            raise PermissionError(f"Only trusted script types are allowed: {allowed}")

        manifest = AutomationTools._load_trusted_script_manifest(trusted_root=trusted_root)
        script_meta = AutomationTools._resolve_manifest_entry(
            manifest=manifest,
            script_name=str(script_name or "").strip(),
            script_path=script_path,
        )
        AutomationTools._verify_script_integrity(script_path=script_path, script_meta=script_meta, manifest=manifest)

        command = AutomationTools._build_script_command(
            script_path=script_path,
            suffix=suffix,
            script_meta=script_meta,
            args=args or [],
        )
        env = AutomationTools._build_child_env(env_overrides=env_overrides)
        return subprocess.Popen(
            command,
            cwd=str(trusted_root),
            shell=False,
            close_fds=False,
            env=env,
        )

    @staticmethod
    def _resolve_trusted_script_path(*, script_name: str, trusted_root: Path) -> Path:
        clean_name = str(script_name or "").strip()
        if not clean_name:
            raise ValueError("script_name is required")

        candidate = (trusted_root / clean_name).resolve()
        if not candidate.exists() or not candidate.is_file():
            raise FileNotFoundError(candidate)
        if candidate.is_symlink():
            raise PermissionError("Symlink scripts are not allowed in trusted execution path.")

        try:
            candidate.relative_to(trusted_root)
        except Exception as exc:
            raise PermissionError("script path escapes trusted_scripts root") from exc
        return candidate

    @staticmethod
    def _load_trusted_script_manifest(*, trusted_root: Path) -> Dict[str, Any]:
        manifest_path = trusted_root / AutomationTools.TRUSTED_SCRIPT_MANIFEST
        if not manifest_path.exists():
            return {}
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Invalid trusted script manifest JSON: {exc}") from exc
        if isinstance(payload, dict):
            return payload
        raise RuntimeError("trusted_scripts manifest must be a JSON object.")

    @staticmethod
    def _resolve_manifest_entry(*, manifest: Dict[str, Any], script_name: str, script_path: Path) -> Dict[str, Any]:
        scripts = manifest.get("scripts")
        if isinstance(scripts, dict):
            row = scripts.get(script_name) or scripts.get(script_path.name)
            return row if isinstance(row, dict) else {}
        if isinstance(scripts, list):
            target_names = {script_name, script_path.name, str(script_path).replace("\\", "/")}
            for item in scripts:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                rel_path = str(item.get("path", "")).strip()
                if name in target_names or rel_path in target_names:
                    return item
        return {}

    @staticmethod
    def _verify_script_integrity(*, script_path: Path, script_meta: Dict[str, Any], manifest: Dict[str, Any]) -> None:
        require_manifest = str(os.getenv("JARVIS_TRUSTED_SCRIPT_REQUIRE_MANIFEST", "0")).strip().lower() in {"1", "true", "yes", "on"}
        enforce_hash = bool(manifest.get("enforce_hash", False))

        if require_manifest and not script_meta:
            raise PermissionError(
                "Trusted script manifest entry is required but missing. "
                "Set JARVIS_TRUSTED_SCRIPT_REQUIRE_MANIFEST=0 to disable enforcement."
            )

        expected_sha256 = str(script_meta.get("sha256", "")).strip().lower()
        if enforce_hash and not expected_sha256:
            raise PermissionError("Manifest enforces hash checks but script sha256 is missing.")
        if not expected_sha256:
            return

        actual_sha256 = AutomationTools._sha256_file(script_path)
        if actual_sha256 != expected_sha256:
            raise PermissionError(
                f"Trusted script integrity mismatch for {script_path.name}: "
                f"expected sha256={expected_sha256}, got={actual_sha256}"
            )

    @staticmethod
    def _build_script_command(
        *,
        script_path: Path,
        suffix: str,
        script_meta: Dict[str, Any],
        args: List[str],
    ) -> List[str]:
        safe_args = [str(arg) for arg in args if str(arg).strip()]

        custom_command = script_meta.get("command")
        if isinstance(custom_command, list) and custom_command:
            resolved = [str(part) for part in custom_command if str(part).strip()]
            if not resolved:
                raise RuntimeError("Manifest command is empty.")
            return resolved + safe_args

        if suffix == ".py":
            return [sys.executable, str(script_path)] + safe_args
        if suffix in {".bat", ".cmd"}:
            return ["cmd.exe", "/d", "/c", str(script_path)] + safe_args
        if suffix == ".ps1":
            return [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
            ] + safe_args
        raise PermissionError(f"Unsupported script type: {suffix}")

    @staticmethod
    def _build_child_env(*, env_overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        env = dict(os.environ)
        env.setdefault("PYTHONUNBUFFERED", "1")
        env["JARVIS_TRUSTED_EXECUTION"] = "1"
        if isinstance(env_overrides, dict):
            for key, value in env_overrides.items():
                clean_key = str(key or "").strip()
                if not clean_key:
                    continue
                env[clean_key] = str(value)
        return env

    @staticmethod
    def _sha256_file(path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest().lower()

    @staticmethod
    async def workflow_executor(
        tasks: list,
        *,
        max_concurrency: int = 4,
        default_timeout_s: float = 120.0,
        continue_on_error: bool = False,
        global_timeout_s: float = 900.0,
    ):
        """
        Execute a DAG workflow with dependency ordering, retries, and timeouts.
        Task row schema:
        - name: str
        - action: callable
        - depends_on: str | list[str] (optional)
        - retries: int (optional)
        - timeout_s: float (optional)
        """
        if not isinstance(tasks, list) or not tasks:
            return {"status": "error", "message": "tasks must be a non-empty list", "results": {}, "errors": {}}

        normalized: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for index, raw in enumerate(tasks):
            if not isinstance(raw, dict):
                return {"status": "error", "message": f"task[{index}] must be a dict", "results": {}, "errors": {}}
            name = str(raw.get("name", "")).strip()
            action = raw.get("action")
            if not name:
                return {"status": "error", "message": f"task[{index}] missing name", "results": {}, "errors": {}}
            if name in normalized:
                return {"status": "error", "message": f"duplicate task name: {name}", "results": {}, "errors": {}}
            if not callable(action):
                return {"status": "error", "message": f"task[{index}] action is not callable", "results": {}, "errors": {}}

            raw_depends = raw.get("depends_on", [])
            if isinstance(raw_depends, str):
                depends_on = [raw_depends.strip()] if raw_depends.strip() else []
            elif isinstance(raw_depends, list):
                depends_on = [str(item).strip() for item in raw_depends if str(item).strip()]
            else:
                depends_on = []
            normalized[name] = {
                "action": action,
                "depends_on": depends_on,
                "retries": max(0, min(int(raw.get("retries", 0) or 0), 8)),
                "timeout_s": max(0.2, min(float(raw.get("timeout_s", default_timeout_s) or default_timeout_s), 3600.0)),
            }
            order.append(name)

        for name, row in normalized.items():
            for dependency in row["depends_on"]:
                if dependency not in normalized:
                    return {
                        "status": "error",
                        "message": f"task '{name}' depends on unknown task '{dependency}'",
                        "results": {},
                        "errors": {},
                    }

        start_monotonic = asyncio.get_running_loop().time()
        deadline = start_monotonic + max(5.0, min(float(global_timeout_s), 24.0 * 3600.0))
        semaphore = asyncio.Semaphore(max(1, min(int(max_concurrency), 32)))

        pending = set(order)
        running: Dict[asyncio.Task, str] = {}
        completed: Dict[str, Any] = {}
        failures: Dict[str, Dict[str, Any]] = {}
        skipped: Dict[str, str] = {}
        timeline: List[Dict[str, Any]] = []

        async def _run_task(name: str) -> Any:
            task_row = normalized[name]
            retries = int(task_row["retries"])
            timeout_s = float(task_row["timeout_s"])
            action = task_row["action"]
            is_async_fn = bool(inspect.iscoroutinefunction(action))
            last_error: Optional[Exception] = None
            for attempt in range(1, retries + 2):
                try:
                    async with semaphore:
                        if is_async_fn:
                            produced = action()
                            if inspect.isawaitable(produced):
                                return await asyncio.wait_for(produced, timeout=timeout_s)
                            return produced
                        return await asyncio.wait_for(asyncio.to_thread(action), timeout=timeout_s)
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if attempt <= retries:
                        await asyncio.sleep(min(0.3 * (2 ** (attempt - 1)), 2.0))
                        continue
            if last_error is None:
                raise RuntimeError("task execution failed without error")
            raise last_error

        def _mark_skipped_due_to_failure(failed_task_name: str) -> None:
            if continue_on_error:
                return
            changed = True
            while changed:
                changed = False
                for candidate in list(pending):
                    deps = normalized[candidate]["depends_on"]
                    if any(dep in failures or dep in skipped for dep in deps):
                        skipped[candidate] = f"dependency_failed:{failed_task_name}"
                        pending.discard(candidate)
                        timeline.append({"task": candidate, "status": "skipped", "reason": skipped[candidate]})
                        changed = True

        while pending or running:
            now = asyncio.get_running_loop().time()
            if now >= deadline:
                for task_obj, task_name in list(running.items()):
                    task_obj.cancel()
                    failures[task_name] = {"message": "global workflow timeout", "type": "TimeoutError"}
                break

            ready = [
                name
                for name in list(pending)
                if all(dep in completed for dep in normalized[name]["depends_on"])
            ]
            if ready:
                for name in sorted(ready):
                    pending.discard(name)
                    timeline.append({"task": name, "status": "started", "at_s": round(now - start_monotonic, 6)})
                    task_obj = asyncio.create_task(_run_task(name))
                    running[task_obj] = name

            if not running:
                if pending:
                    unresolved = sorted(pending)
                    for name in unresolved:
                        skipped[name] = "dependency_unresolved"
                        timeline.append({"task": name, "status": "skipped", "reason": "dependency_unresolved"})
                    pending.clear()
                break

            done, _ = await asyncio.wait(
                set(running.keys()),
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0,
            )
            if not done:
                continue

            for completed_task in done:
                task_name = running.pop(completed_task, "")
                if not task_name:
                    continue
                try:
                    completed[task_name] = completed_task.result()
                    timeline.append({"task": task_name, "status": "completed", "at_s": round(asyncio.get_running_loop().time() - start_monotonic, 6)})
                except Exception as exc:  # noqa: BLE001
                    failures[task_name] = {"message": str(exc), "type": type(exc).__name__}
                    timeline.append(
                        {
                            "task": task_name,
                            "status": "failed",
                            "error": str(exc),
                            "error_type": type(exc).__name__,
                            "at_s": round(asyncio.get_running_loop().time() - start_monotonic, 6),
                        }
                    )
                    _mark_skipped_due_to_failure(task_name)

        status = "success"
        if failures and completed:
            status = "partial"
        elif failures and not completed:
            status = "failed"

        return {
            "status": status,
            "results": completed,
            "errors": failures,
            "skipped": skipped,
            "timeline": timeline,
            "completed_count": len(completed),
            "failed_count": len(failures),
            "skipped_count": len(skipped),
            "duration_s": round(asyncio.get_running_loop().time() - start_monotonic, 6),
        }

    @staticmethod
    async def resource_trigger(cpu_limit: float, callback, check_interval=2):
        """Trigger callback when CPU usage exceeds threshold."""
        while True:
            usage = SystemTools.get_resource_usage()
            if usage["cpu_percent"] > cpu_limit:
                callback(usage)
            await asyncio.sleep(check_interval)

    @staticmethod
    async def scheduled_script(script_name: str, run_at: datetime):
        await TimeTools.schedule(
            lambda: AutomationTools.run_trusted_script(script_name),
            run_at
        )

    @staticmethod
    async def file_watchdog(path: str, on_change):
        """Monitors file changes."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(path)

        last_hash = FileTools.compute_hash(path)

        while True:
            await asyncio.sleep(1)
            new_hash = FileTools.compute_hash(path)
            if new_hash != last_hash:
                last_hash = new_hash
                on_change(path)
