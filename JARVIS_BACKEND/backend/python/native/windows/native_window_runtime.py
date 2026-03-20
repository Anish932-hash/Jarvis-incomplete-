from __future__ import annotations

import os
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any, Dict


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


class NativeWindowRuntime:
    """
    Optional C++ + Cython fast path for low-latency Windows window inspection.

    The runtime is intentionally non-fatal: if the extension is missing or fails to
    load, the rest of the desktop stack can fall back to the Python / Rust path.
    """

    def __init__(
        self,
        *,
        extension_module: str = "backend.python.native.windows._window_bridge",
        module: Any | None = None,
    ) -> None:
        self._extension_module = extension_module
        self._module = module
        self._load_attempted = module is not None
        self._load_error = ""
        self._disabled = _env_enabled("JARVIS_NATIVE_WINDOWS_DISABLED", False)
        self._project_root = Path(__file__).resolve().parents[4]
        self._build_hint = str(self._project_root / "scripts" / "build_native_windows_bridge.ps1")

    def _load_module(self) -> Any | None:
        if self._module is not None:
            return self._module
        if self._load_attempted:
            return None
        self._load_attempted = True

        if self._disabled:
            self._load_error = "Native Windows runtime is disabled via JARVIS_NATIVE_WINDOWS_DISABLED."
            return None
        if os.name != "nt":
            self._load_error = "Native Windows runtime is only available on Windows."
            return None

        try:
            self._module = import_module(self._extension_module)
        except Exception as exc:  # noqa: BLE001
            self._load_error = str(exc)
            self._module = None
        return self._module

    def availability(self) -> Dict[str, Any]:
        module = self._load_module()
        return {
            "status": "success",
            "available": module is not None,
            "disabled": self._disabled,
            "module": self._extension_module,
            "backend": "cpp_cython",
            "build_hint": self._build_hint,
            "message": "" if module is not None else (self._load_error or "Native extension not loaded."),
        }

    def _unavailable_payload(self, *, action: str) -> Dict[str, Any]:
        availability = self.availability()
        return {
            "status": "error",
            "action": action,
            "backend": "cpp_cython",
            "message": availability.get("message", "Native extension not loaded."),
            "build_hint": availability.get("build_hint", self._build_hint),
            "available": bool(availability.get("available")),
        }

    def _call(self, action: str, method_name: str, *args: Any) -> Dict[str, Any]:
        module = self._load_module()
        if module is None:
            return self._unavailable_payload(action=action)

        method = getattr(module, method_name, None)
        if method is None:
            return {
                "status": "error",
                "action": action,
                "backend": "cpp_cython",
                "message": f"Native extension is missing method '{method_name}'.",
                "build_hint": self._build_hint,
            }
        try:
            payload = method(*args)
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "action": action,
                "backend": "cpp_cython",
                "message": str(exc),
                "build_hint": self._build_hint,
            }
        if not isinstance(payload, dict):
            return {
                "status": "error",
                "action": action,
                "backend": "cpp_cython",
                "message": "Native extension returned a non-dict payload.",
                "build_hint": self._build_hint,
            }
        payload.setdefault("backend", "cpp_cython")
        payload.setdefault("build_hint", self._build_hint)
        return payload

    def list_windows(self, *, limit: int = 120) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        return self._call("list_windows", "list_windows", safe_limit)

    def active_window(self) -> Dict[str, Any]:
        return self._call("active_window", "active_window")

    def focus_window(self, *, title_contains: str = "", hwnd: int | None = None) -> Dict[str, Any]:
        title = str(title_contains or "")
        hwnd_value = 0 if hwnd is None else int(hwnd)
        return self._call("focus_window", "focus_window", title, hwnd_value)

    @staticmethod
    def _encode_title_sequence(value: Any) -> str:
        if isinstance(value, (list, tuple, set)):
            parts = [str(item).strip() for item in value if str(item).strip()]
            return "||".join(parts[:8])
        clean_value = str(value or "").strip()
        if not clean_value:
            return ""
        if "||" in clean_value:
            return "||".join(part.strip() for part in clean_value.split("||") if part.strip())
        return clean_value

    def focus_related_window(
        self,
        *,
        query: str = "",
        hint_query: str = "",
        descendant_hint_query: str = "",
        descendant_title_sequence: Any = None,
        campaign_hint_query: str = "",
        campaign_preferred_title: str = "",
        campaign_descendant_title_sequence: Any = None,
        portfolio_hint_query: str = "",
        portfolio_preferred_title: str = "",
        portfolio_descendant_title_sequence: Any = None,
        preferred_title: str = "",
        window_title: str = "",
        hwnd: int | None = None,
        pid: int | None = None,
        follow_descendant_chain: bool = False,
        max_descendant_focus_steps: int = 1,
        limit: int = 120,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        hwnd_value = 0 if hwnd is None else int(hwnd)
        pid_value = 0 if pid is None else int(pid)
        safe_max_descendant_focus_steps = max(1, min(int(max_descendant_focus_steps), 6))
        return self._call(
            "focus_related_window",
            "focus_related_window",
            str(query or ""),
            str(hint_query or ""),
            str(descendant_hint_query or ""),
            self._encode_title_sequence(descendant_title_sequence),
            str(campaign_hint_query or ""),
            str(campaign_preferred_title or ""),
            self._encode_title_sequence(campaign_descendant_title_sequence),
            str(portfolio_hint_query or ""),
            str(portfolio_preferred_title or ""),
            self._encode_title_sequence(portfolio_descendant_title_sequence),
            str(preferred_title or ""),
            str(window_title or ""),
            hwnd_value,
            pid_value,
            bool(follow_descendant_chain),
            safe_max_descendant_focus_steps,
            safe_limit,
        )

    def reacquire_related_window(
        self,
        *,
        query: str = "",
        hint_query: str = "",
        descendant_hint_query: str = "",
        descendant_title_sequence: Any = None,
        campaign_hint_query: str = "",
        campaign_preferred_title: str = "",
        campaign_descendant_title_sequence: Any = None,
        portfolio_hint_query: str = "",
        portfolio_preferred_title: str = "",
        portfolio_descendant_title_sequence: Any = None,
        preferred_title: str = "",
        window_title: str = "",
        hwnd: int | None = None,
        pid: int | None = None,
        limit: int = 120,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        hwnd_value = 0 if hwnd is None else int(hwnd)
        pid_value = 0 if pid is None else int(pid)
        return self._call(
            "reacquire_related_window",
            "reacquire_related_window",
            str(query or ""),
            str(hint_query or ""),
            str(descendant_hint_query or ""),
            self._encode_title_sequence(descendant_title_sequence),
            str(campaign_hint_query or ""),
            str(campaign_preferred_title or ""),
            self._encode_title_sequence(campaign_descendant_title_sequence),
            str(portfolio_hint_query or ""),
            str(portfolio_preferred_title or ""),
            self._encode_title_sequence(portfolio_descendant_title_sequence),
            str(preferred_title or ""),
            str(window_title or ""),
            hwnd_value,
            pid_value,
            safe_limit,
        )

    def trace_related_window_chain(
        self,
        *,
        query: str = "",
        hint_query: str = "",
        descendant_hint_query: str = "",
        descendant_title_sequence: Any = None,
        campaign_hint_query: str = "",
        campaign_preferred_title: str = "",
        campaign_descendant_title_sequence: Any = None,
        portfolio_hint_query: str = "",
        portfolio_preferred_title: str = "",
        portfolio_descendant_title_sequence: Any = None,
        preferred_title: str = "",
        window_title: str = "",
        hwnd: int | None = None,
        pid: int | None = None,
        limit: int = 120,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        hwnd_value = 0 if hwnd is None else int(hwnd)
        pid_value = 0 if pid is None else int(pid)
        return self._call(
            "trace_related_window_chain",
            "trace_related_window_chain",
            str(query or ""),
            str(hint_query or ""),
            str(descendant_hint_query or ""),
            self._encode_title_sequence(descendant_title_sequence),
            str(campaign_hint_query or ""),
            str(campaign_preferred_title or ""),
            self._encode_title_sequence(campaign_descendant_title_sequence),
            str(portfolio_hint_query or ""),
            str(portfolio_preferred_title or ""),
            self._encode_title_sequence(portfolio_descendant_title_sequence),
            str(preferred_title or ""),
            str(window_title or ""),
            hwnd_value,
            pid_value,
            safe_limit,
        )


@lru_cache(maxsize=1)
def get_native_window_runtime() -> NativeWindowRuntime:
    return NativeWindowRuntime()
