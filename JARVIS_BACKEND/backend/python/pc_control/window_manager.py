from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import psutil

try:
    import win32gui
    import win32process
except Exception:  # noqa: BLE001
    win32gui = None
    win32process = None


class WindowManager:
    """Safe monitor-only window manager with lightweight surface heuristics."""

    _BROWSER_PROCESSES = {"chrome", "msedge", "firefox", "brave", "opera", "iexplore"}
    _EDITOR_PROCESSES = {"code", "cursor", "devenv", "notepad++", "sublime_text", "idea64", "pycharm64"}
    _TERMINAL_PROCESSES = {
        "windowsterminal",
        "windows terminal",
        "wt",
        "cmd",
        "powershell",
        "pwsh",
        "conhost",
        "alacritty",
    }
    _FILE_MANAGER_PROCESSES = {"explorer", "totalcmd64", "doublecmd"}
    _ADMIN_PROCESSES = {"mmc", "taskmgr", "regedit", "services", "devmgmt"}

    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        text = str(value or "").strip().lower()
        return " ".join(text.split())

    @classmethod
    def _tokenize(cls, value: Any) -> List[str]:
        text = cls._normalize_text(value)
        if not text:
            return []
        normalized = text
        for token in ("|", "-", "_", "/", "\\", "(", ")", "[", "]", "{", "}", ".", ",", ":"):
            normalized = normalized.replace(token, " ")
        return [part for part in normalized.split() if part]

    @classmethod
    def _derive_app_name(cls, *, exe: str = "", process_name: str = "", title: str = "") -> str:
        base = Path(str(exe or "")).stem.strip().lower()
        if base:
            return base
        process_clean = cls._normalize_text(process_name).replace(".exe", "").strip()
        if process_clean:
            return process_clean
        tokens = cls._tokenize(title)
        return tokens[0] if tokens else "unknown"

    @classmethod
    def _build_window_signature(
        cls,
        *,
        title: str = "",
        exe: str = "",
        process_name: str = "",
        class_name: str = "",
        rect: tuple[int, int, int, int] | None = None,
    ) -> str:
        app_name = cls._derive_app_name(exe=exe, process_name=process_name, title=title)
        title_tokens = cls._tokenize(title)[:6]
        class_token = cls._normalize_text(class_name).replace(" ", "_")
        if rect:
            left, top, right, bottom = rect
            width = max(0, int(right) - int(left))
            height = max(0, int(bottom) - int(top))
            size_token = f"{width}x{height}"
        else:
            size_token = "0x0"
        title_fragment = "_".join(title_tokens) or "untitled"
        return f"{app_name}|{class_token or 'window'}|{size_token}|{title_fragment}"

    @classmethod
    def _infer_surface_hints(
        cls,
        *,
        title: str = "",
        process_name: str = "",
        class_name: str = "",
        app_name: str = "",
    ) -> Dict[str, bool]:
        title_norm = cls._normalize_text(title)
        process_norm = cls._normalize_text(process_name).replace(".exe", "")
        class_norm = cls._normalize_text(class_name)
        app_norm = cls._normalize_text(app_name)

        combined = " ".join(
            part for part in {title_norm, process_norm, class_norm, app_norm} if part
        )
        dialog_like = (
            "#32770" in class_name
            or "dialog" in combined
            or any(token in combined for token in ("properties", "options", "warning", "error", "confirm", "permission"))
        )
        browser_like = process_norm in cls._BROWSER_PROCESSES or app_norm in cls._BROWSER_PROCESSES
        editor_like = process_norm in cls._EDITOR_PROCESSES or app_norm in cls._EDITOR_PROCESSES
        terminal_like = process_norm in cls._TERMINAL_PROCESSES or app_norm in cls._TERMINAL_PROCESSES
        file_manager_like = process_norm in cls._FILE_MANAGER_PROCESSES or app_norm in cls._FILE_MANAGER_PROCESSES
        settings_like = "settings" in combined or "control panel" in combined
        admin_like = (
            process_norm in cls._ADMIN_PROCESSES
            or any(token in combined for token in ("device manager", "event viewer", "task scheduler", "registry editor", "services"))
        )
        return {
            "dialog_like": bool(dialog_like),
            "browser_like": bool(browser_like),
            "editor_like": bool(editor_like),
            "terminal_like": bool(terminal_like),
            "file_manager_like": bool(file_manager_like),
            "settings_like": bool(settings_like),
            "admin_like": bool(admin_like),
        }

    def _get_hwnd_info(self, hwnd: int) -> Dict[str, Any]:
        if win32gui is None or win32process is None:
            return {}
        try:
            title = win32gui.GetWindowText(hwnd)
            rect = win32gui.GetWindowRect(hwnd)
            class_name = win32gui.GetClassName(hwnd)
            visible = bool(win32gui.IsWindowVisible(hwnd))
            enabled = bool(win32gui.IsWindowEnabled(hwnd))
            minimized = bool(win32gui.IsIconic(hwnd))
            maximized = bool(win32gui.IsZoomed(hwnd))
            foreground_hwnd = win32gui.GetForegroundWindow()
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            proc = psutil.Process(pid)
            exe = proc.exe() if proc else None
            process_name = proc.name() if proc else None
            left, top, right, bottom = rect
            width = max(0, int(right) - int(left))
            height = max(0, int(bottom) - int(top))
            app_name = self._derive_app_name(exe=exe or "", process_name=process_name or "", title=title)
            surface_hints = self._infer_surface_hints(
                title=title,
                process_name=process_name or "",
                class_name=class_name,
                app_name=app_name,
            )
            signature = self._build_window_signature(
                title=title,
                exe=exe or "",
                process_name=process_name or "",
                class_name=class_name,
                rect=rect,
            )
            return {
                "hwnd": hwnd,
                "title": title,
                "pid": pid,
                "exe": exe,
                "process_name": process_name,
                "app_name": app_name,
                "class_name": class_name,
                "visible": visible,
                "enabled": enabled,
                "minimized": minimized,
                "maximized": maximized,
                "is_foreground": bool(hwnd == foreground_hwnd),
                "width": width,
                "height": height,
                "area": width * height,
                "title_tokens": self._tokenize(title),
                "window_signature": signature,
                "surface_hints": surface_hints,
                "position": {
                    "left": left,
                    "top": top,
                    "right": right,
                    "bottom": bottom,
                },
            }
        except Exception:
            return {}

    def list_windows(self) -> List[Dict[str, Any]]:
        if win32gui is None:
            return []
        windows: List[Dict[str, Any]] = []

        def callback(hwnd, _):
            if win32gui.IsWindowVisible(hwnd) and win32gui.GetWindowText(hwnd):
                info = self._get_hwnd_info(hwnd)
                if info:
                    windows.append(info)
            return True

        win32gui.EnumWindows(callback, None)
        return windows

    def active_window(self) -> Dict[str, Any]:
        if win32gui is None:
            return {"status": "error", "message": "pywin32 is not available"}
        try:
            hwnd = win32gui.GetForegroundWindow()
            return self._get_hwnd_info(hwnd)
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def get_active_window(self) -> Dict[str, Any]:
        """Compatibility alias used by higher-level context monitoring."""
        return self.active_window()

    def describe_window(self, *, title_contains: str = "", hwnd: int | None = None) -> Dict[str, Any]:
        """Return a richer view of a specific or active window without changing focus."""
        if hwnd is not None:
            payload = self._get_hwnd_info(int(hwnd))
            return {"status": "success", "window": payload} if payload else {"status": "error", "message": "Window not found"}

        if title_contains:
            needle = self._normalize_text(title_contains)
            for item in self.list_windows():
                if needle and needle in self._normalize_text(item.get("title", "")):
                    return {"status": "success", "window": item}
            return {"status": "error", "message": "Window not found"}

        active = self.active_window()
        if isinstance(active, dict) and active.get("status") == "error":
            return active
        return {"status": "success", "window": active}

    def focus_window(self, title_contains: str = "", hwnd: int | None = None) -> Dict[str, Any]:
        if win32gui is None:
            return {"status": "error", "message": "pywin32 is not available"}

        target_hwnd: int | None = None
        if hwnd is not None:
            try:
                if win32gui.IsWindow(hwnd):
                    target_hwnd = hwnd
            except Exception:
                target_hwnd = None

        if target_hwnd is None:
            needle = title_contains.strip().lower()
            if not needle:
                return {"status": "error", "message": "title_contains or hwnd is required"}

            found: list[int] = []

            def callback(candidate_hwnd: int, _: object) -> bool:
                try:
                    if not win32gui.IsWindowVisible(candidate_hwnd):
                        return True
                    title = win32gui.GetWindowText(candidate_hwnd)
                    if title and needle in title.lower():
                        found.append(candidate_hwnd)
                        return False
                except Exception:
                    return True
                return True

            win32gui.EnumWindows(callback, None)
            target_hwnd = found[0] if found else None

        if target_hwnd is None:
            return {"status": "error", "message": "Window not found"}

        try:
            # 5 = SW_SHOW
            win32gui.ShowWindow(target_hwnd, 5)
            win32gui.SetForegroundWindow(target_hwnd)
            return {"status": "success", "window": self._get_hwnd_info(target_hwnd)}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
