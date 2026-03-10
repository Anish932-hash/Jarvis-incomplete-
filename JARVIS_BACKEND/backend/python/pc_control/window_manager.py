from typing import Any, Dict, List

import psutil

try:
    import win32gui
    import win32process
except Exception:  # noqa: BLE001
    win32gui = None
    win32process = None


class WindowManager:
    """Safe monitor-only window manager."""

    def _get_hwnd_info(self, hwnd: int) -> Dict[str, Any]:
        if win32gui is None or win32process is None:
            return {}
        try:
            title = win32gui.GetWindowText(hwnd)
            rect = win32gui.GetWindowRect(hwnd)
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            proc = psutil.Process(pid)
            exe = proc.exe() if proc else None
            return {
                "hwnd": hwnd,
                "title": title,
                "pid": pid,
                "exe": exe,
                "position": {
                    "left": rect[0],
                    "top": rect[1],
                    "right": rect[2],
                    "bottom": rect[3],
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
