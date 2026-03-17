from __future__ import annotations

from backend.python.native.windows.native_window_runtime import NativeWindowRuntime
from backend.python.pc_control.window_manager import WindowManager


def test_window_manager_build_window_signature_and_surface_hints() -> None:
    signature = WindowManager._build_window_signature(  # noqa: SLF001
        title="Bluetooth & devices - Settings",
        exe=r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
        process_name="SystemSettings.exe",
        class_name="ApplicationFrameWindow",
        rect=(0, 0, 1440, 900),
    )
    hints = WindowManager._infer_surface_hints(  # noqa: SLF001
        title="Bluetooth & devices - Settings",
        process_name="SystemSettings.exe",
        class_name="ApplicationFrameWindow",
        app_name="systemsettings",
    )

    assert signature.startswith("systemsettings|applicationframewindow|1440x900|")
    assert hints["settings_like"] is True
    assert hints["dialog_like"] is False


def test_window_manager_derive_app_name_prefers_executable_name() -> None:
    app_name = WindowManager._derive_app_name(  # noqa: SLF001
        exe=r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe",
        process_name="Code.exe",
        title="main.py - Visual Studio Code",
    )

    assert app_name == "code"


def test_window_manager_prefers_native_runtime_windows() -> None:
    class _FakeNativeRuntime:
        def list_windows(self, *, limit: int = 120) -> dict:
            assert limit == 300
            return {
                "status": "success",
                "backend": "cpp_cython",
                "windows": [
                    {
                        "hwnd": 4242,
                        "title": "Bluetooth & devices - Settings",
                        "pid": 888,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "ApplicationFrameWindow",
                        "visible": True,
                        "enabled": True,
                        "minimized": False,
                        "maximized": True,
                        "is_foreground": True,
                        "left": 0,
                        "top": 0,
                        "right": 1440,
                        "bottom": 900,
                    }
                ],
            }

    manager = WindowManager(native_runtime=_FakeNativeRuntime())
    windows = manager.list_windows()

    assert len(windows) == 1
    assert windows[0]["hwnd"] == 4242
    assert windows[0]["app_name"] == "systemsettings"
    assert windows[0]["observation_backend"] == "cpp_cython"
    assert windows[0]["surface_hints"]["settings_like"] is True


def test_window_manager_focus_window_prefers_native_runtime() -> None:
    class _FakeNativeRuntime:
        def focus_window(self, *, title_contains: str = "", hwnd: int | None = None) -> dict:
            assert title_contains == ""
            assert hwnd == 9001
            return {
                "status": "success",
                "backend": "cpp_cython",
                "focus_applied": True,
                "window": {
                    "hwnd": 9001,
                    "title": "Windows Terminal",
                    "pid": 222,
                    "exe": r"C:\Program Files\WindowsApps\Microsoft.WindowsTerminal.exe",
                    "process_name": "WindowsTerminal.exe",
                    "class_name": "CASCADIA_HOSTING_WINDOW_CLASS",
                    "visible": True,
                    "enabled": True,
                    "minimized": False,
                    "maximized": False,
                    "is_foreground": True,
                    "left": 20,
                    "top": 40,
                    "right": 1280,
                    "bottom": 900,
                },
            }

    manager = WindowManager(native_runtime=_FakeNativeRuntime())
    payload = manager.focus_window(hwnd=9001)

    assert payload["status"] == "success"
    assert payload["focus_applied"] is True
    assert payload["window"]["app_name"] == "microsoft.windowsterminal"
    assert payload["window"]["observation_backend"] == "cpp_cython"


def test_native_window_runtime_reports_missing_extension_build_hint(monkeypatch) -> None:
    from backend.python.native.windows import native_window_runtime as runtime_module

    def _raise_import(_name: str):
        raise ImportError("extension missing")

    monkeypatch.setattr(runtime_module, "import_module", _raise_import)
    runtime = NativeWindowRuntime(extension_module="backend.python.native.windows._missing_bridge")
    payload = runtime.availability()

    assert payload["status"] == "success"
    assert payload["available"] is False
    assert payload["backend"] == "cpp_cython"
    assert "build_native_windows_bridge.ps1" in payload["build_hint"]


def test_native_window_runtime_delegates_to_loaded_extension() -> None:
    class _FakeModule:
        @staticmethod
        def list_windows(limit: int) -> dict:
            assert limit == 5
            return {"status": "success", "windows": [], "count": 0}

    runtime = NativeWindowRuntime(module=_FakeModule())
    payload = runtime.list_windows(limit=5)

    assert payload["status"] == "success"
    assert payload["backend"] == "cpp_cython"
    assert payload["count"] == 0
