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


def test_window_manager_tracks_owner_window_topology_and_reacquisition() -> None:
    class _FakeNativeRuntime:
        @staticmethod
        def list_windows(*, limit: int = 120) -> dict:
            assert limit == 300
            return {
                "status": "success",
                "backend": "cpp_cython",
                "windows": [
                    {
                        "hwnd": 5000,
                        "owner_hwnd": 0,
                        "title": "Settings",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "ApplicationFrameWindow",
                        "visible": True,
                        "enabled": True,
                        "minimized": False,
                        "maximized": True,
                        "is_foreground": False,
                        "left": 0,
                        "top": 0,
                        "right": 1440,
                        "bottom": 900,
                    },
                    {
                        "hwnd": 5001,
                        "owner_hwnd": 5000,
                        "title": "Bluetooth & devices",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "#32770",
                        "visible": True,
                        "enabled": True,
                        "minimized": False,
                        "maximized": False,
                        "is_foreground": True,
                        "left": 120,
                        "top": 80,
                        "right": 1100,
                        "bottom": 760,
                    },
                    {
                        "hwnd": 5002,
                        "owner_hwnd": 5001,
                        "title": "Pair device",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "#32770",
                        "visible": True,
                        "enabled": True,
                        "minimized": False,
                        "maximized": False,
                        "is_foreground": False,
                        "left": 280,
                        "top": 160,
                        "right": 980,
                        "bottom": 640,
                    },
                ],
            }

        @staticmethod
        def active_window() -> dict:
            return {
                "status": "success",
                "backend": "cpp_cython",
                "window": {
                    "hwnd": 5001,
                    "owner_hwnd": 5000,
                    "title": "Bluetooth & devices",
                    "pid": 777,
                    "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                    "process_name": "SystemSettings.exe",
                    "class_name": "#32770",
                    "visible": True,
                    "enabled": True,
                    "minimized": False,
                    "maximized": False,
                    "is_foreground": True,
                    "left": 120,
                    "top": 80,
                    "right": 1100,
                    "bottom": 760,
                },
            }

        @staticmethod
        def trace_related_window_chain(*, query: str = "", window_title: str = "", hwnd: int | None = None, pid: int | None = None, limit: int = 120) -> dict:
            assert limit >= 3
            if int(hwnd or 0) == 5001:
                return {
                    "status": "success",
                    "backend": "cpp_cython",
                    "candidate": {
                        "hwnd": 5001,
                        "owner_hwnd": 5000,
                        "title": "Bluetooth & devices",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "#32770",
                        "visible": True,
                        "enabled": True,
                        "minimized": False,
                        "maximized": False,
                        "is_foreground": True,
                        "left": 120,
                        "top": 80,
                        "right": 1100,
                        "bottom": 760,
                    },
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "direct_child_titles": ["Pair device"],
                    "descendant_chain_depth": 1,
                    "descendant_dialog_chain_depth": 1,
                    "descendant_query_match_count": 1,
                    "descendant_chain_titles": ["Pair device"],
                    "child_chain_signature": "5001|1|1|Pair device",
                    "preferred_descendant": {
                        "hwnd": 5002,
                        "owner_hwnd": 5001,
                        "title": "Pair device",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "#32770",
                        "visible": True,
                        "enabled": True,
                        "minimized": False,
                        "maximized": False,
                        "is_foreground": False,
                        "left": 280,
                        "top": 160,
                        "right": 980,
                        "bottom": 640,
                    },
                }
            return {
                "status": "success",
                "backend": "cpp_cython",
                "candidate": {
                    "hwnd": 5002,
                    "owner_hwnd": 5001,
                    "title": "Pair device",
                    "pid": 777,
                    "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                    "process_name": "SystemSettings.exe",
                    "class_name": "#32770",
                    "visible": True,
                    "enabled": True,
                    "minimized": False,
                    "maximized": False,
                    "is_foreground": False,
                    "left": 280,
                    "top": 160,
                    "right": 980,
                    "bottom": 640,
                },
                "direct_child_window_count": 0,
                "direct_child_dialog_like_count": 0,
                "direct_child_titles": [],
                "descendant_chain_depth": 0,
                "descendant_dialog_chain_depth": 0,
                "descendant_query_match_count": 0,
                "descendant_chain_titles": [],
                "child_chain_signature": "5002|0|0",
                "preferred_descendant": {},
            }

    manager = WindowManager(native_runtime=_FakeNativeRuntime())

    topology = manager.window_topology_snapshot(app_name="settings", query="bluetooth", include_windows=True)
    reacquired = manager.reacquire_window(app_name="settings", query="pair device", hwnd=5001, pid=777)

    assert topology["status"] == "success"
    assert topology["backend"] == "cpp_cython"
    assert topology["owner_chain_visible"] is True
    assert topology["owner_link_count"] >= 2
    assert topology["same_root_owner_window_count"] == 3
    assert topology["same_root_owner_dialog_like_count"] == 2
    assert topology["active_owner_chain_depth"] == 1
    assert topology["max_owner_chain_depth"] == 2
    assert topology["modal_chain_signature"] == "5000|2|1|Bluetooth & devices|Pair device"
    assert topology["branch_family_signature"] == "5000|2|Bluetooth & devices|Pair device"
    assert topology["direct_child_window_count"] == 1
    assert topology["direct_child_dialog_like_count"] == 1
    assert topology["descendant_chain_depth"] == 1
    assert topology["descendant_chain_titles"] == ["Pair device"]
    assert topology["child_chain_signature"] == "5001|1|1|Pair device"
    assert topology["preferred_descendant"]["hwnd"] == 5002
    assert "Settings" in topology["owner_window_titles"]
    assert "Pair device" in topology["owner_window_titles"]
    assert topology["owner_chain_titles"] == ["Settings", "Bluetooth & devices"]
    assert "Pair device" in topology["same_root_owner_titles"]
    assert len(topology["owner_windows"]) >= 2
    assert reacquired["status"] == "success"
    assert reacquired["candidate"]["hwnd"] == 5002
    assert reacquired["candidate"]["owner_hwnd"] == 5001
    assert reacquired["candidate"]["root_owner_hwnd"] == 5000
    assert reacquired["candidate"]["owner_chain_depth"] == 2
    assert reacquired["owner_chain_visible"] is True
    assert reacquired["owner_link_count"] >= 2
    assert reacquired["same_root_owner_window_count"] == 3
    assert reacquired["same_root_owner_dialog_like_count"] == 2
    assert reacquired["candidate_root_owner_hwnd"] == 5000
    assert reacquired["candidate_owner_chain_depth"] == 2
    assert reacquired["max_owner_chain_depth"] == 2
    assert reacquired["modal_chain_signature"] == "5000|2|2|Bluetooth & devices|Pair device"
    assert reacquired["branch_family_signature"] == "5000|2|Bluetooth & devices|Pair device"
    assert reacquired["child_chain_signature"] == "5002|0|0"
    assert reacquired["descendant_chain_depth"] == 0
    assert "Settings" in reacquired["owner_chain_titles"]


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


def test_window_manager_native_reacquire_applies_benchmark_guidance_to_child_dialog_rerank() -> None:
    class _GuidedNativeRuntime:
        def reacquire_related_window(
            self,
            *,
            query: str = "",
            window_title: str = "",
            hwnd: int | None = None,
            pid: int | None = None,
            limit: int = 80,
        ) -> dict:
            del query, window_title, hwnd, pid, limit
            return {
                "status": "success",
                "backend": "cpp_cython",
                "candidate": {
                    "hwnd": 5001,
                    "title": "Bluetooth & devices",
                    "pid": 777,
                    "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                    "process_name": "SystemSettings.exe",
                    "class_name": "ApplicationFrameWindow",
                    "visible": True,
                    "enabled": True,
                    "owner_hwnd": 5000,
                    "root_owner_hwnd": 5000,
                    "owner_chain_depth": 1,
                },
                "candidates": [
                    {
                        "hwnd": 5001,
                        "title": "Bluetooth & devices",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "ApplicationFrameWindow",
                        "visible": True,
                        "enabled": True,
                        "owner_hwnd": 5000,
                        "root_owner_hwnd": 5000,
                        "owner_chain_depth": 1,
                        "match_score": 0.95,
                    },
                    {
                        "hwnd": 5002,
                        "title": "Pair device",
                        "pid": 777,
                        "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe",
                        "process_name": "SystemSettings.exe",
                        "class_name": "#32770",
                        "visible": True,
                        "enabled": True,
                        "owner_hwnd": 5001,
                        "root_owner_hwnd": 5000,
                        "owner_chain_depth": 2,
                        "match_score": 0.7,
                    },
                ],
                "same_process_window_count": 2,
                "related_window_count": 2,
                "owner_link_count": 2,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 2,
                "same_root_owner_dialog_like_count": 1,
                "candidate_root_owner_hwnd": 5000,
                "candidate_owner_chain_depth": 1,
                "max_owner_chain_depth": 2,
                "child_dialog_like_visible": True,
                "owner_chain_titles": ["Settings", "Bluetooth & devices"],
                "same_root_owner_titles": ["Bluetooth & devices", "Pair device"],
                "same_root_owner_dialog_titles": ["Pair device"],
            }

        def trace_related_window_chain(
            self,
            *,
            query: str = "",
            window_title: str = "",
            hwnd: int | None = None,
            pid: int | None = None,
            limit: int = 80,
        ) -> dict:
            del query, window_title, hwnd, pid, limit
            return {
                "status": "success",
                "backend": "cpp_cython",
                "direct_child_window_count": 1,
                "direct_child_dialog_like_count": 1,
                "direct_child_titles": ["Pair device"],
                "descendant_chain_depth": 1,
                "descendant_dialog_chain_depth": 1,
                "descendant_query_match_count": 0,
                "descendant_chain_titles": ["Pair device"],
                "child_chain_signature": "5001|1|1|Pair device",
                "preferred_descendant": {"hwnd": 5002, "title": "Pair device"},
            }

    manager = WindowManager(native_runtime=_GuidedNativeRuntime())

    payload = manager.reacquire_window(
        app_name="settings",
        query="",
        pid=777,
        benchmark_guidance={
            "control_biases": {
                "dialog_resolution": 0.95,
                "descendant_focus": 0.95,
                "navigation_branch": 0.2,
                "recovery_reacquire": 0.95,
                "loop_guard": 0.3,
                "native_focus": 0.95,
            }
        },
    )

    assert payload["status"] == "success"
    assert payload["candidate"]["hwnd"] == 5002
    assert "benchmark_deeper_owner_chain" in payload["candidate"]["match_reasons"]
    assert "benchmark_native_descendant_pressure" in payload["candidate"]["match_reasons"]


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
