from __future__ import annotations

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
