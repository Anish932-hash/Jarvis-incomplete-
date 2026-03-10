"""
PC control exports are lazy because some adapters are OS/package specific.
"""

from importlib import import_module
from typing import Any

__all__ = [
    "AppLauncher",
    "FileManager",
    "FolderManager",
    "MediaController",
    "SystemMonitor",
    "WindowManager",
    "DefenderMonitor",
    "NotificationManager",
]

_MODULE_MAP = {
    "AppLauncher": "backend.python.pc_control.app_launcher",
    "FileManager": "backend.python.pc_control.file_manager",
    "FolderManager": "backend.python.pc_control.folder_manager",
    "MediaController": "backend.python.pc_control.media_control",
    "SystemMonitor": "backend.python.pc_control.system_monitor",
    "WindowManager": "backend.python.pc_control.window_manager",
    "DefenderMonitor": "backend.python.pc_control.defender_monitor",
    "NotificationManager": "backend.python.pc_control.notification_manager",
}


def __getattr__(name: str) -> Any:
    module_name = _MODULE_MAP.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)
