"""
Tool package exports are lazy to avoid hard import failures for optional dependencies.
"""

from importlib import import_module
from typing import Any

__all__ = [
    "FileTools",
    "SystemTools",
    "SearchTools",
    "TimeTools",
    "AutomationTools",
    "VisionTools",
]

_MODULE_MAP = {
    "FileTools": "backend.python.tools.file_tools",
    "SystemTools": "backend.python.tools.system_tools",
    "SearchTools": "backend.python.tools.search_tools",
    "TimeTools": "backend.python.tools.time_tools",
    "AutomationTools": "backend.python.tools.automation_tools",
    "VisionTools": "backend.python.tools.vision_tools",
}


def __getattr__(name: str) -> Any:
    module_name = _MODULE_MAP.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)
