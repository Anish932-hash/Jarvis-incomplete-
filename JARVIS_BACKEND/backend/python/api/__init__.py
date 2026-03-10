from importlib import import_module
from typing import Any

__all__ = [
    "GroqClient",
    "NvidiaClient",
    "FirebaseClient",
    "BrowserAPI",
    "HttpClient",
]

_MODULE_MAP = {
    "GroqClient": "backend.python.api.groq_client",
    "NvidiaClient": "backend.python.api.nvidia_client",
    "FirebaseClient": "backend.python.api.firebase_client",
    "BrowserAPI": "backend.python.api.browser_api",
    "HttpClient": "backend.python.api.http_client",
}


def __getattr__(name: str) -> Any:
    module_name = _MODULE_MAP.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)
