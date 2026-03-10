"""
backend.python
==============

This package initializes all core backend subsystems for the JARVIS AI.

It performs:
- Dynamic module discovery
- Capability validation (safe PC-control limits enforced)
- System diagnostics
- Logging bootstrap
- Public API export
"""

import importlib
import pkgutil
import platform
import asyncio
from typing import Dict, Any

from backend.python.utils.logger import Logger
from backend.python.settings import Settings
from backend.python.event_bus import EventBus

__all__ = [
    "initialize_backend",
    "get_backend_state",
    "event_bus",
    "settings",
    "diagnostics",
]

# ---------------------------------------------------------
# GLOBAL OBJECTS
# ---------------------------------------------------------

logger = Logger("BackendInit").get_logger()
settings = Settings()
event_bus = EventBus()

diagnostics: Dict[str, Any] = {
    "python_version": platform.python_version(),
    "os": platform.system(),
    "os_release": platform.release(),
    "modules_loaded": [],
    "safe_pc_control": {
        "keyboard": False,  # intentionally disabled (safety)
        "mouse": False,     # intentionally disabled (safety)
        "windows": True,    # window switching and reading allowed
        "system_info": True,
    },
}


# ---------------------------------------------------------
# INTERNAL AUTO-LOADER
# ---------------------------------------------------------

def _dynamic_import(package: str):
    """
    Dynamically imports all modules inside a package.
    Records module names for debugging / diagnostics.
    """
    try:
        imported = []
        pkg = importlib.import_module(package)

        for module in pkgutil.iter_modules(pkg.__path__):
            module_name = f"{package}.{module.name}"
            try:
                importlib.import_module(module_name)
                imported.append(module_name)
            except Exception as e:
                logger.error(f"Module load failed: {module_name} ({e})")

        diagnostics["modules_loaded"].extend(imported)
        return imported

    except Exception as e:
        logger.error(f"Failed to scan package {package}: {e}")
        return []


# ---------------------------------------------------------
# BACKEND INITIALIZATION
# ---------------------------------------------------------

async def initialize_backend():
    """
    Comprehensive async backend initializer.
    Loads all subsystems:
    - pc_control (safe capabilities only)
    - speech
    - tools
    - utils
    Registers event bus routing (performed in main.py)
    """

    logger.info("Initializing backend.python subsystem…")

    # Auto-load subpackages
    packages = [
        "backend.python.pc_control",
        "backend.python.speech",
        "backend.python.tools",
        "backend.python.utils",
    ]

    for pkg in packages:
        _dynamic_import(pkg)

    logger.info("All backend modules imported.")

    # Safe system validation
    _validate_system_environment()

    # Confirm backend readiness
    diagnostics["ready"] = True
    logger.info("Backend initialization complete.")

    return True


# ---------------------------------------------------------
# SYSTEM ENVIRONMENT & SAFETY VALIDATION
# ---------------------------------------------------------

def _validate_system_environment():
    """
    Validates system environment and updates diagnostics.
    Enforces safety limits on capabilities.
    """
    system = platform.system()
    diagnostics["environment"] = {"platform": system}

    if system not in ("Windows", "Linux", "Darwin"):
        logger.warning(f"Unsupported OS detected: {system}")

    # Safety enforcement: Hard lock on dangerous PC controls
    diagnostics["safe_pc_control"]["keyboard"] = False
    diagnostics["safe_pc_control"]["mouse"] = False

    logger.info("Safety constraints applied to PC-control modules.")


# ---------------------------------------------------------
# PUBLIC BACKEND STATE API
# ---------------------------------------------------------

def get_backend_state() -> Dict[str, Any]:
    """
    Returns a structured snapshot of backend status.
    Useful for GUI dashboards, debugging, or voice queries.
    """
    return {
        "ready": diagnostics.get("ready", False),
        "system": diagnostics.get("environment", {}),
        "python": diagnostics.get("python_version"),
        "modules_loaded": diagnostics.get("modules_loaded"),
        "safe_pc_control": diagnostics.get("safe_pc_control"),
    }
