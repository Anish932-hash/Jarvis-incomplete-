import asyncio
import importlib
import inspect
import pkgutil
from typing import Any, Callable, Dict

from backend.python.utils.logger import Logger


class Router:
    """
    Dynamic event dispatcher / action router.
    Loads route handlers tagged with @route decorator.
    """

    def __init__(self, event_bus: Any, settings: Any):
        self.event_bus = event_bus
        self.settings = settings
        self.logger = Logger.get_logger("Router")
        self.routes: Dict[str, Callable[..., Any]] = {}

    async def load_routes(self) -> None:
        self.logger.info("Scanning for dynamic routes...")
        packages = [
            "backend.python.tools",
            "backend.python.speech",
            "backend.python.pc_control",
        ]

        for package in packages:
            try:
                pkg = importlib.import_module(package)
            except Exception as exc:  # noqa: BLE001
                self.logger.error(f"Package import failed: {package} ({exc})")
                continue

            for module in pkgutil.iter_modules(pkg.__path__):
                mod_name = f"{package}.{module.name}"
                try:
                    mod = importlib.import_module(mod_name)
                except Exception as exc:  # noqa: BLE001
                    self.logger.error(f"Module load failed: {mod_name} ({exc})")
                    continue

                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if callable(attr) and hasattr(attr, "_route_event"):
                        event_name = str(attr._route_event)
                        self.routes[event_name] = attr
                        self.logger.info(f"Route registered: {event_name} -> {mod_name}.{attr_name}")

        self.logger.info(f"Router loaded {len(self.routes)} routes.")

    async def dispatch(self, event_type: str, payload: dict) -> Any:
        handler = self.routes.get(event_type)
        if handler is None:
            raise ValueError(f"No route handler for event: {event_type}")

        if inspect.iscoroutinefunction(handler):
            return await handler(payload)

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, handler, payload)


def route(event_name: str):
    """
    Decorator to mark function as a route handler.
    """

    def decorator(func):
        func._route_event = event_name
        return func

    return decorator
