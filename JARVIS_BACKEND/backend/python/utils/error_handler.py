import asyncio
import traceback
from functools import wraps
from typing import Any, Callable, Optional

from .logger import Logger

log = Logger.get_logger("ErrorHandler")


class JarvisError(Exception):
    """Base error class for JARVIS failures."""


class ConfigError(JarvisError):
    """Configuration loading/validation error."""


class ValidationError(JarvisError):
    """Input validation error."""


class SafeErrorHandler:
    @staticmethod
    def handle_error(err: Exception) -> dict:
        info = "".join(traceback.format_exception(type(err), err, err.__traceback__))
        log.error(f"ERROR: {err}")
        log.debug(info)
        return {"status": "error", "details": str(err)}

    @staticmethod
    def guard(func: Callable[..., Any]) -> Callable[..., Any]:
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await func(*args, **kwargs)
                except Exception as err:  # noqa: BLE001
                    SafeErrorHandler.handle_error(err)
                    return None

            return async_wrapper

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as err:  # noqa: BLE001
                SafeErrorHandler.handle_error(err)
                return None

        return wrapper


def global_exception_handler(logger: Optional[Any] = None) -> None:
    """
    Register process-wide handlers for uncaught exceptions.
    """
    active_logger = logger or log

    def handle_exception(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        msg = context.get("message", "Unhandled async exception")
        if exc is not None:
            active_logger.error(f"{msg}: {exc}")
        else:
            active_logger.error(msg)

    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(handle_exception)
    except RuntimeError:
        # No running loop at import/setup time.
        pass
