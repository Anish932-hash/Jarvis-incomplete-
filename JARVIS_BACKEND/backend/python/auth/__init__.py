from .permissions import Permissions

# Token/JWT auth is optional in local desktop mode; keep permissions importable
# even when jwt extras are not installed.
try:
    from .token_manager import TokenManager
    from .user_auth import UserAuth
except ModuleNotFoundError:  # pragma: no cover - depends on optional extras
    TokenManager = None  # type: ignore[assignment]
    UserAuth = None  # type: ignore[assignment]

__all__ = [
    "TokenManager",
    "UserAuth",
    "Permissions",
]
