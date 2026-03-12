from __future__ import annotations

from typing import Any, Dict

from backend.python.core.desktop_action_router import DesktopActionRouter
from backend.python.router import route


_DESKTOP_ACTION_ROUTER = DesktopActionRouter()


@route("desktop_action_advice")
def desktop_action_advice_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _DESKTOP_ACTION_ROUTER.advise(payload if isinstance(payload, dict) else {})


@route("desktop_interact")
def desktop_interact_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _DESKTOP_ACTION_ROUTER.execute(payload if isinstance(payload, dict) else {})
