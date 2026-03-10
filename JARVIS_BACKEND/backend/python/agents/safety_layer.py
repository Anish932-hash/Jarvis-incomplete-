from typing import Dict, Any
from backend.python.utils.logger import Logger


ALLOWED_ACTIONS = {
    "tts_speak",
    "open_app",
    "media_search",
    "defender_status",
}


class SafetyLayer:
    """
    Ensures task plans remain safe.
    Blocks dangerous OS-level operations.
    """

    def __init__(self):
        self.log = Logger("SafetyLayer")

    # --------------------------------------------------------------------

    async def validate_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Checks each step for safety before approval."""

        for step in plan["steps"]:
            action = step.get("action")

            if action not in ALLOWED_ACTIONS:
                reason = f"Action '{action}' is not allowed for safety."
                self.log.warn(reason)
                return {"allowed": False, "reason": reason}

        return {"allowed": True, "reason": None}