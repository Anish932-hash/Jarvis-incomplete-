import asyncio
from typing import Dict, Any, Optional
from backend.python.event_bus import EventBus
from backend.python.utils.logger import Logger


class ActionSupervisor:
    """
    Executes safe steps in a plan.
    Delegates to system modules via EventBus.
    """

    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.log = Logger("ActionSupervisor")

    # --------------------------------------------------------------------

    async def execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        results = []

        for step in plan["steps"]:
            action = step["action"]
            result = await self._execute_step(action, step)
            results.append(result)

        return {"steps": results}

    # --------------------------------------------------------------------

    async def _execute_step(self, action: str, step: Dict[str, Any]) -> Dict[str, Any]:
        """Delegates safe commands to the event bus."""
        self.log.info(f"Executing: {step}")

        event_data = {"action": action, **step}
        try:
            result = await self.event_bus.request("agent.action", event_data)
        except Exception as exc:  # noqa: BLE001
            self.log.error(f"Action dispatch failed for {action}: {exc}")
            return {"action": action, "status": "failed", "error": str(exc)}

        return {"action": action, "result": result}

    # --------------------------------------------------------------------

    async def tick(self):
        """Periodic supervisor tasks."""
        await asyncio.sleep(0)
