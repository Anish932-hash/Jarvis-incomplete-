import asyncio
from typing import Dict, Any, Optional

from .task_planner import TaskPlanner
from .reasoning_engine import ReasoningEngine
from .safety_layer import SafetyLayer
from .action_supervisor import ActionSupervisor
from .memory_manager import MemoryManager
from .context_builder import ContextBuilder

from backend.python.event_bus import EventBus
from backend.python.utils.logger import Logger


class CoreAgent:
    """
    The central orchestrator of the entire Jarvis backend.
    Handles:
    - User command understanding
    - Context building
    - Reasoning + task planning
    - Safety validation
    - Action execution delegation
    - Memory updates
    """

    def __init__(self):
        self.event_bus = EventBus()
        self.log = Logger("CoreAgent")

        self.memory = MemoryManager()
        self.context_builder = ContextBuilder(self.memory)
        self.reasoning = ReasoningEngine()
        self.planner = TaskPlanner()
        self.safety = SafetyLayer()
        self.supervisor = ActionSupervisor(self.event_bus)

    # -----------------------------------------------------------

    async def handle_user_command(self, text: str) -> Dict[str, Any]:
        """Full command pipeline (context → reasoning → planning → safety → execution)."""

        self.log.info(f"Received user command: {text!r}")

        context = await self.context_builder.build(text)
        reasoning = await self.reasoning.analyze(text, context)
        plan = await self.planner.generate_plan(reasoning, context)

        if not plan or len(plan["steps"]) == 0:
            return {"status": "error", "message": "Unable to create an actionable plan."}

        safety_check = await self.safety.validate_plan(plan)

        if not safety_check["allowed"]:
            self.log.warn(f"Safety blocked: {safety_check['reason']}")
            return {"status": "blocked", "reason": safety_check["reason"]}

        execution_result = await self.supervisor.execute_plan(plan)

        self.memory.store_interaction(text, plan)
        return {"status": "success", "result": execution_result}

    # -----------------------------------------------------------

    async def system_loop(self):
        """
        Continuous task runner (heartbeat loop).
        Safe and lightweight.
        """
        self.log.info("CoreAgent heartbeat started.")
        while True:
            await asyncio.sleep(1)
            await self.supervisor.tick()