from typing import Dict, Any
from backend.python.utils.logger import Logger
from .memory_manager import MemoryManager


class ContextBuilder:
    """
    Builds full context for reasoning:
    - Recent memory
    - Conversation pattern
    - System state
    """

    def __init__(self, memory: MemoryManager):
        self.memory = memory
        self.log = Logger("ContextBuilder")

    # -----------------------------------------------------------

    async def build(self, user_text: str) -> Dict[str, Any]:
        recent = self.memory.load_recent()

        return {
            "user_text": user_text,
            "recent_memory": recent,
        }