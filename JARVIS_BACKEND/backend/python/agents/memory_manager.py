from typing import Dict, Any, List
from backend.python.database.memory_db import MemoryDB
from backend.python.utils.logger import Logger


class MemoryManager:
    """
    Handles persistent + short-term memory.
    Stores conversations, plans, and context signals.
    """

    def __init__(self):
        self.db = MemoryDB()
        self.log = Logger("MemoryManager")

    # ------------------------------------------------------------

    def store_interaction(self, user_text: str, plan: Dict[str, Any]):
        self.db.insert_entry({
            "type": "interaction",
            "user_text": user_text,
            "plan": plan
        })

    # ------------------------------------------------------------

    def load_recent(self, limit: int = 10) -> List[Dict[str, Any]]:
        return self.db.get_recent(limit)