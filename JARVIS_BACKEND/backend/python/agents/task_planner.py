from typing import Dict, Any, List
from backend.python.utils.logger import Logger


class TaskPlanner:
    """
    Translates model reasoning into deterministic, safe,
    multi-step execution plans the supervisor can execute.
    """

    def __init__(self):
        self.log = Logger("TaskPlanner")

    # --------------------------------------------------------------------

    async def generate_plan(self, reasoning: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Input:
            reasoning: structured reasoning output
            context: user + system context
        Output:
            plan: structured deterministic steps
        """

        self.log.info("Generating execution plan from reasoning...")

        intent = reasoning.get("intent")
        arguments = reasoning.get("arguments", {})

        if not intent:
            return {}

        steps = []

        # -------------------------
        # INTENT → ACTION MAPPING
        # SAFE operations only
        # -------------------------

        if intent == "open_application":
            steps.append({
                "action": "open_app",
                "app_name": arguments.get("app"),
            })

        elif intent == "search_media":
            steps.append({
                "action": "media_search",
                "query": arguments.get("query")
            })

        elif intent == "check_security":
            steps.append({"action": "defender_status"})

        elif intent == "speak":
            steps.append({
                "action": "tts_speak",
                "text": arguments.get("text")
            })

        else:
            # fallback to safe text response
            steps.append({
                "action": "tts_speak",
                "text": f"I understood your request but cannot perform that action safely."
            })

        return {
            "intent": intent,
            "steps": steps,
            "context": context
        }