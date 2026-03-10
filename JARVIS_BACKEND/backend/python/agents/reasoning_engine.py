from typing import Any, Dict

from backend.python.api.groq_client import GroqClient
from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.utils.logger import Logger


class ReasoningEngine:
    """
    Interprets user text into structured intent + arguments.
    """

    def __init__(self):
        self.credentials = ProviderCredentialManager()
        self.credentials.refresh(overwrite_env=False)
        self.client = GroqClient(api_key=self.credentials.get_api_key("groq") or None)
        self.log = Logger.get_logger("ReasoningEngine")

    async def analyze(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        prompt = (
            "You are a safe command interpreter.\n"
            "Convert user request into JSON with keys intent and arguments.\n"
            "Allowed intents: open_application, search_media, check_security, speak.\n"
            f"Context: {context}\n"
            f"User: {text}\n"
        )
        response = await self.client.reason(prompt)
        result = {
            "intent": response.get("intent", "speak"),
            "arguments": response.get("arguments", {}),
            "raw": response,
            "connector": self.client.diagnostics() if hasattr(self.client, "diagnostics") else {},
        }
        self.log.debug(f"Reasoning result: {result}")
        return result
