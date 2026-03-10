import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from backend.python.utils.logger import Logger


class Settings:
    """
    Settings loader with JSON/YAML support and environment overrides.
    """

    def __init__(self, config_path: str = "configs/jarvis.yaml"):
        self.logger = Logger.get_logger("Settings")
        self.path = Path(config_path)
        self.data: Dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            self.logger.warning(f"Config file not found: {self.path}. Using defaults.")
            self.data = {"jarvis": {"name": "Jarvis", "environment": "development"}}
            return

        try:
            if self.path.suffix.lower() in (".yaml", ".yml"):
                self.data = self._load_yaml(self.path)
            else:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"Failed to load config {self.path}: {exc}")
            self.data = {}
            return

        self._apply_env_overrides()
        self.logger.info(f"Settings loaded from {self.path}.")

    def save_json(self, target_path: Optional[str] = None) -> None:
        target = Path(target_path) if target_path else self.path.with_suffix(".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
        self.logger.info(f"Settings saved to {target}.")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Dot-path getter. Example: get("jarvis.name")
        """
        current: Any = self.data
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def _apply_env_overrides(self) -> None:
        # Flat override for common keys.
        mapping = {
            "JARVIS_NAME": "jarvis.name",
            "JARVIS_ENVIRONMENT": "jarvis.environment",
            "GROQ_API_KEY": "services.groq.api_key",
            "ELEVENLABS_API_KEY": "services.elevenlabs.api_key",
            "NVIDIA_API_KEY": "services.nvidia.api_key",
        }
        for env_key, dot_key in mapping.items():
            value = os.getenv(env_key)
            if value is not None:
                self._set(dot_key, value)

    def _set(self, key: str, value: Any) -> None:
        parts = key.split(".")
        current = self.data
        for part in parts[:-1]:
            node = current.get(part)
            if not isinstance(node, dict):
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    @staticmethod
    def _load_yaml(path: Path) -> Dict[str, Any]:
        try:
            import yaml  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("PyYAML is required to load YAML configs.") from exc

        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
