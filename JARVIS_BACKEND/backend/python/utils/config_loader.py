import json
import threading
from pathlib import Path
from .validators import Validators
from .logger import Logger

log = Logger.get_logger("ConfigLoader")

class ConfigLoader:
    _cache = {}
    _lock = threading.Lock()

    DEFAULT_CONFIG = Path("config/default.json")
    USER_CONFIG = Path("config/user.json")

    @staticmethod
    def load_config():
        with ConfigLoader._lock:
            base = {}
            if ConfigLoader.DEFAULT_CONFIG.exists():
                base = json.loads(ConfigLoader.DEFAULT_CONFIG.read_text())

            if ConfigLoader.USER_CONFIG.exists():
                user = json.loads(ConfigLoader.USER_CONFIG.read_text())
                base.update(user)

            ConfigLoader._cache = base
            return base

    @staticmethod
    def get(key, default=None):
        if not ConfigLoader._cache:
            ConfigLoader.load_config()
        return ConfigLoader._cache.get(key, default)

    @staticmethod
    def set(key, value):
        with ConfigLoader._lock:
            ConfigLoader._cache[key] = value
            ConfigLoader.save()

    @staticmethod
    def save():
        ConfigLoader.USER_CONFIG.parent.mkdir(exist_ok=True)
        ConfigLoader.USER_CONFIG.write_text(
            json.dumps(ConfigLoader._cache, indent=4)
        )
        log.info("Configuration saved.")

    @staticmethod
    def reload():
        log.info("Configuration reloaded.")
        return ConfigLoader.load_config()