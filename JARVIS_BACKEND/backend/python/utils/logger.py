import json
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


class Logger:
    """
    Backward-compatible logging utility.

    Supported usage:
    - Logger("Name").get_logger()
    - Logger.get_logger("Name")
    """

    LOG_DIR = Path("logs")
    LOG_DIR.mkdir(exist_ok=True)

    def __init__(self, name: str = "JARVIS") -> None:
        self._name = name
        self._logger = self._build_logger(name)
        # Backward-compatible instance call: Logger("X").get_logger()
        self.get_logger = lambda: self._logger  # type: ignore[assignment]

    def __getattr__(self, item: str) -> Any:
        # Allow old code that calls Logger("X").info(...).
        return getattr(self._logger, item)

    @staticmethod
    def get_logger(name: str = "JARVIS") -> logging.Logger:
        return Logger._build_logger(name)

    @classmethod
    def _build_logger(cls, name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        if logger.handlers:
            return logger

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(
            logging.Formatter("\033[94m[%(levelname)s]\033[0m %(message)s")
        )

        file_handler = RotatingFileHandler(
            cls.LOG_DIR / "system.log",
            maxBytes=10_000_000,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter("%(message)s"))

        logger.addHandler(console)
        logger.addHandler(file_handler)

        # Attach structured logging helper.
        def json_log(level: int, msg: str, **kwargs: Any) -> None:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": logging.getLevelName(level),
                "message": msg,
                **kwargs,
            }
            logger.log(level, json.dumps(payload, ensure_ascii=True))

        logger.json = json_log  # type: ignore[attr-defined]
        return logger
