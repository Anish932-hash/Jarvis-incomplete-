from pathlib import Path
from typing import Any
from .error_handler import ValidationError

class Validators:

    @staticmethod
    def validate_path(path: str, must_exist=True):
        p = Path(path)
        if must_exist and not p.exists():
            raise ValidationError(f"Path does not exist: {path}")
        return p

    @staticmethod
    def validate_type(value: Any, expected_type: type, name="value"):
        if not isinstance(value, expected_type):
            raise ValidationError(
                f"Invalid type for {name}. Expected {expected_type}, got {type(value)}"
            )
        return True

    @staticmethod
    def validate_non_empty(text: str, name="value"):
        if not text or not text.strip():
            raise ValidationError(f"{name} cannot be empty.")
        return text

    @staticmethod
    def validate_schema(data: dict, schema: dict):
        for key, val_type in schema.items():
            if key not in data:
                raise ValidationError(f"Missing key: {key}")
            Validators.validate_type(data[key], val_type, key)
        return True