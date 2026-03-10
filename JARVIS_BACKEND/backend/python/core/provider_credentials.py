from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class ProviderCredentialManager:
    """
    Unified credential resolver for cloud providers used by JARVIS.

    Resolution order:
    1) environment variables
    2) plaintext provider credential config file
    3) encrypted key-store (optional)

    The manager never returns raw keys in snapshots; values are redacted/fingerprinted.
    """

    _PLACEHOLDER_RE = re.compile(
        r"(your_|replace_|changeme|example|placeholder|dummy|xxx+|<[^>]+>)",
        flags=re.IGNORECASE,
    )
    _FORMAT_RULES = {
        "groq": re.compile(r"^gsk_[A-Za-z0-9_\-]{16,}$"),
        "elevenlabs": re.compile(r"^[A-Za-z0-9_\-]{20,}$"),
        "nvidia": re.compile(r"^(nvapi[-_])?[A-Za-z0-9._\-]{20,}$", flags=re.IGNORECASE),
    }
    _PROVIDER_DEFS: Dict[str, Dict[str, Any]] = {
        "groq": {
            "env": "GROQ_API_KEY",
            "aliases": ["GROQ_KEY"],
            "keystore_key": "groq.api_key",
            "config_paths": [
                ("providers", "groq", "api_key"),
                ("services", "groq", "api_key"),
                ("groq", "api_key"),
            ],
        },
        "elevenlabs": {
            "env": "ELEVENLABS_API_KEY",
            "aliases": ["ELEVEN_API_KEY"],
            "keystore_key": "elevenlabs.api_key",
            "config_paths": [
                ("providers", "elevenlabs", "api_key"),
                ("services", "elevenlabs", "api_key"),
                ("elevenlabs", "api_key"),
            ],
            "required_env": ["ELEVENLABS_VOICE_ID"],
            "required_env_map": {
                "ELEVENLABS_VOICE_ID": [
                    ("providers", "elevenlabs", "voice_id"),
                    ("services", "elevenlabs", "voice_id"),
                    ("elevenlabs", "voice_id"),
                ]
            },
        },
        "nvidia": {
            "env": "NVIDIA_API_KEY",
            "aliases": ["NIM_API_KEY"],
            "keystore_key": "nvidia.api_key",
            "config_paths": [
                ("providers", "nvidia", "api_key"),
                ("services", "nvidia", "api_key"),
                ("nvidia", "api_key"),
            ],
        },
    }

    def __init__(
        self,
        *,
        config_path: str = "configs/provider_credentials.json",
        key_store_path: str = "data/provider_keys.json",
        master_key_env: str = "JARVIS_MASTER_KEY",
    ) -> None:
        self._config_path = str(config_path or "configs/provider_credentials.json").strip() or "configs/provider_credentials.json"
        self._key_store_path = str(key_store_path or "data/provider_keys.json").strip() or "data/provider_keys.json"
        self._master_key_env = str(master_key_env or "JARVIS_MASTER_KEY").strip() or "JARVIS_MASTER_KEY"
        self._status_cache: Dict[str, Any] = {}

    @classmethod
    def providers(cls) -> list[str]:
        return list(cls._PROVIDER_DEFS.keys())

    def refresh(self, *, overwrite_env: bool = False) -> Dict[str, Any]:
        config_payload = self._load_config_payload()
        keystore_payload = self._load_keystore_payload()
        providers: Dict[str, Dict[str, Any]] = {}

        for provider, definition in self._PROVIDER_DEFS.items():
            env_name = str(definition.get("env", "")).strip()
            aliases = [str(item).strip() for item in definition.get("aliases", []) if str(item).strip()]
            required_env = [str(item).strip() for item in definition.get("required_env", []) if str(item).strip()]

            env_value, env_source = self._first_present_env(env_name=env_name, aliases=aliases)
            config_value = self._extract_from_config(provider=provider, payload=config_payload)
            keystore_value = self._extract_from_keystore(provider=provider, payload=keystore_payload)

            selected_value = ""
            source = "none"
            if env_value:
                selected_value = env_value
                source = env_source
            elif config_value:
                selected_value = config_value
                source = "config"
            elif keystore_value:
                selected_value = keystore_value
                source = "keystore"

            if selected_value and (overwrite_env or not str(os.getenv(env_name, "")).strip()):
                os.environ[env_name] = selected_value
            if selected_value and overwrite_env:
                for alias in aliases:
                    os.environ[alias] = selected_value

            format_valid, format_reason = self._validate_format(provider=provider, value=selected_value)
            requirement_state: Dict[str, Any] = {}
            missing_requirements: list[str] = []
            for requirement_env in required_env:
                requirement_value = str(os.getenv(requirement_env, "")).strip()
                if not requirement_value:
                    requirement_value = self._extract_required_env_from_config(
                        provider=provider,
                        env_name=requirement_env,
                        payload=config_payload,
                    )
                    if requirement_value:
                        os.environ[requirement_env] = requirement_value
                present = bool(requirement_value)
                requirement_state[requirement_env] = {"present": present, "value_preview": self._redact(requirement_value)}
                if not present:
                    missing_requirements.append(requirement_env)

            present = bool(selected_value)
            ready = bool(present and format_valid and not missing_requirements)
            providers[provider] = {
                "provider": provider,
                "env_var": env_name,
                "aliases": aliases,
                "present": present,
                "ready": ready,
                "source": source,
                "format_valid": bool(format_valid),
                "format_reason": str(format_reason),
                "missing_requirements": missing_requirements,
                "requirements": requirement_state,
                "redacted": self._redact(selected_value),
                "fingerprint": self._fingerprint(selected_value),
            }

        ready_count = sum(1 for row in providers.values() if bool(row.get("ready", False)))
        payload = {
            "status": "success",
            "providers": providers,
            "provider_count": len(providers),
            "ready_count": ready_count,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "config_path": self._resolved_path(self._config_path),
            "key_store_path": self._resolved_path(self._key_store_path),
        }
        self._status_cache = payload
        return payload

    def snapshot(self) -> Dict[str, Any]:
        if not self._status_cache:
            return self.refresh(overwrite_env=False)
        return dict(self._status_cache)

    def provider_status(self, provider: str) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        if not clean_provider:
            return {"status": "error", "message": "provider is required"}
        snapshot = self.snapshot()
        providers = snapshot.get("providers", {}) if isinstance(snapshot.get("providers", {}), dict) else {}
        payload = providers.get(clean_provider, {})
        return dict(payload) if isinstance(payload, dict) else {"status": "error", "message": "provider not found"}

    def get_api_key(self, provider: str) -> str:
        clean_provider = str(provider or "").strip().lower()
        definition = self._PROVIDER_DEFS.get(clean_provider, {})
        env_name = str(definition.get("env", "")).strip()
        if env_name:
            return str(os.getenv(env_name, "")).strip()
        return ""

    def is_ready(self, provider: str, *, strict_format: bool = True) -> bool:
        row = self.provider_status(provider)
        if not isinstance(row, dict):
            return False
        if not bool(row.get("present", False)):
            return False
        if strict_format and not bool(row.get("format_valid", False)):
            return False
        missing = row.get("missing_requirements", [])
        if isinstance(missing, list) and missing:
            return False
        return True

    def _load_config_payload(self) -> Dict[str, Any]:
        path = Path(self._resolved_path(self._config_path))
        if not path.exists() or not path.is_file():
            return {}
        try:
            raw = path.read_text(encoding="utf-8")
            payload = json.loads(raw) if raw.strip() else {}
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _load_keystore_payload(self) -> Dict[str, str]:
        master_key = self._decode_master_key(str(os.getenv(self._master_key_env, "")).strip())
        if master_key is None:
            return {}
        path = Path(self._resolved_path(self._key_store_path))
        if not path.exists() or not path.is_file():
            return {}
        try:
            from backend.python.database.key_store import KeyStore
        except Exception:
            return {}

        payload: Dict[str, str] = {}
        try:
            store = KeyStore(path=str(path), master_key=master_key)
        except Exception:
            return {}
        for provider, definition in self._PROVIDER_DEFS.items():
            key_name = str(definition.get("keystore_key", "")).strip()
            if not key_name:
                continue
            try:
                value = store.get(key_name)
            except Exception:
                value = ""
            clean = self._clean_secret(value)
            if clean:
                payload[provider] = clean
        return payload

    def _extract_from_config(self, *, provider: str, payload: Dict[str, Any]) -> str:
        definition = self._PROVIDER_DEFS.get(provider, {})
        for path in definition.get("config_paths", []):
            value = self._dig(payload, path)
            clean = self._clean_secret(value)
            if clean:
                return clean
        return ""

    def _extract_required_env_from_config(
        self,
        *,
        provider: str,
        env_name: str,
        payload: Dict[str, Any],
    ) -> str:
        definition = self._PROVIDER_DEFS.get(provider, {})
        required_map = definition.get("required_env_map", {})
        if not isinstance(required_map, dict):
            return ""
        paths = required_map.get(env_name, [])
        if not isinstance(paths, list):
            return ""
        for path in paths:
            if not isinstance(path, tuple):
                continue
            value = self._dig(payload, path)
            clean = str(value or "").strip()
            if clean:
                return clean
        return ""

    @staticmethod
    def _extract_from_keystore(*, provider: str, payload: Dict[str, str]) -> str:
        if not isinstance(payload, dict):
            return ""
        return str(payload.get(provider, "")).strip()

    def _first_present_env(self, *, env_name: str, aliases: list[str]) -> tuple[str, str]:
        keys = [env_name, *aliases]
        for key in keys:
            if not key:
                continue
            clean = self._clean_secret(os.getenv(key, ""))
            if clean:
                return clean, ("env" if key == env_name else f"env_alias:{key}")
        return "", "none"

    @classmethod
    def _clean_secret(cls, value: object) -> str:
        clean = str(value or "").strip()
        if not clean:
            return ""
        if cls._PLACEHOLDER_RE.search(clean):
            return ""
        return clean

    @classmethod
    def _validate_format(cls, *, provider: str, value: str) -> tuple[bool, str]:
        clean = str(value or "").strip()
        if not clean:
            return (False, "missing")
        if cls._PLACEHOLDER_RE.search(clean):
            return (False, "placeholder")
        rule = cls._FORMAT_RULES.get(provider)
        if rule is None:
            return (True, "no_rule")
        if rule.fullmatch(clean):
            return (True, "ok")
        return (False, "unexpected_format")

    @classmethod
    def _redact(cls, value: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            return ""
        if len(clean) <= 8:
            return "*" * len(clean)
        return f"{clean[:4]}...{clean[-4:]}"

    @classmethod
    def _fingerprint(cls, value: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            return ""
        digest = hashlib.sha256(clean.encode("utf-8")).hexdigest()
        return digest[:16]

    @staticmethod
    def _dig(payload: Dict[str, Any], path: tuple[str, ...]) -> Any:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current

    @staticmethod
    def _decode_master_key(raw: str) -> Optional[bytes]:
        clean = str(raw or "").strip()
        if not clean:
            return None
        if clean.startswith("hex:"):
            clean = clean[4:]
            try:
                decoded = bytes.fromhex(clean)
                return decoded if len(decoded) == 32 else None
            except Exception:
                return None
        if clean.startswith("base64:"):
            clean = clean[7:]
        if len(clean) == 64 and re.fullmatch(r"[0-9a-fA-F]{64}", clean):
            try:
                decoded = bytes.fromhex(clean)
                return decoded if len(decoded) == 32 else None
            except Exception:
                return None
        try:
            decoded = base64.b64decode(clean)
            if len(decoded) == 32:
                return decoded
        except Exception:
            pass
        if len(clean.encode("utf-8")) == 32:
            return clean.encode("utf-8")
        return None

    @staticmethod
    def _resolved_path(raw_path: str) -> str:
        clean = str(raw_path or "").strip()
        if not clean:
            return str(Path.cwd())
        candidate = Path(clean)
        if candidate.is_absolute():
            return str(candidate)
        cwd = Path.cwd().resolve()
        options = [
            cwd / clean,
            cwd.parent / clean,
            cwd.parent.parent / clean,
        ]
        for option in options:
            if option.exists():
                return str(option)
        return str(options[0])
