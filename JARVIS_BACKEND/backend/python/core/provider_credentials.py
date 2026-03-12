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
        "huggingface": re.compile(r"^hf_[A-Za-z0-9]{16,}$"),
    }
    _PROVIDER_DEFS: Dict[str, Dict[str, Any]] = {
        "groq": {
            "env": "GROQ_API_KEY",
            "aliases": ["GROQ_KEY"],
            "keystore_key": "groq.api_key",
            "credential_label": "API Key",
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
            "credential_label": "API Key",
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
            "credential_label": "API Key",
            "config_paths": [
                ("providers", "nvidia", "api_key"),
                ("services", "nvidia", "api_key"),
                ("nvidia", "api_key"),
            ],
        },
        "huggingface": {
            "env": "HUGGINGFACE_HUB_TOKEN",
            "aliases": ["HF_TOKEN", "HUGGINGFACE_TOKEN"],
            "keystore_key": "huggingface.token",
            "credential_label": "Access Token",
            "config_paths": [
                ("providers", "huggingface", "token"),
                ("services", "huggingface", "token"),
                ("huggingface", "token"),
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

    @classmethod
    def provider_catalog(cls) -> Dict[str, Dict[str, Any]]:
        catalog: Dict[str, Dict[str, Any]] = {}
        for provider, definition in cls._PROVIDER_DEFS.items():
            required_map = definition.get("required_env_map", {})
            required_paths: Dict[str, list[str]] = {}
            if isinstance(required_map, dict):
                for env_name, paths in required_map.items():
                    clean_env = str(env_name or "").strip()
                    if not clean_env:
                        continue
                    required_paths[clean_env] = [
                        ".".join(path)
                        for path in paths
                        if isinstance(path, tuple) and path
                    ]
            catalog[provider] = {
                "provider": provider,
                "env": str(definition.get("env", "")).strip(),
                "aliases": [str(item).strip() for item in definition.get("aliases", []) if str(item).strip()],
                "credential_label": str(definition.get("credential_label", "API Key") or "API Key").strip() or "API Key",
                "required_env": [str(item).strip() for item in definition.get("required_env", []) if str(item).strip()],
                "config_paths": [
                    ".".join(path)
                    for path in definition.get("config_paths", [])
                    if isinstance(path, tuple) and path
                ],
                "required_env_map": required_paths,
            }
        return catalog

    def storage_capabilities(self) -> Dict[str, Any]:
        config_path = Path(self._resolved_path(self._config_path))
        key_store_path = Path(self._resolved_path(self._key_store_path))
        master_key = self._decode_master_key(str(os.getenv(self._master_key_env, "")).strip())
        return {
            "status": "success",
            "config_path": str(config_path),
            "key_store_path": str(key_store_path),
            "master_key_env": self._master_key_env,
            "master_key_configured": master_key is not None,
            "keystore_enabled": master_key is not None,
            "config_parent": str(config_path.parent),
            "key_store_parent": str(key_store_path.parent),
        }

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
            "storage": self.storage_capabilities(),
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
        aliases = [str(item).strip() for item in definition.get("aliases", []) if str(item).strip()]
        if env_name:
            env_value, _ = self._first_present_env(env_name=env_name, aliases=aliases)
            if env_value:
                return env_value
        config_payload = self._load_config_payload()
        config_value = self._extract_from_config(provider=clean_provider, payload=config_payload)
        if config_value:
            return config_value
        keystore_payload = self._load_keystore_payload()
        return self._extract_from_keystore(provider=clean_provider, payload=keystore_payload)

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

    def update_provider_credentials(
        self,
        *,
        provider: str,
        api_key: Optional[str] = None,
        requirements: Optional[Dict[str, Any]] = None,
        persist_plaintext: bool = True,
        persist_encrypted: Optional[bool] = None,
        overwrite_env: bool = True,
        clear_api_key: bool = False,
    ) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        definition = self._PROVIDER_DEFS.get(clean_provider)
        if not isinstance(definition, dict):
            return {"status": "error", "message": f"unsupported provider: {clean_provider or provider}"}

        raw_api_key = "" if api_key is None else str(api_key or "").strip()
        api_key_provided = api_key is not None
        clean_api_key = self._clean_secret(raw_api_key)
        if api_key_provided and raw_api_key and not clean_api_key and not clear_api_key:
            return {"status": "error", "message": "api_key looks like a placeholder or invalid empty value"}
        if clean_api_key:
            format_valid, format_reason = self._validate_format(provider=clean_provider, value=clean_api_key)
            if not format_valid:
                return {"status": "error", "message": f"api_key failed validation: {format_reason}"}

        requirement_inputs: Dict[str, str] = {}
        if isinstance(requirements, dict):
            for key, value in requirements.items():
                clean_key = str(key or "").strip().upper()
                clean_value = str(value or "").strip()
                if clean_key and clean_value:
                    requirement_inputs[clean_key] = clean_value

        required_env = [str(item).strip() for item in definition.get("required_env", []) if str(item).strip()]
        requirement_updates = {env_name: requirement_inputs.get(env_name, "") for env_name in required_env if requirement_inputs.get(env_name, "")}

        if not clean_api_key and not requirement_updates and not clear_api_key:
            return {"status": "error", "message": "nothing to update"}

        storage = self.storage_capabilities()
        master_key_configured = bool(storage.get("master_key_configured", False))
        use_keystore = master_key_configured if persist_encrypted is None else bool(persist_encrypted)
        warnings: list[str] = []
        if bool(persist_encrypted) and not master_key_configured:
            warnings.append(
                f"Encrypted keystore mirroring skipped because {self._master_key_env} is not configured."
            )
        if (clean_api_key or clear_api_key) and not bool(persist_plaintext) and not use_keystore:
            return {
                "status": "error",
                "message": "no writable destination selected for api_key; enable plaintext persistence or configure JARVIS_MASTER_KEY",
            }

        config_payload = self._load_config_payload()
        if not isinstance(config_payload, dict):
            config_payload = {}

        updated_fields: list[str] = []
        if clean_api_key or clear_api_key:
            for path in definition.get("config_paths", []):
                if not isinstance(path, tuple) or not path:
                    continue
                if persist_plaintext and clean_api_key:
                    self._set_nested_value(config_payload, path, clean_api_key)
                elif clear_api_key:
                    self._set_nested_value(config_payload, path, None)
            updated_fields.append("api_key")

        required_map = definition.get("required_env_map", {})
        if isinstance(required_map, dict):
            for env_name, env_value in requirement_updates.items():
                paths = required_map.get(env_name, [])
                for path in paths:
                    if not isinstance(path, tuple) or not path:
                        continue
                    self._set_nested_value(config_payload, path, env_value)
                updated_fields.append(env_name)

        if updated_fields:
            config_payload.setdefault("_meta", {})
            if isinstance(config_payload.get("_meta"), dict):
                config_payload["_meta"]["provider_credentials_updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write_config_payload(config_payload)

        keystore_written = False
        if clean_api_key or clear_api_key:
            try:
                keystore_written = self._write_keystore_secret(
                    provider=clean_provider,
                    secret_value=clean_api_key,
                    delete=bool(clear_api_key and not clean_api_key),
                ) if use_keystore else False
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Encrypted keystore write failed: {exc}")

        env_name = str(definition.get("env", "")).strip()
        aliases = [str(item).strip() for item in definition.get("aliases", []) if str(item).strip()]
        if overwrite_env:
            if clean_api_key:
                if env_name:
                    os.environ[env_name] = clean_api_key
                for alias in aliases:
                    os.environ[alias] = clean_api_key
            elif clear_api_key:
                if env_name:
                    os.environ.pop(env_name, None)
                for alias in aliases:
                    os.environ.pop(alias, None)
            for env_var, env_value in requirement_updates.items():
                os.environ[env_var] = env_value

        snapshot = self.refresh(overwrite_env=overwrite_env)
        provider_row = snapshot.get("providers", {}).get(clean_provider, {}) if isinstance(snapshot, dict) else {}
        return {
            "status": "success",
            "provider": clean_provider,
            "updated_fields": updated_fields,
            "persist_plaintext": bool(persist_plaintext),
            "persist_encrypted": bool(keystore_written),
            "overwrite_env": bool(overwrite_env),
            "storage": self.storage_capabilities(),
            "warnings": warnings,
            "provider_status": dict(provider_row) if isinstance(provider_row, dict) else {},
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
        }

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
    def _set_nested_value(payload: Dict[str, Any], path: tuple[str, ...], value: Optional[str]) -> None:
        current: Dict[str, Any] = payload
        for key in path[:-1]:
            child = current.get(key)
            if not isinstance(child, dict):
                child = {}
                current[key] = child
            current = child
        if value is None:
            current.pop(path[-1], None)
            return
        current[path[-1]] = value

    def _write_config_payload(self, payload: Dict[str, Any]) -> None:
        path = Path(self._resolved_path(self._config_path))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def _write_keystore_secret(self, *, provider: str, secret_value: str, delete: bool = False) -> bool:
        master_key = self._decode_master_key(str(os.getenv(self._master_key_env, "")).strip())
        if master_key is None:
            return False
        try:
            from backend.python.database.key_store import KeyStore
        except Exception:
            return False
        definition = self._PROVIDER_DEFS.get(provider, {})
        key_name = str(definition.get("keystore_key", "")).strip()
        if not key_name:
            return False
        store = KeyStore(path=self._resolved_path(self._key_store_path), master_key=master_key)
        if delete:
            store.delete(key_name)
            return True
        if not secret_value:
            return False
        store.set(key_name, secret_value)
        return True

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
