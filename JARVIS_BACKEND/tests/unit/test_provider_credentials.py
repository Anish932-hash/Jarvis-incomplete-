from __future__ import annotations

import json
import os
from pathlib import Path

from backend.python.core.provider_credentials import ProviderCredentialManager


def test_update_provider_credentials_writes_config_and_required_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    api_key = "elevenlabskey1234567890abcd"
    voice_id = "voice_primary_01"

    payload = manager.update_provider_credentials(
        provider="elevenlabs",
        api_key=api_key,
        requirements={"ELEVENLABS_VOICE_ID": voice_id},
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=True,
    )

    assert payload["status"] == "success"
    config_path = tmp_path / "configs" / "provider_credentials.json"
    config_payload = json.loads(config_path.read_text(encoding="utf-8"))

    assert config_payload["providers"]["elevenlabs"]["api_key"] == api_key
    assert config_payload["services"]["elevenlabs"]["api_key"] == api_key
    assert config_payload["providers"]["elevenlabs"]["voice_id"] == voice_id
    assert config_payload["services"]["elevenlabs"]["voice_id"] == voice_id
    assert os.environ["ELEVENLABS_API_KEY"] == api_key
    assert os.environ["ELEVENLABS_VOICE_ID"] == voice_id

    snapshot = manager.snapshot()
    assert snapshot["providers"]["elevenlabs"]["ready"] is True
    assert snapshot["providers"]["elevenlabs"]["present"] is True


def test_update_provider_credentials_can_store_only_in_encrypted_keystore(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("JARVIS_MASTER_KEY", "hex:" + ("11" * 32))

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    api_key = "gsk_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"

    payload = manager.update_provider_credentials(
        provider="groq",
        api_key=api_key,
        persist_plaintext=False,
        persist_encrypted=True,
        overwrite_env=True,
    )

    assert payload["status"] == "success"
    config_path = tmp_path / "configs" / "provider_credentials.json"
    config_payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert "api_key" not in config_payload.get("providers", {}).get("groq", {})

    key_store_path = tmp_path / "data" / "provider_keys.json"
    raw_keystore = key_store_path.read_text(encoding="utf-8")
    assert api_key not in raw_keystore

    refreshed = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    ).refresh(overwrite_env=False)
    assert refreshed["providers"]["groq"]["ready"] is True
    assert refreshed["providers"]["groq"]["present"] is True


def test_update_provider_credentials_supports_huggingface_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    token = "hf_" + ("A1b2C3d4E5f6G7h8" * 2)

    payload = manager.update_provider_credentials(
        provider="huggingface",
        api_key=token,
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=True,
    )

    assert payload["status"] == "success"
    config_path = tmp_path / "configs" / "provider_credentials.json"
    config_payload = json.loads(config_path.read_text(encoding="utf-8"))

    assert config_payload["providers"]["huggingface"]["token"] == token
    assert config_payload["services"]["huggingface"]["token"] == token
    assert os.environ["HUGGINGFACE_HUB_TOKEN"] == token
    assert manager.get_api_key("huggingface") == token

    snapshot = manager.snapshot()
    assert snapshot["providers"]["huggingface"]["ready"] is True
    assert snapshot["providers"]["huggingface"]["present"] is True


def test_get_api_key_falls_back_to_plaintext_config_when_env_is_cleared(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("GROQ_API_KEY", "GROQ_KEY"):
        monkeypatch.delenv(env_name, raising=False)

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    api_key = "gsk_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"

    payload = manager.update_provider_credentials(
        provider="groq",
        api_key=api_key,
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=False,
    )

    assert payload["status"] == "success"
    for env_name in ("GROQ_API_KEY", "GROQ_KEY"):
        monkeypatch.delenv(env_name, raising=False)

    reloaded = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    assert reloaded.get_api_key("groq") == api_key
