from __future__ import annotations

import json
import urllib.request
from pathlib import Path

from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.core.provider_verifier import ProviderCredentialVerifier


class _FakeResponse:
    def __init__(
        self,
        *,
        final_url: str,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        status: int = 200,
    ) -> None:
        self._final_url = final_url
        self.headers = headers or {}
        self._body = body
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def geturl(self) -> str:
        return self._final_url

    def read(self) -> bytes:
        return self._body


def test_provider_verifier_huggingface_checks_identity_and_repo_access(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("HUGGINGFACE_HUB_TOKEN", "HF_TOKEN", "HUGGINGFACE_TOKEN"):
        monkeypatch.delenv(env_name, raising=False)

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    token = "hf_" + ("A1b2C3d4E5f6G7h8" * 2)
    manager.update_provider_credentials(
        provider="huggingface",
        api_key=token,
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=True,
    )
    verifier = ProviderCredentialVerifier(manager, history_path="data/provider_verification_history.json")

    whoami_body = json.dumps({"name": "thecy", "type": "user", "orgs": [{"name": "meta-llama"}]}).encode("utf-8")
    repo_body = json.dumps({"sha": "abc123", "gated": True, "private": False, "siblings": []}).encode("utf-8")

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        assert request.get_header("Authorization") == f"Bearer {token}"
        if request.full_url.endswith("/api/whoami-v2"):
            return _FakeResponse(
                final_url="https://huggingface.co/api/whoami-v2",
                headers={"Content-Type": "application/json"},
                body=whoami_body,
                status=200,
            )
        assert request.full_url.endswith("/api/models/meta-llama/Llama-3.1-8B-Instruct")
        return _FakeResponse(
            final_url="https://huggingface.co/api/models/meta-llama/Llama-3.1-8B-Instruct",
            headers={"Content-Type": "application/json"},
            body=repo_body,
            status=200,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = verifier.verify(
        provider="huggingface",
        repo_items=[{"key": "llama", "source_ref": "meta-llama/Llama-3.1-8B-Instruct"}],
        force_refresh=True,
    )

    assert payload["status"] == "success"
    assert payload["verified"] is True
    assert payload["identity"]["name"] == "thecy"
    assert payload["repo_access_ok"] is True
    assert payload["repo_access"][0]["repo_id"] == "meta-llama/Llama-3.1-8B-Instruct"
    latest_map = verifier.latest_map(["huggingface"])
    assert latest_map["huggingface"]["verified"] is True


def test_provider_verifier_elevenlabs_warns_about_missing_voice_id(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("ELEVENLABS_API_KEY", "ELEVEN_API_KEY", "ELEVENLABS_VOICE_ID"):
        monkeypatch.delenv(env_name, raising=False)

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    api_key = "elevenlabskey1234567890abcd"
    manager.update_provider_credentials(
        provider="elevenlabs",
        api_key=api_key,
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=True,
    )
    monkeypatch.delenv("ELEVENLABS_VOICE_ID", raising=False)
    verifier = ProviderCredentialVerifier(manager, history_path="data/provider_verification_history.json")

    body = json.dumps({"user_id": "user_123", "subscription": {"tier": "creator"}}).encode("utf-8")

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        header_value = request.get_header("xi-api-key") or request.get_header("Xi-api-key") or request.headers.get("Xi-api-key")
        assert header_value == api_key
        return _FakeResponse(
            final_url="https://api.elevenlabs.io/v1/user",
            headers={"Content-Type": "application/json"},
            body=body,
            status=200,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = verifier.verify(provider="elevenlabs", force_refresh=True)

    assert payload["status"] == "success"
    assert payload["verified"] is True
    assert payload["identity"]["user_id"] == "user_123"
    assert any("missing" in warning.lower() for warning in payload["warnings"])
    assert payload["provider_status"]["ready"] is False


def test_provider_verifier_nvidia_uses_minimal_inference_probe(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("NVIDIA_API_KEY", "NIM_API_KEY"):
        monkeypatch.delenv(env_name, raising=False)

    manager = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    api_key = "nvapi-" + ("A1b2C3d4E5f6G7h8" * 2)
    manager.update_provider_credentials(
        provider="nvidia",
        api_key=api_key,
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=True,
    )
    verifier = ProviderCredentialVerifier(manager, history_path="data/provider_verification_history.json")

    body = json.dumps(
        {
            "id": "cmpl-123",
            "model": "meta/llama-3.1-8b-instruct",
            "choices": [{"message": {"role": "assistant", "content": "pong"}}],
            "usage": {"total_tokens": 2},
        }
    ).encode("utf-8")

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        assert request.get_header("Authorization") == f"Bearer {api_key}"
        payload = json.loads(request.data.decode("utf-8"))
        assert payload["model"] == "meta/llama-3.1-8b-instruct"
        assert payload["max_tokens"] == 1
        return _FakeResponse(
            final_url="https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Content-Type": "application/json"},
            body=body,
            status=200,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = verifier.verify(provider="nvidia", force_refresh=True)

    assert payload["status"] == "success"
    assert payload["verified"] is True
    assert payload["identity"]["model"] == "meta/llama-3.1-8b-instruct"
    assert any("minimal live model invocation" in warning.lower() for warning in payload["warnings"])
