from __future__ import annotations

from backend.python.core.local_reasoning_bridge import LocalReasoningBridge


def test_local_reasoning_bridge_runtime_overrides_update_status(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_LOCAL_REASONING_HTTP_ENDPOINT", "http://127.0.0.1:9100")
    monkeypatch.setenv("JARVIS_LOCAL_REASONING_SERVER_API_MODE", "openai_chat")
    bridge = LocalReasoningBridge()

    payload = bridge.set_runtime_overrides(
        updates={
            "endpoint": "http://127.0.0.1:8080",
            "api_mode": "prompt_json",
            "model_hint": "qwen3-14b-q8_0",
            "autostart": True,
        },
        profile_id="reasoning-bridge-local-auto-reasoning-qwen3-14b",
        template_id="reasoning-endpoint-local-auto-reasoning-qwen3-14b",
        replace=True,
    )

    assert payload["status"] == "success"
    assert payload["active_profile_id"] == "reasoning-bridge-local-auto-reasoning-qwen3-14b"
    assert payload["active_template_id"] == "reasoning-endpoint-local-auto-reasoning-qwen3-14b"
    assert payload["endpoint"] == "http://127.0.0.1:8080"
    assert payload["request_url"] == "http://127.0.0.1:8080/generate"
    assert payload["api_mode"] == "prompt_json"
    assert payload["model_hint"] == "qwen3-14b-q8_0"
    assert payload["runtime_overrides"]["endpoint"] == "http://127.0.0.1:8080"


def test_local_reasoning_bridge_clear_overrides_restores_env_config(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_LOCAL_REASONING_HTTP_ENDPOINT", "http://127.0.0.1:9100")
    monkeypatch.setenv("JARVIS_LOCAL_REASONING_SERVER_API_MODE", "openai_chat")
    bridge = LocalReasoningBridge()
    bridge.set_runtime_overrides(
        updates={
            "endpoint": "http://127.0.0.1:8080",
            "api_mode": "prompt_json",
        },
        profile_id="temp-profile",
        replace=True,
    )

    payload = bridge.clear_runtime_overrides()

    assert payload["status"] == "success"
    assert payload["active_profile_id"] == ""
    assert payload["active_template_id"] == ""
    assert payload["runtime_overrides"] == {}
    assert payload["endpoint"] == "http://127.0.0.1:9100"
    assert payload["request_url"] == "http://127.0.0.1:9100/v1/chat/completions"
    assert payload["api_mode"] == "openai_chat"
