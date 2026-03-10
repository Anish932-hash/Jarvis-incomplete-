from __future__ import annotations

from pathlib import Path

from backend.python.speech.tts_policy import TtsPolicyManager


def _manager(tmp_path: Path, monkeypatch) -> TtsPolicyManager:
    monkeypatch.setenv("JARVIS_TTS_POLICY_STATE_PATH", str(tmp_path / "tts_policy_state.json"))
    TtsPolicyManager._instance = None
    return TtsPolicyManager.shared()


def test_tts_policy_prefers_local_for_privacy_context(tmp_path: Path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    decision = manager.choose_provider(
        requested_provider="auto",
        availability={"local": True, "elevenlabs": True},
        context={"privacy_mode": True, "policy_profile": "privacy"},
    )
    assert decision["status"] == "success"
    assert decision["selected_provider"] == "local"
    assert decision["chain"][0] == "local"


def test_tts_policy_records_failures_and_opens_cooldown(tmp_path: Path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    for _ in range(12):
        manager.record_attempt(provider="elevenlabs", status="error", message="transient_http_503", latency_s=0.3)
    payload = manager.status(limit=24)
    providers = payload.get("providers", {})
    elevenlabs = providers.get("elevenlabs", {})
    assert elevenlabs.get("failure_ema", 0) > 0.5
    assert elevenlabs.get("retry_after_s", 0) > 0


def test_tts_policy_update_route_bias_changes_recommendation(tmp_path: Path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    manager.update(
        {
            "route_bias": {"local": 0.92, "elevenlabs": -0.5},
            "learning_enabled": False,
            "persist_now": True,
        }
    )
    payload = manager.status(limit=24, availability={"local": True, "elevenlabs": True})
    assert payload["status"] == "success"
    assert payload["recommended_provider"] == "local"
    assert payload["route_bias"]["local"] == 0.92
