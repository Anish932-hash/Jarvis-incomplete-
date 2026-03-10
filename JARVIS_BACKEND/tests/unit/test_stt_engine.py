from __future__ import annotations

import time

import numpy as np

from backend.python.speech.stt_engine import STTEngine


def test_transcribe_stream_returns_success_when_vad_capture_and_local_transcription_succeed(monkeypatch) -> None:
    engine = STTEngine(groq_api_key=None, model="whisper-large-v3", local_model_path="stt")
    captured_audio = np.array([0.1, -0.1, 0.2, -0.2], dtype=np.float32)

    monkeypatch.setattr(
        engine,
        "_capture_vad_audio",
        lambda **_kwargs: {
            "status": "success",
            "audio": captured_audio,
            "speech_detected": True,
            "captured_duration_s": 0.45,
        },
    )
    monkeypatch.setattr(
        engine,
        "_transcribe_local",
        lambda _audio: {"status": "success", "text": "hello world", "source": "local", "model": "mock"},
    )

    result = engine.transcribe_stream(max_duration_s=3.0, fallback_to_chunk=False)

    assert result["status"] == "success"
    assert result["text"] == "hello world"
    assert result["mode"] == "stream_vad"
    capture = result.get("capture", {})
    assert capture.get("speech_detected") is True
    assert capture.get("captured_duration_s") == 0.45


def test_transcribe_stream_falls_back_to_chunk_transcribe_when_enabled(monkeypatch) -> None:
    engine = STTEngine(groq_api_key=None, model="whisper-large-v3", local_model_path="stt")

    monkeypatch.setattr(
        engine,
        "_capture_vad_audio",
        lambda **_kwargs: {"status": "error", "message": "No speech detected"},
    )
    monkeypatch.setattr(
        engine,
        "transcribe",
        lambda duration=4.0: {
            "status": "success",
            "text": "fallback transcript",
            "source": "local",
            "model": "chunk",
            "duration_s": duration,
        },
    )

    result = engine.transcribe_stream(max_duration_s=5.0, fallback_to_chunk=True)

    assert result["status"] == "success"
    assert result["text"] == "fallback transcript"
    assert result["mode"] == "chunk_fallback"


def test_transcribe_prefers_cloud_when_local_in_cooldown(monkeypatch) -> None:
    engine = STTEngine(groq_api_key="token", model="whisper-large-v3", local_model_path="stt")
    sample_audio = np.array([0.02, -0.01, 0.03, -0.02], dtype=np.float32)

    monkeypatch.setattr(engine, "_record_audio", lambda _duration: sample_audio)
    monkeypatch.setattr(engine, "_transcribe_local", lambda _audio: {"status": "error", "message": "local unavailable"})
    monkeypatch.setattr(
        engine,
        "_transcribe_groq",
        lambda _audio: {"status": "success", "text": "cloud transcript", "source": "groq", "model": "cloud"},
    )

    # Force local provider cooldown so cloud is selected first.
    engine._provider_state["local"]["cooldown_until_epoch"] = time.time() + 120.0

    result = engine.transcribe(duration=2.0)

    assert result["status"] == "success"
    assert result["source"] == "groq"
    assert result["fallback_from"] == "local"
    attempt_chain = result.get("attempt_chain", [])
    assert any(item.get("provider") == "local" and item.get("status") == "skipped" for item in attempt_chain)
    assert any(item.get("provider") == "groq" and item.get("status") == "success" for item in attempt_chain)


def test_diagnostics_exposes_provider_snapshot_and_attempt_history(monkeypatch) -> None:
    engine = STTEngine(groq_api_key=None, model="whisper-large-v3", local_model_path="stt")
    sample_audio = np.array([0.05, -0.05, 0.02, -0.02], dtype=np.float32)

    monkeypatch.setattr(engine, "_record_audio", lambda _duration: sample_audio)
    monkeypatch.setattr(
        engine,
        "_transcribe_local",
        lambda _audio: {"status": "success", "text": "hello diagnostics", "source": "local", "model": "local-model"},
    )

    result = engine.transcribe(duration=1.0)
    diagnostics = engine.diagnostics(history_limit=10)

    assert result["status"] == "success"
    assert diagnostics["providers"]["local"]["attempts"] >= 1
    assert isinstance(diagnostics.get("attempt_chain_history", []), list)
    assert diagnostics["provider_health"] in {"healthy", "degraded", "critical", "disabled", "unknown"}


def test_provider_state_persistence_round_trip(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "stt_provider_state.json"
    sample_audio = np.array([0.04, -0.02, 0.03, -0.01], dtype=np.float32)

    engine = STTEngine(
        groq_api_key=None,
        model="whisper-large-v3",
        local_model_path="stt",
        provider_state_path=str(state_path),
        provider_state_enabled=True,
        provider_state_persist_interval_s=0.0,
    )
    monkeypatch.setattr(engine, "_record_audio", lambda _duration: sample_audio)
    monkeypatch.setattr(
        engine,
        "_transcribe_local",
        lambda _audio: {"status": "error", "source": "local", "model": "local-model", "message": "model failure"},
    )

    result = engine.transcribe(duration=1.2)
    assert result["status"] == "error"
    assert state_path.exists()

    reloaded = STTEngine(
        groq_api_key=None,
        model="whisper-large-v3",
        local_model_path="stt",
        provider_state_path=str(state_path),
        provider_state_enabled=True,
        provider_state_persist_interval_s=0.0,
    )
    diagnostics = reloaded.diagnostics(history_limit=2)

    assert diagnostics.get("provider_state_persistence", {}).get("loaded") is True
    assert diagnostics["providers"]["local"]["error"] >= 1
    assert diagnostics["providers"]["local"]["attempts"] >= 1
    assert isinstance(diagnostics.get("provider_policies", {}), dict)
    assert diagnostics.get("provider_policies")


def test_provider_policy_cooldown_skips_provider_in_plan(monkeypatch) -> None:
    engine = STTEngine(groq_api_key="token", model="whisper-large-v3", local_model_path="stt")
    sample_audio = np.array([0.02, -0.02, 0.03, -0.01], dtype=np.float32)

    monkeypatch.setattr(engine, "_record_audio", lambda _duration: sample_audio)
    monkeypatch.setattr(
        engine,
        "_transcribe_local",
        lambda _audio: {"status": "success", "text": "local transcript", "source": "local", "model": "local-model"},
    )
    monkeypatch.setattr(
        engine,
        "_transcribe_groq",
        lambda _audio: {"status": "success", "text": "cloud transcript", "source": "groq", "model": "cloud-model"},
    )

    policy_key = engine._provider_policy_key("local", engine.model)  # noqa: SLF001
    row = engine._new_provider_policy_state(provider="local", model_name=engine.model)  # noqa: SLF001
    row["cooldown_until_epoch"] = time.time() + 90.0
    row["outage_score"] = 0.88
    row["failure_streak"] = 5
    row["outage_level"] = "critical"
    engine._provider_policy_state[policy_key] = row  # noqa: SLF001

    result = engine.transcribe(duration=2.0)

    assert result["status"] == "success"
    assert result["source"] == "groq"
    attempt_chain = result.get("attempt_chain", [])
    assert any(
        item.get("provider") == "local" and item.get("status") == "skipped" and item.get("reason") == "policy_cooldown"
        for item in attempt_chain
    )


def test_route_policy_reroutes_blacklisted_local_stt_to_groq(monkeypatch) -> None:
    engine = STTEngine(
        groq_api_key="token",
        model="whisper-large-v3",
        local_model_path="stt",
        route_policy_provider=lambda: {
            "status": "success",
            "task": "stt",
            "selected_provider": "local",
            "recommended_provider": "groq",
            "route_adjusted": True,
            "route_blocked": False,
            "local_route_viable": False,
            "blacklisted": True,
            "fallback_candidates": ["groq"],
            "reason_code": "local_launch_template_blacklisted",
        },
    )
    sample_audio = np.array([0.01, -0.01, 0.02, -0.02], dtype=np.float32)

    monkeypatch.setattr(engine, "_record_audio", lambda _duration: sample_audio)
    monkeypatch.setattr(
        engine,
        "_transcribe_local",
        lambda _audio: (_ for _ in ()).throw(AssertionError("local STT should be skipped by route policy")),
    )
    monkeypatch.setattr(
        engine,
        "_transcribe_groq",
        lambda _audio: {"status": "success", "text": "cloud transcript", "source": "groq", "model": "cloud-model"},
    )

    result = engine.transcribe(duration=1.5)

    assert result["status"] == "success"
    assert result["source"] == "groq"
    attempt_chain = result.get("attempt_chain", [])
    assert any(
        item.get("provider") == "local" and item.get("status") == "skipped" and item.get("reason") == "route_policy_blacklisted"
        for item in attempt_chain
    )
    diagnostics = engine.diagnostics(history_limit=8)
    assert diagnostics["route_policy"]["recommended_provider"] == "groq"
    assert diagnostics["route_policy_plan_skips"] >= 1


def test_route_policy_blocks_stt_when_no_safe_reroute_exists(monkeypatch) -> None:
    engine = STTEngine(
        groq_api_key="token",
        model="whisper-large-v3",
        local_model_path="stt",
        route_policy_provider=lambda: {
            "status": "success",
            "task": "stt",
            "selected_provider": "local",
            "recommended_provider": "",
            "route_adjusted": False,
            "route_blocked": True,
            "local_route_viable": False,
            "privacy_mode": True,
            "fallback_candidates": [],
            "reason_code": "local_launch_template_blacklisted",
        },
    )
    sample_audio = np.array([0.01, -0.01, 0.02, -0.02], dtype=np.float32)

    monkeypatch.setattr(engine, "_record_audio", lambda _duration: sample_audio)
    monkeypatch.setattr(
        engine,
        "_transcribe_local",
        lambda _audio: (_ for _ in ()).throw(AssertionError("local STT should not run when route is blocked")),
    )
    monkeypatch.setattr(
        engine,
        "_transcribe_groq",
        lambda _audio: (_ for _ in ()).throw(AssertionError("groq STT should not run without a safe reroute")),
    )

    result = engine.transcribe(duration=1.5)

    assert result["status"] == "error"
    attempt_chain = result.get("attempt_chain", [])
    assert any(
        item.get("provider") == "local" and item.get("status") == "skipped" and item.get("reason") == "route_policy_blocked"
        for item in attempt_chain
    )
    assert any(
        item.get("provider") == "groq" and item.get("status") == "skipped" and item.get("reason") == "route_policy_no_safe_reroute"
        for item in attempt_chain
    )
    diagnostics = engine.diagnostics(history_limit=8)
    assert diagnostics["route_policy_blocks"] >= 1


def test_policy_status_exposes_controlled_provider_state(tmp_path) -> None:
    engine = STTEngine(
        groq_api_key="token",
        model="whisper-large-v3",
        local_model_path="stt",
        provider_state_enabled=True,
        provider_state_path=str(tmp_path / "stt_policy_state.json"),
        provider_state_persist_interval_s=0.0,
    )

    status = engine.policy_status(history_limit=45)

    assert status["status"] == "success"
    assert status["history_limit"] == 45
    assert "providers" in status and isinstance(status["providers"], dict)
    assert "local" in status["providers"]
    assert status["provider_failure_streak_threshold"] >= 1
    assert status["policy_base_cooldown_s"] >= 0.5


def test_update_policy_applies_threshold_and_provider_toggles(tmp_path) -> None:
    state_path = tmp_path / "stt_provider_state.json"
    engine = STTEngine(
        groq_api_key="token",
        model="whisper-large-v3",
        local_model_path="stt",
        provider_state_enabled=True,
        provider_state_path=str(state_path),
        provider_state_persist_interval_s=0.0,
    )

    updated = engine.update_policy(
        {
            "provider_failure_streak_threshold": 4,
            "provider_cooldown_s": 12.5,
            "policy_failure_streak_threshold": 6,
            "provider_state_enabled": True,
            "providers": {"groq": {"enabled": False}},
            "reset_runtime_history": True,
            "persist_now": True,
            "history_limit": 70,
        }
    )

    assert updated["status"] == "success"
    assert updated["updated"] is True
    assert updated["changed"]["provider_failure_streak_threshold"] == 4
    assert updated["changed"]["provider_cooldown_s"] == 12.5
    assert updated["changed"]["providers"]["groq.enabled"] is False
    policy = updated["policy"]
    assert policy["provider_failure_streak_threshold"] == 4
    assert policy["providers"]["groq"]["enabled"] is False
    assert isinstance(policy.get("attempt_chain_history"), list)
    assert state_path.exists()
