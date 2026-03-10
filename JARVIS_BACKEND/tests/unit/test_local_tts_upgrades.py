from __future__ import annotations

import backend.python.speech.local_tts as local_tts_module
from backend.python.speech.local_tts import LocalTTS


def _reset_local_tts_state() -> None:
    LocalTTS._active_engine = None  # noqa: SLF001
    LocalTTS._active_win32_speaker = None  # noqa: SLF001
    LocalTTS._active_audio_output = None  # noqa: SLF001
    LocalTTS._active_started_at = 0.0  # noqa: SLF001
    LocalTTS._active_session_id = ""  # noqa: SLF001
    LocalTTS._active_provider = ""  # noqa: SLF001
    LocalTTS._provider_state = {}  # noqa: SLF001
    LocalTTS._history = []  # noqa: SLF001


def test_local_tts_diagnostics_payload_shape() -> None:
    _reset_local_tts_state()

    payload = LocalTTS.diagnostics(history_limit=12)
    assert payload["status"] == "success"
    assert payload["provider"] == "local"
    assert payload["active_provider"] == ""
    providers = payload.get("providers", {})
    assert isinstance(providers, dict)
    assert "pyttsx3" in providers
    assert "win32_sapi" in providers


def test_local_tts_speak_collects_attempt_failure_trace(monkeypatch) -> None:
    _reset_local_tts_state()
    monkeypatch.setattr(LocalTTS, "_resolve_provider_chain", lambda self, preference: [LocalTTS._PROVIDER_PYTTSX3])
    monkeypatch.setattr(
        LocalTTS,
        "_speak_pyttsx3",
        lambda self, text: {"status": "error", "message": "pyttsx3 unavailable in test"},
    )

    payload = LocalTTS().speak("hello world", provider_preference="auto")
    assert payload["status"] == "error"
    attempts = payload.get("attempts", [])
    assert isinstance(attempts, list)
    assert len(attempts) == 1
    assert attempts[0]["provider"] == "pyttsx3"
    assert attempts[0]["status"] == "error"

    diagnostics = LocalTTS.diagnostics(history_limit=10)
    pyttsx3 = diagnostics["providers"]["pyttsx3"]
    assert int(pyttsx3.get("attempts", 0) or 0) == 1
    assert int(pyttsx3.get("failures", 0) or 0) == 1


def test_local_tts_failure_cooldown_visible_in_diagnostics(monkeypatch) -> None:
    _reset_local_tts_state()
    monkeypatch.setenv("JARVIS_LOCAL_TTS_COOLDOWN_THRESHOLD", "0.2")
    monkeypatch.setenv("JARVIS_LOCAL_TTS_COOLDOWN_BASE_S", "10")
    monkeypatch.setattr(LocalTTS, "_resolve_provider_chain", lambda self, preference: [LocalTTS._PROVIDER_PYTTSX3])
    monkeypatch.setattr(
        LocalTTS,
        "_speak_pyttsx3",
        lambda self, text: {"status": "error", "message": "hard failure"},
    )

    first = LocalTTS().speak("hello world", provider_preference="auto")
    second = LocalTTS().speak("hello world", provider_preference="auto")
    assert first["status"] == "error"
    assert second["status"] == "error"

    diagnostics = LocalTTS.diagnostics(history_limit=5)
    pyttsx3 = diagnostics["providers"]["pyttsx3"]
    assert float(pyttsx3.get("failure_ema", 0.0) or 0.0) > 0.0
    assert float(pyttsx3.get("retry_after_s", 0.0) or 0.0) > 0.0


def test_local_tts_diagnostics_expose_neural_runtime_metadata(tmp_path, monkeypatch) -> None:
    _reset_local_tts_state()
    model_path = tmp_path / "Orpheus-3B-TTS.f16.gguf"
    model_path.write_text("stub", encoding="utf-8")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", str(model_path))
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_ENABLED", "1")

    diagnostics = LocalTTS.diagnostics(history_limit=6)
    neural_runtime = diagnostics.get("neural_runtime", {})

    assert isinstance(neural_runtime, dict)
    assert neural_runtime.get("configured") is True
    assert neural_runtime.get("enabled") is True
    assert neural_runtime.get("model_exists") is True
    assert neural_runtime.get("backend") == "llama_cpp"


def test_local_tts_prefers_neural_runtime_when_ready(monkeypatch) -> None:
    _reset_local_tts_state()
    monkeypatch.setattr(
        LocalTTS,
        "_neural_runtime_metadata",
        classmethod(
            lambda cls: {
                "configured": True,
                "enabled": True,
                "ready": True,
                "backend": "llama_cpp",
                "execution_backend": "command",
                "model_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                "model_exists": True,
                "message": "",
                "issues": [],
            }
        ),
    )
    monkeypatch.setattr(
        LocalTTS,
        "_speak_neural_runtime",
        lambda self, text, runtime_metadata=None: {
            "status": "success",
            "text": text,
            "mode": "local-neural-command",
            "execution_backend": "command",
            "session_id": "tts-1",
        },
    )
    monkeypatch.setattr(
        LocalTTS,
        "_speak_pyttsx3",
        lambda self, text: (_ for _ in ()).throw(AssertionError("pyttsx3 should not run before neural runtime")),
    )

    payload = LocalTTS().speak("neural test", provider_preference="auto")
    assert payload["status"] == "success"
    assert payload["provider_used"] == "neural_runtime"
    assert payload["mode"] == "local-neural-command"


def test_local_tts_command_backend_synthesizes_and_retains_output(tmp_path, monkeypatch) -> None:
    _reset_local_tts_state()
    output_dir = tmp_path / "tts_out"
    output_dir.mkdir()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", str(tmp_path / "Orpheus-3B-TTS.f16.gguf"))
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_BACKEND", "command")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_COMMAND", "echo synth > NUL && copy NUL {output_path_q} > NUL")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_OUTPUT_DIR", str(output_dir))
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_KEEP_OUTPUTS", "1")

    model_path = tmp_path / "Orpheus-3B-TTS.f16.gguf"
    model_path.write_text("stub", encoding="utf-8")

    def _fake_run(command: str, shell: bool, capture_output: bool, text: bool, timeout: float, check: bool):  # type: ignore[override]
        _ = (shell, capture_output, text, timeout, check)
        parts = [token.strip().strip("\"") for token in command.replace(">", " ").replace("&", " ").split()]
        target = next(token for token in parts if token.lower().endswith(".wav"))
        with open(target, "wb") as handle:
            handle.write(b"RIFFfake-wave-data")

        class _Result:
            returncode = 0
            stdout = "ok"
            stderr = ""

        return _Result()

    monkeypatch.setattr("backend.python.speech.local_tts.subprocess.run", _fake_run)
    monkeypatch.setattr("backend.python.speech.audio_output.AudioOutput.play_file", lambda self, path: None)

    payload = LocalTTS().speak("command backend", provider_preference="neural_runtime")
    assert payload["status"] == "success"
    assert payload["provider_used"] == "neural_runtime"
    assert payload["execution_backend"] == "command"
    assert payload["artifact_retained"] is True
    assert str(payload.get("output_path", "")).endswith(".wav")


def test_local_tts_diagnostics_marks_bridge_unreachable_when_http_runtime_is_down(monkeypatch) -> None:
    _reset_local_tts_state()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_ENABLED", "1")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_BACKEND", "openai_http")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "http://127.0.0.1:5055/v1/audio/speech")

    class _BridgeStub:
        def status(self, *, probe: bool = False) -> dict:
            _ = probe
            return {
                "status": "success",
                "enabled": True,
                "configured": True,
                "managed": True,
                "autostart": True,
                "endpoint_configured": True,
                "running": True,
                "ready": False,
                "message": "endpoint probe failed",
                "last_error": "connection refused",
            }

    monkeypatch.setattr(
        local_tts_module.LocalNeuralTtsBridge,
        "shared",
        classmethod(lambda cls: _BridgeStub()),
    )

    diagnostics = LocalTTS.diagnostics(history_limit=6)
    neural_runtime = diagnostics.get("neural_runtime", {})

    assert isinstance(neural_runtime, dict)
    assert neural_runtime.get("ready") is False
    assert neural_runtime.get("reason") == "bridge_unreachable"
    assert neural_runtime.get("bridge_ready") is False
    assert isinstance(neural_runtime.get("bridge"), dict)


def test_local_tts_neural_runtime_metadata_prefers_bridge_overrides(monkeypatch) -> None:
    _reset_local_tts_state()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_ENABLED", "1")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", "E:/env/default-model.gguf")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "http://127.0.0.1:5055/v1/audio/speech")

    class _BridgeStub:
        def status(self, *, probe: bool = False) -> dict:
            return {
                "status": "success",
                "enabled": True,
                "configured": True,
                "managed": True,
                "endpoint": "http://127.0.0.1:6060/v1/audio/speech",
                "endpoint_configured": True,
                "healthcheck_url": "http://127.0.0.1:6060/health",
                "ready": True,
                "running": True,
                "active_profile_id": "tts-bridge-orpheus-3b-tts-f16",
                "active_template_id": "tts-http-bridge-orpheus-3b-tts-f16",
                "runtime_overrides": {
                    "model_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                    "backend": "llama_cpp",
                    "execution_backend": "openai_http",
                    "http_endpoint": "http://127.0.0.1:6060/v1/audio/speech",
                    "healthcheck_url": "http://127.0.0.1:6060/health",
                    "http_model": "orpheus-3b-tts",
                    "voice": "jarvis",
                    "output_format": "wav",
                    "timeout_s": 95,
                },
                "message": "override active",
                "last_error": "",
                "probe": probe,
            }

    monkeypatch.setattr(
        local_tts_module.LocalNeuralTtsBridge,
        "shared",
        classmethod(lambda cls: _BridgeStub()),
    )

    metadata = LocalTTS._neural_runtime_metadata()  # noqa: SLF001

    assert metadata["model_path"] == "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"
    assert metadata["http_endpoint"] == "http://127.0.0.1:6060/v1/audio/speech"
    assert metadata["http_model"] == "orpheus-3b-tts"
    assert metadata["voice"] == "jarvis"
    assert metadata["active_profile_id"] == "tts-bridge-orpheus-3b-tts-f16"
    assert metadata["active_template_id"] == "tts-http-bridge-orpheus-3b-tts-f16"
    assert metadata["runtime_overrides"]["voice"] == "jarvis"
