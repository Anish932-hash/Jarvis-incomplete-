from __future__ import annotations

import json

from backend.python.speech.local_tts_bridge import LocalNeuralTtsBridge


def _reset_bridge() -> None:
    LocalNeuralTtsBridge._shared = None  # noqa: SLF001


class _FakeResponse:
    def __init__(self, payload: dict[str, object] | None = None, status: int = 200) -> None:
        self.status = status
        self._payload = payload or {"status": "ok", "message": "healthy"}

    def read(self, *_: object) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        _ = (exc_type, exc, tb)
        return None


class _FakeProcess:
    def __init__(self) -> None:
        self.pid = 44210
        self._return_code = None

    def poll(self) -> int | None:
        return self._return_code

    def wait(self, timeout: float | None = None) -> int:
        _ = timeout
        self._return_code = 0
        return 0

    def terminate(self) -> None:
        self._return_code = 0

    def kill(self) -> None:
        self._return_code = 0


def test_local_neural_tts_bridge_status_reports_probe_success(monkeypatch) -> None:
    _reset_bridge()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "http://127.0.0.1:5055/v1/audio/speech")
    monkeypatch.setattr("backend.python.speech.local_tts_bridge.urllib.request.urlopen", lambda request, timeout=0: _FakeResponse())  # type: ignore[arg-type]

    payload = LocalNeuralTtsBridge.shared().status(probe=True)

    assert payload["status"] == "success"
    assert payload["endpoint_configured"] is True
    assert payload["ready"] is True
    assert payload["last_probe_url"].startswith("http://127.0.0.1:5055")


def test_local_neural_tts_bridge_start_launches_managed_process(monkeypatch) -> None:
    _reset_bridge()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "http://127.0.0.1:5055/v1/audio/speech")
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_SERVER_COMMAND", "python -m fake_tts_server")
    monkeypatch.setattr("backend.python.speech.local_tts_bridge.subprocess.Popen", lambda *args, **kwargs: _FakeProcess())
    monkeypatch.setattr("backend.python.speech.local_tts_bridge.urllib.request.urlopen", lambda request, timeout=0: _FakeResponse())  # type: ignore[arg-type]

    payload = LocalNeuralTtsBridge.shared().start(wait_ready=True, timeout_s=3.0, reason="test")

    assert payload["status"] == "success"
    assert payload["managed"] is True
    assert payload["running"] is True
    assert payload["ready"] is True
    assert int(payload["pid"]) == 44210


def test_local_neural_tts_bridge_runtime_overrides_update_status(monkeypatch) -> None:
    _reset_bridge()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "http://127.0.0.1:5055/v1/audio/speech")

    payload = LocalNeuralTtsBridge.shared().set_runtime_overrides(
        updates={
            "endpoint": "http://127.0.0.1:6060/v1/audio/speech",
            "healthcheck_url": "http://127.0.0.1:6060/health",
            "model_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
            "voice": "jarvis",
            "timeout_s": 90,
        },
        profile_id="tts-bridge-orpheus-3b-tts-f16",
        template_id="tts-existing-endpoint-orpheus-3b-tts-f16",
        replace=True,
    )

    assert payload["status"] == "success"
    assert payload["active_profile_id"] == "tts-bridge-orpheus-3b-tts-f16"
    assert payload["active_template_id"] == "tts-existing-endpoint-orpheus-3b-tts-f16"
    assert payload["endpoint"] == "http://127.0.0.1:6060/v1/audio/speech"
    assert payload["healthcheck_url"] == "http://127.0.0.1:6060/health"
    assert payload["runtime_overrides"]["voice"] == "jarvis"
    assert float(payload["runtime_overrides"]["timeout_s"]) == 90.0


def test_local_neural_tts_bridge_clear_overrides_restores_env(monkeypatch) -> None:
    _reset_bridge()
    monkeypatch.setenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "http://127.0.0.1:5055/v1/audio/speech")

    bridge = LocalNeuralTtsBridge.shared()
    bridge.set_runtime_overrides(
        updates={"endpoint": "http://127.0.0.1:6060/v1/audio/speech"},
        profile_id="tts-bridge-orpheus-3b-tts-f16",
        replace=True,
    )

    payload = bridge.clear_runtime_overrides()

    assert payload["status"] == "success"
    assert payload["active_profile_id"] == ""
    assert payload["active_template_id"] == ""
    assert payload["endpoint"] == "http://127.0.0.1:5055/v1/audio/speech"
    assert payload["runtime_overrides"] == {}
