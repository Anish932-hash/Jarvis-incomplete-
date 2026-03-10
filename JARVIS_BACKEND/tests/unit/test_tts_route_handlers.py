from __future__ import annotations

from backend.python.speech import elevenlabs_tts, local_tts, tts_policy
from backend.python.tools import route_handlers


class _PolicyStub:
    def choose_provider(self, *, requested_provider: str, availability: dict | None = None, context: dict | None = None) -> dict:
        _ = (requested_provider, availability, context)
        return {
            "status": "success",
            "selected_provider": "elevenlabs",
            "chain": ["elevenlabs", "local"],
            "reason": "adaptive_score",
            "scores": {"elevenlabs": 1.0, "local": 0.6},
        }

    def status(self, *, limit: int, context: dict | None = None, availability: dict | None = None) -> dict:
        _ = (limit, context, availability)
        return {
            "status": "success",
            "recommended_provider": "elevenlabs",
            "recommended_chain": ["elevenlabs", "local"],
        }

    def record_attempt(self, **_: object) -> dict:
        return {"status": "success"}


class _LocalFirstPolicyStub(_PolicyStub):
    def choose_provider(self, *, requested_provider: str, availability: dict | None = None, context: dict | None = None) -> dict:
        _ = (requested_provider, availability, context)
        return {
            "status": "success",
            "selected_provider": "local",
            "chain": ["local", "elevenlabs"],
            "reason": "adaptive_score",
            "scores": {"local": 1.0, "elevenlabs": 0.5},
        }


def test_tts_speak_reorders_auto_chain_from_model_route(monkeypatch) -> None:
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {"task": "tts", "provider": "local", "model": "local-auto-tts-orpheus-3b-tts-f16"},
        "route_bundle": {"status": "success", "selected_local_paths": {"tts": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"}},
        "provider_credentials": {"status": "success", "providers": {"elevenlabs": {"ready": True}}},
    })
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _PolicyStub()))
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "speak",
        lambda self, text, provider_preference="auto": {
            "status": "success",
            "text": text,
            "mode": "local-pyttsx3",
            "provider_used": "pyttsx3",
            "provider_preference": provider_preference,
        },
    )
    monkeypatch.setattr(
        elevenlabs_tts.ElevenLabsTTS,
        "speak",
        lambda self, text: (_ for _ in ()).throw(AssertionError("elevenlabs should not run before local")),
    )

    payload = route_handlers._tts_speak({"text": "hello there", "privacy_mode": True, "requires_offline": True})

    assert payload["status"] == "success"
    assert payload["policy"]["decision"]["selected_provider"] == "local"
    assert payload["policy"]["decision"]["chain"] == ["local"]
    assert payload["model_route"]["selected_provider"] == "local"
    assert payload["selected_local_model_path"] == "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"


def test_tts_speak_skips_blacklisted_local_route_when_remote_ready(monkeypatch) -> None:
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {
            "task": "tts",
            "provider": "local",
            "model": "local-auto-tts-orpheus-3b-tts-f16",
            "route_blocked": True,
            "route_warning": "task:tts:local_launch_template_blacklisted:no_safe_reroute",
            "route_policy": {
                "matched": True,
                "blacklisted": True,
                "local_route_viable": False,
                "autonomy_safe": False,
                "recommended_provider": "elevenlabs",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Local TTS route is blacklisted by launch health policy.",
                "cloud_fallback_candidates": ["elevenlabs"],
            },
        },
        "route_bundle": {"status": "success"},
        "provider_credentials": {"status": "success", "providers": {"elevenlabs": {"ready": True}}},
    })
    monkeypatch.setenv("ELEVENLABS_API_KEY", "test-key")
    monkeypatch.setenv("ELEVENLABS_VOICE_ID", "voice-id")
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _LocalFirstPolicyStub()))
    monkeypatch.setattr(
        elevenlabs_tts.ElevenLabsTTS,
        "speak",
        lambda self, text: {"status": "success", "text": text, "mode": "elevenlabs", "provider": "elevenlabs"},
    )
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "speak",
        lambda self, text, provider_preference="auto": (_ for _ in ()).throw(AssertionError("local route should be skipped")),
    )

    payload = route_handlers._tts_speak({"text": "hello there", "provider": "auto"})

    assert payload["status"] == "success"
    assert payload["policy"]["decision"]["selected_provider"] == "elevenlabs"
    assert payload["policy"]["decision"]["chain"] == ["elevenlabs"]
    assert "route_policy_blocked" in str(payload["policy"]["decision"]["reason"])
    assert payload["model_route"]["route_blocked"] is True
    assert payload["model_route"]["route_policy"]["blacklisted"] is True


def test_tts_diagnostics_expose_route_bundle_and_provider_credentials(monkeypatch) -> None:
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {
            "task": "tts",
            "provider": "local",
            "model": "local-auto-tts-orpheus-3b-tts-f16",
            "route_policy": {
                "matched": True,
                "local_route_viable": True,
                "autonomy_safe": True,
                "recommended_provider": "local",
            },
        },
        "route_bundle": {
            "status": "success",
            "stack_name": "voice",
            "selected_local_paths": {"tts": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"},
            "launch_policy_summary": {"policy_monitored_task_count": 1, "blocked_task_count": 0},
        },
        "provider_credentials": {
            "status": "success",
            "providers": {
                "groq": {"ready": True},
                "nvidia": {"ready": True},
                "elevenlabs": {"ready": True},
            },
        },
    })
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _PolicyStub()))
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "diagnostics",
        classmethod(lambda cls, history_limit=24: {"status": "success", "provider": "local", "providers": {"pyttsx3": {"ready": True}}}),
    )
    monkeypatch.setattr(
        elevenlabs_tts.ElevenLabsTTS,
        "diagnostics",
        classmethod(lambda cls: {"status": "success", "provider": "elevenlabs", "ready": True}),
    )

    payload = route_handlers._tts_diagnostics({"history_limit": 12, "privacy_mode": True, "requires_offline": True})

    assert payload["status"] == "success"
    assert payload["recommended_provider"] == "local"
    assert payload["model_route"]["selected_provider"] == "local"
    assert payload["route_bundle"]["selected_local_paths"]["tts"].endswith("Orpheus-3B-TTS.f16.gguf")
    assert payload["provider_credentials"]["providers"]["groq"]["ready"] is True
    assert payload["model_route"]["route_policy"]["matched"] is True
    assert payload["route_policy_summary"]["policy_monitored_task_count"] == 1


def test_tts_speak_accepts_direct_neural_provider(monkeypatch) -> None:
    seen: dict[str, str] = {}
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {"task": "tts", "provider": "local", "model": "local-auto-tts-orpheus-3b-tts-f16"},
        "route_bundle": {"status": "success", "selected_local_paths": {"tts": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"}},
        "provider_credentials": {"status": "success", "providers": {}},
    })
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _PolicyStub()))
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "diagnostics",
        classmethod(lambda cls, history_limit=24: {"status": "success", "provider": "local", "providers": {"neural_runtime": {"ready": True, "enabled": True}}}),
    )

    def _fake_local_speak(self, text, provider_preference="auto"):
        seen["provider_preference"] = provider_preference
        return {"status": "success", "text": text, "mode": "local-neural-command", "provider_used": "neural_runtime"}

    monkeypatch.setattr(local_tts.LocalTTS, "speak", _fake_local_speak)

    payload = route_handlers._tts_speak({"text": "hello there", "provider": "neural_runtime"})
    assert payload["status"] == "success"
    assert seen["provider_preference"] == "neural_runtime"
    assert payload["requested_provider"] == "neural_runtime"


def test_tts_diagnostics_treat_neural_runtime_as_local_ready(monkeypatch) -> None:
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {"task": "tts", "provider": "local", "model": "local-auto-tts-orpheus-3b-tts-f16"},
        "route_bundle": {"status": "success", "selected_local_paths": {"tts": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"}},
        "provider_credentials": {"status": "success", "providers": {}},
    })
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _PolicyStub()))
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "diagnostics",
        classmethod(
            lambda cls, history_limit=24: {
                "status": "success",
                "provider": "local",
                "providers": {
                    "pyttsx3": {"ready": False, "enabled": True, "failure_ema": 0.8},
                    "neural_runtime": {
                        "ready": True,
                        "enabled": True,
                        "configured": True,
                        "message": "",
                        "backend": "llama_cpp",
                    },
                },
            }
        ),
    )
    monkeypatch.setattr(
        elevenlabs_tts.ElevenLabsTTS,
        "diagnostics",
        classmethod(lambda cls: {"status": "success", "provider": "elevenlabs", "ready": True}),
    )

    payload = route_handlers._tts_diagnostics({"history_limit": 12, "requires_offline": True})
    assert payload["status"] == "success"
    assert payload["recommended_provider"] == "local"
    assert not any(hint.get("code") == "local_tts_cooldown_active" for hint in payload["remediation_hints"])


def test_tts_diagnostics_emit_route_policy_hint_for_blocked_local_route(monkeypatch) -> None:
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {
            "task": "tts",
            "provider": "local",
            "model": "local-auto-tts-orpheus-3b-tts-f16",
            "route_blocked": True,
            "route_warning": "task:tts:local_launch_template_blacklisted:no_safe_reroute",
            "route_policy": {
                "matched": True,
                "blacklisted": True,
                "local_route_viable": False,
                "recommended_provider": "elevenlabs",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Local TTS route is blacklisted by launch health policy.",
                "cloud_fallback_candidates": ["elevenlabs"],
            },
        },
        "route_bundle": {"status": "success", "launch_policy_summary": {"blocked_task_count": 1}},
        "provider_credentials": {"status": "success", "providers": {"elevenlabs": {"ready": True}}},
    })
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _PolicyStub()))
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "diagnostics",
        classmethod(lambda cls, history_limit=24: {"status": "success", "provider": "local", "providers": {"pyttsx3": {"ready": True}}}),
    )
    monkeypatch.setattr(
        elevenlabs_tts.ElevenLabsTTS,
        "diagnostics",
        classmethod(lambda cls: {"status": "success", "provider": "elevenlabs", "ready": True}),
    )
    monkeypatch.setenv("ELEVENLABS_API_KEY", "test-key")
    monkeypatch.setenv("ELEVENLABS_VOICE_ID", "voice-id")

    payload = route_handlers._tts_diagnostics({"history_limit": 12})

    assert payload["status"] == "success"
    assert payload["recommended_provider"] == "elevenlabs"
    assert any(hint.get("code") == "tts_route_policy_blocked" for hint in payload["remediation_hints"])
    assert payload["model_route"]["route_blocked"] is True


def test_tts_policy_status_surfaces_route_policy(monkeypatch) -> None:
    monkeypatch.setattr(route_handlers, "_resolve_tts_model_route", lambda payload, requested_provider: {
        "status": "success",
        "selected_provider": "local",
        "selected_model": "local-auto-tts-orpheus-3b-tts-f16",
        "selected_local_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
        "route_item": {
            "task": "tts",
            "provider": "local",
            "model": "local-auto-tts-orpheus-3b-tts-f16",
            "route_policy": {
                "matched": True,
                "blacklisted": True,
                "local_route_viable": False,
                "recommended_provider": "elevenlabs",
                "reason_code": "local_launch_template_blacklisted",
            },
        },
        "route_bundle": {"status": "success", "launch_policy_summary": {"blocked_task_count": 1}},
        "provider_credentials": {"status": "success", "providers": {"elevenlabs": {"ready": True}}},
    })
    monkeypatch.setattr(tts_policy.TtsPolicyManager, "shared", staticmethod(lambda: _PolicyStub()))
    monkeypatch.setattr(
        local_tts.LocalTTS,
        "diagnostics",
        classmethod(lambda cls, history_limit=24: {"status": "success", "provider": "local", "providers": {"pyttsx3": {"ready": True}}}),
    )
    monkeypatch.setenv("ELEVENLABS_API_KEY", "test-key")
    monkeypatch.setenv("ELEVENLABS_VOICE_ID", "voice-id")

    payload = route_handlers._tts_policy_status({"limit": 20})

    assert payload["status"] == "success"
    assert payload["recommended_provider"] == "elevenlabs"
    assert payload["model_route"]["route_policy"]["blacklisted"] is True
    assert payload["route_policy_summary"]["blocked_task_count"] == 1
