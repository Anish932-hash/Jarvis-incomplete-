from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

from backend.python.desktop_api import DesktopBackendService


def _build_service(tmp_path: Path) -> tuple[DesktopBackendService, SimpleNamespace]:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.log = SimpleNamespace(warning=lambda *_args, **_kwargs: None)
    telemetry = SimpleNamespace(events=[])
    telemetry.emit = lambda event, payload: telemetry.events.append((event, payload))
    service.kernel = SimpleNamespace(telemetry=telemetry)
    service._model_launch_history_lock = threading.RLock()
    service._model_launch_history = []
    service._model_launch_history_loaded = True
    service._model_launch_history_dirty = False
    service._model_launch_history_max = 120
    service._model_launch_history_recent_window = 24
    service._model_launch_history_path = str(tmp_path / "model_launch_history.jsonl")
    service._model_launch_demote_failure_streak_threshold = 2
    service._model_launch_demote_min_attempts = 3
    service._model_launch_demote_failure_rate_threshold = 0.6
    service._model_launch_recovery_success_streak_threshold = 2
    return service, telemetry


def test_rank_model_launch_templates_demotes_unstable_primary(tmp_path: Path) -> None:
    service, telemetry = _build_service(tmp_path)

    for _ in range(3):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-llama",
                "launcher": "llama-server",
                "status": "error",
                "ready": False,
            }
        )
    service._record_model_launch_template_event(  # noqa: SLF001
        {
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-endpoint",
            "launcher": "manual_endpoint",
            "status": "success",
            "ready": True,
        }
    )

    ranked = service._rank_model_launch_templates(  # noqa: SLF001
        bridge_kind="reasoning",
        profile_id="reasoning-bridge-qwen",
        templates=[
            {
                "template_id": "reasoning-llama",
                "title": "Managed llama-server bridge",
                "ready": True,
                "recommended": True,
                "manual_only": False,
                "autostart_capable": True,
            },
            {
                "template_id": "reasoning-endpoint",
                "title": "Existing endpoint",
                "ready": True,
                "recommended": False,
                "manual_only": True,
                "autostart_capable": False,
            },
        ],
    )

    summary = ranked["summary"]
    templates = ranked["templates"]
    primary = next(row for row in templates if row["template_id"] == "reasoning-llama")
    fallback = next(row for row in templates if row["template_id"] == "reasoning-endpoint")

    assert summary["recommended_template_id"] == "reasoning-endpoint"
    assert summary["recommended_shifted"] is True
    assert primary["health"]["demoted"] is True
    assert primary["recommended"] is False
    assert fallback["recommended"] is True
    assert telemetry.events[-1][0] == "runtime.model_launch_template"


def test_rank_model_launch_templates_suppresses_strategy_demoted_ready_template(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    for _ in range(4):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-llama",
                "launcher": "llama-server",
                "status": "error",
                "ready": False,
                "failure_like": True,
                "retry_profile": "stabilized",
                "retry_strategy": "stabilized_backoff",
                "retry_strategy_score": 18.0,
            }
        )
    for _ in range(4):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-endpoint",
                "launcher": "manual_endpoint",
                "status": "success",
                "ready": True,
                "retry_profile": "conservative",
                "retry_strategy": "adaptive_backoff",
                "retry_strategy_score": 82.0,
            }
        )

    ranked = service._rank_model_launch_templates(  # noqa: SLF001
        bridge_kind="reasoning",
        profile_id="reasoning-bridge-qwen",
        templates=[
            {
                "template_id": "reasoning-llama",
                "title": "Managed llama-server bridge",
                "launcher": "llama-server",
                "ready": True,
                "recommended": True,
                "manual_only": False,
                "autostart_capable": True,
            },
            {
                "template_id": "reasoning-endpoint",
                "title": "Existing endpoint",
                "launcher": "manual_endpoint",
                "ready": True,
                "recommended": False,
                "manual_only": True,
                "autostart_capable": False,
            },
        ],
    )

    summary = ranked["summary"]
    templates = ranked["templates"]
    primary = next(row for row in templates if row["template_id"] == "reasoning-llama")
    fallback = next(row for row in templates if row["template_id"] == "reasoning-endpoint")

    assert summary["suppressed_count"] == 1
    assert summary["recommended_retry_profile"] == "conservative"
    assert primary["suppressed"] is True
    assert primary["suppression_reason"] == "retry_strategy_demoted"
    assert primary["health"]["suppressed"] is True
    assert fallback["recommended"] is True
    assert summary["recommended_template_id"] == "reasoning-endpoint"


def test_rank_model_launch_templates_blacklists_persistently_failing_template(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    for _ in range(5):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-llama",
                "launcher": "llama-server",
                "status": "error",
                "ready": False,
                "failure_like": True,
                "retry_profile": "stabilized",
                "retry_strategy": "stabilized_backoff",
                "retry_strategy_score": 15.0,
            }
        )
    for _ in range(3):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-endpoint",
                "launcher": "manual_endpoint",
                "status": "success",
                "ready": True,
                "retry_profile": "conservative",
                "retry_strategy": "adaptive_backoff",
                "retry_strategy_score": 79.0,
            }
        )

    ranked = service._rank_model_launch_templates(  # noqa: SLF001
        bridge_kind="reasoning",
        profile_id="reasoning-bridge-qwen",
        templates=[
            {
                "template_id": "reasoning-llama",
                "title": "Managed llama-server bridge",
                "launcher": "llama-server",
                "ready": True,
                "recommended": True,
                "manual_only": False,
                "autostart_capable": True,
            },
            {
                "template_id": "reasoning-endpoint",
                "title": "Existing endpoint",
                "launcher": "manual_endpoint",
                "ready": True,
                "recommended": False,
                "manual_only": True,
                "autostart_capable": False,
            },
        ],
    )

    summary = ranked["summary"]
    primary = next(row for row in ranked["templates"] if row["template_id"] == "reasoning-llama")

    assert summary["blacklisted_count"] == 1
    assert summary["autonomy_ready_count"] == 1
    assert primary["blacklisted"] is True
    assert primary["blacklist_reason"] in {
        "persistent_failure_streak",
        "long_horizon_failure_rate",
        "strategy_and_template_demoted",
    }
    assert primary["health"]["blacklisted"] is True
    assert int(primary["cooldown_hint_s"]) > 0


def test_model_launch_template_health_recovers_after_success_streak(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    for _ in range(2):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "tts",
                "profile_id": "tts-bridge-orpheus",
                "template_id": "tts-http",
                "launcher": "tts_http_bridge",
                "status": "error",
                "ready": False,
            }
        )
    for _ in range(2):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "tts",
                "profile_id": "tts-bridge-orpheus",
                "template_id": "tts-http",
                "launcher": "tts_http_bridge",
                "status": "success",
                "ready": True,
            }
        )

    health = service._model_launch_template_health(  # noqa: SLF001
        bridge_kind="tts",
        profile_id="tts-bridge-orpheus",
        template_id="tts-http",
        template_ready=True,
        base_recommended=True,
    )

    assert health["recovered"] is True
    assert health["demoted"] is False
    assert health["success_streak"] >= 2
    assert health["health_score"] > 55.0


def test_model_launch_template_history_reports_fallback_filters(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    service._record_model_launch_template_event(  # noqa: SLF001
        {
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-llama",
            "requested_template_id": "reasoning-llama",
            "launcher": "llama-server",
            "status": "error",
            "ready": False,
        }
    )
    service._record_model_launch_template_event(  # noqa: SLF001
        {
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-endpoint",
            "requested_template_id": "reasoning-llama",
            "launcher": "manual_endpoint",
            "status": "success",
            "ready": True,
            "fallback_applied": True,
            "fallback_reason": "demoted",
            "fallback_source": "recommended_launch_template",
        }
    )

    payload = service.model_launch_template_history(
        bridge_kind="reasoning",
        profile_id="reasoning-bridge-qwen",
        template_id="reasoning-llama",
        limit=12,
    )

    assert payload["status"] == "success"
    assert payload["total"] == 2
    assert payload["fallback_count"] == 1
    assert payload["demoted_fallback_count"] == 1
    assert payload["bridge_kind_counts"]["reasoning"] == 2
    assert payload["timeline"]


def test_model_launch_template_history_reports_retry_profile_and_strategy_trends(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    service._record_model_launch_template_event(  # noqa: SLF001
        {
            "occurred_at": "2026-03-07T10:10:00Z",
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-llama",
            "launcher": "llama-server",
            "status": "success",
            "ready": True,
            "retry_profile": "stabilized",
            "retry_strategy": "stabilized_backoff",
            "retry_strategy_score": 88.0,
            "retry_delay_ms": 140,
        }
    )
    service._record_model_launch_template_event(  # noqa: SLF001
        {
            "occurred_at": "2026-03-07T11:12:00Z",
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-endpoint",
            "launcher": "manual_endpoint",
            "status": "error",
            "ready": False,
            "failure_like": True,
            "fallback_applied": True,
            "retry_profile": "conservative",
            "retry_strategy": "adaptive_backoff",
            "retry_strategy_score": 41.0,
            "retry_delay_ms": 320,
        }
    )

    payload = service.model_launch_template_history(
        bridge_kind="reasoning",
        profile_id="reasoning-bridge-qwen",
        limit=12,
    )

    assert payload["status"] == "success"
    assert payload["retry_profile_counts"]["stabilized"] == 1
    assert payload["retry_profile_counts"]["conservative"] == 1
    assert len(payload["retry_profile_trend"]) == 2
    assert any(str(item["dominant_retry_profile"]) == "stabilized" for item in payload["retry_profile_trend"])
    assert any(float(item["average_score"]) >= 80.0 for item in payload["strategy_score_timeline"])
    assert any(float(item["degradation_rate"]) > 0.0 for item in payload["degradation_timeline"])


def test_pick_model_launch_retry_template_prefers_ready_recommended_candidate(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    candidate = service._pick_model_launch_retry_template(  # noqa: SLF001
        profile={
            "launch_templates": [
                {
                    "template_id": "reasoning-llama",
                    "launcher": "llama-server",
                    "ready": True,
                    "recommended": False,
                    "selection_score": 71.0,
                    "health": {"unstable": True},
                },
                {
                    "template_id": "reasoning-endpoint",
                    "launcher": "manual_endpoint",
                    "ready": True,
                    "recommended": True,
                    "selection_score": 68.0,
                    "health": {"unstable": False},
                },
                {
                    "template_id": "reasoning-cold-standby",
                    "launcher": "manual_endpoint",
                    "ready": False,
                    "recommended": False,
                    "selection_score": 95.0,
                    "health": {"unstable": False},
                },
            ]
        },
        exclude_template_ids=["reasoning-primary"],
        allow_unready=False,
    )

    assert candidate is not None
    assert candidate["template_id"] == "reasoning-endpoint"


def test_pick_model_launch_retry_template_avoids_blacklisted_candidate(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    candidate = service._pick_model_launch_retry_template(  # noqa: SLF001
        profile={
            "launch_templates": [
                {
                    "template_id": "reasoning-llama",
                    "launcher": "llama-server",
                    "ready": True,
                    "recommended": True,
                    "selection_score": 96.0,
                    "blacklisted": True,
                    "health": {"unstable": False, "demoted": True, "blacklisted": True},
                },
                {
                    "template_id": "reasoning-endpoint",
                    "launcher": "manual_endpoint",
                    "ready": True,
                    "recommended": False,
                    "selection_score": 72.0,
                    "health": {"unstable": False, "demoted": False},
                },
            ]
        },
        exclude_template_ids=[],
        allow_unready=False,
        preferred_template_id="reasoning-llama",
    )

    assert candidate is not None
    assert candidate["template_id"] == "reasoning-endpoint"


def test_model_launch_retry_policy_exposes_delay_strategy_and_recommended_escalation(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    policy = service._model_launch_retry_policy(  # noqa: SLF001
        requested_template={
            "template_id": "reasoning-llama",
            "launcher": "llama-server",
            "manual_only": False,
        },
        current_template={
            "template_id": "reasoning-llama",
            "launcher": "llama-server",
            "manual_only": False,
        },
        recommended_template_id="reasoning-endpoint",
        attempt_index=1,
        max_attempts=3,
        failure_like=True,
        retry_profile="stabilized",
    )

    assert policy["enabled"] is True
    assert policy["profile"] == "stabilized"
    assert policy["strategy"] == "stabilized_backoff"
    assert policy["escalation_mode"] == "recommended_first"
    assert policy["prefer_recommended"] is True
    assert policy["recommended_template_id"] == "reasoning-endpoint"
    assert int(policy["delay_ms"]) > 0
    assert len(policy["preview_schedule_ms"]) == 2
    assert int(policy["preview_schedule_ms"][0]) >= int(policy["delay_ms"])


def test_model_launch_retry_strategy_outcomes_recommend_stabler_profile(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    for _ in range(4):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-llama",
                "launcher": "llama-server",
                "status": "error",
                "ready": False,
                "retry_profile": "aggressive",
                "retry_strategy": "aggressive_failover",
                "retry_delay_ms": 90,
            }
        )
    for _ in range(4):
        service._record_model_launch_template_event(  # noqa: SLF001
            {
                "bridge_kind": "reasoning",
                "profile_id": "reasoning-bridge-qwen",
                "template_id": "reasoning-endpoint",
                "launcher": "manual_endpoint",
                "status": "success",
                "ready": True,
                "retry_profile": "adaptive",
                "retry_strategy": "adaptive_backoff",
                "retry_delay_ms": 180,
            }
        )

    outcomes = service._model_launch_retry_strategy_outcomes(  # noqa: SLF001
        bridge_kind="reasoning",
        profile_id="reasoning-bridge-qwen",
    )

    assert outcomes["recommended_retry_profile"] == "adaptive"
    assert any(row["retry_profile"] == "aggressive" and row["demoted"] for row in outcomes["items"])


def test_model_launch_event_detail_returns_chain_and_root_diff(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)

    service._record_model_launch_template_event(  # noqa: SLF001
        {
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-llama",
            "requested_template_id": "reasoning-llama",
            "launcher": "llama-server",
            "requested_launcher": "llama-server",
            "transport": "openai_chat",
            "requested_transport": "openai_chat",
            "status": "error",
            "ready": False,
            "attempt_chain_id": "chain-1",
            "attempt_index": 1,
            "retry_profile": "stabilized",
            "retry_strategy": "stabilized_backoff",
            "retry_escalation_mode": "recommended_first",
            "retry_delay_ms": 180,
        }
    )
    second = service._record_model_launch_template_event(  # noqa: SLF001
        {
            "bridge_kind": "reasoning",
            "profile_id": "reasoning-bridge-qwen",
            "template_id": "reasoning-endpoint",
            "requested_template_id": "reasoning-endpoint",
            "launcher": "manual_endpoint",
            "requested_launcher": "manual_endpoint",
            "transport": "openai_chat",
            "requested_transport": "openai_chat",
            "status": "success",
            "ready": True,
            "attempt_chain_id": "chain-1",
            "attempt_index": 2,
            "retry_profile": "stabilized",
            "retry_strategy": "stabilized_backoff",
            "retry_escalation_mode": "breadth_first",
            "retry_delay_ms": 333,
        }
    )

    detail = service.model_launch_template_event_detail(
        event_id=second["event"]["event_id"],
    )

    assert detail["status"] == "success"
    assert detail["chain_summary"]["attempt_count"] == 2
    assert detail["chain_summary"]["max_delay_ms"] == 333
    assert detail["root_execution_diff"]["template_changed"] is True
    assert len(detail["attempt_chain"]) == 2
    assert detail["attempt_chain"][0]["template_id"] == "reasoning-llama"
    assert detail["attempt_chain"][1]["template_id"] == "reasoning-endpoint"


def test_model_route_bundle_reroutes_blacklisted_local_reasoning_route(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)
    base_bundle = {
        "status": "success",
        "stack_name": "desktop_agent",
        "count": 1,
        "success_count": 1,
        "error_count": 0,
        "items": [
            {
                "index": 1,
                "status": "success",
                "task": "reasoning",
                "model": "local-auto-reasoning-qwen3-14b",
                "provider": "local",
                "score": 2.5,
                "fallback_chain": ["groq", "nvidia"],
                "alternatives": ["groq-llm"],
                "selected_path": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf",
                "metadata": {"path": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf"},
                "diagnostics": {},
            }
        ],
        "provider_counts": {"local": 1},
        "provider_distribution": {"local": 1.0},
        "selected_local_paths": {"reasoning": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf"},
        "warnings": [],
    }
    service.model_router = SimpleNamespace(
        _PRIVACY_LOCAL_TASKS={"wakeword", "stt", "tts", "embedding", "intent", "vision"},
        route_bundle=lambda **_kwargs: dict(base_bundle),
        choose=lambda _task, **_kwargs: SimpleNamespace(
            model="groq-llm",
            provider="groq",
            score=3.2,
            alternatives=["nvidia-nim"],
            diagnostics={"winner": {"provider": "groq"}},
        ),
        _decision_fallback_chain=lambda **_kwargs: ["nvidia"],
        registry=SimpleNamespace(get=lambda _name: None),
    )
    service._provider_credentials_snapshot = lambda refresh=False: {
        "status": "success",
        "providers": {
            "groq": {"provider": "groq", "ready": True, "present": True},
            "nvidia": {"provider": "nvidia", "ready": True, "present": True},
            "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
            "local": {"provider": "local", "ready": True, "present": True},
        },
    }
    service.model_bridge_profiles = lambda **_kwargs: {
        "status": "success",
        "profiles": [
            {
                "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
                "bridge_kind": "reasoning",
                "task": "reasoning",
                "name": "local-auto-reasoning-qwen3-14b",
                "detected_model_path": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf",
                "recommended_launch_template_id": "reasoning-llama-qwen",
                "launch_ready_count": 1,
                "launch_stable_ready_count": 0,
                "launch_health": {
                    "ready_count": 1,
                    "stable_ready_count": 0,
                    "blacklisted_count": 1,
                    "autonomy_ready_count": 0,
                },
                "cloud_route_fallbacks": {"groq": True, "nvidia": True},
                "launch_templates": [
                    {
                        "template_id": "reasoning-llama-qwen",
                        "title": "Managed llama-server bridge",
                        "ready": True,
                        "blacklisted": True,
                        "health": {
                            "blacklisted": True,
                            "autonomy_policy": {
                                "autonomous_allowed": False,
                                "review_required": True,
                                "blacklist_reason": "persistent_failure_streak",
                                "cooldown_hint_s": 180,
                                "autonomy_score": 0.18,
                            },
                        },
                    }
                ],
            }
        ],
    }

    routed = service.model_route_bundle(stack_name="desktop_agent", requires_offline=False, privacy_mode=False)

    item = routed["items"][0]
    assert item["provider"] == "groq"
    assert item["model"] == "groq-llm"
    assert item["requested_provider"] == "local"
    assert item["route_adjusted"] is True
    assert item["route_blocked"] is False
    assert item["route_policy"]["blacklisted"] is True
    assert routed["launch_policy_summary"]["rerouted_task_count"] == 1
    assert routed["provider_counts"]["groq"] == 1


def test_model_route_bundle_blocks_blacklisted_local_stt_route_when_offline_required(tmp_path: Path) -> None:
    service, _ = _build_service(tmp_path)
    base_bundle = {
        "status": "success",
        "stack_name": "voice",
        "count": 1,
        "success_count": 1,
        "error_count": 0,
        "items": [
            {
                "index": 1,
                "status": "success",
                "task": "stt",
                "model": "local-auto-stt-whisper-large-v3",
                "provider": "local",
                "score": 2.4,
                "fallback_chain": ["groq", "nvidia"],
                "alternatives": [],
                "selected_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
                "metadata": {"path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)"},
                "diagnostics": {},
            }
        ],
        "provider_counts": {"local": 1},
        "provider_distribution": {"local": 1.0},
        "selected_local_paths": {"stt": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)"},
        "warnings": [],
    }
    service.model_router = SimpleNamespace(
        _PRIVACY_LOCAL_TASKS={"wakeword", "stt", "tts", "embedding", "intent", "vision"},
        route_bundle=lambda **_kwargs: dict(base_bundle),
        choose=lambda _task, **_kwargs: SimpleNamespace(
            model="groq-stt",
            provider="groq",
            score=3.2,
            alternatives=[],
            diagnostics={},
        ),
        _decision_fallback_chain=lambda **_kwargs: ["nvidia"],
        registry=SimpleNamespace(get=lambda _name: None),
    )
    service._provider_credentials_snapshot = lambda refresh=False: {
        "status": "success",
        "providers": {
            "groq": {"provider": "groq", "ready": True, "present": True},
            "nvidia": {"provider": "nvidia", "ready": True, "present": True},
            "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
            "local": {"provider": "local", "ready": True, "present": True},
        },
    }
    service.model_bridge_profiles = lambda **_kwargs: {
        "status": "success",
        "profiles": [
            {
                "profile_id": "stt-runtime-whisper-large-v3",
                "bridge_kind": "stt",
                "task": "stt",
                "name": "whisper-large-v3",
                "detected_model_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
                "recommended_launch_template_id": "stt-local-runtime-whisper-large-v3",
                "launch_ready_count": 1,
                "launch_stable_ready_count": 0,
                "launch_health": {
                    "ready_count": 1,
                    "stable_ready_count": 0,
                    "blacklisted_count": 1,
                    "autonomy_ready_count": 0,
                },
                "cloud_route_fallbacks": {"groq": True},
                "launch_templates": [
                    {
                        "template_id": "stt-local-runtime-whisper-large-v3",
                        "title": "Managed local Whisper runtime",
                        "ready": True,
                        "blacklisted": True,
                        "health": {
                            "blacklisted": True,
                            "autonomy_policy": {
                                "autonomous_allowed": False,
                                "review_required": True,
                                "blacklist_reason": "persistent_failure_streak",
                                "cooldown_hint_s": 240,
                                "autonomy_score": 0.12,
                            },
                        },
                    }
                ],
            }
        ],
    }

    routed = service.model_route_bundle(stack_name="voice", requires_offline=True, privacy_mode=True)

    item = routed["items"][0]
    assert item["provider"] == "local"
    assert item["route_adjusted"] is False
    assert item["route_blocked"] is True
    assert item["route_policy"]["blacklisted"] is True
    assert routed["launch_policy_summary"]["blocked_task_count"] == 1
