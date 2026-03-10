from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from backend.python.core.model_connector_orchestrator import ModelConnectorOrchestrator


@dataclass
class _Profile:
    name: str
    provider: str
    quality: int
    latency: float
    privacy: int
    available: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class _Registry:
    def __init__(self) -> None:
        self.rows: List[_Profile] = [
            _Profile("groq-llm", "groq", 90, 220.0, 46),
            _Profile("nvidia-nim", "nvidia", 92, 240.0, 52),
            _Profile("local-auto-reasoning-qwen3-14b", "local", 88, 460.0, 98),
        ]

    def list_by_task(self, task: str):  # noqa: ANN201
        return list(self.rows) if str(task).strip().lower() == "reasoning" else []


def test_orchestrator_prefers_local_for_offline_or_privacy(tmp_path: Path) -> None:
    state_path = tmp_path / "connector_orchestrator_state.json"
    orchestrator = ModelConnectorOrchestrator(state_path=str(state_path))
    registry = _Registry()
    provider_snapshot = {
        "groq": {"provider": "groq", "ready": True, "present": True},
        "nvidia": {"provider": "nvidia", "ready": True, "present": True},
        "local": {"provider": "local", "ready": True, "present": True},
    }

    offline_plan = orchestrator.plan_reasoning_route(
        registry=registry,
        provider_snapshot=provider_snapshot,
        requires_offline=True,
        privacy_mode=False,
        mission_profile="balanced",
    )
    assert offline_plan["status"] == "success"
    assert offline_plan["preferred_provider"] == "local"

    privacy_plan = orchestrator.plan_reasoning_route(
        registry=registry,
        provider_snapshot=provider_snapshot,
        requires_offline=False,
        privacy_mode=True,
        mission_profile="privacy",
    )
    assert privacy_plan["preferred_provider"] == "local"


def test_orchestrator_outcomes_trigger_cooldown(tmp_path: Path) -> None:
    state_path = tmp_path / "connector_orchestrator_state.json"
    orchestrator = ModelConnectorOrchestrator(state_path=str(state_path))
    for _ in range(4):
        orchestrator.report_outcome(provider="groq", success=False, latency_ms=820.0, error="timeout")

    diag = orchestrator.diagnostics(provider="groq")
    provider = diag.get("providers", {}).get("groq", {})
    assert float(provider.get("failure_ema", 0.0) or 0.0) > 0.2
    assert float(provider.get("cooldown_until_epoch", 0.0) or 0.0) > 0.0
    assert int(provider.get("failure_streak", 0) or 0) >= 3


def test_orchestrator_policy_update_and_reset(tmp_path: Path) -> None:
    state_path = tmp_path / "connector_orchestrator_state.json"
    orchestrator = ModelConnectorOrchestrator(state_path=str(state_path))
    updated = orchestrator.update_policy({"readiness_weight": 2.05, "latency_weight": 1.4, "failure_streak_threshold": 4})
    assert updated["status"] == "success"
    assert int(updated["count"]) >= 2

    diag = orchestrator.diagnostics()
    policy = diag.get("policy", {})
    assert float(policy.get("readiness_weight", 0.0)) == 2.05
    assert float(policy.get("latency_weight", 0.0)) == 1.4

    reset_payload = orchestrator.reset()
    assert reset_payload["status"] == "success"
