from __future__ import annotations

from backend.python.core.kernel import AgentKernel


class _RiskEngineStub:
    def runtime_snapshot(self, *, limit: int = 200):  # noqa: ARG002
        return {"status": "success", "items": []}


class _PolicyStub:
    def __init__(self) -> None:
        self.risk_engine = _RiskEngineStub()

    def guardrail_snapshot(self, *, limit: int = 200, min_samples: int = 1):  # noqa: ARG002
        return {"status": "success", "count": 28}


class _CircuitBreakerStub:
    def snapshot(self, *, limit: int = 200):  # noqa: ARG002
        return {"status": "success", "open_count": 2}


class _ExternalReliabilityStub:
    def snapshot(self, *, limit: int = 200):  # noqa: ARG002
        return {
            "status": "success",
            "degraded_count": 1,
            "items": [
                {"provider": "google", "failure_ema": 0.66, "outage_active": True, "cooldown_active": False},
            ],
            "mission_outage_policy": {"pressure_ema": 0.58, "blocked_ratio_ema": 0.34},
        }


class _ModelRegistryStub:
    def runtime_snapshot(self, *, limit: int = 200):  # noqa: ARG002
        return {"status": "success", "total": 2}


class _ModelRouterStub:
    def __init__(self) -> None:
        self.registry = _ModelRegistryStub()


class _PlannerStub:
    def __init__(self) -> None:
        self.model_router = _ModelRouterStub()


def test_runtime_diagnostics_bundle_contains_pressure_alerts_and_trends() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.queue_diagnostics = lambda limit=1200, include_terminal=False: {  # type: ignore[method-assign]
        "status": "success",
        "queue_length": 240,
        "orphaned_pending_count": 9,
    }
    kernel.policy = _PolicyStub()
    kernel.action_circuit_breaker = _CircuitBreakerStub()
    kernel.external_reliability = _ExternalReliabilityStub()
    kernel.planner = _PlannerStub()
    kernel._summarize_mission_trends = lambda limit=240: {  # type: ignore[method-assign]
        "status": "success",
        "trend": {"pressure": 0.44},
        "recommendation": "stability",
    }

    payload = kernel.runtime_diagnostics_bundle(limit=200)
    assert payload["status"] == "success"
    readiness = payload.get("readiness", {})
    assert isinstance(readiness, dict)
    assert 0.0 <= float(readiness.get("score", 0.0)) <= 1.0
    pressure = payload.get("pressure", {})
    assert isinstance(pressure, dict)
    assert float(pressure.get("score", 0.0)) > 0.0
    alerts = payload.get("alerts", [])
    assert isinstance(alerts, list)
    assert len(alerts) >= 1
    recommendations = payload.get("recommendations", [])
    assert isinstance(recommendations, list)
    assert len(recommendations) >= 1
    mission_trends = payload.get("mission_trends", {})
    assert isinstance(mission_trends, dict)
    assert mission_trends.get("status") == "success"
