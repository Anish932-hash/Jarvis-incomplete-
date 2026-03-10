from __future__ import annotations

from backend.python.core.contracts import ActionResult
from backend.python.core.execution_strategy import ExecutionStrategyController


def test_execution_strategy_recommend_returns_balanced_for_new_task_class(tmp_path) -> None:
    controller = ExecutionStrategyController(store_path=str(tmp_path / "execution_strategy.json"))

    payload = controller.recommend(
        task_class="desktop_ui:query:simple",
        source_name="desktop-ui",
        metadata={},
    )

    assert payload["status"] == "success"
    assert payload["mode"] == "balanced"
    strategy = payload.get("strategy", {})
    assert isinstance(strategy, dict)
    assert strategy.get("execution_allow_parallel") is True
    assert int(strategy.get("execution_max_parallel_steps", 0)) >= 1


def test_execution_strategy_shifts_to_strict_after_repeated_failures(tmp_path) -> None:
    controller = ExecutionStrategyController(store_path=str(tmp_path / "execution_strategy.json"))
    rows = [
        ActionResult(
            action="computer_click_target",
            status="failed",
            error="target not visible",
            output={"status": "failed"},
            attempt=3,
            duration_ms=1800,
        )
    ]
    for _ in range(10):
        controller.record_outcome(
            task_class="desktop_ui:desktop:compound",
            outcome="failed",
            results=rows,
            metadata={"goal_id": "g1", "source": "desktop-ui"},
        )

    payload = controller.recommend(
        task_class="desktop_ui:desktop:compound",
        source_name="desktop-ui",
        metadata={},
    )

    assert payload["status"] == "success"
    assert payload["mode"] == "strict"
    strategy = payload.get("strategy", {})
    assert isinstance(strategy, dict)
    assert strategy.get("execution_allow_parallel") is False
    assert int(strategy.get("execution_max_parallel_steps", 0)) == 1
    assert strategy.get("external_branch_strategy") == "enforce"
    assert strategy.get("verification_strictness") == "strict"


def test_execution_strategy_shifts_to_throughput_after_stable_successes(tmp_path) -> None:
    controller = ExecutionStrategyController(store_path=str(tmp_path / "execution_strategy.json"))
    rows = [
        ActionResult(
            action="browser_read_dom",
            status="success",
            output={"status": "success"},
            attempt=1,
            duration_ms=180,
        ),
        ActionResult(
            action="browser_extract_links",
            status="success",
            output={"status": "success"},
            attempt=1,
            duration_ms=190,
        ),
    ]
    for _ in range(14):
        controller.record_outcome(
            task_class="desktop_ui:browser:compound",
            outcome="completed",
            results=rows,
            metadata={"goal_id": "g2", "source": "desktop-ui"},
        )

    payload = controller.recommend(
        task_class="desktop_ui:browser:compound",
        source_name="desktop-ui",
        metadata={},
    )

    assert payload["status"] == "success"
    assert payload["mode"] in {"throughput", "balanced"}
    strategy = payload.get("strategy", {})
    assert isinstance(strategy, dict)
    assert strategy.get("execution_allow_parallel") is True
    assert int(strategy.get("execution_max_parallel_steps", 0)) >= 2


def test_execution_strategy_tune_updates_bias_and_parallel_cap(tmp_path) -> None:
    controller = ExecutionStrategyController(store_path=str(tmp_path / "execution_strategy.json"))
    payload = controller.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.78, "open_breaker_pressure": 0.55},
            "scores": {"reliability": 42.0, "autonomy": 38.0},
        },
        mission_summary={
            "trend": {"pressure": 0.72},
            "recommendation": "stability",
        },
        dry_run=False,
        reason="unit-test",
    )

    assert payload["status"] == "success"
    assert payload["reason"] == "unit-test"
    assert isinstance(payload.get("state"), dict)
    state = payload["state"]
    assert int(state.get("global_parallel_cap", 3)) <= 3


def test_execution_strategy_family_hotspot_tuning_influences_new_recommendations(tmp_path) -> None:
    controller = ExecutionStrategyController(store_path=str(tmp_path / "execution_strategy.json"))
    tune = controller.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.74, "open_breaker_pressure": 0.46},
            "scores": {"reliability": 40.0, "autonomy": 33.0},
            "action_hotspots": [
                {"action": "external_email_send", "failures": 12, "runs": 14, "failure_rate": 0.86},
                {"action": "external_doc_update", "failures": 8, "runs": 10, "failure_rate": 0.8},
            ],
        },
        mission_summary={
            "trend": {"pressure": 0.66},
            "recommendation": "stability",
        },
        dry_run=False,
        reason="unit-test-family",
    )

    assert tune["status"] == "success"
    assert "task_family_targets" in tune.get("targets", {})
    recommendation = controller.recommend(
        task_class="desktop_ui:external:simple",
        source_name="desktop-ui",
        metadata={},
    )
    assert recommendation["status"] == "success"
    assert recommendation.get("task_family") == "external"
    assert recommendation["mode"] in {"strict", "balanced"}
    signals = recommendation.get("signals", {})
    assert float(signals.get("family_strict_bias", 0.0)) >= -0.52


def test_execution_strategy_snapshot_persists_task_family_bias_rows(tmp_path) -> None:
    store_path = tmp_path / "execution_strategy.json"
    controller = ExecutionStrategyController(store_path=str(store_path))
    controller.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.62, "open_breaker_pressure": 0.34},
            "scores": {"reliability": 47.0, "autonomy": 45.0},
            "action_hotspots": [{"action": "computer_click_target", "failures": 9, "runs": 16, "failure_rate": 0.56}],
        },
        mission_summary={"trend": {"pressure": 0.41}, "recommendation": "stability"},
        dry_run=False,
        reason="unit-test-family-persist",
    )

    reloaded = ExecutionStrategyController(store_path=str(store_path))
    snapshot = reloaded.snapshot(limit=20)
    assert snapshot["status"] == "success"
    assert int(snapshot.get("tracked_task_families", 0) or 0) >= 1
    rows = snapshot.get("task_family_bias", [])
    assert isinstance(rows, list)
    assert rows
