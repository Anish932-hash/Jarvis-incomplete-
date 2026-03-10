from __future__ import annotations

from backend.python.core.contracts import ActionResult, ExecutionPlan, PlanStep
from backend.python.core.mission_control import MissionControl


def test_mission_checkpoint_and_resume_payload(tmp_path) -> None:
    control = MissionControl(store_path=str(tmp_path / "missions.json"), max_records=50, max_checkpoints=50)
    mission = control.create_for_goal(
        goal_id="goal-1",
        text="open webpage and extract links",
        source="desktop-ui",
        metadata={"policy_profile": "interactive"},
    )
    mission_id = mission.mission_id

    step1 = PlanStep(step_id="step-open", action="open_url", args={"url": "https://example.com"}, depends_on=[], verify={})
    step2 = PlanStep(
        step_id="step-links",
        action="browser_extract_links",
        args={"url": "https://example.com"},
        depends_on=["step-open"],
        verify={},
    )
    plan = ExecutionPlan(plan_id="plan-1", goal_id="goal-1", intent="compound", steps=[step1, step2], context={})
    control.set_plan(mission_id, plan)

    result = ActionResult(
        action="open_url",
        status="success",
        output={"status": "success", "url": "https://example.com"},
        evidence={"step_id": "step-open"},
    )
    control.checkpoint_step(mission_id, result)

    payload = control.build_resume_payload(mission_id)
    assert payload["status"] == "success"
    assert payload["remaining_steps"] == 1
    assert payload["completed_step_ids"] == ["step-open"]
    assert payload["resume_cursor"]["step_id"] == "step-links"
    resume_plan = payload["resume_plan"]
    steps = resume_plan["steps"]
    assert len(steps) == 1
    assert steps[0]["step_id"] == "step-links"
    assert steps[0]["depends_on"] == []


def test_mark_finished_and_resume_tracking(tmp_path) -> None:
    control = MissionControl(store_path=str(tmp_path / "missions.json"), max_records=50, max_checkpoints=50)
    mission = control.create_for_goal(goal_id="goal-1", text="task", source="desktop-ui")
    mission_id = mission.mission_id

    control.mark_finished(mission_id, status="failed", error="network timeout")
    stored = control.get(mission_id)
    assert stored is not None
    assert stored["status"] == "failed"
    assert "network timeout" in stored["last_error"]

    control.mark_resumed(mission_id, new_goal_id="goal-2")
    resumed = control.get(mission_id)
    assert resumed is not None
    assert resumed["status"] == "running"
    assert resumed["latest_goal_id"] == "goal-2"
    assert resumed["resume_count"] == 1


def test_resume_cursor_uses_running_step_from_execution_journal(tmp_path) -> None:
    control = MissionControl(store_path=str(tmp_path / "missions.json"), max_records=50, max_checkpoints=100)
    mission = control.create_for_goal(goal_id="goal-1", text="multi-step", source="desktop-ui")
    mission_id = mission.mission_id

    plan = ExecutionPlan(
        plan_id="plan-journal",
        goal_id="goal-1",
        intent="journal_resume",
        steps=[
            PlanStep(step_id="s1", action="open_url", args={"url": "https://example.com"}, depends_on=[], verify={}),
            PlanStep(step_id="s2", action="browser_read_dom", args={"url": "https://example.com"}, depends_on=["s1"], verify={}),
            PlanStep(step_id="s3", action="browser_extract_links", args={"url": "https://example.com"}, depends_on=["s2"], verify={}),
        ],
        context={},
    )
    control.set_plan(mission_id, plan)

    control.checkpoint_step_finished(
        mission_id,
        ActionResult(
            action="open_url",
            status="success",
            output={"status": "success"},
            evidence={"step_id": "s1"},
        ),
        goal_id="goal-1",
        plan_id="plan-journal",
    )
    control.checkpoint_step_started(
        mission_id,
        goal_id="goal-1",
        plan_id="plan-journal",
        step=plan.steps[1],
        attempt=1,
    )

    payload = control.build_resume_payload(mission_id)
    assert payload["status"] == "success"
    assert payload["resume_cursor"]["step_id"] == "s2"
    assert payload["resume_cursor"]["index"] == 1
    assert payload["resume_cursor"]["status"] == "running"
    assert payload["remaining_steps"] == 2
    assert [row["step_id"] for row in payload["resume_plan"]["steps"]] == ["s2", "s3"]


def test_mission_timeline_filters_and_sorting(tmp_path) -> None:
    control = MissionControl(store_path=str(tmp_path / "missions.json"), max_records=50, max_checkpoints=100)
    mission = control.create_for_goal(goal_id="goal-1", text="timeline", source="desktop-ui")
    mission_id = mission.mission_id

    step = PlanStep(step_id="s1", action="time_now", args={"timezone": "UTC"}, depends_on=[], verify={})
    control.checkpoint_step_started(mission_id, goal_id="goal-1", plan_id="plan-1", step=step, attempt=1)
    control.checkpoint_step_finished(
        mission_id,
        ActionResult(action="time_now", status="success", output={"status": "success"}, evidence={"step_id": "s1"}),
        goal_id="goal-1",
        plan_id="plan-1",
        step_args={"timezone": "UTC"},
    )

    payload = control.timeline(mission_id, event="finished", status="success", limit=10, descending=True)
    assert payload["status"] == "success"
    assert payload["count"] == 1
    assert payload["items"][0]["event"] == "finished"
    assert payload["items"][0]["status"] == "success"


def test_mission_diagnostics_reports_dependency_and_retry_hotspots(tmp_path) -> None:
    control = MissionControl(store_path=str(tmp_path / "missions.json"), max_records=50, max_checkpoints=100)
    mission = control.create_for_goal(goal_id="goal-1", text="diagnostics", source="desktop-ui")
    mission_id = mission.mission_id

    plan = ExecutionPlan(
        plan_id="plan-diag",
        goal_id="goal-1",
        intent="diagnostics",
        steps=[
            PlanStep(step_id="s1", action="open_url", args={"url": "https://example.com"}, depends_on=[], verify={}),
            PlanStep(
                step_id="s2",
                action="browser_read_dom",
                args={"url": "https://example.com"},
                depends_on=["s1"],
                verify={},
            ),
            PlanStep(
                step_id="s3",
                action="browser_extract_links",
                args={"url": "https://example.com"},
                depends_on=["s2", "missing-step"],
                verify={},
            ),
        ],
        context={},
    )
    control.set_plan(mission_id, plan)

    control.checkpoint_step_finished(
        mission_id,
        ActionResult(
            action="open_url",
            status="success",
            output={"status": "success"},
            attempt=1,
            duration_ms=120,
            evidence={"step_id": "s1"},
        ),
        goal_id="goal-1",
        plan_id="plan-diag",
    )
    control.checkpoint_step_finished(
        mission_id,
        ActionResult(
            action="browser_read_dom",
            status="failed",
            error="request timed out",
            output={"status": "failed"},
            attempt=3,
            duration_ms=2200,
            evidence={"step_id": "s2"},
        ),
        goal_id="goal-1",
        plan_id="plan-diag",
    )

    payload = control.diagnostics(mission_id, hotspot_limit=5)
    assert payload["status"] == "success"
    assert payload["plan"]["step_count"] == 3
    assert payload["step_counts"]["failed"] >= 1
    assert payload["hotspots"]["retry"][0]["step_id"] == "s2"
    assert payload["hotspots"]["retry"][0]["attempts"] == 3
    assert payload["hotspots"]["failures"][0]["step_id"] == "s2"
    assert any(item["step_id"] == "s3" for item in payload["dependency_issues"])
    assert payload["risk"]["level"] in {"medium", "high"}
    quality = payload.get("quality", {})
    assert isinstance(quality, dict)
    assert 0.0 <= float(quality.get("score", 0.0)) <= 1.0
    assert quality.get("level") in {"low", "medium", "high"}
    assert quality.get("recommended_recovery_profile") in {"safe", "balanced", "aggressive"}
    assert quality.get("recommended_verification_strictness") in {"standard", "strict"}
