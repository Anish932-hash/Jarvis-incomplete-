from __future__ import annotations

from backend.python.core.policy_bandit import MissionPolicyBandit


def test_policy_bandit_prefers_profile_with_higher_reward_history(tmp_path) -> None:
    bandit = MissionPolicyBandit(store_path=str(tmp_path / "policy_bandit.json"))
    for _ in range(8):
        bandit.record_outcome(
            task_class="desktop_ui:external:compound",
            profile="automation_safe",
            reward=0.92,
            outcome="completed",
            metadata={},
        )
    for _ in range(6):
        bandit.record_outcome(
            task_class="desktop_ui:external:compound",
            profile="interactive",
            reward=0.22,
            outcome="failed",
            metadata={},
        )

    choice = bandit.choose_profile(
        task_class="desktop_ui:external:compound",
        candidate_profiles=["interactive", "automation_safe", "automation_power"],
        metadata={"source": "desktop-ui"},
    )

    assert choice["status"] == "success"
    assert choice["selected_profile"] == "automation_safe"


def test_policy_bandit_tuning_and_reset(tmp_path) -> None:
    bandit = MissionPolicyBandit(store_path=str(tmp_path / "policy_bandit.json"))
    tune = bandit.tune_from_operational_signals(
        autonomy_report={"pressures": {"failure_pressure": 0.55, "open_breaker_pressure": 0.24}},
        mission_summary={"recommendation": "stability"},
        dry_run=False,
        reason="unit-test",
    )
    assert tune["status"] == "success"
    assert tune["mode"] == "stability"

    bandit.record_outcome(
        task_class="desktop_schedule:automation:compound",
        profile="automation_safe",
        reward=0.82,
        outcome="completed",
        metadata={"goal_id": "goal-1"},
    )
    snapshot = bandit.snapshot(limit=20)
    assert snapshot["status"] == "success"
    assert snapshot["count"] >= 1

    reset = bandit.reset(task_class="desktop_schedule:automation:compound")
    assert reset["status"] == "success"
    assert reset["removed"] >= 1


def test_policy_bandit_tuning_supports_throughput_mode_and_persists_adaptive_state(tmp_path) -> None:
    store = tmp_path / "policy_bandit.json"
    bandit = MissionPolicyBandit(store_path=str(store))

    dry_run = bandit.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.05, "open_breaker_pressure": 0.0},
            "scores": {"reliability": 94.0, "autonomy": 89.0},
            "policy_guardrails": {"critical_count": 0, "unstable_count": 0},
            "circuit_breakers": {"open_count": 0},
        },
        mission_summary={
            "count": 180,
            "risk": {"avg_score": 0.12},
            "quality": {"avg_score": 0.91},
            "failed_ratio": 0.03,
            "blocked_ratio": 0.01,
            "hotspots": {"retry_total": 1, "failure_total": 0},
            "recommendation": "throughput",
        },
        dry_run=True,
        reason="unit-test-throughput",
    )
    assert dry_run["status"] == "success"
    assert dry_run["mode"] == "throughput"
    assert dry_run["applied"] is False
    assert isinstance(dry_run.get("mode_scores"), dict)
    assert isinstance(dry_run.get("signals"), dict)
    assert isinstance(dry_run.get("adaptive_state"), dict)

    applied = bandit.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.05, "open_breaker_pressure": 0.0},
            "scores": {"reliability": 94.0, "autonomy": 89.0},
            "policy_guardrails": {"critical_count": 0, "unstable_count": 0},
            "circuit_breakers": {"open_count": 0},
        },
        mission_summary={
            "count": 180,
            "risk": {"avg_score": 0.12},
            "quality": {"avg_score": 0.91},
            "failed_ratio": 0.03,
            "blocked_ratio": 0.01,
            "hotspots": {"retry_total": 1, "failure_total": 0},
            "recommendation": "throughput",
        },
        dry_run=False,
        reason="unit-test-throughput-apply",
    )
    assert applied["status"] == "success"
    assert applied["mode"] == "throughput"
    assert applied["applied"] is True
    assert store.exists() is True

    reloaded = MissionPolicyBandit(store_path=str(store))
    snapshot = reloaded.snapshot(limit=10)
    config = snapshot.get("config", {})
    adaptive = config.get("adaptive_state", {}) if isinstance(config, dict) else {}
    assert isinstance(adaptive, dict)
    assert str(adaptive.get("last_mode", "")) == "throughput"
    assert int(adaptive.get("tune_runs", 0)) >= 1
