from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from backend.python.desktop_api import DesktopBackendService


def _build_scheduler_service() -> DesktopBackendService:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service._opportunity_lock = threading.RLock()
    service._context_opportunity_queue = []
    service._context_opportunity_runs = []
    service._context_opportunity_max_records = 100
    service._context_opportunity_priority_weight = 1.0
    service._context_opportunity_confidence_weight = 2.0
    service._context_opportunity_retry_penalty = 0.75
    service._context_opportunity_aging_window_s = 30.0
    service._context_opportunity_fairness_window_s = 60.0
    service._context_opportunity_per_type_max_in_window = 1
    service._context_opportunity_per_type_max_concurrency = 1
    service._context_opportunity_class_weights = {
        "recovery": 1.15,
        "automation": 1.0,
        "external": 0.95,
        "insight": 0.9,
        "other": 0.85,
    }
    service._context_opportunity_class_limits_in_window = {
        "recovery": 2,
        "automation": 1,
        "external": 1,
        "insight": 1,
        "other": 1,
    }
    service._context_opportunity_class_max_concurrency = {
        "recovery": 2,
        "automation": 1,
        "external": 1,
        "insight": 1,
        "other": 1,
    }
    service._context_opportunity_external_pressure_enabled = True
    service._context_opportunity_external_refresh_s = 120.0
    service._context_opportunity_external_penalty_weight = 2.8
    service._context_opportunity_external_recovery_boost = 1.2
    service._context_opportunity_external_penalty_weight_offset = 0.0
    service._context_opportunity_external_recovery_boost_offset = 0.0
    service._context_opportunity_external_limit_floor_scale = 0.35
    service._context_opportunity_external_concurrency_floor_scale = 0.4
    service._context_opportunity_external_autotune_enabled = True
    service._context_opportunity_external_autotune_alpha = 0.24
    service._context_opportunity_external_autotune_bad_threshold = 0.58
    service._context_opportunity_external_autotune_good_threshold = 0.36
    service._context_opportunity_dynamic_class_limit_scale_by_class = {}
    service._context_opportunity_dynamic_class_concurrency_scale_by_class = {}
    service._context_opportunity_external_policy_learning_state = {}
    service._context_opportunity_external_snapshot = {}
    service._context_opportunity_external_last_refresh_ts = 0.0
    service._context_opportunity_external_last_error = ""
    service._context_opportunity_starvation_override_s = 45.0
    service._context_opportunity_next_ready_at = 0.0
    service._context_opportunity_active_runs = {}
    service._context_opportunity_dispatch_history_by_type = {}
    service._context_opportunity_dispatch_history_by_class = {}
    service._context_opportunity_dynamic_class_weight_offsets = {}
    service._context_opportunity_multiobjective_enabled = True
    service._context_opportunity_deadline_weight = 2.6
    service._context_opportunity_utility_weight = 1.8
    service._context_opportunity_risk_weight = 1.4
    service._context_opportunity_duration_weight = 0.65
    service._context_opportunity_dynamic_priority_offsets_by_type = {}
    service._context_opportunity_dynamic_confidence_offsets_by_type = {}
    service._context_opportunity_dynamic_retry_penalty_by_type = {}
    service._context_opportunity_self_tune_enabled = True
    service._context_opportunity_self_tune_alpha = 0.4
    service._context_opportunity_self_tune_min_samples = 2
    service._context_opportunity_self_tune_bad_threshold = 0.4
    service._context_opportunity_self_tune_good_threshold = 0.75
    service._context_opportunity_learning_state_by_type = {}
    service._context_opportunity_tuning_state_path = ""
    service._context_opportunity_tuning_persist_every = 4
    service._context_opportunity_tuning_dirty_updates = 0
    service._context_opportunity_tuning_last_loaded_at = ""
    service._context_opportunity_tuning_last_saved_at = ""
    service._context_opportunity_tuning_last_save_error = ""
    telemetry = SimpleNamespace(events=[])
    telemetry.emit = lambda event, payload: telemetry.events.append((event, payload))
    service.kernel = SimpleNamespace(telemetry=telemetry)
    return service


def test_scheduler_selects_highest_scoring_job() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc)
    service._context_opportunity_queue = [
        {
            "run_id": "run-low",
            "queued_ts": now.timestamp(),
            "attempt": 1,
            "opportunity": {"priority": 7, "confidence": 0.95, "expires_at": ""},
        },
        {
            "run_id": "run-high",
            "queued_ts": now.timestamp(),
            "attempt": 1,
            "opportunity": {"priority": 9, "confidence": 0.25, "expires_at": ""},
        },
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-high"
    assert float(selected.get("_scheduler_score", 0.0) or 0.0) > 0.0


def test_scheduler_drops_expired_jobs_before_dispatch() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc)
    service._context_opportunity_queue = [
        {
            "run_id": "run-expired",
            "opportunity_id": "opp-expired",
            "opportunity_type": "workflow_automation",
            "reason": "autorun",
            "attempt": 1,
            "queued_ts": now.timestamp() - 15.0,
            "opportunity": {
                "priority": 9,
                "confidence": 0.9,
                "expires_at": (now - timedelta(seconds=5)).isoformat(),
            },
        },
        {
            "run_id": "run-valid",
            "opportunity_id": "opp-valid",
            "opportunity_type": "workflow_automation",
            "reason": "autorun",
            "attempt": 1,
            "queued_ts": now.timestamp() - 5.0,
            "opportunity": {
                "priority": 5,
                "confidence": 0.6,
                "expires_at": (now + timedelta(seconds=300)).isoformat(),
            },
        },
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-valid"
    expired_rows = [row for row in service._context_opportunity_runs if str(row.get("status", "")) == "skipped_expired"]
    assert expired_rows
    assert expired_rows[0].get("run_id") == "run-expired"


def test_scheduler_fairness_prefers_other_type_when_quota_exceeded() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc).timestamp()
    service._context_opportunity_dispatch_history_by_type = {"workflow_automation": [now - 5.0]}
    service._context_opportunity_per_type_max_in_window = 1
    service._context_opportunity_fairness_window_s = 120.0
    service._context_opportunity_starvation_override_s = 60.0
    service._context_opportunity_queue = [
        {
            "run_id": "run-auto",
            "queued_ts": now - 10.0,
            "attempt": 1,
            "opportunity": {"opportunity_type": "workflow_automation", "priority": 10, "confidence": 0.9, "expires_at": ""},
        },
        {
            "run_id": "run-error",
            "queued_ts": now - 2.0,
            "attempt": 1,
            "opportunity": {"opportunity_type": "error_detected", "priority": 4, "confidence": 0.6, "expires_at": ""},
        },
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-error"


def test_scheduler_fairness_respects_class_quota() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc).timestamp()
    service._context_opportunity_dispatch_history_by_class = {"automation": [now - 3.0]}
    service._context_opportunity_class_limits_in_window = {
        "recovery": 4,
        "automation": 1,
        "external": 2,
        "insight": 2,
        "other": 2,
    }
    service._context_opportunity_queue = [
        {
            "run_id": "run-automation-class",
            "queued_ts": now - 8.0,
            "attempt": 1,
            "opportunity": {
                "opportunity_type": "workflow_automation",
                "priority": 10,
                "confidence": 0.95,
                "expires_at": "",
            },
        },
        {
            "run_id": "run-recovery-class",
            "queued_ts": now - 2.0,
            "attempt": 1,
            "opportunity": {
                "opportunity_type": "error_detected",
                "priority": 4,
                "confidence": 0.5,
                "expires_at": "",
            },
        },
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-recovery-class"
    assert selected.get("_opportunity_class") == "recovery"


def test_scheduler_allows_starvation_override_when_quota_exceeded() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc).timestamp()
    service._context_opportunity_dispatch_history_by_type = {"workflow_automation": [now - 5.0]}
    service._context_opportunity_per_type_max_in_window = 1
    service._context_opportunity_fairness_window_s = 120.0
    service._context_opportunity_starvation_override_s = 20.0
    service._context_opportunity_queue = [
        {
            "run_id": "run-auto-starved",
            "queued_ts": now - 70.0,
            "attempt": 1,
            "opportunity": {"opportunity_type": "workflow_automation", "priority": 6, "confidence": 0.75, "expires_at": ""},
        }
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-auto-starved"
    assert bool(selected.get("_starvation_override", False)) is True


def test_scheduler_defers_when_same_type_concurrency_reached() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc).timestamp()
    service._context_opportunity_per_type_max_concurrency = 1
    service._context_opportunity_active_runs = {
        "active-1": {"run_id": "active-1", "opportunity_type": "workflow_automation"},
    }
    service._context_opportunity_queue = [
        {
            "run_id": "queued-1",
            "queued_ts": now - 1.0,
            "attempt": 1,
            "opportunity": {"opportunity_type": "workflow_automation", "priority": 8, "confidence": 0.8, "expires_at": ""},
        }
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert selected is None
    assert float(service._context_opportunity_next_ready_at or 0.0) > 0.0


def test_scheduler_multiobjective_prioritizes_deadline_utility_mix() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc)
    service._context_opportunity_per_type_max_in_window = 50
    service._context_opportunity_class_limits_in_window = {
        "recovery": 50,
        "automation": 50,
        "external": 50,
        "insight": 50,
        "other": 50,
    }
    service._context_opportunity_queue = [
        {
            "run_id": "run-base-high-priority",
            "queued_ts": now.timestamp(),
            "attempt": 1,
            "opportunity": {
                "opportunity_type": "workflow_automation",
                "priority": 10,
                "confidence": 0.8,
                "utility": 0.3,
                "risk": "high",
                "estimated_duration_s": 300,
            },
        },
        {
            "run_id": "run-deadline-utility",
            "queued_ts": now.timestamp(),
            "attempt": 1,
            "opportunity": {
                "opportunity_type": "workflow_automation",
                "priority": 7,
                "confidence": 0.6,
                "utility": 1.0,
                "risk": "low",
                "estimated_duration_s": 60,
                "sla_deadline_at": (now + timedelta(seconds=30)).isoformat(),
            },
        },
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-deadline-utility"


def test_scheduler_external_pressure_penalizes_external_class() -> None:
    service = _build_scheduler_service()
    now = datetime.now(timezone.utc).timestamp()
    service._context_opportunity_external_penalty_weight = 7.5
    service._context_opportunity_external_snapshot = {
        "status": "success",
        "global_pressure": 0.95,
        "class_pressure": {
            "external": 0.95,
            "recovery": 0.8,
            "automation": 0.3,
            "insight": 0.2,
            "other": 0.2,
        },
        "provider_pressure_by_name": {"google": 0.98},
    }
    service._context_opportunity_external_last_refresh_ts = now
    service._context_opportunity_external_refresh_s = 180.0
    service._context_opportunity_per_type_max_in_window = 50
    service._context_opportunity_class_limits_in_window = {
        "recovery": 50,
        "automation": 50,
        "external": 50,
        "insight": 50,
        "other": 50,
    }
    service._context_opportunity_queue = [
        {
            "run_id": "run-external-heavy",
            "queued_ts": now - 2.0,
            "attempt": 1,
            "opportunity": {
                "opportunity_type": "external_connector",
                "priority": 10,
                "confidence": 0.95,
                "provider": "google",
            },
        },
        {
            "run_id": "run-recovery",
            "queued_ts": now - 1.0,
            "attempt": 1,
            "opportunity": {
                "opportunity_type": "error_detected",
                "priority": 6,
                "confidence": 0.7,
            },
        },
    ]

    selected = service._pop_next_context_opportunity_job()  # noqa: SLF001

    assert isinstance(selected, dict)
    assert selected.get("run_id") == "run-recovery"
    assert float(selected.get("_external_global_pressure", 0.0) or 0.0) > 0.5


def test_scheduler_external_pressure_scales_effective_class_limits() -> None:
    service = _build_scheduler_service()
    snapshot = {
        "global_pressure": 1.0,
        "class_pressure": {
            "external": 1.0,
            "recovery": 0.9,
            "automation": 0.4,
            "insight": 0.3,
            "other": 0.2,
        },
    }
    service._context_opportunity_class_limits_in_window["external"] = 10
    service._context_opportunity_class_limits_in_window["recovery"] = 4
    service._context_opportunity_class_max_concurrency["external"] = 4
    service._context_opportunity_class_max_concurrency["recovery"] = 2

    external_limit = service._context_opportunity_effective_class_limit("external", pressure_snapshot=snapshot)  # noqa: SLF001
    recovery_limit = service._context_opportunity_effective_class_limit("recovery", pressure_snapshot=snapshot)  # noqa: SLF001
    external_concurrency = service._context_opportunity_effective_class_concurrency(  # noqa: SLF001
        "external",
        pressure_snapshot=snapshot,
    )
    recovery_concurrency = service._context_opportunity_effective_class_concurrency(  # noqa: SLF001
        "recovery",
        pressure_snapshot=snapshot,
    )

    assert external_limit < 10
    assert recovery_limit > 4
    assert external_concurrency <= 4
    assert recovery_concurrency >= 2


def test_context_opportunity_learning_degrades_offsets_on_failures() -> None:
    service = _build_scheduler_service()
    service._context_opportunity_self_tune_min_samples = 1
    service._context_opportunity_self_tune_bad_threshold = 0.0
    service._context_opportunity_self_tune_good_threshold = 0.95

    for _ in range(4):
        service._context_opportunity_record_outcome(  # noqa: SLF001
            opportunity_type="workflow_automation",
            status="verification_failed",
            attempt=2,
            scheduler_score=1.2,
            verification={"status": "failed"},
        )

    assert float(service._context_opportunity_dynamic_priority_offsets_by_type.get("workflow_automation", 0.0)) < 0.0
    assert float(service._context_opportunity_dynamic_retry_penalty_by_type.get("workflow_automation", 0.0)) > 0.0
    assert float(service._context_opportunity_dynamic_class_weight_offsets.get("automation", 0.0)) < 0.0


def test_context_opportunity_learning_promotes_offsets_on_success() -> None:
    service = _build_scheduler_service()
    service._context_opportunity_self_tune_min_samples = 1
    service._context_opportunity_self_tune_bad_threshold = 0.95
    service._context_opportunity_self_tune_good_threshold = 0.0

    for _ in range(4):
        service._context_opportunity_record_outcome(  # noqa: SLF001
            opportunity_type="error_detected",
            status="verified",
            attempt=1,
            scheduler_score=2.1,
            verification={"status": "success"},
        )

    assert float(service._context_opportunity_dynamic_priority_offsets_by_type.get("error_detected", 0.0)) > 0.0
    assert float(service._context_opportunity_dynamic_confidence_offsets_by_type.get("error_detected", 0.0)) > 0.0
    assert float(service._context_opportunity_dynamic_class_weight_offsets.get("recovery", 0.0)) > 0.0


def test_context_opportunity_tuning_state_persistence_roundtrip(tmp_path) -> None:
    state_path = tmp_path / "context_tuning_state.json"
    service = _build_scheduler_service()
    service._context_opportunity_tuning_state_path = str(state_path)
    service._context_opportunity_tuning_persist_every = 1
    service._context_opportunity_self_tune_min_samples = 1
    service._context_opportunity_self_tune_bad_threshold = 0.0
    service._context_opportunity_self_tune_good_threshold = 0.95
    service._context_opportunity_dynamic_class_limit_scale_by_class = {"external": 0.72}
    service._context_opportunity_dynamic_class_concurrency_scale_by_class = {"external": 0.8}
    service._context_opportunity_external_penalty_weight_offset = 0.35
    service._context_opportunity_external_recovery_boost_offset = 0.12
    service._context_opportunity_external_policy_learning_state = {
        "external": {"scope": "external", "samples": 2, "ema_pressure": 0.7}
    }

    service._context_opportunity_record_outcome(  # noqa: SLF001
        opportunity_type="workflow_automation",
        status="verification_failed",
        attempt=2,
        scheduler_score=1.3,
        verification={"status": "failed"},
    )

    assert state_path.exists()
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    dynamic = payload.get("dynamic", {})
    assert "priority_offsets_by_type" in dynamic
    assert "class_limit_scale_by_class" in dynamic
    assert "class_concurrency_scale_by_class" in dynamic
    assert "external_penalty_weight_offset" in dynamic

    cloned = _build_scheduler_service()
    cloned._context_opportunity_tuning_state_path = str(state_path)
    cloned._load_context_opportunity_tuning_state()  # noqa: SLF001

    assert "workflow_automation" in cloned._context_opportunity_dynamic_priority_offsets_by_type
    assert "external" in cloned._context_opportunity_dynamic_class_limit_scale_by_class
    assert float(cloned._context_opportunity_external_penalty_weight_offset) != 0.0
    assert str(cloned._context_opportunity_tuning_last_loaded_at)
