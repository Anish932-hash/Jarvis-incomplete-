from __future__ import annotations

import time

from backend.python.core.desktop_state import DesktopState
from backend.python.core.episodic_memory import EpisodicMemory
from backend.python.core.runtime_memory import RuntimeMemory
from backend.python.core.telemetry import Telemetry
from backend.python.perception.context_engine import ActivitySnapshot, ActivityType, ContextEngine, FocusMode


class _StubVisionEngine:
    def capture_screen(self):
        return object()

    def compare_screens(self, old, new):  # noqa: ANN001
        del old, new
        return {"changed": False, "change_percentage": 0.0}

    def analyze_screen_context(self, *args, **kwargs):  # noqa: ANN002, ANN003
        del args, kwargs
        return None


def _make_engine(tmp_path) -> ContextEngine:
    return ContextEngine(
        vision_engine=_StubVisionEngine(),
        desktop_state=DesktopState(store_path=str(tmp_path / "desktop_state.jsonl"), max_items=200),
        runtime_memory=RuntimeMemory(store_path=str(tmp_path / "runtime_memory.jsonl"), max_items=200),
        episodic_memory=EpisodicMemory(store_path=str(tmp_path / "episodic_memory.jsonl"), max_items=300, embedding_dim=64),
        telemetry=Telemetry(max_events=500),
        monitoring_interval=1.0,
    )


def test_context_engine_typing_fallback_classification(tmp_path) -> None:
    engine = _make_engine(tmp_path)
    engine._keystroke_count = 40  # noqa: SLF001
    engine._last_activity_time = time.time()  # noqa: SLF001

    activity = engine._classify_activity(  # noqa: SLF001
        app_name="unknown",
        window_title="",
        screen_changed=True,
        cpu_usage=10.0,
    )
    assert activity == ActivityType.TYPING


def test_context_engine_activity_trend_computes_anomaly_score(tmp_path) -> None:
    engine = _make_engine(tmp_path)
    now = time.time()

    # Baseline: mostly browsing, low CPU.
    for index in range(24):
        engine._activity_history.append(  # noqa: SLF001
            ActivitySnapshot(
                timestamp=now - (3600 - (index * 80)),
                activity_type=ActivityType.BROWSING,
                focus_mode=FocusMode.AVAILABLE,
                active_window_title="Browser",
                active_application="chrome",
                visual_context=None,
                typing_speed=10.0,
                mouse_activity=20.0,
                cpu_usage=18.0,
                memory_usage=35.0,
                screen_changed=bool(index % 2),
                confidence=0.9,
            )
        )

    # Current window: coding, high CPU, more changes.
    for index in range(12):
        engine._activity_history.append(  # noqa: SLF001
            ActivitySnapshot(
                timestamp=now - (index * 40),
                activity_type=ActivityType.CODING,
                focus_mode=FocusMode.DEEP_WORK,
                active_window_title="VS Code",
                active_application="vscode",
                visual_context=None,
                typing_speed=95.0,
                mouse_activity=45.0,
                cpu_usage=72.0,
                memory_usage=58.0,
                screen_changed=True,
                confidence=0.92,
            )
        )

    trend = engine.get_activity_trend(window_minutes=20, baseline_minutes=120)
    assert trend["status"] == "success"
    assert float(trend["anomaly_score"]) >= 0.2
    assert trend["mode"] in {"stable", "shifting", "volatile"}


def test_context_engine_predict_next_intents_from_activity_transitions(tmp_path) -> None:
    engine = _make_engine(tmp_path)
    now = time.time()

    for index in range(16):
        if index % 2 == 0:
            activity = ActivityType.BROWSING
            focus = FocusMode.AVAILABLE
            app = "chrome"
            title = "Browser"
        else:
            activity = ActivityType.CODING
            focus = FocusMode.FOCUSED
            app = "vscode"
            title = "VS Code"
        engine._activity_history.append(  # noqa: SLF001
            ActivitySnapshot(
                timestamp=now - (300 - (index * 12)),
                activity_type=activity,
                focus_mode=focus,
                active_window_title=title,
                active_application=app,
                visual_context=None,
                typing_speed=55.0 if activity == ActivityType.CODING else 14.0,
                mouse_activity=32.0,
                cpu_usage=48.0 if activity == ActivityType.CODING else 20.0,
                memory_usage=41.0,
                screen_changed=True,
                confidence=0.9,
            )
        )

    prediction = engine.predict_next_intents(window=60, min_support=3, top_k=3)
    assert prediction["status"] == "success"
    rows = prediction.get("predictions", [])
    assert isinstance(rows, list) and rows
    assert str(rows[0].get("activity", "")) in {"coding", "browsing"}
    assert float(rows[0].get("probability", 0.0)) > 0.0


def test_context_engine_assistance_contract_respects_focus_and_priority(tmp_path) -> None:
    engine = _make_engine(tmp_path)
    now = time.time()
    engine._activity_history.append(  # noqa: SLF001
        ActivitySnapshot(
            timestamp=now,
            activity_type=ActivityType.CODING,
            focus_mode=FocusMode.DEEP_WORK,
            active_window_title="VS Code",
            active_application="vscode",
            visual_context=None,
            typing_speed=95.0,
            mouse_activity=35.0,
            cpu_usage=64.0,
            memory_usage=52.0,
            screen_changed=True,
            confidence=0.92,
        )
    )

    low_priority = engine.assistance_contract(priority=4)
    assert low_priority["status"] == "success"
    assert bool(low_priority["allowed"]) is False

    high_priority = engine.assistance_contract(priority=9)
    assert high_priority["status"] == "success"
    assert bool(high_priority["allowed"]) is True
