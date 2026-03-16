from __future__ import annotations

from backend.python.perception.context_engine import ContextEngine


class _VisionStub:
    def capture_screen(self):  # noqa: ANN001
        raise AssertionError("capture_screen should not be called in this test")


class _StateStub:
    def update(self, _payload):  # noqa: ANN001
        return None


class _TelemetryStub:
    def emit(self, *_args, **_kwargs):  # noqa: ANN001
        return None


def test_context_engine_get_surface_summary_uses_active_window_and_accessibility(monkeypatch) -> None:
    engine = ContextEngine(
        vision_engine=_VisionStub(),
        desktop_state=_StateStub(),
        runtime_memory=object(),
        episodic_memory=object(),
        telemetry=_TelemetryStub(),
    )

    engine.window_manager = type(
        "_WindowManagerStub",
        (),
        {
            "get_active_window": lambda self: {  # noqa: ARG005
                "title": "Bluetooth & devices - Settings",
                "app_name": "systemsettings",
                "window_signature": "systemsettings|applicationframewindow|1440x900|bluetooth_devices_settings",
                "surface_hints": {"settings_like": True},
            }
        },
    )()

    from backend.python.tools.accessibility_tools import AccessibilityTools

    def _fake_surface_summary(cls, **_kwargs):  # type: ignore[no-untyped-def]
        return {
            "status": "success",
            "element_count": 12,
            "surface_flags": {
                "navigation_tree_visible": True,
                "list_surface_visible": True,
                "form_surface_visible": True,
                "settings_surface_visible": True,
                "dialog_visible": False,
                "toolbar_visible": False,
                "data_table_visible": False,
                "text_entry_surface_visible": True,
                "value_control_visible": False,
                "search_surface_visible": True,
            },
            "recommended_actions": ["select_tree_item", "set_field_value"],
            "query_candidates": [{"element_id": "uia_bluetooth", "name": "Bluetooth", "control_type": "CheckBox", "match_score": 0.91}],
            "control_inventory": [],
            "surface_role_candidates": ["settings", "navigator"],
        }

    monkeypatch.setattr(AccessibilityTools, "surface_summary", classmethod(_fake_surface_summary))

    payload = engine.get_surface_summary(query="Bluetooth")

    assert payload["surface_role"] == "settings"
    assert payload["query_resolution"]["best_candidate_name"] == "Bluetooth"
    assert engine._latest_surface_analysis["surface_role"] == "settings"  # noqa: SLF001
