from __future__ import annotations

from types import SimpleNamespace

from backend.python.tools.accessibility_tools import AccessibilityTools


class _Rect:
    def __init__(self, left: int, top: int, width: int, height: int) -> None:
        self.left = left
        self.top = top
        self._width = width
        self._height = height

    def width(self) -> int:
        return self._width

    def height(self) -> int:
        return self._height


class _Element:
    handle = 77

    def __init__(self) -> None:
        self.element_info = SimpleNamespace(
            control_type="Slider",
            automation_id="brightness_slider",
            class_name="SliderControl",
            name="Brightness",
        )
        self.iface_selection_item = SimpleNamespace(CurrentIsSelected=False)
        self.iface_toggle = SimpleNamespace(CurrentToggleState=1)
        self.iface_expand_collapse = SimpleNamespace(CurrentExpandCollapseState=1)
        self.iface_range_value = SimpleNamespace(CurrentValue=75.0, CurrentMinimum=0.0, CurrentMaximum=100.0)

    def window_text(self) -> str:
        return "Brightness"

    def rectangle(self) -> _Rect:
        return _Rect(10, 20, 140, 18)

    def is_enabled(self) -> bool:
        return True

    def is_visible(self) -> bool:
        return True


def test_accessibility_tools_serialize_element_captures_state_metadata() -> None:
    payload = AccessibilityTools._serialize_element(_Element(), parent_id="root")  # noqa: SLF001

    assert payload["control_type"] == "Slider"
    assert payload["automation_id"] == "brightness_slider"
    assert payload["checked"] is True
    assert payload["expanded"] is True
    assert payload["range_value"] == 75
    assert payload["range_min"] == 0
    assert payload["range_max"] == 100
    assert payload["state_text"] == "not selected checked on expanded value 75"


def test_accessibility_tools_find_element_matches_automation_and_state_fields(monkeypatch) -> None:
    def _fake_list_elements(cls, **_kwargs):  # type: ignore[no-untyped-def]
        return {
            "status": "success",
            "items": [
                {
                    "name": "Theme",
                    "automation_id": "dark_mode_radio",
                    "control_type": "RadioButton",
                    "state_text": "selected",
                },
                {
                    "name": "Brightness",
                    "automation_id": "brightness_slider",
                    "control_type": "Slider",
                    "value_text": "75",
                },
            ],
        }

    monkeypatch.setattr(AccessibilityTools, "list_elements", classmethod(_fake_list_elements))

    radio_result = AccessibilityTools.find_element(query="dark mode", max_results=3)
    value_result = AccessibilityTools.find_element(query="75", max_results=3)

    assert radio_result["status"] == "success"
    assert radio_result["items"][0]["automation_id"] == "dark_mode_radio"
    assert value_result["status"] == "success"
    assert value_result["items"][0]["automation_id"] == "brightness_slider"


def test_accessibility_tools_surface_summary_infers_navigation_and_form_signals(monkeypatch) -> None:
    def _fake_list_elements(cls, **_kwargs):  # type: ignore[no-untyped-def]
        return {
            "status": "success",
            "items": [
                {"name": "Settings", "control_type": "Window", "window_title": "Settings", "root_window_title": "Settings"},
                {"name": "Bluetooth & devices", "control_type": "TreeItem", "window_title": "Settings", "root_window_title": "Settings"},
                {"name": "Device name", "control_type": "Edit", "window_title": "Settings", "root_window_title": "Settings"},
                {"name": "Bluetooth", "control_type": "CheckBox", "checked": True, "window_title": "Settings", "root_window_title": "Settings"},
                {"name": "Apply", "control_type": "Button", "window_title": "Settings", "root_window_title": "Settings"},
            ],
        }

    def _fake_find_element(cls, **_kwargs):  # type: ignore[no-untyped-def]
        return {
            "status": "success",
            "items": [
                {
                    "element_id": "uia_bluetooth",
                    "name": "Bluetooth",
                    "control_type": "CheckBox",
                    "match_score": 0.92,
                }
            ],
        }

    monkeypatch.setattr(AccessibilityTools, "list_elements", classmethod(_fake_list_elements))
    monkeypatch.setattr(AccessibilityTools, "find_element", classmethod(_fake_find_element))

    summary = AccessibilityTools.surface_summary(window_title="Settings", query="Bluetooth")

    assert summary["status"] == "success"
    assert summary["surface_flags"]["navigation_tree_visible"] is True
    assert summary["surface_flags"]["form_surface_visible"] is True
    assert summary["surface_role_candidates"][0] in {"navigator", "settings", "form"}
    assert "select_query_target" in summary["recommended_actions"]
    assert summary["query_candidates"][0]["element_id"] == "uia_bluetooth"


def test_accessibility_tools_summarize_rows_reuses_live_rows_for_query_ranking() -> None:
    summary = AccessibilityTools.summarize_rows(
        rows=[
            {
                "element_id": "uia_sidebar_bluetooth",
                "name": "Bluetooth",
                "control_type": "ListItem",
                "automation_id": "settings_bluetooth",
                "root_window_title": "Settings",
                "window_title": "Settings",
            },
            {
                "element_id": "uia_device_name",
                "name": "Device name",
                "control_type": "Edit",
                "root_window_title": "Settings",
                "window_title": "Settings",
            },
            {
                "element_id": "uia_apply",
                "name": "Apply",
                "control_type": "Button",
                "root_window_title": "Settings",
                "window_title": "Settings",
            },
        ],
        window_title="Settings",
        query="Bluetooth",
    )

    assert summary["status"] == "success"
    assert summary["query_candidates"][0]["element_id"] == "uia_sidebar_bluetooth"
    assert summary["surface_flags"]["list_surface_visible"] is True
    assert "select_query_target" in summary["recommended_actions"]
