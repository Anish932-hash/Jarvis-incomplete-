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
