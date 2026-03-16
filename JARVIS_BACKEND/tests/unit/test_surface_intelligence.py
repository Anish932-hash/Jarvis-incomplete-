from __future__ import annotations

from backend.python.perception.surface_intelligence import SurfaceIntelligenceAnalyzer


def test_surface_intelligence_analyzer_prefers_settings_navigation_mode() -> None:
    analyzer = SurfaceIntelligenceAnalyzer()
    payload = analyzer.analyze(
        window={
            "title": "Bluetooth & devices - Settings",
            "app_name": "systemsettings",
            "window_signature": "systemsettings|applicationframewindow|1440x900|bluetooth_devices_settings",
            "surface_hints": {"settings_like": True},
        },
        surface_summary={
            "element_count": 18,
            "surface_flags": {
                "navigation_tree_visible": True,
                "list_surface_visible": True,
                "form_surface_visible": True,
                "settings_surface_visible": True,
                "dialog_visible": False,
                "toolbar_visible": False,
                "data_table_visible": False,
                "text_entry_surface_visible": True,
                "value_control_visible": True,
                "search_surface_visible": True,
            },
            "recommended_actions": ["select_tree_item", "set_field_value", "set_value_control"],
            "query_candidates": [{"element_id": "uia_bluetooth", "name": "Bluetooth", "control_type": "CheckBox", "match_score": 0.88}],
            "control_inventory": [{"name": "Apply", "control_type": "Button"}],
            "surface_role_candidates": ["settings", "navigator", "form"],
        },
        visual_context=None,
        query="Bluetooth",
    )

    assert payload["surface_role"] == "settings"
    assert payload["interaction_mode"] == "settings_navigation"
    assert payload["grounding_confidence"] > 0.5
    assert "select_query_target" in payload["affordances"]
    assert payload["query_resolution"]["best_candidate_name"] == "Bluetooth"
