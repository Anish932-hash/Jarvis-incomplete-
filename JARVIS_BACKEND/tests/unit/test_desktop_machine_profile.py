from __future__ import annotations

from backend.python.core.desktop_machine_profile import DesktopMachineProfileManager


def test_desktop_machine_profile_builds_recommendations_and_persists_preferences(tmp_path) -> None:
    manager = DesktopMachineProfileManager(store_path=str(tmp_path / "desktop_machine_profile.json"))

    preferences = manager.update_task_preferences(
        task="reasoning",
        provider="local",
        model_name="qwen3-14b",
        execution_backend="llama_cpp",
        model_path=r"E:\models\qwen3-14b.gguf",
        preferred_runtime="llama_cpp",
        allow_remote=False,
        source="unit_test",
    )
    assert preferences["status"] == "success"
    assert preferences["count"] == 1
    assert preferences["items"][0]["task"] == "reasoning"

    snapshot = manager.build_snapshot(
        system_profile={
            "hostname": "jarvis-dev",
            "windows": {"caption": "Windows 11 Pro", "build_number": "22631"},
            "cpu": {"name": "AMD Ryzen 9", "logical_cores": 24},
            "gpus": [{"name": "RTX 4080", "adapter_ram_bytes": 16 * 1024 * 1024 * 1024}],
            "runtimes": {
                "rustc": {"available": True},
                "cargo": {"available": True},
                "huggingface_cli": {"available": False},
                "hf": {"available": False},
            },
        },
        app_inventory={
            "status": "success",
            "total": 3,
            "path_ready_count": 3,
            "items": [
                {"display_name": "Visual Studio Code", "category": "developer_tool", "usage_score": 15.0, "path_ready": True},
                {"display_name": "Google Chrome", "category": "browser", "usage_score": 11.0, "path_ready": True},
                {"display_name": "Photoshop", "category": "media", "usage_score": 7.0, "path_ready": True},
            ],
        },
        launch_memory={"status": "success", "total": 0, "items": []},
        provider_snapshot={
            "providers": {
                "huggingface": {"present": False, "ready": False, "required_by_manifest": True},
                "groq": {"present": True, "ready": True, "required_by_manifest": False},
            },
            "missing_required_count": 1,
            "manifest_required_providers": ["huggingface"],
        },
        provider_verifications={
            "huggingface": {"status": "error", "verified": False, "summary": "Credential missing"},
            "groq": {"status": "success", "verified": True, "summary": "Verified Groq access"},
        },
        local_models={
            "task_counts": {"reasoning": 1},
            "missing_task_counts": {"vision": 1},
            "inventory": {
                "present_count": 1,
                "items": [
                    {
                        "task": "reasoning",
                        "present": True,
                        "name": "qwen3-14b",
                        "path": r"E:\models\qwen3-14b.gguf",
                        "backend": "llama_cpp",
                    }
                ],
            },
            "bridge_profiles": [
                {
                    "task": "reasoning",
                    "provider": "local",
                    "name": "qwen3-14b",
                    "path": r"E:\models\qwen3-14b.gguf",
                    "execution_backend": "llama_cpp",
                    "launch_ready_count": 1,
                }
            ],
        },
        model_setup_workspace={
            "status": "success",
            "recommendations": [
                {
                    "code": "workspace_refresh",
                    "severity": "low",
                    "title": "Refresh workspace",
                    "message": "Workspace suggests a light refresh.",
                }
            ],
        },
        task_preferences=manager.task_model_preferences(),
        source="unit_test",
    )

    assert snapshot["status"] == "success"
    assert snapshot["models"]["recommended_models"][0]["task"] == "reasoning"
    recommendation_codes = {item["code"] for item in snapshot["recommendations"]}
    assert "configure_huggingface_token" in recommendation_codes
    assert "install_local_vision_model" in recommendation_codes
    assert "run_app_discovery" in recommendation_codes
    assert "install_huggingface_cli" in recommendation_codes

    recorded = manager.record_snapshot(snapshot, source="unit_test")
    assert recorded["source"] == "unit_test"

    latest = manager.latest_snapshot()
    assert latest["machine_id"] == snapshot["machine_id"]
    assert latest["readiness"]["score"] >= 0
