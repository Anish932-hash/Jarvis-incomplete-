from __future__ import annotations

from backend.python.desktop_api import DesktopBackendService


class _DummyInstallManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def start(self, **kwargs):  # noqa: ANN003
        self.calls.append(dict(kwargs))
        item_keys = kwargs.get("item_keys") or []
        return {
            "status": "accepted",
            "run": {
                "run_id": "setup-run-1",
                "status": "queued",
                "selected_item_keys": list(item_keys),
            },
        }


def test_model_setup_install_launch_uses_launchable_subset_when_preflight_is_partial() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    manager = _DummyInstallManager()
    service.model_setup_install_manager = manager
    service.model_setup_plan = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "manifest": {"path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt"},
        "items": [
            {"key": "embedding-main", "automation_ready": True},
            {"key": "reasoning-llama", "automation_ready": True},
        ],
    }
    service.model_setup_preflight = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "summary": {"blocked_count": 1, "launchable_count": 1},
        "items": [
            {
                "key": "embedding-main",
                "status": "ready",
                "launch_ready": True,
                "blockers": [],
                "warnings": [],
            },
            {
                "key": "reasoning-llama",
                "status": "blocked",
                "launch_ready": False,
                "remote_probe": {"credential_state": "missing"},
                "blockers": ["Remote source requires a configured Hugging Face access token before automation can download it."],
                "warnings": [],
            },
        ],
    }

    payload = service.model_setup_install_launch(limit=24, refresh_remote=False)

    assert payload["status"] == "accepted"
    assert payload["launch_scope"] == "partial"
    assert payload["launch_item_keys"] == ["embedding-main"]
    assert payload["deferred_item_keys"] == ["reasoning-llama"]
    assert payload["deferred_actions"][0]["kind"] == "configure_provider_credentials"
    assert manager.calls[0]["item_keys"] == ["embedding-main"]
