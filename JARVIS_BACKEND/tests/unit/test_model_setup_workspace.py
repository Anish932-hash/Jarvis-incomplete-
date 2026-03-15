from __future__ import annotations

from pathlib import Path

from backend.python.inference.model_requirement_manifest import load_model_requirement_manifest
from backend.python.inference.model_setup_workspace import build_model_setup_workspace
from backend.python.inference.model_setup_workspace import scaffold_model_setup_workspace


def _provider_snapshot() -> dict:
    return {
        "status": "success",
        "providers": {
            "groq": {
                "provider": "groq",
                "present": False,
                "ready": False,
                "source": "none",
                "missing_requirements": [],
            },
            "elevenlabs": {
                "provider": "elevenlabs",
                "present": True,
                "ready": False,
                "source": "config",
                "missing_requirements": ["ELEVENLABS_VOICE_ID"],
                "verification_status": "success",
                "verification_verified": True,
                "verification_checked_at": "2026-03-15T08:00:00+00:00",
                "verification_summary": "Verified ElevenLabs access.",
                "last_verification": {"status": "success", "verified": True},
            },
        },
    }


def test_model_setup_workspace_summarizes_manifest_directories_and_required_providers(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    (tmp_path / "reasoning").mkdir(parents=True, exist_ok=True)
    manifest_dir = tmp_path / "JARVIS_BACKEND"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "Models to Download.txt").write_text(
        "\n".join(
            [
                "these local AI models:",
                "1)reasoning",
                "2)tts",
                f'"{tmp_path / "reasoning" / "Qwen2.5-14B-Instruct-Q8_0.gguf"}"',
                f'"{tmp_path / "tts" / "Kokoro-82M"}"',
                "",
                "And these API keys:",
                "1)Groq",
                "2)ElevenLabs",
            ]
        ),
        encoding="utf-8",
    )

    manifest = load_model_requirement_manifest()
    workspace = build_model_setup_workspace(
        manifest_payload=manifest,
        provider_snapshot=_provider_snapshot(),
    )

    assert workspace["status"] == "success"
    assert workspace["summary"]["present_directory_count"] == 1
    assert workspace["summary"]["missing_directory_count"] == 1
    assert workspace["summary"]["missing_required_provider_count"] == 2
    assert workspace["summary"]["missing_model_count"] == 2
    assert workspace["summary"]["workspace_ready"] is False
    assert workspace["summary"]["stack_ready"] is False
    assert int(workspace["summary"]["readiness_score"]) < 100
    assert len(workspace["directory_actions"]) == 1
    elevenlabs = next(row for row in workspace["required_providers"] if str(row.get("provider", "")) == "elevenlabs")
    assert elevenlabs["verification_verified"] is True
    assert elevenlabs["verification_checked_at"] == "2026-03-15T08:00:00+00:00"
    assert any("Create 1 missing manifest directory" in recommendation for recommendation in workspace["recommendations"])


def test_model_setup_workspace_scaffold_creates_missing_directories(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    manifest_dir = tmp_path / "JARVIS_BACKEND"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    target_root = tmp_path / "embeddings"
    (manifest_dir / "Models to Download.txt").write_text(
        "\n".join(
            [
                "these local AI models:",
                "1)embeddings",
                "",
                "And these API keys:",
                "1)Groq",
            ]
        ),
        encoding="utf-8",
    )

    manifest = load_model_requirement_manifest()
    preview = scaffold_model_setup_workspace(
        manifest_payload=manifest,
        provider_snapshot=_provider_snapshot(),
        dry_run=True,
    )
    assert preview["status"] == "success"
    assert preview["dry_run"] is True
    assert preview["created_count"] == 0
    assert preview["action_count"] == 1
    assert preview["actions"][0]["status"] == "planned"
    assert target_root.exists() is False

    applied = scaffold_model_setup_workspace(
        manifest_payload=manifest,
        provider_snapshot=_provider_snapshot(),
        dry_run=False,
    )
    assert applied["status"] == "success"
    assert applied["dry_run"] is False
    assert applied["created_count"] == 1
    assert applied["error_count"] == 0
    assert applied["actions"][0]["status"] == "created"
    assert target_root.is_dir() is True
