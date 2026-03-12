from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from backend.python.inference.model_requirement_manifest import load_model_requirement_manifest
from backend.python.inference.model_setup_planner import build_model_setup_plan


def _provider_snapshot() -> Dict[str, Any]:
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
                "present": False,
                "ready": False,
                "source": "none",
                "missing_requirements": ["ELEVENLABS_VOICE_ID"],
            },
            "huggingface": {
                "provider": "huggingface",
                "present": False,
                "ready": False,
                "source": "none",
                "missing_requirements": [],
            },
        },
        "storage": {
            "status": "success",
            "keystore_enabled": False,
        },
    }


def test_model_setup_plan_classifies_known_and_manual_manifest_entries(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    embedding_dir = tmp_path / "embeddings" / "all-mpnet-base-v2(Embeddings_model)"
    reasoning_file = tmp_path / "reasoning" / "Meta-Llama-3.1-8B-Instruct-Q8_0.gguf"
    vision_file = tmp_path / "JARVIS_BACKEND" / "models" / "vision" / "sam_vit_h_4b8939.pth"

    manifest_dir = tmp_path / "JARVIS_BACKEND"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "Models to Download.txt").write_text(
        "\n".join(
            [
                "Name, the models according to your PC, and create these Folders, along with the path you have to keep, these local AI models:",
                f'"{embedding_dir}"',
                f'"{reasoning_file}"',
                f'"{vision_file}"',
                "",
                "And these API keys:",
                "1) Groq",
                "2) ElevenLabs",
            ]
        ),
        encoding="utf-8",
    )

    manifest = load_model_requirement_manifest()
    plan = build_model_setup_plan(
        manifest_payload=manifest,
        provider_snapshot=_provider_snapshot(),
        limit=20,
    )

    assert plan["status"] == "success"
    assert int(plan["summary"]["planned_count"]) == 3

    items = [item for item in plan.get("items", []) if isinstance(item, dict)]
    embedding_item = next(item for item in items if "all-mpnet-base-v2" in str(item.get("path", "")).lower())
    llama_item = next(item for item in items if "meta-llama-3.1-8b-instruct-q8_0.gguf" in str(item.get("path", "")).lower())
    sam_item = next(item for item in items if str(item.get("path", "")).lower().endswith("sam_vit_h_4b8939.pth"))

    assert embedding_item["strategy"] == "huggingface_snapshot"
    assert str(embedding_item["source_url"]).startswith("https://huggingface.co/sentence-transformers/")
    assert isinstance(embedding_item.get("commands", []), list) and embedding_item["commands"]

    assert llama_item["strategy"] == "manual_quantization"
    assert any("GGUF" in str(note) for note in llama_item.get("notes", []))
    assert llama_item["blockers"]

    assert sam_item["strategy"] == "direct_url"
    assert str(sam_item["source_url"]).startswith("https://dl.fbaipublicfiles.com/segment_anything/")

    providers = {
        str(item.get("provider", "")).strip().lower(): item
        for item in plan.get("providers", [])
        if isinstance(item, dict)
    }
    assert "elevenlabs" in providers
    assert "huggingface" in providers
    assert providers["huggingface"]["optional"] is True
    assert providers["huggingface"]["credential_label"] == "Access Token"
    assert any(
        str(field.get("name", "")).strip() == "ELEVENLABS_VOICE_ID"
        for field in providers["elevenlabs"].get("fields", [])
        if isinstance(field, dict)
    )
    assert any(
        str(field.get("label", "")).strip() == "Access Token"
        for field in providers["huggingface"].get("fields", [])
        if isinstance(field, dict)
    )
