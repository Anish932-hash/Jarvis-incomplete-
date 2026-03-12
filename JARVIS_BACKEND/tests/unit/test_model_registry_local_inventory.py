from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from backend.python.inference.model_registry import ModelRegistry


class _FakeProviderCredentials:
    def refresh(self, *, overwrite_env: bool = False) -> Dict[str, Any]:  # noqa: ARG002
        return self.snapshot()

    @staticmethod
    def snapshot() -> Dict[str, Any]:
        return {
            "status": "success",
            "provider_count": 3,
            "ready_count": 0,
            "providers": {
                "groq": {"provider": "groq", "present": False, "ready": False, "source": "none"},
                "nvidia": {"provider": "nvidia", "present": False, "ready": False, "source": "none"},
                "elevenlabs": {"provider": "elevenlabs", "present": False, "ready": False, "source": "none"},
            },
        }

    @staticmethod
    def get_api_key(provider: str) -> str:  # noqa: ARG004
        return ""


def test_model_registry_scans_local_models(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    stt_dir = tmp_path / "stt" / "whisper-medium"
    stt_dir.mkdir(parents=True, exist_ok=True)
    (stt_dir / "config.json").write_text("{}", encoding="utf-8")
    (stt_dir / "model.safetensors").write_bytes(b"weights")

    reasoning_dir = tmp_path / "reasoning"
    reasoning_dir.mkdir(parents=True, exist_ok=True)
    (reasoning_dir / "tiny-model.gguf").write_bytes(b"gguf")

    registry = ModelRegistry(scan_local_models=True, enforce_provider_keys=False)
    inventory = registry.local_inventory_snapshot(limit=100)

    assert inventory["status"] == "success"
    assert int(inventory["count"]) >= 2

    stt_models = registry.list_by_task("stt")
    assert any(model.name.startswith("local-auto-stt-") for model in stt_models)

    reasoning_models = registry.list_by_task("reasoning")
    assert any(model.name.startswith("local-auto-reasoning-") for model in reasoning_models)


def test_model_registry_scans_all_rounder_hf_directory(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    qwen_dir = tmp_path / "all_rounder" / "qwen" / "Qwen3-14B"
    qwen_dir.mkdir(parents=True, exist_ok=True)
    (qwen_dir / "config.json").write_text("{}", encoding="utf-8")
    (qwen_dir / "model.safetensors.index.json").write_text("{}", encoding="utf-8")
    (qwen_dir / "model-00001-of-00002.safetensors").write_bytes(b"weights")

    registry = ModelRegistry(scan_local_models=True, enforce_provider_keys=False)
    inventory = registry.local_inventory_snapshot(task="reasoning", limit=200)

    assert inventory["status"] == "success"
    paths = {str(row.get("path", "")).replace("\\", "/").lower() for row in inventory.get("items", [])}
    assert any("/all_rounder/" in path and "/qwen3-14b" in path for path in paths)

    models = registry.list_by_task("reasoning")
    names = {model.name for model in models}
    assert "local-auto-reasoning-qwen3-14b" in names


def test_model_registry_enforce_provider_keys_keeps_local_reasoning() -> None:
    fake_provider = _FakeProviderCredentials()
    registry = ModelRegistry(
        provider_credentials=fake_provider,  # type: ignore[arg-type]
        enforce_provider_keys=True,
        scan_local_models=False,
    )

    reasoning_models = registry.list_by_task("reasoning")
    providers = {model.provider for model in reasoning_models}
    assert providers == {"local"}

    provider_status = registry.provider_status_snapshot()
    assert provider_status["groq"]["ready"] is False
    assert provider_status["nvidia"]["ready"] is False


def test_model_registry_detects_embedding_and_intent_assets_and_capabilities(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    embedding_dir = tmp_path / "embeddings" / "all-mpnet-base-v2(Embeddings_model)"
    embedding_dir.mkdir(parents=True, exist_ok=True)
    (embedding_dir / "config.json").write_text("{}", encoding="utf-8")
    (embedding_dir / "modules.json").write_text("[]", encoding="utf-8")

    intent_dir = tmp_path / "custom_intents" / "bart-large-mnli (Custom_intent_model)"
    intent_dir.mkdir(parents=True, exist_ok=True)
    (intent_dir / "config.json").write_text("{}", encoding="utf-8")

    registry = ModelRegistry(scan_local_models=True, enforce_provider_keys=False)

    embedding_models = registry.list_by_task("embedding")
    intent_models = registry.list_by_task("intent")
    assert any(model.name.startswith("local-auto-embedding-") for model in embedding_models)
    assert any(model.name.startswith("local-auto-intent-") for model in intent_models)

    capability_summary = registry.capability_summary(limit_per_task=3)
    tasks = {str(item.get("task", "")).strip().lower(): item for item in capability_summary.get("tasks", [])}
    assert "embedding" in tasks
    assert "intent" in tasks
    assert int(tasks["embedding"].get("inventory_count", 0)) >= 1
    assert int(tasks["intent"].get("inventory_count", 0)) >= 1


def test_model_registry_merges_manifest_declared_missing_and_present_assets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    present_reasoning = tmp_path / "reasoning" / "Qwen2.5-14B-Instruct-Q8_0.gguf"
    present_reasoning.parent.mkdir(parents=True, exist_ok=True)
    present_reasoning.write_bytes(b"gguf")

    missing_tts = tmp_path / "tts" / "Kokoro-82M"
    manifest_dir = tmp_path / "JARVIS_BACKEND"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "Models to Download.txt").write_text(
        "\n".join(
            [
                "Name, the models according to your PC, and create these Folders, along with the path you have to keep, these local AI models:",
                f'"{present_reasoning}"',
                f'"{missing_tts}"',
                "",
                "And these API keys:",
                "1)Groq",
                "2)ElevenLabs",
            ]
        ),
        encoding="utf-8",
    )

    registry = ModelRegistry(scan_local_models=True, enforce_provider_keys=False)
    inventory = registry.local_inventory_snapshot(limit=100)

    assert inventory["status"] == "success"
    assert int(inventory.get("declared_count", 0)) >= 2
    assert int(inventory.get("missing_count", 0)) >= 1
    assert inventory.get("manifest", {}).get("provider_count") == 2

    rows_by_path = {
        str(row.get("path", "")).lower(): row
        for row in inventory.get("items", [])
        if isinstance(row, dict)
    }

    present_row = rows_by_path[str(present_reasoning.resolve()).lower()]
    assert present_row["declared"] is True
    assert present_row["present"] is True
    assert present_row["missing"] is False
    assert present_row["detected"] is True

    missing_row = rows_by_path[str(missing_tts.resolve()).lower()]
    assert missing_row["declared"] is True
    assert missing_row["present"] is False
    assert missing_row["missing"] is True
    assert missing_row["detected"] is False

    tts_models = registry.list_by_task("tts")
    names = {model.name for model in tts_models}
    assert "local-auto-tts-kokoro-82m" not in names
