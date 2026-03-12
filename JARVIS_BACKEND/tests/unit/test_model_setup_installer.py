from __future__ import annotations

import base64
import hashlib
import io
import sys
import types
import urllib.request
from pathlib import Path

from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.inference.model_setup_installer import ModelSetupInstaller


def test_model_setup_installer_dry_run_selects_only_auto_ready_items(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    installer = ModelSetupInstaller(history_path="data/install_history.json")
    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "embeddings-main",
                "name": "all-mpnet-base-v2",
                "task": "embedding",
                "path": str(tmp_path / "embeddings" / "all-mpnet-base-v2"),
                "strategy": "huggingface_snapshot",
                "source_ref": "sentence-transformers/all-mpnet-base-v2",
                "automation_ready": True,
            },
            {
                "key": "reasoning-manual",
                "name": "manual-gguf",
                "task": "reasoning",
                "path": str(tmp_path / "reasoning" / "manual.gguf"),
                "strategy": "manual_quantization",
                "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
                "automation_ready": False,
            },
        ],
    }

    payload = installer.install(plan_payload=plan, dry_run=True)

    assert payload["status"] == "success"
    assert payload["selected_count"] == 1
    assert payload["success_count"] == 1
    assert payload["error_count"] == 0
    assert len(payload["items"]) == 1
    assert payload["items"][0]["status"] == "planned"
    assert payload["items"][0]["key"] == "embeddings-main"

    history = installer.history(limit=10)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["items"][0]["run_id"] == payload["run_id"]


def test_model_setup_installer_direct_url_writes_file_and_tracks_latest_history(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    installer = ModelSetupInstaller(history_path="data/install_history.json")

    class _FakeResponse:
        def __init__(self, payload: bytes) -> None:
            self._stream = io.BytesIO(payload)
            self.headers = {"Content-Length": str(len(payload)), "ETag": ""}

        def read(self, size: int = -1) -> bytes:
            return self._stream.read(size)

        def geturl(self) -> str:
            return "https://example.com/models/yolov10x.pt"

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001, ANN201
            return False

    payload_bytes = b"jarvis-model-binary"

    def _fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:
        assert request.full_url == "https://example.com/models/yolov10x.pt"
        assert timeout == 300.0
        return _FakeResponse(payload_bytes)

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "vision-yolo",
                "name": "yolov10x.pt",
                "task": "vision",
                "path": str(tmp_path / "models" / "vision" / "yolov10x.pt"),
                "strategy": "direct_url",
                "source_url": "https://example.com/models/yolov10x.pt",
                "automation_ready": True,
            }
        ],
    }

    first = installer.install(plan_payload=plan, item_keys=["vision-yolo"], dry_run=False)
    assert first["status"] == "success"
    assert first["items"][0]["status"] == "success"
    assert first["items"][0]["integrity_status"] == "observed"
    target_path = tmp_path / "models" / "vision" / "yolov10x.pt"
    assert target_path.read_bytes() == payload_bytes
    assert first["items"][0]["bytes_written"] == len(payload_bytes)

    second = installer.install(plan_payload=plan, item_keys=["vision-yolo"], dry_run=True, force=True)
    history = installer.history(limit=10)
    assert history["count"] == 2
    assert history["items"][0]["run_id"] == second["run_id"]
    assert history["items"][1]["run_id"] == first["run_id"]


def test_model_setup_installer_huggingface_snapshot_uses_snapshot_download(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    installer = ModelSetupInstaller(history_path="data/install_history.json")
    calls: list[dict[str, object]] = []

    fake_module = types.ModuleType("huggingface_hub")

    def _fake_snapshot_download(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(dict(kwargs))
        target = Path(str(kwargs["local_dir"]))
        target.mkdir(parents=True, exist_ok=True)
        (target / "config.json").write_text("{}", encoding="utf-8")
        return str(target)

    fake_module.snapshot_download = _fake_snapshot_download  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)

    target_path = tmp_path / "all_rounder" / "qwen" / "Qwen3.5-9B"
    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "qwen-main",
                "name": "Qwen3.5-9B",
                "task": "reasoning",
                "path": str(target_path),
                "strategy": "huggingface_snapshot",
                "source_ref": "Qwen/Qwen3.5-9B",
                "automation_ready": True,
            }
        ],
    }

    payload = installer.install(plan_payload=plan, item_keys=["qwen-main"])

    assert payload["status"] == "success"
    assert payload["items"][0]["status"] == "success"
    assert payload["items"][0]["integrity_status"] == "observed"
    assert int(payload["items"][0]["verification"]["file_count"]) == 1
    assert calls and calls[0]["repo_id"] == "Qwen/Qwen3.5-9B"
    assert calls[0]["local_dir"] == str(target_path)
    assert calls[0]["local_dir_use_symlinks"] is False
    assert calls[0]["resume_download"] is True
    assert (target_path / "config.json").exists()


def test_model_setup_installer_huggingface_snapshot_passes_configured_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("HUGGINGFACE_HUB_TOKEN", "HF_TOKEN", "HUGGINGFACE_TOKEN"):
        monkeypatch.delenv(env_name, raising=False)
    token = "hf_" + ("A1b2C3d4E5f6G7h8" * 2)
    credentials = ProviderCredentialManager(
        config_path="configs/provider_credentials.json",
        key_store_path="data/provider_keys.json",
    )
    credentials.update_provider_credentials(
        provider="huggingface",
        api_key=token,
        persist_plaintext=True,
        persist_encrypted=False,
        overwrite_env=False,
    )
    installer = ModelSetupInstaller(
        history_path="data/install_history.json",
        provider_credentials=credentials,
    )
    calls: list[dict[str, object]] = []

    fake_module = types.ModuleType("huggingface_hub")

    def _fake_snapshot_download(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(dict(kwargs))
        target = Path(str(kwargs["local_dir"]))
        target.mkdir(parents=True, exist_ok=True)
        (target / "config.json").write_text("{}", encoding="utf-8")
        return str(target)

    fake_module.snapshot_download = _fake_snapshot_download  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)

    target_path = tmp_path / "stt" / "whisper-medium"
    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "whisper-medium",
                "name": "whisper-medium",
                "task": "stt",
                "path": str(target_path),
                "strategy": "huggingface_snapshot",
                "source_ref": "openai/whisper-medium",
                "automation_ready": True,
            }
        ],
    }

    payload = installer.install(plan_payload=plan, item_keys=["whisper-medium"])

    assert payload["status"] == "success"
    assert payload["items"][0]["status"] == "success"
    assert calls and calls[0]["token"] == token


def test_model_setup_installer_direct_url_verifies_trusted_md5_hint(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    installer = ModelSetupInstaller(history_path="data/install_history.json")

    class _FakeResponse:
        def __init__(self, payload: bytes) -> None:
            self._stream = io.BytesIO(payload)
            self.headers = {"Content-Length": str(len(payload)), "ETag": ""}

        def read(self, size: int = -1) -> bytes:
            return self._stream.read(size)

        def geturl(self) -> str:
            return "https://release-assets.githubusercontent.com/yolov10x.pt"

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001, ANN201
            return False

    payload_bytes = b"jarvis-verified-download"
    md5_base64 = base64.b64encode(hashlib.md5(payload_bytes).digest()).decode("ascii")

    def _fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:
        assert timeout == 300.0
        return _FakeResponse(payload_bytes)

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "vision-yolo",
                "name": "yolov10x.pt",
                "task": "vision",
                "path": str(tmp_path / "models" / "vision" / "yolov10x.pt"),
                "strategy": "direct_url",
                "source_url": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
                "automation_ready": True,
            }
        ],
    }
    remote_metadata = {
        "item_map": {
            "vision-yolo": {
                "status": "success",
                "final_url": "https://release-assets.githubusercontent.com/yolov10x.pt",
                "final_host": "release-assets.githubusercontent.com",
                "digest_hints": {"md5": md5_base64},
            }
        }
    }

    payload = installer.install(
        plan_payload=plan,
        item_keys=["vision-yolo"],
        remote_metadata=remote_metadata,
    )

    item = payload["items"][0]
    assert payload["status"] == "success"
    assert item["verified"] is True
    assert item["integrity_status"] == "verified"
    assert item["verification"]["matched_hashes"] == ["md5_base64"]


def test_model_setup_installer_direct_url_rejects_digest_mismatch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    installer = ModelSetupInstaller(history_path="data/install_history.json")

    class _FakeResponse:
        def __init__(self, payload: bytes) -> None:
            self._stream = io.BytesIO(payload)
            self.headers = {"Content-Length": str(len(payload)), "ETag": ""}

        def read(self, size: int = -1) -> bytes:
            return self._stream.read(size)

        def geturl(self) -> str:
            return "https://release-assets.githubusercontent.com/yolov10x.pt"

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001, ANN201
            return False

    def _fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:
        assert timeout == 300.0
        return _FakeResponse(b"jarvis-mismatch")

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    target_path = tmp_path / "models" / "vision" / "yolov10x.pt"
    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "vision-yolo",
                "name": "yolov10x.pt",
                "task": "vision",
                "path": str(target_path),
                "strategy": "direct_url",
                "source_url": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
                "automation_ready": True,
            }
        ],
    }
    remote_metadata = {
        "item_map": {
            "vision-yolo": {
                "status": "success",
                "final_url": "https://release-assets.githubusercontent.com/yolov10x.pt",
                "final_host": "release-assets.githubusercontent.com",
                "digest_hints": {"md5": "AAAAAAAAAAAAAAAAAAAAAA=="},
            }
        }
    }

    payload = installer.install(
        plan_payload=plan,
        item_keys=["vision-yolo"],
        remote_metadata=remote_metadata,
    )

    item = payload["items"][0]
    assert payload["status"] == "error"
    assert item["status"] == "error"
    assert item["verification_failed"] is True
    assert item["integrity_status"] == "mismatch"
    assert not target_path.exists()
