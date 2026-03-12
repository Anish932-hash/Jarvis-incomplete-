from __future__ import annotations

import shutil
from pathlib import Path

from backend.python.inference.model_setup_preflight import build_model_setup_preflight


def test_model_setup_preflight_reports_ready_for_trusted_writable_item(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    plan = {
        "items": [
            {
                "key": "vision-sam",
                "name": "sam_vit_h_4b8939.pth",
                "task": "vision",
                "path": str(tmp_path / "models" / "vision" / "sam_vit_h_4b8939.pth"),
                "strategy": "direct_url",
                "automation_ready": True,
                "matched_rule": "vision-sam-vit-h",
                "source_kind": "direct_url",
                "source_ref": "https://dl.fbaipublicfiles.com/segment_anything/sam_vit_h_4b8939.pth",
                "source_url": "https://dl.fbaipublicfiles.com/segment_anything/sam_vit_h_4b8939.pth",
                "family": "segment-anything",
                "blockers": [],
            }
        ]
    }

    monkeypatch.setattr(shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100_000_000_000, 20_000_000_000, 80_000_000_000))

    payload = build_model_setup_preflight(plan_payload=plan)

    assert payload["status"] == "success"
    assert int(payload["summary"]["blocked_count"]) == 0
    assert int(payload["summary"]["ready_count"]) == 1
    row = payload["items"][0]
    assert row["status"] == "ready"
    assert row["source_trust"]["trusted"] is True
    assert row["disk"]["enough_space"] is True


def test_model_setup_preflight_blocks_untrusted_or_low_disk_item(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    plan = {
        "items": [
            {
                "key": "vision-custom",
                "name": "custom-weight.bin",
                "task": "vision",
                "path": str(tmp_path / "models" / "vision" / "custom-weight.bin"),
                "strategy": "direct_url",
                "automation_ready": True,
                "matched_rule": "custom-manual",
                "source_kind": "direct_url",
                "source_ref": "http://example.com/custom-weight.bin",
                "source_url": "http://example.com/custom-weight.bin",
                "family": "custom",
                "blockers": [],
            }
        ]
    }

    monkeypatch.setattr(shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(10_000_000_000, 9_600_000_000, 400_000_000))

    payload = build_model_setup_preflight(plan_payload=plan)

    assert payload["status"] == "success"
    assert int(payload["summary"]["blocked_count"]) == 1
    row = payload["items"][0]
    assert row["status"] == "blocked"
    assert row["source_trust"]["trusted"] is False
    assert row["blockers"]


def test_model_setup_preflight_prefers_remote_size_and_redirect_host(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    plan = {
        "items": [
            {
                "key": "vision-yolo",
                "name": "yolov10x.pt",
                "task": "vision",
                "path": str(tmp_path / "models" / "vision" / "yolov10x.pt"),
                "strategy": "direct_url",
                "automation_ready": True,
                "matched_rule": "vision-yolov10x",
                "source_kind": "direct_url",
                "source_ref": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
                "source_url": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
                "family": "vision",
                "blockers": [],
            }
        ]
    }
    remote_metadata = {
        "status": "success",
        "item_map": {
            "vision-yolo": {
                "status": "success",
                "probe_mode": "head",
                "final_url": "https://release-assets.githubusercontent.com/yolov10x.pt",
                "final_host": "release-assets.githubusercontent.com",
                "size_bytes": 250_000_000,
                "cached": False,
            }
        },
    }

    monkeypatch.setattr(shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100_000_000_000, 20_000_000_000, 80_000_000_000))

    payload = build_model_setup_preflight(plan_payload=plan, remote_metadata=remote_metadata)

    row = payload["items"][0]
    assert row["status"] == "ready"
    assert int(row["disk"]["required_bytes"]) == 250_000_000
    assert row["source_trust"]["trusted"] is True
    assert str(row["source_trust"]["resolved_host"]) == "release-assets.githubusercontent.com"
    assert row["remote_probe"]["size_known"] is True
    assert int(payload["summary"]["remote_known_size_count"]) == 1


def test_model_setup_preflight_blocks_gated_huggingface_repo(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    plan = {
        "items": [
            {
                "key": "tts-kokoro",
                "name": "Kokoro-82M",
                "task": "tts",
                "path": str(tmp_path / "tts" / "Kokoro-82M"),
                "strategy": "huggingface_snapshot",
                "automation_ready": True,
                "matched_rule": "tts-kokoro",
                "source_kind": "huggingface",
                "source_ref": "hexgrad/Kokoro-82M",
                "source_url": "https://huggingface.co/hexgrad/Kokoro-82M",
                "family": "tts",
                "blockers": [],
            }
        ]
    }
    remote_metadata = {
        "status": "success",
        "item_map": {
            "tts-kokoro": {
                "status": "success",
                "probe_mode": "api",
                "repo_id": "hexgrad/Kokoro-82M",
                "gated": True,
                "private": False,
                "requires_auth": True,
                "auth_configured": False,
                "auth_used": False,
                "size_bytes": 650_000_000,
            }
        },
    }

    monkeypatch.setattr(shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100_000_000_000, 20_000_000_000, 80_000_000_000))

    payload = build_model_setup_preflight(plan_payload=plan, remote_metadata=remote_metadata)

    row = payload["items"][0]
    assert row["status"] == "blocked"
    assert any("access token" in str(message).lower() for message in row["blockers"])
    assert row["source_trust"]["trusted"] is True


def test_model_setup_preflight_allows_gated_huggingface_repo_when_auth_is_configured(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    plan = {
        "items": [
            {
                "key": "reasoning-llama",
                "name": "Llama-3.1-8B-Instruct",
                "task": "reasoning",
                "path": str(tmp_path / "reasoning" / "Llama-3.1-8B-Instruct"),
                "strategy": "huggingface_snapshot",
                "automation_ready": True,
                "matched_rule": "reasoning-meta-llama-3.1-8b-gguf",
                "source_kind": "huggingface",
                "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
                "source_url": "https://huggingface.co/meta-llama/Llama-3.1-8B-Instruct",
                "family": "llama",
                "blockers": [],
            }
        ]
    }
    remote_metadata = {
        "status": "success",
        "item_map": {
            "reasoning-llama": {
                "status": "success",
                "probe_mode": "api",
                "repo_id": "meta-llama/Llama-3.1-8B-Instruct",
                "gated": True,
                "private": False,
                "requires_auth": True,
                "auth_configured": True,
                "auth_used": True,
                "size_bytes": 1_850_000_000,
            }
        },
    }

    monkeypatch.setattr(shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100_000_000_000, 20_000_000_000, 80_000_000_000))

    payload = build_model_setup_preflight(plan_payload=plan, remote_metadata=remote_metadata)

    row = payload["items"][0]
    assert row["status"] == "ready"
    assert row["remote_probe"]["auth_configured"] is True
    assert not any("access token" in str(message).lower() for message in row["blockers"])


def test_model_setup_preflight_blocks_when_configured_huggingface_token_lacks_repo_access(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    plan = {
        "items": [
            {
                "key": "reasoning-llama",
                "name": "Llama-3.1-8B-Instruct",
                "task": "reasoning",
                "path": str(tmp_path / "reasoning" / "Llama-3.1-8B-Instruct"),
                "strategy": "huggingface_snapshot",
                "automation_ready": True,
                "matched_rule": "reasoning-meta-llama-3.1-8b-gguf",
                "source_kind": "huggingface",
                "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
                "source_url": "https://huggingface.co/meta-llama/Llama-3.1-8B-Instruct",
                "family": "llama",
                "blockers": [],
            }
        ]
    }
    remote_metadata = {
        "status": "success",
        "item_map": {
            "reasoning-llama": {
                "status": "auth_required",
                "probe_mode": "api",
                "repo_id": "meta-llama/Llama-3.1-8B-Instruct",
                "gated": True,
                "private": False,
                "requires_auth": True,
                "auth_configured": True,
                "auth_used": True,
                "message": "Hugging Face access token is required or lacks access to this repository.",
            }
        },
    }

    monkeypatch.setattr(shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100_000_000_000, 20_000_000_000, 80_000_000_000))

    payload = build_model_setup_preflight(plan_payload=plan, remote_metadata=remote_metadata)

    row = payload["items"][0]
    assert row["status"] == "blocked"
    assert any("could not access this repository" in str(message).lower() for message in row["blockers"])
