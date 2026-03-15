from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path

from backend.python.inference.model_setup_remote_metadata import ModelSetupRemoteMetadataProbe


class _FakeResponse:
    def __init__(
        self,
        *,
        final_url: str,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        status: int = 200,
    ) -> None:
        self._final_url = final_url
        self.headers = headers or {}
        self._body = body
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def geturl(self) -> str:
        return self._final_url

    def read(self) -> bytes:
        return self._body


def test_remote_metadata_probe_direct_url_caches_head_response(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    probe = ModelSetupRemoteMetadataProbe(cache_path="data/model_setup_remote_metadata.json", cache_ttl_s=3600.0)

    calls: list[str] = []

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        calls.append(request.get_method())
        return _FakeResponse(
            final_url="https://release-assets.githubusercontent.com/yolov10x.pt",
            headers={"Content-Length": "123456789", "ETag": '"abc123"'},
            status=200,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    item = {
        "key": "vision-yolo",
        "name": "yolov10x.pt",
        "source_kind": "direct_url",
        "source_ref": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
        "source_url": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
    }

    first = probe.item_metadata(item=item)
    second = probe.item_metadata(item=item)

    assert calls == ["HEAD"]
    assert first["status"] == "success"
    assert first["probe_mode"] == "head"
    assert int(first["size_bytes"]) == 123456789
    assert str(first["final_host"]) == "release-assets.githubusercontent.com"
    assert second["cached"] is True
    assert float(second["cache_age_s"]) >= 0.0


def test_remote_metadata_probe_falls_back_to_range_request(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    probe = ModelSetupRemoteMetadataProbe(cache_path="data/model_setup_remote_metadata.json", cache_ttl_s=3600.0)

    calls: list[str] = []

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        method = request.get_method()
        calls.append(method)
        if method == "HEAD":
            raise RuntimeError("head blocked")
        return _FakeResponse(
            final_url="https://dl.fbaipublicfiles.com/segment_anything/sam_vit_h_4b8939.pth",
            headers={"Content-Range": "bytes 0-0/2650000000"},
            status=206,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = probe.item_metadata(
        item={
            "key": "vision-sam",
            "name": "sam_vit_h_4b8939.pth",
            "source_kind": "direct_url",
            "source_ref": "https://dl.fbaipublicfiles.com/segment_anything/sam_vit_h_4b8939.pth",
            "source_url": "https://dl.fbaipublicfiles.com/segment_anything/sam_vit_h_4b8939.pth",
        }
    )

    assert calls == ["HEAD", "GET"]
    assert payload["status"] == "success"
    assert payload["probe_mode"] == "range"
    assert int(payload["http_status"]) == 206
    assert int(payload["size_bytes"]) == 2_650_000_000


def test_remote_metadata_probe_huggingface_api_collects_repo_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("HUGGINGFACE_HUB_TOKEN", "HF_TOKEN", "HUGGINGFACE_TOKEN"):
        monkeypatch.delenv(env_name, raising=False)
    probe = ModelSetupRemoteMetadataProbe(cache_path="data/model_setup_remote_metadata.json", cache_ttl_s=3600.0)

    body = json.dumps(
        {
            "sha": "abc123",
            "gated": False,
            "private": False,
            "siblings": [
                {"rfilename": "config.json", "size": 1024},
                {"rfilename": "model.safetensors", "lfs": {"size": 2048}},
            ],
        }
    ).encode("utf-8")

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        return _FakeResponse(
            final_url="https://huggingface.co/api/models/openai/whisper-medium",
            headers={"Content-Type": "application/json"},
            body=body,
            status=200,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = probe.item_metadata(
        item={
            "key": "stt-whisper-medium",
            "name": "whisper-medium",
            "source_kind": "huggingface",
            "source_ref": "openai/whisper-medium",
            "source_url": "https://huggingface.co/openai/whisper-medium",
        }
    )

    assert payload["status"] == "success"
    assert payload["probe_mode"] == "api"
    assert payload["repo_id"] == "openai/whisper-medium"
    assert payload["commit_sha"] == "abc123"
    assert payload["requires_auth"] is False
    assert payload["auth_configured"] is False
    assert payload["auth_used"] is False
    assert int(payload["size_bytes"]) == 3072
    assert int(payload["siblings_with_size"]) == 2


def test_remote_metadata_probe_huggingface_uses_authorization_header_when_token_available(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HUGGINGFACE_HUB_TOKEN", "hf_testtoken1234567890abcd")
    probe = ModelSetupRemoteMetadataProbe(cache_path="data/model_setup_remote_metadata.json", cache_ttl_s=3600.0)

    body = json.dumps({"sha": "secure123", "gated": True, "private": False, "siblings": []}).encode("utf-8")

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        assert request.get_header("Authorization") == "Bearer hf_testtoken1234567890abcd"
        return _FakeResponse(
            final_url="https://huggingface.co/api/models/meta-llama/Llama-3.1-8B-Instruct",
            headers={"Content-Type": "application/json"},
            body=body,
            status=200,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = probe.item_metadata(
        item={
            "key": "llama-gated",
            "name": "Llama-3.1-8B-Instruct",
            "source_kind": "huggingface",
            "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
            "source_url": "https://huggingface.co/meta-llama/Llama-3.1-8B-Instruct",
        }
    )

    assert payload["status"] == "success"
    assert payload["requires_auth"] is True
    assert payload["auth_configured"] is True
    assert payload["auth_used"] is True


def test_remote_metadata_probe_huggingface_marks_auth_required_on_access_denied(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for env_name in ("HUGGINGFACE_HUB_TOKEN", "HF_TOKEN", "HUGGINGFACE_TOKEN"):
        monkeypatch.delenv(env_name, raising=False)
    probe = ModelSetupRemoteMetadataProbe(cache_path="data/model_setup_remote_metadata.json", cache_ttl_s=3600.0)

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0.0) -> _FakeResponse:  # noqa: ARG001
        raise urllib.error.HTTPError(
            url=request.full_url,
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=None,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = probe.item_metadata(
        item={
            "key": "private-whisper",
            "name": "whisper-medium",
            "source_kind": "huggingface",
            "source_ref": "openai/whisper-medium",
            "source_url": "https://huggingface.co/openai/whisper-medium",
        },
        refresh=True,
    )

    assert payload["status"] == "auth_required"
    assert payload["requires_auth"] is True
    assert payload["auth_configured"] is False
    assert payload["auth_used"] is False
    assert int(payload["http_status"]) == 401


def test_remote_metadata_probe_plan_metadata_summarizes_acquisition_states(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    probe = ModelSetupRemoteMetadataProbe(cache_path="data/model_setup_remote_metadata.json", cache_ttl_s=3600.0)

    def fake_item_metadata(*, item: dict[str, object], refresh: bool = False, timeout_s: float = 0.0) -> dict[str, object]:  # noqa: ARG001
        item_key = str(item.get("key", "") or "").strip().lower()
        if item_key == "public-direct":
            return {
                "key": "public-direct",
                "name": "sam_vit_h_4b8939.pth",
                "source_kind": "direct_url",
                "status": "success",
                "size_bytes": 2650000000,
            }
        return {
            "key": "gated-hf",
            "name": "Llama-3.1-8B-Instruct",
            "source_kind": "huggingface",
            "status": "auth_required",
            "requires_auth": True,
            "auth_configured": False,
            "gated": True,
        }

    monkeypatch.setattr(probe, "item_metadata", fake_item_metadata)

    payload = probe.plan_metadata(
        plan_payload={
            "items": [
                {"key": "public-direct", "automation_ready": True},
                {"key": "gated-hf", "automation_ready": True},
            ]
        }
    )

    assert payload["status"] == "success"
    assert int(payload["download_ready_count"]) == 1
    assert int(payload["auth_missing_count"]) == 1
    assert int(payload["gated_count"]) == 1
    assert int(payload["blocked_count"]) == 1
    assert payload["item_map"]["public-direct"]["acquisition_stage"] == "ready_public"
    assert payload["item_map"]["gated-hf"]["credential_state"] == "missing"
