from __future__ import annotations

from pathlib import Path

from backend.python.core.rust_runtime import RustRuntimeBridge


class _DummyLogger:
    @staticmethod
    def debug(_message: str) -> None:
        return


def test_request_requires_non_empty_event(tmp_path: Path) -> None:
    bridge = RustRuntimeBridge(logger=_DummyLogger(), binary_path=str(tmp_path / "missing-rust-bin.exe"))
    payload = bridge.request("", payload={})
    assert payload["status"] == "error"
    assert payload["error_code"] == "invalid_event"


def test_health_reports_disabled_runtime(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JARVIS_RUST_DISABLED", "1")
    bridge = RustRuntimeBridge(logger=_DummyLogger(), binary_path=str(tmp_path / "missing-rust-bin.exe"))
    payload = bridge.health()
    assert payload["status"] == "success"
    assert payload["disabled"] is True
    assert payload["available"] is False
    assert payload["running"] is False


def test_health_reports_missing_binary(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("JARVIS_RUST_DISABLED", raising=False)
    bridge = RustRuntimeBridge(logger=_DummyLogger(), binary_path=str(tmp_path / "missing-rust-bin.exe"))
    payload = bridge.health()
    assert payload["status"] == "success"
    assert payload["available"] is False
    assert payload["running"] is False
    assert payload["disabled"] is False
    assert "not found" in str(payload.get("message", "")).lower()


def test_request_returns_missing_binary_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("JARVIS_RUST_DISABLED", raising=False)
    bridge = RustRuntimeBridge(logger=_DummyLogger(), binary_path=str(tmp_path / "missing-rust-bin.exe"))
    payload = bridge.request("health_check", payload={})
    assert payload["status"] == "error"
    assert payload["error_code"] == "runtime_missing"
