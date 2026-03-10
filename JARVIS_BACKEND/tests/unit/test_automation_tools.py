from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

from backend.python.tools.automation_tools import AutomationTools


class _FakeProcess:
    def __init__(self, pid: int = 4321) -> None:
        self.pid = pid


def test_run_trusted_script_validates_manifest_hash_and_builds_safe_command(monkeypatch, tmp_path) -> None:
    trusted_root = tmp_path / "trusted_scripts"
    trusted_root.mkdir(parents=True, exist_ok=True)
    script = trusted_root / "hello.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    expected_sha = hashlib.sha256(script.read_bytes()).hexdigest().lower()

    manifest = {
        "enforce_hash": True,
        "scripts": {
            "hello.py": {
                "sha256": expected_sha,
            }
        },
    }
    (trusted_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    calls: dict[str, object] = {}

    def _fake_popen(command, cwd=None, shell=None, close_fds=None, env=None):  # noqa: ANN001
        calls["command"] = command
        calls["cwd"] = cwd
        calls["shell"] = shell
        calls["close_fds"] = close_fds
        calls["env"] = env
        return _FakeProcess()

    monkeypatch.setattr(AutomationTools, "TRUSTED_SCRIPT_DIR", trusted_root)
    monkeypatch.setattr("backend.python.tools.automation_tools.subprocess.Popen", _fake_popen)
    monkeypatch.setenv("JARVIS_TRUSTED_SCRIPT_REQUIRE_MANIFEST", "1")

    process = AutomationTools.run_trusted_script("hello.py", args=["--ping"], env_overrides={"X_TEST": "1"})

    assert isinstance(process, _FakeProcess)
    command = calls.get("command")
    assert isinstance(command, list)
    assert command[:3] == [sys.executable, str(script.resolve()), "--ping"]
    assert calls.get("cwd") == str(trusted_root.resolve())
    assert calls.get("shell") is False
    assert calls.get("close_fds") is False
    env = calls.get("env")
    assert isinstance(env, dict)
    assert env.get("JARVIS_TRUSTED_EXECUTION") == "1"
    assert env.get("X_TEST") == "1"


def test_run_trusted_script_rejects_path_escape(monkeypatch, tmp_path) -> None:
    trusted_root = tmp_path / "trusted_scripts"
    trusted_root.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside.py"
    outside.write_text("print('bad')\n", encoding="utf-8")

    monkeypatch.setattr(AutomationTools, "TRUSTED_SCRIPT_DIR", trusted_root)
    with pytest.raises(PermissionError):
        AutomationTools.run_trusted_script("../outside.py")


def test_run_trusted_script_requires_manifest_entry_when_enforced(monkeypatch, tmp_path) -> None:
    trusted_root = tmp_path / "trusted_scripts"
    trusted_root.mkdir(parents=True, exist_ok=True)
    script = trusted_root / "script.py"
    script.write_text("print('no-manifest')\n", encoding="utf-8")

    monkeypatch.setattr(AutomationTools, "TRUSTED_SCRIPT_DIR", trusted_root)
    monkeypatch.setenv("JARVIS_TRUSTED_SCRIPT_REQUIRE_MANIFEST", "1")
    with pytest.raises(PermissionError):
        AutomationTools.run_trusted_script("script.py")
