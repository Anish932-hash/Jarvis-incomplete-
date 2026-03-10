from __future__ import annotations

import json

from backend.python.pc_control.defender_monitor import DefenderMonitor


def test_defender_monitor_parses_status_and_builds_posture(monkeypatch) -> None:
    sample = {
        "AMServiceEnabled": True,
        "AntispywareEnabled": True,
        "AntivirusEnabled": True,
        "RealTimeProtectionEnabled": False,
        "IoavProtectionEnabled": True,
        "NISEnabled": True,
        "OnAccessProtectionEnabled": True,
        "QuickScanAge": 20,
        "FullScanAge": 10,
        "AntivirusSignatureAge": 4,
        "QuickScanEndTime": "2026-03-02T10:00:00Z",
        "FullScanEndTime": "2026-02-20T10:00:00Z",
    }

    monkeypatch.setattr("backend.python.pc_control.defender_monitor.platform.system", lambda: "Windows")
    monkeypatch.setattr(
        "backend.python.pc_control.defender_monitor.subprocess.check_output",
        lambda *args, **kwargs: json.dumps(sample),
    )

    payload = DefenderMonitor().get_status()
    assert payload["status"] == "success"
    posture = payload.get("posture", {})
    assert isinstance(posture, dict)
    assert posture.get("status") in {"healthy", "degraded", "critical"}
    assert int(posture.get("score", 0)) < 100


def test_defender_monitor_hardening_alerts_and_history_persistence(monkeypatch, tmp_path) -> None:
    status_payload = {
        "AMServiceEnabled": True,
        "AntispywareEnabled": True,
        "AntivirusEnabled": True,
        "RealTimeProtectionEnabled": False,
        "IoavProtectionEnabled": True,
        "NISEnabled": True,
        "OnAccessProtectionEnabled": True,
        "QuickScanAge": 28,
        "FullScanAge": 70,
        "AntivirusSignatureAge": 6,
        "QuickScanEndTime": "2026-03-01T10:00:00Z",
        "FullScanEndTime": "2026-01-20T10:00:00Z",
    }
    preference_payload = {
        "DisableRealtimeMonitoring": True,
        "DisableScriptScanning": True,
        "PUAProtection": 0,
        "MAPSReporting": 0,
        "SubmitSamplesConsent": 1,
        "CheckForSignaturesBeforeRunningScan": False,
    }

    monkeypatch.setenv("JARVIS_DEFENDER_HISTORY_PATH", str(tmp_path / "defender_history.jsonl"))
    DefenderMonitor._history = []  # noqa: SLF001
    DefenderMonitor._history_bootstrapped = False  # noqa: SLF001
    monkeypatch.setattr("backend.python.pc_control.defender_monitor.platform.system", lambda: "Windows")

    def _fake_check_output(args, **kwargs):  # noqa: ANN001
        del kwargs
        cmd = " ".join(str(item) for item in args)
        if "Get-MpPreference" in cmd:
            return json.dumps(preference_payload)
        return json.dumps(status_payload)

    monkeypatch.setattr(
        "backend.python.pc_control.defender_monitor.subprocess.check_output",
        _fake_check_output,
    )

    payload = DefenderMonitor().get_status(include_history=True, include_preferences=True)
    assert payload["status"] == "success"
    assert payload.get("hardening", {}).get("status") in {"degraded", "critical"}
    assert int(payload.get("hardening", {}).get("score", 100)) < 80
    alerts = payload.get("alerts", [])
    assert isinstance(alerts, list)
    assert len(alerts) >= 1
    history = payload.get("history", {})
    assert int(history.get("count", 0)) >= 1

    DefenderMonitor._history = []  # noqa: SLF001
    DefenderMonitor._history_bootstrapped = False  # noqa: SLF001
    persisted = DefenderMonitor.history(limit=5)
    assert persisted["status"] == "success"
    assert int(persisted.get("count", 0)) >= 1
