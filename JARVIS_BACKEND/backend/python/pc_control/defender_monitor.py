import platform
import subprocess
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class DefenderMonitor:
    """
    Read-only Microsoft Defender status helper.
    """
    _history_lock = threading.RLock()
    _history: list[Dict[str, Any]] = []
    _max_history = 240
    _history_bootstrapped = False

    def get_status(self, *, include_history: bool = True, include_preferences: bool = True) -> Dict[str, Any]:
        self._ensure_history_loaded()
        if platform.system() != "Windows":
            return {"status": "error", "message": "Defender status is available on Windows only."}

        cmd = (
            "Get-MpComputerStatus | "
            "Select-Object AMServiceEnabled,AntispywareEnabled,AntivirusEnabled,RealTimeProtectionEnabled,"
            "QuickScanAge,QuickScanEndTime,FullScanAge,FullScanEndTime,IoavProtectionEnabled,NISEnabled,"
            "OnAccessProtectionEnabled,TamperProtectionSource,AntivirusSignatureAge "
            "| ConvertTo-Json -Compress"
        )
        try:
            output = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command", cmd],
                stderr=subprocess.STDOUT,
                text=True,
                timeout=12,
            ).strip()
            parsed = self._parse_output(output)
            if not parsed:
                return {"status": "error", "message": "Failed to parse Defender status payload.", "raw": output}
            posture = self._compute_posture(parsed)
            history_row = self._record_history(posture=posture, parsed=parsed)
            trend = self._build_trend(window=40)
            payload: Dict[str, Any] = {
                "status": "success",
                "raw": output,
                "data": parsed,
                "posture": posture,
                "trend": trend,
                "history_ref": {
                    "sample_id": str(history_row.get("sample_id", "")),
                    "sample_at": str(history_row.get("captured_at", "")),
                },
            }
            preferences_payload: Dict[str, Any] = {"status": "skipped", "message": "preferences disabled"}
            if include_preferences:
                preferences_payload = self._read_preferences()
                payload["preferences"] = preferences_payload
            hardening = self._compute_hardening(
                preferences=preferences_payload if isinstance(preferences_payload, dict) else {},
                posture=posture,
                parsed=parsed,
            )
            payload["hardening"] = hardening
            payload["alerts"] = self._build_alerts(posture=posture, trend=trend, hardening=hardening, parsed=parsed)
            if include_history:
                payload["history"] = self.history(limit=20)
            return payload
        except subprocess.CalledProcessError as exc:
            return {"status": "error", "message": exc.output.strip()}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @staticmethod
    def _parse_output(raw: str) -> Dict[str, Any]:
        try:
            payload = json.loads(str(raw or "").strip() or "{}")
        except Exception:
            return {}
        if isinstance(payload, list):
            payload = payload[0] if payload and isinstance(payload[0], dict) else {}
        if not isinstance(payload, dict):
            return {}

        def _to_bool(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            text = str(value or "").strip().lower()
            return text in {"1", "true", "yes", "on"}

        def _to_int(value: Any, default: int = -1) -> int:
            try:
                return int(value)
            except Exception:
                return default

        return {
            "service_enabled": _to_bool(payload.get("AMServiceEnabled")),
            "antispyware_enabled": _to_bool(payload.get("AntispywareEnabled")),
            "antivirus_enabled": _to_bool(payload.get("AntivirusEnabled")),
            "realtime_enabled": _to_bool(payload.get("RealTimeProtectionEnabled")),
            "ioav_enabled": _to_bool(payload.get("IoavProtectionEnabled")),
            "nis_enabled": _to_bool(payload.get("NISEnabled")),
            "on_access_enabled": _to_bool(payload.get("OnAccessProtectionEnabled")),
            "tamper_protection_source": str(payload.get("TamperProtectionSource", "")).strip(),
            "quick_scan_age_days": _to_int(payload.get("QuickScanAge"), default=-1),
            "full_scan_age_days": _to_int(payload.get("FullScanAge"), default=-1),
            "signature_age_days": _to_int(payload.get("AntivirusSignatureAge"), default=-1),
            "quick_scan_end_time": str(payload.get("QuickScanEndTime", "")).strip(),
            "full_scan_end_time": str(payload.get("FullScanEndTime", "")).strip(),
        }

    @staticmethod
    def _compute_posture(payload: Dict[str, Any]) -> Dict[str, Any]:
        score = 100
        issues = []
        recommendations = []

        def _flag(condition: bool, *, score_penalty: int, issue: str, recommendation: str) -> None:
            nonlocal score
            if not condition:
                return
            score = max(0, score - int(score_penalty))
            issues.append(issue)
            recommendations.append(recommendation)

        _flag(not bool(payload.get("service_enabled")), score_penalty=25, issue="Defender service disabled.", recommendation="Enable the Microsoft Defender service.")
        _flag(not bool(payload.get("antivirus_enabled")), score_penalty=20, issue="Antivirus engine disabled.", recommendation="Turn on Antivirus protection in Windows Security.")
        _flag(not bool(payload.get("realtime_enabled")), score_penalty=20, issue="Real-time protection disabled.", recommendation="Enable real-time protection for active threat blocking.")
        _flag(not bool(payload.get("ioav_enabled")), score_penalty=8, issue="IOAV protection disabled.", recommendation="Enable scanning of downloaded files and attachments.")
        _flag(not bool(payload.get("nis_enabled")), score_penalty=8, issue="Network Inspection Service disabled.", recommendation="Enable network inspection in Defender settings.")

        quick_scan_age = int(payload.get("quick_scan_age_days", -1) or -1)
        full_scan_age = int(payload.get("full_scan_age_days", -1) or -1)
        signature_age = int(payload.get("signature_age_days", -1) or -1)
        _flag(quick_scan_age >= 14, score_penalty=6, issue=f"Quick scan is stale ({quick_scan_age} days).", recommendation="Run a quick scan at least once per week.")
        _flag(full_scan_age >= 45, score_penalty=6, issue=f"Full scan is stale ({full_scan_age} days).", recommendation="Run a full scan at least once per month.")
        _flag(signature_age >= 3, score_penalty=10, issue=f"Signature definitions are stale ({signature_age} days).", recommendation="Update Defender signatures immediately.")

        status = "healthy"
        if score < 70:
            status = "degraded"
        if score < 45:
            status = "critical"

        return {
            "status": status,
            "score": score,
            "issues": issues,
            "recommendations": recommendations,
            "last_quick_scan": str(payload.get("quick_scan_end_time", "")),
            "last_full_scan": str(payload.get("full_scan_end_time", "")),
        }

    @classmethod
    def _record_history(cls, *, posture: Dict[str, Any], parsed: Dict[str, Any]) -> Dict[str, Any]:
        cls._ensure_history_loaded()
        row = {
            "sample_id": f"defender-{time.time_ns()}",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "score": int(posture.get("score", 0) or 0),
            "status": str(posture.get("status", "")).strip().lower(),
            "issue_count": len(posture.get("issues", []) if isinstance(posture.get("issues", []), list) else []),
            "signature_age_days": int(parsed.get("signature_age_days", -1) or -1),
            "quick_scan_age_days": int(parsed.get("quick_scan_age_days", -1) or -1),
            "full_scan_age_days": int(parsed.get("full_scan_age_days", -1) or -1),
            "realtime_enabled": bool(parsed.get("realtime_enabled", False)),
            "service_enabled": bool(parsed.get("service_enabled", False)),
        }
        with cls._history_lock:
            cls._history.append(row)
            if len(cls._history) > cls._max_history:
                cls._history = cls._history[-cls._max_history :]
        cls._persist_history_row(row)
        return row

    @classmethod
    def history(cls, *, limit: int = 120) -> Dict[str, Any]:
        cls._ensure_history_loaded()
        bounded = max(1, min(int(limit), cls._max_history))
        with cls._history_lock:
            rows = list(cls._history[-bounded:])
        return {
            "status": "success",
            "count": len(rows),
            "items": rows,
        }

    @classmethod
    def _build_trend(cls, *, window: int = 40) -> Dict[str, Any]:
        cls._ensure_history_loaded()
        bounded = max(4, min(int(window), cls._max_history))
        with cls._history_lock:
            rows = list(cls._history[-bounded:])
        if len(rows) < 4:
            return {
                "status": "insufficient_data",
                "sample_count": len(rows),
                "window": bounded,
            }

        scores = [float(row.get("score", 0) or 0.0) for row in rows]
        latest = scores[-1]
        avg_score = sum(scores) / float(len(scores))
        min_score = min(scores)
        max_score = max(scores)
        span = max(1, len(scores) - 1)
        slope = (scores[-1] - scores[0]) / float(span)
        stability = 1.0 - min(1.0, (max_score - min_score) / 100.0)
        status_counts: Dict[str, int] = {}
        for row in rows:
            state = str(row.get("status", "unknown")).strip().lower() or "unknown"
            status_counts[state] = int(status_counts.get(state, 0)) + 1

        mode = "stable"
        if slope <= -2.0:
            mode = "deteriorating"
        elif slope >= 1.6:
            mode = "improving"
        elif stability < 0.65:
            mode = "volatile"

        risk = "low"
        if latest < 70 or mode in {"deteriorating", "volatile"}:
            risk = "medium"
        if latest < 45:
            risk = "high"

        return {
            "status": "success",
            "sample_count": len(rows),
            "window": bounded,
            "latest_score": round(latest, 3),
            "avg_score": round(avg_score, 3),
            "min_score": round(min_score, 3),
            "max_score": round(max_score, 3),
            "slope_per_sample": round(slope, 6),
            "stability": round(stability, 6),
            "mode": mode,
            "risk": risk,
            "status_counts": status_counts,
        }

    @classmethod
    def _history_store_path(cls) -> Path:
        raw = str(os.getenv("JARVIS_DEFENDER_HISTORY_PATH", "data/security/defender_history.jsonl")).strip()
        return Path(raw or "data/security/defender_history.jsonl")

    @classmethod
    def _ensure_history_loaded(cls) -> None:
        with cls._history_lock:
            if bool(cls._history_bootstrapped):
                return
            cls._history_bootstrapped = True
            path = cls._history_store_path()
            if not path.exists():
                return
            loaded: list[Dict[str, Any]] = []
            try:
                with path.open("r", encoding="utf-8") as handle:
                    for raw in handle:
                        line = str(raw or "").strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except Exception:
                            continue
                        if isinstance(row, dict):
                            loaded.append(row)
            except Exception:
                return
            if loaded:
                cls._history = loaded[-cls._max_history :]

    @classmethod
    def _persist_history_row(cls, row: Dict[str, Any]) -> None:
        path = cls._history_store_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n")
        except Exception:
            return

    @staticmethod
    def _compute_hardening(*, preferences: Dict[str, Any], posture: Dict[str, Any], parsed: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(preferences, dict) or str(preferences.get("status", "")).strip().lower() not in {"success", "ok"}:
            return {
                "status": "unavailable",
                "score": int(posture.get("score", 0) or 0),
                "issues": [],
                "recommendations": [],
            }
        score = 100
        issues = []
        recommendations = []

        def _flag(condition: bool, *, penalty: int, issue: str, recommendation: str) -> None:
            nonlocal score
            if not condition:
                return
            score = max(0, score - int(penalty))
            issues.append(issue)
            recommendations.append(recommendation)

        _flag(
            bool(preferences.get("disable_realtime_monitoring", False)),
            penalty=28,
            issue="Realtime monitoring preference is disabled.",
            recommendation="Enable realtime monitoring in Defender preferences.",
        )
        _flag(
            bool(preferences.get("disable_script_scanning", False)),
            penalty=14,
            issue="Script scanning is disabled.",
            recommendation="Enable script scanning for macro/script threat coverage.",
        )
        _flag(
            int(preferences.get("pua_protection", 0) or 0) <= 0,
            penalty=10,
            issue="PUA protection appears disabled.",
            recommendation="Enable PUA protection to block unwanted software.",
        )
        _flag(
            int(preferences.get("maps_reporting", 0) or 0) <= 0,
            penalty=8,
            issue="Cloud-delivered protection telemetry is limited.",
            recommendation="Enable MAPS reporting for stronger cloud intelligence.",
        )
        _flag(
            int(parsed.get("signature_age_days", -1) or -1) >= 3,
            penalty=8,
            issue="Signature baseline is stale.",
            recommendation="Refresh Defender signatures before sensitive operations.",
        )

        status = "healthy"
        if score < 76:
            status = "degraded"
        if score < 50:
            status = "critical"
        return {
            "status": status,
            "score": int(score),
            "issues": issues,
            "recommendations": recommendations,
        }

    @staticmethod
    def _build_alerts(
        *,
        posture: Dict[str, Any],
        trend: Dict[str, Any],
        hardening: Dict[str, Any],
        parsed: Dict[str, Any],
    ) -> list[Dict[str, Any]]:
        alerts: list[Dict[str, Any]] = []
        posture_status = str(posture.get("status", "")).strip().lower()
        trend_mode = str(trend.get("mode", "")).strip().lower() if isinstance(trend, dict) else ""
        hardening_status = str(hardening.get("status", "")).strip().lower()
        signature_age = int(parsed.get("signature_age_days", -1) or -1)

        if posture_status == "critical":
            alerts.append(
                {
                    "severity": "high",
                    "code": "defender_posture_critical",
                    "message": "Defender posture is critical; immediate intervention required.",
                    "recommendation": "Enable realtime protection and run a full system scan.",
                }
            )
        if trend_mode in {"deteriorating", "volatile"}:
            alerts.append(
                {
                    "severity": "medium",
                    "code": f"defender_trend_{trend_mode}",
                    "message": f"Defender trend indicates {trend_mode} security posture.",
                    "recommendation": "Audit recent policy changes and investigate stability drift.",
                }
            )
        if signature_age >= 5:
            alerts.append(
                {
                    "severity": "high",
                    "code": "signature_stale",
                    "message": f"Defender signatures are stale ({signature_age} days).",
                    "recommendation": "Force signature update before executing external workflows.",
                }
            )
        if hardening_status in {"degraded", "critical"}:
            alerts.append(
                {
                    "severity": "medium" if hardening_status == "degraded" else "high",
                    "code": f"hardening_{hardening_status}",
                    "message": f"Defender hardening profile is {hardening_status}.",
                    "recommendation": "Apply hardening recommendations to improve defensive baseline.",
                }
            )
        return alerts

    @staticmethod
    def _read_preferences() -> Dict[str, Any]:
        cmd = (
            "Get-MpPreference | "
            "Select-Object DisableRealtimeMonitoring,DisableScriptScanning,PUAProtection,MAPSReporting,SubmitSamplesConsent,CheckForSignaturesBeforeRunningScan "
            "| ConvertTo-Json -Compress"
        )
        try:
            output = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command", cmd],
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10,
            ).strip()
            payload = json.loads(output) if output else {}
            if isinstance(payload, list):
                payload = payload[0] if payload and isinstance(payload[0], dict) else {}
            if not isinstance(payload, dict):
                payload = {}
            return {
                "status": "success",
                "disable_realtime_monitoring": bool(payload.get("DisableRealtimeMonitoring", False)),
                "disable_script_scanning": bool(payload.get("DisableScriptScanning", False)),
                "pua_protection": int(payload.get("PUAProtection", 0) or 0),
                "maps_reporting": int(payload.get("MAPSReporting", 0) or 0),
                "submit_samples_consent": int(payload.get("SubmitSamplesConsent", 0) or 0),
                "check_signatures_before_scan": bool(payload.get("CheckForSignaturesBeforeRunningScan", False)),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
