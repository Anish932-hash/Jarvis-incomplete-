from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Set, Tuple

from backend.python.core.contracts import ActionRequest
from .risk_engine import RiskEngine


class PolicyGuard:
    """
    Enforces static policy and risk-based blocks.
    """

    def __init__(self, permissions_path: str = "configs/permissions.json") -> None:
        self.risk_engine = RiskEngine()
        self.permissions_path = Path(permissions_path)
        self.allowed_actions: Set[str] = set()
        self.runtime_actions: Set[str] = set()
        self.denied_actions: Set[str] = {"run_script", "delete_file"}
        self.source_allow_actions: Dict[str, Set[str]] = {}
        self.source_deny_actions: Dict[str, Set[str]] = {}
        self.profile_allow_actions: Dict[str, Set[str]] = {}
        self.profile_deny_actions: Dict[str, Set[str]] = {}
        self.profile_allow_high_risk: Dict[str, bool] = {}
        self.profile_default_max_runtime_s: Dict[str, int] = {}
        self.profile_default_max_steps: Dict[str, int] = {}
        self.source_default_profile: Dict[str, str] = {}
        self.default_profile_name: str = ""
        self._known_profiles: Set[str] = set()
        self._lock = RLock()

        self.guardrails_enabled = self._env_flag("JARVIS_POLICY_GUARDRAILS_ENABLED", default=True)
        self.guardrails_store_path = Path(os.getenv("JARVIS_POLICY_GUARDRAILS_STORE", "data/policy_guardrails.json"))
        self.guardrails_max_records = self._coerce_int(
            os.getenv("JARVIS_POLICY_GUARDRAILS_MAX_RECORDS", "1500"),
            minimum=100,
            maximum=20000,
            default=1500,
        )
        self.guardrails_min_samples = self._coerce_int(
            os.getenv("JARVIS_POLICY_GUARDRAILS_MIN_SAMPLES", "8"),
            minimum=1,
            maximum=500,
            default=8,
        )
        self.guardrails_warn_unstable = self._coerce_float(
            os.getenv("JARVIS_POLICY_GUARDRAILS_WARN_UNSTABLE", "0.45"),
            minimum=0.05,
            maximum=0.99,
            default=0.45,
        )
        self.guardrails_block_unstable_high = self._coerce_float(
            os.getenv("JARVIS_POLICY_GUARDRAILS_BLOCK_HIGH", "0.68"),
            minimum=0.1,
            maximum=0.99,
            default=0.68,
        )
        self.guardrails_block_unstable_critical = self._coerce_float(
            os.getenv("JARVIS_POLICY_GUARDRAILS_BLOCK_CRITICAL", "0.58"),
            minimum=0.1,
            maximum=0.99,
            default=0.58,
        )
        self.guardrails_block_unstable_medium_automation = self._coerce_float(
            os.getenv("JARVIS_POLICY_GUARDRAILS_BLOCK_MEDIUM_AUTOMATION", "0.8"),
            minimum=0.15,
            maximum=1.0,
            default=0.8,
        )
        self.guardrails_decay = self._coerce_float(
            os.getenv("JARVIS_POLICY_GUARDRAILS_EMA_DECAY", "0.9"),
            minimum=0.55,
            maximum=0.995,
            default=0.9,
        )
        self._guardrails: Dict[str, Dict[str, Any]] = {}
        self._guardrail_updates_since_save = 0
        self._guardrail_last_save_monotonic = 0.0
        self._adaptive_default_profile = ""
        self._adaptive_source_default_profiles: Dict[str, str] = {}
        self._adaptive_tuning_state: Dict[str, Any] = {
            "status": "idle",
            "last_run_at": "",
            "mode": "",
            "changed": False,
            "dry_run": False,
            "reason": "",
        }

        self._capability_aliases: Dict[str, Set[str]] = {
            "open_application": {"open_app"},
            "system_monitor": {"system_snapshot", "list_processes", "list_windows", "active_window", "media_info", "time_now"},
            "app_status": {"list_processes", "list_windows", "active_window"},
            "power_status": {"system_snapshot"},
            "safe_file_write": {"write_file", "create_folder", "copy_file", "backup_file"},
            "non_invasive_tasks": {"tts_speak", "tts_stop", "media_search", "open_url", "send_notification"},
            "task_runner": {
                "open_app",
                "focus_window",
                "terminate_process",
                "media_play_pause",
                "media_play",
                "media_pause",
                "media_stop",
                "media_next",
                "media_previous",
            },
        }
        self._load_policy()
        self._load_guardrails()

    def _load_policy(self) -> None:
        if not self.permissions_path.exists():
            return
        try:
            data = json.loads(self.permissions_path.read_text(encoding="utf-8"))
            capabilities = data.get("capabilities", {})
            for capability_actions in capabilities.values():
                if isinstance(capability_actions, list):
                    for action in capability_actions:
                        if isinstance(action, str):
                            self.allowed_actions.add(action)
            explicit = data.get("allowed_actions")
            if isinstance(explicit, list):
                for action in explicit:
                    if isinstance(action, str):
                        self.allowed_actions.add(action)

            source_overrides = data.get("source_overrides", {})
            if isinstance(source_overrides, dict):
                for source_name, raw_rules in source_overrides.items():
                    if not isinstance(source_name, str) or not isinstance(raw_rules, dict):
                        continue
                    self.source_allow_actions[source_name.strip().lower()] = self._parse_action_set(raw_rules.get("allow"))
                    self.source_deny_actions[source_name.strip().lower()] = self._parse_action_set(raw_rules.get("deny"))

            profiles = data.get("profiles", {})
            if isinstance(profiles, dict):
                for profile_name, raw_rules in profiles.items():
                    if not isinstance(profile_name, str) or not isinstance(raw_rules, dict):
                        continue
                    normalized_profile = profile_name.strip().lower()
                    self._known_profiles.add(normalized_profile)
                    self.profile_allow_actions[normalized_profile] = self._parse_action_set(raw_rules.get("allow"))
                    self.profile_deny_actions[normalized_profile] = self._parse_action_set(raw_rules.get("deny"))
                    self.profile_allow_high_risk[normalized_profile] = bool(raw_rules.get("allow_high_risk", False))
                    runtime_budget = self._parse_optional_int(
                        raw_rules.get("default_max_runtime_s"),
                        minimum=10,
                        maximum=3600,
                    )
                    if runtime_budget is not None:
                        self.profile_default_max_runtime_s[normalized_profile] = runtime_budget

                    step_budget = self._parse_optional_int(
                        raw_rules.get("default_max_steps"),
                        minimum=1,
                        maximum=250,
                    )
                    if step_budget is not None:
                        self.profile_default_max_steps[normalized_profile] = step_budget

            default_profile = data.get("default_profile")
            if isinstance(default_profile, str) and default_profile.strip():
                self.default_profile_name = default_profile.strip().lower()

            default_profiles = data.get("default_profiles", {})
            if isinstance(default_profiles, dict):
                for source_name, profile_name in default_profiles.items():
                    if not isinstance(source_name, str) or not isinstance(profile_name, str):
                        continue
                    normalized_source = source_name.strip().lower()
                    normalized_profile = profile_name.strip().lower()
                    if not normalized_source or not normalized_profile:
                        continue
                    self.source_default_profile[normalized_source] = normalized_profile
        except Exception:
            # Keep default conservative policy.
            pass

    def set_runtime_actions(self, actions: Set[str]) -> None:
        self.runtime_actions = {action for action in actions if isinstance(action, str) and action}

    def _resolved_allow_actions(self) -> Set[str]:
        if not self.allowed_actions:
            return set()

        if not self.runtime_actions:
            return set(self.allowed_actions)

        resolved: Set[str] = {action for action in self.allowed_actions if action in self.runtime_actions}
        for capability, mapped_actions in self._capability_aliases.items():
            if capability in self.allowed_actions:
                resolved.update(mapped_actions.intersection(self.runtime_actions))
        return resolved

    @staticmethod
    def _parse_action_set(raw: object) -> Set[str]:
        if not isinstance(raw, list):
            return set()
        out: Set[str] = set()
        for item in raw:
            if isinstance(item, str) and item.strip():
                out.add(item.strip())
        return out

    @staticmethod
    def _parse_optional_int(raw: object, *, minimum: int, maximum: int) -> int | None:
        try:
            parsed = int(raw)
        except Exception:  # noqa: BLE001
            return None
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_int(value: object, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: object, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _env_flag(name: str, *, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        clean = str(raw).strip().lower()
        if clean in {"1", "true", "yes", "on"}:
            return True
        if clean in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    def authorize(self, request: ActionRequest) -> Tuple[bool, str]:
        payload = self.evaluate_policy_contract(request)
        return bool(payload.get("allowed", False)), str(payload.get("message", "Blocked")).strip() or "Blocked"

    def evaluate_policy_contract(
        self,
        request: ActionRequest,
        *,
        include_runtime_overrides: bool = False,
    ) -> Dict[str, Any]:
        action_name = str(request.action or "").strip()
        source_name = str(request.source or "").strip().lower()
        metadata = request.metadata if isinstance(request.metadata, dict) else {}
        profile_name = self.resolve_policy_profile(source_name=source_name, metadata=metadata)
        checks: List[Dict[str, Any]] = []
        allowed = True
        message = "Allowed"

        def _push(
            check_id: str,
            *,
            ok: bool,
            severity: str,
            detail: str,
            value: Any = None,
        ) -> None:
            checks.append(
                {
                    "id": str(check_id or "").strip().lower(),
                    "status": "passed" if bool(ok) else "failed",
                    "severity": str(severity or "").strip().lower() or "info",
                    "detail": str(detail or "").strip(),
                    "value": value,
                }
            )

        if action_name in self.denied_actions:
            allowed = False
            message = f"Action explicitly denied: {action_name}"
            _push("denied_actions", ok=False, severity="error", detail=message, value=action_name)
        else:
            _push("denied_actions", ok=True, severity="info", detail="Action is not explicitly denied.", value=action_name)

        if allowed and profile_name and profile_name not in self._known_profiles:
            allowed = False
            message = f"Unknown policy profile '{profile_name}'."
            _push("profile_known", ok=False, severity="error", detail=message, value=profile_name)
        else:
            _push("profile_known", ok=True, severity="info", detail="Policy profile is known or not specified.", value=profile_name)

        source_denied = self.source_deny_actions.get(source_name, set())
        if allowed and action_name in source_denied:
            allowed = False
            message = f"Action denied for source '{source_name}': {action_name}"
            _push("source_deny", ok=False, severity="error", detail=message, value=source_name)
        else:
            _push("source_deny", ok=True, severity="info", detail="Source deny-list does not block this action.", value=source_name)

        profile_denied = self.profile_deny_actions.get(profile_name, set())
        if allowed and action_name in profile_denied:
            allowed = False
            message = f"Action denied for policy profile '{profile_name}': {action_name}"
            _push("profile_deny", ok=False, severity="error", detail=message, value=profile_name)
        else:
            _push("profile_deny", ok=True, severity="info", detail="Profile deny-list does not block this action.", value=profile_name)

        risk = self.risk_engine.rate(
            action_name,
            args=request.args if isinstance(request.args, dict) else {},
            source=request.source,
            metadata=metadata,
        )
        if allowed and risk.level == "critical":
            allowed = False
            message = f"Action risk too high: {risk.reason} factors={','.join(risk.factors[:4])}"
            _push("critical_risk_block", ok=False, severity="error", detail=message, value=risk.score)
        else:
            _push("critical_risk_block", ok=True, severity="info", detail="Risk is below critical block threshold.", value=risk.score)

        if allowed and risk.level == "high" and profile_name and not self.profile_allow_high_risk.get(profile_name, False):
            allowed = False
            message = (
                f"High-risk action requires allow_high_risk for profile '{profile_name}'. "
                f"Risk: {risk.reason}"
            )
            _push("high_risk_profile_gate", ok=False, severity="error", detail=message, value=profile_name)
        else:
            _push("high_risk_profile_gate", ok=True, severity="info", detail="High-risk profile gate satisfied.", value=profile_name)

        safe_defaults = {
            "tts_speak",
            "tts_stop",
            "time_now",
            "defender_status",
            "system_snapshot",
            "list_processes",
            "list_windows",
            "active_window",
            "media_info",
            "media_search",
            "open_url",
            "search_files",
            "search_text",
            "scan_directory",
            "hash_file",
            "list_folder",
            "folder_size",
            "list_files",
            "read_file",
            "clipboard_read",
            "screenshot_capture",
            "browser_read_dom",
            "browser_extract_links",
            "computer_observe",
            "extract_text_from_image",
        }
        resolved_allow = self._resolved_allow_actions()
        if allowed and resolved_allow and action_name not in safe_defaults and action_name not in resolved_allow:
            allowed = False
            message = f"Action not in allow-list: {action_name}"
            _push("global_allowlist", ok=False, severity="error", detail=message, value=sorted(resolved_allow))
        else:
            _push("global_allowlist", ok=True, severity="info", detail="Global allow-list check passed.", value=len(resolved_allow))

        source_allow = self.source_allow_actions.get(source_name, set())
        if allowed and source_allow and action_name not in safe_defaults and action_name not in source_allow:
            allowed = False
            message = f"Action not allowed for source '{source_name}': {action_name}"
            _push("source_allowlist", ok=False, severity="error", detail=message, value=sorted(source_allow))
        else:
            _push("source_allowlist", ok=True, severity="info", detail="Source allow-list check passed.", value=len(source_allow))

        profile_allow = self.profile_allow_actions.get(profile_name, set())
        if allowed and profile_allow and action_name not in safe_defaults and action_name not in profile_allow:
            allowed = False
            message = f"Action not allowed for policy profile '{profile_name}': {action_name}"
            _push("profile_allowlist", ok=False, severity="error", detail=message, value=sorted(profile_allow))
        else:
            _push("profile_allowlist", ok=True, severity="info", detail="Profile allow-list check passed.", value=len(profile_allow))

        blocked_by_guardrail, guardrail_reason = self._evaluate_dynamic_guardrail(
            action=action_name,
            source_name=source_name,
            metadata=metadata,
            risk_level=risk.level,
        )
        if allowed and blocked_by_guardrail:
            allowed = False
            message = guardrail_reason
            _push("adaptive_guardrail", ok=False, severity="error", detail=guardrail_reason)
        else:
            _push("adaptive_guardrail", ok=True, severity="info", detail="Adaptive guardrail check passed.")

        payload: Dict[str, Any] = {
            "status": "success",
            "allowed": bool(allowed),
            "message": str(message or "").strip() or ("Allowed" if allowed else "Blocked"),
            "action": action_name,
            "source": source_name,
            "policy_profile": profile_name,
            "risk": {
                "score": int(risk.score),
                "level": str(risk.level),
                "reason": str(risk.reason),
                "factors": list(risk.factors),
            },
            "checks": checks,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        }
        if include_runtime_overrides:
            overrides = self.recommend_runtime_overrides_for_actions(
                actions=[action_name],
                source_name=source_name,
                metadata=metadata,
            )
            payload["runtime_overrides"] = overrides
        return payload

    def authorize_batch(
        self,
        requests: List[ActionRequest],
        *,
        max_critical: int = 0,
        max_high: int = 4,
        include_runtime_overrides: bool = False,
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        allowed_count = 0
        blocked_count = 0
        high_count = 0
        critical_count = 0
        action_names: List[str] = []
        for request in requests:
            if not isinstance(request, ActionRequest):
                continue
            result = self.evaluate_policy_contract(request, include_runtime_overrides=include_runtime_overrides)
            rows.append(result)
            action_names.append(str(request.action or "").strip())
            risk_payload = result.get("risk", {}) if isinstance(result.get("risk"), dict) else {}
            level = str(risk_payload.get("level", "")).strip().lower()
            if level == "high":
                high_count += 1
            elif level == "critical":
                critical_count += 1
            if bool(result.get("allowed", False)):
                allowed_count += 1
            else:
                blocked_count += 1

        batch_warnings: List[str] = []
        if critical_count > max(0, int(max_critical)):
            batch_warnings.append(
                f"critical_risk_actions_exceeded:{critical_count}>{max(0, int(max_critical))}"
            )
        if high_count > max(0, int(max_high)):
            batch_warnings.append(
                f"high_risk_actions_exceeded:{high_count}>{max(0, int(max_high))}"
            )
        batch_risk = self.risk_engine.rate_batch(action_names, source="batch-policy", metadata={})
        return {
            "status": "success",
            "count": len(rows),
            "allowed_count": allowed_count,
            "blocked_count": blocked_count,
            "high_risk_count": high_count,
            "critical_risk_count": critical_count,
            "warnings": batch_warnings,
            "batch_risk": batch_risk,
            "items": rows,
        }

    def _evaluate_dynamic_guardrail(
        self,
        *,
        action: str,
        source_name: str,
        metadata: Dict[str, Any],
        risk_level: str,
    ) -> Tuple[bool, str]:
        if not self.guardrails_enabled:
            return (False, "")

        if self._coerce_bool(metadata.get("guardrail_override", False), default=False):
            return (False, "")

        if self._coerce_bool(metadata.get("allow_unstable_actions", False), default=False):
            return (False, "")

        state = self._guardrails.get(str(action or "").strip())
        if not isinstance(state, dict):
            return (False, "")
        samples = self._coerce_int(state.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        if samples < self.guardrails_min_samples:
            return (False, "")

        unstable_score = self._coerce_float(state.get("unstable_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        reliability_score = self._coerce_float(state.get("reliability_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        consecutive_failures = self._coerce_int(
            state.get("consecutive_failures", 0),
            minimum=0,
            maximum=100_000,
            default=0,
        )
        normalized_risk = str(risk_level or "").strip().lower()
        threshold = 2.0
        if normalized_risk == "critical":
            threshold = self.guardrails_block_unstable_critical
        elif normalized_risk == "high":
            threshold = self.guardrails_block_unstable_high
        elif normalized_risk == "medium" and source_name in {"desktop-trigger", "desktop-schedule", "voice-loop"}:
            threshold = self.guardrails_block_unstable_medium_automation

        if unstable_score < threshold:
            return (False, "")

        return (
            True,
            (
                f"Adaptive guardrail blocked '{action}' due to unstable recent outcomes "
                f"(reliability={reliability_score:.2f}, unstable={unstable_score:.2f}, "
                f"samples={samples}, consecutive_failures={consecutive_failures}, risk={normalized_risk or 'unknown'}). "
                "Set metadata.guardrail_override=true only if this action is intentionally required."
            ),
        )

    def record_action_outcome(
        self,
        *,
        action: str,
        status: str,
        source: str = "",
        metadata: Dict[str, Any] | None = None,
        evidence: Dict[str, Any] | None = None,
        error: str = "",
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip()
        if not clean_action:
            return {"status": "error", "message": "action is required"}
        if not self.guardrails_enabled:
            return {"status": "disabled", "action": clean_action}

        normalized_status = str(status or "").strip().lower() or "unknown"
        payload_meta = metadata if isinstance(metadata, dict) else {}
        payload_evidence = evidence if isinstance(evidence, dict) else {}
        try:
            self.risk_engine.record_outcome(
                action=clean_action,
                status=normalized_status,
                source=str(source or "").strip(),
                error=str(error or "").strip(),
                metadata=payload_meta,
            )
        except Exception:
            pass
        risk = self.risk_engine.rate(
            clean_action,
            args={},
            source=str(source or "").strip(),
            metadata=payload_meta,
        )

        failure_signal = 0.0
        if normalized_status == "failed":
            failure_signal = 1.0
        elif normalized_status == "blocked":
            failure_signal = 0.85

        confirm_policy = payload_evidence.get("confirm_policy")
        if isinstance(confirm_policy, dict) and confirm_policy.get("satisfied") is False:
            failure_signal = max(failure_signal, 0.95)

        desktop_state = payload_evidence.get("desktop_state")
        if isinstance(desktop_state, dict):
            changed = bool(desktop_state.get("state_changed", False))
            change_count = self._coerce_int(desktop_state.get("change_count", 0), minimum=0, maximum=100_000, default=0)
            if not changed and change_count == 0 and normalized_status in {"failed", "blocked"}:
                failure_signal = max(failure_signal, 0.9)

        external_reliability = payload_evidence.get("external_reliability_preflight")
        if not isinstance(external_reliability, dict):
            external_reliability = payload_evidence.get("external_reliability")
        if isinstance(external_reliability, dict):
            preflight_status = str(external_reliability.get("status", "")).strip().lower()
            contract_diag = external_reliability.get("contract_diagnostic")
            contract = contract_diag if isinstance(contract_diag, dict) else {}
            contract_code = str(contract.get("code", "")).strip().lower()
            if contract_code in {"auth_preflight_failed", "no_provider_candidates_after_contract"}:
                failure_signal = max(failure_signal, 0.95)
            elif contract_code in {"provider_not_supported_for_action", "invalid_field_type_or_range", "invalid_event_window"}:
                failure_signal = max(failure_signal, 0.9)
            elif contract_code in {"missing_required_fields", "missing_any_of_fields", "contract_validation_failed"}:
                failure_signal = max(failure_signal, 0.82)
            if preflight_status in {"blocked", "error"} and normalized_status in {"failed", "blocked"}:
                failure_signal = max(failure_signal, 0.84)

        lowered_error = str(error or "").strip().lower()
        if lowered_error:
            if "confirm policy failed" in lowered_error:
                failure_signal = max(failure_signal, 0.95)
            elif "timeout" in lowered_error or "timed out" in lowered_error:
                failure_signal = max(failure_signal, 0.78)
            elif "rate limit" in lowered_error or "429" in lowered_error:
                failure_signal = max(failure_signal, 0.72)
            elif any(token in lowered_error for token in ("invalid", "not allowed", "explicitly denied", "non-retryable")):
                failure_signal = max(failure_signal, 0.88)

        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            previous = self._guardrails.get(clean_action, {})
            samples = self._coerce_int(previous.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
            successes = self._coerce_int(previous.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
            failures = self._coerce_int(previous.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
            blocked = self._coerce_int(previous.get("blocked", 0), minimum=0, maximum=10_000_000, default=0)
            consecutive_failures = self._coerce_int(
                previous.get("consecutive_failures", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            )
            ema_failure = self._coerce_float(previous.get("ema_failure", 0.0), minimum=0.0, maximum=1.0, default=0.0)

            if normalized_status == "success":
                successes += 1
                consecutive_failures = 0
            elif normalized_status == "blocked":
                failures += 1
                blocked += 1
                consecutive_failures += 1
            elif normalized_status != "skipped":
                failures += 1
                consecutive_failures += 1

            ema_failure = (ema_failure * self.guardrails_decay) + (failure_signal * (1.0 - self.guardrails_decay))
            ema_failure = max(0.0, min(1.0, ema_failure))

            empirical_success = (float(successes) + 1.5) / (float(samples) + 3.0)
            ema_success = 1.0 - ema_failure
            confidence = min(1.0, float(samples) / 40.0)
            blended_success = (empirical_success * 0.58) + (ema_success * 0.42)
            reliability_score = (blended_success * (0.72 + (0.28 * confidence))) - min(0.45, float(consecutive_failures) * 0.06)
            reliability_score = max(0.0, min(1.0, reliability_score))
            unstable_score = max(0.0, min(1.0, 1.0 - reliability_score))

            row = {
                "action": clean_action,
                "samples": samples,
                "successes": successes,
                "failures": failures,
                "blocked": blocked,
                "ema_failure": round(ema_failure, 6),
                "reliability_score": round(reliability_score, 6),
                "unstable_score": round(unstable_score, 6),
                "consecutive_failures": consecutive_failures,
                "last_status": normalized_status,
                "last_error": str(error or "").strip(),
                "last_source": str(source or "").strip(),
                "risk_level": risk.level,
                "risk_score": int(risk.score),
                "updated_at": now_iso,
            }
            self._guardrails[clean_action] = row
            self._trim_guardrails_locked()
            self._guardrail_updates_since_save += 1
            self._maybe_save_guardrails_locked(force=False)
            return {"status": "success", "guardrail": dict(row)}

    def recommend_runtime_overrides_for_actions(
        self,
        *,
        actions: List[str],
        source_name: str = "",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if not self.guardrails_enabled:
            return {
                "status": "disabled",
                "metadata_overrides": {},
                "action_overrides": {},
                "triggered_actions": [],
            }

        cleaned_actions = [str(item or "").strip() for item in actions if str(item or "").strip()]
        if not cleaned_actions:
            return {"status": "success", "metadata_overrides": {}, "action_overrides": {}, "triggered_actions": []}

        data = metadata if isinstance(metadata, dict) else {}
        current_profile = str(data.get("policy_profile", "")).strip().lower()
        strictness_rank = {"off": 0, "standard": 1, "strict": 2}
        current_strictness = str(data.get("verification_strictness", "")).strip().lower()
        current_strictness_rank = strictness_rank.get(current_strictness, 0)
        contract_pressure_raw = data.get("external_contract_pressure", {})
        contract_pressure_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(contract_pressure_raw, dict):
            for action_name, payload in contract_pressure_raw.items():
                clean_action = str(action_name or "").strip()
                if not clean_action:
                    continue
                row = payload if isinstance(payload, dict) else {}
                pressure = self._coerce_float(
                    row.get("pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if pressure <= 0.0:
                    continue
                contract_pressure_map[clean_action] = {
                    "pressure": pressure,
                    "code": str(row.get("code", "")).strip().lower(),
                    "severity": str(row.get("severity", "")).strip().lower(),
                    "attempt": self._coerce_int(row.get("attempt", 1), minimum=1, maximum=10000, default=1),
                }

        action_overrides: Dict[str, Dict[str, Any]] = {}
        triggered: List[Dict[str, Any]] = []
        target_strictness_rank = current_strictness_rank
        target_recovery = str(data.get("recovery_profile", "")).strip().lower()
        target_profile = current_profile

        with self._lock:
            for action_name in cleaned_actions:
                state = self._guardrails.get(action_name)
                contract_row = contract_pressure_map.get(action_name, {})
                contract_pressure = self._coerce_float(
                    contract_row.get("pressure", 0.0) if isinstance(contract_row, dict) else 0.0,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if not isinstance(state, dict) and contract_pressure <= 0.0:
                    continue
                samples = self._coerce_int(state.get("samples", 0), minimum=0, maximum=10_000_000, default=0) if isinstance(state, dict) else 0
                if samples < self.guardrails_min_samples and contract_pressure <= 0.0:
                    continue
                unstable_score = self._coerce_float(
                    state.get("unstable_score", 0.0) if isinstance(state, dict) else 0.0,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                reliability_score = self._coerce_float(
                    state.get("reliability_score", 0.0) if isinstance(state, dict) else 0.0,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                risk_level = str(state.get("risk_level", "")).strip().lower() if isinstance(state, dict) else ""
                effective_unstable = max(unstable_score, contract_pressure)
                if contract_pressure > 0.0 and not risk_level:
                    risk_level = "high" if contract_pressure >= 0.6 else "medium"

                severity = "stable"
                if effective_unstable >= self.guardrails_block_unstable_critical:
                    severity = "critical"
                elif effective_unstable >= self.guardrails_block_unstable_high:
                    severity = "high"
                elif effective_unstable >= self.guardrails_warn_unstable:
                    severity = "warning"

                if severity == "stable":
                    continue

                row = {
                    "action": action_name,
                    "severity": severity,
                    "samples": samples,
                    "unstable_score": round(effective_unstable, 4),
                    "reliability_score": round(reliability_score, 4),
                    "risk_level": risk_level,
                }
                if contract_pressure > 0.0:
                    row["contract_pressure"] = round(contract_pressure, 4)
                    row["contract_code"] = str(contract_row.get("code", "")).strip().lower()
                triggered.append(row)

                if severity in {"critical", "high"}:
                    action_overrides[action_name] = {
                        "max_retries_cap": 1,
                        "timeout_factor": 1.35 if severity == "critical" else 1.2,
                        "retry_multiplier": 1.25 if severity == "critical" else 1.12,
                    }
                    target_strictness_rank = max(target_strictness_rank, strictness_rank["strict"])
                    target_recovery = "safe"
                    if current_profile == "automation_power":
                        target_profile = "automation_safe"
                else:
                    action_overrides[action_name] = {
                        "max_retries_cap": 2,
                        "timeout_factor": 1.12,
                        "retry_multiplier": 1.08,
                    }
                    target_strictness_rank = max(target_strictness_rank, strictness_rank["standard"])
                    if not target_recovery:
                        target_recovery = "balanced"

        metadata_overrides: Dict[str, Any] = {}
        if target_strictness_rank > current_strictness_rank:
            reverse = {value: key for key, value in strictness_rank.items()}
            metadata_overrides["verification_strictness"] = reverse[target_strictness_rank]

        if target_recovery:
            current_recovery = str(data.get("recovery_profile", "")).strip().lower()
            if current_recovery != target_recovery:
                metadata_overrides["recovery_profile"] = target_recovery

        if target_profile and target_profile != current_profile:
            metadata_overrides["policy_profile"] = target_profile

        recommended_level = "none"
        if any(item.get("severity") == "critical" for item in triggered):
            recommended_level = "critical"
        elif any(item.get("severity") == "high" for item in triggered):
            recommended_level = "high"
        elif triggered:
            recommended_level = "warning"

        return {
            "status": "success",
            "source_name": str(source_name or "").strip(),
            "recommended_level": recommended_level,
            "metadata_overrides": metadata_overrides,
            "action_overrides": action_overrides,
            "triggered_actions": triggered,
        }

    def tune_from_operational_signals(
        self,
        *,
        autonomy_report: Dict[str, Any] | None = None,
        mission_summary: Dict[str, Any] | None = None,
        dry_run: bool = False,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        report = autonomy_report if isinstance(autonomy_report, dict) else {}
        missions = mission_summary if isinstance(mission_summary, dict) else {}
        try:
            self.risk_engine.ingest_mission_feedback(
                autonomy_report=report,
                mission_summary=missions,
                reason=str(reason or "").strip() or "policy_autotune",
            )
        except Exception:
            pass
        pressures = report.get("pressures", {}) if isinstance(report.get("pressures", {}), dict) else {}
        scores = report.get("scores", {}) if isinstance(report.get("scores", {}), dict) else {}
        guardrails = report.get("policy_guardrails", {}) if isinstance(report.get("policy_guardrails", {}), dict) else {}
        mission_risk = missions.get("risk", {}) if isinstance(missions.get("risk", {}), dict) else {}
        mission_quality = missions.get("quality", {}) if isinstance(missions.get("quality", {}), dict) else {}

        failure_pressure = self._coerce_float(pressures.get("failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        open_breaker_pressure = self._coerce_float(
            pressures.get("open_breaker_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        reliability_score = self._coerce_float(scores.get("reliability", 0.0), minimum=0.0, maximum=100.0, default=0.0)
        autonomy_score = self._coerce_float(scores.get("autonomy", 0.0), minimum=0.0, maximum=100.0, default=0.0)
        critical_guardrails = self._coerce_int(guardrails.get("critical_count", 0), minimum=0, maximum=100000, default=0)
        unstable_guardrails = self._coerce_int(guardrails.get("unstable_count", 0), minimum=0, maximum=100000, default=0)
        mission_risk_score = self._coerce_float(mission_risk.get("avg_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        mission_quality_score = self._coerce_float(mission_quality.get("avg_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        mission_failed_ratio = self._coerce_float(missions.get("failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)

        mode = "balanced"
        if (
            failure_pressure >= 0.34
            or mission_risk_score >= 0.58
            or critical_guardrails >= 2
            or open_breaker_pressure >= 0.2
        ):
            mode = "stability"
        elif (
            reliability_score >= 84
            and autonomy_score >= 78
            and mission_quality_score >= 0.72
            and mission_failed_ratio <= 0.12
            and critical_guardrails == 0
        ):
            mode = "throughput"

        current_config = {
            "guardrails_warn_unstable": float(self.guardrails_warn_unstable),
            "guardrails_block_unstable_high": float(self.guardrails_block_unstable_high),
            "guardrails_block_unstable_critical": float(self.guardrails_block_unstable_critical),
            "guardrails_block_unstable_medium_automation": float(self.guardrails_block_unstable_medium_automation),
            "guardrails_min_samples": int(self.guardrails_min_samples),
            "adaptive_default_profile": str(self._adaptive_default_profile or "").strip().lower(),
            "adaptive_source_defaults": dict(self._adaptive_source_default_profiles),
        }

        target_config = dict(current_config)
        if mode == "stability":
            target_config["guardrails_warn_unstable"] = 0.36
            target_config["guardrails_block_unstable_high"] = 0.57
            target_config["guardrails_block_unstable_critical"] = 0.49
            target_config["guardrails_block_unstable_medium_automation"] = 0.67
            target_config["guardrails_min_samples"] = max(4, min(12, int(self.guardrails_min_samples)))
            target_config["adaptive_default_profile"] = "automation_safe"
            target_config["adaptive_source_defaults"] = {
                "desktop-schedule": "automation_safe",
                "desktop-trigger": "automation_safe",
                "voice-loop": "automation_safe",
            }
        elif mode == "throughput":
            target_config["guardrails_warn_unstable"] = 0.52
            target_config["guardrails_block_unstable_high"] = 0.76
            target_config["guardrails_block_unstable_critical"] = 0.65
            target_config["guardrails_block_unstable_medium_automation"] = 0.86
            target_config["guardrails_min_samples"] = min(20, max(8, int(self.guardrails_min_samples)))
            target_config["adaptive_default_profile"] = ""
            target_config["adaptive_source_defaults"] = {
                "desktop-schedule": "automation_safe",
                "desktop-trigger": "automation_safe",
            }
        else:
            target_config["guardrails_warn_unstable"] = 0.45
            target_config["guardrails_block_unstable_high"] = 0.68
            target_config["guardrails_block_unstable_critical"] = 0.58
            target_config["guardrails_block_unstable_medium_automation"] = 0.8
            target_config["guardrails_min_samples"] = max(6, min(16, int(self.guardrails_min_samples)))
            target_config["adaptive_default_profile"] = ""
            target_config["adaptive_source_defaults"] = {
                "desktop-schedule": "automation_safe",
                "desktop-trigger": "automation_safe",
            }

        valid_profiles = set(self._known_profiles)
        target_default_profile = str(target_config.get("adaptive_default_profile", "")).strip().lower()
        if target_default_profile and target_default_profile not in valid_profiles:
            target_config["adaptive_default_profile"] = ""
        source_defaults = target_config.get("adaptive_source_defaults", {})
        if isinstance(source_defaults, dict):
            filtered_defaults = {
                str(key).strip().lower(): str(value).strip().lower()
                for key, value in source_defaults.items()
                if str(key).strip() and str(value).strip().lower() in valid_profiles
            }
            target_config["adaptive_source_defaults"] = filtered_defaults

        changed_fields: Dict[str, Any] = {}
        for key in (
            "guardrails_warn_unstable",
            "guardrails_block_unstable_high",
            "guardrails_block_unstable_critical",
            "guardrails_block_unstable_medium_automation",
            "guardrails_min_samples",
            "adaptive_default_profile",
            "adaptive_source_defaults",
        ):
            if target_config.get(key) != current_config.get(key):
                changed_fields[key] = {
                    "from": current_config.get(key),
                    "to": target_config.get(key),
                }

        applied = False
        if changed_fields and not dry_run:
            self.guardrails_warn_unstable = self._coerce_float(
                target_config["guardrails_warn_unstable"],
                minimum=0.05,
                maximum=0.99,
                default=self.guardrails_warn_unstable,
            )
            self.guardrails_block_unstable_high = self._coerce_float(
                target_config["guardrails_block_unstable_high"],
                minimum=0.1,
                maximum=0.99,
                default=self.guardrails_block_unstable_high,
            )
            self.guardrails_block_unstable_critical = self._coerce_float(
                target_config["guardrails_block_unstable_critical"],
                minimum=0.1,
                maximum=0.99,
                default=self.guardrails_block_unstable_critical,
            )
            self.guardrails_block_unstable_medium_automation = self._coerce_float(
                target_config["guardrails_block_unstable_medium_automation"],
                minimum=0.15,
                maximum=1.0,
                default=self.guardrails_block_unstable_medium_automation,
            )
            self.guardrails_min_samples = self._coerce_int(
                target_config["guardrails_min_samples"],
                minimum=1,
                maximum=500,
                default=self.guardrails_min_samples,
            )
            self._adaptive_default_profile = str(target_config.get("adaptive_default_profile", "")).strip().lower()
            source_defaults = target_config.get("adaptive_source_defaults", {})
            self._adaptive_source_default_profiles = (
                {str(k).strip().lower(): str(v).strip().lower() for k, v in source_defaults.items() if str(k).strip() and str(v).strip()}
                if isinstance(source_defaults, dict)
                else {}
            )
            with self._lock:
                self._maybe_save_guardrails_locked(force=True)
            applied = True

        state = {
            "status": "success",
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "changed": bool(changed_fields),
            "applied": applied,
            "dry_run": bool(dry_run),
            "reason": str(reason or "").strip() or "manual",
            "changes": changed_fields,
        }
        self._adaptive_tuning_state = state
        return dict(state)

    def guardrail_snapshot(
        self,
        *,
        action: str = "",
        limit: int = 100,
        min_samples: int = 0,
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=100)
        min_rows = self._coerce_int(min_samples, minimum=0, maximum=500000, default=0)
        target_action = str(action or "").strip()
        with self._lock:
            rows = list(self._guardrails.values())
        if target_action:
            rows = [row for row in rows if str(row.get("action", "")).strip() == target_action]
        if min_rows > 0:
            rows = [
                row
                for row in rows
                if self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0) >= min_rows
            ]
        rows.sort(
            key=lambda row: (
                -float(row.get("unstable_score", 0.0) or 0.0),
                -int(row.get("samples", 0) or 0),
                str(row.get("action", "")),
            )
        )
        items = [dict(row) for row in rows[:bounded]]
        return {
            "status": "success",
            "enabled": bool(self.guardrails_enabled),
            "count": len(items),
            "total": len(rows),
            "min_samples": self.guardrails_min_samples,
            "thresholds": {
                "warn_unstable": self.guardrails_warn_unstable,
                "block_unstable_high": self.guardrails_block_unstable_high,
                "block_unstable_critical": self.guardrails_block_unstable_critical,
                "block_unstable_medium_automation": self.guardrails_block_unstable_medium_automation,
            },
            "adaptive_defaults": {
                "default_profile": self._adaptive_default_profile,
                "source_defaults": dict(self._adaptive_source_default_profiles),
            },
            "last_tune": dict(self._adaptive_tuning_state),
            "items": items,
        }

    def reset_guardrails(self, *, action: str = "") -> Dict[str, Any]:
        target = str(action or "").strip()
        removed = 0
        with self._lock:
            if target:
                if target in self._guardrails:
                    self._guardrails.pop(target, None)
                    removed = 1
            else:
                removed = len(self._guardrails)
                self._guardrails = {}
            self._maybe_save_guardrails_locked(force=True)
        return {
            "status": "success",
            "removed": removed,
            "action": target,
        }

    def _load_guardrails(self) -> None:
        if not self.guardrails_store_path.exists():
            return
        try:
            payload = json.loads(self.guardrails_store_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return
        config = payload.get("config", {}) if isinstance(payload, dict) else {}
        if isinstance(config, dict):
            self.guardrails_warn_unstable = self._coerce_float(
                config.get("guardrails_warn_unstable", self.guardrails_warn_unstable),
                minimum=0.05,
                maximum=0.99,
                default=self.guardrails_warn_unstable,
            )
            self.guardrails_block_unstable_high = self._coerce_float(
                config.get("guardrails_block_unstable_high", self.guardrails_block_unstable_high),
                minimum=0.1,
                maximum=0.99,
                default=self.guardrails_block_unstable_high,
            )
            self.guardrails_block_unstable_critical = self._coerce_float(
                config.get("guardrails_block_unstable_critical", self.guardrails_block_unstable_critical),
                minimum=0.1,
                maximum=0.99,
                default=self.guardrails_block_unstable_critical,
            )
            self.guardrails_block_unstable_medium_automation = self._coerce_float(
                config.get("guardrails_block_unstable_medium_automation", self.guardrails_block_unstable_medium_automation),
                minimum=0.15,
                maximum=1.0,
                default=self.guardrails_block_unstable_medium_automation,
            )
            self.guardrails_min_samples = self._coerce_int(
                config.get("guardrails_min_samples", self.guardrails_min_samples),
                minimum=1,
                maximum=500,
                default=self.guardrails_min_samples,
            )
            self._adaptive_default_profile = str(config.get("adaptive_default_profile", "")).strip().lower()
            source_defaults = config.get("adaptive_source_default_profiles", {})
            if isinstance(source_defaults, dict):
                self._adaptive_source_default_profiles = {
                    str(key).strip().lower(): str(value).strip().lower()
                    for key, value in source_defaults.items()
                    if str(key).strip() and str(value).strip()
                }
            tune_state = config.get("adaptive_tuning_state", {})
            if isinstance(tune_state, dict):
                self._adaptive_tuning_state.update(
                    {
                        "status": str(tune_state.get("status", self._adaptive_tuning_state.get("status", ""))).strip(),
                        "last_run_at": str(tune_state.get("last_run_at", self._adaptive_tuning_state.get("last_run_at", ""))).strip(),
                        "mode": str(tune_state.get("mode", self._adaptive_tuning_state.get("mode", ""))).strip(),
                        "changed": bool(tune_state.get("changed", self._adaptive_tuning_state.get("changed", False))),
                        "dry_run": bool(tune_state.get("dry_run", self._adaptive_tuning_state.get("dry_run", False))),
                        "reason": str(tune_state.get("reason", self._adaptive_tuning_state.get("reason", ""))).strip(),
                    }
                )
        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            action = str(row.get("action", "")).strip()
            if not action:
                continue
            loaded[action] = {
                "action": action,
                "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": self._coerce_int(row.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": self._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "blocked": self._coerce_int(row.get("blocked", 0), minimum=0, maximum=10_000_000, default=0),
                "ema_failure": self._coerce_float(row.get("ema_failure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "reliability_score": self._coerce_float(
                    row.get("reliability_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "unstable_score": self._coerce_float(row.get("unstable_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "consecutive_failures": self._coerce_int(
                    row.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "last_status": str(row.get("last_status", "")).strip().lower(),
                "last_error": str(row.get("last_error", "")).strip(),
                "last_source": str(row.get("last_source", "")).strip(),
                "risk_level": str(row.get("risk_level", "")).strip().lower(),
                "risk_score": self._coerce_int(row.get("risk_score", 0), minimum=0, maximum=100, default=0),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        with self._lock:
            self._guardrails = loaded
            self._trim_guardrails_locked()

    def _trim_guardrails_locked(self) -> None:
        if len(self._guardrails) <= self.guardrails_max_records:
            return
        rows = sorted(
            self._guardrails.values(),
            key=lambda row: (
                str(row.get("updated_at", "")),
                int(row.get("samples", 0) or 0),
                str(row.get("action", "")),
            ),
            reverse=True,
        )
        trimmed = rows[: self.guardrails_max_records]
        self._guardrails = {str(row.get("action", "")).strip(): dict(row) for row in trimmed if str(row.get("action", "")).strip()}

    def _maybe_save_guardrails_locked(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force:
            if self._guardrail_updates_since_save < 16 and (now - self._guardrail_last_save_monotonic) < 20.0:
                return
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "guardrails_warn_unstable": self.guardrails_warn_unstable,
                "guardrails_block_unstable_high": self.guardrails_block_unstable_high,
                "guardrails_block_unstable_critical": self.guardrails_block_unstable_critical,
                "guardrails_block_unstable_medium_automation": self.guardrails_block_unstable_medium_automation,
                "guardrails_min_samples": self.guardrails_min_samples,
                "adaptive_default_profile": self._adaptive_default_profile,
                "adaptive_source_default_profiles": dict(self._adaptive_source_default_profiles),
                "adaptive_tuning_state": dict(self._adaptive_tuning_state),
            },
            "items": list(self._guardrails.values()),
        }
        try:
            self.guardrails_store_path.parent.mkdir(parents=True, exist_ok=True)
            self.guardrails_store_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            self._guardrail_last_save_monotonic = now
            self._guardrail_updates_since_save = 0
        except Exception:
            # Non-fatal: keep in-memory state.
            pass

    @staticmethod
    def _coerce_bool(value: object, *, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        clean = str(value or "").strip().lower()
        if clean in {"1", "true", "yes", "on"}:
            return True
        if clean in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    def resolve_policy_profile(self, source_name: str, metadata: Dict[str, Any] | None = None) -> str:
        normalized_source = str(source_name or "").strip().lower()
        data = metadata if isinstance(metadata, dict) else {}
        explicit = str(data.get("policy_profile", "")).strip().lower()
        if explicit:
            return explicit
        adaptive_source = self._adaptive_source_default_profiles.get(normalized_source, "")
        if adaptive_source and adaptive_source in self._known_profiles:
            return adaptive_source
        source_default = self.source_default_profile.get(normalized_source, "")
        if source_default:
            return source_default
        if self._adaptive_default_profile and self._adaptive_default_profile in self._known_profiles:
            return self._adaptive_default_profile
        if self.default_profile_name:
            return self.default_profile_name
        return ""

    def decorate_metadata_with_defaults(self, source_name: str, metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
        out: Dict[str, Any] = dict(metadata) if isinstance(metadata, dict) else {}
        profile = self.resolve_policy_profile(source_name=source_name, metadata=out)
        if profile and not str(out.get("policy_profile", "")).strip():
            out["policy_profile"] = profile
        if profile:
            runtime = self.profile_default_max_runtime_s.get(profile)
            if runtime is not None and not str(out.get("max_runtime_s", "")).strip():
                out["max_runtime_s"] = runtime

            steps = self.profile_default_max_steps.get(profile)
            if steps is not None and not str(out.get("max_steps", "")).strip():
                out["max_steps"] = steps
        return out

    def list_profiles(self) -> Dict[str, Any]:
        names = sorted(self._known_profiles)
        items = []
        for name in names:
            allow = sorted(self.profile_allow_actions.get(name, set()))
            deny = sorted(self.profile_deny_actions.get(name, set()))
            items.append(
                {
                    "name": name,
                    "allow": allow,
                    "deny": deny,
                    "allow_high_risk": bool(self.profile_allow_high_risk.get(name, False)),
                    "default_max_runtime_s": self.profile_default_max_runtime_s.get(name),
                    "default_max_steps": self.profile_default_max_steps.get(name),
                }
            )
        return {
            "items": items,
            "count": len(items),
            "default_profile": self.default_profile_name,
            "source_defaults": dict(self.source_default_profile),
            "adaptive_defaults": {
                "default_profile": self._adaptive_default_profile,
                "source_defaults": dict(self._adaptive_source_default_profiles),
            },
            "last_tune": dict(self._adaptive_tuning_state),
        }
