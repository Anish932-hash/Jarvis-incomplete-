from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_desktop_governance_profile(value: object) -> str:
    clean = str(value or "").strip().lower()
    if clean in {"conservative", "balanced", "power", "custom"}:
        return clean
    return "balanced"


def desktop_governance_profile_defaults(profile: str) -> Dict[str, Any]:
    normalized = normalize_desktop_governance_profile(profile)
    if normalized == "conservative":
        return {
            "allow_high_risk": False,
            "allow_critical_risk": False,
            "allow_admin_clearance": False,
            "allow_destructive": False,
            "allow_desktop_approval_reuse": False,
            "allow_action_confirmation_reuse": False,
            "desktop_approval_reuse_window_s": 0,
            "action_confirmation_reuse_window_s": 0,
        }
    if normalized == "power":
        return {
            "allow_high_risk": True,
            "allow_critical_risk": True,
            "allow_admin_clearance": False,
            "allow_destructive": False,
            "allow_desktop_approval_reuse": True,
            "allow_action_confirmation_reuse": True,
            "desktop_approval_reuse_window_s": 240,
            "action_confirmation_reuse_window_s": 120,
        }
    return {
        "allow_high_risk": True,
        "allow_critical_risk": False,
        "allow_admin_clearance": False,
        "allow_destructive": False,
        "allow_desktop_approval_reuse": True,
        "allow_action_confirmation_reuse": True,
        "desktop_approval_reuse_window_s": 90,
        "action_confirmation_reuse_window_s": 45,
    }


def desktop_governance_profile_catalog() -> Dict[str, Dict[str, Any]]:
    return {
        "conservative": {
            "profile": "conservative",
            "title": "Conservative",
            "summary": "Always prefer fresh approvals and never auto-reuse risky desktop actions.",
            **desktop_governance_profile_defaults("conservative"),
        },
        "balanced": {
            "profile": "balanced",
            "title": "Balanced",
            "summary": "Allow bounded exact-match reuse for repeat high-risk actions without relaxing admin or destructive safety.",
            **desktop_governance_profile_defaults("balanced"),
        },
        "power": {
            "profile": "power",
            "title": "Power",
            "summary": "Allow broader exact-match reuse for repeat high-risk desktop work while still requiring fresh admin and destructive approvals.",
            **desktop_governance_profile_defaults("power"),
        },
    }


class DesktopGovernancePolicyManager:
    def __init__(
        self,
        *,
        state_path: str = "data/desktop_governance_policy.json",
        policy_profile: str = "balanced",
        allow_high_risk: Optional[bool] = None,
        allow_critical_risk: Optional[bool] = None,
        allow_admin_clearance: Optional[bool] = None,
        allow_destructive: Optional[bool] = None,
        allow_desktop_approval_reuse: Optional[bool] = None,
        allow_action_confirmation_reuse: Optional[bool] = None,
        desktop_approval_reuse_window_s: Optional[int] = None,
        action_confirmation_reuse_window_s: Optional[int] = None,
    ) -> None:
        self._store = LocalStore(state_path)
        self._lock = threading.RLock()
        self._config = self._default_config(
            policy_profile=policy_profile,
            allow_high_risk=allow_high_risk,
            allow_critical_risk=allow_critical_risk,
            allow_admin_clearance=allow_admin_clearance,
            allow_destructive=allow_destructive,
            allow_desktop_approval_reuse=allow_desktop_approval_reuse,
            allow_action_confirmation_reuse=allow_action_confirmation_reuse,
            desktop_approval_reuse_window_s=desktop_approval_reuse_window_s,
            action_confirmation_reuse_window_s=action_confirmation_reuse_window_s,
        )
        self._meta: Dict[str, Any] = {"updated_at": "", "source": "defaults"}
        self._load()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return self._public_status_locked()

    def configure(
        self,
        *,
        policy_profile: Optional[str] = None,
        allow_high_risk: Optional[bool] = None,
        allow_critical_risk: Optional[bool] = None,
        allow_admin_clearance: Optional[bool] = None,
        allow_destructive: Optional[bool] = None,
        allow_desktop_approval_reuse: Optional[bool] = None,
        allow_action_confirmation_reuse: Optional[bool] = None,
        desktop_approval_reuse_window_s: Optional[int] = None,
        action_confirmation_reuse_window_s: Optional[int] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            if policy_profile is not None:
                self._config["policy_profile"] = normalize_desktop_governance_profile(policy_profile)
                self._apply_profile_defaults_locked(force=True)
            direct_overrides = {
                "allow_high_risk": allow_high_risk,
                "allow_critical_risk": allow_critical_risk,
                "allow_admin_clearance": allow_admin_clearance,
                "allow_destructive": allow_destructive,
                "allow_desktop_approval_reuse": allow_desktop_approval_reuse,
                "allow_action_confirmation_reuse": allow_action_confirmation_reuse,
            }
            for key, value in direct_overrides.items():
                if value is not None:
                    self._config[key] = bool(value)
                    self._config["policy_profile"] = "custom"
            if desktop_approval_reuse_window_s is not None:
                self._config["desktop_approval_reuse_window_s"] = self._coerce_int(
                    desktop_approval_reuse_window_s,
                    minimum=0,
                    maximum=3600,
                    default=90,
                )
                self._config["policy_profile"] = "custom"
            if action_confirmation_reuse_window_s is not None:
                self._config["action_confirmation_reuse_window_s"] = self._coerce_int(
                    action_confirmation_reuse_window_s,
                    minimum=0,
                    maximum=3600,
                    default=45,
                )
                self._config["policy_profile"] = "custom"
            self._meta["updated_at"] = _utc_now_iso()
            self._meta["source"] = str(source or "manual").strip().lower() or "manual"
            self._persist_locked()
            return self._public_status_locked()

    def resolve(
        self,
        *,
        policy_profile: str = "",
        allow_high_risk: Optional[bool] = None,
        allow_critical_risk: Optional[bool] = None,
        allow_admin_clearance: Optional[bool] = None,
        allow_destructive: Optional[bool] = None,
        allow_desktop_approval_reuse: Optional[bool] = None,
        allow_action_confirmation_reuse: Optional[bool] = None,
        desktop_approval_reuse_window_s: Optional[int] = None,
        action_confirmation_reuse_window_s: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            payload = dict(self._config)
        overrides = {
            "policy_profile": policy_profile if str(policy_profile or "").strip() else None,
            "allow_high_risk": allow_high_risk,
            "allow_critical_risk": allow_critical_risk,
            "allow_admin_clearance": allow_admin_clearance,
            "allow_destructive": allow_destructive,
            "allow_desktop_approval_reuse": allow_desktop_approval_reuse,
            "allow_action_confirmation_reuse": allow_action_confirmation_reuse,
            "desktop_approval_reuse_window_s": desktop_approval_reuse_window_s,
            "action_confirmation_reuse_window_s": action_confirmation_reuse_window_s,
        }
        return self._apply_overrides(payload, overrides)

    def _load(self) -> None:
        with self._lock:
            payload = self._store.get("config", default={})
            if isinstance(payload, dict):
                merged = self._apply_overrides(self._config, payload)
                self._config = merged
                self._meta["updated_at"] = str(payload.get("updated_at", "") or "").strip()
                self._meta["source"] = str(payload.get("source", "persisted") or "persisted").strip().lower() or "persisted"

    def _persist_locked(self) -> None:
        payload = dict(self._config)
        payload["updated_at"] = str(self._meta.get("updated_at", "") or "").strip()
        payload["source"] = str(self._meta.get("source", "manual") or "manual").strip().lower() or "manual"
        self._store.set("config", payload)

    def _public_status_locked(self) -> Dict[str, Any]:
        payload = dict(self._config)
        payload["status"] = "success"
        payload["updated_at"] = str(self._meta.get("updated_at", "") or "").strip()
        payload["source"] = str(self._meta.get("source", "") or "").strip().lower()
        payload["profiles"] = copy.deepcopy(desktop_governance_profile_catalog())
        return payload

    @classmethod
    def _default_config(
        cls,
        *,
        policy_profile: str,
        allow_high_risk: Optional[bool],
        allow_critical_risk: Optional[bool],
        allow_admin_clearance: Optional[bool],
        allow_destructive: Optional[bool],
        allow_desktop_approval_reuse: Optional[bool],
        allow_action_confirmation_reuse: Optional[bool],
        desktop_approval_reuse_window_s: Optional[int],
        action_confirmation_reuse_window_s: Optional[int],
    ) -> Dict[str, Any]:
        normalized_profile = normalize_desktop_governance_profile(policy_profile)
        defaults = desktop_governance_profile_defaults(normalized_profile)
        config = {
            "policy_profile": normalized_profile,
            "allow_high_risk": bool(defaults["allow_high_risk"] if allow_high_risk is None else allow_high_risk),
            "allow_critical_risk": bool(defaults["allow_critical_risk"] if allow_critical_risk is None else allow_critical_risk),
            "allow_admin_clearance": bool(defaults["allow_admin_clearance"] if allow_admin_clearance is None else allow_admin_clearance),
            "allow_destructive": bool(defaults["allow_destructive"] if allow_destructive is None else allow_destructive),
            "allow_desktop_approval_reuse": bool(
                defaults["allow_desktop_approval_reuse"]
                if allow_desktop_approval_reuse is None
                else allow_desktop_approval_reuse
            ),
            "allow_action_confirmation_reuse": bool(
                defaults["allow_action_confirmation_reuse"]
                if allow_action_confirmation_reuse is None
                else allow_action_confirmation_reuse
            ),
            "desktop_approval_reuse_window_s": cls._coerce_int(
                defaults["desktop_approval_reuse_window_s"] if desktop_approval_reuse_window_s is None else desktop_approval_reuse_window_s,
                minimum=0,
                maximum=3600,
                default=int(defaults["desktop_approval_reuse_window_s"]),
            ),
            "action_confirmation_reuse_window_s": cls._coerce_int(
                defaults["action_confirmation_reuse_window_s"] if action_confirmation_reuse_window_s is None else action_confirmation_reuse_window_s,
                minimum=0,
                maximum=3600,
                default=int(defaults["action_confirmation_reuse_window_s"]),
            ),
        }
        if any(
            value is not None
            for value in (
                allow_high_risk,
                allow_critical_risk,
                allow_admin_clearance,
                allow_destructive,
                allow_desktop_approval_reuse,
                allow_action_confirmation_reuse,
                desktop_approval_reuse_window_s,
                action_confirmation_reuse_window_s,
            )
        ):
            defaults_all = desktop_governance_profile_defaults(normalized_profile)
            if any(
                bool(config.get(key)) != bool(defaults_all.get(key))
                for key in (
                    "allow_high_risk",
                    "allow_critical_risk",
                    "allow_admin_clearance",
                    "allow_destructive",
                    "allow_desktop_approval_reuse",
                    "allow_action_confirmation_reuse",
                )
            ) or any(
                int(config.get(key, 0) or 0) != int(defaults_all.get(key, 0) or 0)
                for key in ("desktop_approval_reuse_window_s", "action_confirmation_reuse_window_s")
            ):
                config["policy_profile"] = "custom"
        return config

    def _apply_profile_defaults_locked(self, *, force: bool = False) -> None:
        profile = normalize_desktop_governance_profile(self._config.get("policy_profile", "balanced"))
        self._config["policy_profile"] = profile
        if profile == "custom" and not force:
            return
        defaults = desktop_governance_profile_defaults(profile)
        for key, value in defaults.items():
            self._config[key] = value

    @classmethod
    def _apply_overrides(cls, base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(base)
        if not isinstance(overrides, dict):
            return payload
        explicit_profile = overrides.get("policy_profile") if overrides.get("policy_profile") is not None else None
        normalized_profile = normalize_desktop_governance_profile(explicit_profile) if explicit_profile is not None else ""
        explicit_profile_override = False
        for key in (
            "policy_profile",
            "allow_high_risk",
            "allow_critical_risk",
            "allow_admin_clearance",
            "allow_destructive",
            "allow_desktop_approval_reuse",
            "allow_action_confirmation_reuse",
            "desktop_approval_reuse_window_s",
            "action_confirmation_reuse_window_s",
        ):
            if key in overrides and overrides[key] is not None:
                payload[key] = overrides[key]
                if key != "policy_profile":
                    explicit_profile_override = True
        if explicit_profile is not None:
            payload["policy_profile"] = normalized_profile
            if payload["policy_profile"] != "custom":
                defaults = desktop_governance_profile_defaults(payload["policy_profile"])
                for key, value in defaults.items():
                    if overrides.get(key) is None:
                        payload[key] = value
        if explicit_profile_override:
            if not normalized_profile:
                payload["policy_profile"] = "custom"
            elif normalized_profile == "custom":
                payload["policy_profile"] = "custom"
            else:
                defaults = desktop_governance_profile_defaults(normalized_profile)
                bool_keys = (
                    "allow_high_risk",
                    "allow_critical_risk",
                    "allow_admin_clearance",
                    "allow_destructive",
                    "allow_desktop_approval_reuse",
                    "allow_action_confirmation_reuse",
                )
                int_keys = ("desktop_approval_reuse_window_s", "action_confirmation_reuse_window_s")
                if any(
                    overrides.get(key) is not None and bool(overrides.get(key)) != bool(defaults[key])
                    for key in bool_keys
                ) or any(
                    overrides.get(key) is not None and int(overrides.get(key) or 0) != int(defaults[key] or 0)
                    for key in int_keys
                ):
                    payload["policy_profile"] = "custom"
        payload["policy_profile"] = normalize_desktop_governance_profile(payload.get("policy_profile", "balanced"))
        payload["desktop_approval_reuse_window_s"] = cls._coerce_int(
            payload.get("desktop_approval_reuse_window_s", 90),
            minimum=0,
            maximum=3600,
            default=90,
        )
        payload["action_confirmation_reuse_window_s"] = cls._coerce_int(
            payload.get("action_confirmation_reuse_window_s", 45),
            minimum=0,
            maximum=3600,
            default=45,
        )
        for key in (
            "allow_high_risk",
            "allow_critical_risk",
            "allow_admin_clearance",
            "allow_destructive",
            "allow_desktop_approval_reuse",
            "allow_action_confirmation_reuse",
        ):
            payload[key] = bool(payload.get(key, False))
        return payload

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            result = int(value)
        except Exception:
            return default
        return max(minimum, min(maximum, result))
