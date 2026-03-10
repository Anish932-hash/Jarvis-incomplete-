import hashlib
import os
from dataclasses import dataclass

from .contracts import ActionResult, PlanStep


@dataclass(slots=True)
class RecoveryDecision:
    retry: bool
    delay_s: float = 0.0
    reason: str = ""
    category: str = ""
    profile: str = ""


@dataclass(slots=True)
class RetryProfile:
    base_delay_s: float = 0.5
    max_delay_s: float = 5.0
    multiplier: float = 2.0
    jitter_s: float = 0.0


@dataclass(slots=True)
class RecoveryRuntimeProfile:
    name: str = "balanced"
    retry_adjust: int = 0
    base_delay_factor: float = 1.0
    max_delay_factor: float = 1.0
    multiplier_factor: float = 1.0
    jitter_factor: float = 1.0
    retry_unknown_failures: bool = True


class RecoveryManager:
    """
    Determines retry behavior for failed steps.
    """

    _NON_RETRYABLE_HINTS = (
        "not registered",
        "not allowed",
        "explicitly denied",
        "approval required",
        "requires explicit user approval",
        "missing required",
        "invalid",
        "malformed",
        "unknown policy profile",
        "dependency deadlock",
    )
    _TRANSIENT_HINTS = (
        "timeout",
        "timed out",
        "temporar",
        "unavailable",
        "connection",
        "reset by peer",
        "resource exhausted",
        "service busy",
        "try again",
    )

    RUNTIME_PROFILES = {
        "safe": RecoveryRuntimeProfile(
            name="safe",
            retry_adjust=-1,
            base_delay_factor=1.25,
            max_delay_factor=1.35,
            multiplier_factor=1.05,
            jitter_factor=0.75,
            retry_unknown_failures=False,
        ),
        "balanced": RecoveryRuntimeProfile(
            name="balanced",
            retry_adjust=0,
            base_delay_factor=1.0,
            max_delay_factor=1.0,
            multiplier_factor=1.0,
            jitter_factor=1.0,
            retry_unknown_failures=True,
        ),
        "aggressive": RecoveryRuntimeProfile(
            name="aggressive",
            retry_adjust=2,
            base_delay_factor=0.75,
            max_delay_factor=1.25,
            multiplier_factor=0.92,
            jitter_factor=1.2,
            retry_unknown_failures=True,
        ),
    }

    def __init__(self) -> None:
        configured_default = str(os.getenv("JARVIS_RECOVERY_PROFILE", "balanced") or "balanced").strip().lower()
        if configured_default not in self.RUNTIME_PROFILES:
            configured_default = "balanced"
        self.default_profile = configured_default

    def list_profiles(self) -> dict[str, object]:
        items: list[dict[str, object]] = []
        for name, profile in self.RUNTIME_PROFILES.items():
            items.append(
                {
                    "name": profile.name,
                    "retry_adjust": int(profile.retry_adjust),
                    "base_delay_factor": float(profile.base_delay_factor),
                    "max_delay_factor": float(profile.max_delay_factor),
                    "multiplier_factor": float(profile.multiplier_factor),
                    "jitter_factor": float(profile.jitter_factor),
                    "retry_unknown_failures": bool(profile.retry_unknown_failures),
                    "is_default": name == self.default_profile,
                }
            )
        items.sort(key=lambda row: str(row.get("name", "")))
        return {"status": "success", "default_profile": self.default_profile, "items": items, "count": len(items)}

    def set_default_profile(self, profile_name: str) -> tuple[bool, str, str]:
        clean = str(profile_name or "").strip().lower()
        if clean not in self.RUNTIME_PROFILES:
            return (False, f"Unknown recovery profile '{profile_name}'.", self.default_profile)
        self.default_profile = clean
        return (True, "Recovery profile updated.", self.default_profile)

    def decide(
        self,
        step: PlanStep,
        result: ActionResult,
        attempt: int,
        *,
        metadata: dict[str, object] | None = None,
        policy_profile: str = "",
        recovery_profile: str = "",
        verification_strictness: str = "",
    ) -> RecoveryDecision:
        runtime_profile = self._resolve_runtime_profile(
            metadata=metadata,
            policy_profile=policy_profile,
            recovery_profile=recovery_profile,
            verification_strictness=verification_strictness,
        )
        effective_max_retries = self._effective_max_retries(step.max_retries, runtime_profile)

        if not step.can_retry:
            return RecoveryDecision(retry=False, reason="Retry disabled for step.", profile=runtime_profile.name)

        if result.status in ("blocked", "skipped"):
            return RecoveryDecision(
                retry=False,
                reason=f"Status {result.status} is non-retryable.",
                profile=runtime_profile.name,
            )

        if attempt >= effective_max_retries:
            return RecoveryDecision(
                retry=False,
                reason=f"Max retry count reached ({effective_max_retries}).",
                profile=runtime_profile.name,
            )

        category = self._classify_failure(result)
        if category == "non_retryable":
            return RecoveryDecision(
                retry=False,
                reason="Failure classified as non-retryable.",
                category=category,
                profile=runtime_profile.name,
            )
        if category == "unknown" and not runtime_profile.retry_unknown_failures:
            return RecoveryDecision(
                retry=False,
                reason="Unknown failure category is disabled by recovery profile.",
                category=category,
                profile=runtime_profile.name,
            )

        profile = self._retry_profile(step)
        base_delay = profile.base_delay_s * runtime_profile.base_delay_factor
        if category == "rate_limited":
            base_delay = max(base_delay, 1.5)
        elif category == "timeout":
            base_delay = max(base_delay, 0.8)

        max_delay = max(base_delay, profile.max_delay_s * runtime_profile.max_delay_factor)
        multiplier = max(1.0, profile.multiplier * runtime_profile.multiplier_factor)
        jitter = max(0.0, profile.jitter_s * runtime_profile.jitter_factor)

        delay = min(max_delay, base_delay * (multiplier ** max(0, attempt - 1)))
        delay += self._deterministic_jitter(step.step_id, attempt, jitter)
        delay = max(0.0, min(delay, 60.0))
        return RecoveryDecision(
            retry=True,
            delay_s=delay,
            reason=f"Retrying ({category}) with adaptive backoff.",
            category=category,
            profile=runtime_profile.name,
        )

    def _resolve_runtime_profile(
        self,
        *,
        metadata: dict[str, object] | None,
        policy_profile: str,
        recovery_profile: str,
        verification_strictness: str,
    ) -> RecoveryRuntimeProfile:
        direct = str(recovery_profile or "").strip().lower()
        if direct not in self.RUNTIME_PROFILES and isinstance(metadata, dict):
            direct = str(metadata.get("recovery_profile", "")).strip().lower()

        selected = direct
        if selected not in self.RUNTIME_PROFILES:
            strictness = str(verification_strictness or "").strip().lower()
            if strictness == "strict":
                selected = "safe"
            elif strictness == "off":
                selected = "aggressive"

        if selected not in self.RUNTIME_PROFILES:
            profile_name = str(policy_profile or "").strip().lower()
            if profile_name in {"automation_safe"}:
                selected = "safe"
            elif profile_name in {"automation_power"}:
                selected = "aggressive"

        if selected not in self.RUNTIME_PROFILES:
            selected = self.default_profile
        return self.RUNTIME_PROFILES.get(selected, self.RUNTIME_PROFILES["balanced"])

    @staticmethod
    def _effective_max_retries(configured: int, profile: RecoveryRuntimeProfile) -> int:
        base = max(0, int(configured))
        adjusted = base + int(profile.retry_adjust)
        return max(0, min(adjusted, 10))

    def _retry_profile(self, step: PlanStep) -> RetryProfile:
        verify = step.verify if isinstance(step.verify, dict) else {}
        raw_retry = verify.get("retry", {})
        retry_cfg = raw_retry if isinstance(raw_retry, dict) else {}

        base_delay = self._coerce_float(retry_cfg.get("base_delay_s"), default=0.5, minimum=0.0, maximum=30.0)
        max_delay = self._coerce_float(retry_cfg.get("max_delay_s"), default=5.0, minimum=0.1, maximum=60.0)
        multiplier = self._coerce_float(retry_cfg.get("multiplier"), default=2.0, minimum=1.0, maximum=5.0)
        jitter = self._coerce_float(retry_cfg.get("jitter_s"), default=0.0, minimum=0.0, maximum=3.0)

        max_delay = max(max_delay, base_delay)
        return RetryProfile(
            base_delay_s=base_delay,
            max_delay_s=max_delay,
            multiplier=multiplier,
            jitter_s=jitter,
        )

    def _classify_failure(self, result: ActionResult) -> str:
        message = self._failure_message(result).lower()
        if not message:
            return "unknown"

        if "rate limit" in message or "429" in message:
            return "rate_limited"
        if "timeout" in message or "timed out" in message:
            return "timeout"
        if any(hint in message for hint in self._NON_RETRYABLE_HINTS):
            return "non_retryable"
        if any(hint in message for hint in self._TRANSIENT_HINTS):
            return "transient"
        return "unknown"

    @staticmethod
    def _failure_message(result: ActionResult) -> str:
        chunks: list[str] = []
        if isinstance(result.error, str) and result.error.strip():
            chunks.append(result.error.strip())
        output = result.output if isinstance(result.output, dict) else {}
        for key in ("message", "error", "code"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                chunks.append(value.strip())
        return " ".join(chunks)

    @staticmethod
    def _deterministic_jitter(step_id: str, attempt: int, jitter_s: float) -> float:
        span = max(0.0, float(jitter_s))
        if span <= 0:
            return 0.0
        token = f"{step_id}:{attempt}".encode("utf-8", errors="ignore")
        digest = hashlib.sha1(token).hexdigest()  # noqa: S324
        fraction = (int(digest[:8], 16) % 1000) / 1000.0
        return fraction * span

    @staticmethod
    def _coerce_float(value: object, *, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))
