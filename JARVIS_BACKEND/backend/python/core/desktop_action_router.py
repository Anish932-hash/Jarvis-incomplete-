from __future__ import annotations

import re
import time
from typing import Any, Callable, Dict, List, Optional

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry


ActionHandler = Callable[[Dict[str, Any]], Dict[str, Any]]


class DesktopActionRouter:
    def __init__(
        self,
        *,
        action_handlers: Optional[Dict[str, ActionHandler]] = None,
        app_profile_registry: Optional[DesktopAppProfileRegistry] = None,
        settle_delay_s: float = 0.35,
    ) -> None:
        self._handlers = self._default_handlers()
        if isinstance(action_handlers, dict):
            self._handlers.update({str(key): value for key, value in action_handlers.items() if callable(value)})
        self._app_profile_registry = app_profile_registry or DesktopAppProfileRegistry()
        self.settle_delay_s = max(0.0, min(float(settle_delay_s), 5.0))

    def advise(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        args = self._normalize_payload(payload)
        requested_action = str(args.get("action", "observe") or "observe")
        app_profile = self._resolve_app_profile(args=args)
        args, defaults_applied = self._apply_profile_defaults(args=args, app_profile=app_profile)
        capabilities = self._capabilities()
        windows = self._list_windows()
        active_window = self._active_window()
        candidates = self._rank_window_candidates(
            windows=windows,
            active_window=active_window,
            app_name=str(args.get("app_name", "") or ""),
            window_title=str(args.get("window_title", "") or ""),
            app_profile=app_profile,
        )
        primary_candidate = candidates[0] if candidates else {}
        refined_profile = self._resolve_app_profile(args=args, primary_candidate=primary_candidate, active_window=active_window)
        if refined_profile.get("status") == "success" and refined_profile.get("profile_id") != app_profile.get("profile_id"):
            app_profile = refined_profile
            args, extra_defaults = self._apply_profile_defaults(args=args, app_profile=app_profile)
            defaults_applied.update(extra_defaults)
            candidates = self._rank_window_candidates(
                windows=windows,
                active_window=active_window,
                app_name=str(args.get("app_name", "") or ""),
                window_title=str(args.get("window_title", "") or ""),
                app_profile=app_profile,
            )
            primary_candidate = candidates[0] if candidates else {}

        blockers: List[str] = []
        warnings: List[str] = []
        plan: List[Dict[str, Any]] = []
        warnings.extend([str(item).strip() for item in app_profile.get("warnings", []) if str(item).strip()])

        if requested_action == "launch" and not str(args.get("app_name", "") or "").strip():
            blockers.append("app_name is required to launch an application.")
        if requested_action in {"click", "click_and_type"} and not str(args.get("query", "") or "").strip():
            blockers.append("query is required for click-oriented desktop interaction.")
        if requested_action in {"type", "click_and_type"} and not str(args.get("text", "") or "").strip():
            blockers.append("text is required for typing interactions.")
        if requested_action == "hotkey" and not list(args.get("keys", [])):
            blockers.append("keys are required for hotkey interactions.")
        if requested_action in {"click", "click_and_type"} and not (
            bool(capabilities["accessibility"].get("available")) or bool(capabilities["vision"].get("available"))
        ):
            blockers.append("Neither accessibility automation nor OCR vision targeting is available.")
        if requested_action == "observe" and not bool(capabilities["vision"].get("available")):
            blockers.append("Vision capture dependencies are unavailable for screen observation.")

        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        ensure_app_launch = bool(args.get("ensure_app_launch", False))
        focus_first = bool(args.get("focus_first", True))
        active_matches = self._window_matches(active_window, app_name=app_name, window_title=window_title)

        if app_name and not candidates and ensure_app_launch and requested_action in {"launch", "focus", "click", "type", "click_and_type", "hotkey"}:
            plan.append(
                self._plan_step(
                    action="open_app",
                    args={"app_name": app_name},
                    phase="bootstrap",
                    optional=False,
                    reason="No matching window is currently open, so the app should be launched first.",
                )
            )
            warnings.append(f"No running window matched '{app_name}'. The router will launch it first.")
        elif app_name and not candidates and requested_action in {"focus", "click", "type", "click_and_type", "hotkey"}:
            warnings.append(f"No running window matched '{app_name}'. Enable ensure_app_launch to open it automatically.")

        focus_title = str(primary_candidate.get("title", "") or window_title or app_name).strip()
        focus_hwnd = self._to_int(primary_candidate.get("hwnd"))
        if requested_action in {"focus", "click", "type", "click_and_type", "hotkey"} and focus_first and focus_title and not active_matches:
            focus_args: Dict[str, Any] = {"title": focus_title}
            if focus_hwnd is not None and focus_hwnd > 0:
                focus_args["hwnd"] = focus_hwnd
            plan.append(
                self._plan_step(
                    action="focus_window",
                    args=focus_args,
                    phase="focus",
                    optional=False,
                    reason="Bring the target app/window to the foreground before sending desktop input.",
                )
            )

        if requested_action in {"click", "click_and_type"}:
            click_args = {
                "query": str(args.get("query", "") or ""),
                "target_mode": str(args.get("target_mode", "auto") or "auto"),
                "verify_mode": str(args.get("verify_mode", "state_or_visibility") or "state_or_visibility"),
            }
            if str(args.get("verify_text", "") or "").strip():
                click_args["verify_text"] = str(args.get("verify_text", "") or "").strip()
            if focus_title:
                click_args["window_title"] = focus_title
            if str(args.get("control_type", "") or "").strip():
                click_args["control_type"] = str(args.get("control_type"))
            if str(args.get("element_id", "") or "").strip():
                click_args["element_id"] = str(args.get("element_id"))
            plan.append(
                self._plan_step(
                    action="computer_click_target",
                    args=click_args,
                    phase="target",
                    optional=False,
                    reason="Use accessibility-first targeting with OCR fallback for resilient cross-app clicking.",
                )
            )

        if requested_action in {"type", "click_and_type"}:
            plan.append(
                self._plan_step(
                    action="keyboard_type",
                    args={
                        "text": str(args.get("text", "") or ""),
                        "press_enter": bool(args.get("press_enter", False) or args.get("submit", False)),
                    },
                    phase="input",
                    optional=False,
                    reason="Send the requested text to the focused desktop target.",
                )
            )

        if requested_action == "hotkey":
            plan.append(
                self._plan_step(
                    action="keyboard_hotkey",
                    args={"keys": list(args.get("keys", []))},
                    phase="input",
                    optional=False,
                    reason="Dispatch the requested key chord against the focused window.",
                )
            )

        if requested_action == "observe":
            plan.append(
                self._plan_step(
                    action="computer_observe",
                    args={"include_targets": bool(args.get("include_targets", False))},
                    phase="observe",
                    optional=False,
                    reason="Capture the current screen and OCR state for grounded desktop reasoning.",
                )
            )

        route_mode = self._route_mode(requested_action=requested_action, args=args, capabilities=capabilities, app_profile=app_profile)
        confidence = self._confidence(
            requested_action=requested_action,
            primary_candidate=primary_candidate,
            capabilities=capabilities,
            blockers=blockers,
            warnings=warnings,
            app_profile=app_profile,
        )
        risk_level = str(app_profile.get("risk_posture", "") or "").strip().lower() or (
            "low" if requested_action in {"observe", "focus"} else ("medium" if requested_action in {"launch", "hotkey"} else "medium")
        )

        status = "blocked" if blockers else "success"
        strategy_variants = self._build_strategy_variants(args=args, capabilities=capabilities, app_profile=app_profile)
        return {
            "status": status,
            "action": requested_action,
            "route_mode": route_mode,
            "confidence": confidence,
            "risk_level": risk_level,
            "app_profile": app_profile if app_profile.get("status") == "success" else {},
            "profile_defaults_applied": defaults_applied,
            "target_window": primary_candidate,
            "active_window": active_window,
            "candidate_windows": candidates[:6],
            "capabilities": capabilities,
            "execution_plan": plan,
            "blockers": self._dedupe_strings(blockers),
            "warnings": self._dedupe_strings(warnings),
            "autonomy": {
                "ensure_app_launch": ensure_app_launch,
                "focus_first": focus_first,
                "supports_cross_app_fallback": bool(capabilities["vision"].get("available")) and bool(capabilities["accessibility"].get("available")),
                "requires_visual": requested_action in {"click", "click_and_type", "observe"},
            },
            "verification_plan": self._verification_plan(
                args=args,
                primary_candidate=primary_candidate,
                capabilities=capabilities,
                app_profile=app_profile,
            ),
            "strategy_variants": strategy_variants,
        }

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        args = self._normalize_payload(payload)
        advice = self.advise(args)
        if advice.get("status") != "success":
            return {
                "status": "blocked" if advice.get("blockers") else "error",
                "message": "; ".join(str(item) for item in advice.get("blockers", []) if str(item).strip()) or "desktop interaction unavailable",
                "advice": advice,
                "results": [],
            }
        attempts: List[Dict[str, Any]] = []
        strategy_variants = advice.get("strategy_variants", []) if isinstance(advice.get("strategy_variants"), list) else []
        max_attempts = max(1, min(int(args.get("max_strategy_attempts", len(strategy_variants) or 1) or 1), 4))
        variants = [row for row in strategy_variants if isinstance(row, dict)][:max_attempts] or [
            {"strategy_id": "primary", "title": "Primary Route", "reason": "Use the advised routed plan.", "payload_overrides": {}}
        ]
        retry_on_verification_failure = bool(args.get("retry_on_verification_failure", True))
        final_attempt: Dict[str, Any] = {}

        for attempt_index, variant in enumerate(variants, start=1):
            strategy_overrides = variant.get("payload_overrides", {}) if isinstance(variant.get("payload_overrides", {}), dict) else {}
            attempt_args = dict(args)
            attempt_args.update(strategy_overrides)
            if strategy_overrides:
                attempt_args["_provided_fields"] = self._dedupe_strings(
                    list(attempt_args.get("_provided_fields", [])) + list(strategy_overrides.keys())
                )
            attempt_advice = advice if attempt_index == 1 and not strategy_overrides else self.advise(attempt_args)
            if attempt_advice.get("status") != "success":
                attempt_payload = {
                    "attempt": attempt_index,
                    "strategy_id": str(variant.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                    "strategy_title": str(variant.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                    "status": "blocked" if attempt_advice.get("blockers") else "error",
                    "message": "; ".join(
                        str(item) for item in attempt_advice.get("blockers", []) if str(item).strip()
                    ) or str(attempt_advice.get("message", "desktop interaction unavailable") or "desktop interaction unavailable"),
                    "payload": attempt_args,
                    "advice": attempt_advice,
                    "results": [],
                    "verification": {
                        "enabled": bool(attempt_args.get("verify_after_action", True)),
                        "status": "skipped",
                        "verified": False,
                        "message": "route planning failed before execution",
                        "checks": [],
                    },
                }
                attempts.append(attempt_payload)
                final_attempt = attempt_payload
                continue

            attempt_payload = self._execute_strategy(
                args=attempt_args,
                advice=attempt_advice,
                strategy=variant,
                attempt_index=attempt_index,
            )
            attempts.append(attempt_payload)
            final_attempt = attempt_payload
            verification = attempt_payload.get("verification", {}) if isinstance(attempt_payload.get("verification", {}), dict) else {}
            verified = bool(verification.get("verified", False)) or not bool(verification.get("enabled", False))
            if attempt_payload.get("status") == "success" and verified:
                return self._build_execution_response(
                    base_advice=advice,
                    selected_attempt=attempt_payload,
                    attempts=attempts,
                    recovered=attempt_index > 1,
                )
            if attempt_payload.get("status") == "error":
                continue
            if not retry_on_verification_failure:
                break
            if self.settle_delay_s > 0:
                time.sleep(min(self.settle_delay_s, 0.5))

        selected_attempt = final_attempt if isinstance(final_attempt, dict) else {}
        verification = selected_attempt.get("verification", {}) if isinstance(selected_attempt.get("verification", {}), dict) else {}
        unverified = bool(verification.get("enabled", False)) and not bool(verification.get("verified", False))
        status = "partial" if attempts and any(str(item.get("status", "") or "").strip().lower() == "success" for item in attempts) else "error"
        if unverified and status == "error":
            status = "partial"
        message = str(selected_attempt.get("message", "") or "").strip()
        if not message:
            if unverified:
                message = str(verification.get("message", "desktop action could not be verified after execution") or "desktop action could not be verified after execution")
            else:
                message = "desktop interaction did not complete successfully"
        return self._build_execution_response(
            base_advice=advice,
            selected_attempt=selected_attempt,
            attempts=attempts,
            recovered=False,
            status_override=status,
            message_override=message,
        )

    def _capabilities(self) -> Dict[str, Any]:
        accessibility_status = self._call("accessibility_status", {})
        vision_status = self._call("vision_status", {})
        return {
            "accessibility": {
                "available": str(accessibility_status.get("status", "")).strip().lower() == "success"
                or bool(accessibility_status.get("capabilities", {}).get("invoke_element")),
                "provider": str(accessibility_status.get("provider", "") or ""),
                "capabilities": accessibility_status.get("capabilities", {}) if isinstance(accessibility_status.get("capabilities", {}), dict) else {},
            },
            "vision": {
                "available": str(vision_status.get("status", "")).strip().lower() == "success"
                or bool(vision_status.get("capabilities", {}).get("ocr_targets")),
                "capabilities": vision_status.get("capabilities", {}) if isinstance(vision_status.get("capabilities", {}), dict) else {},
            },
        }

    def _list_windows(self) -> List[Dict[str, Any]]:
        payload = self._call("list_windows", {"limit": 80})
        rows = payload.get("windows", []) if isinstance(payload, dict) else []
        return [row for row in rows if isinstance(row, dict)]

    def _active_window(self) -> Dict[str, Any]:
        payload = self._call("active_window", {})
        if isinstance(payload, dict) and isinstance(payload.get("window"), dict):
            return payload.get("window", {})
        return payload if isinstance(payload, dict) else {}

    def _call(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        handler = self._handlers.get(action)
        if handler is None:
            return {"status": "error", "message": f"missing handler for {action}"}
        try:
            result = handler(dict(payload))
            return result if isinstance(result, dict) else {"status": "error", "message": f"invalid result from {action}"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @staticmethod
    def _plan_step(*, action: str, args: Dict[str, Any], phase: str, optional: bool, reason: str) -> Dict[str, Any]:
        return {
            "action": action,
            "args": args,
            "phase": phase,
            "optional": optional,
            "reason": reason,
        }

    def _rank_window_candidates(
        self,
        *,
        windows: List[Dict[str, Any]],
        active_window: Dict[str, Any],
        app_name: str,
        window_title: str,
        app_profile: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        active_hwnd = self._to_int(active_window.get("hwnd"))
        profile_aliases = [str(alias).strip() for alias in app_profile.get("aliases", []) if str(alias).strip()]
        profile_exe_hints = [str(alias).strip().lower() for alias in app_profile.get("exe_hints", []) if str(alias).strip()]
        rows: List[tuple[float, Dict[str, Any]]] = []
        for window in windows:
            title = str(window.get("title", "") or "").strip()
            exe_path = str(window.get("exe", "") or "").strip()
            exe_name = exe_path.rsplit("\\", 1)[-1].rsplit("/", 1)[-1].strip().lower()
            score = 0.0
            reasons: List[str] = []
            if window_title:
                match_score = self._text_match_score(title, window_title)
                if match_score > 0:
                    score += 0.68 * match_score
                    reasons.append("window_title")
            if app_name:
                title_score = self._text_match_score(title, app_name)
                exe_score = self._text_match_score(exe_name, app_name)
                if title_score > 0:
                    score += 0.46 * title_score
                    reasons.append("app_title")
                if exe_score > 0:
                    score += 0.55 * exe_score
                    reasons.append("exe_name")
            for alias in profile_aliases:
                alias_score = self._text_match_score(title, alias)
                if alias_score > 0:
                    score += 0.38 * alias_score
                    reasons.append("profile_alias")
            for exe_hint in profile_exe_hints:
                exe_hint_score = self._text_match_score(exe_name, exe_hint)
                if exe_hint_score > 0:
                    score += 0.58 * exe_hint_score
                    reasons.append("profile_exe")
            if active_hwnd is not None and active_hwnd == self._to_int(window.get("hwnd")):
                score += 0.14
                reasons.append("active")
            if score <= 0 and not (app_name or window_title):
                if title:
                    score = 0.1
                    reasons.append("visible_window")
            if score <= 0:
                continue
            enriched = dict(window)
            enriched["score"] = round(min(score, 1.0), 6)
            enriched["exe_name"] = exe_name
            enriched["match_reasons"] = self._dedupe_strings(reasons)
            rows.append((score, enriched))
        rows.sort(key=lambda item: item[0], reverse=True)
        return [row for _, row in rows]

    def _route_mode(self, *, requested_action: str, args: Dict[str, Any], capabilities: Dict[str, Any], app_profile: Dict[str, Any]) -> str:
        accessibility_ready = bool(capabilities["accessibility"].get("available"))
        vision_ready = bool(capabilities["vision"].get("available"))
        target_mode = str(args.get("target_mode", "auto") or "auto").strip().lower() or "auto"
        capability_preferences = [
            str(item).strip().lower()
            for item in app_profile.get("capability_preferences", [])
            if str(item).strip()
        ]
        if requested_action in {"click", "click_and_type"}:
            can_retry = bool(args.get("retry_on_verification_failure", True))
            if target_mode == "accessibility":
                return "accessibility_then_ocr" if vision_ready and can_retry else "accessibility_only"
            if target_mode == "ocr":
                return "ocr_then_accessibility" if accessibility_ready and can_retry else "ocr_only"
            if accessibility_ready and vision_ready:
                if capability_preferences[:1] == ["vision"]:
                    return "ocr_then_accessibility"
                return "accessibility_then_ocr"
            if accessibility_ready:
                return "accessibility_only"
            if vision_ready:
                return "ocr_only"
        if requested_action in {"type", "hotkey"}:
            return "focused_input"
        if requested_action == "launch":
            return "launch_and_focus"
        if requested_action == "observe":
            return "vision_observe"
        return "generic_desktop"

    def _confidence(
        self,
        *,
        requested_action: str,
        primary_candidate: Dict[str, Any],
        capabilities: Dict[str, Any],
        blockers: List[str],
        warnings: List[str],
        app_profile: Dict[str, Any],
    ) -> float:
        if blockers:
            return 0.0
        score = 0.42
        candidate_score = float(primary_candidate.get("score", 0.0) or 0.0)
        score += min(0.35, candidate_score * 0.35)
        match_score = float(app_profile.get("match_score", 0.0) or 0.0)
        score += min(0.12, match_score * 0.12)
        if requested_action in {"click", "click_and_type"}:
            if bool(capabilities["accessibility"].get("available")):
                score += 0.12
            if bool(capabilities["vision"].get("available")):
                score += 0.09
        elif requested_action in {"type", "hotkey"}:
            score += 0.1
        elif requested_action == "observe":
            score += 0.18 if bool(capabilities["vision"].get("available")) else 0.0
        if warnings:
            score -= min(0.18, 0.06 * len(warnings))
        return round(max(0.0, min(score, 0.99)), 4)

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload if isinstance(payload, dict) else {}
        if isinstance(raw.get("_provided_fields"), list):
            provided_fields = [str(item).strip() for item in raw.get("_provided_fields", []) if str(item).strip()]
        else:
            provided_fields = [
                field_name
                for field_name, aliases in {
                    "app_name": ("app_name", "app"),
                    "window_title": ("window_title", "title"),
                    "query": ("query", "target"),
                    "text": ("text",),
                    "keys": ("keys", "key"),
                    "press_enter": ("press_enter", "submit"),
                    "ensure_app_launch": ("ensure_app_launch", "launch_if_missing"),
                    "focus_first": ("focus_first",),
                    "target_mode": ("target_mode",),
                    "verify_mode": ("verify_mode",),
                    "verify_after_action": ("verify_after_action",),
                    "verify_text": ("verify_text",),
                    "retry_on_verification_failure": ("retry_on_verification_failure",),
                    "max_strategy_attempts": ("max_strategy_attempts",),
                    "control_type": ("control_type",),
                    "element_id": ("element_id",),
                    "include_targets": ("include_targets",),
                }.items()
                if any(alias in raw and raw.get(alias) is not None for alias in aliases)
            ]
        normalized_action = str(raw.get("action", "") or "").strip().lower()
        text = str(raw.get("text", "") or "").strip()
        query = str(raw.get("query", raw.get("target", "")) or "").strip()
        hotkey_keys = raw.get("keys")
        if isinstance(hotkey_keys, str):
            keys = [part.strip().lower() for part in re.split(r"[+,]", hotkey_keys) if part.strip()]
        elif isinstance(hotkey_keys, list):
            keys = [str(part).strip().lower() for part in hotkey_keys if str(part).strip()]
        else:
            key = str(raw.get("key", "") or "").strip().lower()
            keys = [key] if key else []

        if normalized_action not in {"launch", "focus", "click", "type", "click_and_type", "hotkey", "observe"}:
            if keys:
                normalized_action = "hotkey"
            elif text and query:
                normalized_action = "click_and_type"
            elif text:
                normalized_action = "type"
            elif query:
                normalized_action = "click"
            elif str(raw.get("app_name", "") or raw.get("app", "")).strip():
                normalized_action = "launch"
            else:
                normalized_action = "observe"

        return {
            "action": normalized_action,
            "app_name": str(raw.get("app_name", raw.get("app", "")) or "").strip(),
            "window_title": str(raw.get("window_title", raw.get("title", "")) or "").strip(),
            "query": query,
            "text": text,
            "keys": keys,
            "press_enter": bool(raw.get("press_enter", False)),
            "submit": bool(raw.get("submit", False)),
            "ensure_app_launch": bool(raw.get("ensure_app_launch", False) or raw.get("launch_if_missing", False)),
            "focus_first": bool(raw.get("focus_first", True)),
            "target_mode": str(raw.get("target_mode", "auto") or "auto").strip().lower() or "auto",
            "verify_mode": str(raw.get("verify_mode", "state_or_visibility") or "state_or_visibility").strip().lower() or "state_or_visibility",
            "verify_after_action": bool(raw.get("verify_after_action", True)),
            "verify_text": str(raw.get("verify_text", "") or "").strip(),
            "retry_on_verification_failure": bool(raw.get("retry_on_verification_failure", True)),
            "max_strategy_attempts": max(1, min(int(raw.get("max_strategy_attempts", 2) or 2), 4)),
            "control_type": str(raw.get("control_type", "") or "").strip(),
            "element_id": str(raw.get("element_id", "") or "").strip(),
            "include_targets": bool(raw.get("include_targets", False)),
            "_provided_fields": provided_fields,
        }

    def _execute_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []
        pre_context = self._capture_verification_context(args=args, advice=advice)
        message = ""
        status = "success"
        for step in advice.get("execution_plan", []):
            if not isinstance(step, dict):
                continue
            action = str(step.get("action", "") or "").strip()
            action_args = step.get("args", {}) if isinstance(step.get("args", {}), dict) else {}
            handler = self._handlers.get(action)
            if handler is None:
                message = f"missing handler for {action}"
                status = "error"
                break
            result = handler(dict(action_args))
            results.append(
                {
                    "action": action,
                    "phase": str(step.get("phase", "") or ""),
                    "result": result,
                }
            )
            if str(action).strip().lower() == "open_app" and result.get("status") == "success" and self.settle_delay_s > 0:
                time.sleep(self.settle_delay_s)
            if result.get("status") != "success" and not bool(step.get("optional", False)):
                message = str(result.get("message", f"{action} failed") or f"{action} failed")
                status = "error"
                break
        final_action = results[-1]["action"] if results else advice.get("action", "")
        post_context = self._capture_verification_context(args=args, advice=advice) if status == "success" else {}
        verification = self._verify_execution(
            args=args,
            advice=advice,
            results=results,
            pre_context=pre_context,
            post_context=post_context,
            step_status=status,
        )
        if status == "success" and bool(verification.get("enabled", False)) and not bool(verification.get("verified", False)):
            message = str(verification.get("message", "desktop action could not be verified after execution") or "desktop action could not be verified after execution")
        elif status == "success" and not message:
            message = str(verification.get("message", "desktop action executed") or "desktop action executed")
        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": status,
            "message": message,
            "final_action": final_action,
            "results": results,
            "advice": advice,
            "verification": verification,
        }

    def _capture_verification_context(self, *, args: Dict[str, Any], advice: Dict[str, Any]) -> Dict[str, Any]:
        context: Dict[str, Any] = {"timestamp": time.time()}
        action = str(args.get("action", "observe") or "observe").strip().lower()
        verify_enabled = bool(args.get("verify_after_action", True))
        if not verify_enabled:
            return context
        if action in {"launch", "focus", "type", "click", "click_and_type", "hotkey"}:
            context["active_window"] = self._active_window()
        capabilities = advice.get("capabilities", {}) if isinstance(advice.get("capabilities", {}), dict) else {}
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        if vision_ready and action in {"observe", "click", "click_and_type", "type", "hotkey"}:
            context["observation"] = self._call("computer_observe", {"include_targets": False})
        return context

    def _verify_execution(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        results: List[Dict[str, Any]],
        pre_context: Dict[str, Any],
        post_context: Dict[str, Any],
        step_status: str,
    ) -> Dict[str, Any]:
        enabled = bool(args.get("verify_after_action", True))
        if not enabled:
            return {
                "enabled": False,
                "status": "skipped",
                "verified": True,
                "message": "post-action verification disabled",
                "checks": [],
            }
        if str(step_status).strip().lower() != "success":
            return {
                "enabled": True,
                "status": "skipped",
                "verified": False,
                "message": "execution failed before verification could run",
                "checks": [],
            }
        action = str(args.get("action", "observe") or "observe").strip().lower()
        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        verify_text = str(args.get("verify_text", "") or "").strip()
        if not verify_text:
            verify_text = str(args.get("text", "") or "").strip() if action in {"type", "click_and_type"} else str(args.get("query", "") or "").strip()

        pre_active = pre_context.get("active_window", {}) if isinstance(pre_context.get("active_window", {}), dict) else {}
        post_active = post_context.get("active_window", {}) if isinstance(post_context.get("active_window", {}), dict) else {}
        pre_observation = pre_context.get("observation", {}) if isinstance(pre_context.get("observation", {}), dict) else {}
        post_observation = post_context.get("observation", {}) if isinstance(post_context.get("observation", {}), dict) else {}
        checks: List[Dict[str, Any]] = []
        warnings: List[str] = []
        focus_step = self._find_step_result(results, "focus_window")
        if not post_active and isinstance(focus_step.get("window", {}), dict):
            post_active = focus_step.get("window", {})

        active_match = self._window_matches(post_active, app_name=app_name, window_title=window_title)
        target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        if not active_match and target_window:
            active_match = self._to_int(post_active.get("hwnd")) == self._to_int(target_window.get("hwnd"))
        window_present = False
        if app_name or window_title:
            windows = self._list_windows()
            window_present = any(self._window_matches(row, app_name=app_name, window_title=window_title) for row in windows)
        pre_hash = str(pre_observation.get("screen_hash", "") or "").strip()
        post_hash = str(post_observation.get("screen_hash", "") or "").strip()
        screen_changed = bool(pre_hash and post_hash and pre_hash != post_hash)
        pre_text = str(pre_observation.get("text", "") or "")
        post_text = str(post_observation.get("text", "") or "")
        text_visible = self._contains_text(post_text, verify_text)
        pre_text_visible = self._contains_text(pre_text, verify_text)
        active_changed = self._to_int(pre_active.get("hwnd")) != self._to_int(post_active.get("hwnd")) if pre_active and post_active else False
        final_step = results[-1].get("result", {}) if results and isinstance(results[-1].get("result", {}), dict) else {}
        click_step = self._find_step_result(results, "computer_click_target")
        type_step = self._find_step_result(results, "keyboard_type")
        click_changed = bool(click_step.get("screen_changed", False)) if isinstance(click_step, dict) else False
        screenshot_path = str(post_observation.get("screenshot_path", "") or "").strip()
        vision_signal_available = bool(pre_hash or post_hash or post_text or screenshot_path)

        if app_name or window_title:
            checks.append(
                {
                    "name": "window_match",
                    "passed": bool(active_match or (action == "launch" and window_present)),
                    "expected": window_title or app_name,
                    "observed": str(post_active.get("title", "") or post_active.get("process_name", "") or ""),
                }
            )
        if action in {"click", "click_and_type", "type", "hotkey"}:
            checks.append(
                {
                    "name": "screen_changed",
                    "passed": bool(screen_changed or click_changed),
                    "pre_hash": pre_hash,
                    "post_hash": post_hash,
                }
            )
        if verify_text and action in {"click", "click_and_type", "type"}:
            checks.append(
                {
                    "name": "text_visible",
                    "passed": bool(text_visible),
                    "expected": verify_text[:120],
                    "was_visible_before": bool(pre_text_visible),
                }
            )
        if action == "observe":
            checks.append(
                {
                    "name": "observation_ready",
                    "passed": bool(screenshot_path or str(final_step.get("screenshot_path", "") or "").strip()),
                    "screenshot_path": screenshot_path or str(final_step.get("screenshot_path", "") or "").strip(),
                }
            )

        verified = False
        message = "desktop action executed"
        if action == "launch":
            verified = bool(active_match or window_present)
            message = "launch verified" if verified else "launch finished, but no matching window became available"
        elif action == "focus":
            verified = bool(active_match)
            message = "focus verified" if verified else "focus step completed, but the expected window is not active"
        elif action == "click":
            verified = bool(
                str(click_step.get("status", "") or "").strip().lower() == "success"
                and (bool(screen_changed or click_changed or text_visible) or str(args.get("verify_mode", "") or "").strip().lower() == "none")
            )
            if not verified and not vision_signal_available and str(click_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append("Visual verification was unavailable, so JARVIS accepted the successful click handler result as best-effort confirmation.")
            message = "click verified" if verified else "click finished, but no reliable post-click signal was observed"
        elif action == "type":
            verified = bool((screen_changed or text_visible) and (active_match or not (app_name or window_title)))
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append("Visual verification was unavailable, so JARVIS accepted the successful typing step as best-effort confirmation.")
            message = "typing verified" if verified else "typing finished, but JARVIS could not confirm the text landed in the intended window"
        elif action == "click_and_type":
            verified = bool(
                str(click_step.get("status", "") or "").strip().lower() == "success"
                and (screen_changed or text_visible)
                and (active_match or not (app_name or window_title))
            )
            if (
                not verified
                and not vision_signal_available
                and str(click_step.get("status", "") or "").strip().lower() == "success"
                and str(type_step.get("status", "") or "").strip().lower() == "success"
            ):
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append("Visual verification was unavailable, so JARVIS accepted the successful click-and-type chain as best-effort confirmation.")
            message = "click-and-type verified" if verified else "click-and-type finished, but the follow-up state change could not be confirmed"
        elif action == "hotkey":
            verified = bool(screen_changed or active_changed or (active_match if (app_name or window_title) else False))
            if not verified and not vision_signal_available and active_match:
                verified = True
                warnings.append("No visual change was available after the hotkey, so JARVIS relied on the focused target window match.")
            message = "hotkey verified" if verified else "hotkey finished, but no UI change was detected afterward"
        elif action == "observe":
            verified = bool(screenshot_path or str(final_step.get("screenshot_path", "") or "").strip())
            message = "observation verified" if verified else "observe returned, but no screenshot evidence was captured"
        else:
            verified = True

        if not verified and action in {"type", "click_and_type"} and verify_text and not text_visible:
            warnings.append(f"Expected text '{verify_text[:80]}' was not visible in the post-action OCR snapshot.")
        if not verified and action in {"click", "click_and_type", "hotkey"} and not (screen_changed or click_changed):
            warnings.append("No post-action screen hash change was detected.")
        status = "degraded" if verified and warnings else ("success" if verified else "failed")
        return {
            "enabled": True,
            "status": status,
            "verified": verified,
            "message": message,
            "checks": checks,
            "warnings": self._dedupe_strings(warnings),
            "verify_text": verify_text,
            "pre_context": {
                "active_window": pre_active,
                "screen_hash": pre_hash,
            },
            "post_context": {
                "active_window": post_active,
                "screen_hash": post_hash,
                "screenshot_path": screenshot_path or str(final_step.get("screenshot_path", "") or "").strip(),
            },
        }

    def _verification_plan(
        self,
        *,
        args: Dict[str, Any],
        primary_candidate: Dict[str, Any],
        capabilities: Dict[str, Any],
        app_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        action = str(args.get("action", "observe") or "observe").strip().lower()
        verify_text = self._derive_verify_text(args=args, app_profile=app_profile)
        checks: List[str] = []
        if action in {"launch", "focus", "type", "click", "click_and_type", "hotkey"} and (args.get("app_name") or args.get("window_title") or primary_candidate):
            checks.append("active_window_match")
        if action in {"click", "click_and_type", "type", "hotkey"} and bool(capabilities.get("vision", {}).get("available")):
            checks.append("screen_hash_change")
        if verify_text and action in {"click", "click_and_type", "type"} and bool(capabilities.get("vision", {}).get("available")):
            checks.append("ocr_text_visibility")
        if action == "launch":
            checks.append("window_presence")
        if action == "observe":
            checks.append("screenshot_capture")
        return {
            "enabled": bool(args.get("verify_after_action", True)),
            "expected_window": str(args.get("window_title", "") or args.get("app_name", "") or ""),
            "verify_text": verify_text,
            "profile_id": str(app_profile.get("profile_id", "") or "").strip(),
            "profile_verify_text_source": str(
                (app_profile.get("verification_defaults", {}) if isinstance(app_profile.get("verification_defaults", {}), dict) else {}).get("verify_text_source", "")
                or ""
            ).strip(),
            "retry_on_verification_failure": bool(args.get("retry_on_verification_failure", True)),
            "max_strategy_attempts": max(1, min(int(args.get("max_strategy_attempts", 2) or 2), 4)),
            "checks": checks,
        }

    def _build_strategy_variants(self, *, args: Dict[str, Any], capabilities: Dict[str, Any], app_profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        action = str(args.get("action", "observe") or "observe").strip().lower()
        accessibility_ready = bool(capabilities.get("accessibility", {}).get("available")) if isinstance(capabilities.get("accessibility", {}), dict) else False
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        target_mode = str(args.get("target_mode", "auto") or "auto").strip().lower() or "auto"
        focus_retry_needed = bool((args.get("app_name") or args.get("window_title")) and not bool(args.get("focus_first", True)))
        capability_preferences = [
            str(item).strip().lower()
            for item in app_profile.get("capability_preferences", [])
            if str(item).strip()
        ]
        variants: List[Dict[str, Any]] = [
            {
                "strategy_id": "primary",
                "title": "Primary Route",
                "reason": "Run the advised desktop route first.",
                "payload_overrides": {},
            }
        ]
        if action in {"focus", "type", "hotkey"} and focus_retry_needed:
            variants.append(
                {
                    "strategy_id": "refocus_retry",
                    "title": "Refocus Retry",
                    "reason": "Retry after explicitly restoring focus to the target window.",
                    "payload_overrides": {"focus_first": True},
                }
            )
        if action in {"click", "click_and_type"}:
            if focus_retry_needed:
                variants.append(
                    {
                        "strategy_id": "refocus_primary_retry",
                        "title": "Refocus Primary Retry",
                        "reason": "Retry the routed click after re-focusing the target app.",
                        "payload_overrides": {"focus_first": True},
                    }
                )
            prefer_vision = capability_preferences[:1] == ["vision"]
            if prefer_vision and vision_ready and target_mode != "ocr":
                variants.append(
                    {
                        "strategy_id": "ocr_retry",
                        "title": "OCR Retry",
                        "reason": "Retry with OCR-only targeting in case accessibility coordinates drifted.",
                        "payload_overrides": {"target_mode": "ocr", "focus_first": True},
                    }
                )
            if accessibility_ready and target_mode != "accessibility":
                variants.append(
                    {
                        "strategy_id": "accessibility_retry",
                        "title": "Accessibility Retry",
                        "reason": "Retry with accessibility-only targeting for structured UI controls.",
                        "payload_overrides": {"target_mode": "accessibility", "focus_first": True},
                    }
                )
            if not prefer_vision and vision_ready and target_mode != "ocr":
                variants.append(
                    {
                        "strategy_id": "ocr_retry",
                        "title": "OCR Retry",
                        "reason": "Retry with OCR-only targeting in case accessibility coordinates drifted.",
                        "payload_overrides": {"target_mode": "ocr", "focus_first": True},
                    }
                )
        deduped: List[Dict[str, Any]] = []
        seen: set[tuple] = set()
        for variant in variants:
            overrides = variant.get("payload_overrides", {}) if isinstance(variant.get("payload_overrides", {}), dict) else {}
            key = (
                str(variant.get("strategy_id", "") or "").strip().lower(),
                tuple(sorted((str(k), str(v)) for k, v in overrides.items())),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(variant)
        return deduped[: max(1, min(int(args.get("max_strategy_attempts", 2) or 2), 4))]

    def _build_execution_response(
        self,
        *,
        base_advice: Dict[str, Any],
        selected_attempt: Dict[str, Any],
        attempts: List[Dict[str, Any]],
        recovered: bool,
        status_override: str = "",
        message_override: str = "",
    ) -> Dict[str, Any]:
        advice = selected_attempt.get("advice", {}) if isinstance(selected_attempt.get("advice", {}), dict) else base_advice
        verification = selected_attempt.get("verification", {}) if isinstance(selected_attempt.get("verification", {}), dict) else {}
        return {
            "status": str(status_override or selected_attempt.get("status", "success") or "success"),
            "action": advice.get("action", base_advice.get("action", "")),
            "final_action": selected_attempt.get("final_action", advice.get("action", "")),
            "route_mode": advice.get("route_mode", base_advice.get("route_mode", "")),
            "confidence": advice.get("confidence", base_advice.get("confidence", 0.0)),
            "app_profile": advice.get("app_profile", base_advice.get("app_profile", {})),
            "profile_defaults_applied": advice.get("profile_defaults_applied", base_advice.get("profile_defaults_applied", {})),
            "target_window": advice.get("target_window", base_advice.get("target_window", {})),
            "results": selected_attempt.get("results", []),
            "advice": advice,
            "verification": verification,
            "attempts": attempts,
            "attempt_count": len(attempts),
            "executed_strategy": {
                "strategy_id": selected_attempt.get("strategy_id", ""),
                "title": selected_attempt.get("strategy_title", ""),
                "reason": selected_attempt.get("strategy_reason", ""),
                "recovered": bool(recovered),
            },
            "message": str(message_override or selected_attempt.get("message", "") or ""),
        }

    def app_profile_catalog(self, *, query: str = "", category: str = "", limit: int = 400) -> Dict[str, Any]:
        return self._app_profile_registry.catalog(query=query, category=category, limit=limit)

    def _resolve_app_profile(
        self,
        *,
        args: Dict[str, Any],
        primary_candidate: Optional[Dict[str, Any]] = None,
        active_window: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        profile = self._app_profile_registry.match(
            app_name=str(args.get("app_name", "") or "").strip(),
            window_title=str(args.get("window_title", "") or "").strip(),
        )
        if profile.get("status") == "success":
            return profile
        candidate = primary_candidate if isinstance(primary_candidate, dict) else {}
        active = active_window if isinstance(active_window, dict) else {}
        candidate_exe = str(candidate.get("exe_name", "") or str(candidate.get("exe", "") or "")).rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        active_exe = str(active.get("exe", "") or "").rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        return self._app_profile_registry.match(
            app_name=str(args.get("app_name", "") or str(candidate.get("process_name", "") or "")).strip(),
            window_title=str(candidate.get("title", "") or str(active.get("title", "") or "")).strip(),
            exe_name=str(candidate_exe or active_exe).strip(),
        )

    def _apply_profile_defaults(self, *, args: Dict[str, Any], app_profile: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        if app_profile.get("status") != "success":
            return args, {}
        next_args = dict(args)
        provided_fields = {str(item).strip() for item in next_args.get("_provided_fields", []) if str(item).strip()}
        defaults_applied: Dict[str, Any] = {}
        routing_defaults = app_profile.get("routing_defaults", {}) if isinstance(app_profile.get("routing_defaults", {}), dict) else {}
        autonomy_defaults = app_profile.get("autonomy_defaults", {}) if isinstance(app_profile.get("autonomy_defaults", {}), dict) else {}

        for field_name, source in {
            "target_mode": routing_defaults,
            "verify_mode": routing_defaults,
            "ensure_app_launch": autonomy_defaults,
            "focus_first": autonomy_defaults,
            "verify_after_action": autonomy_defaults,
            "retry_on_verification_failure": autonomy_defaults,
            "max_strategy_attempts": autonomy_defaults,
        }.items():
            if field_name in provided_fields or field_name not in source:
                continue
            next_args[field_name] = source[field_name]
            defaults_applied[field_name] = source[field_name]

        if "verify_text" not in provided_fields and not str(next_args.get("verify_text", "") or "").strip():
            derived_verify_text = self._derive_verify_text(args=next_args, app_profile=app_profile)
            if derived_verify_text:
                next_args["verify_text"] = derived_verify_text
                defaults_applied["verify_text"] = derived_verify_text
        return next_args, defaults_applied

    def _derive_verify_text(self, *, args: Dict[str, Any], app_profile: Dict[str, Any]) -> str:
        explicit = str(args.get("verify_text", "") or "").strip()
        if explicit:
            return explicit
        verification_defaults = app_profile.get("verification_defaults", {}) if isinstance(app_profile.get("verification_defaults", {}), dict) else {}
        verify_text_source = str(verification_defaults.get("verify_text_source", "query_or_typed") or "query_or_typed").strip().lower()
        typed_text = str(args.get("text", "") or "").strip()
        query_text = str(args.get("query", "") or "").strip()
        if verify_text_source == "typed_text":
            return typed_text
        if verify_text_source == "query":
            return query_text
        return typed_text or query_text

    @staticmethod
    def _sanitize_payload_for_response(args: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in dict(args).items() if not str(key).startswith("_")}

    @staticmethod
    def _contains_text(haystack: str, needle: str) -> bool:
        clean_haystack = " ".join(str(haystack or "").strip().lower().split())
        clean_needle = " ".join(str(needle or "").strip().lower().split())
        if not clean_haystack or not clean_needle:
            return False
        return clean_needle in clean_haystack

    @staticmethod
    def _find_step_result(results: List[Dict[str, Any]], action: str) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        for row in reversed(results):
            if not isinstance(row, dict):
                continue
            if str(row.get("action", "") or "").strip().lower() == clean_action and isinstance(row.get("result", {}), dict):
                return row.get("result", {})
        return {}

    @staticmethod
    def _window_matches(window: Dict[str, Any], *, app_name: str, window_title: str) -> bool:
        title = str(window.get("title", "") or "").strip()
        exe_name = str(window.get("exe", "") or "").rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        return (
            DesktopActionRouter._text_match_score(title, window_title) > 0
            if window_title
            else False
        ) or (
            DesktopActionRouter._text_match_score(title, app_name) > 0
            if app_name
            else False
        ) or (
            DesktopActionRouter._text_match_score(exe_name, app_name) > 0
            if app_name
            else False
        )

    @staticmethod
    def _text_match_score(left: str, right: str) -> float:
        clean_left = str(left or "").strip().lower()
        clean_right = str(right or "").strip().lower()
        if not clean_left or not clean_right:
            return 0.0
        if clean_left == clean_right:
            return 1.0
        if clean_right in clean_left or clean_left in clean_right:
            return 0.82
        left_tokens = {token for token in re.split(r"[^a-z0-9]+", clean_left) if token}
        right_tokens = {token for token in re.split(r"[^a-z0-9]+", clean_right) if token}
        if not left_tokens or not right_tokens:
            return 0.0
        overlap = len(left_tokens.intersection(right_tokens))
        return overlap / max(1.0, len(right_tokens))

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _dedupe_strings(values: List[str]) -> List[str]:
        rows: List[str] = []
        seen: set[str] = set()
        for value in values:
            clean = str(value or "").strip()
            if not clean:
                continue
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(clean)
        return rows

    @staticmethod
    def _default_handlers() -> Dict[str, ActionHandler]:
        def _open_app(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _open_app as open_app_impl

            return open_app_impl(payload)

        def _list_windows(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _list_windows as list_windows_impl

            return list_windows_impl(payload)

        def _active_window(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _active_window as active_window_impl

            return active_window_impl(payload)

        def _focus_window(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _focus_window as focus_window_impl

            return focus_window_impl(payload)

        def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _keyboard_type as keyboard_type_impl

            return keyboard_type_impl(payload)

        def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _keyboard_hotkey as keyboard_hotkey_impl

            return keyboard_hotkey_impl(payload)

        def _computer_click_target(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _computer_click_target as computer_click_target_impl

            return computer_click_target_impl(payload)

        def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _computer_observe as computer_observe_impl

            return computer_observe_impl(payload)

        def _accessibility_status(_payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.accessibility_tools import AccessibilityTools

            return AccessibilityTools.health()

        def _vision_status(_payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.vision_tools import VisionTools

            return VisionTools.health()

        return {
            "open_app": _open_app,
            "list_windows": _list_windows,
            "active_window": _active_window,
            "focus_window": _focus_window,
            "keyboard_type": _keyboard_type,
            "keyboard_hotkey": _keyboard_hotkey,
            "computer_click_target": _computer_click_target,
            "computer_observe": _computer_observe,
            "accessibility_status": _accessibility_status,
            "vision_status": _vision_status,
        }
