from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class DesktopAnchorMemory:
    """
    Persistent memory for successful desktop interaction anchors.

    Each anchor is keyed by action + query + app/window context and stores
    aggregated reliability signals to support future anchor suggestions.
    """

    _MANAGED_ACTIONS = frozenset(
        {
            "computer_click_target",
            "computer_click_text",
            "accessibility_invoke_element",
        }
    )

    def __init__(
        self,
        *,
        store_path: str = "data/desktop_anchor_memory.json",
        max_entries: int = 6000,
        quarantine_ttl_s: int = 1200,
        quarantine_max_entries: int = 3000,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_entries = self._coerce_int(max_entries, minimum=100, maximum=100_000, default=6000)
        self.quarantine_ttl_s = self._coerce_int(quarantine_ttl_s, minimum=60, maximum=172_800, default=1200)
        self.quarantine_max_entries = self._coerce_int(quarantine_max_entries, minimum=100, maximum=100_000, default=3000)
        self._lock = RLock()
        self._anchors: Dict[str, Dict[str, Any]] = {}
        self._quarantine: Dict[str, Dict[str, Any]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._load()

    def is_managed_action(self, action: str) -> bool:
        return str(action or "").strip() in self._MANAGED_ACTIONS

    def lookup(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
        limit: int = 3,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip()
        if clean_action not in self._MANAGED_ACTIONS:
            return {"status": "skip", "items": []}

        payload = args if isinstance(args, dict) else {}
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        query = self._extract_query(payload)
        app = self._extract_app(payload, {}, runtime_meta)
        window_title = self._extract_window_title(payload, {}, runtime_meta)
        pre_state_hash = self._extract_lookup_pre_state_hash(payload, runtime_meta)
        transition_signature = self._extract_lookup_transition_signature(payload, runtime_meta)
        bounded_limit = self._coerce_int(limit, minimum=1, maximum=30, default=3)

        with self._lock:
            rows = list(self._anchors.values())
            self._prune_quarantine_locked(now_ts=time.time())
            quarantine_rows = dict(self._quarantine)

        candidates: List[Dict[str, Any]] = []
        quarantined_matches: List[Dict[str, Any]] = []
        unstable_filtered = 0
        auto_quarantine_rows: List[Dict[str, Any]] = []
        for row in rows:
            if str(row.get("action", "")).strip() != clean_action:
                continue
            key = str(row.get("key", "")).strip()
            quarantine = quarantine_rows.get(key, {}) if key else {}
            if isinstance(quarantine, dict) and quarantine:
                quarantined_matches.append(self._public_quarantine_row(quarantine))
                continue
            score = self._match_score(
                row=row,
                query=query,
                app=app,
                window_title=window_title,
                pre_state_hash=pre_state_hash,
                transition_signature=transition_signature,
            )
            if score <= 0.0:
                continue
            viability = self._lookup_viability(
                row=row,
                app=app,
                window_title=window_title,
                pre_state_hash=pre_state_hash,
                transition_signature=transition_signature,
            )
            viability_score = self._coerce_float(
                viability.get("score", 1.0),
                minimum=0.0,
                maximum=1.0,
                default=1.0,
            )
            viability_risk = self._coerce_float(
                viability.get("risk", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            viability_policy = str(viability.get("policy", "use")).strip().lower() or "use"
            invalidation_flags_raw = viability.get("flags", [])
            invalidation_flags = [
                self._normalize_text(item)
                for item in (invalidation_flags_raw if isinstance(invalidation_flags_raw, list) else [])
                if self._normalize_text(item)
            ]
            if viability_policy == "skip":
                unstable_filtered += 1
                if key and bool(viability.get("auto_quarantine", False)):
                    auto_quarantine_rows.append(
                        {
                            "key": key,
                            "row": row,
                            "flags": invalidation_flags,
                            "risk": viability_risk,
                            "score": viability_score,
                        }
                    )
                continue
            adjusted_score = score * (0.68 + (viability_score * 0.32))
            if viability_policy == "use_with_probe":
                adjusted_score *= 0.92
            payload_row = dict(row)
            payload_row["raw_match_score"] = round(score, 6)
            payload_row["match_score"] = round(max(0.0, adjusted_score), 6)
            payload_row["viability_score"] = round(viability_score, 6)
            payload_row["risk_score"] = round(viability_risk, 6)
            payload_row["viability_policy"] = viability_policy
            payload_row["invalidation_flags"] = invalidation_flags[:16]
            candidates.append(payload_row)

        auto_quarantined = 0
        if auto_quarantine_rows:
            now = datetime.now(timezone.utc)
            now_ts = now.timestamp()
            with self._lock:
                for item in auto_quarantine_rows:
                    key = str(item.get("key", "")).strip()
                    if not key:
                        continue
                    if isinstance(self._quarantine.get(key), dict) and self._quarantine.get(key):
                        continue
                    row = item.get("row", {})
                    if not isinstance(row, dict):
                        continue
                    flags = item.get("flags", [])
                    normalized_flags = [
                        self._normalize_text(flag)
                        for flag in (flags if isinstance(flags, list) else [])
                        if self._normalize_text(flag)
                    ]
                    ttl = min(
                        7200,
                        max(
                            300,
                            int(
                                self.quarantine_ttl_s
                                + (
                                    self._coerce_float(
                                        item.get("risk", 0.0),
                                        minimum=0.0,
                                        maximum=1.0,
                                        default=0.0,
                                    )
                                    * 1800.0
                                )
                            ),
                        ),
                    )
                    expires_at = datetime.fromtimestamp(now_ts + float(ttl), tz=timezone.utc).isoformat()
                    self._quarantine[key] = {
                        "key": key,
                        "action": clean_action,
                        "query": str(row.get("query", "")).strip(),
                        "app": str(row.get("app", "")).strip(),
                        "window_title": str(row.get("window_title", "")).strip(),
                        "control_type": str(row.get("control_type", "")).strip(),
                        "target_mode": str(row.get("target_mode", "")).strip(),
                        "reason": "lookup_viability_degraded",
                        "severity": "soft",
                        "signals": (normalized_flags or ["lookup_viability_degraded"])[:16],
                        "hits": 1,
                        "updated_at": now.isoformat(),
                        "expires_at": expires_at,
                    }
                    auto_quarantined += 1
                if auto_quarantined > 0:
                    self._updates_since_save += auto_quarantined
                    self._prune_quarantine_locked(now_ts=now_ts)
                    self._trim_locked()
                    self._maybe_save_locked(force=False)

        candidates.sort(
            key=lambda item: (
                -self._coerce_float(item.get("match_score", 0.0), minimum=0.0, maximum=5.0, default=0.0),
                -self._coerce_int(item.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                str(item.get("updated_at", "")),
            )
        )

        return {
            "status": "success",
            "action": clean_action,
            "query": query,
            "app": app,
            "window_title": window_title,
            "pre_state_hash": pre_state_hash,
            "transition_signature": transition_signature,
            "count": min(len(candidates), bounded_limit),
            "items": [self._public_row(item) for item in candidates[:bounded_limit]],
            "quarantine_skipped": len(quarantined_matches),
            "quarantine": quarantined_matches[:6],
            "filtered_unstable": int(unstable_filtered),
            "auto_quarantined": int(auto_quarantined),
        }

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        status: str,
        output: Dict[str, Any] | None = None,
        evidence: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
        error: str = "",
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip()
        if clean_action not in self._MANAGED_ACTIONS:
            return {"status": "skip"}

        payload = args if isinstance(args, dict) else {}
        result_output = output if isinstance(output, dict) else {}
        result_evidence = evidence if isinstance(evidence, dict) else {}
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        normalized_status = str(status or "").strip().lower() or "unknown"
        normalized_error = str(error or "").strip()

        query = self._extract_query(payload)
        app = self._extract_app(payload, result_output, runtime_meta)
        window_title = self._extract_window_title(payload, result_output, runtime_meta)
        control_type = self._extract_control_type(payload, result_output, result_evidence)
        target_mode = self._extract_target_mode(payload, result_output, result_evidence)
        element_id = self._extract_element_id(payload, result_output, result_evidence)
        x_value, y_value = self._extract_coordinates(payload, result_output, result_evidence)
        confidence = self._extract_confidence(result_output, result_evidence)
        pre_state_hash, post_state_hash = self._extract_state_hashes_from_outcome(result_evidence, runtime_meta)

        if not query:
            return {"status": "skip", "reason": "missing_query"}
        if not element_id and (x_value is None or y_value is None):
            return {"status": "skip", "reason": "missing_anchor_signature"}

        key = self._make_key(
            action=clean_action,
            query=query,
            app=app,
            window_title=window_title,
            control_type=control_type,
            target_mode=target_mode,
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            previous = dict(self._anchors.get(key, {}))
            samples = self._coerce_int(previous.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
            successes = self._coerce_int(previous.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
            failures = self._coerce_int(previous.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
            consecutive_failures = self._coerce_int(
                previous.get("consecutive_failures", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            )
            confidence_ema = self._coerce_float(
                previous.get("confidence_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            state_profile = self._load_state_profile(previous.get("state_profile", {}))
            transition_profile = self._load_transition_profile(previous.get("transition_profile", {}))
            transition_signature = self._extract_transition_signature_from_outcome(
                evidence=result_evidence,
                metadata=runtime_meta,
                pre_state_hash=pre_state_hash,
                post_state_hash=post_state_hash,
            )
            transition_context = self._extract_guardrail_transition_context(
                metadata=runtime_meta,
                pre_state_hash=pre_state_hash,
                post_state_hash=post_state_hash,
            )
            transition_samples = 0
            transition_success_rate = 0.0
            transition_block_ema = 0.0
            transition_mismatch_ema = 0.0
            transition_layout_shift_ema = 0.0
            transition_not_found_ema = 0.0

            if normalized_status == "success":
                successes += 1
                consecutive_failures = 0
            elif normalized_status in {"failed", "blocked"}:
                failures += 1
                consecutive_failures += 1

            if confidence > 0.0:
                confidence_ema = (confidence_ema * 0.84) + (confidence * 0.16)
            success_rate = float(successes) / max(1.0, float(samples))
            if pre_state_hash:
                profile_row = dict(state_profile.get(pre_state_hash, {}))
                profile_samples = self._coerce_int(profile_row.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
                profile_successes = self._coerce_int(profile_row.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
                profile_failures = self._coerce_int(profile_row.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
                if normalized_status == "success":
                    profile_successes += 1
                elif normalized_status in {"failed", "blocked"}:
                    profile_failures += 1
                profile_row = {
                    "samples": profile_samples,
                    "successes": profile_successes,
                    "failures": profile_failures,
                    "updated_at": now_iso,
                }
                state_profile[pre_state_hash] = profile_row
                if len(state_profile) > 12:
                    ordered = sorted(
                        state_profile.items(),
                        key=lambda item: (
                            str(item[1].get("updated_at", "")) if isinstance(item[1], dict) else "",
                            self._coerce_int(
                                item[1].get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            )
                            if isinstance(item[1], dict)
                            else 0,
                            item[0],
                        ),
                        reverse=True,
                    )
                    state_profile = {
                        str(key).strip().lower(): dict(value)
                        for key, value in ordered[:12]
                        if str(key).strip() and isinstance(value, dict)
                    }
            if pre_state_hash and post_state_hash:
                transition_key = self._make_transition_key(pre_state_hash=pre_state_hash, post_state_hash=post_state_hash)
                transition_row = dict(transition_profile.get(transition_key, {}))
                transition_samples = self._coerce_int(
                    transition_row.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ) + 1
                transition_successes = self._coerce_int(
                    transition_row.get("successes", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                transition_failures = self._coerce_int(
                    transition_row.get("failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                if normalized_status == "success":
                    transition_successes += 1
                elif normalized_status in {"failed", "blocked"}:
                    transition_failures += 1
                transition_success_rate = float(transition_successes) / max(1.0, float(transition_samples))
                churn_previous = self._coerce_float(
                    transition_row.get("guardrail_churn_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                guardrail_churn = self._coerce_float(
                    transition_context.get("churn_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                churn_ema = (churn_previous * 0.78) + (guardrail_churn * 0.22)
                block_previous = self._coerce_float(
                    transition_row.get("guardrail_block_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                block_signal = max(
                    self._coerce_float(
                        transition_context.get("block_ratio", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    1.0 if normalized_status == "blocked" else 0.0,
                )
                block_ema = (block_previous * 0.8) + (block_signal * 0.2)
                mismatch_previous = self._coerce_float(
                    transition_row.get("anchor_mismatch_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                mismatch_signal = self._coerce_float(
                    transition_context.get("anchor_mismatch_ratio", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                mismatch_ema = (mismatch_previous * 0.8) + (mismatch_signal * 0.2)
                layout_previous = self._coerce_float(
                    transition_row.get("layout_shift_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                layout_signal = self._coerce_float(
                    transition_context.get("layout_shift_hits", 0),
                    minimum=0.0,
                    maximum=12.0,
                    default=0.0,
                ) / 12.0
                layout_ema = (layout_previous * 0.82) + (layout_signal * 0.18)
                not_found_previous = self._coerce_float(
                    transition_row.get("anchor_not_found_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                not_found_signal = self._coerce_float(
                    transition_context.get("anchor_not_found_ratio", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                not_found_ema = (not_found_previous * 0.8) + (not_found_signal * 0.2)
                transition_block_ema = max(0.0, min(1.0, block_ema))
                transition_mismatch_ema = max(0.0, min(1.0, mismatch_ema))
                transition_layout_shift_ema = max(0.0, min(1.0, layout_ema))
                transition_not_found_ema = max(0.0, min(1.0, not_found_ema))
                transition_row = {
                    "samples": transition_samples,
                    "successes": transition_successes,
                    "failures": transition_failures,
                    "success_rate": round(max(0.0, min(1.0, transition_success_rate)), 6),
                    "guardrail_churn_ema": round(max(0.0, min(1.0, churn_ema)), 6),
                    "guardrail_block_ema": round(transition_block_ema, 6),
                    "anchor_mismatch_ema": round(transition_mismatch_ema, 6),
                    "layout_shift_ema": round(transition_layout_shift_ema, 6),
                    "anchor_not_found_ema": round(transition_not_found_ema, 6),
                    "signature": transition_signature,
                    "updated_at": now_iso,
                }
                transition_profile[transition_key] = transition_row
                if len(transition_profile) > 18:
                    ordered_transition = sorted(
                        transition_profile.items(),
                        key=lambda item: (
                            str(item[1].get("updated_at", "")) if isinstance(item[1], dict) else "",
                            self._coerce_int(
                                item[1].get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            )
                            if isinstance(item[1], dict)
                            else 0,
                            item[0],
                        ),
                        reverse=True,
                    )
                    transition_profile = {
                        str(key).strip().lower(): dict(value)
                        for key, value in ordered_transition[:18]
                        if str(key).strip() and isinstance(value, dict)
                    }

            row = {
                "key": key,
                "action": clean_action,
                "query": query,
                "app": app,
                "window_title": window_title,
                "control_type": control_type,
                "target_mode": target_mode,
                "element_id": element_id,
                "x": x_value,
                "y": y_value,
                "samples": samples,
                "successes": successes,
                "failures": failures,
                "success_rate": round(success_rate, 6),
                "consecutive_failures": consecutive_failures,
                "confidence_ema": round(max(0.0, min(1.0, confidence_ema)), 6),
                "last_pre_state_hash": pre_state_hash,
                "last_post_state_hash": post_state_hash,
                "state_profile": state_profile,
                "last_transition_signature": transition_signature,
                "transition_profile": transition_profile,
                "last_status": normalized_status,
                "last_error": normalized_error,
                "updated_at": now_iso,
            }
            self._anchors[key] = row
            if normalized_status == "success":
                self._quarantine.pop(key, None)
            elif normalized_status in {"failed", "blocked"}:
                should_quarantine = (
                    consecutive_failures >= 4
                    and success_rate <= 0.35
                    and samples >= 6
                )
                if (
                    transition_samples >= 3
                    and transition_success_rate <= 0.28
                    and self._coerce_float(transition_context.get("churn_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                    >= 0.38
                ):
                    should_quarantine = True
                if (
                    transition_samples >= 3
                    and (
                        transition_block_ema >= 0.56
                        or transition_mismatch_ema >= 0.48
                        or transition_not_found_ema >= 0.5
                    )
                ):
                    should_quarantine = True
                if should_quarantine:
                    ttl = min(7200, max(300, int(300 + (consecutive_failures * 120))))
                    expires_at = datetime.fromtimestamp(time.time() + float(ttl), tz=timezone.utc).isoformat()
                    previous_quarantine = dict(self._quarantine.get(key, {}))
                    quarantine_hits = self._coerce_int(previous_quarantine.get("hits", 0), minimum=0, maximum=10_000_000, default=0) + 1
                    signals = ["repeated_failures", "low_success_rate"]
                    if transition_samples >= 3 and transition_success_rate <= 0.28:
                        signals.append("transition_low_success")
                    if self._coerce_float(transition_context.get("churn_score", 0.0), minimum=0.0, maximum=1.0, default=0.0) >= 0.38:
                        signals.append("transition_guardrail_churn")
                    if transition_block_ema >= 0.56:
                        signals.append("transition_guardrail_block")
                    if transition_mismatch_ema >= 0.48:
                        signals.append("transition_anchor_mismatch")
                    if transition_layout_shift_ema >= 0.45:
                        signals.append("transition_layout_shift")
                    if transition_not_found_ema >= 0.5:
                        signals.append("transition_anchor_not_found")
                    self._quarantine[key] = {
                        "key": key,
                        "action": clean_action,
                        "query": query,
                        "app": app,
                        "window_title": window_title,
                        "control_type": control_type,
                        "target_mode": target_mode,
                        "reason": normalized_error[:600] or "anchor reliability degraded",
                        "severity": "soft",
                        "signals": signals[:16],
                        "hits": quarantine_hits,
                        "updated_at": now_iso,
                        "expires_at": expires_at,
                    }
            self._prune_quarantine_locked(now_ts=time.time())
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=False)
        return {"status": "success", "key": key}

    def hints(self, *, query: str = "", limit: int = 6) -> List[Dict[str, Any]]:
        q = self._normalize_text(query)
        bounded = self._coerce_int(limit, minimum=1, maximum=50, default=6)
        with self._lock:
            rows = list(self._anchors.values())
        if q:
            rows = [row for row in rows if q in str(row.get("query", ""))]
        rows.sort(
            key=lambda row: (
                -self._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                -self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("updated_at", "")),
            )
        )
        return [self._public_row(row) for row in rows[:bounded]]

    def snapshot(self, *, action: str = "", query: str = "", limit: int = 200) -> Dict[str, Any]:
        target_action = str(action or "").strip()
        target_query = self._normalize_text(query)
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        with self._lock:
            rows = list(self._anchors.values())
            self._prune_quarantine_locked(now_ts=time.time())
            quarantine_rows = list(self._quarantine.values())
        if target_action:
            rows = [row for row in rows if str(row.get("action", "")).strip() == target_action]
            quarantine_rows = [row for row in quarantine_rows if str(row.get("action", "")).strip() == target_action]
        if target_query:
            rows = [row for row in rows if target_query in str(row.get("query", ""))]
            quarantine_rows = [row for row in quarantine_rows if target_query in str(row.get("query", ""))]
        rows.sort(
            key=lambda row: (
                -self._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                -self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("updated_at", "")),
            )
        )
        items = [self._public_row(row) for row in rows[:bounded]]
        quarantine_rows.sort(
            key=lambda row: (
                str(row.get("updated_at", "")),
                -self._coerce_int(row.get("hits", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("key", "")),
            ),
            reverse=True,
        )
        return {
            "status": "success",
            "count": len(items),
            "total": len(rows),
            "quarantine_count": len(quarantine_rows),
            "quarantine_items": [self._public_quarantine_row(row) for row in quarantine_rows[: min(20, bounded)]],
            "items": items,
        }

    def reset(self, *, action: str = "", query: str = "") -> Dict[str, Any]:
        target_action = str(action or "").strip()
        target_query = self._normalize_text(query)
        removed = 0
        quarantine_removed = 0
        with self._lock:
            if not target_action and not target_query:
                removed = len(self._anchors)
                self._anchors = {}
                quarantine_removed = len(self._quarantine)
                self._quarantine = {}
            else:
                keep: Dict[str, Dict[str, Any]] = {}
                for key, row in self._anchors.items():
                    row_action = str(row.get("action", "")).strip()
                    row_query = str(row.get("query", ""))
                    action_match = target_action and row_action == target_action
                    query_match = target_query and target_query in row_query
                    if action_match or query_match:
                        removed += 1
                        continue
                    keep[key] = row
                self._anchors = keep
                quarantine_keep: Dict[str, Dict[str, Any]] = {}
                for key, row in self._quarantine.items():
                    row_action = str(row.get("action", "")).strip()
                    row_query = str(row.get("query", ""))
                    action_match = target_action and row_action == target_action
                    query_match = target_query and target_query in row_query
                    if action_match or query_match:
                        quarantine_removed += 1
                        continue
                    quarantine_keep[key] = row
                self._quarantine = quarantine_keep
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "removed": removed,
            "quarantine_removed": quarantine_removed,
            "action": target_action,
            "query": target_query,
        }

    def quarantine(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        metadata: Dict[str, Any] | None = None,
        reason: str = "",
        severity: str = "soft",
        signals: List[str] | None = None,
        ttl_s: int = 0,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip()
        if clean_action not in self._MANAGED_ACTIONS:
            return {"status": "skip"}
        payload = args if isinstance(args, dict) else {}
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        query = self._extract_query(payload)
        if not query:
            return {"status": "skip", "reason": "missing_query"}
        app = self._extract_app(payload, {}, runtime_meta)
        window_title = self._extract_window_title(payload, {}, runtime_meta)
        control_type = self._extract_control_type(payload, {}, {})
        target_mode = self._extract_target_mode(payload, {}, {})
        key = self._make_key(
            action=clean_action,
            query=query,
            app=app,
            window_title=window_title,
            control_type=control_type,
            target_mode=target_mode,
        )
        now = datetime.now(timezone.utc)
        ttl = self._coerce_int(ttl_s or self.quarantine_ttl_s, minimum=60, maximum=172_800, default=self.quarantine_ttl_s)
        expires_at = datetime.fromtimestamp(now.timestamp() + float(ttl), tz=timezone.utc).isoformat()
        clean_signals = [self._normalize_text(item) for item in (signals if isinstance(signals, list) else []) if self._normalize_text(item)]
        clean_severity = self._normalize_text(severity) or "soft"
        if clean_severity not in {"soft", "hard"}:
            clean_severity = "soft"

        with self._lock:
            previous = dict(self._quarantine.get(key, {}))
            hits = self._coerce_int(previous.get("hits", 0), minimum=0, maximum=10_000_000, default=0) + 1
            row = {
                "key": key,
                "action": clean_action,
                "query": query,
                "app": app,
                "window_title": window_title,
                "control_type": control_type,
                "target_mode": target_mode,
                "reason": str(reason or "").strip()[:600],
                "severity": clean_severity,
                "signals": clean_signals[:16],
                "hits": hits,
                "updated_at": now.isoformat(),
                "expires_at": expires_at,
            }
            self._quarantine[key] = row
            self._prune_quarantine_locked(now_ts=now.timestamp())
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=False)
        return {"status": "success", "item": self._public_quarantine_row(row)}

    def quarantine_snapshot(self, *, action: str = "", query: str = "", limit: int = 200) -> Dict[str, Any]:
        target_action = str(action or "").strip()
        target_query = self._normalize_text(query)
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        with self._lock:
            self._prune_quarantine_locked(now_ts=time.time())
            rows = list(self._quarantine.values())
        if target_action:
            rows = [row for row in rows if str(row.get("action", "")).strip() == target_action]
        if target_query:
            rows = [row for row in rows if target_query in str(row.get("query", ""))]
        rows.sort(
            key=lambda row: (
                str(row.get("updated_at", "")),
                -self._coerce_int(row.get("hits", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("key", "")),
            ),
            reverse=True,
        )
        items = [self._public_quarantine_row(row) for row in rows[:bounded]]
        return {
            "status": "success",
            "count": len(items),
            "total": len(rows),
            "items": items,
        }

    def clear_quarantine(self, *, key: str = "", action: str = "", query: str = "") -> Dict[str, Any]:
        target_key = str(key or "").strip()
        target_action = str(action or "").strip()
        target_query = self._normalize_text(query)
        removed = 0
        with self._lock:
            self._prune_quarantine_locked(now_ts=time.time())
            if target_key:
                if target_key in self._quarantine:
                    self._quarantine.pop(target_key, None)
                    removed = 1
            elif not target_action and not target_query:
                removed = len(self._quarantine)
                self._quarantine = {}
            else:
                keep: Dict[str, Dict[str, Any]] = {}
                for row_key, row in self._quarantine.items():
                    row_action = str(row.get("action", "")).strip()
                    row_query = str(row.get("query", ""))
                    action_match = target_action and row_action == target_action
                    query_match = target_query and target_query in row_query
                    if action_match or query_match:
                        removed += 1
                        continue
                    keep[row_key] = row
                self._quarantine = keep
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "removed": removed,
            "key": target_key,
            "action": target_action,
            "query": target_query,
        }

    def _match_score(
        self,
        *,
        row: Dict[str, Any],
        query: str,
        app: str,
        window_title: str,
        pre_state_hash: str,
        transition_signature: str,
    ) -> float:
        row_query = str(row.get("query", ""))
        row_app = str(row.get("app", ""))
        row_window = str(row.get("window_title", ""))

        score = 0.0
        if query:
            if row_query == query:
                score += 0.62
            elif query in row_query or row_query in query:
                score += 0.3
            else:
                return 0.0
        else:
            score += 0.1

        if app:
            if row_app == app:
                score += 0.18
            elif row_app:
                score -= 0.08
        if window_title:
            if row_window == window_title:
                score += 0.12
            elif row_window and (window_title in row_window or row_window in window_title):
                score += 0.06

        if pre_state_hash:
            row_last_pre_hash = str(row.get("last_pre_state_hash", "")).strip().lower()
            if row_last_pre_hash and row_last_pre_hash == pre_state_hash:
                score += 0.1
            elif row_last_pre_hash and row_last_pre_hash != pre_state_hash:
                score -= 0.04
            state_profile = self._load_state_profile(row.get("state_profile", {}))
            profile_row = state_profile.get(pre_state_hash, {})
            if isinstance(profile_row, dict):
                profile_samples = self._coerce_int(profile_row.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
                profile_successes = self._coerce_int(profile_row.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
                profile_rate = float(profile_successes) / max(1.0, float(profile_samples))
                score += 0.1 + (profile_rate * 0.24) + min(0.1, math.log1p(float(profile_samples)) * 0.03)
        transition_profile = self._load_transition_profile(row.get("transition_profile", {}))
        if transition_signature and transition_profile:
            matched_transition = None
            for transition_key, transition_row in transition_profile.items():
                if not isinstance(transition_row, dict):
                    continue
                candidate_signature = self._normalize_transition_signature(transition_row.get("signature", ""))
                if candidate_signature and candidate_signature == transition_signature:
                    matched_transition = transition_row
                    break
                if str(transition_key).strip().lower() == transition_signature:
                    matched_transition = transition_row
                    break
            if isinstance(matched_transition, dict):
                transition_samples = self._coerce_int(
                    matched_transition.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                transition_success_rate = self._coerce_float(
                    matched_transition.get("success_rate", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                churn_ema = self._coerce_float(
                    matched_transition.get("guardrail_churn_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                block_ema = self._coerce_float(
                    matched_transition.get("guardrail_block_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                mismatch_ema = self._coerce_float(
                    matched_transition.get("anchor_mismatch_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                layout_shift_ema = self._coerce_float(
                    matched_transition.get("layout_shift_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                not_found_ema = self._coerce_float(
                    matched_transition.get("anchor_not_found_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                score += 0.08 + (transition_success_rate * 0.2) + min(0.08, math.log1p(float(transition_samples)) * 0.03)
                score -= min(0.14, churn_ema * 0.16)
                score -= min(0.12, block_ema * 0.16)
                score -= min(0.1, mismatch_ema * 0.14)
                score -= min(0.08, layout_shift_ema * 0.12)
                score -= min(0.1, not_found_ema * 0.14)
            elif len(transition_profile) >= 4:
                score -= 0.08

        success_rate = self._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        confidence_ema = self._coerce_float(row.get("confidence_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        samples = self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        failures = self._coerce_int(row.get("consecutive_failures", 0), minimum=0, maximum=10_000_000, default=0)

        score += (success_rate * 0.32)
        score += (confidence_ema * 0.24)
        score += min(0.18, math.log1p(float(samples)) * 0.05)
        score -= min(0.2, float(failures) * 0.05)
        return max(0.0, score)

    def _lookup_viability(
        self,
        *,
        row: Dict[str, Any],
        app: str,
        window_title: str,
        pre_state_hash: str,
        transition_signature: str,
    ) -> Dict[str, Any]:
        risk = 0.0
        flags: List[str] = []

        samples = self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        success_rate = self._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        consecutive_failures = self._coerce_int(
            row.get("consecutive_failures", 0),
            minimum=0,
            maximum=10_000_000,
            default=0,
        )
        confidence_ema = self._coerce_float(row.get("confidence_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        last_status = self._normalize_text(row.get("last_status", ""))
        row_app = self._normalize_text(row.get("app", ""))
        row_window = self._normalize_text(row.get("window_title", ""))
        row_pre_hash = self._normalize_state_hash(row.get("last_pre_state_hash"))
        row_updated_at = str(row.get("updated_at", "")).strip()
        row_age_s = 0.0
        if row_updated_at:
            try:
                parsed = datetime.fromisoformat(row_updated_at.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                row_age_s = max(0.0, time.time() - parsed.astimezone(timezone.utc).timestamp())
            except Exception:
                row_age_s = 0.0

        if app and row_app and app != row_app:
            risk += 0.16
            flags.append("app_context_mismatch")
        if window_title and row_window and window_title != row_window:
            risk += 0.14
            flags.append("window_context_mismatch")

        state_profile = self._load_state_profile(row.get("state_profile", {}))
        if pre_state_hash:
            if row_pre_hash and row_pre_hash != pre_state_hash:
                risk += 0.08
                flags.append("pre_state_anchor_mismatch")
            if state_profile and pre_state_hash not in state_profile and len(state_profile) >= 3:
                risk += 0.16
                flags.append("state_profile_unseen_pre_hash")
            profile_row = state_profile.get(pre_state_hash, {})
            if isinstance(profile_row, dict):
                profile_samples = self._coerce_int(
                    profile_row.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                if profile_samples >= 3:
                    profile_success_rate = self._coerce_float(
                        profile_row.get("successes", 0),
                        minimum=0.0,
                        maximum=10_000_000.0,
                        default=0.0,
                    ) / max(1.0, float(profile_samples))
                    if profile_success_rate < 0.42:
                        risk += 0.22
                        flags.append("state_profile_pre_hash_low_success")

        transition_profile = self._load_transition_profile(row.get("transition_profile", {}))
        if transition_signature and transition_profile:
            matched = {}
            for transition_key, transition_row in transition_profile.items():
                if not isinstance(transition_row, dict):
                    continue
                signature = self._normalize_transition_signature(transition_row.get("signature", ""))
                if signature and signature == transition_signature:
                    matched = transition_row
                    break
                if self._normalize_transition_signature(transition_key) == transition_signature:
                    matched = transition_row
                    break
            if not matched and len(transition_profile) >= 4:
                risk += 0.17
                flags.append("transition_profile_unseen_signature")
            if isinstance(matched, dict) and matched:
                transition_samples = self._coerce_int(
                    matched.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                transition_success = self._coerce_float(
                    matched.get("success_rate", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                transition_churn = self._coerce_float(
                    matched.get("guardrail_churn_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                transition_block = self._coerce_float(
                    matched.get("guardrail_block_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                transition_mismatch = self._coerce_float(
                    matched.get("anchor_mismatch_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                transition_layout_shift = self._coerce_float(
                    matched.get("layout_shift_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                transition_not_found = self._coerce_float(
                    matched.get("anchor_not_found_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if transition_samples >= 3 and transition_success < 0.4:
                    risk += 0.22
                    flags.append("transition_profile_low_success")
                if transition_samples >= 3 and transition_churn > 0.52:
                    risk += 0.2
                    flags.append("transition_profile_high_churn")
                if transition_samples >= 3 and transition_block >= 0.46:
                    risk += 0.2
                    flags.append("transition_guardrail_blocked")
                if transition_samples >= 3 and transition_mismatch >= 0.4:
                    risk += 0.16
                    flags.append("transition_anchor_mismatch")
                if transition_samples >= 3 and transition_not_found >= 0.44:
                    risk += 0.18
                    flags.append("transition_anchor_not_found")
                if transition_samples >= 3 and transition_layout_shift >= 0.42:
                    risk += 0.14
                    flags.append("transition_layout_shift")

        if samples >= 6 and success_rate < 0.38:
            risk += 0.27
            flags.append("low_success_rate")
        if consecutive_failures >= 3:
            risk += min(0.3, float(consecutive_failures - 2) * 0.07)
            flags.append("recent_consecutive_failures")
        if last_status in {"failed", "blocked"}:
            risk += 0.08
            flags.append("last_status_unstable")
        if confidence_ema < 0.2 and samples >= 5:
            risk += 0.12
            flags.append("low_confidence_ema")

        if row_age_s >= 86_400.0:
            risk += 0.24
            flags.append("stale_anchor_hard")
        elif row_age_s >= 21_600.0:
            risk += 0.12
            flags.append("stale_anchor_soft")

        risk = self._coerce_float(risk, minimum=0.0, maximum=1.0, default=0.0)
        score = self._coerce_float(1.0 - risk, minimum=0.0, maximum=1.0, default=0.0)
        policy = "use"
        if risk >= 0.5:
            policy = "use_with_probe"
        if (
            risk >= 0.82
            and samples >= 6
            and (consecutive_failures >= 4 or success_rate <= 0.28)
        ):
            policy = "skip"
        auto_quarantine = bool(
            policy == "skip"
            and (
                "low_success_rate" in flags
                or "recent_consecutive_failures" in flags
                or "transition_profile_low_success" in flags
                or "transition_guardrail_blocked" in flags
                or "transition_anchor_not_found" in flags
            )
        )
        return {
            "score": score,
            "risk": risk,
            "policy": policy,
            "flags": flags,
            "auto_quarantine": auto_quarantine,
        }

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        try:
            payload = json.loads(self.store_path.read_text(encoding="utf-8"))
        except Exception:
            return
        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return
        quarantine_raw = payload.get("quarantine", []) if isinstance(payload, dict) else []
        quarantine_items = []
        if isinstance(quarantine_raw, list):
            quarantine_items = quarantine_raw
        elif isinstance(quarantine_raw, dict):
            quarantine_items = list(quarantine_raw.values())
        loaded: Dict[str, Dict[str, Any]] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            action = str(row.get("action", "")).strip()
            query = self._normalize_text(row.get("query", ""))
            if not action or not query:
                continue
            key = str(row.get("key", "")).strip() or self._make_key(
                action=action,
                query=query,
                app=self._normalize_text(row.get("app", "")),
                window_title=self._normalize_text(row.get("window_title", "")),
                control_type=self._normalize_text(row.get("control_type", "")),
                target_mode=self._normalize_text(row.get("target_mode", "")),
            )
            loaded[key] = {
                "key": key,
                "action": action,
                "query": query,
                "app": self._normalize_text(row.get("app", "")),
                "window_title": self._normalize_text(row.get("window_title", "")),
                "control_type": self._normalize_text(row.get("control_type", "")),
                "target_mode": self._normalize_text(row.get("target_mode", "")),
                "element_id": str(row.get("element_id", "")).strip(),
                "x": self._coerce_optional_int(row.get("x")),
                "y": self._coerce_optional_int(row.get("y")),
                "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": self._coerce_int(row.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": self._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "success_rate": self._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "consecutive_failures": self._coerce_int(
                    row.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "confidence_ema": self._coerce_float(row.get("confidence_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "last_pre_state_hash": self._normalize_state_hash(row.get("last_pre_state_hash")),
                "last_post_state_hash": self._normalize_state_hash(row.get("last_post_state_hash")),
                "state_profile": self._load_state_profile(row.get("state_profile", {})),
                "last_transition_signature": self._normalize_transition_signature(row.get("last_transition_signature", "")),
                "transition_profile": self._load_transition_profile(row.get("transition_profile", {})),
                "last_status": str(row.get("last_status", "")).strip().lower(),
                "last_error": str(row.get("last_error", "")).strip(),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        loaded_quarantine: Dict[str, Dict[str, Any]] = {}
        now_ts = time.time()
        for row in quarantine_items:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key", "")).strip()
            if not key:
                continue
            expires_at = str(row.get("expires_at", "")).strip()
            expires_ts = self._to_timestamp(expires_at)
            if expires_ts > 0 and expires_ts <= now_ts:
                continue
            loaded_quarantine[key] = {
                "key": key,
                "action": str(row.get("action", "")).strip(),
                "query": self._normalize_text(row.get("query", "")),
                "app": self._normalize_text(row.get("app", "")),
                "window_title": self._normalize_text(row.get("window_title", "")),
                "control_type": self._normalize_text(row.get("control_type", "")),
                "target_mode": self._normalize_text(row.get("target_mode", "")),
                "reason": str(row.get("reason", "")).strip(),
                "severity": self._normalize_text(row.get("severity", "")) or "soft",
                "signals": [
                    self._normalize_text(item)
                    for item in (row.get("signals", []) if isinstance(row.get("signals", []), list) else [])
                    if self._normalize_text(item)
                ][:16],
                "hits": self._coerce_int(row.get("hits", 0), minimum=0, maximum=10_000_000, default=0),
                "updated_at": str(row.get("updated_at", "")).strip(),
                "expires_at": expires_at,
            }
        with self._lock:
            self._anchors = loaded
            self._quarantine = loaded_quarantine
            self._prune_quarantine_locked(now_ts=time.time())
            self._trim_locked()

    def _trim_locked(self) -> None:
        self._prune_quarantine_locked(now_ts=time.time())
        if len(self._anchors) <= self.max_entries:
            if len(self._quarantine) > self.quarantine_max_entries:
                quarantine_rows = sorted(
                    self._quarantine.values(),
                    key=lambda row: (
                        str(row.get("updated_at", "")),
                        self._coerce_int(row.get("hits", 0), minimum=0, maximum=10_000_000, default=0),
                        str(row.get("key", "")),
                    ),
                    reverse=True,
                )
                self._quarantine = {
                    str(row.get("key", "")).strip(): dict(row)
                    for row in quarantine_rows[: self.quarantine_max_entries]
                    if str(row.get("key", "")).strip()
                }
            return
        rows = sorted(
            self._anchors.values(),
            key=lambda row: (
                str(row.get("updated_at", "")),
                self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("key", "")),
            ),
            reverse=True,
        )
        self._anchors = {
            str(row.get("key", "")).strip(): dict(row)
            for row in rows[: self.max_entries]
            if str(row.get("key", "")).strip()
        }
        if len(self._quarantine) > self.quarantine_max_entries:
            quarantine_rows = sorted(
                self._quarantine.values(),
                key=lambda row: (
                    str(row.get("updated_at", "")),
                    self._coerce_int(row.get("hits", 0), minimum=0, maximum=10_000_000, default=0),
                    str(row.get("key", "")),
                ),
                reverse=True,
            )
            self._quarantine = {
                str(row.get("key", "")).strip(): dict(row)
                for row in quarantine_rows[: self.quarantine_max_entries]
                if str(row.get("key", "")).strip()
            }

    def _maybe_save_locked(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force:
            if self._updates_since_save < 16 and (now - self._last_save_monotonic) < 15.0:
                return
        self._prune_quarantine_locked(now_ts=time.time())
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "items": list(self._anchors.values()),
            "quarantine": list(self._quarantine.values()),
        }
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            self.store_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            self._updates_since_save = 0
            self._last_save_monotonic = now
        except Exception:
            return

    @classmethod
    def _make_key(
        cls,
        *,
        action: str,
        query: str,
        app: str,
        window_title: str,
        control_type: str,
        target_mode: str,
    ) -> str:
        parts = [
            cls._normalize_text(action),
            cls._normalize_text(query),
            cls._normalize_text(app),
            cls._normalize_text(window_title),
            cls._normalize_text(control_type),
            cls._normalize_text(target_mode),
        ]
        return "|".join(parts)

    @staticmethod
    def _normalize_text(value: object) -> str:
        return str(value or "").strip().lower()

    @classmethod
    def _extract_query(cls, payload: Dict[str, Any]) -> str:
        for key in ("query", "text", "target"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return cls._normalize_text(value)
        return ""

    @classmethod
    def _extract_app(cls, args: Dict[str, Any], output: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        candidates = [
            args.get("app_name"),
            output.get("app_name"),
            output.get("app"),
            metadata.get("desktop_app"),
            metadata.get("active_app"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return cls._normalize_text(value)
        return ""

    @classmethod
    def _extract_window_title(cls, args: Dict[str, Any], output: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        candidates = [
            args.get("window_title"),
            output.get("window_title"),
            cls._read_path(output, "window.title"),
            metadata.get("active_window_title"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return cls._normalize_text(value)
        return ""

    @classmethod
    def _extract_control_type(cls, args: Dict[str, Any], output: Dict[str, Any], evidence: Dict[str, Any]) -> str:
        candidates = [
            args.get("control_type"),
            output.get("control_type"),
            cls._read_path(evidence, "desktop_anchor.output.items.0.control_type"),
            cls._read_path(evidence, "desktop_recovery.suggestion.control_type"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return cls._normalize_text(value)
        return ""

    @classmethod
    def _extract_target_mode(cls, args: Dict[str, Any], output: Dict[str, Any], evidence: Dict[str, Any]) -> str:
        candidates = [
            args.get("target_mode"),
            output.get("method"),
            cls._read_path(evidence, "desktop_recovery.suggestion.target_mode"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return cls._normalize_text(value)
        return ""

    @classmethod
    def _extract_element_id(cls, args: Dict[str, Any], output: Dict[str, Any], evidence: Dict[str, Any]) -> str:
        candidates = [
            args.get("element_id"),
            output.get("element_id"),
            cls._read_path(evidence, "desktop_anchor.args_patch.element_id"),
            cls._read_path(evidence, "desktop_anchor.output.items.0.element_id"),
            cls._read_path(evidence, "desktop_recovery.suggestion.element_id"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @classmethod
    def _extract_coordinates(
        cls,
        args: Dict[str, Any],
        output: Dict[str, Any],
        evidence: Dict[str, Any],
    ) -> tuple[int | None, int | None]:
        x_val = cls._coerce_optional_int(output.get("x"))
        y_val = cls._coerce_optional_int(output.get("y"))
        if x_val is not None and y_val is not None:
            return (x_val, y_val)
        x_val = cls._coerce_optional_int(args.get("x"))
        y_val = cls._coerce_optional_int(args.get("y"))
        if x_val is not None and y_val is not None:
            return (x_val, y_val)
        recovery_target = cls._read_path(evidence, "desktop_recovery.suggestion.ocr_target")
        if isinstance(recovery_target, dict):
            x_val = cls._coerce_optional_int(recovery_target.get("x"))
            y_val = cls._coerce_optional_int(recovery_target.get("y"))
            if x_val is not None and y_val is not None:
                return (x_val, y_val)
        return (None, None)

    @classmethod
    def _extract_confidence(cls, output: Dict[str, Any], evidence: Dict[str, Any]) -> float:
        values = [
            output.get("confidence"),
            cls._read_path(evidence, "desktop_anchor.output.items.0.match_score"),
            cls._read_path(evidence, "desktop_recovery.suggestion.confidence"),
            cls._read_path(evidence, "desktop_recovery.suggestion.ocr_target.confidence"),
        ]
        for value in values:
            try:
                parsed = float(value)
            except Exception:
                continue
            if 0.0 <= parsed <= 1.0:
                return parsed
        return 0.0

    @classmethod
    def _extract_lookup_pre_state_hash(cls, args: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        values = [
            args.get("pre_state_hash"),
            metadata.get("__desktop_pre_state_hash"),
            metadata.get("desktop_pre_state_hash"),
            metadata.get("state_hash"),
        ]
        for value in values:
            normalized = cls._normalize_state_hash(value)
            if normalized:
                return normalized
        return ""

    @classmethod
    def _extract_lookup_transition_signature(cls, args: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        values = [
            args.get("transition_signature"),
            args.get("state_transition"),
            metadata.get("__desktop_transition_signature"),
            metadata.get("desktop_transition_signature"),
        ]
        for value in values:
            normalized = cls._normalize_transition_signature(value)
            if normalized:
                return normalized
        pre_hash = cls._extract_lookup_pre_state_hash(args, metadata)
        post_values = [
            args.get("post_state_hash"),
            metadata.get("__desktop_post_state_hash"),
            metadata.get("desktop_post_state_hash"),
        ]
        post_hash = ""
        for value in post_values:
            normalized = cls._normalize_state_hash(value)
            if normalized:
                post_hash = normalized
                break
        if pre_hash and post_hash:
            return cls._make_transition_key(pre_state_hash=pre_hash, post_state_hash=post_hash)
        return ""

    @classmethod
    def _extract_state_hashes_from_outcome(
        cls,
        evidence: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> tuple[str, str]:
        pre_values = [
            cls._read_path(evidence, "desktop_state.pre_hash"),
            cls._read_path(evidence, "desktop_state.previous_hash"),
            metadata.get("__desktop_pre_state_hash"),
            metadata.get("desktop_pre_state_hash"),
        ]
        post_values = [
            cls._read_path(evidence, "desktop_state.state_hash"),
            cls._read_path(evidence, "desktop_state.post_hash"),
            metadata.get("__desktop_post_state_hash"),
            metadata.get("desktop_post_state_hash"),
        ]
        pre_hash = ""
        post_hash = ""
        for value in pre_values:
            normalized = cls._normalize_state_hash(value)
            if normalized:
                pre_hash = normalized
                break
        for value in post_values:
            normalized = cls._normalize_state_hash(value)
            if normalized:
                post_hash = normalized
                break
        return (pre_hash, post_hash)

    @classmethod
    def _extract_transition_signature_from_outcome(
        cls,
        *,
        evidence: Dict[str, Any],
        metadata: Dict[str, Any],
        pre_state_hash: str,
        post_state_hash: str,
    ) -> str:
        values = [
            cls._read_path(evidence, "desktop_state.transition_signature"),
            metadata.get("__desktop_transition_signature"),
            metadata.get("desktop_transition_signature"),
        ]
        for value in values:
            normalized = cls._normalize_transition_signature(value)
            if normalized:
                return normalized
        if pre_state_hash and post_state_hash:
            return cls._make_transition_key(
                pre_state_hash=pre_state_hash,
                post_state_hash=post_state_hash,
            )
        return ""

    @classmethod
    def _extract_guardrail_transition_context(
        cls,
        *,
        metadata: Dict[str, Any],
        pre_state_hash: str,
        post_state_hash: str,
    ) -> Dict[str, Any]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        rows = runtime_meta.get("__desktop_guardrail_feedback")
        feedback = rows if isinstance(rows, list) else []
        clean_pre = cls._normalize_state_hash(pre_state_hash)
        clean_post = cls._normalize_state_hash(post_state_hash)
        changed_paths_total = 0
        transition_count = 0
        context_shift_hits = 0
        visual_shift_hits = 0
        layout_shift_hits = 0
        block_hits = 0
        anchor_mismatch_hits = 0
        anchor_not_found_hits = 0
        app_transition_hits = 0
        window_transition_hits = 0
        for row in feedback[-12:]:
            if not isinstance(row, dict):
                continue
            feedback_pre = cls._normalize_state_hash(row.get("pre_hash", ""))
            feedback_post = cls._normalize_state_hash(row.get("state_hash", row.get("post_hash", "")))
            if clean_pre and feedback_pre and clean_pre != feedback_pre:
                continue
            if clean_post and feedback_post and clean_post and feedback_post and clean_post != feedback_post:
                continue
            transition_count += 1
            tags_raw = row.get("reason_tags", [])
            tags = [str(item).strip().lower() for item in tags_raw if str(item).strip()] if isinstance(tags_raw, list) else []
            if any(tag in {"window_transition", "app_transition", "guardrail_context_shift"} for tag in tags):
                context_shift_hits += 1
            if any(tag in {"app_transition", "focus_app_changed"} for tag in tags):
                app_transition_hits += 1
            if any(tag in {"window_transition", "window_title_changed"} for tag in tags):
                window_transition_hits += 1
            if any(tag in {"anchor_mismatch", "target_mismatch", "element_mismatch", "pre_state_anchor_mismatch"} for tag in tags):
                anchor_mismatch_hits += 1
            if any(tag in {"anchor_not_found", "target_not_found", "no_target_match", "element_not_found"} for tag in tags):
                anchor_not_found_hits += 1
            status_text = str(row.get("status", "")).strip().lower()
            decision_text = str(row.get("decision", "")).strip().lower()
            if bool(row.get("blocked", False)) or status_text in {"blocked", "error"} or decision_text in {"block", "abort"}:
                block_hits += 1
            changed_paths_raw = row.get("changed_paths", [])
            changed_paths = [str(item).strip().lower() for item in changed_paths_raw if str(item).strip()] if isinstance(changed_paths_raw, list) else []
            changed_paths_total += len(changed_paths)
            if any(path.startswith("visual.") or "screen_hash" in path for path in changed_paths):
                visual_shift_hits += 1
            if any(path.startswith("layout.") or ".bounds" in path or path.endswith(".bounds") for path in changed_paths):
                layout_shift_hits += 1
        denom = max(1.0, float(transition_count))
        block_ratio = min(1.0, float(block_hits) / denom)
        anchor_mismatch_ratio = min(1.0, float(anchor_mismatch_hits) / denom)
        anchor_not_found_ratio = min(1.0, float(anchor_not_found_hits) / denom)
        churn_score = cls._coerce_float(
            (
                (min(1.0, float(changed_paths_total) / 24.0) * 0.34)
                + (min(1.0, float(context_shift_hits) / 4.0) * 0.2)
                + (min(1.0, float(visual_shift_hits) / 4.0) * 0.12)
                + (min(1.0, float(layout_shift_hits) / 4.0) * 0.1)
                + (anchor_mismatch_ratio * 0.12)
                + (anchor_not_found_ratio * 0.06)
                + (block_ratio * 0.06)
            ),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        return {
            "feedback_matches": transition_count,
            "changed_paths_total": changed_paths_total,
            "context_shift_hits": context_shift_hits,
            "visual_shift_hits": visual_shift_hits,
            "layout_shift_hits": layout_shift_hits,
            "block_hits": block_hits,
            "anchor_mismatch_hits": anchor_mismatch_hits,
            "anchor_not_found_hits": anchor_not_found_hits,
            "app_transition_hits": app_transition_hits,
            "window_transition_hits": window_transition_hits,
            "block_ratio": round(block_ratio, 6),
            "anchor_mismatch_ratio": round(anchor_mismatch_ratio, 6),
            "anchor_not_found_ratio": round(anchor_not_found_ratio, 6),
            "churn_score": churn_score,
        }

    @classmethod
    def _make_transition_key(cls, *, pre_state_hash: str, post_state_hash: str) -> str:
        pre_hash = cls._normalize_state_hash(pre_state_hash)
        post_hash = cls._normalize_state_hash(post_state_hash)
        if not pre_hash or not post_hash:
            return ""
        return f"{pre_hash[:24]}->{post_hash[:24]}"

    @classmethod
    def _normalize_transition_signature(cls, value: object) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        state_transition_match = re.fullmatch(r"([a-f0-9]{16,128})->([a-f0-9]{16,128})", text)
        if state_transition_match:
            return f"{state_transition_match.group(1)[:24]}->{state_transition_match.group(2)[:24]}"
        if len(text) <= 220:
            return text
        return text[:220]

    @staticmethod
    def _normalize_state_hash(value: object) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        if not re.fullmatch(r"[a-f0-9]{16,128}", text):
            return ""
        return text

    def _load_state_profile(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        profile_raw = raw if isinstance(raw, dict) else {}
        out: Dict[str, Dict[str, Any]] = {}
        for state_hash, row in profile_raw.items():
            normalized_hash = self._normalize_state_hash(state_hash)
            if not normalized_hash or not isinstance(row, dict):
                continue
            out[normalized_hash] = {
                "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": self._coerce_int(row.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": self._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        return out

    @classmethod
    def _load_transition_profile(cls, raw: Any) -> Dict[str, Dict[str, Any]]:
        profile_raw = raw if isinstance(raw, dict) else {}
        out: Dict[str, Dict[str, Any]] = {}
        for transition_key, row in profile_raw.items():
            normalized_key = cls._normalize_transition_signature(transition_key)
            if not normalized_key or not isinstance(row, dict):
                continue
            out[normalized_key] = {
                "samples": cls._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": cls._coerce_int(row.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": cls._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "success_rate": cls._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "guardrail_churn_ema": cls._coerce_float(row.get("guardrail_churn_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "guardrail_block_ema": cls._coerce_float(row.get("guardrail_block_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "anchor_mismatch_ema": cls._coerce_float(row.get("anchor_mismatch_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "layout_shift_ema": cls._coerce_float(row.get("layout_shift_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "anchor_not_found_ema": cls._coerce_float(row.get("anchor_not_found_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "signature": cls._normalize_transition_signature(row.get("signature", "")),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        return out

    @staticmethod
    def _public_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": str(row.get("key", "")).strip(),
            "action": str(row.get("action", "")).strip(),
            "query": str(row.get("query", "")).strip(),
            "app": str(row.get("app", "")).strip(),
            "window_title": str(row.get("window_title", "")).strip(),
            "control_type": str(row.get("control_type", "")).strip(),
            "target_mode": str(row.get("target_mode", "")).strip(),
            "element_id": str(row.get("element_id", "")).strip(),
            "x": DesktopAnchorMemory._coerce_optional_int(row.get("x")),
            "y": DesktopAnchorMemory._coerce_optional_int(row.get("y")),
            "samples": DesktopAnchorMemory._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
            "successes": DesktopAnchorMemory._coerce_int(row.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
            "failures": DesktopAnchorMemory._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
            "success_rate": DesktopAnchorMemory._coerce_float(row.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "consecutive_failures": DesktopAnchorMemory._coerce_int(
                row.get("consecutive_failures", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            ),
            "confidence_ema": DesktopAnchorMemory._coerce_float(row.get("confidence_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "last_pre_state_hash": str(row.get("last_pre_state_hash", "")).strip().lower(),
            "last_post_state_hash": str(row.get("last_post_state_hash", "")).strip().lower(),
            "state_profile_size": len(
                row.get("state_profile", {})
                if isinstance(row.get("state_profile", {}), dict)
                else {}
            ),
            "state_profile": DesktopAnchorMemory._public_state_profile(
                row.get("state_profile", {}) if isinstance(row.get("state_profile", {}), dict) else {}
            ),
            "last_transition_signature": DesktopAnchorMemory._normalize_transition_signature(
                row.get("last_transition_signature", "")
            ),
            "transition_profile_size": len(
                row.get("transition_profile", {})
                if isinstance(row.get("transition_profile", {}), dict)
                else {}
            ),
            "transition_profile": DesktopAnchorMemory._public_transition_profile(
                row.get("transition_profile", {}) if isinstance(row.get("transition_profile", {}), dict) else {}
            ),
            "last_status": str(row.get("last_status", "")).strip().lower(),
            "last_error": str(row.get("last_error", "")).strip(),
            "updated_at": str(row.get("updated_at", "")).strip(),
            "match_score": DesktopAnchorMemory._coerce_float(row.get("match_score", 0.0), minimum=0.0, maximum=5.0, default=0.0),
            "raw_match_score": DesktopAnchorMemory._coerce_float(row.get("raw_match_score", 0.0), minimum=0.0, maximum=5.0, default=0.0),
            "viability_score": DesktopAnchorMemory._coerce_float(row.get("viability_score", 1.0), minimum=0.0, maximum=1.0, default=1.0),
            "risk_score": DesktopAnchorMemory._coerce_float(row.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "viability_policy": str(row.get("viability_policy", "use")).strip().lower(),
            "invalidation_flags": [
                str(item).strip().lower()
                for item in (row.get("invalidation_flags", []) if isinstance(row.get("invalidation_flags", []), list) else [])
                if str(item).strip()
            ][:16],
        }

    @staticmethod
    def _public_state_profile(profile: Dict[str, Any], *, limit: int = 10) -> Dict[str, Dict[str, Any]]:
        rows = profile if isinstance(profile, dict) else {}
        bounded = DesktopAnchorMemory._coerce_int(limit, minimum=1, maximum=40, default=10)
        normalized: Dict[str, Dict[str, Any]] = {}
        for key, value in rows.items():
            state_hash = str(key or "").strip().lower()
            if not state_hash or not isinstance(value, dict):
                continue
            samples = DesktopAnchorMemory._coerce_int(
                value.get("samples", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            )
            successes = DesktopAnchorMemory._coerce_int(
                value.get("successes", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            )
            normalized[state_hash] = {
                "samples": samples,
                "success_rate": round(max(0.0, min(1.0, float(successes) / max(1.0, float(samples)))), 6),
                "updated_at": str(value.get("updated_at", "")).strip(),
            }
        if len(normalized) <= bounded:
            return normalized
        ordered = sorted(
            normalized.items(),
            key=lambda item: (
                str(item[1].get("updated_at", "")),
                int(item[1].get("samples", 0) or 0),
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(payload) for name, payload in ordered[:bounded]}

    @staticmethod
    def _public_transition_profile(profile: Dict[str, Any], *, limit: int = 8) -> Dict[str, Dict[str, Any]]:
        rows = profile if isinstance(profile, dict) else {}
        bounded = DesktopAnchorMemory._coerce_int(limit, minimum=1, maximum=40, default=8)
        normalized: Dict[str, Dict[str, Any]] = {}
        for key, value in rows.items():
            transition_key = DesktopAnchorMemory._normalize_transition_signature(key)
            if not transition_key or not isinstance(value, dict):
                continue
            normalized[transition_key] = {
                "samples": DesktopAnchorMemory._coerce_int(value.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "success_rate": DesktopAnchorMemory._coerce_float(value.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "guardrail_churn_ema": DesktopAnchorMemory._coerce_float(
                    value.get("guardrail_churn_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "guardrail_block_ema": DesktopAnchorMemory._coerce_float(
                    value.get("guardrail_block_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "anchor_mismatch_ema": DesktopAnchorMemory._coerce_float(
                    value.get("anchor_mismatch_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "layout_shift_ema": DesktopAnchorMemory._coerce_float(
                    value.get("layout_shift_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "anchor_not_found_ema": DesktopAnchorMemory._coerce_float(
                    value.get("anchor_not_found_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "signature": DesktopAnchorMemory._normalize_transition_signature(value.get("signature", "")),
                "updated_at": str(value.get("updated_at", "")).strip(),
            }
        if len(normalized) <= bounded:
            return normalized
        ordered = sorted(
            normalized.items(),
            key=lambda item: (
                str(item[1].get("updated_at", "")),
                int(item[1].get("samples", 0) or 0),
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(payload) for name, payload in ordered[:bounded]}

    def _prune_quarantine_locked(self, *, now_ts: float) -> None:
        keep: Dict[str, Dict[str, Any]] = {}
        for key, row in self._quarantine.items():
            if not isinstance(row, dict):
                continue
            expires_at = str(row.get("expires_at", "")).strip()
            expires_ts = self._to_timestamp(expires_at)
            if expires_ts > 0 and expires_ts <= now_ts:
                continue
            clean_key = str(key or "").strip()
            if not clean_key:
                continue
            keep[clean_key] = row
        self._quarantine = keep

    @staticmethod
    def _public_quarantine_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": str(row.get("key", "")).strip(),
            "action": str(row.get("action", "")).strip(),
            "query": str(row.get("query", "")).strip(),
            "app": str(row.get("app", "")).strip(),
            "window_title": str(row.get("window_title", "")).strip(),
            "control_type": str(row.get("control_type", "")).strip(),
            "target_mode": str(row.get("target_mode", "")).strip(),
            "reason": str(row.get("reason", "")).strip(),
            "severity": str(row.get("severity", "")).strip().lower(),
            "signals": [
                str(item).strip().lower()
                for item in (row.get("signals", []) if isinstance(row.get("signals", []), list) else [])
                if str(item).strip()
            ],
            "hits": DesktopAnchorMemory._coerce_int(row.get("hits", 0), minimum=0, maximum=10_000_000, default=0),
            "updated_at": str(row.get("updated_at", "")).strip(),
            "expires_at": str(row.get("expires_at", "")).strip(),
        }

    @staticmethod
    def _to_timestamp(value: object) -> float:
        text = str(value or "").strip()
        if not text:
            return 0.0
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    @staticmethod
    def _coerce_int(value: object, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: object, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_optional_int(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _read_path(payload: Any, path: str) -> Any:
        current = payload
        for token in str(path or "").split("."):
            if isinstance(current, dict):
                if token not in current:
                    return None
                current = current[token]
                continue
            if isinstance(current, list):
                try:
                    index = int(token)
                except Exception:
                    return None
                if index < 0 or index >= len(current):
                    return None
                current = current[index]
                continue
            return None
        return current
