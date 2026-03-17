from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class DesktopWorkflowMemory:
    _DEFAULT_INSTANCE: "DesktopWorkflowMemory | None" = None
    _DEFAULT_LOCK = RLock()
    _LEARNED_OVERRIDE_FIELDS = (
        "ensure_app_launch",
        "focus_first",
        "press_enter",
        "target_mode",
        "verify_mode",
        "verify_after_action",
        "retry_on_verification_failure",
        "max_strategy_attempts",
    )
    _EXAMPLE_LIMIT = 6

    def __init__(
        self,
        *,
        store_path: str = "data/desktop_workflow_memory.json",
        max_entries: int = 4000,
        max_variants_per_entry: int = 12,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_entries = self._coerce_int(max_entries, minimum=100, maximum=100_000, default=4000)
        self.max_variants_per_entry = self._coerce_int(max_variants_per_entry, minimum=2, maximum=64, default=12)
        self._lock = RLock()
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._load()

    @classmethod
    def default(cls) -> "DesktopWorkflowMemory":
        with cls._DEFAULT_LOCK:
            if cls._DEFAULT_INSTANCE is None:
                cls._DEFAULT_INSTANCE = cls()
            return cls._DEFAULT_INSTANCE

    def recommend(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        app_profile: Dict[str, Any] | None,
        variants: List[Dict[str, Any]] | None,
        skill_profile: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        rows = [dict(row) for row in (variants or []) if isinstance(row, dict)]
        if not rows:
            return {
                "status": "skip",
                "applied": False,
                "variants": [],
                "ranking": [],
                "skill_profile": skill_profile if isinstance(skill_profile, dict) else {},
            }
        key = self._entry_key(action=action, args=args, app_profile=app_profile)
        with self._lock:
            entry = dict(self._entries.get(key, {}))
        stats_by_signature = entry.get("variants", {}) if isinstance(entry.get("variants", {}), dict) else {}
        effective_skill = skill_profile if isinstance(skill_profile, dict) else self.skill_profile(
            action=action,
            args=args,
            app_profile=app_profile,
        )
        recommended_overrides = (
            dict(effective_skill.get("recommended_overrides", {}))
            if isinstance(effective_skill.get("recommended_overrides", {}), dict)
            else {}
        )
        if not bool(effective_skill.get("should_apply", False)):
            recommended_overrides = {}
        ranking: List[Dict[str, Any]] = []
        original_signatures: List[str] = []
        for index, variant in enumerate(rows):
            signature = self._variant_signature(variant)
            original_signatures.append(signature)
            stats = stats_by_signature.get(signature, {}) if isinstance(stats_by_signature.get(signature, {}), dict) else {}
            samples = self._coerce_int(stats.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
            execution_successes = self._coerce_int(stats.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0)
            verified_successes = self._coerce_int(stats.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0)
            failures = self._coerce_int(stats.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
            consecutive_failures = self._coerce_int(stats.get("consecutive_failures", 0), minimum=0, maximum=10_000_000, default=0)
            execution_rate = float(execution_successes) / max(1.0, float(samples)) if samples else 0.0
            verified_rate = float(verified_successes) / max(1.0, float(samples)) if samples else 0.0
            failure_rate = float(failures) / max(1.0, float(samples)) if samples else 0.0
            base_score = 0.58 if index == 0 else 0.46
            alignment_score = self._variant_alignment_score(variant=variant, recommended_overrides=recommended_overrides)
            score = (
                base_score
                + (verified_rate * 0.42)
                + (execution_rate * 0.18)
                + min(0.12, float(samples) * 0.02)
                - min(0.24, failure_rate * 0.24)
                - min(0.12, float(consecutive_failures) * 0.03)
                + alignment_score
            )
            if bool(stats.get("last_verified", False)):
                score += 0.04
            if index > 0 and verified_successes > 0:
                score += 0.03
            ranking.append(
                {
                    "strategy_id": str(variant.get("strategy_id", "") or "").strip(),
                    "signature": signature,
                    "score": round(max(0.0, min(score, 1.5)), 6),
                    "samples": samples,
                    "execution_successes": execution_successes,
                    "verified_successes": verified_successes,
                    "failures": failures,
                    "consecutive_failures": consecutive_failures,
                    "execution_rate": round(execution_rate, 6),
                    "verified_rate": round(verified_rate, 6),
                    "last_verified": bool(stats.get("last_verified", False)),
                    "alignment_score": round(alignment_score, 6),
                }
            )
        ranking.sort(
            key=lambda item: (
                -self._coerce_float(item.get("score", 0.0), minimum=0.0, maximum=10.0, default=0.0),
                -self._coerce_int(item.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
                -self._coerce_int(item.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                str(item.get("signature", "")),
            )
        )
        ranked_variants: List[Dict[str, Any]] = []
        by_signature = {self._variant_signature(variant): dict(variant) for variant in rows}
        for item in ranking:
            signature = str(item.get("signature", "") or "").strip()
            variant = by_signature.get(signature)
            if not isinstance(variant, dict):
                continue
            variant["adaptive_score"] = item.get("score", 0.0)
            variant["adaptive_samples"] = item.get("samples", 0)
            variant["adaptive_verified_rate"] = item.get("verified_rate", 0.0)
            variant["adaptive_alignment_score"] = item.get("alignment_score", 0.0)
            ranked_variants.append(variant)
        if not ranked_variants:
            ranked_variants = rows
        best = ranking[0] if ranking else {}
        original_primary_signature = original_signatures[0] if original_signatures else ""
        original_primary = next((item for item in ranking if str(item.get("signature", "") or "") == original_primary_signature), {})
        best_signature = str(best.get("signature", "") or "").strip()
        best_samples = self._coerce_int(best.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        best_verified_successes = self._coerce_int(best.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0)
        best_score = self._coerce_float(best.get("score", 0.0), minimum=0.0, maximum=10.0, default=0.0)
        original_primary_score = self._coerce_float(original_primary.get("score", 0.0), minimum=0.0, maximum=10.0, default=0.0)
        applied = bool(
            best_signature
            and best_signature != original_primary_signature
            and best_samples >= 1
            and best_verified_successes >= 1
            and best_score >= (original_primary_score + 0.08)
        )
        if not applied:
            ranked_variants = rows
        return {
            "status": "success",
            "key": key,
            "applied": applied,
            "recommended_strategy_id": str(best.get("strategy_id", "") or "").strip(),
            "recommended_signature": best_signature,
            "variants": ranked_variants,
            "ranking": ranking,
            "skill_profile": effective_skill if isinstance(effective_skill, dict) else {},
        }

    def skill_profile(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        app_profile: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        key = self._entry_key(action=action, args=args, app_profile=app_profile)
        clean_action = self._normalize_text(action)
        profile_id = self._profile_id(app_profile)
        app_hint = self._normalize_text((args or {}).get("app_name", "")) or self._normalize_text((args or {}).get("window_title", ""))
        intent_hint = self._intent_signature(args)
        with self._lock:
            rows = [dict(row) for row in self._entries.values()]
            exact_entry = dict(self._entries.get(key, {})) if isinstance(self._entries.get(key, {}), dict) else {}
        candidates: List[Dict[str, Any]] = []
        if exact_entry:
            exact_profile = self._build_skill_profile_from_rows([exact_entry], scope="exact")
            exact_profile["key"] = key
            candidates.append(exact_profile)
        if app_hint:
            intent_rows = [
                row
                for row in rows
                if self._normalize_text(row.get("action", "")) == clean_action
                and self._normalize_text(row.get("intent", "")) == intent_hint
                and (
                    app_hint in self._normalize_text(row.get("app_name", ""))
                    or app_hint in self._normalize_text(row.get("window_title", ""))
                )
            ]
            if intent_rows:
                candidates.append(self._build_skill_profile_from_rows(intent_rows, scope="intent"))
            app_rows = [
                row
                for row in rows
                if self._normalize_text(row.get("action", "")) == clean_action
                and self._normalize_text(row.get("profile_id", "")) == profile_id
                and (
                    app_hint in self._normalize_text(row.get("app_name", ""))
                    or app_hint in self._normalize_text(row.get("window_title", ""))
                )
            ]
            if not app_rows:
                app_rows = [
                    row
                    for row in rows
                    if self._normalize_text(row.get("action", "")) == clean_action
                    and (
                        app_hint in self._normalize_text(row.get("app_name", ""))
                        or app_hint in self._normalize_text(row.get("window_title", ""))
                    )
                ]
            if app_rows:
                candidates.append(self._build_skill_profile_from_rows(app_rows, scope="app"))
        profile_rows = [
            row
            for row in rows
            if self._normalize_text(row.get("action", "")) == clean_action
            and self._normalize_text(row.get("profile_id", "")) == profile_id
        ]
        if profile_rows:
            candidates.append(self._build_skill_profile_from_rows(profile_rows, scope="profile"))
        if not candidates:
            return {
                "status": "insufficient_history",
                "scope": "",
                "confidence": 0.0,
                "should_apply": False,
                "recommended_overrides": {},
                "preferred_route_mode": "",
                "recovery_bias": {},
                "query_examples": [],
                "text_examples": [],
                "matched_entries": 0,
                "samples": 0,
                "execution_successes": 0,
                "verified_successes": 0,
                "matched_scopes": [],
            }
        chosen = self._select_best_skill_profile(candidates)
        return {
            **chosen,
            "matched_scopes": [self._strip_nested_scope(skill) for skill in candidates],
        }

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        app_profile: Dict[str, Any] | None,
        strategy: Dict[str, Any] | None,
        attempt: Dict[str, Any] | None,
        advice: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        strategy_payload = strategy if isinstance(strategy, dict) else {}
        attempt_payload = attempt if isinstance(attempt, dict) else {}
        advice_payload = advice if isinstance(advice, dict) else {}
        key = self._entry_key(action=action, args=args, app_profile=app_profile)
        signature = self._variant_signature(strategy_payload)
        status = str(attempt_payload.get("status", "") or "").strip().lower()
        verification = attempt_payload.get("verification", {}) if isinstance(attempt_payload.get("verification", {}), dict) else {}
        verified = bool(verification.get("verified", False))
        success = status == "success"
        recovered = self._coerce_int(attempt_payload.get("attempt", 1), minimum=1, maximum=1000, default=1) > 1
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            entry = dict(self._entries.get(key, {}))
            variants = entry.get("variants", {}) if isinstance(entry.get("variants", {}), dict) else {}
            row = dict(variants.get(signature, {}))
            samples = self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
            execution_successes = self._coerce_int(row.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0)
            verified_successes = self._coerce_int(row.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0)
            failures = self._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
            consecutive_failures = self._coerce_int(row.get("consecutive_failures", 0), minimum=0, maximum=10_000_000, default=0)
            if success:
                execution_successes += 1
            if success and verified:
                verified_successes += 1
                consecutive_failures = 0
            else:
                failures += 1
                consecutive_failures += 1
            row.update(
                {
                    "signature": signature,
                    "strategy_id": str(strategy_payload.get("strategy_id", "") or "").strip(),
                    "title": str(strategy_payload.get("title", "") or "").strip(),
                    "payload_overrides": dict(strategy_payload.get("payload_overrides", {})) if isinstance(strategy_payload.get("payload_overrides", {}), dict) else {},
                    "samples": samples,
                    "execution_successes": execution_successes,
                    "verified_successes": verified_successes,
                    "failures": failures,
                    "consecutive_failures": consecutive_failures,
                    "last_status": status,
                    "last_verified": verified,
                    "updated_at": now,
                }
            )
            variants[signature] = row

            metrics = self._entry_metrics(entry)
            metrics["samples"] = self._coerce_int(metrics.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if success:
                metrics["execution_successes"] = self._coerce_int(metrics.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if success and verified:
                metrics["verified_successes"] = self._coerce_int(metrics.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0) + 1
            else:
                metrics["failures"] = self._coerce_int(metrics.get("failures", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if status == "blocked":
                metrics["blocked_count"] = self._coerce_int(metrics.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if status == "error":
                metrics["error_count"] = self._coerce_int(metrics.get("error_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if status == "partial":
                metrics["partial_count"] = self._coerce_int(metrics.get("partial_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            verification_enabled = bool(verification.get("enabled", False))
            if success and verification_enabled and not verified:
                metrics["verification_failures"] = self._coerce_int(metrics.get("verification_failures", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if success and verified and recovered:
                metrics["recovered_successes"] = self._coerce_int(metrics.get("recovered_successes", 0), minimum=0, maximum=10_000_000, default=0) + 1

            learned_defaults = self._normalize_learned_defaults(entry.get("learned_defaults", {}))
            query_example_counts = self._normalize_count_map(entry.get("query_example_counts", {}))
            text_example_counts = self._normalize_count_map(entry.get("text_example_counts", {}))
            if success and verified:
                route_mode = self._normalize_text(advice_payload.get("route_mode", ""))
                surface_snapshot = advice_payload.get("surface_snapshot", {}) if isinstance(advice_payload.get("surface_snapshot", {}), dict) else {}
                surface_intelligence = (
                    surface_snapshot.get("surface_intelligence", {})
                    if isinstance(surface_snapshot.get("surface_intelligence", {}), dict)
                    else {}
                )
                surface_role = self._normalize_text(surface_intelligence.get("surface_role", ""))
                interaction_mode = self._normalize_text(surface_intelligence.get("interaction_mode", ""))
                self._increment_count(metrics.setdefault("route_mode_counts", {}), route_mode)
                self._increment_count(metrics.setdefault("surface_role_counts", {}), surface_role)
                self._increment_count(metrics.setdefault("interaction_mode_counts", {}), interaction_mode)
                payload = args if isinstance(args, dict) else {}
                for field_name in self._LEARNED_OVERRIDE_FIELDS:
                    if field_name not in payload:
                        continue
                    encoded_value = self._encode_learning_value(payload.get(field_name))
                    field_counts = learned_defaults.setdefault(field_name, {})
                    field_counts[encoded_value] = self._coerce_int(field_counts.get(encoded_value, 0), minimum=0, maximum=10_000_000, default=0) + 1
                    learned_defaults[field_name] = self._trim_count_map(field_counts, limit=8)
                query_text = self._normalize_text(payload.get("query", ""))
                typed_text = self._normalize_text(payload.get("text", ""))
                self._increment_count(query_example_counts, query_text)
                self._increment_count(text_example_counts, typed_text)
                query_example_counts = self._trim_count_map(query_example_counts, limit=self._EXAMPLE_LIMIT)
                text_example_counts = self._trim_count_map(text_example_counts, limit=self._EXAMPLE_LIMIT)

            entry.update(
                {
                    "key": key,
                    "action": str(action or "").strip().lower(),
                    "profile_id": self._profile_id(app_profile),
                    "profile_category": self._normalize_text((app_profile or {}).get("category", "")),
                    "app_name": self._normalize_text((args or {}).get("app_name", "")),
                    "window_title": self._normalize_text((args or {}).get("window_title", "")),
                    "intent": self._intent_signature(args),
                    "variants": variants,
                    "metrics": self._serialize_metrics(metrics),
                    "learned_defaults": learned_defaults,
                    "query_example_counts": query_example_counts,
                    "text_example_counts": text_example_counts,
                    "updated_at": now,
                }
            )
            entry["skill_profile"] = self._strip_nested_scope(self._build_skill_profile_from_rows([entry], scope="exact"))
            self._entries[key] = entry
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=bool(success and verified))
        return {
            "status": "success",
            "key": key,
            "signature": signature,
            "samples": samples,
            "verified": verified,
            "skill_profile": entry.get("skill_profile", {}) if isinstance(entry.get("skill_profile", {}), dict) else {},
        }

    def _entry_metrics(self, row: Dict[str, Any]) -> Dict[str, Any]:
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics", {}), dict) else {}
        normalized = {
            "samples": self._coerce_int(metrics.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
            "execution_successes": self._coerce_int(metrics.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0),
            "verified_successes": self._coerce_int(metrics.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
            "failures": self._coerce_int(metrics.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
            "blocked_count": self._coerce_int(metrics.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
            "error_count": self._coerce_int(metrics.get("error_count", 0), minimum=0, maximum=10_000_000, default=0),
            "partial_count": self._coerce_int(metrics.get("partial_count", 0), minimum=0, maximum=10_000_000, default=0),
            "verification_failures": self._coerce_int(metrics.get("verification_failures", 0), minimum=0, maximum=10_000_000, default=0),
            "recovered_successes": self._coerce_int(metrics.get("recovered_successes", 0), minimum=0, maximum=10_000_000, default=0),
            "route_mode_counts": self._normalize_count_map(metrics.get("route_mode_counts", {})),
            "surface_role_counts": self._normalize_count_map(metrics.get("surface_role_counts", {})),
            "interaction_mode_counts": self._normalize_count_map(metrics.get("interaction_mode_counts", {})),
        }
        if normalized["samples"] > 0:
            return normalized
        variants = row.get("variants", {}) if isinstance(row.get("variants", {}), dict) else {}
        derived = dict(normalized)
        for variant in variants.values():
            if not isinstance(variant, dict):
                continue
            derived["samples"] += self._coerce_int(variant.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
            derived["execution_successes"] += self._coerce_int(variant.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0)
            derived["verified_successes"] += self._coerce_int(variant.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0)
            derived["failures"] += self._coerce_int(variant.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
        return derived

    def _serialize_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "samples": self._coerce_int(metrics.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
            "execution_successes": self._coerce_int(metrics.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0),
            "verified_successes": self._coerce_int(metrics.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
            "failures": self._coerce_int(metrics.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
            "blocked_count": self._coerce_int(metrics.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
            "error_count": self._coerce_int(metrics.get("error_count", 0), minimum=0, maximum=10_000_000, default=0),
            "partial_count": self._coerce_int(metrics.get("partial_count", 0), minimum=0, maximum=10_000_000, default=0),
            "verification_failures": self._coerce_int(metrics.get("verification_failures", 0), minimum=0, maximum=10_000_000, default=0),
            "recovered_successes": self._coerce_int(metrics.get("recovered_successes", 0), minimum=0, maximum=10_000_000, default=0),
            "route_mode_counts": self._normalize_count_map(metrics.get("route_mode_counts", {})),
            "surface_role_counts": self._normalize_count_map(metrics.get("surface_role_counts", {})),
            "interaction_mode_counts": self._normalize_count_map(metrics.get("interaction_mode_counts", {})),
        }

    def _build_skill_profile_from_rows(self, rows: List[Dict[str, Any]], *, scope: str) -> Dict[str, Any]:
        aggregate_metrics = self._aggregate_metrics(rows)
        learned_defaults = self._aggregate_learned_defaults(rows)
        query_examples = self._top_examples(self._aggregate_example_counts(rows, "query_example_counts"))
        text_examples = self._top_examples(self._aggregate_example_counts(rows, "text_example_counts"))
        samples = self._coerce_int(aggregate_metrics.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        execution_successes = self._coerce_int(aggregate_metrics.get("execution_successes", 0), minimum=0, maximum=10_000_000, default=0)
        verified_successes = self._coerce_int(aggregate_metrics.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0)
        failures = self._coerce_int(aggregate_metrics.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
        recommended_overrides: Dict[str, Any] = {}
        reinforcement_scores: List[float] = []
        minimum_verified = {"exact": 2, "intent": 2, "app": 3, "profile": 4}.get(scope, 3)
        minimum_override_votes = {"exact": 2, "intent": 2, "app": 3, "profile": 3}.get(scope, 3)
        for field_name, field_counts in learned_defaults.items():
            if field_name not in self._LEARNED_OVERRIDE_FIELDS or not isinstance(field_counts, dict):
                continue
            total_votes = sum(self._coerce_int(count, minimum=0, maximum=10_000_000, default=0) for count in field_counts.values())
            if total_votes <= 0:
                continue
            best_key, best_count = max(
                field_counts.items(),
                key=lambda item: self._coerce_int(item[1], minimum=0, maximum=10_000_000, default=0),
            )
            dominant_votes = self._coerce_int(best_count, minimum=0, maximum=10_000_000, default=0)
            dominance = float(dominant_votes) / max(1.0, float(total_votes))
            if dominant_votes >= minimum_override_votes and dominance >= 0.67:
                recommended_overrides[field_name] = self._decode_learning_value(best_key)
                reinforcement_scores.append(dominance)
        preferred_route_mode = self._top_count_key(aggregate_metrics.get("route_mode_counts", {}), minimum=minimum_override_votes)
        preferred_surface_role = self._top_count_key(aggregate_metrics.get("surface_role_counts", {}), minimum=minimum_override_votes)
        preferred_interaction_mode = self._top_count_key(aggregate_metrics.get("interaction_mode_counts", {}), minimum=minimum_override_votes)
        execution_rate = float(execution_successes) / max(1.0, float(samples)) if samples else 0.0
        verified_rate = float(verified_successes) / max(1.0, float(samples)) if samples else 0.0
        failure_rate = float(failures) / max(1.0, float(samples)) if samples else 0.0
        dominance_score = sum(reinforcement_scores) / len(reinforcement_scores) if reinforcement_scores else 0.0
        confidence = 0.0
        if samples:
            confidence = (
                0.18
                + min(0.22, float(verified_successes) * 0.04)
                + (verified_rate * 0.34)
                + (execution_rate * 0.12)
                + (dominance_score * 0.18)
                - min(0.16, failure_rate * 0.16)
            )
        confidence = round(max(0.0, min(confidence, 0.98)), 4)
        should_apply = bool(recommended_overrides) and verified_successes >= minimum_verified and confidence >= 0.58
        status = "learned" if should_apply else ("observed" if samples else "insufficient_history")
        return {
            "status": status,
            "scope": scope,
            "confidence": confidence,
            "should_apply": should_apply,
            "recommended_overrides": recommended_overrides,
            "preferred_route_mode": preferred_route_mode,
            "preferred_surface_role": preferred_surface_role,
            "preferred_interaction_mode": preferred_interaction_mode,
            "recovery_bias": {
                "prefer_retry": bool(
                    self._coerce_int(aggregate_metrics.get("recovered_successes", 0), minimum=0, maximum=10_000_000, default=0)
                    or self._coerce_int(aggregate_metrics.get("verification_failures", 0), minimum=0, maximum=10_000_000, default=0)
                ),
                "prefer_verification": execution_successes > verified_successes,
                "verified_execution_gap": max(0, execution_successes - verified_successes),
            },
            "query_examples": query_examples,
            "text_examples": text_examples,
            "matched_entries": len(rows),
            "samples": samples,
            "execution_successes": execution_successes,
            "verified_successes": verified_successes,
            "failures": failures,
            "route_mode_counts": self._normalize_count_map(aggregate_metrics.get("route_mode_counts", {})),
            "surface_role_counts": self._normalize_count_map(aggregate_metrics.get("surface_role_counts", {})),
            "interaction_mode_counts": self._normalize_count_map(aggregate_metrics.get("interaction_mode_counts", {})),
        }

    def _aggregate_metrics(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        aggregate = {
            "samples": 0,
            "execution_successes": 0,
            "verified_successes": 0,
            "failures": 0,
            "blocked_count": 0,
            "error_count": 0,
            "partial_count": 0,
            "verification_failures": 0,
            "recovered_successes": 0,
            "route_mode_counts": {},
            "surface_role_counts": {},
            "interaction_mode_counts": {},
        }
        for row in rows:
            metrics = self._entry_metrics(row)
            for field_name in (
                "samples",
                "execution_successes",
                "verified_successes",
                "failures",
                "blocked_count",
                "error_count",
                "partial_count",
                "verification_failures",
                "recovered_successes",
            ):
                aggregate[field_name] += self._coerce_int(metrics.get(field_name, 0), minimum=0, maximum=10_000_000, default=0)
            for count_field in ("route_mode_counts", "surface_role_counts", "interaction_mode_counts"):
                current = aggregate[count_field] if isinstance(aggregate.get(count_field, {}), dict) else {}
                for key, count in self._normalize_count_map(metrics.get(count_field, {})).items():
                    current[key] = self._coerce_int(current.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
                aggregate[count_field] = current
        return aggregate

    def _aggregate_learned_defaults(self, rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
        aggregate: Dict[str, Dict[str, int]] = {}
        for row in rows:
            learned_defaults = self._normalize_learned_defaults(row.get("learned_defaults", {}))
            for field_name, field_counts in learned_defaults.items():
                current = aggregate.setdefault(field_name, {})
                for key, count in field_counts.items():
                    current[key] = self._coerce_int(current.get(key, 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(
                        count,
                        minimum=0,
                        maximum=10_000_000,
                        default=0,
                    )
        return aggregate

    def _aggregate_example_counts(self, rows: List[Dict[str, Any]], field_name: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in rows:
            for key, count in self._normalize_count_map(row.get(field_name, {})).items():
                counts[key] = self._coerce_int(counts.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
        return counts

    def _select_best_skill_profile(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        priorities = {"exact": 4, "intent": 3, "app": 2, "profile": 1}
        ordered = [dict(candidate) for candidate in candidates if isinstance(candidate, dict)]
        ordered.sort(
            key=lambda item: (
                0 if bool(item.get("should_apply", False)) else 1,
                -priorities.get(str(item.get("scope", "")), 0),
                -self._coerce_float(item.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                -self._coerce_int(item.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
                -self._coerce_int(item.get("matched_entries", 0), minimum=0, maximum=10_000_000, default=0),
            )
        )
        return ordered[0] if ordered else {}

    def _snapshot_item(self, row: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(row)
        item["metrics"] = self._entry_metrics(row)
        item["skill_profile"] = (
            self._strip_nested_scope(item.get("skill_profile", {}))
            if isinstance(item.get("skill_profile", {}), dict) and item.get("skill_profile")
            else self._strip_nested_scope(self._build_skill_profile_from_rows([row], scope="exact"))
        )
        item["query_examples"] = self._top_examples(self._normalize_count_map(row.get("query_example_counts", {})))
        item["text_examples"] = self._top_examples(self._normalize_count_map(row.get("text_example_counts", {})))
        return item

    def _snapshot_summary(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        action_counts: Dict[str, int] = {}
        profile_counts: Dict[str, int] = {}
        aggregate_metrics = self._aggregate_metrics(rows)
        top_skills: List[Dict[str, Any]] = []
        for row in rows:
            action_key = self._normalize_text(row.get("action", ""))
            profile_key = self._normalize_text(row.get("profile_id", ""))
            self._increment_count(action_counts, action_key)
            self._increment_count(profile_counts, profile_key)
            skill = self._build_skill_profile_from_rows([row], scope="exact")
            if bool(skill.get("should_apply", False)):
                top_skills.append(
                    {
                        "key": str(row.get("key", "") or "").strip(),
                        "action": str(row.get("action", "") or "").strip(),
                        "profile_id": str(row.get("profile_id", "") or "").strip(),
                        "app_name": str(row.get("app_name", "") or "").strip(),
                        "intent": str(row.get("intent", "") or "").strip(),
                        "confidence": skill.get("confidence", 0.0),
                        "verified_successes": skill.get("verified_successes", 0),
                        "recommended_overrides": dict(skill.get("recommended_overrides", {}))
                        if isinstance(skill.get("recommended_overrides", {}), dict)
                        else {},
                    }
                )
        top_skills.sort(
            key=lambda item: (
                -self._coerce_float(item.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                -self._coerce_int(item.get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
                str(item.get("action", "")),
            )
        )
        return {
            "status": "success",
            "entry_count": len(rows),
            "action_counts": self._trim_count_map(action_counts, limit=24),
            "profile_counts": self._trim_count_map(profile_counts, limit=24),
            "route_mode_counts": self._trim_count_map(self._normalize_count_map(aggregate_metrics.get("route_mode_counts", {})), limit=24),
            "surface_role_counts": self._trim_count_map(self._normalize_count_map(aggregate_metrics.get("surface_role_counts", {})), limit=24),
            "interaction_mode_counts": self._trim_count_map(self._normalize_count_map(aggregate_metrics.get("interaction_mode_counts", {})), limit=24),
            "top_skills": top_skills[:10],
        }

    def _variant_alignment_score(self, *, variant: Dict[str, Any], recommended_overrides: Dict[str, Any]) -> float:
        overrides = variant.get("payload_overrides", {}) if isinstance(variant.get("payload_overrides", {}), dict) else {}
        if not overrides or not recommended_overrides:
            return 0.0
        bonus = 0.0
        penalty = 0.0
        for field_name, preferred_value in recommended_overrides.items():
            if field_name not in overrides:
                continue
            if self._values_equal(overrides.get(field_name), preferred_value):
                bonus += 0.03
            else:
                penalty += 0.05
        return round(max(-0.12, min(bonus - penalty, 0.12)), 6)

    def snapshot(
        self,
        *,
        limit: int = 200,
        action: str = "",
        app_name: str = "",
        profile_id: str = "",
        intent: str = "",
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        clean_action = self._normalize_text(action)
        clean_app_name = self._normalize_text(app_name)
        clean_profile_id = self._normalize_text(profile_id)
        clean_intent = self._normalize_text(intent)
        with self._lock:
            rows = [dict(row) for row in self._entries.values()]
        if clean_action:
            rows = [row for row in rows if self._normalize_text(row.get("action", "")) == clean_action]
        if clean_app_name:
            rows = [
                row
                for row in rows
                if clean_app_name in self._normalize_text(row.get("app_name", ""))
                or clean_app_name in self._normalize_text(row.get("window_title", ""))
            ]
        if clean_profile_id:
            rows = [row for row in rows if self._normalize_text(row.get("profile_id", "")) == clean_profile_id]
        if clean_intent:
            rows = [row for row in rows if clean_intent in self._normalize_text(row.get("intent", ""))]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        items = [self._snapshot_item(row) for row in rows[:bounded]]
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "items": items,
            "filters": {
                "action": clean_action,
                "app_name": clean_app_name,
                "profile_id": clean_profile_id,
                "intent": clean_intent,
            },
            "summary": self._snapshot_summary(rows),
        }

    def reset(
        self,
        *,
        action: str = "",
        app_name: str = "",
        profile_id: str = "",
        intent: str = "",
    ) -> Dict[str, Any]:
        clean_action = self._normalize_text(action)
        clean_app_name = self._normalize_text(app_name)
        clean_profile_id = self._normalize_text(profile_id)
        clean_intent = self._normalize_text(intent)
        with self._lock:
            removed = 0
            if not any((clean_action, clean_app_name, clean_profile_id, clean_intent)):
                removed = len(self._entries)
                self._entries = {}
            else:
                kept: Dict[str, Dict[str, Any]] = {}
                for key, row in self._entries.items():
                    action_match = bool(clean_action) and self._normalize_text(row.get("action", "")) == clean_action
                    app_match = bool(clean_app_name) and (
                        clean_app_name in self._normalize_text(row.get("app_name", ""))
                        or clean_app_name in self._normalize_text(row.get("window_title", ""))
                    )
                    profile_match = bool(clean_profile_id) and self._normalize_text(row.get("profile_id", "")) == clean_profile_id
                    intent_match = bool(clean_intent) and clean_intent in self._normalize_text(row.get("intent", ""))
                    if action_match or app_match or profile_match or intent_match:
                        removed += 1
                        continue
                    kept[key] = row
                self._entries = kept
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "action": clean_action,
                "app_name": clean_app_name,
                "profile_id": clean_profile_id,
                "intent": clean_intent,
            },
        }

    def _load(self) -> None:
        try:
            raw = self.store_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except Exception:
            return
        try:
            payload = json.loads(raw)
        except Exception:
            return
        entries = payload.get("entries", {}) if isinstance(payload, dict) else {}
        if isinstance(entries, dict):
            self._entries = {
                str(key).strip(): dict(value)
                for key, value in entries.items()
                if str(key).strip() and isinstance(value, dict)
            }

    def _maybe_save_locked(self, *, force: bool) -> None:
        if not force and self._updates_since_save < 8 and (time.monotonic() - self._last_save_monotonic) < 5.0:
            return
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": "2.0",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "entries": self._entries,
        }
        self.store_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        self._updates_since_save = 0
        self._last_save_monotonic = time.monotonic()

    def _trim_locked(self) -> None:
        if len(self._entries) > self.max_entries:
            ordered = sorted(
                self._entries.items(),
                key=lambda item: str(item[1].get("updated_at", "")),
                reverse=True,
            )
            self._entries = {key: value for key, value in ordered[: self.max_entries]}
        for key, row in list(self._entries.items()):
            variants = row.get("variants", {}) if isinstance(row.get("variants", {}), dict) else {}
            if len(variants) > self.max_variants_per_entry:
                ordered_variants = sorted(
                    variants.items(),
                    key=lambda item: (
                        self._coerce_int(item[1].get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
                        self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                        str(item[1].get("updated_at", "")),
                    ),
                    reverse=True,
                )
                row["variants"] = {
                    variant_key: variant_value for variant_key, variant_value in ordered_variants[: self.max_variants_per_entry]
                }
            row["query_example_counts"] = self._trim_count_map(
                self._normalize_count_map(row.get("query_example_counts", {})),
                limit=self._EXAMPLE_LIMIT,
            )
            row["text_example_counts"] = self._trim_count_map(
                self._normalize_count_map(row.get("text_example_counts", {})),
                limit=self._EXAMPLE_LIMIT,
            )
            learned_defaults = self._normalize_learned_defaults(row.get("learned_defaults", {}))
            row["learned_defaults"] = {
                field_name: self._trim_count_map(field_counts, limit=8)
                for field_name, field_counts in learned_defaults.items()
            }
            self._entries[key] = row

    def _entry_key(self, *, action: str, args: Dict[str, Any] | None, app_profile: Dict[str, Any] | None) -> str:
        payload = args if isinstance(args, dict) else {}
        parts = [
            self._normalize_text(action),
            self._profile_id(app_profile),
            self._normalize_text(payload.get("app_name", "")) or self._normalize_text(payload.get("window_title", "")) or "desktop",
            self._intent_signature(payload),
        ]
        return "|".join(parts)

    def _intent_signature(self, args: Dict[str, Any] | None) -> str:
        payload = args if isinstance(args, dict) else {}
        action = self._normalize_text(payload.get("action", ""))
        query = self._normalize_text(payload.get("query", ""))
        text = self._normalize_text(payload.get("text", ""))
        keys = payload.get("keys", [])
        key_signature = "+".join(self._normalize_text(item) for item in keys if self._normalize_text(item)) if isinstance(keys, list) else ""
        intent = query or text or key_signature or action or "generic"
        return intent[:160]

    @staticmethod
    def _profile_id(app_profile: Dict[str, Any] | None) -> str:
        profile = app_profile if isinstance(app_profile, dict) else {}
        return (
            DesktopWorkflowMemory._normalize_text(profile.get("profile_id", ""))
            or DesktopWorkflowMemory._normalize_text(profile.get("category", ""))
            or "generic"
        )

    @staticmethod
    def _variant_signature(strategy: Dict[str, Any] | None) -> str:
        payload = strategy if isinstance(strategy, dict) else {}
        overrides = payload.get("payload_overrides", {}) if isinstance(payload.get("payload_overrides", {}), dict) else {}
        if not overrides:
            return str(payload.get("strategy_id", "primary") or "primary").strip().lower()
        normalized: Dict[str, Any] = {}
        for key, value in sorted(overrides.items()):
            clean_key = str(key or "").strip().lower()
            if not clean_key:
                continue
            if isinstance(value, list):
                normalized[clean_key] = [str(item).strip().lower() for item in value if str(item).strip()]
            elif isinstance(value, bool):
                normalized[clean_key] = bool(value)
            else:
                normalized[clean_key] = str(value).strip().lower()
        return json.dumps(normalized, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _normalize_count_map(value: Any) -> Dict[str, int]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, int] = {}
        for raw_key, raw_count in value.items():
            clean_key = str(raw_key or "").strip()
            if not clean_key:
                continue
            count = DesktopWorkflowMemory._coerce_int(raw_count, minimum=0, maximum=10_000_000, default=0)
            if count <= 0:
                continue
            normalized[clean_key] = count
        return normalized

    @staticmethod
    def _normalize_learned_defaults(value: Any) -> Dict[str, Dict[str, int]]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, Dict[str, int]] = {}
        for field_name, field_counts in value.items():
            clean_field = str(field_name or "").strip()
            if not clean_field or not isinstance(field_counts, dict):
                continue
            bucket_counts = DesktopWorkflowMemory._normalize_count_map(field_counts)
            if bucket_counts:
                normalized[clean_field] = bucket_counts
        return normalized

    @staticmethod
    def _increment_count(target: Dict[str, int], key: str, amount: int = 1) -> None:
        clean_key = str(key or "").strip()
        if not clean_key:
            return
        target[clean_key] = DesktopWorkflowMemory._coerce_int(
            target.get(clean_key, 0),
            minimum=0,
            maximum=10_000_000,
            default=0,
        ) + max(1, amount)

    @staticmethod
    def _trim_count_map(value: Dict[str, int], *, limit: int) -> Dict[str, int]:
        normalized = DesktopWorkflowMemory._normalize_count_map(value)
        ordered = sorted(normalized.items(), key=lambda item: (-item[1], item[0]))
        return {key: count for key, count in ordered[: max(1, limit)]}

    @staticmethod
    def _top_examples(value: Dict[str, int], *, limit: int = 3) -> List[str]:
        normalized = DesktopWorkflowMemory._trim_count_map(value, limit=limit)
        return [key for key in normalized.keys()]

    @staticmethod
    def _top_count_key(value: Any, *, minimum: int) -> str:
        normalized = DesktopWorkflowMemory._normalize_count_map(value)
        if not normalized:
            return ""
        best_key, best_count = max(normalized.items(), key=lambda item: (item[1], item[0]))
        return best_key if DesktopWorkflowMemory._coerce_int(best_count, minimum=0, maximum=10_000_000, default=0) >= minimum else ""

    @staticmethod
    def _encode_learning_value(value: Any) -> str:
        payload = {
            "type": "bool" if isinstance(value, bool) else ("int" if isinstance(value, int) and not isinstance(value, bool) else "str"),
            "value": value if not isinstance(value, str) else str(value).strip().lower(),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode_learning_value(value: str) -> Any:
        try:
            payload = json.loads(str(value or ""))
        except Exception:
            return str(value or "")
        if not isinstance(payload, dict):
            return str(value or "")
        value_type = str(payload.get("type", "") or "").strip().lower()
        raw_value = payload.get("value")
        if value_type == "bool":
            return bool(raw_value)
        if value_type == "int":
            try:
                return int(raw_value)
            except Exception:
                return 0
        return str(raw_value or "")

    @staticmethod
    def _strip_nested_scope(skill: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(skill) if isinstance(skill, dict) else {}
        payload.pop("matched_scopes", None)
        return payload

    @staticmethod
    def _values_equal(left: Any, right: Any) -> bool:
        if isinstance(left, str) and isinstance(right, str):
            return DesktopWorkflowMemory._normalize_text(left) == DesktopWorkflowMemory._normalize_text(right)
        return left == right

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            return default
        return max(minimum, min(parsed, maximum))

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            return default
        return max(minimum, min(parsed, maximum))
