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
    ) -> Dict[str, Any]:
        rows = [dict(row) for row in (variants or []) if isinstance(row, dict)]
        if not rows:
            return {"status": "skip", "applied": False, "variants": [], "ranking": []}
        key = self._entry_key(action=action, args=args, app_profile=app_profile)
        with self._lock:
            entry = dict(self._entries.get(key, {}))
        stats_by_signature = entry.get("variants", {}) if isinstance(entry.get("variants", {}), dict) else {}
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
            score = (
                base_score
                + (verified_rate * 0.42)
                + (execution_rate * 0.18)
                + min(0.12, float(samples) * 0.02)
                - min(0.24, failure_rate * 0.24)
                - min(0.12, float(consecutive_failures) * 0.03)
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
        by_signature = {
            self._variant_signature(variant): dict(variant)
            for variant in rows
        }
        for item in ranking:
            signature = str(item.get("signature", "") or "").strip()
            variant = by_signature.get(signature)
            if not isinstance(variant, dict):
                continue
            variant["adaptive_score"] = item.get("score", 0.0)
            variant["adaptive_samples"] = item.get("samples", 0)
            variant["adaptive_verified_rate"] = item.get("verified_rate", 0.0)
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
        }

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        app_profile: Dict[str, Any] | None,
        strategy: Dict[str, Any] | None,
        attempt: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        strategy_payload = strategy if isinstance(strategy, dict) else {}
        attempt_payload = attempt if isinstance(attempt, dict) else {}
        key = self._entry_key(action=action, args=args, app_profile=app_profile)
        signature = self._variant_signature(strategy_payload)
        status = str(attempt_payload.get("status", "") or "").strip().lower()
        verification = attempt_payload.get("verification", {}) if isinstance(attempt_payload.get("verification", {}), dict) else {}
        verified = bool(verification.get("verified", False))
        success = status == "success"
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
            entry.update(
                {
                    "key": key,
                    "action": str(action or "").strip().lower(),
                    "profile_id": self._profile_id(app_profile),
                    "app_name": self._normalize_text((args or {}).get("app_name", "")),
                    "window_title": self._normalize_text((args or {}).get("window_title", "")),
                    "intent": self._intent_signature(args),
                    "variants": variants,
                    "updated_at": now,
                }
            )
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
        }

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
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "items": rows[:bounded],
            "filters": {
                "action": clean_action,
                "app_name": clean_app_name,
                "profile_id": clean_profile_id,
                "intent": clean_intent,
            },
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
            "version": "1.0",
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
            if len(variants) <= self.max_variants_per_entry:
                continue
            ordered_variants = sorted(
                variants.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("verified_successes", 0), minimum=0, maximum=10_000_000, default=0),
                    self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[1].get("updated_at", "")),
                ),
                reverse=True,
            )
            row["variants"] = {variant_key: variant_value for variant_key, variant_value in ordered_variants[: self.max_variants_per_entry]}
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
