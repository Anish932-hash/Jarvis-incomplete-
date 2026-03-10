from __future__ import annotations

import json
import re
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Deque, Dict, List, Optional

from .contracts import ActionResult


class RuntimeMemory:
    """
    Lightweight runtime memory used by planner/kernel for short-term context.
    """

    def __init__(self, max_items: int = 120, store_path: str = "data/runtime_memory.jsonl") -> None:
        self.max_items = max(20, int(max_items))
        self.store_path = Path(store_path)
        self._records: Deque[Dict[str, Any]] = deque(maxlen=self.max_items)
        self._lock = RLock()
        self._load()

    def remember_goal(
        self,
        *,
        text: str,
        status: str,
        results: List[ActionResult],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        actions = [result.action for result in results]
        failures = [result.error for result in results if result.status in {"failed", "blocked"} and result.error]
        summaries = []
        for item in results[:3]:
            if item.status == "success":
                summaries.append(f"{item.action}:ok")
            else:
                summaries.append(f"{item.action}:{item.status}")
        payload_meta = metadata if isinstance(metadata, dict) else {}
        repair_signals = self._extract_repair_signals(results)
        record = {
            "memory_id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "text": text,
            "status": status,
            "actions": actions,
            "failure": failures[-1] if failures else "",
            "result_summary": ", ".join(summaries),
        }
        source_name = str(payload_meta.get("source", "")).strip().lower()
        if source_name:
            record["source"] = source_name
        profile_name = str(payload_meta.get("policy_profile", "")).strip().lower()
        if profile_name:
            record["policy_profile"] = profile_name
        if repair_signals:
            record["repair_signals"] = repair_signals

        with self._lock:
            self._records.append(record)
            self._append_record(record)

    def recent_hints(self, limit: int = 8) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        with self._lock:
            items = list(self._records)
        return items[-limit:]

    def search(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        prompt = str(query or "").strip()
        if not prompt:
            return self.recent_hints(limit=limit)

        tokens = self._tokens(prompt)
        with self._lock:
            records = list(self._records)

        scored: List[tuple[float, Dict[str, Any]]] = []
        total = len(records)
        for index, record in enumerate(records):
            score = self._score_record(record, tokens=tokens)
            if score <= 0.0:
                continue
            recency_bonus = (index + 1) / max(1, total) * 0.35
            scored.append((score + recency_bonus, record))

        scored.sort(key=lambda item: item[0], reverse=True)
        ranked = [dict(item[1], memory_score=round(item[0], 4)) for item in scored[: max(1, min(limit, 50))]]
        return ranked

    def _append_record(self, record: Dict[str, Any]) -> None:
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            with self.store_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=True))
                handle.write("\n")
        except Exception:
            return

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        try:
            lines = self.store_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return
        for raw in lines[-self.max_items :]:
            try:
                record = json.loads(raw)
            except Exception:
                continue
            if isinstance(record, dict):
                self._records.append(record)

    @staticmethod
    def _tokens(value: str) -> set[str]:
        tokens = {item for item in re.split(r"[^a-zA-Z0-9_]+", value.lower()) if item}
        return {item for item in tokens if len(item) >= 2}

    def _score_record(self, record: Dict[str, Any], *, tokens: set[str]) -> float:
        text = str(record.get("text", ""))
        status = str(record.get("status", ""))
        actions = record.get("actions", [])
        failure = str(record.get("failure", ""))

        text_tokens = self._tokens(text)
        fail_tokens = self._tokens(failure)
        action_tokens = {str(item).lower() for item in actions if isinstance(item, str)}
        repair_tokens = self._repair_tokens(record)

        overlap = len(tokens.intersection(text_tokens))
        fail_overlap = len(tokens.intersection(fail_tokens))
        action_overlap = len(tokens.intersection(action_tokens))
        repair_overlap = len(tokens.intersection(repair_tokens))

        score = 0.0
        score += overlap * 1.8
        score += fail_overlap * 1.1
        score += action_overlap * 2.2
        score += repair_overlap * 2.6
        if status == "completed":
            score += 0.35
        if status in {"failed", "blocked"}:
            score += 0.2
        return score

    @staticmethod
    def _safe_value(value: Any, *, depth: int = 0) -> Any:
        if depth >= 4:
            if isinstance(value, (dict, list, tuple)):
                return ""
            if value is None:
                return None
            return str(value)[:200]

        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= 40:
                    break
                clean_key = str(key).strip()[:100]
                if not clean_key:
                    continue
                out[clean_key] = RuntimeMemory._safe_value(item, depth=depth + 1)
            return out

        if isinstance(value, (list, tuple)):
            return [RuntimeMemory._safe_value(item, depth=depth + 1) for item in list(value)[:24]]

        if isinstance(value, (str, int, float, bool)) or value is None:
            if isinstance(value, str):
                return value[:1200]
            return value
        return str(value)[:200]

    def _extract_repair_signals(self, results: List[ActionResult]) -> List[Dict[str, Any]]:
        signals: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in results:
            if not isinstance(item, ActionResult):
                continue
            action = str(item.action or "").strip().lower()
            if not action:
                continue
            if not action.startswith(("external_", "oauth_token_")):
                continue
            status = str(item.status or "").strip().lower() or "unknown"
            evidence = item.evidence if isinstance(item.evidence, dict) else {}
            external = evidence.get("external_reliability_preflight")
            if not isinstance(external, dict):
                external = evidence.get("external_reliability")
            external_payload = external if isinstance(external, dict) else {}
            contract_diag = (
                external_payload.get("contract_diagnostic")
                if isinstance(external_payload.get("contract_diagnostic"), dict)
                else {}
            )
            contract_code = str(contract_diag.get("code", "")).strip().lower()
            provider = ""
            routing = external_payload.get("provider_routing")
            if isinstance(routing, dict):
                provider = str(routing.get("selected_provider", "")).strip().lower()
            request = evidence.get("request")
            request_payload = request if isinstance(request, dict) else {}
            request_args = request_payload.get("args")
            args_payload = request_args if isinstance(request_args, dict) else {}
            if not provider and isinstance(args_payload, dict):
                provider = str(args_payload.get("provider", "")).strip().lower()
            if provider == "auto":
                provider = ""

            key = f"{action}|{status}|{provider}|{contract_code}"
            if key in seen:
                continue
            seen.add(key)

            signal: Dict[str, Any] = {
                "action": action,
                "status": status,
                "provider": provider,
                "contract_code": contract_code,
                "attempt": max(1, int(item.attempt or 1)),
                "completed_at": str(item.completed_at or "").strip(),
            }
            if isinstance(args_payload, dict) and args_payload:
                signal["args"] = self._safe_value(args_payload)
            if str(item.error or "").strip():
                signal["error"] = str(item.error or "").strip()[:280]
            signals.append(signal)
            if len(signals) >= 32:
                break
        return signals

    def _repair_tokens(self, record: Dict[str, Any]) -> set[str]:
        rows = record.get("repair_signals", [])
        if not isinstance(rows, list) or not rows:
            return set()
        tokens: set[str] = set()
        for row in rows[:12]:
            if not isinstance(row, dict):
                continue
            action = str(row.get("action", "")).strip().lower()
            provider = str(row.get("provider", "")).strip().lower()
            code = str(row.get("contract_code", "")).strip().lower()
            status = str(row.get("status", "")).strip().lower()
            if action:
                tokens.add(action)
            if provider:
                tokens.add(provider)
            if code:
                tokens.update(self._tokens(code.replace("_", " ")))
            if status:
                tokens.add(status)
            args_payload = row.get("args", {})
            if isinstance(args_payload, dict):
                for key, value in list(args_payload.items())[:14]:
                    key_text = str(key).strip().lower()
                    if key_text:
                        tokens.update(self._tokens(key_text.replace("_", " ")))
                    if isinstance(value, str):
                        tokens.update(self._tokens(value))
        return tokens
