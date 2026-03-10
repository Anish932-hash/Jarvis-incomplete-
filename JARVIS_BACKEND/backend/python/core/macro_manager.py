from __future__ import annotations

import json
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional

from .contracts import ActionResult


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tokens(value: str) -> set[str]:
    words = {token for token in re.split(r"[^a-zA-Z0-9_]+", value.lower()) if token}
    return {token for token in words if len(token) >= 2}


@dataclass(slots=True)
class MacroRecord:
    macro_id: str
    name: str
    text: str
    source: str
    actions: List[str]
    success_count: int = 0
    usage_count: int = 0
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    last_used_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MacroManager:
    """
    Learns reusable macro candidates from successful goal executions.
    """

    def __init__(self, store_path: str = "data/macros.json", max_records: int = 1500) -> None:
        self.store_path = Path(store_path)
        self.max_records = max(100, int(max_records))
        self._records: Dict[str, MacroRecord] = {}
        self._text_index: Dict[str, str] = {}
        self._lock = RLock()
        self._load()

    def learn_from_goal(self, *, text: str, source: str, status: str, results: List[ActionResult]) -> Optional[MacroRecord]:
        clean_text = str(text or "").strip()
        if not clean_text:
            return None
        if str(status or "").strip().lower() != "completed":
            return None

        actions = [result.action for result in results if isinstance(result.action, str) and result.action]
        if not actions:
            return None
        if len(actions) == 1 and actions[0] == "tts_speak":
            return None

        signature = clean_text.lower()
        with self._lock:
            macro_id = self._text_index.get(signature)
            if macro_id and macro_id in self._records:
                record = self._records[macro_id]
                record.success_count = max(0, int(record.success_count)) + 1
                record.actions = self._merge_actions(record.actions, actions)
                record.updated_at = _utc_now_iso()
            else:
                record = MacroRecord(
                    macro_id=str(uuid.uuid4()),
                    name=self._derive_name(clean_text, actions),
                    text=clean_text,
                    source=str(source or "user"),
                    actions=self._merge_actions([], actions),
                    success_count=1,
                )
                self._records[record.macro_id] = record
                self._text_index[signature] = record.macro_id
                self._trim_locked()

            self._save_locked()
            return record

    def list(self, *, query: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self._records.values())

        query_text = str(query or "").strip().lower()
        if query_text:
            tokens = _tokens(query_text)
            filtered: List[MacroRecord] = []
            for item in rows:
                hay = _tokens(item.text) | {token.lower() for token in item.actions}
                if tokens.intersection(hay):
                    filtered.append(item)
            rows = filtered

        rows.sort(key=lambda item: (item.success_count, item.usage_count, item.updated_at), reverse=True)
        bounded = rows[: max(1, min(int(limit), 500))]
        return [item.to_dict() for item in bounded]

    def get(self, macro_id: str) -> Optional[MacroRecord]:
        with self._lock:
            return self._records.get(macro_id)

    def mark_used(self, macro_id: str) -> Optional[MacroRecord]:
        with self._lock:
            record = self._records.get(macro_id)
            if not record:
                return None
            record.usage_count = max(0, int(record.usage_count)) + 1
            now = _utc_now_iso()
            record.last_used_at = now
            record.updated_at = now
            self._save_locked()
            return record

    @staticmethod
    def _derive_name(text: str, actions: List[str]) -> str:
        headline = re.sub(r"\s+", " ", text).strip()
        if len(headline) > 64:
            headline = headline[:61].rstrip() + "..."
        if headline:
            return headline
        return " + ".join(actions[:3]) or "Macro"

    @staticmethod
    def _merge_actions(existing: List[str], incoming: List[str]) -> List[str]:
        ordered: List[str] = []
        for item in existing + incoming:
            clean = str(item or "").strip()
            if clean and clean not in ordered:
                ordered.append(clean)
        return ordered[:20]

    def _load(self) -> None:
        with self._lock:
            if not self.store_path.exists():
                return
            try:
                payload = json.loads(self.store_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                return
            if not isinstance(payload, list):
                return
            for item in payload:
                record = self._coerce_record(item)
                if not record:
                    continue
                self._records[record.macro_id] = record
                self._text_index[record.text.lower()] = record.macro_id
            self._trim_locked()

    def _coerce_record(self, raw: Any) -> Optional[MacroRecord]:
        if not isinstance(raw, dict):
            return None
        macro_id = str(raw.get("macro_id", "")).strip()
        text = str(raw.get("text", "")).strip()
        if not macro_id or not text:
            return None
        actions = raw.get("actions", [])
        if not isinstance(actions, list):
            actions = []
        return MacroRecord(
            macro_id=macro_id,
            name=str(raw.get("name", text[:64])),
            text=text,
            source=str(raw.get("source", "user")),
            actions=[str(item) for item in actions if isinstance(item, str)],
            success_count=max(0, int(raw.get("success_count", 0))),
            usage_count=max(0, int(raw.get("usage_count", 0))),
            created_at=str(raw.get("created_at", _utc_now_iso())),
            updated_at=str(raw.get("updated_at", _utc_now_iso())),
            last_used_at=str(raw.get("last_used_at", "")),
        )

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.to_dict() for item in self._records.values()]
        rows.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        self.store_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

    def _trim_locked(self) -> None:
        if len(self._records) <= self.max_records:
            return
        rows = sorted(self._records.values(), key=lambda item: item.updated_at)
        overflow = len(rows) - self.max_records
        for item in rows[:overflow]:
            self._records.pop(item.macro_id, None)
            self._text_index.pop(item.text.lower(), None)

