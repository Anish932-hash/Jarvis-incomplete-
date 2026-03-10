from __future__ import annotations

import hashlib
import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Deque, Dict, List, Optional


class DesktopState:
    """
    Persistent desktop state snapshots with lightweight diffing.
    Captures normalized fields from action outputs for planner/executor context.
    """

    def __init__(self, *, max_items: int = 600, store_path: str = "data/desktop_state.jsonl") -> None:
        self.max_items = max(50, int(max_items))
        self.store_path = Path(store_path)
        self._rows: Deque[Dict[str, Any]] = deque(maxlen=self.max_items)
        self._lock = RLock()
        self._load()

    def observe(
        self,
        *,
        action: str,
        output: Dict[str, Any],
        goal_id: str = "",
        plan_id: str = "",
        step_id: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        normalized = self._normalize_output(action=action, output=output)
        current_hash = self._hash_payload(normalized)

        with self._lock:
            previous = self._rows[-1] if self._rows else {}
            previous_hash = str(previous.get("state_hash", ""))
            changed_paths = self._diff_paths(
                previous.get("normalized", {}) if isinstance(previous.get("normalized"), dict) else {},
                normalized,
            )

            row = {
                "state_id": str(len(self._rows) + 1),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "action": str(action or "").strip(),
                "goal_id": str(goal_id or "").strip(),
                "plan_id": str(plan_id or "").strip(),
                "step_id": str(step_id or "").strip(),
                "source": str(source or "").strip(),
                "state_hash": current_hash,
                "previous_hash": previous_hash,
                "changed_paths": changed_paths[:64],
                "normalized": normalized,
            }
            self._rows.append(row)
            self._append_row(row)
        return self._public_row(row, include_normalized=False)

    def latest(self) -> Dict[str, Any]:
        with self._lock:
            row = self._rows[-1] if self._rows else None
        if not row:
            return {"status": "empty", "count": 0}
        payload = self._public_row(row, include_normalized=True)
        payload["status"] = "success"
        payload["count"] = len(self._rows)
        return payload

    def recent(self, *, limit: int = 10, include_normalized: bool = False) -> List[Dict[str, Any]]:
        bounded = max(1, min(int(limit), 200))
        with self._lock:
            rows = list(self._rows)[-bounded:]
        return [self._public_row(row, include_normalized=include_normalized) for row in rows]

    def hints(self, *, limit: int = 6) -> List[Dict[str, Any]]:
        rows = self.recent(limit=limit, include_normalized=True)
        hints: List[Dict[str, Any]] = []
        for row in rows:
            normalized = row.get("normalized", {})
            if not isinstance(normalized, dict):
                continue
            hint = {
                "state_hash": row.get("state_hash", ""),
                "action": row.get("action", ""),
                "created_at": row.get("created_at", ""),
                "app": self._read_path(normalized, "window.title") or self._read_path(normalized, "app"),
                "screen_hash": self._read_path(normalized, "visual.screen_hash") or "",
                "changed_paths": row.get("changed_paths", []),
            }
            hints.append(hint)
        return hints

    def diff(self, *, from_hash: str = "", to_hash: str = "") -> Dict[str, Any]:
        first = str(from_hash or "").strip()
        second = str(to_hash or "").strip()
        with self._lock:
            rows = list(self._rows)
        if not rows:
            return {"status": "empty", "message": "No desktop state snapshots available."}

        if not second:
            second = str(rows[-1].get("state_hash", ""))
        if not first and len(rows) >= 2:
            first = str(rows[-2].get("state_hash", ""))
        if not first:
            first = second

        row_a = next((row for row in rows if str(row.get("state_hash", "")) == first), None)
        row_b = next((row for row in rows if str(row.get("state_hash", "")) == second), None)
        if not row_a or not row_b:
            return {"status": "error", "message": "state hash not found"}

        payload_a = row_a.get("normalized", {}) if isinstance(row_a.get("normalized"), dict) else {}
        payload_b = row_b.get("normalized", {}) if isinstance(row_b.get("normalized"), dict) else {}
        changed = self._diff_paths(payload_a, payload_b)
        return {
            "status": "success",
            "from_hash": first,
            "to_hash": second,
            "changed_paths": changed[:128],
            "change_count": len(changed),
        }

    def _normalize_output(self, *, action: str, output: Dict[str, Any]) -> Dict[str, Any]:
        data = output if isinstance(output, dict) else {}
        normalized: Dict[str, Any] = {
            "action": str(action or "").strip(),
            "status": str(data.get("status", "")).strip().lower(),
            "app": str(data.get("app_name", "")).strip(),
            "window": {
                "title": str(self._read_path(data, "window.title") or data.get("title", "")).strip(),
                "hwnd": self._to_int(self._read_path(data, "window.hwnd") or data.get("hwnd")),
                "focused": bool(self._read_path(data, "window.focused")) if self._read_path(data, "window.focused") is not None else None,
            },
            "input": {
                "mouse": {
                    "x": self._to_int(data.get("x")),
                    "y": self._to_int(data.get("y")),
                    "button": str(data.get("button", "")).strip(),
                },
                "keyboard": {
                    "keys": data.get("keys") if isinstance(data.get("keys"), list) else [],
                    "chars": self._to_int(data.get("chars")),
                },
            },
            "visual": {
                "screenshot_path": str(data.get("screenshot_path", "")).strip(),
                "screen_hash": str(data.get("screen_hash", "")).strip(),
                "ocr_chars": self._to_int(data.get("chars")),
                "target_count": self._to_int(data.get("target_count")),
            },
            "io": {
                "path": str(data.get("path", "")).strip(),
                "source": str(data.get("source", "")).strip(),
                "destination": str(data.get("destination", "")).strip(),
                "url": str(data.get("url", "")).strip(),
            },
            "external": {
                "provider": str(data.get("provider", "")).strip(),
                "event_id": str(data.get("event_id", "")).strip(),
                "document_id": str(data.get("document_id", "")).strip(),
            },
        }
        return self._prune_none(normalized)

    def _append_row(self, row: Dict[str, Any]) -> None:
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            with self.store_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=True))
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
                row = json.loads(raw)
            except Exception:
                continue
            if isinstance(row, dict):
                self._rows.append(row)

    def _public_row(self, row: Dict[str, Any], *, include_normalized: bool) -> Dict[str, Any]:
        payload = {
            "state_id": str(row.get("state_id", "")),
            "created_at": str(row.get("created_at", "")),
            "action": str(row.get("action", "")),
            "goal_id": str(row.get("goal_id", "")),
            "plan_id": str(row.get("plan_id", "")),
            "step_id": str(row.get("step_id", "")),
            "source": str(row.get("source", "")),
            "state_hash": str(row.get("state_hash", "")),
            "previous_hash": str(row.get("previous_hash", "")),
            "changed_paths": list(row.get("changed_paths", [])),
        }
        if include_normalized:
            payload["normalized"] = row.get("normalized", {})
        return payload

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        text = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:24]

    def _diff_paths(self, before: Dict[str, Any], after: Dict[str, Any], *, prefix: str = "") -> List[str]:
        keys = set(before.keys()) | set(after.keys())
        changed: List[str] = []
        for key in sorted(keys):
            left = before.get(key)
            right = after.get(key)
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(left, dict) and isinstance(right, dict):
                changed.extend(self._diff_paths(left, right, prefix=path))
                continue
            if left != right:
                changed.append(path)
        return changed

    @staticmethod
    def _read_path(payload: Any, path: str) -> Any:
        node = payload
        for part in path.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return None
        return node

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    def _prune_none(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, dict):
                child = self._prune_none(value)
                if child:
                    output[key] = child
                continue
            if isinstance(value, list):
                if value:
                    output[key] = value
                continue
            if value is None:
                continue
            if isinstance(value, str) and not value:
                continue
            output[key] = value
        return output
