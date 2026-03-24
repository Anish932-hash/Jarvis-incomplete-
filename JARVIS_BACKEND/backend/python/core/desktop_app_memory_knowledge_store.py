from __future__ import annotations

import json
import math
import re
import sqlite3
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, List, Sequence


_TOKEN_PATTERN = re.compile(r"[a-z0-9_+#.-]+")


class DesktopAppMemoryKnowledgeStore:
    def __init__(
        self,
        *,
        db_path: str = "data/desktop_app_memory.sqlite3",
        vector_dimensions: int = 64,
        max_match_scan: int = 512,
    ) -> None:
        self.db_path = Path(db_path)
        self.vector_dimensions = max(16, min(int(vector_dimensions or 64), 512))
        self.max_match_scan = max(32, min(int(max_match_scan or 512), 4096))
        self._lock = RLock()
        self._ensure_schema()

    def replace_all(self, *, entries: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM app_entry_vectors")
                conn.execute("DELETE FROM app_entry_commands")
                conn.execute("DELETE FROM app_entry_controls")
                conn.execute("DELETE FROM app_entries")
                for entry_key, row in entries.items():
                    if not str(entry_key).strip() or not isinstance(row, dict):
                        continue
                    self._sync_entry_locked(conn, entry_key=str(entry_key).strip(), row=dict(row))
                conn.commit()

    def sync_entry(self, *, entry_key: str, row: Dict[str, Any]) -> None:
        if not str(entry_key).strip() or not isinstance(row, dict):
            return
        with self._lock:
            with self._connect() as conn:
                self._sync_entry_locked(conn, entry_key=str(entry_key).strip(), row=dict(row))
                conn.commit()

    def summary(self, *, app_name: str = "", profile_id: str = "", category: str = "") -> Dict[str, Any]:
        query_filters, parameters = self._summary_filters(app_name=app_name, profile_id=profile_id, category=category)
        with self._lock:
            with self._connect() as conn:
                entry_count = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM app_entries {query_filters}",
                        parameters,
                    ).fetchone()[0]
                )
                control_count = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM app_entry_controls WHERE entry_key IN (SELECT entry_key FROM app_entries {query_filters})",
                        parameters,
                    ).fetchone()[0]
                )
                command_count = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM app_entry_commands WHERE entry_key IN (SELECT entry_key FROM app_entries {query_filters})",
                        parameters,
                    ).fetchone()[0]
                )
                vector_count = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM app_entry_vectors WHERE entry_key IN (SELECT entry_key FROM app_entries {query_filters})",
                        parameters,
                    ).fetchone()[0]
                )
        return {
            "status": "success",
            "entry_count": entry_count,
            "control_count": control_count,
            "command_count": command_count,
            "vector_count": vector_count,
            "db_path": str(self.db_path),
            "vector_dimensions": self.vector_dimensions,
        }

    def semantic_lookup(
        self,
        *,
        query: str,
        app_name: str = "",
        profile_id: str = "",
        limit: int = 8,
        entity_types: Sequence[str] | None = None,
    ) -> Dict[str, Any]:
        clean_query = self._normalize_text(query)
        bounded_limit = max(1, min(int(limit or 8), 64))
        if not clean_query:
            return {"status": "success", "count": 0, "items": []}
        allowed_types = {
            self._normalize_text(item)
            for item in (entity_types or [])
            if self._normalize_text(item)
        }
        query_vector = self._embed_text(clean_query)
        query_tokens = set(self._tokenize(clean_query))
        clauses = ["1=1"]
        parameters: List[Any] = []
        if app_name:
            clauses.append("normalized_app_name LIKE ?")
            parameters.append(f"%{self._normalize_text(app_name)}%")
        if profile_id:
            clauses.append("profile_id = ?")
            parameters.append(self._normalize_text(profile_id))
        if allowed_types:
            placeholders = ",".join("?" for _ in allowed_types)
            clauses.append(f"entity_type IN ({placeholders})")
            parameters.extend(sorted(allowed_types))
        parameters.append(self.max_match_scan)
        sql = (
            "SELECT entity_key, entity_type, app_name, profile_id, label, control_type, semantic_role, "
            "container_role, hotkeys_json, source, text_payload, vector_json, updated_at "
            f"FROM app_entry_vectors WHERE {' AND '.join(clauses)} ORDER BY updated_at DESC LIMIT ?"
        )
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                for raw in conn.execute(sql, parameters).fetchall():
                    vector = self._parse_vector(raw["vector_json"])
                    similarity = self._cosine_similarity(query_vector, vector)
                    text_payload = str(raw["text_payload"] or "")
                    token_overlap = len(query_tokens.intersection(self._tokenize(text_payload)))
                    lexical_bonus = min(token_overlap, 4) * 0.08
                    score = round(similarity + lexical_bonus, 4)
                    rows.append(
                        {
                            "entity_key": str(raw["entity_key"] or ""),
                            "entity_type": str(raw["entity_type"] or ""),
                            "app_name": str(raw["app_name"] or ""),
                            "profile_id": str(raw["profile_id"] or ""),
                            "label": str(raw["label"] or ""),
                            "control_type": str(raw["control_type"] or ""),
                            "semantic_role": str(raw["semantic_role"] or ""),
                            "container_role": str(raw["container_role"] or ""),
                            "source": str(raw["source"] or ""),
                            "hotkeys": self._parse_json_list(raw["hotkeys_json"]),
                            "text_payload": text_payload,
                            "similarity": score,
                            "token_overlap": token_overlap,
                            "updated_at": str(raw["updated_at"] or ""),
                        }
                    )
        rows.sort(key=lambda item: (-float(item.get("similarity", 0.0) or 0.0), str(item.get("label", ""))))
        return {"status": "success", "count": min(len(rows), bounded_limit), "items": rows[:bounded_limit]}

    def reset(self, *, entry_keys: Sequence[str] | None = None) -> None:
        clean_entry_keys = [str(item).strip() for item in (entry_keys or []) if str(item).strip()]
        with self._lock:
            with self._connect() as conn:
                if clean_entry_keys:
                    placeholders = ",".join("?" for _ in clean_entry_keys)
                    conn.execute(f"DELETE FROM app_entry_vectors WHERE entry_key IN ({placeholders})", clean_entry_keys)
                    conn.execute(f"DELETE FROM app_entry_commands WHERE entry_key IN ({placeholders})", clean_entry_keys)
                    conn.execute(f"DELETE FROM app_entry_controls WHERE entry_key IN ({placeholders})", clean_entry_keys)
                    conn.execute(f"DELETE FROM app_entries WHERE entry_key IN ({placeholders})", clean_entry_keys)
                else:
                    conn.execute("DELETE FROM app_entry_vectors")
                    conn.execute("DELETE FROM app_entry_commands")
                    conn.execute("DELETE FROM app_entry_controls")
                    conn.execute("DELETE FROM app_entries")
                conn.commit()

    def _sync_entry_locked(self, conn: sqlite3.Connection, *, entry_key: str, row: Dict[str, Any]) -> None:
        app_name = str(row.get("app_name", "") or "").strip()
        profile_id = str(row.get("profile_id", "") or "").strip().lower()
        profile_name = str(row.get("profile_name", "") or "").strip()
        category = str(row.get("category", "") or "").strip().lower()
        window_title = str(row.get("window_title", "") or "").strip()
        updated_at = str(row.get("updated_at", "") or "").strip()
        controls = row.get("controls", {}) if isinstance(row.get("controls", {}), dict) else {}
        commands = row.get("learned_commands", {}) if isinstance(row.get("learned_commands", {}), dict) else {}
        shortcuts = row.get("shortcut_actions", {}) if isinstance(row.get("shortcut_actions", {}), dict) else {}
        capability_profile = row.get("capability_profile", {}) if isinstance(row.get("capability_profile", {}), dict) else {}
        version_profile = row.get("version_profile", {}) if isinstance(row.get("version_profile", {}), dict) else {}
        conn.execute(
            "REPLACE INTO app_entries(entry_key, app_name, normalized_app_name, profile_id, profile_name, category, window_title, updated_at, "
            "survey_count, discovered_control_count, learned_command_count, shortcut_count, capability_json, version_json, raw_json) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_key,
                app_name,
                self._normalize_text(app_name),
                profile_id,
                profile_name,
                category,
                window_title,
                updated_at,
                int(row.get("survey_count", 0) or 0),
                len(controls),
                len(commands),
                len(shortcuts),
                json.dumps(capability_profile, ensure_ascii=True, sort_keys=True),
                json.dumps(version_profile, ensure_ascii=True, sort_keys=True),
                json.dumps(row, ensure_ascii=True, sort_keys=True),
            ),
        )
        conn.execute("DELETE FROM app_entry_controls WHERE entry_key = ?", (entry_key,))
        conn.execute("DELETE FROM app_entry_commands WHERE entry_key = ?", (entry_key,))
        conn.execute("DELETE FROM app_entry_vectors WHERE entry_key = ?", (entry_key,))
        for control_key, payload in controls.items():
            self._insert_control(conn, entry_key=entry_key, app_name=app_name, profile_id=profile_id, control_key=str(control_key), payload=payload, updated_at=updated_at)
        for command_key, payload in commands.items():
            self._insert_command(conn, entry_key=entry_key, app_name=app_name, profile_id=profile_id, command_key=str(command_key), payload=payload, updated_at=updated_at, source="learned_command")
        for shortcut_key, payload in shortcuts.items():
            self._insert_command(conn, entry_key=entry_key, app_name=app_name, profile_id=profile_id, command_key=str(shortcut_key), payload=payload, updated_at=updated_at, source="shortcut")

    def _insert_control(self, conn: sqlite3.Connection, *, entry_key: str, app_name: str, profile_id: str, control_key: str, payload: Any, updated_at: str) -> None:
        row = dict(payload) if isinstance(payload, dict) else {}
        label = str(row.get("label", "") or row.get("name", "") or control_key).strip()
        if not label:
            return
        hotkeys = self._dedupe_strings(row.get("hotkeys", []))
        entity_key = f"control:{entry_key}:{control_key}"
        text_payload = self._build_text_payload(
            app_name=app_name,
            label=label,
            control_type=str(row.get("control_type", "") or "").strip().lower(),
            semantic_role=str(row.get("semantic_role", "") or "").strip().lower(),
            container_role=str(row.get("container_role", "") or "").strip().lower(),
            hotkeys=hotkeys,
        )
        conn.execute(
            "REPLACE INTO app_entry_controls(control_key, entry_key, app_name, profile_id, label, normalized_label, control_type, semantic_role, container_role, automation_id, effect_kind, confidence, hotkeys_json, updated_at, raw_json) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_key,
                entry_key,
                app_name,
                profile_id,
                label,
                self._normalize_text(label),
                str(row.get("control_type", "") or "").strip().lower(),
                str(row.get("semantic_role", "") or "").strip().lower(),
                str(row.get("container_role", "") or "").strip().lower(),
                str(row.get("automation_id", "") or "").strip(),
                str(row.get("effect_kind", "") or "").strip().lower(),
                float(row.get("verification_confidence", row.get("confidence", 0.0)) or 0.0),
                json.dumps(hotkeys, ensure_ascii=True),
                updated_at,
                json.dumps(row, ensure_ascii=True, sort_keys=True),
            ),
        )
        self._insert_vector(conn, entry_key=entry_key, entity_key=entity_key, entity_type="control", app_name=app_name, profile_id=profile_id, label=label, control_type=str(row.get("control_type", "") or "").strip().lower(), semantic_role=str(row.get("semantic_role", "") or "").strip().lower(), container_role=str(row.get("container_role", "") or "").strip().lower(), hotkeys=hotkeys, source="control", text_payload=text_payload, updated_at=updated_at)

    def _insert_command(self, conn: sqlite3.Connection, *, entry_key: str, app_name: str, profile_id: str, command_key: str, payload: Any, updated_at: str, source: str) -> None:
        row = dict(payload) if isinstance(payload, dict) else {}
        label = str(row.get("label", "") or row.get("name", "") or row.get("command", "") or command_key).strip()
        if not label:
            return
        hotkeys = self._dedupe_strings(row.get("hotkeys", []))
        entity_key = f"command:{entry_key}:{source}:{command_key}"
        text_payload = self._build_text_payload(
            app_name=app_name,
            label=label,
            control_type="command",
            semantic_role=str(row.get("semantic_role", "") or "").strip().lower(),
            container_role="command",
            hotkeys=hotkeys,
        )
        conn.execute(
            "REPLACE INTO app_entry_commands(command_key, entry_key, app_name, profile_id, label, normalized_label, source, hotkeys_json, confidence, updated_at, raw_json) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_key,
                entry_key,
                app_name,
                profile_id,
                label,
                self._normalize_text(label),
                source,
                json.dumps(hotkeys, ensure_ascii=True),
                float(row.get("confidence", row.get("verification_confidence", 0.0)) or 0.0),
                updated_at,
                json.dumps(row, ensure_ascii=True, sort_keys=True),
            ),
        )
        self._insert_vector(conn, entry_key=entry_key, entity_key=entity_key, entity_type="command", app_name=app_name, profile_id=profile_id, label=label, control_type="command", semantic_role=str(row.get("semantic_role", "") or "").strip().lower(), container_role="command", hotkeys=hotkeys, source=source, text_payload=text_payload, updated_at=updated_at)

    def _insert_vector(self, conn: sqlite3.Connection, *, entry_key: str, entity_key: str, entity_type: str, app_name: str, profile_id: str, label: str, control_type: str, semantic_role: str, container_role: str, hotkeys: Sequence[str], source: str, text_payload: str, updated_at: str) -> None:
        conn.execute(
            "REPLACE INTO app_entry_vectors(entity_key, entity_type, entry_key, app_name, normalized_app_name, profile_id, label, control_type, semantic_role, container_role, source, hotkeys_json, text_payload, vector_json, updated_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_key,
                entity_type,
                entry_key,
                app_name,
                self._normalize_text(app_name),
                profile_id,
                label,
                control_type,
                semantic_role,
                container_role,
                source,
                json.dumps(list(hotkeys), ensure_ascii=True),
                text_payload,
                json.dumps(self._embed_text(text_payload), ensure_ascii=True),
                updated_at,
            ),
        )

    def _ensure_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS app_entries (
                    entry_key TEXT PRIMARY KEY,
                    app_name TEXT NOT NULL,
                    normalized_app_name TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    profile_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    window_title TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    survey_count INTEGER NOT NULL DEFAULT 0,
                    discovered_control_count INTEGER NOT NULL DEFAULT 0,
                    learned_command_count INTEGER NOT NULL DEFAULT 0,
                    shortcut_count INTEGER NOT NULL DEFAULT 0,
                    capability_json TEXT NOT NULL DEFAULT '{}',
                    version_json TEXT NOT NULL DEFAULT '{}',
                    raw_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS app_entry_controls (
                    control_key TEXT PRIMARY KEY,
                    entry_key TEXT NOT NULL,
                    app_name TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    normalized_label TEXT NOT NULL,
                    control_type TEXT NOT NULL,
                    semantic_role TEXT NOT NULL,
                    container_role TEXT NOT NULL,
                    automation_id TEXT NOT NULL,
                    effect_kind TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0.0,
                    hotkeys_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS app_entry_commands (
                    command_key TEXT PRIMARY KEY,
                    entry_key TEXT NOT NULL,
                    app_name TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    normalized_label TEXT NOT NULL,
                    source TEXT NOT NULL,
                    hotkeys_json TEXT NOT NULL DEFAULT '[]',
                    confidence REAL NOT NULL DEFAULT 0.0,
                    updated_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS app_entry_vectors (
                    entity_key TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    entry_key TEXT NOT NULL,
                    app_name TEXT NOT NULL,
                    normalized_app_name TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    control_type TEXT NOT NULL,
                    semantic_role TEXT NOT NULL,
                    container_role TEXT NOT NULL,
                    source TEXT NOT NULL,
                    hotkeys_json TEXT NOT NULL DEFAULT '[]',
                    text_payload TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _summary_filters(self, *, app_name: str, profile_id: str, category: str) -> tuple[str, List[Any]]:
        clauses = ["1=1"]
        parameters: List[Any] = []
        if app_name:
            clauses.append("normalized_app_name LIKE ?")
            parameters.append(f"%{self._normalize_text(app_name)}%")
        if profile_id:
            clauses.append("profile_id = ?")
            parameters.append(self._normalize_text(profile_id))
        if category:
            clauses.append("category = ?")
            parameters.append(self._normalize_text(category))
        return f"WHERE {' AND '.join(clauses)}", parameters

    def _embed_text(self, text: str) -> List[float]:
        vector = [0.0] * self.vector_dimensions
        for token in self._tokenize(text):
            slot = hash(token) % self.vector_dimensions
            vector[slot] += 1.0
        norm = math.sqrt(sum(value * value for value in vector))
        if norm <= 0.0:
            return vector
        return [round(value / norm, 6) for value in vector]

    @classmethod
    def _cosine_similarity(cls, left: Sequence[float], right: Sequence[float]) -> float:
        if not left or not right:
            return 0.0
        size = min(len(left), len(right))
        return round(sum(float(left[index]) * float(right[index]) for index in range(size)), 4)

    @classmethod
    def _parse_vector(cls, raw: Any) -> List[float]:
        try:
            payload = json.loads(str(raw or "[]"))
        except Exception:
            return []
        return [float(item or 0.0) for item in payload if isinstance(item, (int, float))]

    @classmethod
    def _parse_json_list(cls, raw: Any) -> List[str]:
        try:
            payload = json.loads(str(raw or "[]"))
        except Exception:
            return []
        if not isinstance(payload, list):
            return []
        return [str(item).strip() for item in payload if str(item).strip()]

    @classmethod
    def _build_text_payload(
        cls,
        *,
        app_name: str,
        label: str,
        control_type: str,
        semantic_role: str,
        container_role: str,
        hotkeys: Sequence[str],
    ) -> str:
        return " ".join(
            part
            for part in [
                app_name.strip(),
                label.strip(),
                control_type.strip(),
                semantic_role.strip(),
                container_role.strip(),
                " ".join(str(item).strip() for item in hotkeys if str(item).strip()),
            ]
            if part
        ).strip()

    @classmethod
    def _tokenize(cls, text: str) -> List[str]:
        return [match.group(0) for match in _TOKEN_PATTERN.finditer(cls._normalize_text(text))]

    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip().lower())

    @classmethod
    def _dedupe_strings(cls, values: Iterable[Any]) -> List[str]:
        seen: set[str] = set()
        ordered: List[str] = []
        for value in values:
            clean = str(value or "").strip().lower()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            ordered.append(clean)
        return ordered
