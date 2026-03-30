from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, List, Sequence


_TOKEN_PATTERN = re.compile(r"[a-z0-9_+#.-]+")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class _PersistentVectorFileStore:
    def __init__(
        self,
        *,
        path: str,
        store_kind: str,
        vector_dimensions: int,
        maintenance_interval_hours: float,
        deprecated_after_hours: float,
    ) -> None:
        self.path = Path(path)
        self.store_kind = str(store_kind or "").strip().lower() or "detail"
        self.vector_dimensions = max(16, min(int(vector_dimensions or 64), 512))
        self.maintenance_interval_hours = max(6.0, min(float(maintenance_interval_hours or 24.0), 24.0 * 30.0))
        self.deprecated_after_hours = max(12.0, min(float(deprecated_after_hours or 72.0), 24.0 * 90.0))
        self._items: Dict[str, Dict[str, Any]] = {}
        self._metadata = self._default_metadata()
        self._load()

    def _default_metadata(self) -> Dict[str, Any]:
        return {
            "store_kind": self.store_kind,
            "vector_dimensions": self.vector_dimensions,
            "maintenance_interval_hours": self.maintenance_interval_hours,
            "deprecated_after_hours": self.deprecated_after_hours,
            "last_synced_at": "",
            "last_maintenance_at": "",
            "last_maintenance_reason": "",
            "last_maintenance_added_count": 0,
            "last_maintenance_removed_count": 0,
            "last_maintenance_removed_labels": [],
            "last_maintenance_entry_count": 0,
            "item_count": 0,
        }

    def _load(self) -> None:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except Exception:
            return
        try:
            payload = json.loads(raw)
        except Exception:
            return
        metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
        if isinstance(metadata, dict):
            for key, value in metadata.items():
                self._metadata[str(key)] = value
        rows = payload.get("items", []) if isinstance(payload, dict) else []
        if isinstance(rows, dict):
            rows = list(rows.values())
        clean_rows: Dict[str, Dict[str, Any]] = {}
        if isinstance(rows, list):
            for row in rows:
                clean = self._clean_item(row)
                entity_key = str(clean.get("entity_key", "") or "").strip()
                if entity_key:
                    clean_rows[entity_key] = clean
        self._items = clean_rows
        self._metadata["item_count"] = len(self._items)

    def _clean_item(self, payload: Any) -> Dict[str, Any]:
        row = dict(payload) if isinstance(payload, dict) else {}
        vector = row.get("vector", [])
        if not isinstance(vector, list):
            vector = []
        return {
            "entity_key": str(row.get("entity_key", "") or "").strip(),
            "entry_key": str(row.get("entry_key", "") or "").strip(),
            "entity_type": str(row.get("entity_type", "") or "").strip().lower(),
            "app_name": str(row.get("app_name", "") or "").strip(),
            "normalized_app_name": str(row.get("normalized_app_name", "") or "").strip().lower(),
            "profile_id": str(row.get("profile_id", "") or "").strip().lower(),
            "category": str(row.get("category", "") or "").strip().lower(),
            "label": str(row.get("label", "") or "").strip(),
            "control_type": str(row.get("control_type", "") or "").strip().lower(),
            "semantic_role": str(row.get("semantic_role", "") or "").strip().lower(),
            "container_role": str(row.get("container_role", "") or "").strip().lower(),
            "source": str(row.get("source", "") or "").strip().lower(),
            "store_kind": self.store_kind,
            "hotkeys": [
                str(item).strip().lower()
                for item in row.get("hotkeys", [])
                if isinstance(row.get("hotkeys", []), list) and str(item).strip()
            ][:12],
            "text_payload": str(row.get("text_payload", "") or "").strip(),
            "vector": [
                round(float(item or 0.0), 6)
                for item in vector
                if isinstance(item, (int, float))
            ][: self.vector_dimensions],
            "updated_at": str(row.get("updated_at", "") or "").strip(),
            "last_seen_at": str(row.get("last_seen_at", "") or "").strip(),
            "details": dict(row.get("details", {})) if isinstance(row.get("details", {}), dict) else {},
        }

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ordered_rows = sorted(
            self._items.values(),
            key=lambda row: (
                str(row.get("normalized_app_name", "") or ""),
                str(row.get("entry_key", "") or ""),
                str(row.get("entity_key", "") or ""),
            ),
        )
        payload = {
            "version": "2.0",
            "updated_at": _utc_now_iso(),
            "store_kind": self.store_kind,
            "metadata": dict(self._metadata),
            "items": ordered_rows,
        }
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def items(self) -> List[Dict[str, Any]]:
        return [dict(row) for row in self._items.values()]

    def filtered_items(
        self,
        *,
        app_name: str = "",
        profile_id: str = "",
        category: str = "",
        entity_types: Sequence[str] | None = None,
    ) -> List[Dict[str, Any]]:
        clean_app_name = str(app_name or "").strip().lower()
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_category = str(category or "").strip().lower()
        allowed_types = {
            str(item or "").strip().lower()
            for item in (entity_types or [])
            if str(item or "").strip()
        }
        rows: List[Dict[str, Any]] = []
        for row in self._items.values():
            if clean_app_name:
                haystack = " ".join(
                    part
                    for part in [
                        str(row.get("normalized_app_name", "") or "").strip().lower(),
                        str(row.get("app_name", "") or "").strip().lower(),
                        str(row.get("label", "") or "").strip().lower(),
                    ]
                    if part
                )
                if clean_app_name not in haystack:
                    continue
            if clean_profile_id and str(row.get("profile_id", "") or "").strip().lower() != clean_profile_id:
                continue
            if clean_category and str(row.get("category", "") or "").strip().lower() != clean_category:
                continue
            if allowed_types and str(row.get("entity_type", "") or "").strip().lower() not in allowed_types:
                continue
            rows.append(dict(row))
        return rows

    def sync_entry(self, *, entry_key: str, items: Sequence[Dict[str, Any]]) -> None:
        clean_entry_key = str(entry_key or "").strip()
        if not clean_entry_key:
            return
        remaining = {
            key: value
            for key, value in self._items.items()
            if str(value.get("entry_key", "") or "").strip() != clean_entry_key
        }
        for row in items:
            clean = self._clean_item(row)
            entity_key = str(clean.get("entity_key", "") or "").strip()
            if entity_key:
                remaining[entity_key] = clean
        self._items = remaining
        self._metadata["last_synced_at"] = _utc_now_iso()
        self._metadata["item_count"] = len(self._items)
        self._save()

    def replace_all(
        self,
        *,
        items: Sequence[Dict[str, Any]],
        maintenance: Dict[str, Any] | None = None,
    ) -> None:
        rebuilt: Dict[str, Dict[str, Any]] = {}
        for row in items:
            clean = self._clean_item(row)
            entity_key = str(clean.get("entity_key", "") or "").strip()
            if entity_key:
                rebuilt[entity_key] = clean
        self._items = rebuilt
        now = _utc_now_iso()
        self._metadata["last_synced_at"] = now
        self._metadata["item_count"] = len(self._items)
        if isinstance(maintenance, dict):
            self._metadata["last_maintenance_at"] = now
            self._metadata["last_maintenance_reason"] = str(maintenance.get("reason", "") or "").strip()
            self._metadata["last_maintenance_added_count"] = max(0, int(maintenance.get("added_count", 0) or 0))
            self._metadata["last_maintenance_removed_count"] = max(0, int(maintenance.get("removed_count", 0) or 0))
            self._metadata["last_maintenance_removed_labels"] = [
                str(item).strip()
                for item in maintenance.get("removed_labels", [])
                if isinstance(maintenance.get("removed_labels", []), list) and str(item).strip()
            ][:12]
            self._metadata["last_maintenance_entry_count"] = max(0, int(maintenance.get("entry_count", 0) or 0))
        self._save()

    def reset(self, *, entry_keys: Sequence[str] | None = None) -> None:
        clean_entry_keys = {
            str(item).strip()
            for item in (entry_keys or [])
            if str(item).strip()
        }
        if clean_entry_keys:
            self._items = {
                key: value
                for key, value in self._items.items()
                if str(value.get("entry_key", "") or "").strip() not in clean_entry_keys
            }
        else:
            self._items = {}
        self._metadata["last_synced_at"] = _utc_now_iso()
        self._metadata["item_count"] = len(self._items)
        self._save()

    def maintenance_state(self) -> Dict[str, Any]:
        last_maintenance_at = str(self._metadata.get("last_maintenance_at", "") or "").strip()
        return {
            "store_kind": self.store_kind,
            "path": str(self.path),
            "item_count": len(self._items),
            "maintenance_interval_hours": self.maintenance_interval_hours,
            "deprecated_after_hours": self.deprecated_after_hours,
            "last_synced_at": str(self._metadata.get("last_synced_at", "") or "").strip(),
            "last_maintenance_at": last_maintenance_at,
            "last_maintenance_reason": str(self._metadata.get("last_maintenance_reason", "") or "").strip(),
            "last_maintenance_added_count": max(0, int(self._metadata.get("last_maintenance_added_count", 0) or 0)),
            "last_maintenance_removed_count": max(0, int(self._metadata.get("last_maintenance_removed_count", 0) or 0)),
            "last_maintenance_removed_labels": [
                str(item).strip()
                for item in self._metadata.get("last_maintenance_removed_labels", [])
                if isinstance(self._metadata.get("last_maintenance_removed_labels", []), list) and str(item).strip()
            ][:12],
            "last_maintenance_entry_count": max(0, int(self._metadata.get("last_maintenance_entry_count", 0) or 0)),
            "maintenance_due": self.maintenance_due(),
        }

    def maintenance_due(self) -> bool:
        last_maintenance_at = self._parse_iso(self._metadata.get("last_maintenance_at", ""))
        if last_maintenance_at is None:
            return True
        elapsed_s = (datetime.now(timezone.utc) - last_maintenance_at).total_seconds()
        return elapsed_s >= (self.maintenance_interval_hours * 3600.0)

    @staticmethod
    def _parse_iso(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


class DesktopAppMemoryKnowledgeStore:
    def __init__(
        self,
        *,
        db_path: str = "data/desktop_app_memory.sqlite3",
        vector_dimensions: int = 64,
        max_match_scan: int = 512,
        detail_vector_db_path: str = "",
        controls_vector_db_path: str = "",
        maintenance_interval_hours: float = 24.0,
        deprecated_after_hours: float = 72.0,
    ) -> None:
        self.db_path = Path(db_path)
        self.vector_dimensions = max(16, min(int(vector_dimensions or 64), 512))
        self.max_match_scan = max(32, min(int(max_match_scan or 512), 4096))
        self.maintenance_interval_hours = max(6.0, min(float(maintenance_interval_hours or 24.0), 24.0 * 30.0))
        self.deprecated_after_hours = max(12.0, min(float(deprecated_after_hours or 72.0), 24.0 * 90.0))
        self.detail_vector_db_path = Path(
            detail_vector_db_path or self._derive_vector_path(self.db_path, "functions_vector_db")
        )
        self.controls_vector_db_path = Path(
            controls_vector_db_path or self._derive_vector_path(self.db_path, "controls_vector_db")
        )
        self._lock = RLock()
        self._detail_vector_store = _PersistentVectorFileStore(
            path=str(self.detail_vector_db_path),
            store_kind="functions_details",
            vector_dimensions=self.vector_dimensions,
            maintenance_interval_hours=self.maintenance_interval_hours,
            deprecated_after_hours=self.deprecated_after_hours,
        )
        self._controls_vector_store = _PersistentVectorFileStore(
            path=str(self.controls_vector_db_path),
            store_kind="controls_only",
            vector_dimensions=self.vector_dimensions,
            maintenance_interval_hours=self.maintenance_interval_hours,
            deprecated_after_hours=self.deprecated_after_hours,
        )
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
            self._replace_vector_files_locked(entries=entries, reason="full_rebuild")

    def sync_entry(self, *, entry_key: str, row: Dict[str, Any]) -> None:
        if not str(entry_key).strip() or not isinstance(row, dict):
            return
        with self._lock:
            with self._connect() as conn:
                self._sync_entry_locked(conn, entry_key=str(entry_key).strip(), row=dict(row))
                conn.commit()
            detail_items, control_items = self._vector_items_from_entry(entry_key=str(entry_key).strip(), row=dict(row))
            self._detail_vector_store.sync_entry(entry_key=str(entry_key).strip(), items=detail_items)
            self._controls_vector_store.sync_entry(entry_key=str(entry_key).strip(), items=control_items)

    def maintenance(
        self,
        *,
        entries: Dict[str, Dict[str, Any]],
        force: bool = False,
        reason: str = "daily_reconcile",
    ) -> Dict[str, Any]:
        with self._lock:
            if not force and not self._maintenance_due_locked():
                return self._maintenance_summary_locked(status="success", performed=False)
            self._replace_vector_files_locked(entries=entries, reason=reason)
            return self._maintenance_summary_locked(status="success", performed=True)

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
                detail_vector_count = len(
                    self._detail_vector_store.filtered_items(
                        app_name=app_name,
                        profile_id=profile_id,
                        category=category,
                    )
                )
                controls_vector_count = len(
                    self._controls_vector_store.filtered_items(
                        app_name=app_name,
                        profile_id=profile_id,
                        category=category,
                    )
                )
        maintenance = self._maintenance_summary_locked(status="success", performed=False)
        return {
            "status": "success",
            "entry_count": entry_count,
            "control_count": control_count,
            "command_count": command_count,
            "vector_count": detail_vector_count,
            "details_vector_count": detail_vector_count,
            "controls_vector_count": controls_vector_count,
            "db_path": str(self.db_path),
            "temporary_sqlite_path": str(self.db_path),
            "detail_vector_db_path": str(self.detail_vector_db_path),
            "controls_vector_db_path": str(self.controls_vector_db_path),
            "vector_db_paths": [str(self.detail_vector_db_path), str(self.controls_vector_db_path)],
            "permanent_vector_db_count": 2,
            "vector_dimensions": self.vector_dimensions,
            "maintenance": maintenance,
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
        rows: List[Dict[str, Any]] = []
        with self._lock:
            detail_candidates = self._detail_vector_store.filtered_items(
                app_name=app_name,
                profile_id=profile_id,
                entity_types=sorted(allowed_types),
            )[: self.max_match_scan]
            compact_candidates = self._controls_vector_store.filtered_items(
                app_name=app_name,
                profile_id=profile_id,
                entity_types=sorted(allowed_types),
            )[: self.max_match_scan]
            merged: Dict[str, Dict[str, Any]] = {}
            for candidate in [*detail_candidates, *compact_candidates]:
                text_payload = str(candidate.get("text_payload", "") or "")
                token_overlap = len(query_tokens.intersection(self._tokenize(text_payload)))
                lexical_bonus = min(token_overlap, 4) * 0.08
                vector = candidate.get("vector", []) if isinstance(candidate.get("vector", []), list) else []
                similarity = self._cosine_similarity(query_vector, vector)
                store_bonus = 0.03 if str(candidate.get("store_kind", "") or "").strip().lower() == "controls_only" else 0.0
                score = round(similarity + lexical_bonus + store_bonus, 4)
                entity_key = str(candidate.get("entity_key", "") or "").strip()
                if not entity_key:
                    continue
                current = merged.get(entity_key)
                if current and float(current.get("similarity", 0.0) or 0.0) >= score:
                    continue
                merged[entity_key] = {
                    "entity_key": entity_key,
                    "entity_type": str(candidate.get("entity_type", "") or ""),
                    "app_name": str(candidate.get("app_name", "") or ""),
                    "profile_id": str(candidate.get("profile_id", "") or ""),
                    "label": str(candidate.get("label", "") or ""),
                    "control_type": str(candidate.get("control_type", "") or ""),
                    "semantic_role": str(candidate.get("semantic_role", "") or ""),
                    "container_role": str(candidate.get("container_role", "") or ""),
                    "source": str(candidate.get("source", "") or ""),
                    "vector_store_kind": str(candidate.get("store_kind", "") or ""),
                    "hotkeys": self._parse_json_list(candidate.get("hotkeys", [])),
                    "text_payload": text_payload,
                    "similarity": score,
                    "token_overlap": token_overlap,
                    "updated_at": str(candidate.get("updated_at", "") or ""),
                }
            rows = list(merged.values())
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
            self._detail_vector_store.reset(entry_keys=clean_entry_keys)
            self._controls_vector_store.reset(entry_keys=clean_entry_keys)

    def _replace_vector_files_locked(self, *, entries: Dict[str, Dict[str, Any]], reason: str) -> None:
        detail_items: List[Dict[str, Any]] = []
        control_items: List[Dict[str, Any]] = []
        for entry_key, row in entries.items():
            if not str(entry_key).strip() or not isinstance(row, dict):
                continue
            detail_rows, control_rows = self._vector_items_from_entry(entry_key=str(entry_key).strip(), row=dict(row))
            detail_items.extend(detail_rows)
            control_items.extend(control_rows)
        old_detail = {str(item.get("entity_key", "") or "").strip(): item for item in self._detail_vector_store.items()}
        old_controls = {str(item.get("entity_key", "") or "").strip(): item for item in self._controls_vector_store.items()}
        new_detail = {str(item.get("entity_key", "") or "").strip(): item for item in detail_items if str(item.get("entity_key", "") or "").strip()}
        new_controls = {str(item.get("entity_key", "") or "").strip(): item for item in control_items if str(item.get("entity_key", "") or "").strip()}
        removed_detail_labels = [
            str(old_detail[key].get("label", "") or key).strip()
            for key in sorted(set(old_detail) - set(new_detail))
            if key in old_detail
        ][:12]
        removed_control_labels = [
            str(old_controls[key].get("label", "") or key).strip()
            for key in sorted(set(old_controls) - set(new_controls))
            if key in old_controls
        ][:12]
        maintenance = {
            "reason": reason,
            "entry_count": len(entries),
        }
        self._detail_vector_store.replace_all(
            items=detail_items,
            maintenance={
                **maintenance,
                "added_count": len(set(new_detail) - set(old_detail)),
                "removed_count": len(set(old_detail) - set(new_detail)),
                "removed_labels": removed_detail_labels,
            },
        )
        self._controls_vector_store.replace_all(
            items=control_items,
            maintenance={
                **maintenance,
                "added_count": len(set(new_controls) - set(old_controls)),
                "removed_count": len(set(old_controls) - set(new_controls)),
                "removed_labels": removed_control_labels,
            },
        )

    def _vector_items_from_entry(self, *, entry_key: str, row: Dict[str, Any]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        detail_items: List[Dict[str, Any]] = []
        control_items: List[Dict[str, Any]] = []
        app_name = str(row.get("app_name", "") or "").strip()
        profile_id = str(row.get("profile_id", "") or "").strip().lower()
        category = str(row.get("category", "") or "").strip().lower()
        updated_at = str(row.get("updated_at", "") or "").strip()
        controls = row.get("controls", {}) if isinstance(row.get("controls", {}), dict) else {}
        commands = row.get("learned_commands", {}) if isinstance(row.get("learned_commands", {}), dict) else {}
        shortcuts = row.get("shortcut_actions", {}) if isinstance(row.get("shortcut_actions", {}), dict) else {}
        deprecated_terms: set[str] = set()
        for payload in [*controls.values(), *commands.values(), *shortcuts.values()]:
            if self._entity_is_active(payload, entry=row):
                continue
            stale_row = dict(payload) if isinstance(payload, dict) else {}
            for raw_term in [
                stale_row.get("label", ""),
                stale_row.get("name", ""),
                stale_row.get("command", ""),
                stale_row.get("automation_id", ""),
                *self._parse_json_list(stale_row.get("label_variants", [])),
                *self._parse_json_list(stale_row.get("command_aliases", [])),
            ]:
                clean_term = self._normalize_text(raw_term)
                if clean_term:
                    deprecated_terms.add(clean_term)
        for control_key, payload in controls.items():
            if not self._entity_is_active(payload, entry=row):
                continue
            control_row = dict(payload) if isinstance(payload, dict) else {}
            label = str(control_row.get("label", "") or control_row.get("name", "") or control_key).strip()
            if not label:
                continue
            if self._normalize_text(label) in deprecated_terms:
                continue
            hotkeys = self._dedupe_strings(
                [
                    *self._parse_json_list(control_row.get("hotkeys", [])),
                    str(control_row.get("accelerator_key", "") or "").strip(),
                    str(control_row.get("access_key", "") or "").strip(),
                ]
            )[:12]
            entity_key = f"control:{entry_key}:{control_key}"
            detail_text_payload = self._build_text_payload(
                app_name=app_name,
                label=label,
                control_type=str(control_row.get("control_type", "") or "").strip().lower(),
                semantic_role=str(control_row.get("semantic_role", control_row.get("learned_role", "")) or "").strip().lower(),
                container_role=str(control_row.get("container_role", "") or "").strip().lower(),
                hotkeys=hotkeys,
                extra_terms=[
                    *self._parse_json_list(control_row.get("command_aliases", [])),
                    *self._parse_json_list(control_row.get("label_variants", [])),
                    *self._parse_json_list(control_row.get("query_examples", [])),
                    str(control_row.get("automation_id", "") or "").strip(),
                    str(control_row.get("effect_kind", control_row.get("last_probe_effect", "")) or "").strip(),
                ],
            )
            compact_text_payload = self._build_text_payload(
                app_name=app_name,
                label=label,
                control_type=str(control_row.get("control_type", "") or "").strip().lower(),
                semantic_role=str(control_row.get("semantic_role", control_row.get("learned_role", "")) or "").strip().lower(),
                container_role=str(control_row.get("container_role", "") or "").strip().lower(),
                hotkeys=hotkeys,
            )
            detail_row = {
                "entity_key": entity_key,
                "entry_key": entry_key,
                "entity_type": "control",
                "app_name": app_name,
                "normalized_app_name": self._normalize_text(app_name),
                "profile_id": profile_id,
                "category": category,
                "label": label,
                "control_type": str(control_row.get("control_type", "") or "").strip().lower(),
                "semantic_role": str(control_row.get("semantic_role", control_row.get("learned_role", "")) or "").strip().lower(),
                "container_role": str(control_row.get("container_role", "") or "").strip().lower(),
                "source": "control",
                "hotkeys": hotkeys,
                "text_payload": detail_text_payload,
                "vector": self._embed_text(detail_text_payload),
                "updated_at": updated_at,
                "last_seen_at": str(control_row.get("last_seen_at", "") or "").strip(),
                "details": {
                    "automation_id": str(control_row.get("automation_id", "") or "").strip(),
                    "effect_kind": str(control_row.get("effect_kind", control_row.get("last_probe_effect", "")) or "").strip(),
                    "verification_confidence": round(
                        max(
                            0.0,
                            min(
                                float(
                                    control_row.get(
                                        "last_verification_confidence",
                                        control_row.get("verification_confidence", control_row.get("confidence", 0.0)),
                                    )
                                    or 0.0
                                ),
                                1.0,
                            ),
                        ),
                        4,
                    ),
                    "query_examples": self._parse_json_list(control_row.get("query_examples", []))[:8],
                    "label_variants": self._parse_json_list(control_row.get("label_variants", []))[:8],
                    "command_aliases": self._parse_json_list(control_row.get("command_aliases", []))[:8],
                },
            }
            detail_items.append(detail_row)
            control_items.append(
                {
                    **detail_row,
                    "text_payload": compact_text_payload,
                    "vector": self._embed_text(compact_text_payload),
                    "details": {
                        "automation_id": str(control_row.get("automation_id", "") or "").strip(),
                        "hotkeys": hotkeys,
                    },
                }
            )
        for source_name, command_map in (("learned_command", commands), ("shortcut", shortcuts)):
            for command_key, payload in command_map.items():
                if not self._entity_is_active(payload, entry=row):
                    continue
                command_row = dict(payload) if isinstance(payload, dict) else {}
                label = str(
                    command_row.get("label", "")
                    or command_row.get("name", "")
                    or command_row.get("command", "")
                    or command_key
                ).strip()
                if not label:
                    continue
                if self._normalize_text(label) in deprecated_terms:
                    continue
                hotkeys = self._dedupe_strings(self._parse_json_list(command_row.get("hotkeys", [])))[:12]
                entity_key = f"command:{entry_key}:{source_name}:{command_key}"
                detail_text_payload = self._build_text_payload(
                    app_name=app_name,
                    label=label,
                    control_type="command",
                    semantic_role=str(command_row.get("semantic_role", "") or "").strip().lower(),
                    container_role="command",
                    hotkeys=hotkeys,
                    extra_terms=[
                        str(command_row.get("source", "") or source_name).strip(),
                        str(command_row.get("message", "") or "").strip(),
                        str(command_row.get("title", "") or "").strip(),
                    ],
                )
                compact_text_payload = self._build_text_payload(
                    app_name=app_name,
                    label=label,
                    control_type="command",
                    semantic_role=str(command_row.get("semantic_role", "") or "").strip().lower(),
                    container_role="command",
                    hotkeys=hotkeys,
                )
                detail_row = {
                    "entity_key": entity_key,
                    "entry_key": entry_key,
                    "entity_type": "command",
                    "app_name": app_name,
                    "normalized_app_name": self._normalize_text(app_name),
                    "profile_id": profile_id,
                    "category": category,
                    "label": label,
                    "control_type": "command",
                    "semantic_role": str(command_row.get("semantic_role", "") or "").strip().lower(),
                    "container_role": "command",
                    "source": source_name,
                    "hotkeys": hotkeys,
                    "text_payload": detail_text_payload,
                    "vector": self._embed_text(detail_text_payload),
                    "updated_at": updated_at,
                    "last_seen_at": str(command_row.get("last_seen_at", "") or "").strip(),
                    "details": {
                        "confidence": round(
                            max(
                                0.0,
                                min(float(command_row.get("confidence", command_row.get("verification_confidence", 0.0)) or 0.0), 1.0),
                            ),
                            4,
                        ),
                        "source": str(command_row.get("source", "") or source_name).strip(),
                    },
                }
                detail_items.append(detail_row)
                control_items.append(
                    {
                        **detail_row,
                        "text_payload": compact_text_payload,
                        "vector": self._embed_text(compact_text_payload),
                        "details": {
                            "source": str(command_row.get("source", "") or source_name).strip(),
                            "hotkeys": hotkeys,
                        },
                    }
                )
        return detail_items, control_items

    def _entity_is_active(self, payload: Any, *, entry: Dict[str, Any]) -> bool:
        row = dict(payload) if isinstance(payload, dict) else {}
        if not row:
            return False
        explicit_status = str(row.get("status", "") or "").strip().lower()
        if explicit_status in {"deprecated", "removed", "retired", "obsolete"}:
            return False
        if bool(row.get("deprecated", False)) or bool(row.get("removed", False)):
            return False
        if "active" in row and not bool(row.get("active", True)):
            return False
        updated_at = self._parse_iso(entry.get("updated_at", "")) or datetime.now(timezone.utc)
        last_seen_at = self._parse_iso(
            row.get("last_seen_at", "")
            or row.get("last_probe_at", "")
            or row.get("updated_at", "")
            or entry.get("updated_at", "")
        )
        if last_seen_at is None:
            return True
        survey_count = self._coerce_int(
            dict(entry.get("metrics", {})).get("survey_count", 0)
            if isinstance(entry.get("metrics", {}), dict)
            else 0,
            minimum=0,
            maximum=10_000_000,
            default=0,
        )
        lag_hours = max(0.0, (updated_at - last_seen_at).total_seconds() / 3600.0)
        age_hours = max(0.0, (datetime.now(timezone.utc) - last_seen_at).total_seconds() / 3600.0)
        blocked_error_pressure = (
            self._coerce_int(row.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
            + self._coerce_int(row.get("error_count", 0), minimum=0, maximum=10_000_000, default=0)
            + self._coerce_int(row.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
            + self._coerce_int(row.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0)
        )
        revalidation_due = bool(row.get("revalidation_due", False))
        if lag_hours >= self.deprecated_after_hours and survey_count >= 2:
            return False
        if revalidation_due and age_hours >= self.deprecated_after_hours:
            return False
        if blocked_error_pressure > 0 and age_hours >= max(24.0, self.deprecated_after_hours / 2.0):
            return False
        return True

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
            self._insert_control(
                conn,
                entry_key=entry_key,
                app_name=app_name,
                profile_id=profile_id,
                control_key=str(control_key),
                payload=payload,
                updated_at=updated_at,
            )
        for command_key, payload in commands.items():
            self._insert_command(
                conn,
                entry_key=entry_key,
                app_name=app_name,
                profile_id=profile_id,
                command_key=str(command_key),
                payload=payload,
                updated_at=updated_at,
                source="learned_command",
            )
        for shortcut_key, payload in shortcuts.items():
            self._insert_command(
                conn,
                entry_key=entry_key,
                app_name=app_name,
                profile_id=profile_id,
                command_key=str(shortcut_key),
                payload=payload,
                updated_at=updated_at,
                source="shortcut",
            )

    def _insert_control(self, conn: sqlite3.Connection, *, entry_key: str, app_name: str, profile_id: str, control_key: str, payload: Any, updated_at: str) -> None:
        row = dict(payload) if isinstance(payload, dict) else {}
        label = str(row.get("label", "") or row.get("name", "") or control_key).strip()
        if not label:
            return
        hotkeys = self._dedupe_strings(
            [
                *self._parse_json_list(row.get("hotkeys", [])),
                str(row.get("accelerator_key", "") or "").strip(),
                str(row.get("access_key", "") or "").strip(),
            ]
        )[:12]
        entity_key = f"control:{entry_key}:{control_key}"
        text_payload = self._build_text_payload(
            app_name=app_name,
            label=label,
            control_type=str(row.get("control_type", "") or "").strip().lower(),
            semantic_role=str(row.get("semantic_role", row.get("learned_role", "")) or "").strip().lower(),
            container_role=str(row.get("container_role", "") or "").strip().lower(),
            hotkeys=hotkeys,
            extra_terms=self._parse_json_list(row.get("command_aliases", [])),
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
                str(row.get("semantic_role", row.get("learned_role", "")) or "").strip().lower(),
                str(row.get("container_role", "") or "").strip().lower(),
                str(row.get("automation_id", "") or "").strip(),
                str(row.get("effect_kind", row.get("last_probe_effect", "")) or "").strip().lower(),
                float(row.get("last_verification_confidence", row.get("verification_confidence", row.get("confidence", 0.0))) or 0.0),
                json.dumps(hotkeys, ensure_ascii=True),
                updated_at,
                json.dumps(row, ensure_ascii=True, sort_keys=True),
            ),
        )
        self._insert_vector(
            conn,
            entry_key=entry_key,
            entity_key=entity_key,
            entity_type="control",
            app_name=app_name,
            profile_id=profile_id,
            label=label,
            control_type=str(row.get("control_type", "") or "").strip().lower(),
            semantic_role=str(row.get("semantic_role", row.get("learned_role", "")) or "").strip().lower(),
            container_role=str(row.get("container_role", "") or "").strip().lower(),
            hotkeys=hotkeys,
            source="control",
            text_payload=text_payload,
            updated_at=updated_at,
        )

    def _insert_command(self, conn: sqlite3.Connection, *, entry_key: str, app_name: str, profile_id: str, command_key: str, payload: Any, updated_at: str, source: str) -> None:
        row = dict(payload) if isinstance(payload, dict) else {}
        label = str(row.get("label", "") or row.get("name", "") or row.get("command", "") or command_key).strip()
        if not label:
            return
        hotkeys = self._dedupe_strings(self._parse_json_list(row.get("hotkeys", [])))[:12]
        entity_key = f"command:{entry_key}:{source}:{command_key}"
        text_payload = self._build_text_payload(
            app_name=app_name,
            label=label,
            control_type="command",
            semantic_role=str(row.get("semantic_role", "") or "").strip().lower(),
            container_role="command",
            hotkeys=hotkeys,
            extra_terms=[str(source or "").strip()],
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
        self._insert_vector(
            conn,
            entry_key=entry_key,
            entity_key=entity_key,
            entity_type="command",
            app_name=app_name,
            profile_id=profile_id,
            label=label,
            control_type="command",
            semantic_role=str(row.get("semantic_role", "") or "").strip().lower(),
            container_role="command",
            hotkeys=hotkeys,
            source=source,
            text_payload=text_payload,
            updated_at=updated_at,
        )

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

    def _maintenance_due_locked(self) -> bool:
        return self._detail_vector_store.maintenance_due() or self._controls_vector_store.maintenance_due()

    def _maintenance_summary_locked(self, *, status: str, performed: bool) -> Dict[str, Any]:
        detail_state = self._detail_vector_store.maintenance_state()
        control_state = self._controls_vector_store.maintenance_state()
        last_maintenance_at = max(
            str(detail_state.get("last_maintenance_at", "") or "").strip(),
            str(control_state.get("last_maintenance_at", "") or "").strip(),
        )
        return {
            "status": status,
            "performed": bool(performed),
            "maintenance_due": bool(detail_state.get("maintenance_due", False) or control_state.get("maintenance_due", False)),
            "maintenance_interval_hours": self.maintenance_interval_hours,
            "deprecated_after_hours": self.deprecated_after_hours,
            "last_maintenance_at": last_maintenance_at,
            "details_vector_db": detail_state,
            "controls_vector_db": control_state,
            "removed_count": max(0, int(detail_state.get("last_maintenance_removed_count", 0) or 0))
            + max(0, int(control_state.get("last_maintenance_removed_count", 0) or 0)),
            "added_count": max(0, int(detail_state.get("last_maintenance_added_count", 0) or 0))
            + max(0, int(control_state.get("last_maintenance_added_count", 0) or 0)),
        }

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
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            slot = int.from_bytes(digest[:8], "big", signed=False) % self.vector_dimensions
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
    def _parse_iso(cls, value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _parse_json_list(cls, raw: Any) -> List[str]:
        if isinstance(raw, list):
            return [str(item).strip() for item in raw if str(item).strip()]
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
        extra_terms: Sequence[str] | None = None,
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
                " ".join(str(item).strip() for item in (extra_terms or []) if str(item).strip()),
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

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            numeric = int(value)
        except Exception:
            return default
        return max(minimum, min(maximum, numeric))

    @staticmethod
    def _derive_vector_path(db_path: Path, suffix: str) -> str:
        clean_suffix = str(suffix or "").strip() or "vector_db"
        if db_path.suffix:
            return str(db_path.with_name(f"{db_path.stem}.{clean_suffix}.json"))
        return str(db_path.with_name(f"{db_path.name}.{clean_suffix}.json"))
