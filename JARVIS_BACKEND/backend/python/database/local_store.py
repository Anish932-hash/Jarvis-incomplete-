import asyncio
import copy
import json
import os
import threading
import time
from typing import Any, Dict, List, Optional


class LocalStore:
    """
    Local persistent store with atomic commits, optional TTL, and transactional updates.
    Backward compatible with the legacy plain key/value JSON layout.
    """

    _META_KEY = "__localstore_meta__"

    def __init__(self, path: str, *, auto_cleanup_expired: bool = True) -> None:
        self.path = path
        self.journal_path = f"{path}.journal"
        self.tmp_path = f"{path}.tmp"
        self._lock = threading.RLock()
        self._auto_cleanup_expired = bool(auto_cleanup_expired)
        self.cache: Dict[str, Any] = {}
        self._entry_meta: Dict[str, Dict[str, Any]] = {}
        self._store_version = 0
        self._last_compacted_at = 0.0
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.journal_path) and not os.path.exists(self.path):
            self._recover_journal()
        if os.path.exists(self.tmp_path) and not os.path.exists(self.path):
            os.replace(self.tmp_path, self.path)

        if not os.path.exists(self.path):
            self.cache = {}
            self._entry_meta = {}
            self._store_version = 0
            return

        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            self.cache = {}
            self._entry_meta = {}
            self._store_version = 0
            return

        if isinstance(payload, dict) and "values" in payload and isinstance(payload.get("values"), dict):
            self.cache = dict(payload.get("values", {}))
            meta = payload.get(self._META_KEY) if isinstance(payload.get(self._META_KEY), dict) else {}
            self._entry_meta = dict(meta.get("entries", {})) if isinstance(meta.get("entries"), dict) else {}
            self._store_version = self._to_int(meta.get("version", 0), default=0, minimum=0, maximum=2_000_000_000)
            self._last_compacted_at = float(meta.get("last_compacted_at", 0.0) or 0.0)
        elif isinstance(payload, dict):
            # Legacy layout: plain key->value map.
            self.cache = dict(payload)
            self._entry_meta = {}
            self._store_version = 0
            self._last_compacted_at = 0.0
        else:
            self.cache = {}
            self._entry_meta = {}
            self._store_version = 0
            self._last_compacted_at = 0.0

        if self._auto_cleanup_expired:
            self._cleanup_expired_locked(now=time.time())

    def _recover_journal(self) -> None:
        try:
            with open(self.journal_path, "r", encoding="utf-8") as jf:
                payload = json.load(jf)
            self._write_payload_atomic(payload)
        finally:
            if os.path.exists(self.journal_path):
                os.remove(self.journal_path)

    def _serialize_locked(self) -> Dict[str, Any]:
        return {
            "values": self.cache,
            self._META_KEY: {
                "version": int(self._store_version),
                "entries": self._entry_meta,
                "last_compacted_at": float(self._last_compacted_at),
                "updated_at": float(time.time()),
            },
        }

    def _write_payload_atomic(self, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)) or ".", exist_ok=True)
        with open(self.tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        os.replace(self.tmp_path, self.path)

    def _persist_locked(self) -> None:
        self._write_payload_atomic(self._serialize_locked())

    @staticmethod
    def _to_int(raw: Any, *, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(raw)
        except Exception:
            value = default
        return max(minimum, min(maximum, value))

    def _is_expired_locked(self, key: str, *, now: float) -> bool:
        meta = self._entry_meta.get(key)
        if not isinstance(meta, dict):
            return False
        expires_at = meta.get("expires_at")
        if expires_at is None:
            return False
        try:
            return float(expires_at) <= float(now)
        except Exception:
            return False

    def _cleanup_expired_locked(self, *, now: float) -> int:
        expired = [key for key in list(self.cache.keys()) if self._is_expired_locked(key, now=now)]
        if not expired:
            return 0
        for key in expired:
            self.cache.pop(key, None)
            self._entry_meta.pop(key, None)
        self._store_version += 1
        self._last_compacted_at = float(now)
        self._persist_locked()
        return len(expired)

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl_s: Optional[float] = None,
        expected_version: Optional[int] = None,
    ) -> Dict[str, Any]:
        clean_key = str(key or "").strip()
        if not clean_key:
            raise ValueError("key is required")
        now = time.time()
        with self._lock:
            current_meta = self._entry_meta.get(clean_key, {})
            current_version = self._to_int(current_meta.get("version", 0), default=0, minimum=0, maximum=2_000_000_000)
            if expected_version is not None and int(expected_version) != current_version:
                return {
                    "status": "conflict",
                    "key": clean_key,
                    "expected_version": int(expected_version),
                    "current_version": current_version,
                }
            expires_at = None
            if ttl_s is not None:
                ttl = max(0.0, float(ttl_s))
                if ttl > 0.0:
                    expires_at = now + ttl
            self.cache[clean_key] = value
            self._entry_meta[clean_key] = {
                "version": current_version + 1,
                "updated_at": now,
                "expires_at": expires_at,
            }
            self._store_version += 1
            self._persist_locked()
            return {
                "status": "success",
                "key": clean_key,
                "version": current_version + 1,
                "expires_at": expires_at,
            }

    def compare_and_set(self, key: str, expected_value: Any, new_value: Any, *, ttl_s: Optional[float] = None) -> bool:
        clean_key = str(key or "").strip()
        if not clean_key:
            return False
        with self._lock:
            current = self.get(clean_key, default=None)
            if current != expected_value:
                return False
            self.set(clean_key, new_value, ttl_s=ttl_s)
            return True

    def get(self, key: str, default: Any = None) -> Any:
        clean_key = str(key or "").strip()
        if not clean_key:
            return default
        with self._lock:
            now = time.time()
            if self._is_expired_locked(clean_key, now=now):
                self.cache.pop(clean_key, None)
                self._entry_meta.pop(clean_key, None)
                self._store_version += 1
                self._persist_locked()
                return default
            return self.cache.get(clean_key, default)

    def get_with_meta(self, key: str, default: Any = None) -> Dict[str, Any]:
        clean_key = str(key or "").strip()
        if not clean_key:
            return {"found": False, "value": default, "meta": {}}
        with self._lock:
            value = self.get(clean_key, default=default)
            if value is default and clean_key not in self.cache:
                return {"found": False, "value": default, "meta": {}}
            meta = dict(self._entry_meta.get(clean_key, {}))
            return {"found": True, "value": value, "meta": meta}

    def delete(self, key: str) -> bool:
        clean_key = str(key or "").strip()
        if not clean_key:
            return False
        with self._lock:
            existed = clean_key in self.cache
            if existed:
                self.cache.pop(clean_key, None)
                self._entry_meta.pop(clean_key, None)
                self._store_version += 1
                self._persist_locked()
            return existed

    def all(self, *, include_meta: bool = False) -> Dict[str, Any]:
        with self._lock:
            if self._auto_cleanup_expired:
                self._cleanup_expired_locked(now=time.time())
            if not include_meta:
                return dict(self.cache)
            return {
                "values": dict(self.cache),
                "meta": copy.deepcopy(self._entry_meta),
                "store_version": int(self._store_version),
            }

    def cleanup_expired(self) -> int:
        with self._lock:
            return self._cleanup_expired_locked(now=time.time())

    def transact(self, operations: List[Dict[str, Any]], *, strict: bool = True) -> Dict[str, Any]:
        if not isinstance(operations, list) or not operations:
            return {"status": "error", "message": "operations must be a non-empty list"}

        with self._lock:
            snapshot_cache = copy.deepcopy(self.cache)
            snapshot_meta = copy.deepcopy(self._entry_meta)
            snapshot_version = int(self._store_version)
            applied = 0
            errors: List[Dict[str, Any]] = []

            for index, op in enumerate(operations):
                if not isinstance(op, dict):
                    errors.append({"index": index, "message": "operation must be a dict"})
                    if strict:
                        break
                    continue

                action = str(op.get("action", "set")).strip().lower() or "set"
                key = str(op.get("key", "")).strip()
                if not key:
                    errors.append({"index": index, "message": "operation key is required"})
                    if strict:
                        break
                    continue

                try:
                    if action == "set":
                        self.set(
                            key,
                            op.get("value"),
                            ttl_s=op.get("ttl_s"),
                            expected_version=op.get("expected_version"),
                        )
                        applied += 1
                    elif action == "delete":
                        self.delete(key)
                        applied += 1
                    elif action == "compare_and_set":
                        if not self.compare_and_set(
                            key,
                            op.get("expected_value"),
                            op.get("value"),
                            ttl_s=op.get("ttl_s"),
                        ):
                            raise ValueError("compare_and_set condition failed")
                        applied += 1
                    else:
                        raise ValueError(f"unsupported action: {action}")
                except Exception as exc:  # noqa: BLE001
                    errors.append({"index": index, "message": str(exc), "action": action, "key": key})
                    if strict:
                        break

            if errors and strict:
                self.cache = snapshot_cache
                self._entry_meta = snapshot_meta
                self._store_version = snapshot_version
                self._persist_locked()
                return {
                    "status": "rolled_back",
                    "applied": 0,
                    "errors": errors,
                    "strict": True,
                }
            return {
                "status": "success" if not errors else "partial",
                "applied": applied,
                "errors": errors,
                "strict": bool(strict),
                "store_version": int(self._store_version),
            }

    async def aset(
        self,
        key: str,
        value: Any,
        *,
        ttl_s: Optional[float] = None,
        expected_version: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(self.set, key, value, ttl_s=ttl_s, expected_version=expected_version)

    async def aget(self, key: str, default: Any = None) -> Any:
        return await asyncio.to_thread(self.get, key, default)

    async def adelete(self, key: str) -> bool:
        return await asyncio.to_thread(self.delete, key)

    async def atransact(self, operations: List[Dict[str, Any]], *, strict: bool = True) -> Dict[str, Any]:
        return await asyncio.to_thread(self.transact, operations, strict=strict)
