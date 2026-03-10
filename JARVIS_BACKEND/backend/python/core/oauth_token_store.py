from __future__ import annotations

import base64
import json
import os
import secrets
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock, RLock
from typing import Any, Callable, Dict, List, Optional, Tuple


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:  # noqa: BLE001
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_provider(provider: str) -> str:
    return str(provider or "").strip().lower()


def _normalize_account(account_id: str) -> str:
    clean = str(account_id or "").strip().lower()
    return clean or "default"


_TOKEN_PROTECTED_PREFIX = "dpapi:v1:"


@dataclass(slots=True)
class OAuthTokenRecord:
    provider: str
    account_id: str
    access_token: str
    refresh_token: str = ""
    token_type: str = "Bearer"
    scopes: List[str] = field(default_factory=list)
    expires_at: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: _iso(_utc_now()))
    updated_at: str = field(default_factory=lambda: _iso(_utc_now()))
    last_refreshed_at: str = ""
    last_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_public_dict(self) -> Dict[str, Any]:
        access_suffix = self.access_token[-6:] if len(self.access_token) >= 6 else self.access_token
        refresh_suffix = self.refresh_token[-6:] if len(self.refresh_token) >= 6 else self.refresh_token
        payload = {
            "provider": self.provider,
            "account_id": self.account_id,
            "token_type": self.token_type,
            "scopes": list(self.scopes),
            "expires_at": self.expires_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_refreshed_at": self.last_refreshed_at,
            "last_error": self.last_error,
            "has_access_token": bool(self.access_token),
            "has_refresh_token": bool(self.refresh_token),
            "access_token_suffix": access_suffix,
            "refresh_token_suffix": refresh_suffix,
            "metadata": dict(self.metadata),
        }
        payload["expires_in_s"] = self.expires_in_seconds()
        payload["is_expired"] = payload["expires_in_s"] <= 0 if isinstance(payload["expires_in_s"], int) else False
        return payload

    def expires_in_seconds(self) -> Optional[int]:
        expires_dt = _parse_iso(self.expires_at)
        if expires_dt is None:
            return None
        delta = expires_dt - _utc_now()
        return int(delta.total_seconds())


OAuthRefresher = Callable[[Dict[str, Any]], Dict[str, Any]]


class OAuthTokenStore:
    """
    Persistent OAuth token lifecycle manager:
    - upsert/list/revoke
    - expiry-aware token resolution
    - optional refresh token rotation
    """

    _shared_instance: OAuthTokenStore | None = None
    _shared_lock = Lock()

    def __init__(self, *, store_path: str = "data/oauth_tokens.json", max_records: int = 1000) -> None:
        self.store_path = Path(store_path)
        self.max_records = max(100, min(int(max_records), 50_000))
        self._records: Dict[Tuple[str, str], OAuthTokenRecord] = {}
        self._refreshers: Dict[str, OAuthRefresher] = {}
        self._secret_protection_enabled = self._env_flag("JARVIS_OAUTH_TOKEN_PROTECT", default=True)
        self._secret_entropy = str(os.getenv("JARVIS_OAUTH_TOKEN_ENTROPY", "jarvis_oauth_tokens"))
        self._lock = RLock()
        self._load_locked()

    @classmethod
    def shared(cls, store_path: Optional[str] = None) -> OAuthTokenStore:
        with cls._shared_lock:
            if cls._shared_instance is None:
                cls._shared_instance = cls(store_path=store_path or "data/oauth_tokens.json")
            elif store_path:
                desired = str(store_path).strip()
                current = str(cls._shared_instance.store_path)
                if desired and desired != current:
                    cls._shared_instance = cls(store_path=desired)
            return cls._shared_instance

    def register_refresher(self, provider: str, refresher: OAuthRefresher) -> None:
        normalized = _normalize_provider(provider)
        if not normalized:
            raise ValueError("provider is required")
        if not callable(refresher):
            raise TypeError("refresher must be callable")
        with self._lock:
            self._refreshers[normalized] = refresher

    def upsert(
        self,
        *,
        provider: str,
        account_id: str = "default",
        access_token: str,
        refresh_token: str = "",
        token_type: str = "Bearer",
        scopes: Optional[List[str]] = None,
        expires_at: str = "",
        expires_in_s: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_provider = _normalize_provider(provider)
        normalized_account = _normalize_account(account_id)
        clean_access = str(access_token or "").strip()
        clean_refresh = str(refresh_token or "").strip()
        if not normalized_provider:
            return {"status": "error", "message": "provider is required"}
        if not clean_access:
            return {"status": "error", "message": "access_token is required"}

        resolved_expiry = self._resolve_expires_at(expires_at=expires_at, expires_in_s=expires_in_s)
        now_iso = _iso(_utc_now())
        clean_scopes = self._normalize_scopes(scopes)
        clean_metadata = dict(metadata or {})
        key = (normalized_provider, normalized_account)

        with self._lock:
            existing = self._records.get(key)
            if existing is None:
                record = OAuthTokenRecord(
                    provider=normalized_provider,
                    account_id=normalized_account,
                    access_token=clean_access,
                    refresh_token=clean_refresh,
                    token_type=str(token_type or "Bearer").strip() or "Bearer",
                    scopes=clean_scopes,
                    expires_at=resolved_expiry,
                    metadata=clean_metadata,
                    created_at=now_iso,
                    updated_at=now_iso,
                )
            else:
                existing.access_token = clean_access
                if clean_refresh:
                    existing.refresh_token = clean_refresh
                existing.token_type = str(token_type or existing.token_type or "Bearer").strip() or "Bearer"
                existing.scopes = clean_scopes
                existing.expires_at = resolved_expiry
                existing.metadata = clean_metadata
                existing.updated_at = now_iso
                existing.last_error = ""
                record = existing
            self._records[key] = record
            self._trim_locked()
            self._save_locked()
            return {"status": "success", "token": record.to_public_dict()}

    def list(
        self,
        *,
        provider: str = "",
        account_id: str = "",
        limit: int = 200,
        include_secrets: bool = False,
    ) -> Dict[str, Any]:
        normalized_provider = _normalize_provider(provider)
        normalized_account = _normalize_account(account_id) if str(account_id or "").strip() else ""
        bounded = max(1, min(int(limit), 2000))
        with self._lock:
            rows = list(self._records.values())
        filtered: List[OAuthTokenRecord] = []
        for row in rows:
            if normalized_provider and row.provider != normalized_provider:
                continue
            if normalized_account and row.account_id != normalized_account:
                continue
            filtered.append(row)
        filtered.sort(key=lambda item: item.updated_at, reverse=True)
        out = []
        for item in filtered[:bounded]:
            if include_secrets:
                payload = item.to_dict()
                payload["expires_in_s"] = item.expires_in_seconds()
                out.append(payload)
            else:
                out.append(item.to_public_dict())
        return {"status": "success", "items": out, "count": len(out), "total": len(filtered)}

    def revoke(self, *, provider: str, account_id: str = "default") -> Dict[str, Any]:
        key = (_normalize_provider(provider), _normalize_account(account_id))
        if not key[0]:
            return {"status": "error", "message": "provider is required"}
        with self._lock:
            record = self._records.pop(key, None)
            self._save_locked()
        if record is None:
            return {"status": "error", "message": "token not found"}
        return {
            "status": "success",
            "provider": key[0],
            "account_id": key[1],
            "revoked_at": _iso(_utc_now()),
        }

    def resolve_access_token(
        self,
        *,
        provider: str,
        account_id: str = "default",
        min_ttl_s: int = 120,
        auto_refresh: bool = True,
    ) -> Dict[str, Any]:
        normalized_provider = _normalize_provider(provider)
        normalized_account = _normalize_account(account_id)
        if not normalized_provider:
            return {"status": "error", "message": "provider is required"}

        with self._lock:
            record = self._records.get((normalized_provider, normalized_account))
        if record is None:
            return {"status": "error", "message": "token not found"}

        ttl = record.expires_in_seconds()
        safe_min_ttl = max(0, min(int(min_ttl_s), 86400))
        if ttl is not None and ttl <= safe_min_ttl and auto_refresh:
            refreshed = self.refresh(provider=normalized_provider, account_id=normalized_account)
            if refreshed.get("status") != "success":
                return refreshed
            with self._lock:
                record = self._records.get((normalized_provider, normalized_account))
            if record is None:
                return {"status": "error", "message": "token refresh completed but record is unavailable"}

        if not record.access_token:
            return {"status": "error", "message": "access token is empty"}
        return {
            "status": "success",
            "provider": record.provider,
            "account_id": record.account_id,
            "access_token": record.access_token,
            "token_type": record.token_type,
            "expires_at": record.expires_at,
            "expires_in_s": record.expires_in_seconds(),
            "scopes": list(record.scopes),
        }

    def refresh(self, *, provider: str, account_id: str = "default") -> Dict[str, Any]:
        normalized_provider = _normalize_provider(provider)
        normalized_account = _normalize_account(account_id)
        key = (normalized_provider, normalized_account)
        with self._lock:
            record = self._records.get(key)
            refresher = self._refreshers.get(normalized_provider)
        if record is None:
            return {"status": "error", "message": "token not found"}
        if not record.refresh_token:
            return {"status": "error", "message": "refresh token is not available"}

        refresh_payload: Dict[str, Any]
        if refresher is not None:
            try:
                refresh_payload = refresher(record.to_dict())
            except Exception as exc:  # noqa: BLE001
                self._mark_refresh_error(key=key, message=str(exc))
                return {"status": "error", "message": str(exc)}
        else:
            refresh_payload = self._refresh_with_token_endpoint(record)
            if refresh_payload.get("status") == "error":
                self._mark_refresh_error(key=key, message=str(refresh_payload.get("message", "refresh failed")))
                return refresh_payload

        access_token = str(refresh_payload.get("access_token", "")).strip()
        if not access_token:
            self._mark_refresh_error(key=key, message="refresh response missing access_token")
            return {"status": "error", "message": "refresh response missing access_token"}

        expires_at = self._resolve_expires_at(
            expires_at=str(refresh_payload.get("expires_at", "")).strip(),
            expires_in_s=self._coerce_int(refresh_payload.get("expires_in_s"), default=3600, minimum=30, maximum=86400 * 365),
        )
        refresh_token = str(refresh_payload.get("refresh_token", "")).strip()
        token_type = str(refresh_payload.get("token_type", record.token_type)).strip() or record.token_type or "Bearer"
        scopes = refresh_payload.get("scopes")
        clean_scopes = self._normalize_scopes(scopes if isinstance(scopes, list) else record.scopes)
        now_iso = _iso(_utc_now())

        with self._lock:
            current = self._records.get(key)
            if current is None:
                return {"status": "error", "message": "token not found"}
            current.access_token = access_token
            if refresh_token:
                current.refresh_token = refresh_token
            current.token_type = token_type
            current.scopes = clean_scopes
            current.expires_at = expires_at
            current.last_refreshed_at = now_iso
            current.last_error = ""
            current.updated_at = now_iso
            self._save_locked()
            return {"status": "success", "token": current.to_public_dict()}

    def maintain(
        self,
        *,
        refresh_window_s: int = 300,
        provider: str = "",
        account_id: str = "",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Proactively refresh tokens that are near expiry and have refresh_token available.
        """
        safe_window = self._coerce_int(refresh_window_s, default=300, minimum=0, maximum=86400 * 7)
        normalized_provider = _normalize_provider(provider)
        normalized_account = _normalize_account(account_id) if str(account_id or "").strip() else ""

        with self._lock:
            rows = list(self._records.values())

        candidates: List[Dict[str, Any]] = []
        refreshed: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        for row in rows:
            if normalized_provider and row.provider != normalized_provider:
                continue
            if normalized_account and row.account_id != normalized_account:
                continue
            ttl = row.expires_in_seconds()
            info = {
                "provider": row.provider,
                "account_id": row.account_id,
                "expires_in_s": ttl,
                "expires_at": row.expires_at,
                "has_refresh_token": bool(row.refresh_token),
            }
            if ttl is None:
                skipped.append({**info, "reason": "no_expiry"})
                continue
            if ttl > safe_window:
                skipped.append({**info, "reason": "outside_refresh_window"})
                continue
            if not row.refresh_token:
                skipped.append({**info, "reason": "missing_refresh_token"})
                continue

            candidates.append(info)
            if dry_run:
                continue
            result = self.refresh(provider=row.provider, account_id=row.account_id)
            if result.get("status") == "success":
                refreshed.append(
                    {
                        "provider": row.provider,
                        "account_id": row.account_id,
                        "expires_at": result.get("token", {}).get("expires_at", ""),
                    }
                )
            else:
                errors.append(
                    {
                        "provider": row.provider,
                        "account_id": row.account_id,
                        "message": str(result.get("message", "refresh failed")),
                    }
                )

        return {
            "status": "success" if not errors else "error",
            "dry_run": bool(dry_run),
            "refresh_window_s": safe_window,
            "provider_filter": normalized_provider,
            "account_filter": normalized_account,
            "candidates": candidates,
            "candidate_count": len(candidates),
            "refreshed": refreshed,
            "refreshed_count": len(refreshed),
            "skipped_count": len(skipped),
            "errors": errors,
            "error_count": len(errors),
        }

    def _refresh_with_token_endpoint(self, record: OAuthTokenRecord) -> Dict[str, Any]:
        token_url = str(record.metadata.get("token_url", "")).strip()
        client_id = str(record.metadata.get("client_id", "")).strip()
        client_secret = str(record.metadata.get("client_secret", "")).strip()
        if not token_url or not client_id:
            return {
                "status": "error",
                "message": "refresh callback not registered and metadata is missing token_url/client_id",
            }
        requests = self._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable"}
        scope_value = " ".join(record.scopes) if record.scopes else ""
        payload: Dict[str, Any] = {
            "grant_type": "refresh_token",
            "refresh_token": record.refresh_token,
            "client_id": client_id,
        }
        if client_secret:
            payload["client_secret"] = client_secret
        if scope_value:
            payload["scope"] = scope_value
        try:
            response = requests.post(token_url, data=payload, timeout=20)
            if response.status_code >= 300:
                return {"status": "error", "message": f"token refresh failed: {response.status_code} {response.text[:240]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "access_token": str(data.get("access_token", "")).strip(),
                "refresh_token": str(data.get("refresh_token", "")).strip(),
                "expires_in_s": self._coerce_int(data.get("expires_in"), default=3600, minimum=30, maximum=86400 * 365),
                "token_type": str(data.get("token_type", "Bearer")).strip() or "Bearer",
                "scopes": self._scopes_from_response(data),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def _mark_refresh_error(self, *, key: Tuple[str, str], message: str) -> None:
        clean = str(message or "").strip() or "refresh failed"
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return
            record.last_error = clean
            record.updated_at = _iso(_utc_now())
            self._save_locked()

    def _load_locked(self) -> None:
        with self._lock:
            if not self.store_path.exists():
                return
            try:
                payload = json.loads(self.store_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                return
            if not isinstance(payload, list):
                return
            for raw in payload:
                row = self._coerce_record(raw)
                if row is None:
                    continue
                self._records[(row.provider, row.account_id)] = row
            self._trim_locked()

    def _coerce_record(self, raw: Any) -> Optional[OAuthTokenRecord]:
        if not isinstance(raw, dict):
            return None
        provider = _normalize_provider(raw.get("provider", ""))
        account_id = _normalize_account(raw.get("account_id", "default"))
        access_token = self._decode_secret_from_store(str(raw.get("access_token", "")).strip())
        refresh_token = self._decode_secret_from_store(str(raw.get("refresh_token", "")).strip())
        if not provider:
            return None
        if not access_token and not refresh_token:
            return None
        scopes = self._normalize_scopes(raw.get("scopes"))
        metadata = raw.get("metadata", {})
        return OAuthTokenRecord(
            provider=provider,
            account_id=account_id,
            access_token=access_token,
            refresh_token=refresh_token,
            token_type=str(raw.get("token_type", "Bearer")).strip() or "Bearer",
            scopes=scopes,
            expires_at=str(raw.get("expires_at", "")).strip(),
            metadata=metadata if isinstance(metadata, dict) else {},
            created_at=str(raw.get("created_at", _iso(_utc_now()))),
            updated_at=str(raw.get("updated_at", _iso(_utc_now()))),
            last_refreshed_at=str(raw.get("last_refreshed_at", "")),
            last_error=str(raw.get("last_error", "")),
        )

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [self._serialize_record(item) for item in self._records.values()]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        self.store_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

    def _trim_locked(self) -> None:
        if len(self._records) <= self.max_records:
            return
        rows = sorted(self._records.values(), key=lambda item: item.updated_at)
        overflow = len(rows) - self.max_records
        for item in rows[:overflow]:
            self._records.pop((item.provider, item.account_id), None)

    def _serialize_record(self, item: OAuthTokenRecord) -> Dict[str, Any]:
        payload = item.to_dict()
        payload["access_token"] = self._encode_secret_for_store(str(payload.get("access_token", "")))
        payload["refresh_token"] = self._encode_secret_for_store(str(payload.get("refresh_token", "")))
        return payload

    def _encode_secret_for_store(self, value: str) -> str:
        secret = str(value or "").strip()
        if not secret:
            return ""
        if not self._secret_protection_enabled:
            return secret
        protected = self._protect_secret_payload(secret)
        if not protected:
            return secret
        return f"{_TOKEN_PROTECTED_PREFIX}{protected}"

    def _decode_secret_from_store(self, value: str) -> str:
        secret = str(value or "").strip()
        if not secret:
            return ""
        if not secret.startswith(_TOKEN_PROTECTED_PREFIX):
            return secret
        encoded = secret[len(_TOKEN_PROTECTED_PREFIX) :]
        if not encoded:
            return ""
        if not self._secret_protection_enabled:
            return ""
        recovered = self._unprotect_secret_payload(encoded)
        return recovered if recovered is not None else ""

    def _protect_secret_payload(self, value: str) -> Optional[str]:
        protected = self._dpapi_encrypt(str(value or "").encode("utf-8", errors="ignore"))
        if protected is None:
            return None
        return base64.b64encode(protected).decode("ascii")

    def _unprotect_secret_payload(self, value: str) -> Optional[str]:
        try:
            raw = base64.b64decode(str(value or "").encode("ascii"), validate=True)
        except Exception:  # noqa: BLE001
            return None
        decrypted = self._dpapi_decrypt(raw)
        if decrypted is None:
            return None
        try:
            return decrypted.decode("utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            return None

    def _dpapi_encrypt(self, plain: bytes) -> Optional[bytes]:
        if not plain:
            return b""
        if os.name != "nt":
            return None
        try:
            import ctypes
            from ctypes import wintypes
        except Exception:  # noqa: BLE001
            return None

        class DATA_BLOB(ctypes.Structure):
            _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

        def _blob_from_bytes(raw: bytes) -> tuple[DATA_BLOB, Any]:
            buf = ctypes.create_string_buffer(raw, len(raw))
            blob = DATA_BLOB(len(raw), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
            return blob, buf

        crypt32 = ctypes.windll.crypt32
        kernel32 = ctypes.windll.kernel32
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p

        in_blob, in_buf = _blob_from_bytes(plain)
        entropy_blob_ptr = None
        entropy_buf = None
        entropy = str(self._secret_entropy or "").encode("utf-8", errors="ignore")
        if entropy:
            entropy_blob, entropy_buf = _blob_from_bytes(entropy)
            entropy_blob_ptr = ctypes.byref(entropy_blob)
        out_blob = DATA_BLOB()
        try:
            ok = crypt32.CryptProtectData(
                ctypes.byref(in_blob),
                "JARVIS OAuth token",
                entropy_blob_ptr,
                None,
                None,
                0,
                ctypes.byref(out_blob),
            )
            if not ok:
                return None
            protected = ctypes.string_at(out_blob.pbData, out_blob.cbData)
            return bytes(protected)
        except Exception:  # noqa: BLE001
            return None
        finally:
            try:
                if out_blob.pbData:
                    kernel32.LocalFree(out_blob.pbData)
            except Exception:
                pass
            _ = in_buf
            _ = entropy_buf

    def _dpapi_decrypt(self, cipher: bytes) -> Optional[bytes]:
        if not cipher:
            return b""
        if os.name != "nt":
            return None
        try:
            import ctypes
            from ctypes import wintypes
        except Exception:  # noqa: BLE001
            return None

        class DATA_BLOB(ctypes.Structure):
            _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

        def _blob_from_bytes(raw: bytes) -> tuple[DATA_BLOB, Any]:
            buf = ctypes.create_string_buffer(raw, len(raw))
            blob = DATA_BLOB(len(raw), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
            return blob, buf

        crypt32 = ctypes.windll.crypt32
        kernel32 = ctypes.windll.kernel32
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p

        in_blob, in_buf = _blob_from_bytes(cipher)
        entropy_blob_ptr = None
        entropy_buf = None
        entropy = str(self._secret_entropy or "").encode("utf-8", errors="ignore")
        if entropy:
            entropy_blob, entropy_buf = _blob_from_bytes(entropy)
            entropy_blob_ptr = ctypes.byref(entropy_blob)
        out_blob = DATA_BLOB()
        try:
            ok = crypt32.CryptUnprotectData(
                ctypes.byref(in_blob),
                None,
                entropy_blob_ptr,
                None,
                None,
                0,
                ctypes.byref(out_blob),
            )
            if not ok:
                return None
            plain = ctypes.string_at(out_blob.pbData, out_blob.cbData)
            return bytes(plain)
        except Exception:  # noqa: BLE001
            return None
        finally:
            try:
                if out_blob.pbData:
                    kernel32.LocalFree(out_blob.pbData)
            except Exception:
                pass
            _ = in_buf
            _ = entropy_buf

    @staticmethod
    def _normalize_scopes(scopes: Any) -> List[str]:
        if isinstance(scopes, str):
            tokens = [item.strip() for item in scopes.replace(",", " ").split(" ")]
            return [item for item in tokens if item]
        if not isinstance(scopes, list):
            return []
        out: List[str] = []
        for item in scopes:
            text = str(item or "").strip()
            if text and text not in out:
                out.append(text)
        return out

    @staticmethod
    def _resolve_expires_at(*, expires_at: str, expires_in_s: Optional[int]) -> str:
        parsed = _parse_iso(expires_at)
        if parsed is not None:
            return _iso(parsed)
        safe_ttl = OAuthTokenStore._coerce_int(expires_in_s, default=3600, minimum=30, maximum=86400 * 365)
        return _iso(_utc_now() + timedelta(seconds=safe_ttl))

    @staticmethod
    def _coerce_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _env_flag(name: str, *, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        value = str(raw or "").strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _requests():
        try:
            import requests  # type: ignore

            return requests
        except Exception:
            return None

    @staticmethod
    def _scopes_from_response(payload: Dict[str, Any]) -> List[str]:
        scope = payload.get("scope")
        if isinstance(scope, str) and scope.strip():
            return [item for item in scope.strip().split(" ") if item]
        scopes = payload.get("scopes")
        if isinstance(scopes, list):
            out: List[str] = []
            for item in scopes:
                text = str(item or "").strip()
                if text and text not in out:
                    out.append(text)
            return out
        return []

    @staticmethod
    def generate_account_id(prefix: str = "acct") -> str:
        clean_prefix = str(prefix or "acct").strip().lower() or "acct"
        return f"{clean_prefix}-{secrets.token_hex(6)}"
