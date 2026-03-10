from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from backend.python.core.oauth_token_store import OAuthTokenStore
from backend.python.tools.browser_tools import BrowserTools, _DOMInspector


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _redact_headers(headers: Dict[str, str]) -> Dict[str, str]:
    sensitive_tokens = ("authorization", "cookie", "x-api-key", "api-key", "token", "secret")
    out: Dict[str, str] = {}
    for key, value in headers.items():
        name = str(key or "").strip()
        if not name:
            continue
        lowered = name.lower()
        if any(token in lowered for token in sensitive_tokens):
            out[name] = "***redacted***"
            continue
        text = str(value or "").strip()
        if len(text) > 200:
            text = f"{text[:200]}...(truncated)"
        out[name] = text
    return out


@dataclass(slots=True)
class BrowserSessionRecord:
    session_id: str
    name: str
    base_url: str
    verify_ssl: bool
    oauth_provider: str = ""
    oauth_account_id: str = "default"
    oauth_min_ttl_s: int = 120
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    last_used_at: str = ""
    request_count: int = 0
    session_ttl_s: int = 604800
    persist_cookies: bool = True
    persist_headers: bool = True
    persist_auth_header: bool = False
    default_headers: Dict[str, str] = field(default_factory=dict)

    def to_public_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        headers = payload.get("default_headers", {})
        if isinstance(headers, dict):
            payload["default_headers"] = _redact_headers({str(k): str(v) for k, v in headers.items()})
            payload["header_count"] = len(headers)
            payload["has_authorization_header"] = any(str(k).strip().lower() == "authorization" for k in headers)
        else:
            payload["default_headers"] = {}
            payload["header_count"] = 0
            payload["has_authorization_header"] = False
        return payload

    def to_store_dict(self, *, headers: Dict[str, str], cookies: Dict[str, str]) -> Dict[str, Any]:
        payload = asdict(self)
        payload["vault_version"] = 2
        payload["default_headers"] = dict(headers)
        payload["cookies"] = dict(cookies)
        return payload


class BrowserSessionTools:
    _lock = threading.RLock()
    _sessions: Dict[str, Tuple[Any, BrowserSessionRecord]] = {}
    _loaded: bool = False

    @classmethod
    def create_session(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        cls._ensure_loaded()
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable"}

        name = str(payload.get("name", "browser-session")).strip() or "browser-session"
        base_url_raw = str(payload.get("base_url", "")).strip()
        base_url = ""
        if base_url_raw:
            ok, value = BrowserTools.validate_url(base_url_raw)
            if not ok:
                return {"status": "error", "message": value}
            base_url = value

        verify_ssl = bool(payload.get("verify_ssl", True))
        headers = payload.get("headers", {})
        cookies = payload.get("cookies", {})
        oauth_provider = str(payload.get("oauth_provider", "")).strip().lower()
        oauth_account_id = str(payload.get("oauth_account_id", "default")).strip().lower() or "default"
        oauth_min_ttl_s = cls._coerce_int(payload.get("oauth_min_ttl_s", 120), default=120, minimum=0, maximum=86_400)
        persist_cookies = bool(payload.get("persist_cookies", True))
        persist_headers = bool(payload.get("persist_headers", True))
        persist_auth_header = bool(
            payload.get("persist_auth_header", cls._coerce_bool(os.getenv("JARVIS_BROWSER_SESSION_PERSIST_AUTH_HEADER"), default=False))
        )
        session_ttl_s = cls._coerce_int(
            payload.get("session_ttl_s", os.getenv("JARVIS_BROWSER_SESSION_TTL_S", "604800")),
            default=604800,
            minimum=60,
            maximum=86400 * 365,
        )

        session = requests.Session()
        session.headers.update({"User-Agent": BrowserTools.USER_AGENT})
        if isinstance(headers, dict):
            for key, value in headers.items():
                key_text = str(key or "").strip()
                value_text = str(value or "").strip()
                if key_text and value_text:
                    session.headers[key_text] = value_text

        if isinstance(cookies, dict):
            for key, value in cookies.items():
                cookie_key = str(key or "").strip()
                cookie_value = str(value or "").strip()
                if cookie_key:
                    session.cookies.set(cookie_key, cookie_value)

        # Optional static bearer token.
        auth_token = str(payload.get("auth_token", "")).strip()
        auth_header = str(payload.get("auth_header", "Authorization")).strip() or "Authorization"
        auth_prefix = str(payload.get("auth_prefix", "Bearer")).strip() or "Bearer"
        if auth_token:
            session.headers[auth_header] = f"{auth_prefix} {auth_token}".strip()

        # OAuth-backed session header setup.
        if oauth_provider:
            token_result = OAuthTokenStore.shared().resolve_access_token(
                provider=oauth_provider,
                account_id=oauth_account_id,
                min_ttl_s=oauth_min_ttl_s,
                auto_refresh=True,
            )
            if token_result.get("status") != "success":
                return token_result
            access_token = str(token_result.get("access_token", "")).strip()
            token_type = str(token_result.get("token_type", "Bearer")).strip() or "Bearer"
            if access_token:
                session.headers["Authorization"] = f"{token_type} {access_token}".strip()

        now_iso = _utc_now_iso()
        session_id = str(uuid.uuid4())
        record = BrowserSessionRecord(
            session_id=session_id,
            name=name,
            base_url=base_url,
            verify_ssl=verify_ssl,
            oauth_provider=oauth_provider,
            oauth_account_id=oauth_account_id,
            oauth_min_ttl_s=oauth_min_ttl_s,
            created_at=now_iso,
            updated_at=now_iso,
            session_ttl_s=session_ttl_s,
            persist_cookies=persist_cookies,
            persist_headers=persist_headers,
            persist_auth_header=persist_auth_header,
            default_headers={key: str(value) for key, value in session.headers.items()},
        )

        with cls._lock:
            cls._sessions[session_id] = (session, record)
            cls._trim_locked()
            cls._save_locked()
        return {"status": "success", "session": record.to_public_dict()}

    @classmethod
    def list_sessions(cls, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        del payload
        cls._ensure_loaded()
        with cls._lock:
            if cls._prune_expired_locked():
                cls._save_locked()
            rows = [item[1].to_public_dict() for item in cls._sessions.values()]
        rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        return {"status": "success", "items": rows, "count": len(rows)}

    @classmethod
    def close_session(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        cls._ensure_loaded()
        session_id = str(payload.get("session_id", "")).strip()
        if not session_id:
            return {"status": "error", "message": "session_id is required"}
        with cls._lock:
            item = cls._sessions.pop(session_id, None)
        if item is None:
            return {"status": "error", "message": "session not found"}
        session, record = item
        try:
            session.close()
        except Exception:
            pass
        with cls._lock:
            cls._save_locked()
        return {"status": "success", "session": record.to_public_dict()}

    @classmethod
    def request(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        cls._ensure_loaded()
        session, record, err = cls._resolve_session(payload)
        if err:
            return err
        assert record is not None
        assert session is not None

        method = str(payload.get("method", "GET")).strip().upper() or "GET"
        raw_url = str(payload.get("url", "")).strip()
        if not raw_url:
            return {"status": "error", "message": "url is required"}
        target_url = cls._resolve_request_url(raw_url=raw_url, base_url=record.base_url)
        ok, safe_or_msg = BrowserTools.validate_url(target_url)
        if not ok:
            return {"status": "error", "message": safe_or_msg}
        safe_url = safe_or_msg

        timeout_s = cls._coerce_float(payload.get("timeout_s", 20.0), default=20.0, minimum=1.0, maximum=60.0)
        max_chars = cls._coerce_int(payload.get("max_chars", 20_000), default=20_000, minimum=512, maximum=500_000)
        allow_redirects = bool(payload.get("allow_redirects", True))
        headers = payload.get("headers", {})
        params = payload.get("params", {})
        body_data = payload.get("data")
        body_json = payload.get("json")

        # Refresh OAuth token if configured and near expiry.
        auth_error = cls._refresh_oauth_header_if_needed(session=session, record=record)
        if auth_error:
            return auth_error

        request_kwargs: Dict[str, Any] = {
            "method": method,
            "url": safe_url,
            "timeout": timeout_s,
            "verify": record.verify_ssl,
            "allow_redirects": allow_redirects,
        }
        if isinstance(headers, dict) and headers:
            request_kwargs["headers"] = {str(k): str(v) for k, v in headers.items() if str(k).strip()}
        if isinstance(params, dict) and params:
            request_kwargs["params"] = params
        if body_json is not None:
            request_kwargs["json"] = body_json
        elif body_data is not None:
            request_kwargs["data"] = body_data

        try:
            response = session.request(**request_kwargs)
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

        record.updated_at = _utc_now_iso()
        record.last_used_at = record.updated_at
        record.request_count = max(0, int(record.request_count)) + 1
        record.default_headers = {str(key): str(value) for key, value in session.headers.items()}
        with cls._lock:
            cls._save_locked()

        text_body = response.text if response.text is not None else ""
        truncated = len(text_body) > max_chars
        body_preview = text_body[:max_chars] if truncated else text_body
        content_type = str(response.headers.get("Content-Type", "")).lower()

        json_body: Any = None
        if "json" in content_type:
            try:
                json_body = response.json()
            except Exception:
                json_body = None

        return {
            "status": "success",
            "session_id": record.session_id,
            "request": {
                "method": method,
                "url": safe_url,
                "timeout_s": timeout_s,
                "allow_redirects": allow_redirects,
            },
            "response": {
                "status_code": int(response.status_code),
                "reason": str(response.reason or ""),
                "ok": bool(response.ok),
                "content_type": content_type,
                "headers": cls._normalize_headers(dict(response.headers)),
                "body": body_preview,
                "chars": len(body_preview),
                "truncated": truncated,
                "json": json_body,
            },
        }

    @classmethod
    def read_dom(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = cls.request(payload)
        if response.get("status") != "success":
            return response
        response_payload = response.get("response", {})
        if not isinstance(response_payload, dict):
            return {"status": "error", "message": "invalid response payload"}
        raw_text = str(response_payload.get("body", ""))
        url = str(response.get("request", {}).get("url", ""))
        parser = _DOMInspector(base_url=url)
        try:
            parser.feed(raw_text)
        except Exception:
            return {"status": "error", "message": "failed to parse html"}
        exported = parser.export()
        max_chars = cls._coerce_int(payload.get("max_chars", 6000), default=6000, minimum=256, maximum=100_000)
        text = str(exported.get("text", ""))
        truncated = len(text) > max_chars
        output_text = text[:max_chars] if truncated else text
        return {
            "status": "success",
            "session_id": response.get("session_id", ""),
            "url": url,
            "title": str(exported.get("title", "")),
            "text": output_text,
            "chars": len(output_text),
            "truncated": truncated,
        }

    @classmethod
    def extract_links(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = cls.request(payload)
        if response.get("status") != "success":
            return response
        response_payload = response.get("response", {})
        if not isinstance(response_payload, dict):
            return {"status": "error", "message": "invalid response payload"}
        raw_text = str(response_payload.get("body", ""))
        url = str(response.get("request", {}).get("url", ""))
        parser = _DOMInspector(base_url=url)
        try:
            parser.feed(raw_text)
        except Exception:
            return {"status": "error", "message": "failed to parse html"}
        exported = parser.export()
        raw_links = exported.get("links", [])
        if not isinstance(raw_links, list):
            raw_links = []

        same_domain_only = bool(payload.get("same_domain_only", False))
        max_links = cls._coerce_int(payload.get("max_links", 80), default=80, minimum=1, maximum=1000)
        links: List[str] = []
        if same_domain_only:
            try:
                from urllib.parse import urlparse

                host = str(urlparse(url).hostname or "").strip().lower()
                for item in raw_links:
                    candidate = str(item or "").strip()
                    if not candidate:
                        continue
                    item_host = str(urlparse(candidate).hostname or "").strip().lower()
                    if item_host == host:
                        links.append(candidate)
            except Exception:
                links = [str(item) for item in raw_links if isinstance(item, str)]
        else:
            links = [str(item) for item in raw_links if isinstance(item, str)]

        return {
            "status": "success",
            "session_id": response.get("session_id", ""),
            "url": url,
            "links": links[:max_links],
            "count": min(len(links), max_links),
            "truncated": len(links) > max_links,
            "same_domain_only": same_domain_only,
        }

    @classmethod
    def _resolve_session(cls, payload: Dict[str, Any]) -> Tuple[Any, Optional[BrowserSessionRecord], Dict[str, Any]]:
        cls._ensure_loaded()
        session_id = str(payload.get("session_id", "")).strip()
        if not session_id:
            return None, None, {"status": "error", "message": "session_id is required"}
        with cls._lock:
            item = cls._sessions.get(session_id)
            if item is not None:
                _session, record = item
                if cls._is_expired(record):
                    try:
                        _session.close()
                    except Exception:
                        pass
                    cls._sessions.pop(session_id, None)
                    cls._save_locked()
                    item = None
        if item is None:
            return None, None, {"status": "error", "message": "session not found"}
        return item[0], item[1], {}

    @classmethod
    def _refresh_oauth_header_if_needed(cls, *, session: Any, record: BrowserSessionRecord) -> Dict[str, Any]:
        if not record.oauth_provider:
            return {}
        token_result = OAuthTokenStore.shared().resolve_access_token(
            provider=record.oauth_provider,
            account_id=record.oauth_account_id,
            min_ttl_s=record.oauth_min_ttl_s,
            auto_refresh=True,
        )
        if token_result.get("status") != "success":
            return token_result
        access_token = str(token_result.get("access_token", "")).strip()
        token_type = str(token_result.get("token_type", "Bearer")).strip() or "Bearer"
        if access_token:
            session.headers["Authorization"] = f"{token_type} {access_token}".strip()
        return {}

    @classmethod
    def _ensure_loaded(cls) -> None:
        with cls._lock:
            if cls._loaded:
                return
            cls._loaded = True
            cls._load_locked()
            cls._prune_expired_locked()
            cls._trim_locked()
            cls._save_locked()

    @classmethod
    def _load_locked(cls) -> None:
        requests = cls._requests()
        if requests is None:
            return
        path = cls._store_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, list):
            return
        for row in payload:
            if not isinstance(row, dict):
                continue
            session_id = str(row.get("session_id", "")).strip()
            if not session_id:
                continue
            session = requests.Session()
            session.headers.update({"User-Agent": BrowserTools.USER_AGENT})

            default_headers = row.get("default_headers", {})
            persist_headers = bool(row.get("persist_headers", True))
            persist_auth_header = bool(
                row.get(
                    "persist_auth_header",
                    cls._coerce_bool(os.getenv("JARVIS_BROWSER_SESSION_PERSIST_AUTH_HEADER"), default=False),
                )
            )
            if isinstance(default_headers, dict):
                for key, value in default_headers.items():
                    key_text = str(key or "").strip()
                    value_text = str(value or "").strip()
                    if key_text:
                        if key_text.lower() == "authorization" and not persist_auth_header:
                            continue
                        session.headers[key_text] = value_text

            persist_cookies = bool(row.get("persist_cookies", True))
            cookies = row.get("cookies", {})
            if persist_cookies and isinstance(cookies, dict):
                for key, value in cookies.items():
                    key_text = str(key or "").strip()
                    if key_text:
                        session.cookies.set(key_text, str(value or ""))

            record = BrowserSessionRecord(
                session_id=session_id,
                name=str(row.get("name", "browser-session")).strip() or "browser-session",
                base_url=str(row.get("base_url", "")).strip(),
                verify_ssl=bool(row.get("verify_ssl", True)),
                oauth_provider=str(row.get("oauth_provider", "")).strip().lower(),
                oauth_account_id=str(row.get("oauth_account_id", "default")).strip().lower() or "default",
                oauth_min_ttl_s=cls._coerce_int(row.get("oauth_min_ttl_s", 120), default=120, minimum=0, maximum=86_400),
                created_at=str(row.get("created_at", _utc_now_iso())),
                updated_at=str(row.get("updated_at", _utc_now_iso())),
                last_used_at=str(row.get("last_used_at", "")),
                request_count=cls._coerce_int(row.get("request_count", 0), default=0, minimum=0, maximum=2_000_000_000),
                session_ttl_s=cls._coerce_int(row.get("session_ttl_s", 604800), default=604800, minimum=60, maximum=86400 * 365),
                persist_cookies=persist_cookies,
                persist_headers=persist_headers,
                persist_auth_header=persist_auth_header,
                default_headers={str(k): str(v) for k, v in session.headers.items()},
            )
            cls._sessions[session_id] = (session, record)

    @classmethod
    def _save_locked(cls) -> None:
        path = cls._store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        rows: List[Dict[str, Any]] = []
        for session, record in cls._sessions.values():
            if record.persist_headers:
                headers = {str(k): str(v) for k, v in session.headers.items() if str(k).strip()}
                if not record.persist_auth_header:
                    headers = {k: v for k, v in headers.items() if k.strip().lower() != "authorization"}
            else:
                headers = {}
            if record.persist_cookies:
                try:
                    cookies = {str(k): str(v) for k, v in session.cookies.get_dict().items()}
                except Exception:
                    cookies = {}
            else:
                cookies = {}
            record.default_headers = headers
            row = record.to_store_dict(headers=headers, cookies=cookies)
            row["updated_at"] = str(record.updated_at or _utc_now_iso())
            try:
                row["vault_saved_at"] = _utc_now_iso()
            except Exception:
                row["vault_saved_at"] = _utc_now_iso()
            rows.append(row)
        rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

    @staticmethod
    def _store_path() -> Path:
        raw = str(os.getenv("JARVIS_BROWSER_SESSION_STORE", "data/browser_sessions.json")).strip() or "data/browser_sessions.json"
        return Path(raw)

    @classmethod
    def _max_records(cls) -> int:
        return cls._coerce_int(
            os.getenv("JARVIS_BROWSER_SESSION_MAX_RECORDS", "300"),
            default=300,
            minimum=20,
            maximum=10_000,
        )

    @classmethod
    def _trim_locked(cls) -> None:
        max_records = cls._max_records()
        if len(cls._sessions) <= max_records:
            return
        rows = sorted(
            cls._sessions.items(),
            key=lambda item: str(getattr(item[1][1], "updated_at", "")),
        )
        overflow = len(rows) - max_records
        for session_id, (session, _record) in rows[:overflow]:
            try:
                session.close()
            except Exception:
                pass
            cls._sessions.pop(session_id, None)

    @classmethod
    def _prune_expired_locked(cls) -> bool:
        expired_ids: List[str] = []
        for session_id, (session, record) in cls._sessions.items():
            if not cls._is_expired(record):
                continue
            try:
                session.close()
            except Exception:
                pass
            expired_ids.append(session_id)
        for session_id in expired_ids:
            cls._sessions.pop(session_id, None)
        return bool(expired_ids)

    @classmethod
    def _is_expired(cls, record: BrowserSessionRecord) -> bool:
        ttl_s = cls._coerce_int(getattr(record, "session_ttl_s", 604800), default=604800, minimum=60, maximum=86400 * 365)
        baseline = _parse_iso(str(record.updated_at or "")) or _parse_iso(str(record.created_at or ""))
        if baseline is None:
            return False
        expires_at = baseline + timedelta(seconds=ttl_s)
        return datetime.now(timezone.utc) >= expires_at

    @classmethod
    def reset_runtime(cls, *, clear_persisted: bool = False) -> None:
        with cls._lock:
            for session, _record in list(cls._sessions.values()):
                try:
                    session.close()
                except Exception:
                    pass
            cls._sessions = {}
            cls._loaded = False
            if clear_persisted:
                try:
                    cls._store_path().unlink(missing_ok=True)
                except Exception:
                    pass

    @staticmethod
    def _resolve_request_url(*, raw_url: str, base_url: str) -> str:
        clean_url = str(raw_url or "").strip()
        if clean_url.startswith(("http://", "https://")):
            return clean_url
        if base_url:
            return urljoin(base_url.rstrip("/") + "/", clean_url.lstrip("/"))
        return BrowserTools.normalize_url(clean_url)

    @staticmethod
    def _normalize_headers(headers: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in headers.items():
            name = str(key or "").strip()
            if not name:
                continue
            text = str(value or "").strip()
            if len(text) > 800:
                text = f"{text[:800]}...(truncated)"
            out[name] = text
        return out

    @staticmethod
    def _requests():
        try:
            import requests  # type: ignore

            return requests
        except Exception:
            return None

    @staticmethod
    def _coerce_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_bool(value: Any, *, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _coerce_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))
