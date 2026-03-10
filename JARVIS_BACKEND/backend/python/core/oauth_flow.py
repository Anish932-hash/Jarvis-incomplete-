from __future__ import annotations

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock, RLock
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .oauth_token_store import OAuthTokenStore


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
    value = str(provider or "").strip().lower()
    if not value:
        return ""
    aliases = {
        "google": "google",
        "gmail": "google",
        "google_workspace": "google",
        "graph": "graph",
        "microsoft": "graph",
        "microsoft_graph": "graph",
        "outlook": "graph",
        "office365": "graph",
        "azuread": "graph",
    }
    return aliases.get(value, value)


def _normalize_account(account_id: str) -> str:
    clean = str(account_id or "").strip().lower()
    return clean or "default"


def _normalize_scopes(scopes: Any) -> List[str]:
    if isinstance(scopes, str):
        rows = [item.strip() for item in scopes.replace(",", " ").split(" ")]
        return [item for item in rows if item]
    if not isinstance(scopes, list):
        return []
    out: List[str] = []
    for item in scopes:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


@dataclass(slots=True)
class OAuthProviderSpec:
    key: str
    display_name: str
    authorize_url: str
    token_url: str
    client_id_env: str
    client_secret_env: str
    default_scopes: List[str]
    supports_offline: bool = True


@dataclass(slots=True)
class OAuthFlowRecord:
    session_id: str
    state: str
    provider: str
    account_id: str
    redirect_uri: str
    scopes: List[str]
    code_verifier: str
    code_challenge: str
    status: str
    created_at: str
    expires_at: str
    completed_at: str = ""
    auth_url: str = ""
    error: str = ""
    error_description: str = ""
    token: Dict[str, Any] | None = None

    def to_public_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "state": self.state,
            "provider": self.provider,
            "account_id": self.account_id,
            "redirect_uri": self.redirect_uri,
            "scopes": list(self.scopes),
            "code_challenge_method": "S256",
            "status": self.status,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "completed_at": self.completed_at,
            "auth_url": self.auth_url,
            "error": self.error,
            "error_description": self.error_description,
            "token": dict(self.token or {}),
        }


class OAuthFlowManager:
    """
    OAuth PKCE orchestration:
    - builds provider authorization URLs
    - tracks short-lived auth sessions
    - exchanges callback code for tokens
    - persists tokens through OAuthTokenStore
    """

    _shared_instance: OAuthFlowManager | None = None
    _shared_lock = Lock()

    PROVIDERS: Dict[str, OAuthProviderSpec] = {
        "google": OAuthProviderSpec(
            key="google",
            display_name="Google",
            authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
            token_url="https://oauth2.googleapis.com/token",
            client_id_env="GOOGLE_OAUTH_CLIENT_ID",
            client_secret_env="GOOGLE_OAUTH_CLIENT_SECRET",
            default_scopes=[
                "openid",
                "email",
                "profile",
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/gmail.send",
                "https://www.googleapis.com/auth/calendar.events",
                "https://www.googleapis.com/auth/documents",
                "https://www.googleapis.com/auth/drive.file",
            ],
            supports_offline=True,
        ),
        "graph": OAuthProviderSpec(
            key="graph",
            display_name="Microsoft Graph",
            authorize_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
            token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
            client_id_env="MICROSOFT_GRAPH_CLIENT_ID",
            client_secret_env="MICROSOFT_GRAPH_CLIENT_SECRET",
            default_scopes=[
                "offline_access",
                "openid",
                "profile",
                "User.Read",
                "Mail.Read",
                "Mail.Send",
                "Calendars.ReadWrite",
                "Files.ReadWrite",
            ],
            supports_offline=True,
        ),
    }

    def __init__(self, *, ttl_s: int = 900, max_records: int = 2000) -> None:
        self.ttl_s = max(60, min(int(ttl_s), 3600))
        self.max_records = max(50, min(int(max_records), 20_000))
        self._flows: Dict[str, OAuthFlowRecord] = {}
        self._state_index: Dict[str, str] = {}
        self._lock = RLock()
        self._register_provider_refreshers()

    @classmethod
    def shared(cls) -> OAuthFlowManager:
        with cls._shared_lock:
            if cls._shared_instance is None:
                cls._shared_instance = cls(
                    ttl_s=int(os.getenv("JARVIS_OAUTH_FLOW_TTL_S", "900")),
                    max_records=int(os.getenv("JARVIS_OAUTH_FLOW_MAX_RECORDS", "2000")),
                )
            return cls._shared_instance

    def list_providers(self) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        for key, spec in self.PROVIDERS.items():
            client_id = str(os.getenv(spec.client_id_env, "")).strip()
            rows.append(
                {
                    "provider": key,
                    "display_name": spec.display_name,
                    "configured": bool(client_id),
                    "client_id_env": spec.client_id_env,
                    "client_secret_env": spec.client_secret_env,
                    "authorize_url": spec.authorize_url,
                    "token_url": spec.token_url,
                    "default_scopes": list(spec.default_scopes),
                }
            )
        rows.sort(key=lambda item: str(item.get("provider", "")))
        return {"status": "success", "items": rows, "count": len(rows)}

    def pending_count(self) -> int:
        self._prune()
        with self._lock:
            return len([row for row in self._flows.values() if row.status == "pending"])

    def start(
        self,
        *,
        provider: str,
        account_id: str = "default",
        scopes: Optional[List[str]] = None,
        redirect_uri: str = "",
    ) -> Dict[str, Any]:
        provider_key = _normalize_provider(provider)
        spec = self.PROVIDERS.get(provider_key)
        if spec is None:
            return {"status": "error", "message": f"Unsupported provider: {provider}"}

        client_id = str(os.getenv(spec.client_id_env, "")).strip()
        if not client_id:
            return {"status": "error", "message": f"{spec.client_id_env} is not configured"}

        target_redirect = str(
            redirect_uri
            or os.getenv("JARVIS_OAUTH_REDIRECT_URI", "http://127.0.0.1:8765/oauth/callback")
        ).strip()
        if not target_redirect:
            return {"status": "error", "message": "redirect_uri is required"}

        scope_rows = _normalize_scopes(scopes if scopes is not None else spec.default_scopes)
        if not scope_rows:
            scope_rows = list(spec.default_scopes)

        code_verifier = self._build_pkce_verifier()
        code_challenge = self._build_pkce_challenge(code_verifier)
        state = secrets.token_urlsafe(24)
        session_id = secrets.token_hex(16)
        created_at = _iso(_utc_now())
        expires_at = _iso(_utc_now() + timedelta(seconds=self.ttl_s))

        params: Dict[str, str] = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": target_redirect,
            "scope": " ".join(scope_rows),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        if provider_key == "google":
            params["access_type"] = "offline"
            params["include_granted_scopes"] = "true"
            params["prompt"] = "consent"
        if provider_key == "graph":
            params["response_mode"] = "query"

        auth_url = f"{spec.authorize_url}?{urlencode(params)}"
        record = OAuthFlowRecord(
            session_id=session_id,
            state=state,
            provider=provider_key,
            account_id=_normalize_account(account_id),
            redirect_uri=target_redirect,
            scopes=scope_rows,
            code_verifier=code_verifier,
            code_challenge=code_challenge,
            status="pending",
            created_at=created_at,
            expires_at=expires_at,
            auth_url=auth_url,
        )

        with self._lock:
            self._flows[session_id] = record
            self._state_index[state] = session_id
            self._prune_locked()

        return {"status": "success", "flow": record.to_public_dict(), "authorize_url": auth_url}

    def status(self, *, session_id: str = "", state: str = "") -> Dict[str, Any]:
        record = self._resolve_record(session_id=session_id, state=state)
        if record is None:
            return {"status": "error", "message": "OAuth flow not found"}
        return {"status": "success", "flow": record.to_public_dict()}

    def complete(
        self,
        *,
        session_id: str = "",
        state: str = "",
        code: str = "",
        redirect_uri: str = "",
        error: str = "",
        error_description: str = "",
    ) -> Dict[str, Any]:
        record = self._resolve_record(session_id=session_id, state=state)
        if record is None:
            return {"status": "error", "message": "OAuth flow not found"}

        now = _utc_now()
        expires = _parse_iso(record.expires_at)
        if expires is not None and now > expires:
            self._mark_failed(record, error="expired", description="Authorization session expired.")
            return {"status": "error", "message": "Authorization session expired.", "flow": record.to_public_dict()}

        if error:
            clean_error = str(error).strip() or "authorization_error"
            clean_description = str(error_description).strip()
            self._mark_failed(record, error=clean_error, description=clean_description)
            message = clean_description or clean_error
            return {"status": "error", "message": message, "flow": record.to_public_dict()}

        auth_code = str(code or "").strip()
        if not auth_code:
            return {"status": "error", "message": "code is required", "flow": record.to_public_dict()}

        exchanged = self._exchange_authorization_code(
            record=record,
            code=auth_code,
            redirect_uri=str(redirect_uri or "").strip(),
        )
        if exchanged.get("status") != "success":
            clean_error = str(exchanged.get("error", "")).strip() or "token_exchange_failed"
            clean_description = str(exchanged.get("message", "")).strip()
            self._mark_failed(record, error=clean_error, description=clean_description)
            return {"status": "error", "message": clean_description or clean_error, "flow": record.to_public_dict()}

        token_payload = exchanged.get("token", {})
        if not isinstance(token_payload, dict):
            token_payload = {}
        access_token = str(token_payload.get("access_token", "")).strip()
        if not access_token:
            self._mark_failed(record, error="missing_access_token", description="Provider response did not include access_token.")
            return {"status": "error", "message": "Provider response missing access_token", "flow": record.to_public_dict()}

        provider_spec = self.PROVIDERS.get(record.provider)
        if provider_spec is None:
            self._mark_failed(record, error="provider_missing", description="Provider config unavailable.")
            return {"status": "error", "message": "Provider config unavailable", "flow": record.to_public_dict()}

        client_id = str(os.getenv(provider_spec.client_id_env, "")).strip()
        client_secret = str(os.getenv(provider_spec.client_secret_env, "")).strip()
        metadata = {
            "token_url": provider_spec.token_url,
            "client_id": client_id,
            "oauth_provider": record.provider,
            "oauth_flow_session_id": record.session_id,
        }
        if client_secret:
            metadata["client_secret"] = client_secret

        stored = OAuthTokenStore.shared().upsert(
            provider=record.provider,
            account_id=record.account_id,
            access_token=access_token,
            refresh_token=str(token_payload.get("refresh_token", "")).strip(),
            token_type=str(token_payload.get("token_type", "Bearer")).strip() or "Bearer",
            scopes=_normalize_scopes(token_payload.get("scopes")) or list(record.scopes),
            expires_at=str(token_payload.get("expires_at", "")).strip(),
            expires_in_s=self._to_int(token_payload.get("expires_in_s"), default=3600, minimum=30, maximum=86400 * 365),
            metadata=metadata,
        )
        if stored.get("status") != "success":
            self._mark_failed(
                record,
                error="token_store_failed",
                description=str(stored.get("message", "token persistence failed")),
            )
            return {"status": "error", "message": str(stored.get("message", "token persistence failed")), "flow": record.to_public_dict()}

        clean_token = stored.get("token", {})
        if not isinstance(clean_token, dict):
            clean_token = {}
        with self._lock:
            record.status = "completed"
            record.error = ""
            record.error_description = ""
            record.completed_at = _iso(_utc_now())
            record.token = clean_token
            self._flows[record.session_id] = record

        return {
            "status": "success",
            "flow": record.to_public_dict(),
            "token": clean_token,
        }

    def _resolve_record(self, *, session_id: str, state: str) -> Optional[OAuthFlowRecord]:
        self._prune()
        clean_session_id = str(session_id or "").strip()
        clean_state = str(state or "").strip()
        with self._lock:
            if clean_session_id and clean_session_id in self._flows:
                return self._flows[clean_session_id]
            if clean_state and clean_state in self._state_index:
                mapped_id = self._state_index.get(clean_state, "")
                if mapped_id and mapped_id in self._flows:
                    return self._flows[mapped_id]
        return None

    def _mark_failed(self, record: OAuthFlowRecord, *, error: str, description: str) -> None:
        with self._lock:
            current = self._flows.get(record.session_id)
            if current is None:
                return
            current.status = "error"
            current.error = str(error or "").strip() or "authorization_failed"
            current.error_description = str(description or "").strip()
            current.completed_at = _iso(_utc_now())
            self._flows[record.session_id] = current

    def _exchange_authorization_code(
        self,
        *,
        record: OAuthFlowRecord,
        code: str,
        redirect_uri: str,
    ) -> Dict[str, Any]:
        spec = self.PROVIDERS.get(record.provider)
        if spec is None:
            return {"status": "error", "error": "unsupported_provider", "message": f"Unsupported provider: {record.provider}"}

        requests = self._requests()
        if requests is None:
            return {"status": "error", "error": "requests_unavailable", "message": "requests package is unavailable"}

        client_id = str(os.getenv(spec.client_id_env, "")).strip()
        if not client_id:
            return {"status": "error", "error": "client_not_configured", "message": f"{spec.client_id_env} is not configured"}

        client_secret = str(os.getenv(spec.client_secret_env, "")).strip()
        target_redirect = str(redirect_uri or record.redirect_uri).strip() or record.redirect_uri
        payload: Dict[str, Any] = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": client_id,
            "redirect_uri": target_redirect,
            "code_verifier": record.code_verifier,
        }
        if client_secret:
            payload["client_secret"] = client_secret

        try:
            response = requests.post(
                spec.token_url,
                data=payload,
                timeout=25,
                headers={"Accept": "application/json"},
            )
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "error": "request_failed", "message": str(exc)}

        body = self._response_json(response)
        if int(response.status_code) >= 300:
            provider_error = str(body.get("error", "")).strip()
            provider_error_description = str(body.get("error_description", "")).strip()
            message = provider_error_description or provider_error or f"token exchange failed ({response.status_code})"
            return {"status": "error", "error": provider_error or "token_exchange_failed", "message": message}

        access_token = str(body.get("access_token", "")).strip()
        if not access_token:
            return {
                "status": "error",
                "error": "missing_access_token",
                "message": "Token endpoint response did not include access_token",
            }

        expires_in_s = self._to_int(body.get("expires_in"), default=3600, minimum=30, maximum=86400 * 365)
        scopes = self._scopes_from_response(body) or list(record.scopes)
        return {
            "status": "success",
            "token": {
                "access_token": access_token,
                "refresh_token": str(body.get("refresh_token", "")).strip(),
                "token_type": str(body.get("token_type", "Bearer")).strip() or "Bearer",
                "expires_in_s": expires_in_s,
                "expires_at": _iso(_utc_now() + timedelta(seconds=expires_in_s)),
                "scopes": scopes,
            },
        }

    def _register_provider_refreshers(self) -> None:
        store = OAuthTokenStore.shared()
        for provider in ("google", "graph", "microsoft_graph", "microsoft"):
            normalized = _normalize_provider(provider)
            if not normalized:
                continue

            def _refresher(record: Dict[str, Any], provider_name: str = normalized) -> Dict[str, Any]:
                return self._refresh_with_provider(record, provider=provider_name)

            store.register_refresher(normalized, _refresher)

    def _refresh_with_provider(self, record: Dict[str, Any], *, provider: str) -> Dict[str, Any]:
        spec = self.PROVIDERS.get(_normalize_provider(provider))
        if spec is None:
            return {"status": "error", "message": f"Unsupported provider: {provider}"}
        requests = self._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable"}

        metadata = record.get("metadata", {}) if isinstance(record, dict) else {}
        metadata = metadata if isinstance(metadata, dict) else {}
        token_url = str(metadata.get("token_url", "")).strip() or spec.token_url
        client_id = str(metadata.get("client_id", "")).strip() or str(os.getenv(spec.client_id_env, "")).strip()
        client_secret = str(metadata.get("client_secret", "")).strip() or str(os.getenv(spec.client_secret_env, "")).strip()
        refresh_token = str(record.get("refresh_token", "")).strip()
        if not token_url or not client_id:
            return {"status": "error", "message": "token_url and client_id are required for OAuth refresh."}
        if not refresh_token:
            return {"status": "error", "message": "refresh token is not available"}

        payload: Dict[str, Any] = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
        }
        if client_secret:
            payload["client_secret"] = client_secret
        scopes = _normalize_scopes(record.get("scopes"))
        if scopes:
            payload["scope"] = " ".join(scopes)

        try:
            response = requests.post(token_url, data=payload, timeout=25, headers={"Accept": "application/json"})
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        body = self._response_json(response)
        if int(response.status_code) >= 300:
            provider_error = str(body.get("error", "")).strip()
            provider_error_description = str(body.get("error_description", "")).strip()
            message = provider_error_description or provider_error or f"refresh failed ({response.status_code})"
            return {"status": "error", "message": message}

        access_token = str(body.get("access_token", "")).strip()
        if not access_token:
            return {"status": "error", "message": "refresh response missing access_token"}
        expires_in_s = self._to_int(body.get("expires_in"), default=3600, minimum=30, maximum=86400 * 365)
        return {
            "status": "success",
            "access_token": access_token,
            "refresh_token": str(body.get("refresh_token", "")).strip(),
            "expires_in_s": expires_in_s,
            "expires_at": _iso(_utc_now() + timedelta(seconds=expires_in_s)),
            "token_type": str(body.get("token_type", "Bearer")).strip() or "Bearer",
            "scopes": self._scopes_from_response(body) or scopes,
        }

    def _prune(self) -> None:
        with self._lock:
            self._prune_locked()

    def _prune_locked(self) -> None:
        now = _utc_now()
        expired: List[str] = []
        for flow_id, record in self._flows.items():
            expires = _parse_iso(record.expires_at)
            if expires is not None and expires <= now:
                expired.append(flow_id)
        for flow_id in expired:
            row = self._flows.pop(flow_id, None)
            if row:
                self._state_index.pop(row.state, None)

        if len(self._flows) <= self.max_records:
            return
        ordered = sorted(self._flows.values(), key=lambda item: item.created_at)
        overflow = len(ordered) - self.max_records
        for item in ordered[:overflow]:
            self._flows.pop(item.session_id, None)
            self._state_index.pop(item.state, None)

    @staticmethod
    def _build_pkce_verifier() -> str:
        raw = secrets.token_urlsafe(72)
        verifier = raw.replace("=", "")
        if len(verifier) < 43:
            verifier = verifier + ("x" * (43 - len(verifier)))
        return verifier[:128]

    @staticmethod
    def _build_pkce_challenge(verifier: str) -> str:
        digest = hashlib.sha256(verifier.encode("ascii", errors="ignore")).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    @staticmethod
    def _to_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _requests():
        try:
            import requests  # type: ignore

            return requests
        except Exception:
            return None

    @staticmethod
    def _response_json(response: Any) -> Dict[str, Any]:
        if response is None:
            return {}
        try:
            payload = response.json()
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        text = str(getattr(response, "text", "") or "").strip()
        if not text:
            return {}
        return {"raw": text[:1000]}

    @staticmethod
    def _scopes_from_response(payload: Dict[str, Any]) -> List[str]:
        scope = payload.get("scope")
        if isinstance(scope, str) and scope.strip():
            return [item for item in scope.strip().split(" ") if item]
        scopes = payload.get("scopes")
        return _normalize_scopes(scopes)
