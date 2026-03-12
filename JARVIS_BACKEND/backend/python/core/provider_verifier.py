from __future__ import annotations

import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError

from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProviderCredentialVerifier:
    def __init__(
        self,
        provider_credentials: ProviderCredentialManager,
        *,
        history_path: str = "data/provider_verification_history.json",
        cache_ttl_s: float = 900.0,
    ) -> None:
        self._provider_credentials = provider_credentials
        self._history_path = self._resolve_path(history_path)
        self._store = LocalStore(self._history_path)
        self._cache_ttl_s = max(60.0, float(cache_ttl_s))

    def verify(
        self,
        *,
        provider: str,
        repo_items: Optional[List[Dict[str, Any]]] = None,
        force_refresh: bool = False,
        timeout_s: float = 8.0,
    ) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        if not clean_provider:
            return {"status": "error", "message": "provider is required", "verified": False}

        snapshot = self._provider_credentials.refresh(overwrite_env=False)
        provider_status = (
            snapshot.get("providers", {}).get(clean_provider, {})
            if isinstance(snapshot.get("providers", {}), dict)
            else {}
        )
        if not isinstance(provider_status, dict):
            provider_status = {}

        clean_repo_items = [
            dict(item)
            for item in (repo_items or [])
            if isinstance(item, dict)
        ]
        cache_key = self._cache_key(clean_provider, clean_repo_items)
        if not force_refresh:
            cached_entry = self._store.get_with_meta(cache_key, default=None)
            cached = cached_entry.get("value")
            if isinstance(cached, dict):
                return self._decorate_cached_result(cached, meta=cached_entry.get("meta"))

        api_key = self._provider_credentials.get_api_key(clean_provider)
        if not api_key:
            result = {
                "status": "error",
                "provider": clean_provider,
                "verified": False,
                "message": "No provider credential is currently configured.",
                "summary": "Credential missing",
                "warnings": [],
                "errors": ["Provider credential is not configured."],
                "provider_status": dict(provider_status),
                "checked_at": _utc_now_iso(),
                "cached": False,
                "cache_key": cache_key,
                "latency_ms": 0.0,
            }
            self._record_result(result)
            return result

        started = time.perf_counter()
        try:
            if clean_provider == "huggingface":
                result = self._verify_huggingface(
                    api_key=api_key,
                    repo_items=clean_repo_items,
                    timeout_s=timeout_s,
                )
            elif clean_provider == "groq":
                result = self._verify_groq(api_key=api_key, timeout_s=timeout_s)
            elif clean_provider == "elevenlabs":
                result = self._verify_elevenlabs(api_key=api_key, timeout_s=timeout_s)
            elif clean_provider == "nvidia":
                result = self._verify_nvidia(api_key=api_key, timeout_s=timeout_s)
            else:
                result = {
                    "status": "unavailable",
                    "provider": clean_provider,
                    "verified": False,
                    "message": f"Active verification is not implemented for {clean_provider}.",
                    "summary": "Verification strategy unavailable",
                    "warnings": [],
                    "errors": [],
                }
        except HTTPError as exc:
            http_status = int(getattr(exc, "code", 0) or 0)
            result = {
                "status": "error",
                "provider": clean_provider,
                "verified": False,
                "message": f"Provider rejected the credential with HTTP {http_status}.",
                "summary": f"Credential rejected (HTTP {http_status})",
                "warnings": [],
                "errors": [f"HTTP {http_status}: {str(getattr(exc, 'reason', '') or getattr(exc, 'msg', '') or 'request failed')}"],
                "http_status": http_status,
                "endpoint": str(getattr(exc, "url", "") or ""),
            }
        except URLError as exc:
            result = {
                "status": "error",
                "provider": clean_provider,
                "verified": False,
                "message": "Provider verification failed due to a network error.",
                "summary": "Network error during verification",
                "warnings": [],
                "errors": [str(getattr(exc, "reason", "") or exc)],
            }
        except Exception as exc:  # noqa: BLE001
            result = {
                "status": "error",
                "provider": clean_provider,
                "verified": False,
                "message": str(exc),
                "summary": "Verification failed",
                "warnings": [],
                "errors": [str(exc)],
            }

        warnings = [
            str(item).strip()
            for item in result.get("warnings", [])
            if str(item).strip()
        ] if isinstance(result.get("warnings", []), list) else []
        errors = [
            str(item).strip()
            for item in result.get("errors", [])
            if str(item).strip()
        ] if isinstance(result.get("errors", []), list) else []
        missing_requirements = [
            str(item).strip()
            for item in provider_status.get("missing_requirements", [])
            if str(item).strip()
        ] if isinstance(provider_status.get("missing_requirements", []), list) else []
        if missing_requirements:
            warnings.append(f"Provider-specific requirements are still missing: {', '.join(missing_requirements)}.")

        finalized = {
            **result,
            "provider": clean_provider,
            "provider_status": dict(provider_status),
            "checked_at": _utc_now_iso(),
            "cached": False,
            "cache_key": cache_key,
            "latency_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "warnings": _dedupe_strings(warnings),
            "errors": _dedupe_strings(errors),
        }
        self._store.set(cache_key, finalized, ttl_s=self._cache_ttl_s)
        self._record_result(finalized)
        return finalized

    def latest(self, provider: str) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        if not clean_provider:
            return {}
        payload = self._store.get(f"latest:{clean_provider}", {})
        return dict(payload) if isinstance(payload, dict) else {}

    def latest_map(self, providers: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        names = providers or self._provider_credentials.providers()
        result: Dict[str, Dict[str, Any]] = {}
        for provider_name in names:
            clean_provider = str(provider_name or "").strip().lower()
            if not clean_provider:
                continue
            row = self.latest(clean_provider)
            if row:
                result[clean_provider] = row
        return result

    def _record_result(self, payload: Dict[str, Any]) -> None:
        provider = str(payload.get("provider", "") or "").strip().lower()
        if not provider:
            return
        self._store.set(f"latest:{provider}", dict(payload))
        runs = self._store.get("runs", [])
        rows = [dict(item) for item in runs if isinstance(item, dict)] if isinstance(runs, list) else []
        rows.insert(0, dict(payload))
        self._store.set("runs", rows[:80])

    def _verify_huggingface(
        self,
        *,
        api_key: str,
        repo_items: List[Dict[str, Any]],
        timeout_s: float,
    ) -> Dict[str, Any]:
        headers = {
            "User-Agent": "JARVIS-ProviderVerifier/1.0",
            "Authorization": f"Bearer {api_key}",
        }
        identity_raw, http_status, endpoint = self._request_json(
            "https://huggingface.co/api/whoami-v2",
            headers=headers,
            timeout_s=timeout_s,
        )
        organizations = identity_raw.get("orgs", []) if isinstance(identity_raw.get("orgs", []), list) else []
        repo_access: List[Dict[str, Any]] = []
        accessible_count = 0
        blocked_count = 0
        for item in repo_items[:12]:
            repo_id = str(item.get("source_ref", "") or "").strip()
            if not repo_id:
                continue
            access_row = self._verify_huggingface_repo(
                repo_id=repo_id,
                timeout_s=timeout_s,
                headers=headers,
            )
            repo_access.append(access_row)
            if bool(access_row.get("accessible", False)):
                accessible_count += 1
            else:
                blocked_count += 1

        warnings: List[str] = []
        errors: List[str] = []
        for row in repo_access:
            if bool(row.get("accessible", False)):
                continue
            message = str(row.get("message", "") or "").strip()
            if message:
                errors.append(message)

        verified = blocked_count == 0
        summary = (
            f"Verified Hugging Face access; checked {len(repo_access)} repo(s), {accessible_count} accessible."
            if repo_access
            else "Verified Hugging Face access token."
        )
        return {
            "status": "success" if verified else "partial",
            "provider": "huggingface",
            "verified": verified,
            "message": summary,
            "summary": summary,
            "warnings": warnings,
            "errors": errors,
            "endpoint": endpoint,
            "http_status": http_status,
            "identity": {
                "name": str(identity_raw.get("name", "") or identity_raw.get("fullname", "") or "").strip(),
                "type": str(identity_raw.get("type", "") or "").strip(),
                "organization_count": len(organizations),
            },
            "repo_access": repo_access,
            "repo_access_checked": len(repo_access),
            "repo_access_ok": blocked_count == 0,
            "repo_accessible_count": accessible_count,
            "repo_blocked_count": blocked_count,
        }

    def _verify_huggingface_repo(
        self,
        *,
        repo_id: str,
        timeout_s: float,
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        url = f"https://huggingface.co/api/models/{repo_id}"
        try:
            payload, http_status, endpoint = self._request_json(
                url,
                headers=headers,
                timeout_s=timeout_s,
            )
            return {
                "repo_id": repo_id,
                "status": "success",
                "accessible": True,
                "http_status": http_status,
                "endpoint": endpoint,
                "gated": bool(payload.get("gated", False)),
                "private": bool(payload.get("private", False)),
                "commit_sha": str(payload.get("sha", "") or "").strip(),
                "message": "repo access verified",
            }
        except HTTPError as exc:
            http_status = int(getattr(exc, "code", 0) or 0)
            requires_auth = http_status in {401, 403, 404}
            return {
                "repo_id": repo_id,
                "status": "auth_required" if requires_auth else "error",
                "accessible": False,
                "http_status": http_status,
                "endpoint": str(getattr(exc, "url", url) or url),
                "gated": False,
                "private": False,
                "message": (
                    f"Credential could not access {repo_id}."
                    if requires_auth
                    else f"Repo probe failed for {repo_id} with HTTP {http_status}."
                ),
            }

    def _verify_groq(self, *, api_key: str, timeout_s: float) -> Dict[str, Any]:
        payload, http_status, endpoint = self._request_json(
            "https://api.groq.com/openai/v1/models",
            headers={
                "User-Agent": "JARVIS-ProviderVerifier/1.0",
                "Authorization": f"Bearer {api_key}",
            },
            timeout_s=timeout_s,
        )
        rows = payload.get("data", []) if isinstance(payload.get("data", []), list) else []
        sample_models = [
            str(item.get("id", "")).strip()
            for item in rows[:5]
            if isinstance(item, dict) and str(item.get("id", "")).strip()
        ]
        return {
            "status": "success",
            "provider": "groq",
            "verified": True,
            "message": f"Verified Groq access; discovered {len(rows)} models.",
            "summary": f"Verified Groq access; discovered {len(rows)} models.",
            "warnings": [],
            "errors": [],
            "endpoint": endpoint,
            "http_status": http_status,
            "identity": {
                "model_count": len(rows),
                "sample_models": sample_models,
            },
        }

    def _verify_elevenlabs(self, *, api_key: str, timeout_s: float) -> Dict[str, Any]:
        payload, http_status, endpoint = self._request_json(
            "https://api.elevenlabs.io/v1/user",
            headers={
                "User-Agent": "JARVIS-ProviderVerifier/1.0",
                "xi-api-key": api_key,
            },
            timeout_s=timeout_s,
        )
        subscription = payload.get("subscription", {}) if isinstance(payload.get("subscription", {}), dict) else {}
        user_id = str(payload.get("user_id", "") or payload.get("subscription_id", "") or "").strip()
        tier = str(subscription.get("tier", "") or "").strip()
        summary = f"Verified ElevenLabs access for {user_id or 'configured account'}."
        return {
            "status": "success",
            "provider": "elevenlabs",
            "verified": True,
            "message": summary,
            "summary": summary,
            "warnings": [],
            "errors": [],
            "endpoint": endpoint,
            "http_status": http_status,
            "identity": {
                "user_id": user_id,
                "subscription_tier": tier,
                "can_extend_character_limit": bool(subscription.get("can_extend_character_limit", False)),
            },
        }

    def _verify_nvidia(self, *, api_key: str, timeout_s: float) -> Dict[str, Any]:
        payload, http_status, endpoint = self._request_json(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            method="POST",
            headers={
                "User-Agent": "JARVIS-ProviderVerifier/1.0",
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            body={
                "model": "meta/llama-3.1-8b-instruct",
                "messages": [{"role": "user", "content": "ping"}],
                "max_tokens": 1,
                "temperature": 0,
            },
            timeout_s=max(4.0, timeout_s),
        )
        usage = payload.get("usage", {}) if isinstance(payload.get("usage", {}), dict) else {}
        summary = "Verified NVIDIA access with a minimal inference probe."
        return {
            "status": "success",
            "provider": "nvidia",
            "verified": True,
            "message": summary,
            "summary": summary,
            "warnings": ["NVIDIA verification uses a minimal live model invocation and may incur negligible usage."],
            "errors": [],
            "endpoint": endpoint,
            "http_status": http_status,
            "identity": {
                "model": str(payload.get("model", "") or "").strip(),
                "total_tokens": int(usage.get("total_tokens", 0) or 0),
            },
        }

    @staticmethod
    def _request_json(
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        timeout_s: float,
        method: str = "GET",
        body: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], int, str]:
        payload_bytes = json.dumps(body).encode("utf-8") if isinstance(body, dict) else None
        request = urllib.request.Request(
            url,
            data=payload_bytes,
            headers=headers or {},
            method=str(method or "GET").strip().upper() or "GET",
        )
        with urllib.request.urlopen(request, timeout=max(1.0, min(float(timeout_s), 30.0))) as response:
            raw = response.read()
            parsed = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
            return (
                parsed if isinstance(parsed, dict) else {},
                int(getattr(response, "status", 200) or 200),
                str(response.geturl() or url),
            )

    @staticmethod
    def _cache_key(provider: str, repo_items: List[Dict[str, Any]]) -> str:
        repo_ids = sorted(
            {
                str(item.get("source_ref", "") or "").strip().lower()
                for item in repo_items
                if str(item.get("source_ref", "") or "").strip()
            }
        )
        suffix = "|".join(repo_ids[:16])
        return f"provider_verify:{provider}:{suffix}" if suffix else f"provider_verify:{provider}"

    @staticmethod
    def _decorate_cached_result(payload: Dict[str, Any], *, meta: Any) -> Dict[str, Any]:
        row = dict(payload)
        updated_at = 0.0
        if isinstance(meta, dict):
            try:
                updated_at = float(meta.get("updated_at", 0.0) or 0.0)
            except Exception:
                updated_at = 0.0
        row["cached"] = True
        row["cache_age_s"] = max(0.0, time.time() - updated_at) if updated_at > 0.0 else 0.0
        return row

    @staticmethod
    def _resolve_path(raw_path: str) -> str:
        clean = str(raw_path or "").strip()
        if not clean:
            return str(Path.cwd())
        candidate = Path(clean)
        if candidate.is_absolute():
            return str(candidate)
        cwd = Path.cwd().resolve()
        for option in (cwd / clean, cwd.parent / clean, cwd.parent.parent / clean):
            if option.exists():
                return str(option)
        return str(cwd / clean)


def _dedupe_strings(values: List[str]) -> List[str]:
    rows: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
    return rows
