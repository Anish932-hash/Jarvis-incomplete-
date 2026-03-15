from __future__ import annotations

import json
import os
import time
import urllib.request
from urllib.error import HTTPError, URLError
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.database.local_store import LocalStore
from backend.python.inference.model_setup_installer import select_install_items


class ModelSetupRemoteMetadataProbe:
    def __init__(
        self,
        *,
        cache_path: str = "data/model_setup_remote_metadata.json",
        cache_ttl_s: float = 21600.0,
        provider_credentials: Optional[ProviderCredentialManager] = None,
    ) -> None:
        self._store = LocalStore(cache_path)
        self._cache_ttl_s = max(60.0, float(cache_ttl_s))
        self._provider_credentials = provider_credentials

    def plan_metadata(
        self,
        *,
        plan_payload: Dict[str, Any],
        item_keys: Optional[List[str]] = None,
        refresh: bool = False,
        timeout_s: float = 6.0,
    ) -> Dict[str, Any]:
        items = select_install_items(plan_payload, item_keys=item_keys)
        rows: List[Dict[str, Any]] = []
        item_map: Dict[str, Dict[str, Any]] = {}
        cache_hits = 0
        size_known_count = 0
        success_count = 0
        for item in items:
            payload = _augment_remote_payload(
                self.item_metadata(item=item, refresh=bool(refresh), timeout_s=timeout_s)
            )
            rows.append(payload)
            item_key = str(payload.get("key", "") or "").strip().lower()
            if item_key:
                item_map[item_key] = payload
            if bool(payload.get("cached", False)):
                cache_hits += 1
            if bool(payload.get("size_bytes", 0)):
                size_known_count += 1
            if str(payload.get("status", "") or "").strip().lower() == "success":
                success_count += 1
        acquisition_summary = _remote_acquisition_summary(rows)
        return {
            "status": "success",
            "count": len(rows),
            "cache_hits": cache_hits,
            "size_known_count": size_known_count,
            "success_count": success_count,
            "download_ready_count": int(acquisition_summary.get("download_ready_count", 0) or 0),
            "auth_required_count": int(acquisition_summary.get("auth_required_count", 0) or 0),
            "auth_configured_count": int(acquisition_summary.get("auth_configured_count", 0) or 0),
            "auth_missing_count": int(acquisition_summary.get("auth_missing_count", 0) or 0),
            "access_blocked_count": int(acquisition_summary.get("access_blocked_count", 0) or 0),
            "gated_count": int(acquisition_summary.get("gated_count", 0) or 0),
            "private_count": int(acquisition_summary.get("private_count", 0) or 0),
            "manual_attention_count": int(acquisition_summary.get("manual_attention_count", 0) or 0),
            "public_ready_count": int(acquisition_summary.get("public_ready_count", 0) or 0),
            "authenticated_ready_count": int(acquisition_summary.get("authenticated_ready_count", 0) or 0),
            "probe_error_count": int(acquisition_summary.get("probe_error_count", 0) or 0),
            "blocked_count": int(acquisition_summary.get("blocked_count", 0) or 0),
            "acquisition_summary": acquisition_summary,
            "items": rows,
            "item_map": item_map,
        }

    def item_metadata(self, *, item: Dict[str, Any], refresh: bool = False, timeout_s: float = 6.0) -> Dict[str, Any]:
        cache_key = self._cache_key(item)
        if not refresh:
            cached_entry = self._store.get_with_meta(cache_key, default=None)
            cached = cached_entry.get("value")
            if isinstance(cached, dict):
                normalized_cached = _augment_remote_payload(cached)
                return _decorate_cached_payload(normalized_cached, meta=cached_entry.get("meta"), cached=True)

        source_kind = str(item.get("source_kind", "unknown") or "unknown").strip().lower()
        source_ref = str(item.get("source_ref", "") or "").strip()
        source_url = str(item.get("source_url", "") or source_ref).strip()
        hf_token = self._huggingface_token()
        try:
            if source_kind == "direct_url" and source_url:
                payload = self._probe_direct_url(source_url, timeout_s=timeout_s)
            elif source_kind == "huggingface" and source_ref:
                payload = self._probe_huggingface(source_ref, timeout_s=timeout_s, token=hf_token)
            else:
                payload = {"status": "unavailable", "message": "remote metadata probing is not supported for this source"}
        except HTTPError as exc:
            http_status = int(getattr(exc, "code", 0) or 0)
            if source_kind == "huggingface":
                payload = {
                    "status": "auth_required" if http_status in {401, 403, 404} else "error",
                    "message": (
                        "Hugging Face access token is required or lacks access to this repository."
                        if http_status in {401, 403, 404}
                        else f"remote probe failed with HTTP {http_status}"
                    ),
                    "http_status": http_status,
                    "final_url": str(getattr(exc, "url", f"https://huggingface.co/{source_ref}") or f"https://huggingface.co/{source_ref}"),
                    "final_host": "huggingface.co",
                    "repo_id": source_ref,
                    "requires_auth": http_status in {401, 403, 404},
                    "auth_configured": bool(hf_token),
                    "auth_used": bool(hf_token),
                }
            else:
                payload = {
                    "status": "error",
                    "message": f"remote probe failed with HTTP {http_status}",
                    "http_status": http_status,
                    "final_url": str(getattr(exc, "url", source_url) or source_url),
                    "final_host": urlparse(str(getattr(exc, "url", source_url) or source_url)).netloc.lower(),
                }
        except URLError as exc:
            payload = {
                "status": "error",
                "message": str(getattr(exc, "reason", "") or exc),
                "final_url": source_url,
                "final_host": urlparse(source_url).netloc.lower(),
            }
        except Exception as exc:  # noqa: BLE001
            payload = {
                "status": "error",
                "message": str(exc),
                "final_url": source_url,
                "final_host": urlparse(source_url).netloc.lower(),
            }

        payload.update(
            {
                "key": str(item.get("key", "") or ""),
                "name": str(item.get("name", "") or ""),
                "source_kind": source_kind,
                "source_ref": source_ref,
                "source_url": source_url,
                "checked_at": time.time(),
                "cached": False,
            }
        )
        payload = _augment_remote_payload(payload)
        self._store.set(cache_key, payload, ttl_s=self._cache_ttl_s)
        return _decorate_cached_payload(payload, meta={"updated_at": payload.get("checked_at", time.time())}, cached=False)

    @staticmethod
    def _cache_key(item: Dict[str, Any]) -> str:
        source_kind = str(item.get("source_kind", "unknown") or "unknown").strip().lower()
        source_ref = str(item.get("source_ref", "") or item.get("source_url", "") or "").strip().lower()
        key = str(item.get("key", "") or "").strip().lower()
        return f"{source_kind}:{key or source_ref}"

    def _probe_direct_url(self, source_url: str, *, timeout_s: float) -> Dict[str, Any]:
        headers = {"User-Agent": "JARVIS-RemoteProbe/1.0"}
        try:
            request = urllib.request.Request(source_url, headers=headers, method="HEAD")
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                return self._direct_url_payload(response=response, source_url=source_url, mode="head")
        except Exception:
            request = urllib.request.Request(source_url, headers={**headers, "Range": "bytes=0-0"})
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                return self._direct_url_payload(response=response, source_url=source_url, mode="range")

    def _direct_url_payload(self, *, response: Any, source_url: str, mode: str) -> Dict[str, Any]:
        final_url = str(response.geturl() or source_url)
        final_host = urlparse(final_url).netloc.lower()
        size_bytes = _response_size_bytes(response)
        digest_hints = _extract_digest_hints(response)
        return {
            "status": "success",
            "probe_mode": mode,
            "http_status": int(getattr(response, "status", 200) or 200),
            "final_url": final_url,
            "final_host": final_host,
            "size_bytes": size_bytes,
            "etag": str(response.headers.get("ETag", "") or ""),
            "last_modified": str(response.headers.get("Last-Modified", "") or ""),
            "digest_hints": digest_hints,
        }

    def _probe_huggingface(self, repo_id: str, *, timeout_s: float, token: str = "") -> Dict[str, Any]:
        api_url = f"https://huggingface.co/api/models/{repo_id}"
        headers = {"User-Agent": "JARVIS-RemoteProbe/1.0"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = json.loads(response.read().decode("utf-8", errors="replace"))
        siblings = raw.get("siblings", []) if isinstance(raw.get("siblings", []), list) else []
        known_size_total = 0
        siblings_with_size = 0
        for sibling in siblings:
            sibling_size = _huggingface_sibling_size_bytes(sibling)
            if sibling_size > 0:
                known_size_total += sibling_size
                siblings_with_size += 1
        requires_auth = bool(raw.get("gated", False) or raw.get("private", False))
        return {
            "status": "success",
            "probe_mode": "api",
            "http_status": 200,
            "final_url": api_url,
            "final_host": "huggingface.co",
            "repo_id": repo_id,
            "commit_sha": str(raw.get("sha", "") or ""),
            "gated": bool(raw.get("gated", False)),
            "private": bool(raw.get("private", False)),
            "requires_auth": requires_auth,
            "auth_configured": bool(token),
            "auth_used": bool(token),
            "size_bytes": known_size_total,
            "sibling_count": len(siblings),
            "siblings_with_size": siblings_with_size,
        }

    def _huggingface_token(self) -> str:
        manager = self._provider_credentials
        if manager is not None:
            try:
                token = str(manager.get_api_key("huggingface") or "").strip()
                if token:
                    return token
            except Exception:
                pass
        for env_name in ("HUGGINGFACE_HUB_TOKEN", "HF_TOKEN", "HUGGINGFACE_TOKEN"):
            token = str(os.getenv(env_name, "") or "").strip()
            if token:
                return token
        return ""


def _response_size_bytes(response: Any) -> int:
    content_length = str(response.headers.get("Content-Length", "") or "").strip()
    try:
        if content_length:
            return max(0, int(content_length))
    except Exception:
        pass
    content_range = str(response.headers.get("Content-Range", "") or "").strip()
    if "/" in content_range:
        total = content_range.rsplit("/", 1)[-1].strip()
        try:
            return max(0, int(total))
        except Exception:
            return 0
    return 0


def _extract_digest_hints(response: Any) -> Dict[str, str]:
    hints: Dict[str, str] = {}
    content_md5 = str(response.headers.get("Content-MD5", "") or "").strip()
    if content_md5:
        hints["content_md5"] = content_md5
    x_goog_hash = str(response.headers.get("x-goog-hash", "") or "").strip()
    if x_goog_hash:
        for part in x_goog_hash.split(","):
            clean = part.strip()
            if "=" not in clean:
                continue
            name, value = clean.split("=", 1)
            name = name.strip().lower()
            value = value.strip()
            if name and value:
                hints[name] = value
    return hints


def _huggingface_sibling_size_bytes(sibling: Any) -> int:
    if not isinstance(sibling, dict):
        return 0
    direct = sibling.get("size")
    try:
        if direct is not None:
            return max(0, int(direct))
    except Exception:
        pass
    lfs = sibling.get("lfs", {})
    if isinstance(lfs, dict):
        try:
            return max(0, int(lfs.get("size", 0) or 0))
        except Exception:
            return 0
    return 0


def _decorate_cached_payload(payload: Dict[str, Any], *, meta: Any, cached: bool) -> Dict[str, Any]:
    normalized = dict(payload)
    updated_at = 0.0
    if isinstance(meta, dict):
        try:
            updated_at = float(meta.get("updated_at", 0.0) or 0.0)
        except Exception:
            updated_at = 0.0
    normalized["cached"] = bool(cached)
    normalized["cache_age_s"] = max(0.0, time.time() - updated_at) if updated_at > 0.0 else 0.0
    return normalized


def _augment_remote_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload) if isinstance(payload, dict) else {}
    source_kind = str(normalized.get("source_kind", "unknown") or "unknown").strip().lower()
    status_name = str(normalized.get("status", "") or "").strip().lower()
    requires_auth = bool(normalized.get("requires_auth", False))
    auth_configured = bool(normalized.get("auth_configured", False))
    gated = bool(normalized.get("gated", False))
    private = bool(normalized.get("private", False))

    access_mode = "unknown"
    credential_state = "unknown"
    acquisition_stage = "unavailable"
    download_ready = False
    acquisition_blocked = False
    manual_attention_required = False

    if source_kind == "huggingface":
        if private:
            access_mode = "private"
        elif gated or requires_auth:
            access_mode = "gated"
        else:
            access_mode = "public"

        if status_name == "success":
            if requires_auth:
                credential_state = "configured" if auth_configured else "missing"
                if auth_configured:
                    acquisition_stage = "ready_authenticated"
                    download_ready = True
                else:
                    acquisition_stage = "blocked_missing_auth"
                    acquisition_blocked = True
                    manual_attention_required = True
            else:
                credential_state = "not_required"
                acquisition_stage = "ready_public"
                download_ready = True
        elif status_name == "auth_required":
            credential_state = "access_denied" if auth_configured else "missing"
            acquisition_stage = "blocked_access_denied" if auth_configured else "blocked_missing_auth"
            acquisition_blocked = True
            manual_attention_required = True
        elif status_name in {"error", "unavailable"}:
            credential_state = "configured" if auth_configured else ("missing" if requires_auth else "not_required")
            acquisition_stage = "probe_error"
            manual_attention_required = True
        else:
            credential_state = "configured" if auth_configured else ("missing" if requires_auth else "not_required")
    elif source_kind == "direct_url":
        access_mode = "public"
        credential_state = "not_required"
        if status_name == "success":
            acquisition_stage = "ready_public"
            download_ready = True
        elif status_name in {"error", "unavailable"}:
            acquisition_stage = "probe_error"
            manual_attention_required = True
    else:
        if status_name == "success":
            acquisition_stage = "ready_public"
            download_ready = True
        elif status_name in {"error", "auth_required", "unavailable"}:
            acquisition_stage = "probe_error"
            manual_attention_required = True

    normalized["access_mode"] = access_mode
    normalized["credential_state"] = credential_state
    normalized["acquisition_stage"] = acquisition_stage
    normalized["download_ready"] = download_ready
    normalized["acquisition_blocked"] = acquisition_blocked
    normalized["manual_attention_required"] = manual_attention_required
    return normalized


def _remote_acquisition_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    download_ready_count = 0
    auth_required_count = 0
    auth_configured_count = 0
    auth_missing_count = 0
    access_blocked_count = 0
    gated_count = 0
    private_count = 0
    manual_attention_count = 0
    public_ready_count = 0
    authenticated_ready_count = 0
    probe_error_count = 0
    blocked_count = 0
    ready_item_keys: List[str] = []
    blocked_item_keys: List[str] = []
    auth_missing_item_keys: List[str] = []
    access_blocked_item_keys: List[str] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        item_key = str(row.get("key", "") or "").strip().lower()
        acquisition_stage = str(row.get("acquisition_stage", "") or "").strip().lower()
        credential_state = str(row.get("credential_state", "") or "").strip().lower()
        access_mode = str(row.get("access_mode", "") or "").strip().lower()
        requires_auth = bool(row.get("requires_auth", False))
        auth_configured = bool(row.get("auth_configured", False))

        if requires_auth:
            auth_required_count += 1
        if auth_configured:
            auth_configured_count += 1
        if credential_state == "missing":
            auth_missing_count += 1
            if item_key:
                auth_missing_item_keys.append(item_key)
        if credential_state == "access_denied":
            access_blocked_count += 1
            if item_key:
                access_blocked_item_keys.append(item_key)
        if access_mode == "gated":
            gated_count += 1
        if access_mode == "private":
            private_count += 1
        if bool(row.get("manual_attention_required", False)):
            manual_attention_count += 1
        if bool(row.get("download_ready", False)):
            download_ready_count += 1
            if item_key:
                ready_item_keys.append(item_key)
        if acquisition_stage == "ready_public":
            public_ready_count += 1
        if acquisition_stage == "ready_authenticated":
            authenticated_ready_count += 1
        if acquisition_stage == "probe_error":
            probe_error_count += 1
        if acquisition_stage in {"blocked_missing_auth", "blocked_access_denied"}:
            blocked_count += 1
            if item_key:
                blocked_item_keys.append(item_key)

    return {
        "download_ready_count": download_ready_count,
        "auth_required_count": auth_required_count,
        "auth_configured_count": auth_configured_count,
        "auth_missing_count": auth_missing_count,
        "access_blocked_count": access_blocked_count,
        "gated_count": gated_count,
        "private_count": private_count,
        "manual_attention_count": manual_attention_count,
        "public_ready_count": public_ready_count,
        "authenticated_ready_count": authenticated_ready_count,
        "probe_error_count": probe_error_count,
        "blocked_count": blocked_count,
        "ready_item_keys": ready_item_keys,
        "blocked_item_keys": blocked_item_keys,
        "auth_missing_item_keys": auth_missing_item_keys,
        "access_blocked_item_keys": access_blocked_item_keys,
    }
