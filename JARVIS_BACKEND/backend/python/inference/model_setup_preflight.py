from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


_TRUSTED_DIRECT_HOSTS = {
    "dl.fbaipublicfiles.com": "official meta checkpoint host",
    "github.com": "official github release host",
    "objects.githubusercontent.com": "github release asset host",
    "release-assets.githubusercontent.com": "github release asset host",
}

_TRUSTED_HF_ORGS = {
    "sentence-transformers",
    "facebook",
    "openai",
    "hexgrad",
    "qwen",
    "meta-llama",
    "deepseek-ai",
    "canopylabs",
}


def build_model_setup_preflight(
    *,
    plan_payload: Dict[str, Any],
    item_keys: Optional[List[str]] = None,
    reserve_bytes: int = 1_073_741_824,
    remote_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    items = [
        dict(item)
        for item in plan_payload.get("items", [])
        if isinstance(item, dict)
    ]
    remote_item_map = _remote_item_map(remote_metadata)
    selected_keys = {
        str(item).strip().lower()
        for item in (item_keys or [])
        if str(item).strip()
    }
    selected_items: List[Dict[str, Any]] = []
    for item in items:
        clean_key = str(item.get("key", "") or "").strip().lower()
        if selected_keys:
            if clean_key in selected_keys:
                selected_items.append(item)
            continue
        if bool(item.get("automation_ready", False)):
            selected_items.append(item)

    rows: List[Dict[str, Any]] = []
    ready_count = 0
    warning_count = 0
    blocked_count = 0
    trusted_count = 0
    writable_count = 0
    required_bytes_total = 0
    remote_probe_count = 0
    remote_cache_hits = 0
    remote_known_size_count = 0
    remote_success_count = 0

    for item in selected_items:
        item_key = str(item.get("key", "") or "").strip().lower()
        remote_item = remote_item_map.get(item_key, {})
        row = _build_item_preflight(
            item=item,
            reserve_bytes=max(0, int(reserve_bytes)),
            remote_item=remote_item if isinstance(remote_item, dict) else None,
        )
        rows.append(row)
        status_name = str(row.get("status", "blocked") or "blocked").strip().lower()
        if status_name == "ready":
            ready_count += 1
        elif status_name == "warning":
            warning_count += 1
        else:
            blocked_count += 1
        trust = row.get("source_trust", {}) if isinstance(row.get("source_trust", {}), dict) else {}
        if bool(trust.get("trusted", False)):
            trusted_count += 1
        filesystem = row.get("filesystem", {}) if isinstance(row.get("filesystem", {}), dict) else {}
        if bool(filesystem.get("writable", False)):
            writable_count += 1
        disk = row.get("disk", {}) if isinstance(row.get("disk", {}), dict) else {}
        required_bytes_total += max(0, int(disk.get("required_bytes", 0) or 0))
        remote_probe = row.get("remote_probe", {}) if isinstance(row.get("remote_probe", {}), dict) else {}
        if remote_probe:
            remote_probe_count += 1
            if bool(remote_probe.get("cached", False)):
                remote_cache_hits += 1
            if bool(remote_probe.get("size_known", False)):
                remote_known_size_count += 1
            if str(remote_probe.get("status", "") or "").strip().lower() == "success":
                remote_success_count += 1

    return {
        "status": "success",
        "summary": {
            "selected_count": len(rows),
            "ready_count": ready_count,
            "warning_count": warning_count,
            "blocked_count": blocked_count,
            "trusted_count": trusted_count,
            "writable_count": writable_count,
            "required_bytes_total": required_bytes_total,
            "reserve_bytes": max(0, int(reserve_bytes)),
            "launch_recommended": blocked_count == 0,
            "remote_probe_count": remote_probe_count,
            "remote_cache_hits": remote_cache_hits,
            "remote_known_size_count": remote_known_size_count,
            "remote_success_count": remote_success_count,
        },
        "items": rows,
    }


def _build_item_preflight(
    *,
    item: Dict[str, Any],
    reserve_bytes: int,
    remote_item: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target_path = Path(str(item.get("path", "") or ""))
    filesystem = _filesystem_snapshot(target_path)
    required_bytes = _estimate_required_bytes(item, remote_item=remote_item)
    disk = _disk_snapshot(target_path=target_path, required_bytes=required_bytes, reserve_bytes=reserve_bytes)
    trust = _source_trust_snapshot(item, remote_item=remote_item)
    remote_probe = _remote_probe_snapshot(remote_item)
    blockers: List[str] = []
    warnings: List[str] = []

    if not bool(item.get("automation_ready", False)):
        blockers.append("This setup item is not automation-ready.")
    if not bool(filesystem.get("writable", False)):
        blockers.append("Target parent path is not writable by the current runtime.")
    if bool(disk.get("known_required_bytes", False)) and not bool(disk.get("enough_space", False)):
        blockers.append("Available disk space is below the estimated requirement plus reserve.")
    if not bool(trust.get("trusted", False)):
        blockers.append(str(trust.get("reason", "source trust could not be verified") or "source trust could not be verified"))
    if not bool(disk.get("known_required_bytes", False)):
        warnings.append("Required download size is unknown, so disk sufficiency could not be fully verified.")
    remote_status = str(remote_probe.get("status", "") or "").strip().lower()
    if remote_probe:
        auth_configured = bool(remote_probe.get("auth_configured", False))
        if bool(remote_probe.get("requires_auth", False)) and not auth_configured:
            blockers.append("Remote source requires a configured Hugging Face access token before automation can download it.")
        elif remote_status == "auth_required" and auth_configured:
            blockers.append("Configured Hugging Face credentials could not access this repository; confirm the token or repo grant.")
        elif remote_status not in {"", "success"}:
            message = str(remote_probe.get("message", "") or "").strip()
            warnings.append(f"Remote probe fallback in use{': ' + message if message else '.'}")
    for entry in item.get("blockers", []):
        clean = str(entry or "").strip()
        if clean:
            warnings.append(clean)

    status_name = "ready"
    if blockers:
        status_name = "blocked"
    elif warnings:
        status_name = "warning"

    return {
        "key": str(item.get("key", "") or ""),
        "name": str(item.get("name", "") or target_path.name or "model"),
        "task": str(item.get("task", "") or "unknown"),
        "path": str(target_path),
        "strategy": str(item.get("strategy", "manual") or "manual"),
        "status": status_name,
        "launch_ready": status_name != "blocked",
        "filesystem": filesystem,
        "disk": disk,
        "source_trust": trust,
        "remote_probe": remote_probe,
        "remote_metadata": remote_probe,
        "blockers": blockers,
        "warnings": warnings,
    }


def _filesystem_snapshot(target_path: Path) -> Dict[str, Any]:
    parent = target_path if target_path.is_dir() else target_path.parent
    parent = parent if str(parent) else Path.cwd()
    nearest_existing = _nearest_existing_parent(parent)
    writable = False
    if nearest_existing is not None:
        writable = os.access(str(nearest_existing), os.W_OK)
    return {
        "target_exists": target_path.exists(),
        "parent_path": str(parent),
        "nearest_existing_parent": str(nearest_existing) if nearest_existing is not None else "",
        "writable": bool(writable),
    }


def _nearest_existing_parent(path: Path) -> Optional[Path]:
    candidate = path
    while True:
        if candidate.exists():
            return candidate
        if candidate.parent == candidate:
            return None
        candidate = candidate.parent


def _disk_snapshot(*, target_path: Path, required_bytes: int, reserve_bytes: int) -> Dict[str, Any]:
    parent = target_path if target_path.is_dir() else target_path.parent
    nearest_existing = _nearest_existing_parent(parent)
    total_bytes = 0
    free_bytes = 0
    if nearest_existing is not None:
        try:
            usage = shutil.disk_usage(str(nearest_existing))
            total_bytes = int(usage.total)
            free_bytes = int(usage.free)
        except Exception:
            total_bytes = 0
            free_bytes = 0
    known_required = required_bytes > 0
    margin_bytes = free_bytes - required_bytes - max(0, int(reserve_bytes))
    return {
        "required_bytes": max(0, int(required_bytes)),
        "known_required_bytes": known_required,
        "reserve_bytes": max(0, int(reserve_bytes)),
        "total_bytes": total_bytes,
        "free_bytes": free_bytes,
        "margin_bytes": margin_bytes,
        "enough_space": bool(not known_required or margin_bytes >= 0),
    }


def _source_trust_snapshot(item: Dict[str, Any], remote_item: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    source_kind = str(item.get("source_kind", "unknown") or "unknown").strip().lower()
    source_url = str(item.get("source_url", "") or "")
    source_ref = str(item.get("source_ref", "") or "")
    matched_rule = str(item.get("matched_rule", "") or "").strip().lower()
    remote = remote_item if isinstance(remote_item, dict) else {}
    resolved_url = str(remote.get("final_url", "") or source_url)
    parsed = urlparse(resolved_url) if resolved_url else None
    host = parsed.netloc.lower() if parsed and parsed.netloc else ""
    if source_kind == "huggingface" and source_ref:
        org = source_ref.split("/", 1)[0].strip().lower()
        trusted = bool(org in _TRUSTED_HF_ORGS and matched_rule and matched_rule != "custom-manual")
        reason = "verified official Hugging Face mapping" if trusted else "Hugging Face source is not in the verified organization allowlist."
        if bool(remote.get("requires_auth", False)):
            reason = f"{reason} but the repository currently requires authenticated access."
        return {
            "trusted": trusted,
            "host": "huggingface.co",
            "resolved_host": host or "huggingface.co",
            "reason": reason,
            "scheme": "https",
            "source_ref": source_ref,
            "trust_source": "remote" if str(remote.get("final_url", "") or "").strip() else "declared",
        }
    if source_kind == "direct_url" and (resolved_url or source_url):
        scheme = parsed.scheme.lower() if parsed and parsed.scheme else ""
        host_reason = _TRUSTED_DIRECT_HOSTS.get(host, "")
        trusted = bool(scheme == "https" and host_reason and matched_rule and matched_rule != "custom-manual")
        if scheme != "https":
            reason = "Direct downloads must use HTTPS."
        elif host_reason and host != urlparse(source_url).netloc.lower():
            reason = f"verified redirect target via {host_reason}"
        elif host_reason:
            reason = f"verified {host_reason}"
        else:
            reason = "Direct download host is not in the verified source allowlist."
        return {
            "trusted": trusted,
            "host": host,
            "resolved_host": host,
            "reason": reason,
            "scheme": scheme,
            "source_ref": source_ref,
            "trust_source": "remote" if str(remote.get("final_url", "") or "").strip() else "declared",
            "resolved_url": resolved_url,
        }
    return {
        "trusted": False,
        "host": host,
        "resolved_host": host,
        "reason": "No verified upstream source mapping is available.",
        "scheme": parsed.scheme.lower() if parsed and parsed.scheme else "",
        "trust_source": "remote" if str(remote.get("final_url", "") or "").strip() else "declared",
    }


def _estimate_required_bytes(item: Dict[str, Any], remote_item: Optional[Dict[str, Any]] = None) -> int:
    remote = remote_item if isinstance(remote_item, dict) else {}
    try:
        remote_size = int(remote.get("size_bytes", 0) or 0)
    except Exception:
        remote_size = 0
    if remote_size > 0:
        return remote_size

    strategy = str(item.get("strategy", "") or "").strip().lower()
    source_ref = str(item.get("source_ref", "") or "").strip().lower()
    name = str(item.get("name", "") or "").strip().lower()
    family = str(item.get("family", "") or "").strip().lower()
    haystack = " ".join([strategy, source_ref, name, family])

    if "yolov10x" in haystack:
        return 115_000_000
    if "sam_vit_h" in haystack:
        return 2_650_000_000
    if "all-mpnet-base-v2" in haystack or "multi-qa-mpnet-base-dot-v1" in haystack:
        return 550_000_000
    if "bart-large-mnli" in haystack:
        return 1_700_000_000
    if "whisper-large-v3" in haystack:
        return 3_400_000_000
    if "whisper-medium" in haystack:
        return 1_700_000_000
    if "kokoro-82m" in haystack:
        return 650_000_000

    approx_b = _extract_billion_scale(source_ref or name)
    if approx_b > 0:
        return int(approx_b * 2_250_000_000)
    return 0


def _extract_billion_scale(value: str) -> float:
    text = str(value or "").lower()
    for token in text.replace("_", "-").split("-"):
        clean = token.strip()
        if clean.endswith("b"):
            number = clean[:-1]
            try:
                return float(number)
            except Exception:
                continue
    return 0.0


def _remote_item_map(remote_metadata: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(remote_metadata, dict):
        return {}
    item_map = remote_metadata.get("item_map", {})
    if isinstance(item_map, dict):
        normalized: Dict[str, Dict[str, Any]] = {}
        for key, value in item_map.items():
            clean_key = str(key or "").strip().lower()
            if clean_key and isinstance(value, dict):
                normalized[clean_key] = dict(value)
        if normalized:
            return normalized
    items = remote_metadata.get("items", [])
    normalized = {}
    if isinstance(items, list):
        for value in items:
            if not isinstance(value, dict):
                continue
            clean_key = str(value.get("key", "") or "").strip().lower()
            if clean_key:
                normalized[clean_key] = dict(value)
    return normalized


def _remote_probe_snapshot(remote_item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(remote_item, dict) or not remote_item:
        return {}
    snapshot = {
        "status": str(remote_item.get("status", "") or ""),
        "message": str(remote_item.get("message", "") or ""),
        "probe_mode": str(remote_item.get("probe_mode", "") or ""),
        "cached": bool(remote_item.get("cached", False)),
        "cache_age_s": float(remote_item.get("cache_age_s", 0.0) or 0.0),
        "checked_at": float(remote_item.get("checked_at", 0.0) or 0.0),
        "final_url": str(remote_item.get("final_url", "") or ""),
        "final_host": str(remote_item.get("final_host", "") or ""),
        "http_status": int(remote_item.get("http_status", 0) or 0),
        "size_bytes": max(0, int(remote_item.get("size_bytes", 0) or 0)),
        "size_known": bool(int(remote_item.get("size_bytes", 0) or 0) > 0),
        "etag": str(remote_item.get("etag", "") or ""),
        "last_modified": str(remote_item.get("last_modified", "") or ""),
        "digest_hints": dict(remote_item.get("digest_hints", {})) if isinstance(remote_item.get("digest_hints", {}), dict) else {},
        "repo_id": str(remote_item.get("repo_id", "") or ""),
        "commit_sha": str(remote_item.get("commit_sha", "") or ""),
        "gated": bool(remote_item.get("gated", False)),
        "private": bool(remote_item.get("private", False)),
        "requires_auth": bool(remote_item.get("requires_auth", False)),
        "auth_configured": bool(remote_item.get("auth_configured", False)),
        "auth_used": bool(remote_item.get("auth_used", False)),
        "sibling_count": int(remote_item.get("sibling_count", 0) or 0),
        "siblings_with_size": int(remote_item.get("siblings_with_size", 0) or 0),
    }
    return snapshot
