from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def build_remote_item_map(remote_metadata: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
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


def verify_installed_artifact(
    *,
    target_path: Path,
    item: Dict[str, Any],
    remote_item: Optional[Dict[str, Any]] = None,
    install_metadata: Optional[Dict[str, Any]] = None,
    sample_limit: int = 8,
) -> Dict[str, Any]:
    if not target_path.exists():
        return {
            "status": "missing",
            "verified": False,
            "mode": "missing",
            "errors": ["installed artifact is missing"],
            "warnings": [],
            "size_bytes": 0,
        }
    if target_path.is_file():
        return _verify_file(
            target_path=target_path,
            remote_item=remote_item if isinstance(remote_item, dict) else {},
            install_metadata=install_metadata if isinstance(install_metadata, dict) else {},
        )
    return _verify_directory(
        target_path=target_path,
        remote_item=remote_item if isinstance(remote_item, dict) else {},
        sample_limit=sample_limit,
    )


def _verify_file(*, target_path: Path, remote_item: Dict[str, Any], install_metadata: Dict[str, Any]) -> Dict[str, Any]:
    size_bytes = _to_int(install_metadata.get("bytes_written", 0))
    if size_bytes <= 0:
        try:
            size_bytes = max(0, int(target_path.stat().st_size))
        except Exception:
            size_bytes = 0
    sha256_hex = str(install_metadata.get("sha256_hex", "") or "").strip().lower()
    md5_hex = str(install_metadata.get("md5_hex", "") or "").strip().lower()
    md5_base64 = str(install_metadata.get("md5_base64", "") or "").strip()
    if not sha256_hex or not md5_hex or not md5_base64:
        digests = _hash_file(target_path)
        sha256_hex = sha256_hex or digests["sha256_hex"]
        md5_hex = md5_hex or digests["md5_hex"]
        md5_base64 = md5_base64 or digests["md5_base64"]

    expected_hashes = _expected_file_hashes(remote_item)
    matched_hashes: List[str] = []
    errors: List[str] = []
    warnings: List[str] = []
    status = "observed"
    verified = False

    expected_md5_base64 = str(expected_hashes.get("md5_base64", "") or "").strip()
    expected_md5_hex = str(expected_hashes.get("md5_hex", "") or "").strip().lower()
    if expected_md5_base64 or expected_md5_hex:
        status = "mismatch"
        if expected_md5_base64 and expected_md5_base64 == md5_base64:
            matched_hashes.append("md5_base64")
        if expected_md5_hex and expected_md5_hex == md5_hex:
            matched_hashes.append("md5_hex")
        if matched_hashes:
            status = "verified"
            verified = True
        else:
            errors.append("downloaded file digest did not match the trusted upstream digest hint")
    else:
        warnings.append("No trusted upstream digest was available, so integrity was recorded locally but not externally verified.")

    return {
        "status": status,
        "verified": verified,
        "mode": "file",
        "size_bytes": size_bytes,
        "sha256_hex": sha256_hex,
        "md5_hex": md5_hex,
        "md5_base64": md5_base64,
        "expected_hashes": expected_hashes,
        "matched_hashes": matched_hashes,
        "source_final_url": str(install_metadata.get("final_url", remote_item.get("final_url", "")) or ""),
        "source_final_host": str(install_metadata.get("final_host", remote_item.get("final_host", "")) or ""),
        "etag": str(install_metadata.get("etag", remote_item.get("etag", "")) or ""),
        "errors": errors,
        "warnings": warnings,
    }


def _verify_directory(*, target_path: Path, remote_item: Dict[str, Any], sample_limit: int) -> Dict[str, Any]:
    sample_cap = max(1, min(int(sample_limit), 32))
    files = sorted(item for item in target_path.rglob("*") if item.is_file())
    total_bytes = 0
    manifest_hasher = hashlib.sha256()
    sample_files: List[Dict[str, Any]] = []
    for index, file_path in enumerate(files):
        relative_path = file_path.relative_to(target_path).as_posix()
        try:
            size = int(file_path.stat().st_size)
        except Exception:
            size = 0
        total_bytes += size
        manifest_hasher.update(relative_path.encode("utf-8", errors="ignore"))
        manifest_hasher.update(b"\0")
        manifest_hasher.update(str(size).encode("ascii", errors="ignore"))
        manifest_hasher.update(b"\n")
        if index < sample_cap:
            sample_digest = _hash_file(file_path)
            sample_files.append(
                {
                    "path": relative_path,
                    "size_bytes": size,
                    "sha256_hex": sample_digest["sha256_hex"],
                }
            )

    errors: List[str] = []
    warnings: List[str] = []
    status = "observed"
    if not files:
        status = "missing"
        errors.append("snapshot directory is empty")
    else:
        warnings.append("Snapshot integrity was recorded locally from the installed directory layout.")

    commit_sha = str(remote_item.get("commit_sha", "") or "").strip()
    if commit_sha:
        warnings.append(f"Upstream repo commit observed: {commit_sha[:12]}")

    return {
        "status": status,
        "verified": False,
        "mode": "directory",
        "file_count": len(files),
        "size_bytes": total_bytes,
        "manifest_sha256": manifest_hasher.hexdigest(),
        "sample_files": sample_files,
        "commit_sha": commit_sha,
        "source_final_url": str(remote_item.get("final_url", "") or ""),
        "source_final_host": str(remote_item.get("final_host", "") or ""),
        "errors": errors,
        "warnings": warnings,
    }


def _hash_file(path: Path) -> Dict[str, str]:
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            sha256.update(chunk)
            md5.update(chunk)
    md5_bytes = md5.digest()
    return {
        "sha256_hex": sha256.hexdigest(),
        "md5_hex": md5.hexdigest(),
        "md5_base64": base64.b64encode(md5_bytes).decode("ascii"),
    }


def _expected_file_hashes(remote_item: Dict[str, Any]) -> Dict[str, str]:
    digest_hints = remote_item.get("digest_hints", {})
    values: Dict[str, str] = {}
    if isinstance(digest_hints, dict):
        content_md5 = str(digest_hints.get("content_md5", "") or "").strip()
        if content_md5:
            values["md5_base64"] = content_md5
        md5_hint = str(digest_hints.get("md5", "") or "").strip()
        if md5_hint:
            values["md5_base64"] = md5_hint
    etag = str(remote_item.get("etag", "") or "").strip().strip('"').strip("'")
    if len(etag) == 32 and all(char in "0123456789abcdefABCDEF" for char in etag):
        values.setdefault("md5_hex", etag.lower())
    return values


def _to_int(raw: Any) -> int:
    try:
        return max(0, int(raw))
    except Exception:
        return 0


def build_directory_manifest_json(*, verification: Dict[str, Any]) -> str:
    payload = {
        "file_count": _to_int(verification.get("file_count", 0)),
        "size_bytes": _to_int(verification.get("size_bytes", 0)),
        "manifest_sha256": str(verification.get("manifest_sha256", "") or ""),
        "sample_files": verification.get("sample_files", []) if isinstance(verification.get("sample_files", []), list) else [],
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
