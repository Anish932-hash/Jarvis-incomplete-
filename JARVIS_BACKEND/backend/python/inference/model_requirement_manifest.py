from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List


_PATH_RE = re.compile(r"([A-Za-z]:[\\/][^\"'\r\n]+)")
_NUMBERED_ITEM_RE = re.compile(r"^\s*\d+\)\s*(.+?)\s*$")
_PROVIDER_ALIASES = {
    "groq": "groq",
    "elevenlabs": "elevenlabs",
    "eleven": "elevenlabs",
    "nvidia": "nvidia",
    "nim": "nvidia",
}


def load_model_requirement_manifest(*, manifest_path: str = "") -> Dict[str, Any]:
    path = _resolve_manifest_path(manifest_path)
    payload: Dict[str, Any] = {
        "status": "missing",
        "path": str(path),
        "model_count": 0,
        "provider_count": 0,
        "models": [],
        "providers": [],
    }
    if not path.exists() or not path.is_file():
        return payload

    try:
        raw_text = path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        payload["status"] = "error"
        payload["message"] = str(exc)
        return payload

    section = "models"
    model_paths: List[str] = []
    providers: List[str] = []
    seen_model_paths: set[str] = set()
    seen_providers: set[str] = set()

    for raw_line in raw_text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        lowered = line.lower()
        if "api keys" in lowered:
            section = "providers"
            continue

        path_candidate = _extract_path_candidate(line)
        if path_candidate:
            key = path_candidate.lower()
            if key not in seen_model_paths:
                seen_model_paths.add(key)
                model_paths.append(path_candidate)
            continue

        if section != "providers":
            continue

        provider_name = _normalize_provider_name(line)
        if provider_name and provider_name not in seen_providers:
            seen_providers.add(provider_name)
            providers.append(provider_name)

    models = [_manifest_model_row(raw_path) for raw_path in model_paths]
    payload.update(
        {
            "status": "success",
            "model_count": len(models),
            "provider_count": len(providers),
            "models": models,
            "providers": providers,
        }
    )
    return payload


def _resolve_manifest_path(raw_path: str) -> Path:
    clean = str(raw_path or "").strip()
    if clean:
        candidate = Path(clean)
        if candidate.is_absolute():
            return _resolve_path(candidate)
        cwd = Path.cwd().resolve()
        for option in (cwd / clean, cwd / "JARVIS_BACKEND" / clean, cwd.parent / clean, cwd.parent / "JARVIS_BACKEND" / clean):
            if option.exists():
                return _resolve_path(option)
        return _resolve_path(cwd / clean)

    cwd = Path.cwd().resolve()
    candidates = (
        cwd / "JARVIS_BACKEND" / "Models to Download.txt",
        cwd / "Models to Download.txt",
        cwd.parent / "JARVIS_BACKEND" / "Models to Download.txt",
        cwd.parent / "Models to Download.txt",
        cwd.parent.parent / "JARVIS_BACKEND" / "Models to Download.txt",
    )
    for candidate in candidates:
        if candidate.exists():
            return _resolve_path(candidate)
    return _resolve_path(candidates[0])


def _resolve_path(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except Exception:
        try:
            return path.resolve()
        except Exception:
            return path


def _extract_path_candidate(line: str) -> str:
    match = _PATH_RE.search(str(line or ""))
    if not match:
        return ""
    return str(match.group(1) or "").strip().strip("\"'")


def _normalize_provider_name(line: str) -> str:
    candidate = str(line or "").strip().strip("\"'")
    numbered = _NUMBERED_ITEM_RE.match(candidate)
    if numbered:
        candidate = str(numbered.group(1) or "").strip()
    slug = re.sub(r"[^a-z0-9]+", "", candidate.lower())
    return _PROVIDER_ALIASES.get(slug, "")


def _manifest_model_row(raw_path: str) -> Dict[str, Any]:
    original = str(raw_path or "").strip().strip("\"'")
    path = _resolve_path(Path(original))
    present = path.exists()
    is_file = path.is_file()
    fmt = path.suffix.strip().lower() if path.suffix else ""
    task = _infer_task(str(path))
    size_bytes = 0
    modified_epoch = 0.0
    if present:
        try:
            stat = path.stat()
            if is_file:
                size_bytes = int(stat.st_size)
            elif path.is_dir():
                size_bytes = int(sum(item.stat().st_size for item in path.glob("*") if item.is_file()))
            modified_epoch = float(stat.st_mtime)
        except Exception:
            size_bytes = 0
            modified_epoch = 0.0

    display_name = path.stem if (fmt and (present or original.lower().endswith(fmt))) else path.name
    normalized_format = fmt or "directory"
    return {
        "key": f"{task}:{str(path).lower()}",
        "task": task,
        "name": display_name or path.name or "model",
        "path": str(path),
        "source": "manifest",
        "format": normalized_format,
        "size_bytes": max(0, size_bytes),
        "modified_epoch": modified_epoch,
        "declared": True,
        "present": bool(present),
        "missing": not bool(present),
        "detected": False,
        "required_by_manifest": True,
    }


def _infer_task(path_text: str) -> str:
    text = str(path_text or "").strip().lower()
    if text.endswith(".ppn") or "wakeword" in text:
        return "wakeword"
    if "yolo" in text or "vision" in text or "segment_anything" in text or "sam" in text:
        return "vision"
    if "custom_intent" in text or "custom_intents" in text or "bart-large-mnli" in text or "mnli" in text:
        return "intent"
    if "/stt/" in text or "\\stt\\" in text or "whisper" in text or "speech-to-text" in text:
        return "stt"
    if "/tts/" in text or "\\tts\\" in text or "orpheus" in text or "kokoro" in text or "voice" in text:
        return "tts"
    if "embed" in text or "mpnet" in text or "multi-qa" in text:
        return "embedding"
    return "reasoning"
