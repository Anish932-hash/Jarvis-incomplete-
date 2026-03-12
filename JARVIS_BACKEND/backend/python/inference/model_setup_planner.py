from __future__ import annotations

import importlib.metadata
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.python.core.provider_credentials import ProviderCredentialManager


_KNOWN_MODEL_SPECS: List[Dict[str, Any]] = [
    {
        "id": "embedding-all-mpnet-base-v2",
        "tokens": ("all-mpnet-base-v2",),
        "tasks": ("embedding", "intent"),
        "family": "sentence-transformer",
        "strategy": "huggingface_snapshot",
        "backend": "sentence_transformers",
        "source_kind": "huggingface",
        "source_ref": "sentence-transformers/all-mpnet-base-v2",
        "notes": (
            "Downloads the full sentence-transformer directory into the manifest target path.",
        ),
    },
    {
        "id": "embedding-multi-qa-mpnet-base-dot-v1",
        "tokens": ("multi-qa-mpnet-base-dot-v1",),
        "tasks": ("embedding",),
        "family": "sentence-transformer",
        "strategy": "huggingface_snapshot",
        "backend": "sentence_transformers",
        "source_kind": "huggingface",
        "source_ref": "sentence-transformers/multi-qa-mpnet-base-dot-v1",
        "notes": (
            "Downloads the official QA embedding model into the manifest target path.",
        ),
    },
    {
        "id": "intent-bart-large-mnli",
        "tokens": ("bart-large-mnli",),
        "tasks": ("intent",),
        "family": "zero-shot-classification",
        "strategy": "huggingface_snapshot",
        "backend": "transformers",
        "source_kind": "huggingface",
        "source_ref": "facebook/bart-large-mnli",
        "notes": (
            "The intent classifier expects the full Transformers checkpoint directory.",
        ),
    },
    {
        "id": "stt-whisper-large-v3",
        "tokens": ("whisper-large-v3",),
        "tasks": ("stt",),
        "family": "whisper",
        "strategy": "huggingface_snapshot",
        "backend": "transformers",
        "source_kind": "huggingface",
        "source_ref": "openai/whisper-large-v3",
        "notes": (
            "Fetches the official Whisper large v3 checkpoint directory.",
        ),
    },
    {
        "id": "stt-whisper-medium",
        "tokens": ("whisper-medium",),
        "tasks": ("stt",),
        "family": "whisper",
        "strategy": "huggingface_snapshot",
        "backend": "transformers",
        "source_kind": "huggingface",
        "source_ref": "openai/whisper-medium",
        "notes": (
            "Fetches the official Whisper medium checkpoint directory.",
        ),
    },
    {
        "id": "tts-kokoro-82m",
        "tokens": ("kokoro-82m",),
        "tasks": ("tts",),
        "family": "kokoro",
        "strategy": "huggingface_snapshot",
        "backend": "transformers",
        "source_kind": "huggingface",
        "source_ref": "hexgrad/Kokoro-82M",
        "notes": (
            "Downloads the official Kokoro checkpoint directory.",
        ),
    },
    {
        "id": "reasoning-qwen3.5-9b",
        "tokens": ("qwen3.5-9b",),
        "tasks": ("reasoning",),
        "family": "qwen",
        "strategy": "huggingface_snapshot",
        "backend": "transformers",
        "source_kind": "huggingface",
        "source_ref": "Qwen/Qwen3.5-9B",
        "notes": (
            "Downloads the official Qwen3.5-9B directory into the expected all-rounder path.",
        ),
    },
    {
        "id": "reasoning-qwen3-14b",
        "tokens": ("qwen3-14b",),
        "tasks": ("reasoning",),
        "family": "qwen",
        "strategy": "huggingface_snapshot",
        "backend": "transformers",
        "source_kind": "huggingface",
        "source_ref": "Qwen/Qwen3-14B",
        "notes": (
            "Downloads the official Qwen3-14B directory into the expected all-rounder path.",
        ),
    },
    {
        "id": "reasoning-qwen2.5-14b-gguf",
        "tokens": ("qwen2.5-14b-instruct-q8_0.gguf",),
        "tasks": ("reasoning",),
        "family": "qwen",
        "strategy": "manual_quantization",
        "backend": "gguf",
        "source_kind": "huggingface",
        "source_ref": "Qwen/Qwen2.5-14B-Instruct",
        "notes": (
            "The manifest expects a GGUF quantization, but the official upstream publishes Transformer weights.",
            "Download a compatible GGUF build manually or convert the official checkpoint locally.",
        ),
        "blockers": (
            "Requires a GGUF quantization source or local conversion step.",
        ),
    },
    {
        "id": "reasoning-meta-llama-3.1-8b-gguf",
        "tokens": ("meta-llama-3.1-8b-instruct-q8_0.gguf",),
        "tasks": ("reasoning",),
        "family": "llama",
        "strategy": "manual_quantization",
        "backend": "gguf",
        "source_kind": "huggingface",
        "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
        "notes": (
            "The manifest expects a GGUF quantization, while the official upstream distributes Transformer weights.",
            "Meta Llama access may also require an approved gated license on Hugging Face.",
        ),
        "blockers": (
            "Requires a GGUF quantization source or local conversion step.",
            "Official upstream may require gated access approval.",
        ),
    },
    {
        "id": "reasoning-deepseek-r1-distill-qwen-14b-gguf",
        "tokens": ("deepseek-r1-distill-qwen-14b-q5_k_m.gguf",),
        "tasks": ("reasoning",),
        "family": "deepseek",
        "strategy": "manual_quantization",
        "backend": "gguf",
        "source_kind": "huggingface",
        "source_ref": "deepseek-ai/DeepSeek-R1-Distill-Qwen-14B",
        "notes": (
            "The manifest expects a GGUF quantization, while the official upstream distributes Transformer weights.",
            "Download a compatible GGUF build manually or convert the official checkpoint locally.",
        ),
        "blockers": (
            "Requires a GGUF quantization source or local conversion step.",
        ),
    },
    {
        "id": "tts-orpheus-3b-gguf",
        "tokens": ("orpheus-3b-tts.f16.gguf",),
        "tasks": ("tts",),
        "family": "orpheus",
        "strategy": "manual_quantization",
        "backend": "gguf",
        "source_kind": "huggingface",
        "source_ref": "canopylabs/orpheus-3b-0.1-ft",
        "notes": (
            "The manifest expects a GGUF artifact, while the upstream checkpoint is distributed in its native model format.",
            "Use a compatible GGUF export or convert the upstream checkpoint locally.",
        ),
        "blockers": (
            "Requires a GGUF quantization source or local conversion step.",
        ),
    },
    {
        "id": "vision-sam-vit-h",
        "tokens": ("sam_vit_h_4b8939.pth",),
        "tasks": ("vision",),
        "family": "segment-anything",
        "strategy": "direct_url",
        "backend": "pytorch_checkpoint",
        "source_kind": "direct_url",
        "source_ref": "https://dl.fbaipublicfiles.com/segment_anything/sam_vit_h_4b8939.pth",
        "notes": (
            "Downloads the official Segment Anything ViT-H checkpoint file.",
        ),
    },
    {
        "id": "vision-yolov10x",
        "tokens": ("yolov10x.pt",),
        "tasks": ("vision",),
        "family": "yolov10",
        "strategy": "direct_url",
        "backend": "pytorch_checkpoint",
        "source_kind": "direct_url",
        "source_ref": "https://github.com/THU-MIG/yolov10/releases/download/v1.1/yolov10x.pt",
        "notes": (
            "Downloads the official YOLOv10x release weight file.",
        ),
    },
    {
        "id": "vision-ggml-model-q4-k",
        "tokens": ("ggml-model-q4_k.gguf",),
        "tasks": ("vision",),
        "family": "gguf-vision",
        "strategy": "manual",
        "backend": "gguf",
        "source_kind": "unknown",
        "source_ref": "",
        "notes": (
            "The manifest names a GGUF vision artifact but does not identify the upstream repository.",
            "Keep the target filename and place the resolved artifact exactly at the manifest path.",
        ),
        "blockers": (
            "Upstream source is not identified in the manifest.",
        ),
    },
]


def build_model_setup_plan(
    *,
    manifest_payload: Dict[str, Any],
    provider_snapshot: Optional[Dict[str, Any]] = None,
    task: str = "",
    limit: int = 200,
    include_present: bool = False,
) -> Dict[str, Any]:
    manifest = manifest_payload if isinstance(manifest_payload, dict) else {}
    clean_task = str(task or "").strip().lower()
    bounded_limit = max(1, min(int(limit), 2000))
    model_rows = [
        dict(row)
        for row in manifest.get("models", [])
        if isinstance(row, dict)
    ]
    if clean_task:
        model_rows = [row for row in model_rows if str(row.get("task", "")).strip().lower() == clean_task]
    if not include_present:
        model_rows = [row for row in model_rows if not bool(row.get("present", False))]

    tools = _tool_status_snapshot()
    items: List[Dict[str, Any]] = []
    strategy_counts: Dict[str, int] = {}
    task_counts: Dict[str, int] = {}
    auto_installable_count = 0
    manual_count = 0
    warnings: List[str] = []

    for row in model_rows[:bounded_limit]:
        item = _build_plan_item(row=row, tools=tools)
        strategy_name = str(item.get("strategy", "unknown") or "unknown")
        task_name = str(item.get("task", "unknown") or "unknown")
        strategy_counts[strategy_name] = int(strategy_counts.get(strategy_name, 0)) + 1
        task_counts[task_name] = int(task_counts.get(task_name, 0)) + 1
        if bool(item.get("automation_ready", False)):
            auto_installable_count += 1
        else:
            manual_count += 1
        warnings.extend(
            str(entry).strip()
            for entry in item.get("warnings", [])
            if str(entry).strip()
        )
        items.append(item)

    provider_rows = _provider_setup_rows(
        manifest_payload=manifest,
        provider_snapshot=provider_snapshot if isinstance(provider_snapshot, dict) else {},
        items=items,
    )

    warning_rows = _dedupe_strings(warnings)
    provider_ready_count = sum(1 for row in provider_rows if bool(row.get("ready", False)))
    provider_required_count = len(provider_rows)
    missing_count = sum(1 for row in manifest.get("models", []) if isinstance(row, dict) and bool(row.get("missing", False)))
    present_count = sum(1 for row in manifest.get("models", []) if isinstance(row, dict) and bool(row.get("present", False)))

    return {
        "status": "success",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task": clean_task,
        "limit": bounded_limit,
        "include_present": bool(include_present),
        "manifest": {
            "status": str(manifest.get("status", "missing") or "missing"),
            "path": str(manifest.get("path", "") or ""),
            "model_count": int(manifest.get("model_count", 0) or 0),
            "provider_count": int(manifest.get("provider_count", 0) or 0),
            "providers": [str(item).strip().lower() for item in manifest.get("providers", []) if str(item).strip()],
        },
        "summary": {
            "declared_count": int(manifest.get("model_count", 0) or 0),
            "present_count": present_count,
            "missing_count": missing_count,
            "planned_count": len(items),
            "auto_installable_count": auto_installable_count,
            "manual_count": manual_count,
            "provider_required_count": provider_required_count,
            "provider_ready_count": provider_ready_count,
            "strategy_counts": strategy_counts,
            "task_counts": task_counts,
        },
        "tools": tools,
        "providers": provider_rows,
        "items": items,
        "warnings": warning_rows,
    }


def _build_plan_item(*, row: Dict[str, Any], tools: Dict[str, Any]) -> Dict[str, Any]:
    target_path = Path(str(row.get("path", "") or ""))
    spec = _match_spec(row)
    strategy = str(spec.get("strategy", "manual") or "manual")
    source_kind = str(spec.get("source_kind", "unknown") or "unknown")
    source_ref = str(spec.get("source_ref", "") or "")
    source_url = _source_url(spec)
    blockers = [
        str(item).strip()
        for item in spec.get("blockers", [])
        if str(item).strip()
    ]
    warnings: List[str] = []

    if strategy == "huggingface_snapshot" and not bool(tools.get("huggingface_hub", {}).get("available", False)):
        blockers.append("Python package huggingface_hub is not importable in the current runtime.")
    if strategy == "direct_url" and not bool(tools.get("powershell", {}).get("available", False)):
        blockers.append("PowerShell download tooling is not available in the current runtime.")
    if strategy in {"manual", "manual_quantization"} and source_ref:
        warnings.append("Source guidance is provided, but the final artifact still needs manual selection or conversion.")

    commands = _build_commands(
        strategy=strategy,
        source_ref=source_ref,
        target_path=target_path,
    )
    automation_ready = strategy in {"huggingface_snapshot", "direct_url"} and not blockers
    return {
        "key": str(row.get("key", "") or ""),
        "task": str(row.get("task", "") or "unknown"),
        "name": str(row.get("name", "") or target_path.name or "model"),
        "path": str(target_path),
        "present": bool(row.get("present", False)),
        "missing": bool(row.get("missing", False)),
        "format": str(row.get("format", "") or "directory"),
        "family": str(spec.get("family", "custom") or "custom"),
        "backend": str(spec.get("backend", "custom") or "custom"),
        "matched_rule": str(spec.get("id", "custom") or "custom"),
        "strategy": strategy,
        "automation_ready": automation_ready,
        "install_ready": automation_ready,
        "source_kind": source_kind,
        "source_ref": source_ref,
        "source_url": source_url,
        "target_kind": "directory" if (not target_path.suffix or str(row.get("format", "") or "").lower() == "directory") else "file",
        "commands": commands,
        "notes": [
            str(item).strip()
            for item in spec.get("notes", [])
            if str(item).strip()
        ],
        "blockers": _dedupe_strings(blockers),
        "warnings": _dedupe_strings(warnings),
    }


def _provider_setup_rows(
    *,
    manifest_payload: Dict[str, Any],
    provider_snapshot: Dict[str, Any],
    items: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    required_providers = [
        str(item).strip().lower()
        for item in manifest_payload.get("providers", [])
        if str(item).strip()
    ]
    supplemental_providers = set()
    hf_item_count = 0
    hf_task_names: List[str] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("source_kind", "") or "").strip().lower() != "huggingface":
            continue
        supplemental_providers.add("huggingface")
        hf_item_count += 1
        task_name = str(item.get("task", "") or "").strip().lower()
        if task_name:
            hf_task_names.append(task_name)
    snapshot_rows = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers", {}), dict) else {}
    catalog = ProviderCredentialManager.provider_catalog()
    rows: List[Dict[str, Any]] = []
    provider_order = list(dict.fromkeys(required_providers + sorted(supplemental_providers)))
    required_set = set(required_providers)
    for provider in provider_order:
        definition = catalog.get(provider, {})
        snapshot = snapshot_rows.get(provider, {}) if isinstance(snapshot_rows.get(provider, {}), dict) else {}
        fields = [
            {
                "name": "api_key",
                "label": str(definition.get("credential_label", "API Key") or "API Key"),
                "env_var": str(definition.get("env", "") or ""),
                "secret": True,
                "required": provider in required_set,
            }
        ]
        for requirement_env in definition.get("required_env", []):
            fields.append(
                {
                    "name": str(requirement_env),
                    "label": str(requirement_env).replace("_", " ").title(),
                    "env_var": str(requirement_env),
                    "secret": False,
                    "required": True,
                }
            )
        rows.append(
            {
                "provider": provider,
                "ready": bool(snapshot.get("ready", False)),
                "present": bool(snapshot.get("present", False)),
                "source": str(snapshot.get("source", "none") or "none"),
                "required_by_manifest": provider in required_set,
                "optional": provider not in required_set,
                "required_env": [str(item) for item in definition.get("required_env", []) if str(item).strip()],
                "missing_requirements": [
                    str(item).strip()
                    for item in snapshot.get("missing_requirements", [])
                    if str(item).strip()
                ] if isinstance(snapshot.get("missing_requirements", []), list) else [],
                "fields": fields,
                "credential_label": str(definition.get("credential_label", "API Key") or "API Key"),
                "usage_hint": (
                    f"Used for gated/private Hugging Face model access across {hf_item_count} setup item(s)."
                    if provider == "huggingface" and hf_item_count > 0
                    else ""
                ),
                "task_scope": sorted(set(hf_task_names)) if provider == "huggingface" else [],
            }
        )
    return rows


def _match_spec(row: Dict[str, Any]) -> Dict[str, Any]:
    haystack = " ".join(
        [
            str(row.get("task", "") or ""),
            str(row.get("name", "") or ""),
            str(row.get("path", "") or ""),
            str(row.get("format", "") or ""),
        ]
    ).lower()
    task_name = str(row.get("task", "") or "").strip().lower()
    for spec in _KNOWN_MODEL_SPECS:
        tasks = tuple(str(item).strip().lower() for item in spec.get("tasks", ()) if str(item).strip())
        if tasks and task_name and task_name not in tasks:
            continue
        tokens = tuple(str(item).strip().lower() for item in spec.get("tokens", ()) if str(item).strip())
        if tokens and all(token in haystack for token in tokens):
            return spec
    return {
        "id": "custom-manual",
        "family": "custom",
        "strategy": "manual",
        "backend": "custom",
        "source_kind": "unknown",
        "source_ref": "",
        "notes": (
            "No verified upstream mapping is defined for this manifest entry yet.",
        ),
        "blockers": (
            "Requires a manual source selection step.",
        ),
    }


def _source_url(spec: Dict[str, Any]) -> str:
    source_kind = str(spec.get("source_kind", "unknown") or "unknown")
    source_ref = str(spec.get("source_ref", "") or "")
    if source_kind == "huggingface" and source_ref:
        return f"https://huggingface.co/{source_ref}"
    if source_kind == "direct_url":
        return source_ref
    return ""


def _build_commands(*, strategy: str, source_ref: str, target_path: Path) -> List[str]:
    if strategy == "huggingface_snapshot" and source_ref:
        return [
            "\n".join(
                [
                    "@'",
                    "import os",
                    "from huggingface_hub import snapshot_download",
                    "from pathlib import Path",
                    f"target = Path({json.dumps(str(target_path))})",
                    "token = os.getenv('HUGGINGFACE_HUB_TOKEN') or os.getenv('HF_TOKEN') or None",
                    "target.parent.mkdir(parents=True, exist_ok=True)",
                    "snapshot_download(",
                    f"    repo_id={json.dumps(source_ref)},",
                    "    local_dir=str(target),",
                    "    local_dir_use_symlinks=False,",
                    "    resume_download=True,",
                    "    token=token,",
                    ")",
                    "'@ | python -",
                ]
            )
        ]
    if strategy == "direct_url" and source_ref:
        return [
            "\n".join(
                [
                    f"New-Item -ItemType Directory -Force -Path {_ps_quote(str(target_path.parent))} | Out-Null",
                    f"Invoke-WebRequest -Uri {_ps_quote(source_ref)} -OutFile {_ps_quote(str(target_path))}",
                ]
            )
        ]
    return []


def _tool_status_snapshot() -> Dict[str, Any]:
    python_path = str(Path(sys.executable).resolve()) if sys.executable else ""
    powershell_path = shutil.which("powershell") or shutil.which("pwsh") or ""
    git_path = shutil.which("git") or ""
    git_lfs_path = shutil.which("git-lfs") or ""
    huggingface_version = ""
    huggingface_available = False
    try:
        huggingface_version = importlib.metadata.version("huggingface_hub")
        huggingface_available = True
    except Exception:
        huggingface_version = ""
        huggingface_available = False
    return {
        "python": {
            "available": bool(python_path),
            "path": python_path,
            "version": sys.version.split()[0] if sys.version else "",
        },
        "powershell": {
            "available": bool(powershell_path or os.name == "nt"),
            "path": powershell_path,
            "version": "",
        },
        "git": {
            "available": bool(git_path),
            "path": git_path,
            "version": "",
        },
        "git_lfs": {
            "available": bool(git_lfs_path),
            "path": git_lfs_path,
            "version": "",
        },
        "huggingface_hub": {
            "available": huggingface_available,
            "path": python_path,
            "version": huggingface_version,
        },
    }


def _ps_quote(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


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
