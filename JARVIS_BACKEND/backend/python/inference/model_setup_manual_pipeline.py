from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


_WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
_BACKEND_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_TOOLCHAIN_ROOT = _WORKSPACE_ROOT / "data" / "toolchains"
_DEFAULT_LLAMA_CPP_ROOT = _DEFAULT_TOOLCHAIN_ROOT / "llama.cpp"
_DEFAULT_PIPELINE_ROOT = _WORKSPACE_ROOT / "data" / "model_setup_manual_pipeline"
_MANUAL_STRATEGIES = {"manual", "manual_quantization"}
_HF_CONVERSION_PACKAGES = (
    ("huggingface_hub", "huggingface_hub"),
    ("transformers", "transformers"),
    ("safetensors", "safetensors"),
    ("sentencepiece", "sentencepiece"),
)
_RUNTIME_PACKAGES = (("llama_cpp", "llama-cpp-python"),)
_QUANTIZATION_PATTERN = re.compile(r"(?P<quant>(?:q\d+_[a-z0-9_]+)|f16|bf16)(?=\.gguf$)", re.IGNORECASE)


def build_model_setup_manual_pipeline(
    *,
    plan_payload: Dict[str, Any],
    item_keys: Optional[List[str]] = None,
    workspace_root: Optional[Path | str] = None,
    toolchain_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    plan = plan_payload if isinstance(plan_payload, dict) else {}
    if str(plan.get("status", "error") or "error").strip().lower() != "success":
        return {"status": "error", "message": "invalid setup plan payload"}

    selected_keys = {
        str(item).strip().lower()
        for item in (item_keys or [])
        if str(item).strip()
    }
    root = Path(workspace_root).resolve() if workspace_root else _WORKSPACE_ROOT
    pipeline_root = root / "data" / "model_setup_manual_pipeline"
    toolchain = toolchain_snapshot if isinstance(toolchain_snapshot, dict) else _toolchain_snapshot(workspace_root=root)
    provider_map = _provider_map(plan.get("providers", []))
    manual_items = [
        dict(item)
        for item in plan.get("items", [])
        if isinstance(item, dict) and str(item.get("strategy", "") or "").strip().lower() in _MANUAL_STRATEGIES
    ]
    if selected_keys:
        manual_items = [
            item for item in manual_items if str(item.get("key", "") or "").strip().lower() in selected_keys
        ]

    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    upgrade_actions = _build_global_upgrade_actions(
        toolchain=toolchain,
        provider_map=provider_map,
        manual_items=manual_items,
    )

    for item in manual_items:
        row = _build_manual_item_pipeline(
            item=item,
            toolchain=toolchain,
            provider_map=provider_map,
            pipeline_root=pipeline_root,
        )
        warnings.extend(str(entry).strip() for entry in row.get("warnings", []) if str(entry).strip())
        rows.append(row)

    ready_count = sum(1 for item in rows if str(item.get("status", "") or "").strip().lower() == "ready")
    warning_count = sum(1 for item in rows if str(item.get("status", "") or "").strip().lower() == "warning")
    blocked_count = sum(1 for item in rows if str(item.get("status", "") or "").strip().lower() == "blocked")
    convertible_count = sum(1 for item in rows if bool(item.get("convertible", False)))
    auth_required_count = sum(1 for item in rows if bool(item.get("auth_required", False)))
    auth_ready_count = sum(
        1 for item in rows if bool(item.get("auth_required", False)) and bool(item.get("auth_configured", False))
    )
    unresolved_source_count = sum(1 for item in rows if not str(item.get("source_ref", "") or "").strip())
    quantization_count = sum(
        1 for item in rows if str(item.get("strategy", "") or "").strip().lower() == "manual_quantization"
    )

    return {
        "status": "success",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_item_keys": sorted(selected_keys),
        "summary": {
            "manual_count": len(rows),
            "convertible_count": convertible_count,
            "quantization_count": quantization_count,
            "blocked_count": blocked_count,
            "warning_count": warning_count,
            "ready_count": ready_count,
            "auth_required_count": auth_required_count,
            "auth_ready_count": auth_ready_count,
            "unresolved_source_count": unresolved_source_count,
            "upgrade_action_count": len(upgrade_actions),
        },
        "toolchain": toolchain,
        "upgrade_actions": upgrade_actions,
        "items": rows,
        "warnings": _dedupe_strings(warnings),
        "pipeline_root": str(pipeline_root),
    }


def _build_manual_item_pipeline(
    *,
    item: Dict[str, Any],
    toolchain: Dict[str, Any],
    provider_map: Dict[str, Dict[str, Any]],
    pipeline_root: Path,
) -> Dict[str, Any]:
    item_key = str(item.get("key", "") or "").strip() or _slug(str(item.get("name", "") or "manual-model"))
    target_path = Path(str(item.get("path", "") or "")).expanduser()
    strategy = str(item.get("strategy", "manual") or "manual").strip().lower()
    source_kind = str(item.get("source_kind", "unknown") or "unknown").strip().lower()
    source_ref = str(item.get("source_ref", "") or "").strip()
    source_url = str(item.get("source_url", "") or "").strip()
    family = str(item.get("family", "custom") or "custom").strip().lower()
    name = str(item.get("name", "") or target_path.name or item_key)
    pipeline_dir = pipeline_root / _slug(item_key)
    source_stage_dir = pipeline_dir / "source"
    converted_dir = pipeline_dir / "converted"
    artifact_dir = pipeline_dir / "artifacts"
    target_quantization = _extract_target_quantization(target_path.name)
    intermediate_filename = _fp16_intermediate_name(target_path.name)
    intermediate_path = converted_dir / intermediate_filename
    staged_output_path = artifact_dir / target_path.name

    huggingface_provider = provider_map.get("huggingface", {})
    auth_required = bool(
        source_kind == "huggingface"
        and (_source_requires_auth(source_ref=source_ref, family=family) or bool(huggingface_provider.get("required_by_manifest")))
    )
    auth_configured = bool(huggingface_provider.get("ready", False))
    packages = toolchain.get("packages", {}) if isinstance(toolchain.get("packages", {}), dict) else {}
    commands = toolchain.get("commands", {}) if isinstance(toolchain.get("commands", {}), dict) else {}
    llama_cpp = toolchain.get("llama_cpp", {}) if isinstance(toolchain.get("llama_cpp", {}), dict) else {}
    python_available = bool(as_bool(commands.get("python", {}).get("available", False)))
    git_available = bool(as_bool(commands.get("git", {}).get("available", False)))
    cmake_available = bool(as_bool(commands.get("cmake", {}).get("available", False)))
    can_bootstrap_llama_cpp = python_available and git_available and cmake_available
    converter_present = bool(as_bool(llama_cpp.get("convert_script_available", False)))
    quantize_present = bool(as_bool(llama_cpp.get("quantize_binary_available", False)))
    llama_cpp_ready = converter_present and (quantize_present or target_quantization in {"", "f16", "bf16"})

    required_conversion_packages = _required_conversion_packages(strategy=strategy, source_kind=source_kind)
    missing_conversion_packages = [
        package_name
        for package_name in required_conversion_packages
        if not bool(as_bool(packages.get(package_name, {}).get("available", False)))
    ]
    missing_runtime_packages = [
        package_name
        for package_name, _dist_name in _RUNTIME_PACKAGES
        if not bool(as_bool(packages.get(package_name, {}).get("available", False)))
    ]

    blockers: List[str] = []
    warnings: List[str] = []
    notes = [str(entry).strip() for entry in item.get("notes", []) if str(entry).strip()]

    if strategy == "manual" and not source_ref:
        blockers.append("Upstream source is not identified for this artifact yet.")
    if strategy == "manual_quantization" and not source_ref:
        blockers.append("A source repository is required before the GGUF conversion pipeline can run.")
    if strategy == "manual_quantization" and not python_available:
        blockers.append("Python is required to run the manual GGUF conversion pipeline.")
    if strategy == "manual_quantization" and not (converter_present or can_bootstrap_llama_cpp):
        blockers.append("llama.cpp conversion tooling is unavailable and cannot be bootstrapped automatically.")
    if strategy == "manual_quantization" and target_quantization not in {"", "f16", "bf16"} and not (
        quantize_present or can_bootstrap_llama_cpp
    ):
        blockers.append("The target quantization requires llama-quantize, but no build path is available.")
    if auth_required and not auth_configured:
        blockers.append("A verified Hugging Face access token is required before this source can be downloaded.")

    if missing_conversion_packages:
        warnings.append("Python conversion packages are missing: " + ", ".join(sorted(missing_conversion_packages)))
    if strategy == "manual_quantization" and source_kind == "huggingface" and not auth_required and not auth_configured:
        warnings.append("A Hugging Face token is not configured. Public repos can still work, but gated repos will fail.")
    if strategy == "manual_quantization" and not converter_present and can_bootstrap_llama_cpp:
        warnings.append("llama.cpp is not prepared yet; bootstrap commands are included below.")
    if missing_runtime_packages and str(item.get("backend", "") or "").strip().lower() == "gguf":
        warnings.append("Runtime GGUF support is not fully ready until llama-cpp-python is installed.")
    if strategy == "manual_quantization" and not target_quantization:
        warnings.append("Target quantization could not be inferred from the manifest filename; promotion will stay manual.")

    package_install_command = _build_package_install_command(
        missing_packages=missing_conversion_packages + missing_runtime_packages,
    )
    bootstrap_commands = _bootstrap_commands(llama_cpp)
    snapshot_command = (
        _build_huggingface_snapshot_command(source_ref=source_ref, target_dir=source_stage_dir)
        if source_kind == "huggingface" and source_ref
        else ""
    )
    convert_command = (
        _build_convert_command(
            llama_cpp=llama_cpp,
            source_dir=source_stage_dir,
            output_path=intermediate_path,
        )
        if strategy == "manual_quantization" and source_ref
        else ""
    )
    quantize_command = (
        _build_quantize_command(
            llama_cpp=llama_cpp,
            source_path=intermediate_path,
            output_path=staged_output_path,
            quantization=target_quantization,
        )
        if strategy == "manual_quantization" and target_quantization not in {"", "f16", "bf16"}
        else ""
    )
    promote_command = _build_promote_command(
        source_path=staged_output_path if quantize_command else intermediate_path,
        target_path=target_path,
    )
    if strategy == "manual_quantization" and target_quantization in {"f16", "bf16"}:
        staged_output_path = intermediate_path
        promote_command = _build_promote_command(source_path=intermediate_path, target_path=target_path)

    steps = _build_steps(
        strategy=strategy,
        item_name=name,
        source_ref=source_ref,
        source_url=source_url,
        target_quantization=target_quantization,
        target_path=target_path,
        source_stage_dir=source_stage_dir,
        intermediate_path=intermediate_path,
        staged_output_path=staged_output_path,
        package_install_command=package_install_command,
        bootstrap_commands=bootstrap_commands,
        snapshot_command=snapshot_command,
        convert_command=convert_command,
        quantize_command=quantize_command,
        promote_command=promote_command,
        auth_required=auth_required,
        auth_configured=auth_configured,
        missing_conversion_packages=missing_conversion_packages,
        has_bootstrap_path=can_bootstrap_llama_cpp,
        converter_present=converter_present,
        quantize_present=quantize_present,
    )
    command_list = [
        command
        for step in steps
        for command in step.get("commands", [])
        if isinstance(command, str) and command.strip()
    ]
    convertible = strategy == "manual_quantization" and source_kind == "huggingface" and bool(source_ref)
    if strategy == "manual":
        convertible = False

    status = "blocked" if blockers else ("warning" if warnings else "ready")
    recommended_next_action = _recommended_next_action(
        blockers=blockers,
        warnings=warnings,
        strategy=strategy,
        auth_required=auth_required,
        auth_configured=auth_configured,
        missing_conversion_packages=missing_conversion_packages,
        converter_present=converter_present,
        quantize_present=quantize_present,
        has_bootstrap_path=can_bootstrap_llama_cpp,
        source_ref=source_ref,
    )

    return {
        "key": item_key,
        "name": name,
        "task": str(item.get("task", "") or "unknown"),
        "strategy": strategy,
        "status": status,
        "convertible": convertible,
        "family": family,
        "backend": str(item.get("backend", "") or ""),
        "path": str(target_path),
        "source_kind": source_kind,
        "source_ref": source_ref,
        "source_url": source_url,
        "auth_required": auth_required,
        "auth_configured": auth_configured,
        "pipeline_kind": "hf_to_gguf" if convertible else ("unresolved_source" if not source_ref else "manual_resolution"),
        "target_quantization": target_quantization,
        "pipeline_root": str(pipeline_dir),
        "source_stage_dir": str(source_stage_dir),
        "intermediate_path": str(intermediate_path),
        "staged_output_path": str(staged_output_path),
        "toolchain_ready": llama_cpp_ready,
        "toolchain_bootstrap_ready": can_bootstrap_llama_cpp,
        "missing_conversion_packages": missing_conversion_packages,
        "missing_runtime_packages": missing_runtime_packages,
        "recommended_next_action": recommended_next_action,
        "notes": _dedupe_strings(notes),
        "blockers": _dedupe_strings(blockers),
        "warnings": _dedupe_strings(warnings),
        "steps": steps,
        "commands": command_list,
    }


def _build_steps(
    *,
    strategy: str,
    item_name: str,
    source_ref: str,
    source_url: str,
    target_quantization: str,
    target_path: Path,
    source_stage_dir: Path,
    intermediate_path: Path,
    staged_output_path: Path,
    package_install_command: str,
    bootstrap_commands: List[str],
    snapshot_command: str,
    convert_command: str,
    quantize_command: str,
    promote_command: str,
    auth_required: bool,
    auth_configured: bool,
    missing_conversion_packages: List[str],
    has_bootstrap_path: bool,
    converter_present: bool,
    quantize_present: bool,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    source_status = "ready"
    source_blockers: List[str] = []
    if not source_ref:
        source_status = "blocked"
        source_blockers.append("No upstream source is mapped for this item yet.")
    if auth_required and not auth_configured:
        source_status = "blocked"
        source_blockers.append("Save and verify a Hugging Face access token before downloading this source.")
    rows.append(
        {
            "id": "resolve-source",
            "title": "Resolve Source Checkpoint",
            "status": source_status,
            "required": True,
            "description": (
                f"Stage the upstream checkpoint for {item_name} into the workspace pipeline directory."
                if source_ref
                else "Identify the upstream repository or artifact before any conversion can begin."
            ),
            "commands": [snapshot_command] if snapshot_command else [],
            "artifacts": {
                "source_ref": source_ref,
                "source_url": source_url,
                "source_stage_dir": str(source_stage_dir),
            },
            "blockers": source_blockers,
        }
    )

    dependency_status = "ready" if not missing_conversion_packages else "warning"
    rows.append(
        {
            "id": "python-dependencies",
            "title": "Install Conversion Dependencies",
            "status": dependency_status,
            "required": bool(strategy == "manual_quantization"),
            "description": "Ensure Hugging Face and tokenizer packages are available for local GGUF conversion.",
            "commands": [package_install_command] if package_install_command else [],
            "artifacts": {},
            "blockers": [],
        }
    )

    toolchain_status = "ready" if converter_present and (quantize_present or target_quantization in {"", "f16", "bf16"}) else (
        "warning" if has_bootstrap_path else "blocked"
    )
    toolchain_blockers: List[str] = []
    if toolchain_status == "blocked":
        toolchain_blockers.append("git and cmake are required to bootstrap llama.cpp locally.")
    rows.append(
        {
            "id": "bootstrap-llama-cpp",
            "title": "Bootstrap llama.cpp Toolchain",
            "status": toolchain_status,
            "required": bool(strategy == "manual_quantization"),
            "description": "Prepare the converter script and quantization binary used for GGUF export.",
            "commands": bootstrap_commands,
            "artifacts": {},
            "blockers": toolchain_blockers,
        }
    )

    convert_status = "ready" if strategy != "manual_quantization" else (
        "ready" if source_ref and (converter_present or has_bootstrap_path) else "blocked"
    )
    convert_blockers: List[str] = []
    if convert_status == "blocked":
        convert_blockers.append("No upstream checkpoint is available to convert." if not source_ref else "The llama.cpp converter is not available.")
    rows.append(
        {
            "id": "convert-f16-gguf",
            "title": "Convert To F16 GGUF",
            "status": convert_status,
            "required": bool(strategy == "manual_quantization"),
            "description": "Export the staged source checkpoint into a full precision GGUF artifact first.",
            "commands": [convert_command] if convert_command else [],
            "artifacts": {"intermediate_path": str(intermediate_path)},
            "blockers": convert_blockers,
        }
    )

    needs_quantize = strategy == "manual_quantization" and target_quantization not in {"", "f16", "bf16"}
    quantize_status = "ready"
    quantize_blockers: List[str] = []
    if needs_quantize:
        quantize_status = "ready" if (quantize_present or has_bootstrap_path) else "blocked"
        if quantize_status == "blocked":
            quantize_blockers.append("llama-quantize is required to produce the requested target artifact.")
    rows.append(
        {
            "id": "quantize-target",
            "title": "Quantize Target Artifact",
            "status": quantize_status if needs_quantize else "ready",
            "required": needs_quantize,
            "description": (
                f"Quantize the F16 GGUF into the requested {target_quantization.upper()} target."
                if needs_quantize and target_quantization
                else "No extra quantization step is required for this target filename."
            ),
            "commands": [quantize_command] if quantize_command else [],
            "artifacts": {"staged_output_path": str(staged_output_path)},
            "blockers": quantize_blockers,
        }
    )

    rows.append(
        {
            "id": "promote-artifact",
            "title": "Promote Final Artifact",
            "status": "ready" if promote_command else "blocked",
            "required": True,
            "description": "Copy the finished artifact into the exact manifest path so JARVIS can discover it.",
            "commands": [promote_command] if promote_command else [],
            "artifacts": {"target_path": str(target_path)},
            "blockers": [] if promote_command else ["The final promotion command could not be generated."],
        }
    )
    return rows


def _toolchain_snapshot(*, workspace_root: Path) -> Dict[str, Any]:
    python_path = str(Path(sys.executable).resolve()) if sys.executable else ""
    commands = {
        "python": {
            "available": bool(python_path),
            "path": python_path,
            "version": sys.version.split()[0] if sys.version else "",
        },
        "powershell": _command_status("powershell") if os.name != "nt" else {"available": True, "path": shutil.which("powershell") or "", "version": ""},
        "git": _command_status("git"),
        "cmake": _command_status("cmake"),
        "ninja": _command_status("ninja"),
        "git_lfs": _command_status("git-lfs"),
        "winget": _command_status("winget"),
    }
    packages: Dict[str, Any] = {}
    for module_name, distribution_name in _HF_CONVERSION_PACKAGES + _RUNTIME_PACKAGES:
        packages[module_name] = _package_status(module_name=module_name, distribution_name=distribution_name)
    llama_cpp = _llama_cpp_status(workspace_root=workspace_root, commands=commands)
    return {
        "workspace_root": str(workspace_root),
        "pipeline_root": str(_DEFAULT_PIPELINE_ROOT if workspace_root == _WORKSPACE_ROOT else workspace_root / "data" / "model_setup_manual_pipeline"),
        "commands": commands,
        "packages": packages,
        "llama_cpp": llama_cpp,
        "summary": {
            "conversion_packages_ready": all(
                bool(as_bool(packages.get(package_name, {}).get("available", False)))
                for package_name, _dist_name in _HF_CONVERSION_PACKAGES
            ),
            "runtime_packages_ready": all(
                bool(as_bool(packages.get(package_name, {}).get("available", False)))
                for package_name, _dist_name in _RUNTIME_PACKAGES
            ),
            "llama_cpp_ready": bool(as_bool(llama_cpp.get("ready", False))),
        },
    }


def _llama_cpp_status(*, workspace_root: Path, commands: Dict[str, Any]) -> Dict[str, Any]:
    root_candidates = [
        workspace_root / "data" / "toolchains" / "llama.cpp",
        workspace_root / "tools" / "llama.cpp",
        _BACKEND_ROOT / "tools" / "llama.cpp",
        _DEFAULT_LLAMA_CPP_ROOT,
    ]
    selected_root = next(
        (
            candidate
            for candidate in root_candidates
            if (candidate / "convert_hf_to_gguf.py").exists() or (candidate / "CMakeLists.txt").exists()
        ),
        root_candidates[0],
    )
    convert_script = selected_root / "convert_hf_to_gguf.py"
    quantize_binary = next(
        (
            path
            for path in (
                selected_root / "build" / "bin" / "llama-quantize.exe",
                selected_root / "build" / "bin" / "Release" / "llama-quantize.exe",
                selected_root / "build" / "bin" / "llama-quantize",
                selected_root / "build" / "bin" / "Release" / "llama-quantize",
                selected_root / "bin" / "llama-quantize.exe",
                selected_root / "bin" / "llama-quantize",
            )
            if path.exists()
        ),
        selected_root / "build" / "bin" / "llama-quantize.exe",
    )
    build_dir = selected_root / "build"
    repo_present = selected_root.exists() and (
        (selected_root / ".git").exists() or (selected_root / "CMakeLists.txt").exists() or convert_script.exists()
    )
    convert_script_available = convert_script.exists()
    quantize_available = quantize_binary.exists()
    ready = convert_script_available and quantize_available
    bootstrap_commands = [
        f"git clone https://github.com/ggml-org/llama.cpp.git {_ps_quote(str(selected_root))}",
        f"cmake -S {_ps_quote(str(selected_root))} -B {_ps_quote(str(build_dir))}",
        f"cmake --build {_ps_quote(str(build_dir))} --config Release --target llama-quantize",
    ]
    if repo_present:
        bootstrap_commands[0] = f"git -C {_ps_quote(str(selected_root))} pull --ff-only"
    return {
        "root": str(selected_root),
        "root_candidates": [str(candidate) for candidate in root_candidates],
        "repo_present": repo_present,
        "convert_script_path": str(convert_script),
        "convert_script_available": convert_script_available,
        "quantize_binary_path": str(quantize_binary),
        "quantize_binary_available": quantize_available,
        "build_dir": str(build_dir),
        "ready": ready,
        "bootstrap_commands": bootstrap_commands,
        "bootstrap_ready": bool(as_bool(commands.get("git", {}).get("available", False)))
        and bool(as_bool(commands.get("cmake", {}).get("available", False))),
    }


def _build_global_upgrade_actions(
    *,
    toolchain: Dict[str, Any],
    provider_map: Dict[str, Dict[str, Any]],
    manual_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commands = toolchain.get("commands", {}) if isinstance(toolchain.get("commands", {}), dict) else {}
    packages = toolchain.get("packages", {}) if isinstance(toolchain.get("packages", {}), dict) else {}
    llama_cpp = toolchain.get("llama_cpp", {}) if isinstance(toolchain.get("llama_cpp", {}), dict) else {}
    rows: List[Dict[str, Any]] = []
    missing_conversion_packages = [
        package_name
        for package_name, _dist_name in _HF_CONVERSION_PACKAGES
        if not bool(as_bool(packages.get(package_name, {}).get("available", False)))
    ]
    if missing_conversion_packages:
        rows.append(
            {
                "id": "install-conversion-packages",
                "title": "Install GGUF conversion packages",
                "status": "recommended",
                "description": "Add the Python packages needed for Hugging Face checkpoint download and tokenizer-safe GGUF export.",
                "commands": [_build_package_install_command(missing_packages=missing_conversion_packages)],
                "applies_to": [str(item.get("key", "") or "") for item in manual_items if isinstance(item, dict)],
            }
        )
    if not bool(as_bool(packages.get("llama_cpp", {}).get("available", False))):
        rows.append(
            {
                "id": "install-llama-cpp-python",
                "title": "Install local GGUF runtime",
                "status": "recommended",
                "description": "Reasoning and GGUF-backed neural TTS stay limited until llama-cpp-python is available locally.",
                "commands": [_build_package_install_command(missing_packages=["llama_cpp"])],
                "applies_to": [
                    str(item.get("key", "") or "")
                    for item in manual_items
                    if str(item.get("backend", "") or "").strip().lower() == "gguf"
                ],
            }
        )
    if not bool(as_bool(llama_cpp.get("ready", False))) and bool(as_bool(llama_cpp.get("bootstrap_ready", False))):
        rows.append(
            {
                "id": "bootstrap-llama-cpp",
                "title": "Bootstrap llama.cpp tools",
                "status": "recommended",
                "description": "Prepare the upstream converter script and llama-quantize binary used by manual GGUF artifacts.",
                "commands": [str(command).strip() for command in llama_cpp.get("bootstrap_commands", []) if str(command).strip()],
                "applies_to": [
                    str(item.get("key", "") or "")
                    for item in manual_items
                    if str(item.get("strategy", "") or "").strip().lower() == "manual_quantization"
                ],
            }
        )
    if any(
        _source_requires_auth(
            source_ref=str(item.get("source_ref", "") or ""),
            family=str(item.get("family", "") or ""),
        )
        for item in manual_items
        if isinstance(item, dict)
    ) and not bool(provider_map.get("huggingface", {}).get("ready", False)):
        rows.append(
            {
                "id": "verify-huggingface-token",
                "title": "Save and verify Hugging Face access",
                "status": "required",
                "description": "Some manual sources are gated. Save a Hugging Face token and run verification before conversion.",
                "commands": [],
                "applies_to": [
                    str(item.get("key", "") or "")
                    for item in manual_items
                    if _source_requires_auth(
                        source_ref=str(item.get("source_ref", "") or ""),
                        family=str(item.get("family", "") or ""),
                    )
                ],
            }
        )
    for binary_name, action_id, title, winget_id, description in (
        ("git", "install-git", "Install Git", "Git.Git", "Git is required to clone or update llama.cpp for the GGUF conversion pipeline."),
        ("cmake", "install-cmake", "Install CMake", "Kitware.CMake", "CMake is required to build llama-quantize when the binary is not already present."),
    ):
        if bool(as_bool(commands.get(binary_name, {}).get("available", False))):
            continue
        rows.append(
            {
                "id": action_id,
                "title": title,
                "status": "required",
                "description": description,
                "commands": (
                    [f"winget install --id {winget_id} -e --source winget"]
                    if bool(as_bool(commands.get("winget", {}).get("available", False)))
                    else []
                ),
                "applies_to": [
                    str(item.get("key", "") or "")
                    for item in manual_items
                    if str(item.get("strategy", "") or "").strip().lower() == "manual_quantization"
                ],
            }
        )
    return rows


def _provider_map(rows: Any) -> Dict[str, Dict[str, Any]]:
    provider_rows = rows if isinstance(rows, list) else []
    return {
        str(item.get("provider", "") or "").strip().lower(): dict(item)
        for item in provider_rows
        if isinstance(item, dict) and str(item.get("provider", "") or "").strip()
    }


def _required_conversion_packages(*, strategy: str, source_kind: str) -> List[str]:
    if strategy != "manual_quantization" or source_kind != "huggingface":
        return []
    return [package_name for package_name, _dist_name in _HF_CONVERSION_PACKAGES]


def _source_requires_auth(*, source_ref: str, family: str) -> bool:
    clean_ref = str(source_ref or "").strip().lower()
    clean_family = str(family or "").strip().lower()
    return clean_ref.startswith("meta-llama/") or clean_family == "llama"


def _build_package_install_command(*, missing_packages: Iterable[str]) -> str:
    package_map = {
        "huggingface_hub": "huggingface_hub",
        "transformers": "transformers",
        "safetensors": "safetensors",
        "sentencepiece": "sentencepiece",
        "llama_cpp": "llama-cpp-python",
    }
    resolved = [package_map.get(str(item).strip(), str(item).strip()) for item in missing_packages if str(item).strip()]
    unique = list(dict.fromkeys(resolved))
    if not unique:
        return ""
    return f"python -m pip install -U {' '.join(unique)}"


def _bootstrap_commands(llama_cpp: Dict[str, Any]) -> List[str]:
    return [str(command).strip() for command in llama_cpp.get("bootstrap_commands", []) if str(command).strip()]


def _build_huggingface_snapshot_command(*, source_ref: str, target_dir: Path) -> str:
    return "\n".join(
        [
            "@'",
            "import os",
            "from pathlib import Path",
            "from huggingface_hub import snapshot_download",
            f"target = Path({json.dumps(str(target_dir))})",
            "target.parent.mkdir(parents=True, exist_ok=True)",
            "token = os.getenv('HUGGINGFACE_HUB_TOKEN') or os.getenv('HF_TOKEN') or None",
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


def _build_convert_command(*, llama_cpp: Dict[str, Any], source_dir: Path, output_path: Path) -> str:
    convert_script_path = str(llama_cpp.get("convert_script_path", "") or "").strip()
    return "\n".join(
        [
            f"New-Item -ItemType Directory -Force -Path {_ps_quote(str(output_path.parent))} | Out-Null",
            "python "
            + _ps_quote(convert_script_path)
            + " "
            + _ps_quote(str(source_dir))
            + " --outtype f16 --outfile "
            + _ps_quote(str(output_path)),
        ]
    )


def _build_quantize_command(*, llama_cpp: Dict[str, Any], source_path: Path, output_path: Path, quantization: str) -> str:
    quantize_binary = str(llama_cpp.get("quantize_binary_path", "") or "").strip()
    if not quantization:
        return ""
    return "\n".join(
        [
            f"New-Item -ItemType Directory -Force -Path {_ps_quote(str(output_path.parent))} | Out-Null",
            _ps_quote(quantize_binary)
            + " "
            + _ps_quote(str(source_path))
            + " "
            + _ps_quote(str(output_path))
            + " "
            + quantization.upper(),
        ]
    )


def _build_promote_command(*, source_path: Path, target_path: Path) -> str:
    return "\n".join(
        [
            f"New-Item -ItemType Directory -Force -Path {_ps_quote(str(target_path.parent))} | Out-Null",
            f"Copy-Item -Path {_ps_quote(str(source_path))} -Destination {_ps_quote(str(target_path))} -Force",
        ]
    )


def _extract_target_quantization(filename: str) -> str:
    match = _QUANTIZATION_PATTERN.search(str(filename or "").strip().lower())
    return str(match.group("quant")).lower() if match else ""


def _fp16_intermediate_name(filename: str) -> str:
    clean_name = str(filename or "").strip()
    quantization = _extract_target_quantization(clean_name)
    if not clean_name:
        return "model.f16.gguf"
    if quantization:
        return re.sub(_QUANTIZATION_PATTERN, "f16", clean_name, count=1)
    if clean_name.lower().endswith(".gguf"):
        return f"{clean_name[:-5]}.f16.gguf"
    return f"{clean_name}.f16.gguf"


def _recommended_next_action(
    *,
    blockers: List[str],
    warnings: List[str],
    strategy: str,
    auth_required: bool,
    auth_configured: bool,
    missing_conversion_packages: List[str],
    converter_present: bool,
    quantize_present: bool,
    has_bootstrap_path: bool,
    source_ref: str,
) -> str:
    if blockers:
        if auth_required and not auth_configured:
            return "Save and verify a Hugging Face access token, then re-run the manual pipeline refresh."
        if not source_ref:
            return "Resolve the upstream source first, then place or convert the artifact into the manifest path."
        if strategy == "manual_quantization" and has_bootstrap_path and (not converter_present or not quantize_present):
            return "Run the llama.cpp bootstrap commands so conversion and quantization can proceed."
        return blockers[0]
    if strategy == "manual_quantization" and has_bootstrap_path and (not converter_present or not quantize_present):
        return "Run the llama.cpp bootstrap commands so conversion and quantization can proceed."
    if missing_conversion_packages:
        return "Install the missing Python conversion packages before running the GGUF export commands."
    if warnings:
        return warnings[0]
    if strategy == "manual_quantization":
        return "Run the staged download, conversion, quantization, and promotion commands in order."
    return "Place the resolved artifact at the manifest target path, then refresh inventory."


def _package_status(*, module_name: str, distribution_name: str) -> Dict[str, Any]:
    available = importlib.util.find_spec(module_name) is not None
    version = ""
    if available:
        try:
            version = importlib.metadata.version(distribution_name)
        except Exception:
            version = ""
    return {"available": available, "module": module_name, "distribution": distribution_name, "version": version}


def _command_status(command_name: str) -> Dict[str, Any]:
    command_path = shutil.which(command_name) or ""
    return {"available": bool(command_path), "path": command_path, "version": ""}


def _ps_quote(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _slug(value: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip()).strip("-_.").lower()
    return clean or "manual-model"


def _dedupe_strings(values: Iterable[str]) -> List[str]:
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


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "ready", "available"}
