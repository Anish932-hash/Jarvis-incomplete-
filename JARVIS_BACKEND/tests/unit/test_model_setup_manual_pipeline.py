from __future__ import annotations

from typing import Any, Dict

from backend.python.inference.model_setup_manual_pipeline import build_model_setup_manual_pipeline


def _toolchain_snapshot(*, ready: bool = True, bootstrap_ready: bool = True, packages_ready: bool = True) -> Dict[str, Any]:
    package_available = {"available": packages_ready, "version": "1.0"}
    return {
        "commands": {
            "python": {"available": True, "path": "C:/Python/python.exe"},
            "git": {"available": bootstrap_ready, "path": "C:/Program Files/Git/bin/git.exe"},
            "cmake": {"available": bootstrap_ready, "path": "C:/Program Files/CMake/bin/cmake.exe"},
            "winget": {"available": True, "path": "C:/Windows/System32/winget.exe"},
        },
        "packages": {
            "huggingface_hub": dict(package_available),
            "transformers": dict(package_available),
            "safetensors": dict(package_available),
            "sentencepiece": dict(package_available),
            "llama_cpp": {"available": ready, "version": "0.3"},
        },
        "llama_cpp": {
            "ready": ready,
            "convert_script_available": ready,
            "convert_script_path": "C:/toolchains/llama.cpp/convert_hf_to_gguf.py",
            "quantize_binary_available": ready,
            "quantize_binary_path": "C:/toolchains/llama.cpp/build/bin/llama-quantize.exe",
            "bootstrap_ready": bootstrap_ready,
            "bootstrap_commands": [
                "git clone https://github.com/ggml-org/llama.cpp.git 'C:/toolchains/llama.cpp'",
                "cmake -S 'C:/toolchains/llama.cpp' -B 'C:/toolchains/llama.cpp/build'",
                "cmake --build 'C:/toolchains/llama.cpp/build' --config Release --target llama-quantize",
            ],
        },
    }


def _plan_payload(*, items: list[Dict[str, Any]], huggingface_ready: bool = True) -> Dict[str, Any]:
    return {
        "status": "success",
        "items": items,
        "providers": [
            {
                "provider": "huggingface",
                "ready": huggingface_ready,
                "required_by_manifest": False,
            }
        ],
    }


def test_manual_pipeline_builds_convertible_gguf_commands() -> None:
    plan = _plan_payload(
        items=[
            {
                "key": "reasoning-qwen2.5-14b-gguf",
                "name": "Qwen2.5 14B Instruct",
                "task": "reasoning",
                "strategy": "manual_quantization",
                "source_kind": "huggingface",
                "source_ref": "Qwen/Qwen2.5-14B-Instruct",
                "source_url": "https://huggingface.co/Qwen/Qwen2.5-14B-Instruct",
                "family": "qwen",
                "backend": "gguf",
                "path": "E:/J.A.R.V.I.S/reasoning/Qwen2.5-14B-Instruct-Q8_0.gguf",
                "notes": [],
            }
        ],
        huggingface_ready=True,
    )

    payload = build_model_setup_manual_pipeline(
        plan_payload=plan,
        toolchain_snapshot=_toolchain_snapshot(ready=True, bootstrap_ready=True, packages_ready=True),
    )

    assert payload["status"] == "success"
    assert int(payload["summary"]["ready_count"]) == 1
    item = payload["items"][0]
    assert item["convertible"] is True
    assert item["status"] == "ready"
    assert item["target_quantization"] == "q8_0"
    assert any("convert_hf_to_gguf.py" in command for command in item["commands"])
    assert any("llama-quantize" in command for command in item["commands"])


def test_manual_pipeline_blocks_gated_and_unresolved_sources() -> None:
    plan = _plan_payload(
        items=[
            {
                "key": "reasoning-meta-llama-3.1-8b-gguf",
                "name": "Meta Llama 3.1 8B",
                "task": "reasoning",
                "strategy": "manual_quantization",
                "source_kind": "huggingface",
                "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
                "source_url": "https://huggingface.co/meta-llama/Llama-3.1-8B-Instruct",
                "family": "llama",
                "backend": "gguf",
                "path": "E:/J.A.R.V.I.S/reasoning/Meta-Llama-3.1-8B-Instruct-Q8_0.gguf",
                "notes": [],
            },
            {
                "key": "vision-ggml-model-q4-k",
                "name": "Vision GGML",
                "task": "vision",
                "strategy": "manual",
                "source_kind": "unknown",
                "source_ref": "",
                "source_url": "",
                "family": "gguf-vision",
                "backend": "gguf",
                "path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision/ggml-model-q4_k.gguf",
                "notes": [],
            },
        ],
        huggingface_ready=False,
    )

    payload = build_model_setup_manual_pipeline(
        plan_payload=plan,
        toolchain_snapshot=_toolchain_snapshot(ready=False, bootstrap_ready=True, packages_ready=True),
    )

    assert payload["status"] == "success"
    assert int(payload["summary"]["blocked_count"]) == 2
    items = {str(item["key"]): item for item in payload["items"]}
    meta = items["reasoning-meta-llama-3.1-8b-gguf"]
    vision = items["vision-ggml-model-q4-k"]
    assert meta["auth_required"] is True
    assert any("Hugging Face access token" in blocker for blocker in meta["blockers"])
    assert vision["pipeline_kind"] == "unresolved_source"
    assert any("Upstream source" in blocker for blocker in vision["blockers"])
    assert any(str(action.get("id")) == "verify-huggingface-token" for action in payload["upgrade_actions"])


def test_manual_pipeline_surfaces_upgrade_actions_when_bootstrap_is_needed() -> None:
    plan = _plan_payload(
        items=[
            {
                "key": "tts-orpheus-3b-gguf",
                "name": "Orpheus 3B",
                "task": "tts",
                "strategy": "manual_quantization",
                "source_kind": "huggingface",
                "source_ref": "canopylabs/orpheus-3b-0.1-ft",
                "source_url": "https://huggingface.co/canopylabs/orpheus-3b-0.1-ft",
                "family": "orpheus",
                "backend": "gguf",
                "path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                "notes": [],
            }
        ],
        huggingface_ready=True,
    )

    payload = build_model_setup_manual_pipeline(
        plan_payload=plan,
        toolchain_snapshot=_toolchain_snapshot(ready=False, bootstrap_ready=True, packages_ready=False),
    )

    assert payload["status"] == "success"
    assert int(payload["summary"]["warning_count"]) == 1
    item = payload["items"][0]
    assert item["status"] == "warning"
    assert "llama.cpp bootstrap commands" in str(item["recommended_next_action"])
    action_ids = {str(action.get("id")) for action in payload["upgrade_actions"]}
    assert "install-conversion-packages" in action_ids
    assert "install-llama-cpp-python" in action_ids
    assert "bootstrap-llama-cpp" in action_ids
