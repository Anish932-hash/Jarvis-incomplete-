from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.python.speech.audio_output import AudioOutput
from backend.python.speech.local_tts_bridge import LocalNeuralTtsBridge
from backend.python.speech.playback_session import PlaybackSessionRegistry


class LocalTTS:
    """
    Local TTS orchestrator with provider fallbacks and runtime diagnostics.
    Neural local TTS can run through a local HTTP speech endpoint, a custom
    command wrapper, or Coqui CLI when a compatible model is configured.
    """

    _PROVIDER_NEURAL_RUNTIME = "neural_runtime"
    _PROVIDER_PYTTSX3 = "pyttsx3"
    _PROVIDER_WIN32_SAPI = "win32_sapi"
    _NEURAL_PROVIDER_ALIASES = {
        "neural",
        "neural_runtime",
        "local-neural",
        "local-neural-runtime",
        "orpheus",
        "command",
        "http",
        "openai_http",
        "coqui",
        "coqui_cli",
        "coqui_python",
        "llama_cpp",
    }

    _engine_lock = threading.RLock()
    _active_engine: Any = None
    _active_win32_speaker: Any = None
    _active_audio_output: Optional[AudioOutput] = None
    _active_started_at = 0.0
    _active_session_id = ""
    _active_provider = ""

    _provider_state: Dict[str, Dict[str, Any]] = {}
    _history: List[Dict[str, Any]] = []
    _history_max = 160

    def __init__(self, voice: str = "", rate: int = 175, volume: float = 1.0) -> None:
        self.voice = voice.strip()
        self.rate = max(80, min(int(rate), 320))
        self.volume = max(0.0, min(float(volume), 1.0))

    def speak(self, text: str, *, provider_preference: str = "auto") -> Dict[str, Any]:
        value = str(text or "").strip()
        if not value:
            return {"status": "error", "message": "Text is required."}

        preference = str(provider_preference or os.getenv("JARVIS_LOCAL_TTS_PROVIDER", "auto") or "auto").strip().lower() or "auto"
        neural_runtime = self._neural_runtime_metadata()
        try:
            providers = self._resolve_provider_chain(preference, neural_runtime)
        except TypeError:
            providers = self._resolve_provider_chain(preference)
        if not providers:
            return {
                "status": "error",
                "message": "No local TTS providers are enabled.",
                "provider_preference": preference,
                "neural_runtime": neural_runtime,
            }

        attempts: List[Dict[str, Any]] = []
        for provider in providers:
            readiness = self._provider_ready(provider)
            if not bool(readiness.get("ready", True)):
                attempts.append(
                    {
                        "provider": provider,
                        "status": "skipped",
                        "reason": str(readiness.get("reason", "unavailable")).strip() or "unavailable",
                        "retry_after_s": float(readiness.get("retry_after_s", 0.0) or 0.0),
                        "message": str(readiness.get("message", readiness.get("last_error", ""))).strip(),
                    }
                )
                self._record_history({"kind": "attempt", "provider": provider, "status": "skipped", **attempts[-1]})
                continue

            self._mark_provider_attempt(provider)
            started = time.monotonic()
            if provider == self._PROVIDER_NEURAL_RUNTIME:
                result = self._speak_neural_runtime(value, runtime_metadata=neural_runtime)
            elif provider == self._PROVIDER_PYTTSX3:
                result = self._speak_pyttsx3(value)
            elif provider == self._PROVIDER_WIN32_SAPI:
                result = self._speak_win32_sapi(value)
            else:
                result = {"status": "error", "message": f"Unsupported local provider '{provider}'."}
            latency_s = max(0.0, time.monotonic() - started)
            if str(result.get("status", "")).lower() == "success":
                self._mark_provider_success(provider, latency_s=latency_s)
                attempts.append({"provider": provider, "status": "success", "latency_s": round(latency_s, 4)})
                result["provider_preference"] = preference
                result["provider_used"] = provider
                result["attempts"] = attempts
                result["local_rate"] = self.rate
                result["local_volume"] = self.volume
                result["neural_runtime"] = neural_runtime
                self._record_history({"kind": "attempt", "provider": provider, "status": "success", "latency_s": round(latency_s, 4)})
                return result

            message = str(result.get("message", "local provider failed")).strip() or "local provider failed"
            self._mark_provider_failure(provider, error=message, transient=self._is_transient_error(message))
            attempts.append({"provider": provider, "status": "error", "latency_s": round(latency_s, 4), "message": message})
            self._record_history({"kind": "attempt", "provider": provider, "status": "error", "latency_s": round(latency_s, 4), "message": message})

        errors = [str(item.get("message", "")).strip() for item in attempts if str(item.get("message", "")).strip()]
        return {
            "status": "error",
            "message": "; ".join(errors[:4]) or "All local TTS providers failed.",
            "provider_preference": preference,
            "attempts": attempts,
            "neural_runtime": neural_runtime,
        }

    def _speak_neural_runtime(self, text: str, *, runtime_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        runtime = dict(runtime_metadata or self._neural_runtime_metadata())
        if not bool(runtime.get("enabled", False)):
            return {"status": "error", "message": "Local neural TTS is disabled.", "neural_runtime": runtime}
        if not bool(runtime.get("configured", False)):
            return {"status": "error", "message": "Local neural TTS is not configured.", "neural_runtime": runtime}
        execution_backend = str(runtime.get("execution_backend", "")).strip().lower()
        if execution_backend == "openai_http":
            bridge_status = LocalNeuralTtsBridge.shared().ensure_started(
                reason="tts_speak",
                wait_ready=True,
                timeout_s=min(20.0, float(runtime.get("timeout_s", 120.0) or 120.0)),
            )
            runtime = dict(self._neural_runtime_metadata())
            runtime["bridge"] = bridge_status
        if not bool(runtime.get("ready", False)):
            return {
                "status": "error",
                "message": str(runtime.get("message", "Local neural TTS is not ready.")).strip() or "Local neural TTS is not ready.",
                "neural_runtime": runtime,
            }

        artifact_path, retain_output = self._allocate_neural_output_path(output_format=str(runtime.get("output_format", "wav")))
        generation_started = time.monotonic()
        try:
            if execution_backend == "openai_http":
                synth_result = self._run_neural_http(text=text, runtime=runtime, output_path=artifact_path)
            elif execution_backend == "command":
                synth_result = self._run_neural_command(text=text, runtime=runtime, output_path=artifact_path)
            elif execution_backend == "coqui_cli":
                synth_result = self._run_neural_coqui_cli(text=text, runtime=runtime, output_path=artifact_path)
            else:
                synth_result = {"status": "error", "message": f"Unsupported neural execution backend '{execution_backend}'."}
            generation_latency_s = max(0.0, time.monotonic() - generation_started)
            if str(synth_result.get("status", "")).lower() != "success":
                self._safe_unlink(artifact_path, keep=retain_output)
                synth_result["neural_runtime"] = runtime
                synth_result["generation_latency_s"] = round(generation_latency_s, 4)
                return synth_result

            audio_output = AudioOutput(volume=self.volume)
            session = PlaybackSessionRegistry.start(
                channel="tts",
                provider=f"local-neural-{execution_backend}",
                metadata={
                    "voice": str(runtime.get("voice", "")).strip(),
                    "rate": self.rate,
                    "volume": self.volume,
                    "backend": execution_backend,
                    "model": str(runtime.get("model_label", "")).strip(),
                },
                stop_callback=audio_output.stop,
            )
            session_id = str(session.get("session_id", "")).strip()
            playback_started = time.monotonic()
            with self._engine_lock:
                self._active_audio_output = audio_output
                self._active_started_at = time.time()
                self._active_session_id = session_id
                self._active_provider = self._PROVIDER_NEURAL_RUNTIME
            try:
                audio_output.play_file(str(artifact_path))
                PlaybackSessionRegistry.finish(session_id, status="completed", message="")
            except Exception as exc:  # noqa: BLE001
                PlaybackSessionRegistry.finish(session_id, status="error", message=str(exc))
                return {"status": "error", "message": str(exc), "neural_runtime": runtime, "generation_latency_s": round(generation_latency_s, 4)}
            finally:
                with self._engine_lock:
                    self._active_audio_output = None
                    self._active_started_at = 0.0
                    self._active_session_id = ""
                    self._active_provider = ""

            result: Dict[str, Any] = {
                "status": "success",
                "text": text,
                "mode": f"local-neural-{execution_backend}",
                "voice": str(runtime.get("voice", "")).strip(),
                "session_id": session_id,
                "execution_backend": execution_backend,
                "generation_latency_s": round(generation_latency_s, 4),
                "playback_latency_s": round(max(0.0, time.monotonic() - playback_started), 4),
                "artifact_retained": retain_output,
                "neural_runtime": runtime,
            }
            if retain_output:
                result["output_path"] = str(artifact_path)
            return result
        finally:
            self._safe_unlink(artifact_path, keep=retain_output)

    def _run_neural_command(self, *, text: str, runtime: Dict[str, Any], output_path: Path) -> Dict[str, Any]:
        template = str(runtime.get("command_template", "")).strip()
        if not template:
            return {"status": "error", "message": "Local neural TTS command template is not configured."}
        replacements = {
            "text": text,
            "output_path": str(output_path),
            "model_path": str(runtime.get("model_path", "")).strip(),
            "model_label": str(runtime.get("model_label", "")).strip(),
            "config_path": str(runtime.get("config_path", "")).strip(),
            "voice": str(runtime.get("voice", "")).strip(),
            "format": str(runtime.get("output_format", "wav")).strip(),
            "backend": str(runtime.get("backend", "")).strip(),
        }
        command_text = str(template)
        for key, value in replacements.items():
            command_text = command_text.replace(f"{{{key}}}", str(value))
            command_text = command_text.replace(f"{{{key}_q}}", subprocess.list2cmdline([str(value)]))
        timeout_s = float(runtime.get("timeout_s", 120.0) or 120.0)
        try:
            completed = subprocess.run(command_text, shell=True, capture_output=True, text=True, timeout=timeout_s, check=False)
        except subprocess.TimeoutExpired as exc:
            return {"status": "error", "message": f"Local neural TTS command timed out after {int(timeout_s)}s: {exc}"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"Local neural TTS command failed to start: {exc}"}
        stdout = str(completed.stdout or "").strip()
        stderr = str(completed.stderr or "").strip()
        if completed.returncode != 0:
            return {"status": "error", "message": stderr or stdout or f"Local neural TTS command exited with code {completed.returncode}."}
        if not output_path.exists() or output_path.stat().st_size <= 0:
            return {"status": "error", "message": "Local neural TTS command finished without producing audio output."}
        return {"status": "success", "stdout": stdout, "stderr": stderr, "execution_backend": "command"}

    def _run_neural_http(self, *, text: str, runtime: Dict[str, Any], output_path: Path) -> Dict[str, Any]:
        endpoint = str(runtime.get("http_endpoint", "")).strip()
        if not endpoint:
            return {"status": "error", "message": "Local neural TTS HTTP endpoint is not configured."}
        request_payload = {
            "model": str(runtime.get("http_model", "")).strip() or "tts-1",
            "input": text,
            "voice": str(runtime.get("voice", "")).strip() or "default",
            "response_format": str(runtime.get("output_format", "wav")).strip() or "wav",
        }
        speed = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_SPEED", "") or "").strip()
        if speed:
            try:
                request_payload["speed"] = float(speed)
            except Exception:
                pass
        body = json.dumps(request_payload).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/octet-stream, audio/wav, audio/mpeg, application/json"}
        auth_token = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_AUTH_TOKEN", "") or "").strip()
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
        timeout_s = float(runtime.get("timeout_s", 120.0) or 120.0)
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                raw_bytes = response.read()
                content_type = str(response.headers.get("Content-Type", "")).strip().lower()
        except urllib.error.HTTPError as exc:
            error_payload = exc.read().decode("utf-8", errors="ignore").strip()
            return {"status": "error", "message": error_payload or f"HTTP {exc.code} from local neural TTS endpoint."}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"Local neural TTS HTTP request failed: {exc}"}
        audio_bytes = self._extract_audio_bytes(raw_bytes=raw_bytes, content_type=content_type)
        if not audio_bytes:
            return {"status": "error", "message": "Local neural TTS endpoint returned no audio payload."}
        output_path.write_bytes(audio_bytes)
        return {"status": "success", "execution_backend": "openai_http", "content_type": content_type}

    def _run_neural_coqui_cli(self, *, text: str, runtime: Dict[str, Any], output_path: Path) -> Dict[str, Any]:
        cli_path = str(runtime.get("coqui_cli_path", "")).strip()
        if not cli_path:
            return {"status": "error", "message": "Coqui CLI executable was not found."}
        args = [cli_path, "--text", text, "--out_path", str(output_path)]
        model_name = str(runtime.get("coqui_model_name", "")).strip()
        model_path = str(runtime.get("model_path", "")).strip()
        config_path = str(runtime.get("config_path", "")).strip()
        if model_name:
            args.extend(["--model_name", model_name])
        elif model_path and config_path:
            args.extend(["--model_path", model_path, "--config_path", config_path])
        else:
            return {"status": "error", "message": "Coqui CLI requires a model name or both model and config paths."}
        speaker_idx = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_SPEAKER", "") or "").strip()
        language_idx = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_LANGUAGE", "") or "").strip()
        speaker_wav = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_SPEAKER_WAV", "") or "").strip()
        if speaker_idx:
            args.extend(["--speaker_idx", speaker_idx])
        if language_idx:
            args.extend(["--language_idx", language_idx])
        if speaker_wav:
            args.extend(["--speaker_wav", speaker_wav])
        if self._as_bool(os.getenv("JARVIS_LOCAL_NEURAL_TTS_USE_CUDA", "0"), default=False):
            args.append("--use_cuda")
        timeout_s = float(runtime.get("timeout_s", 120.0) or 120.0)
        try:
            completed = subprocess.run(args, capture_output=True, text=True, timeout=timeout_s, check=False)
        except subprocess.TimeoutExpired as exc:
            return {"status": "error", "message": f"Coqui CLI timed out after {int(timeout_s)}s: {exc}"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"Coqui CLI failed to start: {exc}"}
        stdout = str(completed.stdout or "").strip()
        stderr = str(completed.stderr or "").strip()
        if completed.returncode != 0:
            return {"status": "error", "message": stderr or stdout or f"Coqui CLI exited with code {completed.returncode}."}
        if not output_path.exists() or output_path.stat().st_size <= 0:
            return {"status": "error", "message": "Coqui CLI finished without producing audio output."}
        return {"status": "success", "stdout": stdout, "stderr": stderr, "execution_backend": "coqui_cli"}

    def _speak_pyttsx3(self, text: str) -> Dict[str, Any]:
        try:
            import pyttsx3  # type: ignore
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"pyttsx3 unavailable: {exc}"}

        engine: Any = None
        session_id = ""
        try:
            engine = pyttsx3.init()
            engine.setProperty("rate", self.rate)
            engine.setProperty("volume", self.volume)
            chosen_voice = self._pick_pyttsx3_voice(engine=engine, preference=self.voice)
            if chosen_voice:
                engine.setProperty("voice", chosen_voice)
            session = PlaybackSessionRegistry.start(
                channel="tts",
                provider="local-pyttsx3",
                metadata={"voice": chosen_voice or "", "rate": self.rate, "volume": self.volume},
                stop_callback=engine.stop,
            )
            session_id = str(session.get("session_id", "")).strip()
            with self._engine_lock:
                self._active_engine = engine
                self._active_started_at = time.time()
                self._active_session_id = session_id
                self._active_provider = self._PROVIDER_PYTTSX3
            engine.say(text)
            engine.runAndWait()
            PlaybackSessionRegistry.finish(session_id, status="completed", message="")
            return {"status": "success", "text": text, "mode": "local-pyttsx3", "voice": chosen_voice or "", "session_id": session_id}
        except Exception as exc:  # noqa: BLE001
            if session_id:
                PlaybackSessionRegistry.finish(session_id, status="error", message=str(exc))
            return {"status": "error", "message": str(exc)}
        finally:
            with self._engine_lock:
                if engine is not None and self._active_engine is engine:
                    self._active_engine = None
                    self._active_started_at = 0.0
                    self._active_session_id = ""
                    self._active_provider = ""

    def _speak_win32_sapi(self, text: str) -> Dict[str, Any]:
        if os.name != "nt":
            return {"status": "error", "message": "win32_sapi is only available on Windows."}
        try:
            import win32com.client  # type: ignore
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"win32com unavailable: {exc}"}

        speaker: Any = None
        session_id = ""
        try:
            speaker = win32com.client.Dispatch("SAPI.SpVoice")
            speaker.Rate = self._map_rate_to_sapi(self.rate)
            speaker.Volume = int(round(max(0.0, min(self.volume, 1.0)) * 100))
            selected_voice = self._pick_win32_voice(speaker=speaker, preference=self.voice)
            session = PlaybackSessionRegistry.start(
                channel="tts",
                provider="local-win32-sapi",
                metadata={"voice": selected_voice or "", "rate": self.rate, "volume": self.volume},
                stop_callback=lambda: self._stop_win32_speaker(speaker),
            )
            session_id = str(session.get("session_id", "")).strip()
            with self._engine_lock:
                self._active_win32_speaker = speaker
                self._active_started_at = time.time()
                self._active_session_id = session_id
                self._active_provider = self._PROVIDER_WIN32_SAPI
            speaker.Speak(text)
            PlaybackSessionRegistry.finish(session_id, status="completed", message="")
            return {"status": "success", "text": text, "mode": "local-win32-sapi", "voice": selected_voice, "session_id": session_id}
        except Exception as exc:  # noqa: BLE001
            if session_id:
                PlaybackSessionRegistry.finish(session_id, status="error", message=str(exc))
            return {"status": "error", "message": str(exc)}
        finally:
            with self._engine_lock:
                if speaker is not None and self._active_win32_speaker is speaker:
                    self._active_win32_speaker = None
                    self._active_started_at = 0.0
                    self._active_session_id = ""
                    self._active_provider = ""

    @classmethod
    def stop(cls, *, session_id: str = "") -> Dict[str, Any]:
        clean_session = str(session_id or "").strip()
        interrupted = PlaybackSessionRegistry.interrupt(session_id=clean_session, channel="tts", reason="tts_stop_local")
        if bool(interrupted.get("stopped", False)):
            session = interrupted.get("session") if isinstance(interrupted.get("session"), dict) else {}
            provider = str(session.get("provider", "")).strip() or "local"
            return {"status": "success", "stopped": True, "mode": provider, "session_id": str(session.get("session_id", "")).strip(), "session": session}

        with cls._engine_lock:
            engine = cls._active_engine
            speaker = cls._active_win32_speaker
            audio_output = cls._active_audio_output
            started = float(cls._active_started_at or 0.0)
            active_session_id = str(cls._active_session_id or "").strip()
            active_provider = str(cls._active_provider or "").strip()
        if engine is None and speaker is None and audio_output is None:
            return {"status": "success", "stopped": False, "message": "No active local TTS playback."}
        stopped = False
        errors: List[str] = []
        if engine is not None:
            try:
                engine.stop()
                stopped = True
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
        if speaker is not None:
            try:
                cls._stop_win32_speaker(speaker)
                stopped = True
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
        if audio_output is not None:
            try:
                audio_output.stop()
                stopped = True
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
        with cls._engine_lock:
            cls._active_engine = None
            cls._active_win32_speaker = None
            cls._active_audio_output = None
            cls._active_started_at = 0.0
            cls._active_session_id = ""
            cls._active_provider = ""
        if active_session_id:
            PlaybackSessionRegistry.finish(active_session_id, status="interrupted", message="tts_stop_local_fallback")
        if stopped:
            duration_s = max(0.0, time.time() - started) if started > 0 else 0.0
            return {"status": "success", "stopped": True, "mode": active_provider or "local", "active_duration_s": round(duration_s, 3), "session_id": active_session_id}
        return {"status": "error", "stopped": False, "message": "; ".join(errors) or "unable to stop local TTS"}

    @classmethod
    def diagnostics(cls, *, history_limit: int = 24) -> Dict[str, Any]:
        bounded = max(1, min(int(history_limit), 200))
        neural_runtime = cls._neural_runtime_metadata()
        with cls._engine_lock:
            provider_snapshot: Dict[str, Dict[str, Any]] = {}
            for provider in (cls._PROVIDER_NEURAL_RUNTIME, cls._PROVIDER_PYTTSX3, cls._PROVIDER_WIN32_SAPI):
                state = cls._provider_state_for(provider)
                readiness = cls._provider_ready(provider)
                payload: Dict[str, Any] = {
                    "ready": bool(readiness.get("ready", False)),
                    "reason": str(readiness.get("reason", "")).strip(),
                    "message": str(readiness.get("message", readiness.get("last_error", ""))).strip(),
                    "retry_after_s": round(float(readiness.get("retry_after_s", 0.0) or 0.0), 3),
                    "attempts": int(state.get("attempts", 0) or 0),
                    "successes": int(state.get("successes", 0) or 0),
                    "failures": int(state.get("failures", 0) or 0),
                    "failure_ema": round(float(state.get("failure_ema", 0.0) or 0.0), 6),
                    "latency_ema_s": round(float(state.get("latency_ema_s", 0.0) or 0.0), 6),
                    "last_error": str(state.get("last_error", "")).strip(),
                    "last_success_at": float(state.get("last_success_at", 0.0) or 0.0),
                    "enabled": True,
                }
                if provider == cls._PROVIDER_NEURAL_RUNTIME:
                    payload.update({
                        "enabled": bool(neural_runtime.get("enabled", False)),
                        "configured": bool(neural_runtime.get("configured", False)),
                        "backend": str(neural_runtime.get("backend", "")).strip(),
                        "execution_backend": str(neural_runtime.get("execution_backend", "")).strip(),
                        "model_path": str(neural_runtime.get("model_path", "")).strip(),
                        "model_exists": bool(neural_runtime.get("model_exists", False)),
                        "issues": list(neural_runtime.get("issues", [])) if isinstance(neural_runtime.get("issues", []), list) else [],
                        "bridge": dict(neural_runtime.get("bridge", {})) if isinstance(neural_runtime.get("bridge"), dict) else {},
                        "bridge_ready": bool(neural_runtime.get("bridge_ready", False)),
                    })
                elif provider == cls._PROVIDER_WIN32_SAPI:
                    payload["enabled"] = os.name == "nt" and cls._win32_sapi_enabled()
                provider_snapshot[provider] = payload
            return {
                "status": "success",
                "provider": "local",
                "active_provider": str(cls._active_provider or "").strip(),
                "active_session_id": str(cls._active_session_id or "").strip(),
                "providers": provider_snapshot,
                "history_tail": list(cls._history[-bounded:]),
                "neural_runtime": neural_runtime,
            }

    def _resolve_provider_chain(self, provider_preference: str, neural_runtime: Optional[Dict[str, Any]] = None) -> List[str]:
        pref = str(provider_preference or "auto").strip().lower() or "auto"
        allow_win32 = os.name == "nt" and self._win32_sapi_enabled()
        runtime = dict(neural_runtime or self._neural_runtime_metadata())
        allow_neural = bool(runtime.get("enabled", False)) and bool(runtime.get("configured", False))
        prefer_neural = allow_neural and bool(runtime.get("ready", False))
        if pref in {"", "auto"}:
            chain: List[str] = []
            if prefer_neural:
                chain.append(self._PROVIDER_NEURAL_RUNTIME)
            chain.append(self._PROVIDER_PYTTSX3)
            if allow_win32:
                chain.append(self._PROVIDER_WIN32_SAPI)
            return chain
        if pref in {"local", self._PROVIDER_PYTTSX3, "local-pyttsx3"}:
            chain = [self._PROVIDER_PYTTSX3]
            if allow_neural:
                chain.insert(0, self._PROVIDER_NEURAL_RUNTIME)
            if allow_win32:
                chain.append(self._PROVIDER_WIN32_SAPI)
            return self._dedupe_providers(chain)
        if pref in self._NEURAL_PROVIDER_ALIASES:
            return [self._PROVIDER_NEURAL_RUNTIME]
        if pref in {"win32", "sapi", self._PROVIDER_WIN32_SAPI, "local-win32-sapi"}:
            return [self._PROVIDER_WIN32_SAPI] if allow_win32 else []
        chain = [self._PROVIDER_PYTTSX3]
        if prefer_neural:
            chain.insert(0, self._PROVIDER_NEURAL_RUNTIME)
        if allow_win32:
            chain.append(self._PROVIDER_WIN32_SAPI)
        return self._dedupe_providers(chain)

    @classmethod
    def _provider_state_for(cls, provider: str) -> Dict[str, Any]:
        key = str(provider or "").strip().lower() or "unknown"
        state = cls._provider_state.get(key)
        if isinstance(state, dict):
            return state
        created = {
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "failure_ema": 0.0,
            "latency_ema_s": 0.0,
            "cooldown_until": 0.0,
            "last_error": "",
            "last_success_at": 0.0,
        }
        cls._provider_state[key] = created
        return created

    @classmethod
    def _provider_ready(cls, provider: str) -> Dict[str, Any]:
        now = time.time()
        with cls._engine_lock:
            state = cls._provider_state_for(provider)
            retry_after_s = max(0.0, float(state.get("cooldown_until", 0.0) or 0.0) - now)
        base_ready = True
        reason = "available"
        message = str(state.get("last_error", "")).strip()
        if provider == cls._PROVIDER_NEURAL_RUNTIME:
            runtime = cls._neural_runtime_metadata()
            base_ready = bool(runtime.get("ready", False))
            reason = "available" if base_ready else str(runtime.get("reason", "unavailable")).strip() or "unavailable"
            message = str(runtime.get("message", "")).strip() or message
        elif provider == cls._PROVIDER_WIN32_SAPI:
            base_ready = os.name == "nt" and cls._win32_sapi_enabled()
            if not base_ready:
                reason = "unsupported"
                message = "win32_sapi is only available on Windows when enabled."
        ready = base_ready and retry_after_s <= 0.0
        if base_ready and retry_after_s > 0.0:
            reason = "cooldown"
        return {
            "ready": ready,
            "retry_after_s": round(retry_after_s, 3),
            "last_error": str(state.get("last_error", "")).strip(),
            "message": message,
            "reason": reason,
            "failure_ema": round(float(state.get("failure_ema", 0.0) or 0.0), 6),
        }

    @classmethod
    def _neural_runtime_metadata(cls) -> Dict[str, Any]:
        base_bridge_status = LocalNeuralTtsBridge.shared().status(probe=False)
        runtime_overrides = (
            dict(base_bridge_status.get("runtime_overrides", {}))
            if isinstance(base_bridge_status, dict) and isinstance(base_bridge_status.get("runtime_overrides", {}), dict)
            else {}
        )
        model_path = str(runtime_overrides.get("model_path", os.getenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", "")) or "").strip()
        config_path = str(runtime_overrides.get("config_path", os.getenv("JARVIS_LOCAL_NEURAL_TTS_CONFIG_PATH", "")) or "").strip()
        requested_backend = str(runtime_overrides.get("backend", os.getenv("JARVIS_LOCAL_NEURAL_TTS_BACKEND", "")) or "").strip().lower()
        http_endpoint = str(
            runtime_overrides.get(
                "http_endpoint",
                runtime_overrides.get("endpoint", os.getenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "")),
            )
            or ""
        ).strip()
        command_template = str(runtime_overrides.get("command_template", os.getenv("JARVIS_LOCAL_NEURAL_TTS_COMMAND", "")) or "").strip()
        coqui_model_name = str(
            runtime_overrides.get(
                "coqui_model_name",
                os.getenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_NAME", os.getenv("JARVIS_LOCAL_NEURAL_TTS_COQUI_MODEL_NAME", "")),
            )
            or ""
        ).strip()
        configured = bool(model_path or http_endpoint or command_template or coqui_model_name)
        enabled_override = runtime_overrides.get("enabled")
        enabled = (
            bool(enabled_override)
            if isinstance(enabled_override, bool)
            else cls._as_bool(os.getenv("JARVIS_LOCAL_NEURAL_TTS_ENABLED", str(configured)), default=configured)
        )
        model_exists = cls._path_exists(model_path)
        config_exists = cls._path_exists(config_path)
        coqui_cli_path = cls._discover_coqui_cli()
        coqui_cli_available = bool(coqui_cli_path)
        backend = requested_backend
        if not backend:
            lowered_path = model_path.lower()
            if lowered_path.endswith(".gguf"):
                backend = "llama_cpp"
            elif http_endpoint:
                backend = "openai_http"
            elif command_template:
                backend = "command"
            elif coqui_model_name or config_path:
                backend = "coqui_cli"
            elif lowered_path.endswith(".onnx"):
                backend = "onnx"
            elif model_path:
                backend = "custom"
            else:
                backend = ""
        execution_backend = ""
        issues: List[str] = []
        if not enabled:
            message = "Local neural TTS is disabled."
        elif not configured:
            issues.append("not_configured")
            message = "Configure a neural model path, HTTP endpoint, command template, or Coqui model name."
        else:
            if model_path and not model_exists and backend not in {"openai_http", "http", "server"}:
                issues.append("model_missing")
            if backend in {"llama_cpp", "gguf", "orpheus"}:
                if http_endpoint:
                    execution_backend = "openai_http"
                elif command_template:
                    execution_backend = "command"
                else:
                    issues.append("transport_missing")
            elif backend in {"openai_http", "http", "server"}:
                if http_endpoint:
                    execution_backend = "openai_http"
                else:
                    issues.append("endpoint_missing")
            elif backend in {"command", "custom_command"}:
                if command_template:
                    execution_backend = "command"
                else:
                    issues.append("command_missing")
            elif backend in {"coqui", "coqui_cli", "coqui_python"}:
                if not coqui_cli_available:
                    issues.append("coqui_cli_missing")
                elif coqui_model_name or (model_exists and config_exists):
                    execution_backend = "coqui_cli"
                else:
                    issues.append("coqui_model_missing")
            else:
                if http_endpoint:
                    execution_backend = "openai_http"
                elif command_template:
                    execution_backend = "command"
                elif coqui_cli_available and (coqui_model_name or (model_exists and config_exists)):
                    execution_backend = "coqui_cli"
                else:
                    issues.append("transport_missing")
            if issues:
                issue = issues[0]
                if issue == "model_missing":
                    message = f"Local neural TTS model path does not exist: {model_path}"
                elif issue == "endpoint_missing":
                    message = "Set JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT to use a local speech server."
                elif issue == "command_missing":
                    message = "Set JARVIS_LOCAL_NEURAL_TTS_COMMAND to run a local neural TTS wrapper command."
                elif issue == "coqui_cli_missing":
                    message = "Coqui CLI was not found; install Coqui TTS or point JARVIS_LOCAL_NEURAL_TTS_COQUI_CLI to tts.exe."
                elif issue == "coqui_model_missing":
                    message = "Configure JARVIS_LOCAL_NEURAL_TTS_MODEL_NAME or provide both model and config paths for Coqui CLI."
                elif issue == "transport_missing" and backend in {"llama_cpp", "gguf", "orpheus"}:
                    message = "GGUF neural TTS models need JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT or JARVIS_LOCAL_NEURAL_TTS_COMMAND."
                elif issue == "transport_missing":
                    message = "No neural TTS transport is configured. Provide an HTTP endpoint, command template, or Coqui model."
                else:
                    message = "Local neural TTS is not ready."
            else:
                message = ""
        model_label = str(runtime_overrides.get("model_label", coqui_model_name or (Path(model_path).stem if model_path else "")) or "").strip()
        output_format = str(runtime_overrides.get("output_format", os.getenv("JARVIS_LOCAL_NEURAL_TTS_OUTPUT_FORMAT", "wav")) or "wav").strip().lower() or "wav"
        bridge_status = LocalNeuralTtsBridge.shared().status(probe=bool(http_endpoint))
        bridge_ready = bool(bridge_status.get("ready", False))
        bridge_applicable = bool(bridge_status.get("endpoint_configured", False) or bridge_status.get("managed", False))
        ready = enabled and configured and bool(execution_backend) and not issues
        reason = "available" if ready else (issues[0] if issues else ("disabled" if not enabled else "not_configured"))
        if execution_backend == "openai_http" and bridge_applicable and not bridge_ready:
            ready = False
            if "bridge_unreachable" not in issues:
                issues.append("bridge_unreachable")
            reason = "bridge_unreachable"
            bridge_message = str(bridge_status.get("message", bridge_status.get("last_error", ""))).strip()
            if bridge_message:
                message = bridge_message
            elif not message:
                message = "Local neural TTS bridge is not ready."
        return {
            "configured": configured,
            "enabled": enabled,
            "ready": ready,
            "reason": reason,
            "message": message,
            "model_path": model_path,
            "model_exists": model_exists,
            "config_path": config_path,
            "config_exists": config_exists,
            "backend": backend,
            "execution_backend": execution_backend,
            "http_endpoint": http_endpoint,
            "endpoint_configured": bool(http_endpoint),
            "http_model": str(runtime_overrides.get("http_model", os.getenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_MODEL", model_label or "tts-1")) or "tts-1").strip() or "tts-1",
            "command_template": command_template,
            "command_configured": bool(command_template),
            "coqui_model_name": coqui_model_name,
            "coqui_cli_path": coqui_cli_path,
            "coqui_cli_available": coqui_cli_available,
            "issues": issues,
            "model_label": model_label,
            "output_format": output_format,
            "timeout_s": (
                max(5.0, min(float(runtime_overrides.get("timeout_s", 0.0) or 0.0), 900.0))
                if runtime_overrides.get("timeout_s") is not None
                else cls._env_float("JARVIS_LOCAL_NEURAL_TTS_TIMEOUT_S", 120.0, minimum=5.0, maximum=900.0)
            ),
            "retain_output": (
                bool(runtime_overrides.get("retain_output"))
                if isinstance(runtime_overrides.get("retain_output"), bool)
                else cls._as_bool(os.getenv("JARVIS_LOCAL_NEURAL_TTS_KEEP_OUTPUTS", "0"), default=False)
            ),
            "voice": str(
                runtime_overrides.get(
                    "voice",
                    os.getenv("JARVIS_LOCAL_NEURAL_TTS_VOICE", os.getenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_VOICE", os.getenv("JARVIS_LOCAL_TTS_VOICE", ""))),
                )
                or ""
            ).strip(),
            "active_profile_id": str(bridge_status.get("active_profile_id", "") or "").strip(),
            "active_template_id": str(bridge_status.get("active_template_id", "") or "").strip(),
            "runtime_overrides": runtime_overrides,
            "bridge": bridge_status,
            "bridge_ready": bridge_ready,
        }

    @classmethod
    def _mark_provider_attempt(cls, provider: str) -> None:
        with cls._engine_lock:
            state = cls._provider_state_for(provider)
            state["attempts"] = int(state.get("attempts", 0) or 0) + 1

    @classmethod
    def _mark_provider_success(cls, provider: str, *, latency_s: float) -> None:
        with cls._engine_lock:
            state = cls._provider_state_for(provider)
            state["successes"] = int(state.get("successes", 0) or 0) + 1
            state["failure_ema"] = max(0.0, float(state.get("failure_ema", 0.0) or 0.0) * 0.72)
            state["last_error"] = ""
            state["last_success_at"] = time.time()
            previous_latency = float(state.get("latency_ema_s", 0.0) or 0.0)
            latency = max(0.0, float(latency_s))
            state["latency_ema_s"] = latency if previous_latency <= 0.0 else ((0.25 * latency) + (0.75 * previous_latency))
            if state["failure_ema"] < 0.08:
                state["cooldown_until"] = 0.0

    @classmethod
    def _mark_provider_failure(cls, provider: str, *, error: str, transient: bool) -> None:
        now = time.time()
        with cls._engine_lock:
            state = cls._provider_state_for(provider)
            state["failures"] = int(state.get("failures", 0) or 0) + 1
            signal = 0.7 if transient else 1.0
            state["failure_ema"] = (0.82 * float(state.get("failure_ema", 0.0) or 0.0)) + (0.18 * signal)
            state["last_error"] = str(error or "").strip()
            threshold = cls._env_float("JARVIS_LOCAL_TTS_COOLDOWN_THRESHOLD", 0.58, minimum=0.2, maximum=0.95)
            if state["failure_ema"] >= threshold:
                base_cooldown_s = cls._env_float("JARVIS_LOCAL_TTS_COOLDOWN_BASE_S", 10.0, minimum=1.0, maximum=300.0)
                factor_s = cls._env_float("JARVIS_LOCAL_TTS_COOLDOWN_FACTOR_S", 65.0, minimum=1.0, maximum=300.0)
                max_cooldown_s = cls._env_float("JARVIS_LOCAL_TTS_COOLDOWN_MAX_S", 180.0, minimum=5.0, maximum=1800.0)
                cooldown_s = min(max_cooldown_s, base_cooldown_s + (float(state["failure_ema"]) * factor_s))
                state["cooldown_until"] = max(float(state.get("cooldown_until", 0.0) or 0.0), now + cooldown_s)

    @classmethod
    def _record_history(cls, row: Dict[str, Any]) -> None:
        payload = dict(row)
        payload["at"] = time.time()
        with cls._engine_lock:
            cls._history.append(payload)
            if len(cls._history) > cls._history_max:
                cls._history = cls._history[-cls._history_max :]

    @staticmethod
    def _pick_pyttsx3_voice(*, engine: Any, preference: str) -> Optional[str]:
        try:
            voices = engine.getProperty("voices") or []
        except Exception:  # noqa: BLE001
            return None
        if not voices:
            return None
        if not preference:
            voice = voices[0]
            return str(getattr(voice, "id", "")).strip() or None
        needle = preference.lower()
        for voice in voices:
            voice_id = str(getattr(voice, "id", ""))
            name = str(getattr(voice, "name", ""))
            if needle in voice_id.lower() or needle in name.lower():
                return voice_id or None
        voice = voices[0]
        return str(getattr(voice, "id", "")).strip() or None

    @staticmethod
    def _map_rate_to_sapi(rate: int) -> int:
        normalized = max(80, min(int(rate), 320))
        scaled = int(round((normalized - 175) / 11.0))
        return max(-10, min(10, scaled))

    @classmethod
    def _pick_win32_voice(cls, *, speaker: Any, preference: str) -> str:
        try:
            voices = speaker.GetVoices()
            count = int(getattr(voices, "Count", 0) or 0)
        except Exception:  # noqa: BLE001
            return ""
        if count <= 0:
            return ""
        def _voice_descriptor(token: Any) -> str:
            try:
                desc = str(token.GetDescription() or "").strip()
            except Exception:
                desc = ""
            if desc:
                return desc
            try:
                return str(getattr(token, "Id", "")).strip()
            except Exception:
                return ""
        needle = str(preference or "").strip().lower()
        if needle:
            for idx in range(count):
                try:
                    token = voices.Item(idx)
                except Exception:
                    continue
                descriptor = _voice_descriptor(token)
                if needle in descriptor.lower():
                    try:
                        speaker.Voice = token
                    except Exception:
                        continue
                    return descriptor
        try:
            fallback = voices.Item(0)
            speaker.Voice = fallback
            return _voice_descriptor(fallback)
        except Exception:  # noqa: BLE001
            return ""

    @staticmethod
    def _stop_win32_speaker(speaker: Any) -> None:
        if speaker is None:
            return
        try:
            speaker.Speak("", 2)
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _is_transient_error(message: str) -> bool:
        lowered = str(message or "").strip().lower()
        return any(token in lowered for token in ("timeout", "tempor", "busy", "resource", "unavailable", "retry"))

    @staticmethod
    def _as_bool(value: Any, *, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        text = str(value).strip().lower()
        if not text:
            return default
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    @classmethod
    def _win32_sapi_enabled(cls) -> bool:
        return cls._as_bool(os.getenv("JARVIS_LOCAL_TTS_ENABLE_WIN32_SAPI", "1"), default=True)

    @staticmethod
    def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
        raw = os.getenv(name, str(default))
        try:
            value = float(raw)
        except Exception:
            value = float(default)
        return max(minimum, min(maximum, value))

    @staticmethod
    def _path_exists(raw_path: str) -> bool:
        if not raw_path:
            return False
        try:
            return Path(raw_path).expanduser().exists()
        except Exception:
            return False

    @classmethod
    def _workspace_root(cls) -> Path:
        return Path(__file__).resolve().parents[4]

    @classmethod
    def _discover_coqui_cli(cls) -> str:
        explicit = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_COQUI_CLI", "") or "").strip()
        candidates = [explicit] if explicit else []
        workspace = cls._workspace_root()
        candidates.extend([str(workspace / ".venv" / "Scripts" / "tts.exe"), str(workspace / ".venv" / "bin" / "tts"), shutil.which("tts") or ""])
        for candidate in candidates:
            if not candidate:
                continue
            try:
                path = Path(candidate).expanduser()
            except Exception:
                continue
            if path.exists():
                return str(path)
        return ""

    @staticmethod
    def _dedupe_providers(providers: List[str]) -> List[str]:
        seen: set[str] = set()
        ordered: List[str] = []
        for provider in providers:
            clean = str(provider or "").strip().lower()
            if clean and clean not in seen:
                ordered.append(clean)
                seen.add(clean)
        return ordered

    def _allocate_neural_output_path(self, *, output_format: str) -> tuple[Path, bool]:
        clean_format = str(output_format or "wav").strip().lower() or "wav"
        suffix = clean_format if clean_format.startswith(".") else f".{clean_format}"
        retain_output = self._as_bool(os.getenv("JARVIS_LOCAL_NEURAL_TTS_KEEP_OUTPUTS", "0"), default=False)
        configured_dir = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_OUTPUT_DIR", "") or "").strip()
        if configured_dir:
            base_dir = Path(configured_dir).expanduser()
            base_dir.mkdir(parents=True, exist_ok=True)
        elif retain_output:
            base_dir = self._workspace_root() / "data" / "tts_cache"
            base_dir.mkdir(parents=True, exist_ok=True)
        else:
            base_dir = Path(tempfile.gettempdir()) / "jarvis_tts"
            base_dir.mkdir(parents=True, exist_ok=True)
        return base_dir / f"jarvis-neural-tts-{time.time_ns()}{suffix}", retain_output

    @staticmethod
    def _safe_unlink(path: Path, *, keep: bool) -> None:
        if keep:
            return
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

    @staticmethod
    def _extract_audio_bytes(*, raw_bytes: bytes, content_type: str) -> bytes:
        if not raw_bytes:
            return b""
        clean_content_type = str(content_type or "").strip().lower()
        if clean_content_type.startswith("audio/") or "octet-stream" in clean_content_type:
            return raw_bytes
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except Exception:
            return raw_bytes
        candidates: List[str] = []
        if isinstance(payload, dict):
            for key in ("audio_base64", "audio", "b64_json", "output_audio"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value.strip())
            for group in (payload.get("data"), payload.get("output")):
                if isinstance(group, list):
                    for row in group:
                        if not isinstance(row, dict):
                            continue
                        value = row.get("b64_json") or row.get("audio_base64") or row.get("audio")
                        if isinstance(value, str) and value.strip():
                            candidates.append(value.strip())
                            break
        for encoded in candidates:
            try:
                return base64.b64decode(encoded)
            except Exception:
                continue
        return b""
