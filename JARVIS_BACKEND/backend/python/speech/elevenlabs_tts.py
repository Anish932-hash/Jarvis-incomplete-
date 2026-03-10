from __future__ import annotations

import os
import re
import threading
import time
from typing import Any, Dict, Optional

import numpy as np
import requests
import sounddevice as sd

from backend.python.speech.playback_session import PlaybackSessionRegistry


class ElevenLabsTTS:
    """
    Advanced ElevenLabs TTS client with streaming audio output.
    """

    def __init__(self, api_key: str, voice_id: str):
        self.api_key = api_key
        self.voice_id = voice_id
        self.endpoint = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream"
    _session_lock = threading.RLock()
    _active_cancel_event: Optional[threading.Event] = None
    _active_session_id = ""
    _failure_ema = 0.0
    _cooldown_until = 0.0
    _last_error = ""
    _last_success_at = 0.0
    _failure_count = 0
    _success_count = 0
    _segment_latency_ema = 0.0
    _segment_bytes_ema = 0.0
    _session_history: list[Dict[str, Any]] = []
    _session_history_max = 120

    def speak(self, text: str) -> Dict[str, Any]:
        """Synthesize speech with adaptive retries, cooldown, and cooperative cancellation."""
        clean_text = str(text or "").strip()
        if not clean_text:
            return {"status": "error", "message": "text is required"}
        ready = self._provider_ready()
        if not ready["ready"]:
            fallback = self._fallback_to_local_if_enabled(clean_text, reason="provider_cooldown")
            if isinstance(fallback, dict) and fallback.get("status") == "success":
                return fallback
            return {
                "status": "error",
                "message": "ElevenLabs provider is in cooldown due to recent failures.",
                "retry_after_s": ready["retry_after_s"],
                "mode": "elevenlabs",
            }

        max_attempts = max(1, min(int(os.getenv("JARVIS_ELEVENLABS_MAX_RETRIES", "2")), 5))
        segments = self._split_text_segments(clean_text, max_chars=280)
        headers = {
            "xi-api-key": self.api_key,
            "Accept": "audio/mpeg",
            "Content-Type": "application/json"
        }
        selected_model = self._select_model_id(clean_text)
        base_payload = {
            "model_id": selected_model,
            "output_format": str(os.getenv("JARVIS_ELEVENLABS_OUTPUT_FORMAT", "mp3_44100_128")).strip() or "mp3_44100_128",
        }

        cancel_event = threading.Event()
        session = PlaybackSessionRegistry.start(
            channel="tts",
            provider="elevenlabs",
            metadata={"voice_id": self.voice_id, "model_id": base_payload["model_id"], "segments": len(segments)},
            stop_callback=lambda: self._interrupt_active(cancel_event=cancel_event),
        )
        session_id = str(session.get("session_id", "")).strip()
        with self._session_lock:
            self._active_cancel_event = cancel_event
            self._active_session_id = session_id

        try:
            started = time.monotonic()
            spoken_segments = 0
            total_audio_bytes = 0
            for index, segment in enumerate(segments, start=1):
                if cancel_event.is_set():
                    break
                payload = dict(base_payload)
                payload["text"] = segment
                payload["voice_settings"] = self._voice_settings_for_segment(segment)
                result = self._speak_segment(
                    payload=payload,
                    headers=headers,
                    cancel_event=cancel_event,
                    max_attempts=max_attempts,
                )
                if result.get("status") != "success":
                    message = str(result.get("message", "ElevenLabs synthesis failed."))
                    fallback = self._fallback_to_local_if_enabled(clean_text, reason=f"segment_failed:{index}")
                    if isinstance(fallback, dict) and fallback.get("status") == "success":
                        PlaybackSessionRegistry.finish(session_id, status="interrupted", message=f"fallback_local:{message}")
                        fallback["fallback_from"] = "elevenlabs"
                        fallback["fallback_reason"] = message
                        fallback["session_id"] = session_id
                        self._record_session_result(
                            session_id=session_id,
                            model_id=selected_model,
                            status="fallback_local",
                            segments=spoken_segments,
                            total_audio_bytes=total_audio_bytes,
                            duration_s=max(0.0, time.monotonic() - started),
                        )
                        return fallback
                    PlaybackSessionRegistry.finish(session_id, status="error", message=message)
                    self._record_session_result(
                        session_id=session_id,
                        model_id=selected_model,
                        status="error",
                        segments=spoken_segments,
                        total_audio_bytes=total_audio_bytes,
                        duration_s=max(0.0, time.monotonic() - started),
                        error=message,
                    )
                    return {
                        "status": "error",
                        "message": message,
                        "mode": "elevenlabs",
                        "session_id": session_id,
                        "segment_index": index,
                        "segments_total": len(segments),
                    }
                spoken_segments += 1
                total_audio_bytes += int(result.get("audio_bytes", 0) or 0)

            if cancel_event.is_set():
                PlaybackSessionRegistry.finish(session_id, status="interrupted", message="tts_stop_elevenlabs")
                self._record_session_result(
                    session_id=session_id,
                    model_id=selected_model,
                    status="interrupted",
                    segments=spoken_segments,
                    total_audio_bytes=total_audio_bytes,
                    duration_s=max(0.0, time.monotonic() - started),
                )
                return {
                    "status": "success",
                    "text": clean_text,
                    "interrupted": True,
                    "mode": "elevenlabs",
                    "session_id": session_id,
                    "model_id": selected_model,
                    "segments_spoken": spoken_segments,
                    "segments_total": len(segments),
                    "audio_bytes": total_audio_bytes,
                }

            self._mark_provider_success()
            self._record_session_result(
                session_id=session_id,
                model_id=selected_model,
                status="success",
                segments=spoken_segments,
                total_audio_bytes=total_audio_bytes,
                duration_s=max(0.0, time.monotonic() - started),
            )
            PlaybackSessionRegistry.finish(session_id, status="completed", message="")
            return {
                "status": "success",
                "text": clean_text,
                "mode": "elevenlabs",
                "session_id": session_id,
                "model_id": selected_model,
                "segments_spoken": spoken_segments,
                "segments_total": len(segments),
                "audio_bytes": total_audio_bytes,
            }

        except Exception as e:
            self._mark_provider_failure(error=str(e), transient=False)
            PlaybackSessionRegistry.finish(session_id, status="error", message=str(e))
            self._record_session_result(
                session_id=session_id,
                model_id=selected_model,
                status="error",
                segments=0,
                total_audio_bytes=0,
                duration_s=0.0,
                error=str(e),
            )
            return {"status": "error", "message": str(e)}
        finally:
            with self._session_lock:
                if self._active_session_id == session_id:
                    self._active_cancel_event = None
                    self._active_session_id = ""

    def _speak_segment(
        self,
        *,
        payload: Dict[str, Any],
        headers: Dict[str, str],
        cancel_event: threading.Event,
        max_attempts: int,
    ) -> Dict[str, Any]:
        format_fallback_used = False
        for attempt in range(1, max_attempts + 1):
            start = time.monotonic()
            try:
                with requests.post(self.endpoint, json=payload, headers=headers, stream=True, timeout=(8, 50)) as response:
                    status = int(response.status_code or 0)
                    if status == 400 and not format_fallback_used:
                        default_format = "mp3_44100_128"
                        current_format = str(payload.get("output_format", "")).strip()
                        if current_format and current_format != default_format:
                            payload["output_format"] = default_format
                            format_fallback_used = True
                            continue
                    if status in {429, 500, 502, 503, 504}:
                        raise RuntimeError(f"transient_http_{status}")
                    response.raise_for_status()
                    audio_buffer = bytearray()
                    for chunk in response.iter_content(chunk_size=4096):
                        if cancel_event.is_set():
                            return {"status": "cancelled", "message": "cancelled"}
                        if chunk:
                            audio_buffer.extend(chunk)
                if cancel_event.is_set():
                    return {"status": "cancelled", "message": "cancelled"}
                if not audio_buffer:
                    raise RuntimeError("empty_audio_stream")
                self._play(bytes(audio_buffer), cancel_event=cancel_event)
                latency_s = max(0.0, time.monotonic() - start)
                bytes_len = len(audio_buffer)
                self._record_segment_metrics(status="success", latency_s=latency_s, audio_bytes=bytes_len)
                return {"status": "success", "attempt": attempt, "audio_bytes": bytes_len}
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                transient = any(token in message.lower() for token in ("timeout", "transient_http_", "connection", "tempor"))
                self._mark_provider_failure(error=message, transient=transient)
                self._record_segment_metrics(status="error", latency_s=max(0.0, time.monotonic() - start), audio_bytes=0)
                if cancel_event.is_set():
                    return {"status": "cancelled", "message": "cancelled"}
                if attempt >= max_attempts or not transient:
                    return {"status": "error", "message": message}
                time.sleep(min(0.6 * (2 ** (attempt - 1)), 3.5))
        return {"status": "error", "message": "synthesis retries exhausted"}

    def _play(self, audio_bytes: bytes, *, cancel_event: threading.Event) -> None:
        """Decode MP3 to PCM and play it."""
        import io
        from pydub import AudioSegment

        if cancel_event.is_set():
            return
        audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format="mp3")
        pcm = np.array(audio.get_array_of_samples(), dtype=np.int16)

        sd.play(pcm, audio.frame_rate)
        while True:
            if cancel_event.is_set():
                sd.stop()
                return
            stream = sd.get_stream()
            if stream is None or not bool(getattr(stream, "active", False)):
                break
            time.sleep(0.02)
        sd.wait()

    @classmethod
    def _interrupt_active(cls, *, cancel_event: Optional[threading.Event] = None) -> None:
        event = cancel_event
        if event is None:
            with cls._session_lock:
                event = cls._active_cancel_event
        if isinstance(event, threading.Event):
            event.set()
        sd.stop()

    @classmethod
    def _provider_ready(cls) -> Dict[str, Any]:
        now = time.time()
        with cls._session_lock:
            retry_after = max(0.0, float(cls._cooldown_until or 0.0) - now)
            failure_ema = float(cls._failure_ema or 0.0)
            last_error = str(cls._last_error or "").strip()
        return {
            "ready": retry_after <= 0.0,
            "retry_after_s": round(retry_after, 3),
            "failure_ema": round(failure_ema, 6),
            "last_error": last_error,
        }

    @classmethod
    def _mark_provider_success(cls) -> None:
        with cls._session_lock:
            cls._failure_ema = max(0.0, float(cls._failure_ema or 0.0) * 0.72)
            cls._success_count = int(cls._success_count or 0) + 1
            cls._last_success_at = time.time()
            if cls._failure_ema < 0.08:
                cls._cooldown_until = 0.0
                cls._last_error = ""

    @classmethod
    def _mark_provider_failure(cls, *, error: str, transient: bool) -> None:
        now = time.time()
        with cls._session_lock:
            signal = 0.75 if transient else 1.0
            cls._failure_ema = (0.82 * float(cls._failure_ema or 0.0)) + (0.18 * signal)
            cls._last_error = str(error or "").strip()
            cls._failure_count = int(cls._failure_count or 0) + 1
            if cls._failure_ema >= 0.58:
                cooldown = min(300.0, 30.0 + (cls._failure_ema * 180.0))
                cls._cooldown_until = max(float(cls._cooldown_until or 0.0), now + cooldown)

    @classmethod
    def _record_segment_metrics(cls, *, status: str, latency_s: float, audio_bytes: int) -> None:
        clean_status = str(status or "").strip().lower() or "unknown"
        latency = max(0.0, float(latency_s))
        byte_count = max(0, int(audio_bytes))
        with cls._session_lock:
            alpha = 0.22
            current_latency = float(cls._segment_latency_ema or 0.0)
            current_bytes = float(cls._segment_bytes_ema or 0.0)
            cls._segment_latency_ema = latency if current_latency <= 0.0 else ((alpha * latency) + ((1.0 - alpha) * current_latency))
            cls._segment_bytes_ema = float(byte_count) if current_bytes <= 0.0 else ((alpha * float(byte_count)) + ((1.0 - alpha) * current_bytes))
            cls._session_history.append(
                {
                    "kind": "segment",
                    "status": clean_status,
                    "latency_s": round(latency, 6),
                    "audio_bytes": byte_count,
                    "at": time.time(),
                }
            )
            if len(cls._session_history) > cls._session_history_max:
                cls._session_history = cls._session_history[-cls._session_history_max :]

    @classmethod
    def _record_session_result(
        cls,
        *,
        session_id: str,
        model_id: str,
        status: str,
        segments: int,
        total_audio_bytes: int,
        duration_s: float,
        error: str = "",
    ) -> None:
        row = {
            "kind": "session",
            "session_id": str(session_id or "").strip(),
            "model_id": str(model_id or "").strip(),
            "status": str(status or "").strip().lower() or "unknown",
            "segments": max(0, int(segments)),
            "total_audio_bytes": max(0, int(total_audio_bytes)),
            "duration_s": round(max(0.0, float(duration_s)), 6),
            "error": str(error or "").strip(),
            "at": time.time(),
        }
        with cls._session_lock:
            cls._session_history.append(row)
            if len(cls._session_history) > cls._session_history_max:
                cls._session_history = cls._session_history[-cls._session_history_max :]

    @staticmethod
    def _select_model_id(text: str) -> str:
        default_model = str(os.getenv("JARVIS_ELEVENLABS_MODEL_DEFAULT", "eleven_turbo_v2")).strip() or "eleven_turbo_v2"
        dialogue_model = str(os.getenv("JARVIS_ELEVENLABS_MODEL_DIALOGUE", "eleven_turbo_v2")).strip() or default_model
        narration_model = str(os.getenv("JARVIS_ELEVENLABS_MODEL_NARRATION", "eleven_multilingual_v2")).strip() or default_model
        clean = str(text or "").strip()
        token_count = len([token for token in re.split(r"\s+", clean) if token.strip()])
        if token_count >= 90 or len(clean) >= 520:
            return narration_model
        question_marks = clean.count("?")
        exclaim_marks = clean.count("!")
        if question_marks >= 2 or exclaim_marks >= 2:
            return dialogue_model
        return default_model

    @staticmethod
    def _split_text_segments(text: str, *, max_chars: int) -> list[str]:
        clean = str(text or "").strip()
        if len(clean) <= max_chars:
            return [clean] if clean else []
        segments: list[str] = []
        normalized = clean.replace("\r", " ").replace("\n", " ")
        sentence_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", normalized) if part.strip()]
        current = ""
        for part in sentence_parts:
            candidate = f"{current} {part}".strip() if current else part
            if len(candidate) > max_chars and current:
                segments.append(current.strip())
                current = part
            else:
                current = candidate
        if current:
            segments.append(current.strip())
        if not segments:
            return [clean[:max_chars]]
        return segments

    @staticmethod
    def _voice_settings_for_segment(text: str) -> Dict[str, Any]:
        clean = str(text or "").strip()
        length = len(clean)
        question = clean.endswith("?")
        exclaim = clean.endswith("!")
        base_stability = 0.42
        base_similarity = 0.78
        base_style = 0.25
        if question:
            base_style = min(0.45, base_style + 0.1)
            base_stability = max(0.35, base_stability - 0.04)
        if exclaim:
            base_style = min(0.55, base_style + 0.14)
            base_similarity = min(0.86, base_similarity + 0.04)
        if length > 200:
            base_stability = min(0.58, base_stability + 0.08)
            base_style = max(0.18, base_style - 0.05)
        speed = 1.0
        if length <= 32:
            speed = 1.03
        elif length >= 260:
            speed = 0.97
        return {
            "similarity_boost": round(base_similarity, 3),
            "stability": round(base_stability, 3),
            "style": round(base_style, 3),
            "speed": round(speed, 3),
            "use_speaker_boost": True,
        }

    @staticmethod
    def _fallback_enabled() -> bool:
        raw = str(os.getenv("JARVIS_ELEVENLABS_FALLBACK_LOCAL", "1")).strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def _fallback_to_local_if_enabled(self, text: str, *, reason: str) -> Dict[str, Any]:
        if not self._fallback_enabled():
            return {"status": "disabled"}
        try:
            from backend.python.speech.local_tts import LocalTTS

            payload = LocalTTS().speak(text)
            if payload.get("status") == "success":
                payload["mode"] = "local-fallback"
                payload["fallback_reason"] = str(reason or "").strip()
            return payload
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"local fallback failed: {exc}"}

    @classmethod
    def diagnostics(cls) -> Dict[str, Any]:
        with cls._session_lock:
            now = time.time()
            retry_after = max(0.0, float(cls._cooldown_until or 0.0) - now)
            history = list(cls._session_history[-24:])
            return {
                "status": "success",
                "provider": "elevenlabs",
                "ready": retry_after <= 0.0,
                "retry_after_s": round(retry_after, 3),
                "failure_ema": round(float(cls._failure_ema or 0.0), 6),
                "failure_count": int(cls._failure_count or 0),
                "success_count": int(cls._success_count or 0),
                "segment_latency_ema_s": round(float(cls._segment_latency_ema or 0.0), 6),
                "segment_audio_bytes_ema": round(float(cls._segment_bytes_ema or 0.0), 6),
                "last_error": str(cls._last_error or "").strip(),
                "last_success_at": float(cls._last_success_at or 0.0),
                "active_session_id": str(cls._active_session_id or "").strip(),
                "history_tail": history,
            }

    @classmethod
    def stop(cls, *, session_id: str = "") -> Dict[str, Any]:
        clean_session = str(session_id or "").strip()
        interrupted = PlaybackSessionRegistry.interrupt(
            session_id=clean_session,
            channel="tts",
            reason="tts_stop_elevenlabs",
        )
        if bool(interrupted.get("stopped", False)):
            with cls._session_lock:
                event = cls._active_cancel_event
                active_session = str(cls._active_session_id or "").strip()
            if isinstance(event, threading.Event):
                event.set()
            try:
                sd.stop()
            except Exception:
                pass
            return {
                "status": "success",
                "stopped": True,
                "mode": "elevenlabs-sounddevice",
                "session_id": clean_session or active_session,
                "session": interrupted.get("session", {}),
            }
        try:
            cls._interrupt_active()
            sd.stop()
            return {"status": "success", "stopped": True, "mode": "elevenlabs-sounddevice"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "stopped": False, "message": str(exc)}
