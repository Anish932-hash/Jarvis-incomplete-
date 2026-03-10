from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from backend.python.utils.logger import Logger

from .http_client import HttpClient, HttpRequestError


class GroqClient:
    BASE_URL = "https://api.groq.com/openai/v1"
    DEFAULT_MODEL = "llama-3.3-70b-versatile"

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = str(api_key or os.getenv("GROQ_API_KEY", "")).strip()
        self.http = HttpClient(
            timeout=int(os.getenv("JARVIS_GROQ_HTTP_TIMEOUT_S", "35")),
            max_retries=int(os.getenv("JARVIS_GROQ_HTTP_MAX_RETRIES", "3")),
            backoff_factor=float(os.getenv("JARVIS_GROQ_HTTP_BACKOFF_S", "0.75")),
        )
        self.log = Logger.get_logger("GroqClient")

        self._failure_ema = 0.0
        self._failure_streak = 0
        self._cooldown_until_epoch = 0.0
        self._last_error = ""
        self._last_status = 0
        self._requests_total = 0
        self._success_total = 0
        self._error_total = 0
        self._last_request_at = ""

        self._cooldown_base_s = max(1.0, min(float(os.getenv("JARVIS_GROQ_COOLDOWN_BASE_S", "8")), 300.0))
        self._cooldown_max_s = max(
            self._cooldown_base_s,
            min(float(os.getenv("JARVIS_GROQ_COOLDOWN_MAX_S", "180")), 3600.0),
        )
        self._cooldown_streak_threshold = max(2, min(int(os.getenv("JARVIS_GROQ_COOLDOWN_STREAK_THRESHOLD", "3")), 10))
        self._model_fallbacks = self._parse_fallback_models(
            os.getenv("JARVIS_GROQ_MODEL_FALLBACKS", "llama-3.3-70b-versatile,llama3-70b-8192,llama-3.1-8b-instant")
        )

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise RuntimeError("GROQ_API_KEY is not set.")
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def is_ready(self) -> bool:
        if not self.api_key:
            return False
        return time.time() >= float(self._cooldown_until_epoch)

    def diagnostics(self) -> Dict[str, Any]:
        now = time.time()
        retry_after = max(0.0, float(self._cooldown_until_epoch) - now)
        return {
            "provider": "groq",
            "ready": self.is_ready(),
            "api_key_present": bool(self.api_key),
            "failure_ema": round(max(0.0, min(self._failure_ema, 1.0)), 6),
            "failure_streak": int(self._failure_streak),
            "cooldown_until_epoch": float(self._cooldown_until_epoch),
            "retry_after_s": round(retry_after, 3),
            "last_error": str(self._last_error),
            "last_status": int(self._last_status),
            "requests_total": int(self._requests_total),
            "success_total": int(self._success_total),
            "error_total": int(self._error_total),
            "last_request_at": str(self._last_request_at),
            "http_runtime": self.http.runtime_snapshot(),
        }

    async def chat(
        self,
        messages: list,
        model: str = DEFAULT_MODEL,
        stream: bool = True,
        temperature: float = 0.4,
    ) -> AsyncGenerator[str, None]:
        if not self.is_ready():
            yield ""
            return

        payload = {
            "model": str(model or self.DEFAULT_MODEL).strip() or self.DEFAULT_MODEL,
            "messages": self._normalize_messages(messages),
            "temperature": max(0.0, min(float(temperature), 2.0)),
            "stream": bool(stream),
        }
        started = time.monotonic()
        self._record_request_start()
        try:
            if not stream:
                content = await self.ask_from_messages(payload["messages"], model=payload["model"], temperature=float(payload["temperature"]))
                self._record_result(success=bool(content), status_code=200)
                yield content
                return

            sse_buffer = ""
            async for chunk in self.http.request_stream(
                "POST",
                f"{self.BASE_URL}/chat/completions",
                headers=self._headers(),
                json_data=payload,
                timeout_s=float(os.getenv("JARVIS_GROQ_STREAM_TIMEOUT_S", "60")),
            ):
                try:
                    text_chunk = chunk.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                sse_buffer += text_chunk
                while "\n" in sse_buffer:
                    line, sse_buffer = sse_buffer.split("\n", 1)
                    clean_line = line.strip()
                    if not clean_line.startswith("data:"):
                        continue
                    data = clean_line[5:].strip()
                    if data == "[DONE]":
                        self._record_result(success=True, status_code=200)
                        return
                    try:
                        payload_obj = json.loads(data)
                    except Exception:
                        continue
                    delta = self._extract_delta_content(payload_obj)
                    if delta:
                        yield delta
            self._record_result(success=True, status_code=200)
        except Exception as exc:  # noqa: BLE001
            status_code = self._extract_status_code(exc)
            self._record_result(success=False, status_code=status_code, error=str(exc))
            self.log.warning(f"Groq chat streaming failed: {exc}")
            yield ""
        finally:
            _ = (time.monotonic() - started) * 1000.0

    async def ask(
        self,
        prompt: str,
        model: str = DEFAULT_MODEL,
        temperature: float = 0.2,
    ) -> str:
        clean_prompt = str(prompt or "").strip()
        if not clean_prompt or not self.is_ready():
            return ""
        messages = [{"role": "user", "content": clean_prompt}]
        return await self.ask_from_messages(messages, model=model, temperature=temperature)

    async def ask_from_messages(
        self,
        messages: List[Dict[str, Any]],
        *,
        model: str = DEFAULT_MODEL,
        temperature: float = 0.2,
    ) -> str:
        if not self.is_ready():
            return ""

        clean_model = str(model or self.DEFAULT_MODEL).strip() or self.DEFAULT_MODEL
        started = time.monotonic()
        self._record_request_start()
        attempts = [clean_model, *[name for name in self._model_fallbacks if name != clean_model]]

        for model_name in attempts:
            try:
                resp = await self.http.request(
                    "POST",
                    f"{self.BASE_URL}/chat/completions",
                    headers=self._headers(),
                    json_data={
                        "model": model_name,
                        "messages": self._normalize_messages(messages),
                        "stream": False,
                        "temperature": max(0.0, min(float(temperature), 2.0)),
                    },
                    timeout_s=float(os.getenv("JARVIS_GROQ_REQUEST_TIMEOUT_S", "40")),
                )
                content = self._extract_content_from_chat_response(resp)
                if content:
                    self._record_result(success=True, status_code=200)
                    _ = (time.monotonic() - started) * 1000.0
                    return content
            except Exception as exc:  # noqa: BLE001
                status_code = self._extract_status_code(exc)
                self._record_result(success=False, status_code=status_code, error=str(exc))
                self.log.warning(f"Groq ask failed on model={model_name}: {exc}")
                continue

        return ""

    async def reason(self, prompt: str) -> Dict[str, Any]:
        """
        Return structured reasoning output with fallback when API is unavailable.
        """
        content = await self.ask(
            (
                "Return strict JSON with keys intent and arguments.\n"
                "Allowed intents: open_application, search_media, check_security, speak.\n"
                f"User request: {prompt}"
            )
        )
        parsed = self._extract_json(content)
        if parsed:
            return parsed
        return self._fallback_reason(prompt)

    def _record_request_start(self) -> None:
        self._requests_total += 1
        self._last_request_at = datetime.now(timezone.utc).isoformat()

    def _record_result(self, *, success: bool, status_code: int = 0, error: str = "") -> None:
        sample = 0.0 if success else 1.0
        alpha = 0.24
        self._failure_ema = ((1.0 - alpha) * self._failure_ema) + (alpha * sample)
        self._last_status = int(status_code or 0)
        self._last_error = str(error or "")
        if success:
            self._success_total += 1
            self._failure_streak = 0
            self._cooldown_until_epoch = 0.0
            return

        self._error_total += 1
        self._failure_streak += 1
        if self._failure_streak >= self._cooldown_streak_threshold:
            multiplier = 2 ** max(0, self._failure_streak - self._cooldown_streak_threshold)
            cooldown_s = min(self._cooldown_max_s, self._cooldown_base_s * float(multiplier))
            self._cooldown_until_epoch = max(self._cooldown_until_epoch, time.time() + cooldown_s)

    @staticmethod
    def _normalize_messages(messages: list) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in messages:
            if not isinstance(row, dict):
                continue
            role = str(row.get("role", "")).strip().lower()
            content = row.get("content", "")
            if role not in {"system", "user", "assistant", "tool"}:
                continue
            if isinstance(content, list):
                normalized.append({"role": role, "content": content})
                continue
            normalized.append({"role": role, "content": str(content)})
        return normalized or [{"role": "user", "content": ""}]

    @staticmethod
    def _extract_delta_content(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        choices = payload.get("choices", [])
        if not isinstance(choices, list) or not choices:
            return ""
        first = choices[0] if isinstance(choices[0], dict) else {}
        delta = first.get("delta", {}) if isinstance(first, dict) else {}
        if isinstance(delta, dict):
            content = delta.get("content", "")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict) and isinstance(item.get("text"), str):
                        parts.append(item["text"])
                return "".join(parts)
        return ""

    @classmethod
    def _extract_content_from_chat_response(cls, payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        choices = payload.get("choices", [])
        if not isinstance(choices, list) or not choices:
            return ""
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message", {}) if isinstance(first, dict) else {}
        content = message.get("content", "")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts: List[str] = []
            for row in content:
                if isinstance(row, dict) and isinstance(row.get("text"), str):
                    parts.append(row["text"])
            return "".join(parts).strip()
        text = first.get("text", "")
        if isinstance(text, str):
            return text.strip()
        return ""

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        if not text:
            return {}
        text = text.strip()
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {}
        try:
            obj = json.loads(match.group(0))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _fallback_reason(prompt: str) -> Dict[str, Any]:
        lowered = prompt.lower()
        if any(k in lowered for k in ("open ", "launch ", "start app")):
            app_name = "notepad"
            m = re.search(r"(?:open|launch|start app)\s+(.+)$", prompt, flags=re.IGNORECASE)
            if m:
                app_name = m.group(1).strip().strip(".")
            return {"intent": "open_application", "arguments": {"app": app_name}}
        if any(k in lowered for k in ("security", "defender", "virus")):
            return {"intent": "check_security", "arguments": {}}
        if any(k in lowered for k in ("play", "search", "music", "youtube")):
            return {"intent": "search_media", "arguments": {"query": prompt}}
        return {"intent": "speak", "arguments": {"text": "Acknowledged."}}

    @staticmethod
    def _extract_status_code(exc: Exception) -> int:
        if isinstance(exc, HttpRequestError):
            return int(exc.status_code or 0)
        status = getattr(exc, "status", None)
        try:
            if status is not None:
                return int(status)
        except Exception:
            pass
        return 0

    @staticmethod
    def _parse_fallback_models(raw: str) -> List[str]:
        rows: List[str] = []
        for item in str(raw or "").split(","):
            clean = str(item or "").strip()
            if clean and clean not in rows:
                rows.append(clean)
        if not rows:
            rows.append(GroqClient.DEFAULT_MODEL)
        return rows
