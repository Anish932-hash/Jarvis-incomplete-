from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.python.utils.logger import Logger

from .http_client import HttpClient, HttpRequestError


class NvidiaClient:
    BASE_URL = "https://integrate.api.nvidia.com/v1"

    def __init__(self, api_key: str):
        self.api_key = str(api_key or "").strip()
        self.http = HttpClient(
            timeout=int(os.getenv("JARVIS_NVIDIA_HTTP_TIMEOUT_S", "40")),
            max_retries=int(os.getenv("JARVIS_NVIDIA_HTTP_MAX_RETRIES", "3")),
            backoff_factor=float(os.getenv("JARVIS_NVIDIA_HTTP_BACKOFF_S", "0.8")),
        )
        self.log = Logger.get_logger("NvidiaClient")
        self._failure_ema = 0.0
        self._failure_streak = 0
        self._cooldown_until_epoch = 0.0
        self._last_error = ""
        self._last_status = 0
        self._requests_total = 0
        self._success_total = 0
        self._error_total = 0
        self._last_request_at = ""
        self._cooldown_base_s = max(1.0, min(float(os.getenv("JARVIS_NVIDIA_COOLDOWN_BASE_S", "8")), 300.0))
        self._cooldown_max_s = max(
            self._cooldown_base_s,
            min(float(os.getenv("JARVIS_NVIDIA_COOLDOWN_MAX_S", "240")), 3600.0),
        )
        self._cooldown_streak_threshold = max(2, min(int(os.getenv("JARVIS_NVIDIA_COOLDOWN_STREAK_THRESHOLD", "3")), 10))

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise RuntimeError("NVIDIA_API_KEY is not set.")
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
            "provider": "nvidia",
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

    async def generate_text(
        self,
        prompt: str,
        model: str = "meta/llama-3.1-70b-instruct",
        max_tokens: int = 1024,
        *,
        temperature: float = 0.1,
    ) -> str:
        clean_prompt = str(prompt or "").strip()
        if not clean_prompt or not self.is_ready():
            return ""

        bounded_tokens = max(16, min(int(max_tokens), 8192))
        bounded_temp = max(0.0, min(float(temperature), 2.0))
        clean_model = str(model or "meta/llama-3.1-70b-instruct").strip() or "meta/llama-3.1-70b-instruct"
        self._record_request_start()

        attempts = [
            {
                "url": f"{self.BASE_URL}/chat/completions",
                "payload": {
                    "model": clean_model,
                    "messages": [{"role": "user", "content": clean_prompt}],
                    "max_tokens": bounded_tokens,
                    "temperature": bounded_temp,
                    "stream": False,
                },
            },
            {
                "url": f"{self.BASE_URL}/text/generate",
                "payload": {
                    "model": clean_model,
                    "input": clean_prompt,
                    "max_output_tokens": bounded_tokens,
                    "temperature": bounded_temp,
                },
            },
        ]

        for attempt in attempts:
            try:
                resp = await self.http.request(
                    "POST",
                    attempt["url"],
                    headers=self._headers(),
                    json_data=attempt["payload"],
                    timeout_s=float(os.getenv("JARVIS_NVIDIA_REQUEST_TIMEOUT_S", "45")),
                )
                text = self._extract_text_response(resp)
                if text:
                    self._record_result(success=True, status_code=200)
                    return text
            except Exception as exc:  # noqa: BLE001
                status_code = self._extract_status_code(exc)
                self._record_result(success=False, status_code=status_code, error=str(exc))
                self.log.warning(f"NVIDIA generate_text attempt failed ({attempt['url']}): {exc}")
                continue
        return ""

    async def embed(self, text: str, model: str = "nvidia-embed-qa") -> Optional[list]:
        clean_text = str(text or "").strip()
        if not clean_text or not self.is_ready():
            return None

        self._record_request_start()
        try:
            resp = await self.http.request(
                "POST",
                f"{self.BASE_URL}/embeddings",
                headers=self._headers(),
                json_data={"model": str(model or "nvidia-embed-qa").strip(), "input": clean_text},
                timeout_s=float(os.getenv("JARVIS_NVIDIA_EMBED_TIMEOUT_S", "45")),
            )
            embeddings = self._extract_embeddings(resp)
            self._record_result(success=bool(embeddings), status_code=200)
            return embeddings
        except Exception as exc:  # noqa: BLE001
            status_code = self._extract_status_code(exc)
            self._record_result(success=False, status_code=status_code, error=str(exc))
            self.log.warning(f"NVIDIA embeddings failed: {exc}")
            return None

    async def vision_analyze(self, image_b64: str, model: str = "nvidia-vision-base") -> Dict[str, Any]:
        clean_image = str(image_b64 or "").strip()
        if not clean_image or not self.is_ready():
            return {"status": "error", "message": "image is required or provider unavailable"}

        self._record_request_start()
        try:
            resp = await self.http.request(
                "POST",
                f"{self.BASE_URL}/vision/analyze",
                headers=self._headers(),
                json_data={"model": str(model or "nvidia-vision-base").strip(), "image": clean_image},
                timeout_s=float(os.getenv("JARVIS_NVIDIA_VISION_TIMEOUT_S", "55")),
            )
            payload = resp if isinstance(resp, dict) else {"status": "success", "raw": resp}
            self._record_result(success=True, status_code=200)
            return payload
        except Exception as exc:  # noqa: BLE001
            status_code = self._extract_status_code(exc)
            self._record_result(success=False, status_code=status_code, error=str(exc))
            self.log.warning(f"NVIDIA vision analyze failed: {exc}")
            return {"status": "error", "message": str(exc)}

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
    def _extract_text_response(resp: Any) -> str:
        if isinstance(resp, str):
            return resp.strip()
        if not isinstance(resp, dict):
            return ""

        choices = resp.get("choices", [])
        if isinstance(choices, list) and choices:
            first = choices[0] if isinstance(choices[0], dict) else {}
            msg = first.get("message", {}) if isinstance(first, dict) else {}
            content = msg.get("content", "") if isinstance(msg, dict) else ""
            if isinstance(content, str) and content.strip():
                return content.strip()
            text = first.get("text", "") if isinstance(first, dict) else ""
            if isinstance(text, str) and text.strip():
                return text.strip()

        output_text = resp.get("output_text", "")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        data = resp.get("data", {})
        if isinstance(data, dict):
            for key in ("output_text", "text", "content"):
                value = data.get(key, "")
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    @staticmethod
    def _extract_embeddings(resp: Any) -> Optional[list]:
        if not isinstance(resp, dict):
            return None
        direct = resp.get("embeddings")
        if isinstance(direct, list):
            return direct
        data = resp.get("data")
        if isinstance(data, list):
            return data
        return None

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
