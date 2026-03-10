from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, AsyncGenerator, Dict, Iterable, Optional

import aiohttp

from backend.python.utils.logger import Logger


@dataclass(slots=True)
class HttpRequestError(RuntimeError):
    method: str
    url: str
    status_code: int
    message: str
    retryable: bool = False
    response_body: str = ""
    attempt: int = 0

    def __str__(self) -> str:
        hint = f"{self.method} {self.url} status={self.status_code} retryable={self.retryable}"
        if self.message:
            return f"{self.message} ({hint})"
        return hint


class HttpClient:
    """
    Async HTTP client with adaptive retries, jittered backoff, and runtime diagnostics.

    Backward compatible entry points:
    - request(method, url, headers=None, json_data=None)
    - request_stream(method, url, headers=None, json_data=None)
    """

    _DEFAULT_RETRYABLE_STATUS = frozenset({408, 409, 425, 429, 500, 502, 503, 504})

    def __init__(
        self,
        timeout: int = 20,
        max_retries: int = 3,
        backoff_factor: float = 0.8,
        *,
        jitter_ratio: float = 0.22,
        max_backoff_s: float = 20.0,
        retryable_statuses: Optional[Iterable[int]] = None,
    ) -> None:
        self.timeout_s = max(1.0, min(float(timeout), 300.0))
        self.max_retries = max(1, min(int(max_retries), 12))
        self.backoff_factor = max(0.05, min(float(backoff_factor), 20.0))
        self.jitter_ratio = max(0.0, min(float(jitter_ratio), 0.8))
        self.max_backoff_s = max(0.2, min(float(max_backoff_s), 180.0))
        self.retryable_statuses = set(self._DEFAULT_RETRYABLE_STATUS)
        if retryable_statuses is not None:
            self.retryable_statuses = {int(code) for code in retryable_statuses}

        self.log = Logger.get_logger("HttpClient")
        self._runtime: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_error": 0,
            "retries_total": 0,
            "latency_ema_ms": 0.0,
            "last_error": "",
            "last_status": 0,
            "last_request_at": "",
        }

    async def _retry_wait(self, wait_s: float) -> None:
        await asyncio.sleep(max(0.0, wait_s))

    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        *,
        data: Any = None,
        params: Optional[Dict[str, Any]] = None,
        timeout_s: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_for_statuses: Optional[Iterable[int]] = None,
        allow_text: bool = True,
    ) -> Any:
        clean_method = str(method or "").strip().upper() or "GET"
        clean_url = str(url or "").strip()
        if not clean_url:
            raise ValueError("url is required")

        bounded_retries = self._bounded_retries(max_retries)
        retry_statuses = (
            {int(code) for code in retry_for_statuses}
            if retry_for_statuses is not None
            else set(self.retryable_statuses)
        )
        timeout_value = self._bounded_timeout(timeout_s)
        timeout_obj = aiohttp.ClientTimeout(total=timeout_value)
        request_started = time.monotonic()
        last_error: Optional[Exception] = None

        for attempt in range(1, bounded_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                    async with session.request(
                        method=clean_method,
                        url=clean_url,
                        headers=headers,
                        json=json_data,
                        data=data,
                        params=params,
                    ) as response:
                        status_code = int(response.status or 0)
                        body_text = await response.text()
                        if status_code >= 400:
                            retryable = self._is_retryable_status(status_code, retry_statuses)
                            error = HttpRequestError(
                                method=clean_method,
                                url=clean_url,
                                status_code=status_code,
                                message=f"HTTP request failed with status {status_code}",
                                retryable=retryable,
                                response_body=self._truncate(body_text),
                                attempt=attempt,
                            )
                            if retryable and attempt < bounded_retries:
                                self._record_retry(status_code=status_code, message=error.message)
                                await self._retry_wait(
                                    self._compute_backoff_s(
                                        attempt=attempt,
                                        retry_after_header=str(response.headers.get("Retry-After", "")).strip(),
                                    )
                                )
                                continue
                            raise error

                        payload = self._parse_payload(
                            text=body_text,
                            content_type=str(response.headers.get("Content-Type", "")).strip().lower(),
                            allow_text=allow_text,
                        )
                        self._record_success(
                            status_code=status_code,
                            latency_ms=(time.monotonic() - request_started) * 1000.0,
                        )
                        return payload
            except HttpRequestError as exc:
                last_error = exc
                if exc.retryable and attempt < bounded_retries:
                    self._record_retry(status_code=exc.status_code, message=exc.message)
                    await self._retry_wait(self._compute_backoff_s(attempt=attempt))
                    continue
                self._record_error(message=str(exc), status_code=exc.status_code)
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                retryable = True
                if attempt < bounded_retries:
                    self._record_retry(status_code=0, message=str(exc))
                    await self._retry_wait(self._compute_backoff_s(attempt=attempt))
                    continue
                self._record_error(message=str(exc), status_code=0)
                raise HttpRequestError(
                    method=clean_method,
                    url=clean_url,
                    status_code=0,
                    message=f"HTTP transport error: {exc}",
                    retryable=retryable,
                    response_body="",
                    attempt=attempt,
                ) from exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self._record_error(message=str(exc), status_code=0)
                raise

        message = f"HTTP request exhausted retries: {clean_method} {clean_url}"
        if last_error is not None:
            message = f"{message} ({last_error})"
        raise RuntimeError(message)

    async def request_stream(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout_s: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_for_statuses: Optional[Iterable[int]] = None,
    ) -> AsyncGenerator[bytes, None]:
        clean_method = str(method or "").strip().upper() or "GET"
        clean_url = str(url or "").strip()
        if not clean_url:
            raise ValueError("url is required")

        bounded_retries = self._bounded_retries(max_retries)
        retry_statuses = (
            {int(code) for code in retry_for_statuses}
            if retry_for_statuses is not None
            else set(self.retryable_statuses)
        )
        timeout_value = self._bounded_timeout(timeout_s)
        timeout_obj = aiohttp.ClientTimeout(total=timeout_value)
        request_started = time.monotonic()

        for attempt in range(1, bounded_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                    async with session.request(
                        method=clean_method,
                        url=clean_url,
                        headers=headers,
                        json=json_data,
                        params=params,
                    ) as response:
                        status_code = int(response.status or 0)
                        if status_code >= 400:
                            body_text = await response.text()
                            retryable = self._is_retryable_status(status_code, retry_statuses)
                            error = HttpRequestError(
                                method=clean_method,
                                url=clean_url,
                                status_code=status_code,
                                message=f"HTTP stream failed with status {status_code}",
                                retryable=retryable,
                                response_body=self._truncate(body_text),
                                attempt=attempt,
                            )
                            if retryable and attempt < bounded_retries:
                                self._record_retry(status_code=status_code, message=error.message)
                                await self._retry_wait(
                                    self._compute_backoff_s(
                                        attempt=attempt,
                                        retry_after_header=str(response.headers.get("Retry-After", "")).strip(),
                                    )
                                )
                                continue
                            raise error

                        async for chunk in response.content.iter_any():
                            if chunk:
                                yield chunk
                        self._record_success(
                            status_code=status_code,
                            latency_ms=(time.monotonic() - request_started) * 1000.0,
                        )
                        return
            except HttpRequestError as exc:
                if exc.retryable and attempt < bounded_retries:
                    self._record_retry(status_code=exc.status_code, message=exc.message)
                    await self._retry_wait(self._compute_backoff_s(attempt=attempt))
                    continue
                self._record_error(message=str(exc), status_code=exc.status_code)
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < bounded_retries:
                    self._record_retry(status_code=0, message=str(exc))
                    await self._retry_wait(self._compute_backoff_s(attempt=attempt))
                    continue
                self._record_error(message=str(exc), status_code=0)
                raise HttpRequestError(
                    method=clean_method,
                    url=clean_url,
                    status_code=0,
                    message=f"HTTP stream transport error: {exc}",
                    retryable=False,
                    response_body="",
                    attempt=attempt,
                ) from exc

        raise RuntimeError(f"HTTP stream request failed after retries: {clean_method} {clean_url}")

    def runtime_snapshot(self) -> Dict[str, Any]:
        return dict(self._runtime)

    @staticmethod
    def _parse_payload(*, text: str, content_type: str, allow_text: bool) -> Any:
        clean_text = str(text or "")
        lower_content_type = str(content_type or "").lower()
        if "application/json" in lower_content_type or clean_text.strip().startswith("{") or clean_text.strip().startswith("["):
            try:
                return json.loads(clean_text)
            except Exception:
                pass
        if allow_text:
            return clean_text
        return {}

    def _compute_backoff_s(self, *, attempt: int, retry_after_header: str = "") -> float:
        retry_after_s = self._parse_retry_after_header(retry_after_header)
        if retry_after_s is not None:
            base = retry_after_s
        else:
            base = min(self.max_backoff_s, self.backoff_factor * (2 ** max(0, attempt - 1)))
        jitter = 1.0 + random.uniform(-self.jitter_ratio, self.jitter_ratio)
        return max(0.0, min(self.max_backoff_s, base * jitter))

    @staticmethod
    def _parse_retry_after_header(raw: str) -> Optional[float]:
        value = str(raw or "").strip()
        if not value:
            return None
        try:
            seconds = float(value)
            return max(0.0, min(seconds, 300.0))
        except Exception:
            pass
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            delta = (when - datetime.now(timezone.utc)).total_seconds()
            return max(0.0, min(delta, 300.0))
        except Exception:
            return None

    @staticmethod
    def _is_retryable_status(status_code: int, retry_statuses: Iterable[int]) -> bool:
        try:
            code = int(status_code)
        except Exception:
            return False
        return code in {int(item) for item in retry_statuses}

    def _record_success(self, *, status_code: int, latency_ms: float) -> None:
        self._runtime["requests_total"] = int(self._runtime.get("requests_total", 0) or 0) + 1
        self._runtime["requests_success"] = int(self._runtime.get("requests_success", 0) or 0) + 1
        self._runtime["last_status"] = int(status_code)
        self._runtime["last_error"] = ""
        self._runtime["last_request_at"] = datetime.now(timezone.utc).isoformat()
        previous = float(self._runtime.get("latency_ema_ms", 0.0) or 0.0)
        alpha = 0.24
        self._runtime["latency_ema_ms"] = (previous * (1.0 - alpha)) + (max(0.0, float(latency_ms)) * alpha)

    def _record_error(self, *, message: str, status_code: int) -> None:
        self._runtime["requests_total"] = int(self._runtime.get("requests_total", 0) or 0) + 1
        self._runtime["requests_error"] = int(self._runtime.get("requests_error", 0) or 0) + 1
        self._runtime["last_status"] = int(status_code)
        self._runtime["last_error"] = str(message or "")
        self._runtime["last_request_at"] = datetime.now(timezone.utc).isoformat()
        self.log.warning(f"HTTP client error: status={status_code} message={message}")

    def _record_retry(self, *, status_code: int, message: str) -> None:
        self._runtime["retries_total"] = int(self._runtime.get("retries_total", 0) or 0) + 1
        self._runtime["last_status"] = int(status_code)
        self._runtime["last_error"] = str(message or "")

    def _bounded_timeout(self, timeout_s: Optional[float]) -> float:
        if timeout_s is None:
            return self.timeout_s
        return max(1.0, min(float(timeout_s), 300.0))

    def _bounded_retries(self, retries: Optional[int]) -> int:
        if retries is None:
            return self.max_retries
        return max(1, min(int(retries), 12))

    @staticmethod
    def _truncate(text: str, limit: int = 400) -> str:
        clean = str(text or "")
        if len(clean) <= limit:
            return clean
        return f"{clean[:limit]}...[truncated]"
