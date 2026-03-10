from __future__ import annotations

from backend.python.api.groq_client import GroqClient
from backend.python.api.http_client import HttpClient
from backend.python.api.nvidia_client import NvidiaClient


def test_http_client_retry_after_and_retryable_status() -> None:
    client = HttpClient()
    retry_after = client._parse_retry_after_header("5")  # noqa: SLF001
    assert retry_after is not None
    assert retry_after >= 4.9
    assert HttpClient._is_retryable_status(429, {429, 500}) is True
    assert HttpClient._is_retryable_status(400, {429, 500}) is False


def test_groq_extract_content_and_delta_parsers() -> None:
    payload = {
        "choices": [
            {
                "message": {
                    "content": [
                        {"type": "text", "text": "hello"},
                        {"type": "text", "text": " world"},
                    ]
                }
            }
        ]
    }
    assert GroqClient._extract_content_from_chat_response(payload) == "hello world"  # noqa: SLF001

    delta_payload = {"choices": [{"delta": {"content": "chunk"}}]}
    assert GroqClient._extract_delta_content(delta_payload) == "chunk"  # noqa: SLF001


def test_groq_diagnostics_reports_cooldown_state() -> None:
    client = GroqClient(api_key="gsk_test_key_12345678901234567890")
    for _ in range(client._cooldown_streak_threshold + 1):  # noqa: SLF001
        client._record_result(success=False, status_code=503, error="provider unavailable")  # noqa: SLF001
    diag = client.diagnostics()
    assert diag["api_key_present"] is True
    assert diag["failure_streak"] >= client._cooldown_streak_threshold  # noqa: SLF001
    assert float(diag["retry_after_s"]) >= 0.0


def test_nvidia_extract_text_response_variants() -> None:
    client = NvidiaClient(api_key="nvapi-test-key")
    chat_payload = {
        "choices": [
            {
                "message": {"content": "chat-response"},
            }
        ]
    }
    assert client._extract_text_response(chat_payload) == "chat-response"  # noqa: SLF001

    text_payload = {"output_text": "generated-text"}
    assert client._extract_text_response(text_payload) == "generated-text"  # noqa: SLF001


def test_nvidia_extract_embeddings_payloads() -> None:
    client = NvidiaClient(api_key="nvapi-test-key")
    payload_direct = {"embeddings": [[0.1, 0.2], [0.3, 0.4]]}
    payload_data = {"data": [[0.1, 0.2]]}
    assert client._extract_embeddings(payload_direct) == [[0.1, 0.2], [0.3, 0.4]]  # noqa: SLF001
    assert client._extract_embeddings(payload_data) == [[0.1, 0.2]]  # noqa: SLF001
