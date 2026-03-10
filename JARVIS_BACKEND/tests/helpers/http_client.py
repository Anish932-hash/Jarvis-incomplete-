from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, Tuple


def request_json(
    method: str,
    url: str,
    payload: Dict[str, Any] | None = None,
    timeout_s: float = 8.0,
) -> Tuple[int, Dict[str, Any]]:
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=data,
        method=method.upper(),
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8") or "{}"
            body = json.loads(raw)
            if not isinstance(body, dict):
                body = {"data": body}
            return response.getcode(), body
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8") or "{}"
        try:
            body = json.loads(raw)
        except Exception:  # noqa: BLE001
            body = {"status": "error", "message": raw}
        if not isinstance(body, dict):
            body = {"data": body}
        return exc.code, body

