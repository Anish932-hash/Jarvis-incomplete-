from __future__ import annotations

import ipaddress
import os
import socket
from html.parser import HTMLParser
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


class _DOMInspector(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self._in_title = False
        self._ignore_depth = 0
        self._title_parts: List[str] = []
        self._text_parts: List[str] = []
        self._links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "noscript"}:
            self._ignore_depth += 1
            return
        if normalized == "title":
            self._in_title = True
            return
        if normalized == "a":
            attr_map = {str(key).lower(): value for key, value in attrs}
            href = attr_map.get("href")
            if isinstance(href, str) and href.strip():
                self._links.append(urljoin(self.base_url, href.strip()))

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "noscript"} and self._ignore_depth > 0:
            self._ignore_depth -= 1
            return
        if normalized == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._ignore_depth > 0:
            return
        value = data.strip()
        if not value:
            return
        if self._in_title:
            self._title_parts.append(value)
        else:
            self._text_parts.append(value)

    def export(self) -> Dict[str, Any]:
        title = " ".join(self._title_parts).strip()
        text = " ".join(self._text_parts).strip()
        links: List[str] = []
        seen = set()
        for item in self._links:
            if item in seen:
                continue
            seen.add(item)
            links.append(item)
        return {"title": title, "text": text, "links": links}


class BrowserTools:
    USER_AGENT = "JARVIS-Desktop-Agent/1.0"

    @staticmethod
    def normalize_url(raw_url: str) -> str:
        value = str(raw_url or "").strip()
        if not value:
            return ""
        if value.startswith(("http://", "https://")):
            return value
        return f"https://{value}"

    @staticmethod
    def _allowed_domains() -> List[str]:
        raw = os.getenv("JARVIS_BROWSER_ALLOWED_DOMAINS", "")
        if not raw.strip():
            return []
        values = [item.strip().lower() for item in raw.split(",")]
        return [item for item in values if item]

    @staticmethod
    def _is_private_host(hostname: str) -> bool:
        host = hostname.strip().lower()
        if not host:
            return True
        if host in {"localhost", "127.0.0.1", "::1"}:
            return True
        try:
            parsed_ip = ipaddress.ip_address(host)
            return bool(parsed_ip.is_private or parsed_ip.is_loopback or parsed_ip.is_link_local)
        except Exception:
            pass

        try:
            resolved_ip = socket.gethostbyname(host)
            parsed_ip = ipaddress.ip_address(resolved_ip)
            return bool(parsed_ip.is_private or parsed_ip.is_loopback or parsed_ip.is_link_local)
        except Exception:
            return False

    @staticmethod
    def validate_url(raw_url: str) -> Tuple[bool, str]:
        url = BrowserTools.normalize_url(raw_url)
        if not url:
            return (False, "url is required")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return (False, "Only http/https URLs are allowed")
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            return (False, "URL hostname is required")

        allow_private = os.getenv("JARVIS_BROWSER_ALLOW_PRIVATE_HOSTS", "0") == "1"
        if not allow_private and BrowserTools._is_private_host(host):
            return (False, "Private/local hosts are blocked by policy")

        allowed_domains = BrowserTools._allowed_domains()
        if allowed_domains:
            if not any(host == domain or host.endswith(f".{domain}") for domain in allowed_domains):
                return (False, f"Hostname '{host}' is not in JARVIS_BROWSER_ALLOWED_DOMAINS")
        return (True, url)

    @staticmethod
    def fetch_html(url: str, timeout_s: float = 10.0, max_bytes: int = 1_500_000) -> str:
        safe_timeout = max(1.0, min(float(timeout_s), 30.0))
        safe_limit = max(8_192, min(int(max_bytes), 5_000_000))
        req = Request(
            url,
            headers={
                "User-Agent": BrowserTools.USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.2",
            },
        )
        with urlopen(req, timeout=safe_timeout) as response:  # noqa: S310
            data = response.read(safe_limit + 1)
            content_type = str(response.headers.get("Content-Type", "")).lower()
        if len(data) > safe_limit:
            raise RuntimeError(f"response exceeds max_bytes={safe_limit}")
        if "html" not in content_type and "xml" not in content_type and content_type:
            raise RuntimeError(f"Unsupported content type: {content_type}")
        return data.decode("utf-8", errors="replace")

    @staticmethod
    def read_dom(url: str, *, max_chars: int = 5000, timeout_s: float = 10.0) -> Dict[str, Any]:
        ok, value = BrowserTools.validate_url(url)
        if not ok:
            raise ValueError(value)
        safe_url = value
        html = BrowserTools.fetch_html(safe_url, timeout_s=timeout_s)
        parser = _DOMInspector(base_url=safe_url)
        parser.feed(html)
        exported = parser.export()

        title = str(exported.get("title", "")).strip()
        text = str(exported.get("text", "")).strip()
        bounded = max(256, min(int(max_chars), 50_000))
        truncated = len(text) > bounded
        text_output = text[:bounded] if truncated else text

        return {
            "status": "success",
            "url": safe_url,
            "title": title,
            "text": text_output,
            "chars": len(text_output),
            "truncated": truncated,
        }

    @staticmethod
    def extract_links(
        url: str,
        *,
        max_links: int = 50,
        same_domain_only: bool = False,
        timeout_s: float = 10.0,
    ) -> Dict[str, Any]:
        ok, value = BrowserTools.validate_url(url)
        if not ok:
            raise ValueError(value)
        safe_url = value
        parsed_base = urlparse(safe_url)
        base_host = str(parsed_base.hostname or "").strip().lower()

        html = BrowserTools.fetch_html(safe_url, timeout_s=timeout_s)
        parser = _DOMInspector(base_url=safe_url)
        parser.feed(html)
        exported = parser.export()

        links = exported.get("links", [])
        if not isinstance(links, list):
            links = []
        filtered: List[str] = []
        for item in links:
            if not isinstance(item, str):
                continue
            candidate = item.strip()
            if not candidate:
                continue
            if same_domain_only:
                host = str(urlparse(candidate).hostname or "").strip().lower()
                if host != base_host:
                    continue
            filtered.append(candidate)

        bounded = max(1, min(int(max_links), 500))
        return {
            "status": "success",
            "url": safe_url,
            "same_domain_only": bool(same_domain_only),
            "links": filtered[:bounded],
            "count": min(len(filtered), bounded),
            "truncated": len(filtered) > bounded,
        }
