from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from backend.python.tools.browser_tools import BrowserTools


class _PageHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = (
            "<html><head><title>JARVIS Test</title></head>"
            "<body><h1>Hello Agent</h1>"
            '<a href="/a">A</a>'
            '<a href="https://example.com/out">External</a>'
            "</body></html>"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def _run_server() -> tuple[str, ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _PageHandler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    host, port = server.server_address
    return (f"http://{host}:{port}", server, thread)


def test_read_dom_from_local_page(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_BROWSER_ALLOW_PRIVATE_HOSTS", "1")
    base_url, server, thread = _run_server()
    try:
        result = BrowserTools.read_dom(base_url, max_chars=1000)
        assert result["status"] == "success"
        assert result["title"] == "JARVIS Test"
        assert "Hello Agent" in result["text"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_extract_links_same_domain_filter(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_BROWSER_ALLOW_PRIVATE_HOSTS", "1")
    base_url, server, thread = _run_server()
    try:
        result = BrowserTools.extract_links(base_url, max_links=20, same_domain_only=True)
        assert result["status"] == "success"
        assert result["count"] == 1
        assert result["links"][0].startswith(base_url)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
