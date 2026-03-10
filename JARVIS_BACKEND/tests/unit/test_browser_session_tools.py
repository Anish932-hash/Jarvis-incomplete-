from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from backend.python.core.oauth_token_store import OAuthTokenStore
from backend.python.tools.browser_session_tools import BrowserSessionTools


class _AuthPageHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        auth = str(self.headers.get("Authorization", ""))
        if auth != "Bearer token-abc":
            body = b"unauthorized"
            self.send_response(401)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        body = (
            "<html><head><title>Secure Area</title></head>"
            "<body><h1>Welcome</h1>"
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


class _CookieSessionHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/set":
            body = b"cookie-set"
            self.send_response(200)
            self.send_header("Set-Cookie", "session=abc123; Path=/")
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/check":
            cookie = str(self.headers.get("Cookie", ""))
            if "session=abc123" in cookie:
                body = b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = b"missing-cookie"
            self.send_response(401)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        body = b"not-found"
        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def _run_server() -> tuple[str, ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _AuthPageHandler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    host, port = server.server_address
    return (f"http://{host}:{port}", server, thread)


def _run_cookie_server() -> tuple[str, ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CookieSessionHandler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    host, port = server.server_address
    return (f"http://{host}:{port}", server, thread)


def test_browser_session_oauth_request_dom_and_links(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JARVIS_BROWSER_ALLOW_PRIVATE_HOSTS", "1")
    store = OAuthTokenStore.shared(store_path=str(tmp_path / "oauth_tokens.json"))
    store.upsert(
        provider="google",
        account_id="default",
        access_token="token-abc",
        refresh_token="refresh-abc",
        expires_in_s=3600,
    )

    base_url, server, thread = _run_server()
    try:
        created = BrowserSessionTools.create_session(
            {
                "name": "secure-session",
                "base_url": base_url,
                "oauth_provider": "google",
                "oauth_account_id": "default",
            }
        )
        assert created["status"] == "success"
        session_id = created["session"]["session_id"]

        requested = BrowserSessionTools.request({"session_id": session_id, "url": f"{base_url}/secure"})
        assert requested["status"] == "success"
        assert requested["response"]["status_code"] == 200

        dom = BrowserSessionTools.read_dom({"session_id": session_id, "url": f"{base_url}/secure", "max_chars": 2000})
        assert dom["status"] == "success"
        assert dom["title"] == "Secure Area"
        assert "Welcome" in dom["text"]

        links = BrowserSessionTools.extract_links(
            {"session_id": session_id, "url": f"{base_url}/secure", "same_domain_only": True, "max_links": 20}
        )
        assert links["status"] == "success"
        assert links["count"] == 1
        assert links["links"][0].startswith(base_url)
    finally:
        sessions = BrowserSessionTools.list_sessions().get("items", [])
        for item in sessions:
            session_id = str(item.get("session_id", ""))
            if session_id:
                BrowserSessionTools.close_session({"session_id": session_id})
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_browser_session_persists_cookie_across_runtime_reset(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JARVIS_BROWSER_ALLOW_PRIVATE_HOSTS", "1")
    monkeypatch.setenv("JARVIS_BROWSER_SESSION_STORE", str(tmp_path / "browser_sessions.json"))
    BrowserSessionTools.reset_runtime(clear_persisted=True)

    base_url, server, thread = _run_cookie_server()
    try:
        created = BrowserSessionTools.create_session(
            {
                "name": "cookie-session",
                "base_url": base_url,
                "persist_cookies": True,
                "persist_headers": True,
            }
        )
        assert created["status"] == "success"
        session_id = str(created["session"]["session_id"])

        set_cookie = BrowserSessionTools.request({"session_id": session_id, "url": f"{base_url}/set"})
        assert set_cookie["status"] == "success"
        assert set_cookie["response"]["status_code"] == 200

        BrowserSessionTools.reset_runtime(clear_persisted=False)
        listed = BrowserSessionTools.list_sessions()
        assert listed["status"] == "success"
        assert listed["count"] == 1
        restored_id = str(listed["items"][0]["session_id"])
        assert restored_id == session_id

        checked = BrowserSessionTools.request({"session_id": restored_id, "url": f"{base_url}/check"})
        assert checked["status"] == "success"
        assert checked["response"]["status_code"] == 200
    finally:
        sessions = BrowserSessionTools.list_sessions().get("items", [])
        for item in sessions:
            sid = str(item.get("session_id", ""))
            if sid:
                BrowserSessionTools.close_session({"session_id": sid})
        BrowserSessionTools.reset_runtime(clear_persisted=True)
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
