from __future__ import annotations

import json

from backend.python.core.oauth_token_store import OAuthTokenStore


def test_upsert_list_and_revoke_token(tmp_path) -> None:
    store = OAuthTokenStore(store_path=str(tmp_path / "oauth_tokens.json"), max_records=20)

    upserted = store.upsert(
        provider="google",
        account_id="default",
        access_token="access-123",
        refresh_token="refresh-123",
        scopes=["email", "calendar"],
        expires_in_s=3600,
    )
    assert upserted["status"] == "success"

    listed = store.list(provider="google", account_id="default", include_secrets=False, limit=10)
    assert listed["status"] == "success"
    assert listed["count"] == 1
    assert listed["items"][0]["has_access_token"] is True
    assert listed["items"][0]["has_refresh_token"] is True

    revoked = store.revoke(provider="google", account_id="default")
    assert revoked["status"] == "success"
    assert store.list(provider="google", account_id="default", limit=10)["count"] == 0


def test_resolve_access_token_auto_refresh_with_registered_refresher(tmp_path) -> None:
    store = OAuthTokenStore(store_path=str(tmp_path / "oauth_tokens.json"), max_records=20)
    store.upsert(
        provider="google",
        account_id="default",
        access_token="stale-token",
        refresh_token="refresh-token",
        expires_in_s=1,
        metadata={"token_url": "https://unused.local/token"},
    )

    def _refresher(record: dict) -> dict:
        assert record["provider"] == "google"
        assert record["account_id"] == "default"
        return {
            "access_token": "fresh-token",
            "refresh_token": "refresh-rotated",
            "expires_in_s": 7200,
            "token_type": "Bearer",
            "scopes": ["email"],
        }

    store.register_refresher("google", _refresher)
    resolved = store.resolve_access_token(provider="google", account_id="default", min_ttl_s=120, auto_refresh=True)
    assert resolved["status"] == "success"
    assert resolved["access_token"] == "fresh-token"
    assert resolved["expires_in_s"] is not None and int(resolved["expires_in_s"]) > 120


def test_refresh_without_refresh_token_returns_error(tmp_path) -> None:
    store = OAuthTokenStore(store_path=str(tmp_path / "oauth_tokens.json"), max_records=20)
    store.upsert(
        provider="graph",
        account_id="default",
        access_token="token",
        refresh_token="",
        expires_in_s=3600,
    )
    refreshed = store.refresh(provider="graph", account_id="default")
    assert refreshed["status"] == "error"
    assert "refresh token" in str(refreshed.get("message", "")).lower()


def test_maintain_refreshes_candidates_within_window(tmp_path) -> None:
    store = OAuthTokenStore(store_path=str(tmp_path / "oauth_tokens.json"), max_records=20)
    store.upsert(
        provider="google",
        account_id="default",
        access_token="stale-token",
        refresh_token="refresh-token",
        expires_in_s=1,
        metadata={"token_url": "https://unused.local/token"},
    )

    def _refresher(_record: dict) -> dict:
        return {
            "access_token": "fresh-token",
            "refresh_token": "refresh-token-2",
            "expires_in_s": 3600,
            "token_type": "Bearer",
            "scopes": ["email"],
        }

    store.register_refresher("google", _refresher)
    maintained = store.maintain(refresh_window_s=120, provider="google", account_id="default", dry_run=False)
    assert maintained["status"] == "success"
    assert maintained["candidate_count"] == 1
    assert maintained["refreshed_count"] == 1
    assert maintained["error_count"] == 0

    resolved = store.resolve_access_token(provider="google", account_id="default", min_ttl_s=60, auto_refresh=False)
    assert resolved["status"] == "success"
    assert resolved["access_token"] == "fresh-token"


def test_store_persists_tokens_with_secret_codec_wrapping(tmp_path, monkeypatch) -> None:
    def _encode(self: OAuthTokenStore, value: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            return ""
        return f"enc:test:{clean[::-1]}"

    def _decode(self: OAuthTokenStore, value: str) -> str:
        clean = str(value or "").strip()
        if clean.startswith("enc:test:"):
            return clean.removeprefix("enc:test:")[::-1]
        return clean

    monkeypatch.setattr(OAuthTokenStore, "_encode_secret_for_store", _encode)
    monkeypatch.setattr(OAuthTokenStore, "_decode_secret_from_store", _decode)

    store_path = tmp_path / "oauth_tokens.json"
    store = OAuthTokenStore(store_path=str(store_path), max_records=20)
    upserted = store.upsert(
        provider="google",
        account_id="default",
        access_token="token-abc",
        refresh_token="refresh-xyz",
        scopes=["email"],
        expires_in_s=3600,
    )
    assert upserted["status"] == "success"

    raw = json.loads(store_path.read_text(encoding="utf-8"))
    assert isinstance(raw, list) and len(raw) == 1
    row = raw[0]
    assert str(row.get("access_token", "")).startswith("enc:test:")
    assert str(row.get("refresh_token", "")).startswith("enc:test:")
    assert row.get("access_token") != "token-abc"
    assert row.get("refresh_token") != "refresh-xyz"

    loaded = OAuthTokenStore(store_path=str(store_path), max_records=20)
    resolved = loaded.resolve_access_token(provider="google", account_id="default", auto_refresh=False)
    assert resolved["status"] == "success"
    assert resolved["access_token"] == "token-abc"
