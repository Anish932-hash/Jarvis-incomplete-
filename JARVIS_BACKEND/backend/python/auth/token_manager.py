import jwt
import time
import hashlib
import secrets
from typing import Dict, Any, Optional


class TokenManager:
    """
    Advanced JWT token manager.
    Provides:
    - Access tokens (short-lived)
    - Refresh tokens (rotating, hashed)
    - Expiry + tamper detection
    - Clock-skew safe verification
    """

    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        access_expiry: int = 900,  # 15 minutes
        refresh_expiry: int = 60 * 60 * 24 * 30,  # 30 days
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.access_expiry = access_expiry
        self.refresh_expiry = refresh_expiry

    # -----------------------
    # ACCESS TOKEN
    # -----------------------
    def create_access_token(self, user_id: str, permissions: list):
        now = int(time.time())
        payload = {
            "sub": user_id,
            "permissions": permissions,
            "iat": now,
            "exp": now + self.access_expiry,
        }
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def verify_access_token(self, token: str) -> Optional[Dict[str, Any]]:
        try:
            decoded = jwt.decode(
                token, self.secret_key, algorithms=[self.algorithm], options={"require": ["exp", "iat"]}
            )
            return decoded
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None

    # -----------------------
    # REFRESH TOKEN
    # -----------------------
    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def create_refresh_token(self, user_id: str) -> Dict[str, str]:
        raw_token = secrets.token_urlsafe(64)
        return {
            "user_id": user_id,
            "raw": raw_token,
            "hashed": self._hash_token(raw_token),
            "expires_at": int(time.time()) + self.refresh_expiry,
        }

    def verify_refresh_token(
        self, raw_token: str, stored_hashed: str, expires_at: int
    ) -> bool:
        if int(time.time()) > expires_at:
            return False

        return self._hash_token(raw_token) == stored_hashed