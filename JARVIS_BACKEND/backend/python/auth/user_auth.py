import hashlib
import os
import time
from typing import Optional, Dict, Any

from .token_manager import TokenManager


class UserAuth:
    """
    Full authentication pipeline:
    - Secure password hashing (PBKDF2)
    - Login sessions using JWT + refresh tokens
    - Lockout protection
    - User registry (can be replaced with real DB)
    """

    def __init__(self, token_manager: TokenManager):
        self.token_manager = token_manager

        # Replaceable by Firestore / SQL later
        self.users: Dict[str, Dict[str, Any]] = {}

    # -------------------------
    # PASSWORD SECURITY
    # -------------------------
    @staticmethod
    def _hash_password(password: str, salt: Optional[bytes] = None):
        if salt is None:
            salt = os.urandom(32)
        pwd_hash = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt, 390000
        )
        return salt, pwd_hash

    def create_user(self, email: str, password: str, permissions: list):
        email = email.strip().lower()

        if email in self.users:
            raise ValueError("User already exists.")

        salt, hashed = self._hash_password(password)

        self.users[email] = {
            "email": email,
            "salt": salt,
            "password_hash": hashed,
            "permissions": permissions,
            "failed_attempts": 0,
            "lock_until": 0,
            "refresh_token": None,
        }

    def _verify_password(self, email: str, password: str) -> bool:
        user = self.users[email]

        salt = user["salt"]
        correct_hash = user["password_hash"]

        _, attempt_hash = self._hash_password(password, salt)
        return attempt_hash == correct_hash

    # -------------------------
    # LOGIN & TOKEN SYSTEM
    # -------------------------
    def login(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        email = email.strip().lower()

        if email not in self.users:
            return None

        user = self.users[email]

        # Lockout logic
        if user["lock_until"] > time.time():
            return None

        if not self._verify_password(email, password):
            user["failed_attempts"] += 1

            if user["failed_attempts"] >= 5:
                user["lock_until"] = time.time() + 300  # 5-minute lock
                user["failed_attempts"] = 0

            return None

        # Successful login
        user["failed_attempts"] = 0
        user["lock_until"] = 0

        access = self.token_manager.create_access_token(
            user_id=email,
            permissions=user["permissions"],
        )

        refresh_obj = self.token_manager.create_refresh_token(email)
        user["refresh_token"] = refresh_obj

        return {
            "access_token": access,
            "refresh_token": refresh_obj["raw"],
            "expires_in": self.token_manager.access_expiry,
        }

    # -------------------------
    # TOKEN REFRESH
    # -------------------------
    def refresh_access(self, email: str, raw_refresh: str) -> Optional[str]:
        if email not in self.users:
            return None

        user = self.users[email]
        rt = user.get("refresh_token")

        if not rt:
            return None

        ok = self.token_manager.verify_refresh_token(
            raw_refresh, rt["hashed"], rt["expires_at"]
        )

        if not ok:
            return None

        # Rotate refresh token
        new_rt = self.token_manager.create_refresh_token(email)
        user["refresh_token"] = new_rt

        # Issue new access token
        return self.token_manager.create_access_token(
            user_id=email, permissions=user["permissions"]
        )