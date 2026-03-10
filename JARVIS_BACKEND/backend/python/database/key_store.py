import os
import json
import base64
import hashlib
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from typing import Dict, Any


class KeyStore:
    """
    AES-encrypted key storage.
    Features:
    - GCM encryption
    - Tamper detection
    - Auto-salts
    - Key rotation
    """

    def __init__(self, path: str, master_key: bytes):
        if len(master_key) != 32:
            raise ValueError("master_key must be 32 bytes")

        self.path = path
        self.master_key = master_key
        self.data: Dict[str, Any] = {}

        if os.path.exists(path):
            self._load()

    # ------------------------
    # INTERNAL
    # ------------------------
    def _encrypt(self, text: str) -> Dict[str, str]:
        aes = AESGCM(self.master_key)
        nonce = os.urandom(12)
        ct = aes.encrypt(nonce, text.encode(), None)
        return {
            "nonce": base64.b64encode(nonce).decode(),
            "ciphertext": base64.b64encode(ct).decode(),
        }

    def _decrypt(self, blob: Dict[str, str]) -> str:
        aes = AESGCM(self.master_key)
        nonce = base64.b64decode(blob["nonce"])
        ct = base64.b64decode(blob["ciphertext"])
        return aes.decrypt(nonce, ct, None).decode()

    def _save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)

    def _load(self):
        with open(self.path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

    # ------------------------
    # PUBLIC API
    # ------------------------
    def set(self, key_name: str, secret_value: str):
        self.data[key_name] = self._encrypt(secret_value)
        self._save()

    def get(self, key_name: str) -> str:
        blob = self.data.get(key_name)
        if not blob:
            raise KeyError(f"Key not found: {key_name}")
        return self._decrypt(blob)

    def rotate_master_key(self, new_master_key: bytes):
        if len(new_master_key) != 32:
            raise ValueError("new_master_key must be 32 bytes")

        # decrypt all with old key → re-encrypt with new key
        decrypted = {k: self.get(k) for k in self.data}

        self.master_key = new_master_key
        self.data = {}
        for k, v in decrypted.items():
            self.set(k, v)