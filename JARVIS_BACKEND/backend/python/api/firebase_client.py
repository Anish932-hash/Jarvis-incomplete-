import firebase_admin
from firebase_admin import credentials, firestore, db, auth
from typing import Any, Dict, Optional


class FirebaseClient:
    """
    Secure, production-grade Firebase client.
    Supports:
    - Firestore
    - RealtimeDB
    - Auth
    """

    def __init__(self, service_key_path: str, realtime_db_url: Optional[str] = None):
        cred = credentials.Certificate(service_key_path)

        if not firebase_admin._apps:
            firebase_admin.initialize_app(
                cred,
                {"databaseURL": realtime_db_url} if realtime_db_url else None,
            )

        self.firestore = firestore.client()
        self.realtime = db.reference("/") if realtime_db_url else None

    # ---------- FIRESTORE ----------
    def set_document(self, collection: str, doc_id: str, data: Dict[str, Any]):
        self.firestore.collection(collection).document(doc_id).set(data)

    def get_document(self, collection: str, doc_id: str):
        return (
            self.firestore.collection(collection)
            .document(doc_id)
            .get()
            .to_dict()
        )

    def update_document(self, collection: str, doc_id: str, data: Dict[str, Any]):
        self.firestore.collection(collection).document(doc_id).update(data)

    # ---------- REALTIME DB ----------
    def rt_set(self, path: str, value: Any):
        if not self.realtime:
            raise RuntimeError("Realtime DB was not initialized.")
        self.realtime.child(path).set(value)

    def rt_get(self, path: str):
        if not self.realtime:
            raise RuntimeError("Realtime DB was not initialized.")
        return self.realtime.child(path).get()

    # ---------- AUTH ----------
    def verify_token(self, token: str):
        return auth.verify_id_token(token)
