import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str, separators=(",", ":"))


@dataclass(slots=True)
class ApprovalRecord:
    approval_id: str
    action: str
    source: str
    args_hash: str
    args_preview: Dict[str, Any]
    reason: str
    status: str = "pending"
    created_at: str = field(default_factory=_utc_now_iso)
    expires_at: str = ""
    approved_at: Optional[str] = None
    consumed_at: Optional[str] = None
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ApprovalGate:
    """
    Two-step approval gate for high-risk actions.
    """

    def __init__(self, ttl_s: int = 300, max_records: int = 2048) -> None:
        self.ttl_s = max(30, int(ttl_s))
        self.max_records = max(256, int(max_records))
        self._records: Dict[str, ApprovalRecord] = {}
        self._lock = RLock()

    def request(self, action: str, args: Dict[str, Any], source: str, reason: str = "") -> ApprovalRecord:
        with self._lock:
            self._cleanup()
            args_hash = self._hash_args(args)
            for record in self._records.values():
                if (
                    record.status in {"pending", "approved"}
                    and record.action == action
                    and record.args_hash == args_hash
                    and record.source == source
                    and not self._is_expired(record)
                ):
                    return record

            expires_at = (_utc_now() + timedelta(seconds=self.ttl_s)).isoformat()
            record = ApprovalRecord(
                approval_id=str(uuid.uuid4()),
                action=action,
                source=source,
                args_hash=args_hash,
                args_preview=self._preview_args(args),
                reason=reason or f"Action '{action}' requires explicit user approval.",
                expires_at=expires_at,
            )
            self._records[record.approval_id] = record
            self._trim_if_needed()
            return record

    def approve(self, approval_id: str, note: str = "") -> Tuple[bool, str, Optional[ApprovalRecord]]:
        with self._lock:
            self._cleanup()
            record = self._records.get(approval_id)
            if not record:
                return False, "Approval request not found.", None
            if self._is_expired(record):
                record.status = "expired"
                return False, "Approval request expired.", record
            if record.status == "consumed":
                return False, "Approval token already consumed.", record
            record.status = "approved"
            record.approved_at = _utc_now_iso()
            if note:
                record.note = note
            return True, "Approval granted.", record

    def consume(
        self,
        approval_id: str,
        *,
        action: str,
        args: Dict[str, Any],
        source: str,
    ) -> Tuple[bool, str, Optional[ApprovalRecord]]:
        with self._lock:
            self._cleanup()
            record = self._records.get(approval_id)
            if not record:
                return False, "Approval token not found.", None
            if self._is_expired(record):
                record.status = "expired"
                return False, "Approval token expired.", record
            if record.status != "approved":
                return False, "Approval token is not approved yet.", record
            if record.action != action:
                return False, f"Approval token action mismatch (expected {record.action}, got {action}).", record
            if record.args_hash != self._hash_args(args):
                return False, "Approval token arguments mismatch.", record
            if record.source != source:
                return False, f"Approval token source mismatch (expected {record.source}, got {source}).", record

            record.status = "consumed"
            record.consumed_at = _utc_now_iso()
            return True, "Approval token consumed.", record

    def get(self, approval_id: str) -> Optional[ApprovalRecord]:
        with self._lock:
            self._cleanup()
            return self._records.get(approval_id)

    def list(self, *, status: Optional[str] = None, include_expired: bool = False, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            self._cleanup()
            rows: List[ApprovalRecord] = []
            for record in self._records.values():
                if not include_expired and record.status == "expired":
                    continue
                if status and record.status != status:
                    continue
                rows.append(record)
            rows.sort(key=lambda item: item.created_at, reverse=True)
            return [record.to_dict() for record in rows[: max(1, min(limit, 1000))]]

    def pending_count(self) -> int:
        with self._lock:
            self._cleanup()
            return sum(1 for record in self._records.values() if record.status == "pending")

    @staticmethod
    def extract_approval_id(metadata: Dict[str, Any], action: str) -> str:
        if not isinstance(metadata, dict):
            return ""
        direct = metadata.get("approval_id") or metadata.get("approval_token")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        nested = metadata.get("approval")
        if isinstance(nested, dict):
            nested_id = nested.get("id") or nested.get("token")
            if isinstance(nested_id, str) and nested_id.strip():
                return nested_id.strip()

        tokens = metadata.get("approval_tokens")
        if isinstance(tokens, dict):
            token = tokens.get(action)
            if isinstance(token, str) and token.strip():
                return token.strip()
        return ""

    def _cleanup(self) -> None:
        for record in self._records.values():
            if record.status in {"pending", "approved"} and self._is_expired(record):
                record.status = "expired"
        self._trim_if_needed()

    def _trim_if_needed(self) -> None:
        if len(self._records) <= self.max_records:
            return
        # Drop oldest consumed/expired first, then oldest records.
        values = sorted(self._records.values(), key=lambda item: item.created_at)
        removable = [r for r in values if r.status in {"consumed", "expired"}]
        if not removable:
            removable = values
        overflow = len(self._records) - self.max_records
        for record in removable[:overflow]:
            self._records.pop(record.approval_id, None)

    @staticmethod
    def _is_expired(record: ApprovalRecord) -> bool:
        try:
            expires_ts = datetime.fromisoformat(record.expires_at).timestamp()
        except Exception:
            return True
        return time.time() > expires_ts

    @staticmethod
    def _hash_args(args: Dict[str, Any]) -> str:
        encoded = _stable_json(args).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _preview_args(args: Dict[str, Any]) -> Dict[str, Any]:
        preview: Dict[str, Any] = {}
        for key, value in args.items():
            skey = str(key)
            if isinstance(value, str):
                preview[skey] = value if len(value) <= 300 else f"{value[:300]}...(truncated)"
            else:
                preview[skey] = value
        return preview
