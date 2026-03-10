from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Deque, Dict, List, Optional, Tuple

from .contracts import ActionResult


class EpisodicMemory:
    """
    Long-horizon episodic memory with semantic retrieval.
    Uses sentence-transformers when available, otherwise deterministic hashing vectors.
    """

    def __init__(
        self,
        *,
        max_items: int = 4000,
        store_path: str = "data/episodic_memory.jsonl",
        embedding_dim: int = 256,
    ) -> None:
        self.max_items = max(200, int(max_items))
        self.store_path = Path(store_path)
        self.embedding_dim = max(32, min(int(embedding_dim), 1536))
        self._records: Deque[Dict[str, Any]] = deque(maxlen=self.max_items)
        self._lock = RLock()

        self._encoder: Any = None
        self._encoder_backend = "hashing"
        configured_model = str(os.getenv("JARVIS_EPISODIC_EMBEDDING_MODEL", "all-MiniLM-L6-v2")).strip()
        self._encoder_model_name = self._resolve_embedding_model_name(configured_model)
        self._encoder_init_attempted = False
        self._load()

    def remember_goal(
        self,
        *,
        goal_id: str,
        text: str,
        status: str,
        source: str,
        results: List[ActionResult],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        clean_text = str(text or "").strip()
        if not clean_text:
            clean_text = "(empty request)"

        action_rows: List[str] = []
        action_names: List[str] = []
        failures: List[str] = []
        for item in results[:20]:
            action = str(item.action or "").strip()
            if action:
                action_names.append(action)
                action_rows.append(f"{action}:{item.status}")
            if item.status in {"failed", "blocked"} and item.error:
                failures.append(str(item.error).strip())

        summary = ", ".join(action_rows[:8]).strip()
        tags = self._derive_tags(source=source, status=status, actions=action_names, metadata=metadata)
        embed_text = self._compose_embedding_text(
            text=clean_text,
            status=status,
            source=source,
            actions=action_names,
            failure=failures[-1] if failures else "",
            tags=tags,
        )
        embedding = self._embed_text(embed_text)

        record: Dict[str, Any] = {
            "memory_id": str(uuid.uuid4()),
            "goal_id": str(goal_id or "").strip(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "text": clean_text,
            "status": str(status or "").strip().lower() or "unknown",
            "source": str(source or "").strip().lower() or "unknown",
            "actions": action_names[:20],
            "result_count": len(results),
            "failure": failures[-1] if failures else "",
            "summary": summary,
            "tags": tags,
            "embedding": embedding,
        }

        with self._lock:
            self._records.append(record)
            self._append_record(record)
        return self._public_record(record, score=None)

    def recent(self, limit: int = 10) -> List[Dict[str, Any]]:
        bounded = max(1, min(int(limit), 200))
        with self._lock:
            rows = list(self._records)[-bounded:]
        return [self._public_record(row, score=None) for row in rows]

    def search(
        self,
        query: str,
        *,
        limit: int = 6,
        min_score: float = 0.12,
        status: str = "",
        source: str = "",
        must_tags: Optional[List[str]] = None,
        prefer_tags: Optional[List[str]] = None,
        exclude_goal_ids: Optional[List[str]] = None,
        diversify_by_goal: bool = True,
    ) -> List[Dict[str, Any]]:
        clean_query = str(query or "").strip()
        bounded_limit = max(1, min(int(limit), 200))
        if not clean_query:
            return self.recent(limit=bounded_limit)

        query_embedding = self._embed_text(clean_query)
        query_tokens = self._tokens(clean_query)
        status_filter = str(status or "").strip().lower()
        source_filter = str(source or "").strip().lower()
        must_tag_set = self._normalize_tag_filter(must_tags)
        prefer_tag_set = self._normalize_tag_filter(prefer_tags)
        excluded_goal_ids = {str(item or "").strip() for item in (exclude_goal_ids or []) if str(item or "").strip()}

        with self._lock:
            rows = list(self._records)

        ranked: List[tuple[float, Dict[str, Any]]] = []
        now_ts = datetime.now(timezone.utc).timestamp()
        for row in rows:
            row_status = str(row.get("status", "")).strip().lower()
            row_source = str(row.get("source", "")).strip().lower()
            row_goal_id = str(row.get("goal_id", "")).strip()
            if status_filter and row_status != status_filter:
                continue
            if source_filter and row_source != source_filter:
                continue
            if row_goal_id and row_goal_id in excluded_goal_ids:
                continue
            row_tags = {
                str(item or "").strip().lower()
                for item in row.get("tags", [])
                if isinstance(item, str) and str(item or "").strip()
            }
            if must_tag_set and not must_tag_set.issubset(row_tags):
                continue

            stored_embedding = row.get("embedding")
            vector = stored_embedding if isinstance(stored_embedding, list) else []
            if not vector:
                vector = self._embed_text(self._compose_embedding_text_from_record(row))

            semantic = self._cosine(query_embedding, vector)
            lexical = self._lexical_overlap(query_tokens, row)
            recency = self._recency_bonus(now_ts, str(row.get("created_at", "")))
            status_bonus = 0.05 if row_status == "completed" else 0.0
            tag_bonus = 0.0
            if prefer_tag_set:
                matched = len(row_tags.intersection(prefer_tag_set))
                if matched > 0:
                    tag_bonus = min(0.16, 0.04 * matched)

            score = (semantic * 0.72) + (lexical * 0.14) + (recency * 0.05) + status_bonus + tag_bonus
            if score < float(min_score):
                continue
            ranked.append((score, row))

        ranked.sort(key=lambda item: item[0], reverse=True)
        if diversify_by_goal:
            deduped: List[tuple[float, Dict[str, Any]]] = []
            seen_goals: set[str] = set()
            for score, row in ranked:
                goal_id = str(row.get("goal_id", "")).strip()
                key = goal_id or str(row.get("memory_id", "")).strip()
                if key in seen_goals:
                    continue
                seen_goals.add(key)
                deduped.append((score, row))
                if len(deduped) >= bounded_limit:
                    break
            ranked = deduped
        else:
            ranked = ranked[:bounded_limit]
        return [self._public_record(row, score=score) for score, row in ranked]

    def search_with_policy(
        self,
        query: str,
        *,
        limit: int = 8,
        policy: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        data = policy if isinstance(policy, dict) else {}
        min_score = float(data.get("min_score", 0.12) or 0.12)
        min_score = max(-1.0, min(1.0, min_score))
        status = str(data.get("status", "")).strip().lower()
        source = str(data.get("source", "")).strip().lower()
        must_tags = data.get("must_tags")
        prefer_tags = data.get("prefer_tags")
        exclude_goal_ids = data.get("exclude_goal_ids")
        diversify = bool(data.get("diversify_by_goal", True))
        return self.search(
            query,
            limit=limit,
            min_score=min_score,
            status=status,
            source=source,
            must_tags=[str(item) for item in must_tags] if isinstance(must_tags, list) else None,
            prefer_tags=[str(item) for item in prefer_tags] if isinstance(prefer_tags, list) else None,
            exclude_goal_ids=[str(item) for item in exclude_goal_ids] if isinstance(exclude_goal_ids, list) else None,
            diversify_by_goal=diversify,
        )

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            count = len(self._records)
        return {
            "count": count,
            "max_items": self.max_items,
            "embedding_backend": self._encoder_backend,
            "embedding_model": self._encoder_model_name if self._encoder_backend == "sentence-transformers" else "",
            "embedding_dim": self.embedding_dim,
            "store_path": str(self.store_path),
        }

    def strategy(
        self,
        query: str,
        *,
        limit: int = 12,
        min_score: float = 0.08,
        include_failures: bool = True,
    ) -> Dict[str, Any]:
        clean_query = str(query or "").strip()
        bounded = max(4, min(int(limit), 64))
        score_floor = max(-1.0, min(1.0, float(min_score)))

        if clean_query:
            retrieved = self.search(
                clean_query,
                limit=max(bounded * 3, bounded),
                min_score=score_floor,
                diversify_by_goal=False,
            )
        else:
            retrieved = self.recent(limit=max(bounded * 3, bounded))
        if not retrieved:
            return {
                "status": "success",
                "query": clean_query,
                "sample_count": 0,
                "recommended_actions": [],
                "avoid_actions": [],
                "top_sequences": [],
                "failure_patterns": [],
                "strategy_hint": "No episodic memories matched this query yet.",
            }

        action_stats: Dict[str, Dict[str, float]] = {}
        sequence_counts: Dict[Tuple[str, ...], int] = {}
        failure_patterns: Dict[str, int] = {}
        used = retrieved[: max(bounded * 4, bounded)]

        for row in used:
            status = str(row.get("status", "")).strip().lower()
            memory_score = row.get("memory_score")
            base_score = float(memory_score) if isinstance(memory_score, (int, float)) else 0.22
            base_weight = max(0.05, min(base_score, 1.4))
            if status == "completed":
                status_weight = 1.0
            elif status in {"failed", "blocked"}:
                status_weight = 0.62
            else:
                status_weight = 0.78
            row_weight = base_weight * status_weight

            actions_raw = row.get("actions", [])
            unique_actions: List[str] = []
            if isinstance(actions_raw, list):
                for action_item in actions_raw:
                    clean_action = str(action_item or "").strip().lower()
                    if clean_action and clean_action not in unique_actions:
                        unique_actions.append(clean_action)
            for index, action in enumerate(unique_actions[:8]):
                depth_factor = max(0.5, 1.0 - (0.08 * index))
                action_weight = row_weight * depth_factor
                stats = action_stats.setdefault(
                    action,
                    {
                        "support": 0.0,
                        "success_weight": 0.0,
                        "failure_weight": 0.0,
                        "success_count": 0.0,
                        "failure_count": 0.0,
                    },
                )
                stats["support"] += action_weight
                if status == "completed":
                    stats["success_weight"] += action_weight
                    stats["success_count"] += 1.0
                elif status in {"failed", "blocked"}:
                    stats["failure_weight"] += action_weight
                    stats["failure_count"] += 1.0

            if status == "completed" and unique_actions:
                sequence_key = tuple(unique_actions[:4])
                sequence_counts[sequence_key] = sequence_counts.get(sequence_key, 0) + 1

            if include_failures and status in {"failed", "blocked"}:
                failure_text = str(row.get("failure", "")).strip()
                if failure_text:
                    bucket = self._failure_pattern_bucket(failure_text)
                    failure_patterns[bucket] = failure_patterns.get(bucket, 0) + 1

        recommended: List[Dict[str, Any]] = []
        avoid: List[Dict[str, Any]] = []
        for action, stats in action_stats.items():
            support = float(stats.get("support", 0.0))
            if support <= 0:
                continue
            success_weight = float(stats.get("success_weight", 0.0))
            failure_weight = float(stats.get("failure_weight", 0.0))
            success_rate = success_weight / max(1e-9, support)
            failure_rate = failure_weight / max(1e-9, support)
            support_count = int(max(stats.get("success_count", 0.0), 0.0) + max(stats.get("failure_count", 0.0), 0.0))

            row = {
                "action": action,
                "support": round(support, 6),
                "support_count": support_count,
                "success_rate": round(success_rate, 6),
                "failure_rate": round(failure_rate, 6),
            }
            if success_rate >= 0.52 and success_weight >= 0.08:
                recommended.append(row)
            if include_failures and failure_rate >= 0.48 and failure_weight >= 0.08:
                avoid.append(row)

        if not recommended and action_stats:
            fallback = sorted(
                (
                    {
                        "action": action,
                        "support": round(float(stats.get("support", 0.0)), 6),
                        "support_count": int(stats.get("success_count", 0.0) + stats.get("failure_count", 0.0)),
                        "success_rate": round(
                            float(stats.get("success_weight", 0.0)) / max(1e-9, float(stats.get("support", 0.0))),
                            6,
                        ),
                        "failure_rate": round(
                            float(stats.get("failure_weight", 0.0)) / max(1e-9, float(stats.get("support", 0.0))),
                            6,
                        ),
                    }
                    for action, stats in action_stats.items()
                ),
                key=lambda item: (float(item.get("success_rate", 0.0)), float(item.get("support", 0.0))),
                reverse=True,
            )
            recommended = fallback[:3]

        recommended.sort(key=lambda item: (float(item.get("success_rate", 0.0)), float(item.get("support", 0.0))), reverse=True)
        avoid.sort(key=lambda item: (float(item.get("failure_rate", 0.0)), float(item.get("support", 0.0))), reverse=True)

        top_sequences = sorted(sequence_counts.items(), key=lambda item: item[1], reverse=True)
        failure_rows = sorted(failure_patterns.items(), key=lambda item: item[1], reverse=True)

        recommended_actions = recommended[: max(1, min(bounded, 8))]
        avoid_actions = avoid[: max(1, min(bounded, 8))]
        sequence_items = [
            {"sequence": list(sequence), "support_count": count}
            for sequence, count in top_sequences[: max(1, min(bounded // 2, 6))]
        ]
        pattern_items = [
            {"pattern": pattern, "count": count}
            for pattern, count in failure_rows[: max(1, min(bounded // 2, 6))]
        ]

        if recommended_actions:
            lead_action = str(recommended_actions[0].get("action", "")).strip()
            strategy_hint = (
                f"Prefer '{lead_action}' first for similar goals."
                if lead_action
                else "Use top recommended actions from episodic history."
            )
        else:
            strategy_hint = "No strong recommended action signal was found."
        if avoid_actions:
            blocked = str(avoid_actions[0].get("action", "")).strip()
            if blocked:
                strategy_hint = f"{strategy_hint} Avoid '{blocked}' unless context changed."

        return {
            "status": "success",
            "query": clean_query,
            "sample_count": len(used),
            "recommended_actions": recommended_actions,
            "avoid_actions": avoid_actions,
            "top_sequences": sequence_items,
            "failure_patterns": pattern_items,
            "strategy_hint": strategy_hint,
        }

    @staticmethod
    def _failure_pattern_bucket(message: str) -> str:
        lowered = str(message or "").strip().lower()
        if not lowered:
            return "unknown"
        if any(token in lowered for token in ("timeout", "timed out", "deadline", "took too long")):
            return "timeout"
        if any(token in lowered for token in ("auth", "token", "unauthorized", "forbidden", "permission")):
            return "auth_or_permission"
        if any(token in lowered for token in ("network", "dns", "connection", "reset", "unreachable")):
            return "network"
        if any(token in lowered for token in ("path", "file not found", "no such file", "directory")):
            return "path_or_io"
        if any(token in lowered for token in ("dependency", "module", "package", "import")):
            return "dependency"
        if any(token in lowered for token in ("policy", "blocked", "approval")):
            return "policy_or_approval"
        return "other"

    def _derive_tags(
        self,
        *,
        source: str,
        status: str,
        actions: List[str],
        metadata: Optional[Dict[str, Any]],
    ) -> List[str]:
        tags: List[str] = []
        clean_source = str(source or "").strip().lower()
        clean_status = str(status or "").strip().lower()
        if clean_source:
            tags.append(f"source:{clean_source}")
        if clean_status:
            tags.append(f"status:{clean_status}")
        for action in actions[:8]:
            clean_action = str(action or "").strip().lower()
            if clean_action:
                tags.append(f"action:{clean_action}")

        data = metadata if isinstance(metadata, dict) else {}
        profile = str(data.get("policy_profile", "")).strip().lower()
        macro_id = str(data.get("macro_id", "")).strip()
        if profile:
            tags.append(f"profile:{profile}")
        if macro_id:
            tags.append("macro")
        if "__jarvis_trigger_id" in data:
            tags.append("trigger")
        if "__jarvis_schedule_id" in data:
            tags.append("schedule")

        deduped: List[str] = []
        for item in tags:
            if item and item not in deduped:
                deduped.append(item)
        return deduped[:24]

    def _compose_embedding_text(
        self,
        *,
        text: str,
        status: str,
        source: str,
        actions: List[str],
        failure: str,
        tags: List[str],
    ) -> str:
        actions_text = " ".join(actions[:12])
        tags_text = " ".join(tags[:16])
        return (
            f"request: {text}\n"
            f"status: {status}\n"
            f"source: {source}\n"
            f"actions: {actions_text}\n"
            f"failure: {failure}\n"
            f"tags: {tags_text}\n"
        ).strip()

    def _compose_embedding_text_from_record(self, row: Dict[str, Any]) -> str:
        return self._compose_embedding_text(
            text=str(row.get("text", "")),
            status=str(row.get("status", "")),
            source=str(row.get("source", "")),
            actions=[str(item) for item in row.get("actions", []) if isinstance(item, str)],
            failure=str(row.get("failure", "")),
            tags=[str(item) for item in row.get("tags", []) if isinstance(item, str)],
        )

    def _append_record(self, record: Dict[str, Any]) -> None:
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            with self.store_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=True))
                handle.write("\n")
        except Exception:
            return

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        try:
            lines = self.store_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return
        for raw in lines[-self.max_items :]:
            try:
                row = json.loads(raw)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            embedding = row.get("embedding")
            if not isinstance(embedding, list) or not embedding:
                embedding = self._embed_text(self._compose_embedding_text_from_record(row))
                row["embedding"] = embedding
            self._records.append(row)

    def _embed_text(self, text: str) -> List[float]:
        clean = str(text or "").strip()
        if not clean:
            return [0.0] * self.embedding_dim

        self._ensure_encoder()
        if self._encoder is not None:
            try:
                raw = self._encoder.encode(clean, normalize_embeddings=True)  # type: ignore[attr-defined]
                if hasattr(raw, "tolist"):
                    vector = raw.tolist()
                else:
                    vector = list(raw)
                if isinstance(vector, list) and vector:
                    return self._normalize_vector(vector, size_hint=len(vector))
            except Exception:
                self._encoder = None
                self._encoder_backend = "hashing"

        vector = [0.0] * self.embedding_dim
        tokens = self._tokens(clean)
        if not tokens:
            tokens = [clean.lower()[:32]]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8", errors="ignore")).digest()
            idx = int.from_bytes(digest[:4], byteorder="big", signed=False) % self.embedding_dim
            polarity = 1.0 if (digest[4] % 2 == 0) else -1.0
            weight = 1.0 + (min(len(token), 14) / 14.0)
            vector[idx] += polarity * weight
        return self._normalize_vector(vector, size_hint=self.embedding_dim)

    def _ensure_encoder(self) -> None:
        if self._encoder_init_attempted:
            return
        self._encoder_init_attempted = True

        # Opt-in model loading to keep runtime predictable in local/offline environments.
        if os.getenv("JARVIS_EPISODIC_ENABLE_ST", "0") != "1":
            self._encoder_backend = "hashing"
            return
        if os.getenv("JARVIS_EPISODIC_DISABLE_ST", "0") == "1":
            self._encoder_backend = "hashing"
            return
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
        except Exception:
            self._encoder_backend = "hashing"
            return

        try:
            self._encoder = SentenceTransformer(self._encoder_model_name)
            self._encoder_backend = "sentence-transformers"
        except Exception:
            self._encoder = None
            self._encoder_backend = "hashing"

    def _resolve_embedding_model_name(self, configured_model: str) -> str:
        clean = str(configured_model or "").strip()
        explicit_path = self._resolve_embedding_path(clean)
        if explicit_path is not None:
            return str(explicit_path)

        discovered = self._discover_local_embedding_models()
        if discovered:
            return str(discovered[0])
        return clean or "all-MiniLM-L6-v2"

    def _resolve_embedding_path(self, configured_model: str) -> Optional[Path]:
        clean = str(configured_model or "").strip()
        if not clean:
            return None
        candidate = Path(clean)
        if candidate.exists():
            return candidate.resolve()
        cwd = Path.cwd().resolve()
        normalized = clean.lower()
        search_roots = [
            cwd / "embeddings",
            cwd.parent / "embeddings",
            cwd.parent.parent / "embeddings",
        ]
        for root in search_roots:
            if not root.exists() or not root.is_dir():
                continue
            for child in root.iterdir():
                if not child.is_dir():
                    continue
                name = child.name.strip().lower()
                if normalized in name or name in normalized:
                    return child.resolve()
        return None

    def _discover_local_embedding_models(self) -> List[Path]:
        cwd = Path.cwd().resolve()
        search_roots = [
            cwd / "embeddings",
            cwd.parent / "embeddings",
            cwd.parent.parent / "embeddings",
        ]
        candidates: List[Path] = []
        seen: set[str] = set()
        for root in search_roots:
            if not root.exists() or not root.is_dir():
                continue
            for child in root.iterdir():
                if not child.is_dir():
                    continue
                markers = {
                    (child / "config_sentence_transformers.json").exists(),
                    (child / "modules.json").exists(),
                    (child / "config.json").exists(),
                }
                if not any(markers):
                    continue
                resolved = child.resolve()
                key = str(resolved).lower()
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(resolved)
        candidates.sort(key=self._embedding_candidate_rank)
        return candidates

    @staticmethod
    def _embedding_candidate_rank(path: Path) -> tuple[int, int, str]:
        name = path.name.strip().lower()
        score = 0
        if "all-mpnet-base-v2" in name:
            score += 120
        if "multi-qa-mpnet-base-dot-v1" in name:
            score += 110
        if "mpnet" in name:
            score += 60
        if "embedding" in name or "embeddings" in str(path).replace("\\", "/").lower():
            score += 24
        return (-score, len(str(path)), str(path).lower())

    @staticmethod
    def _tokens(value: str) -> List[str]:
        return [item for item in re.split(r"[^a-zA-Z0-9_]+", value.lower()) if len(item) >= 2]

    @staticmethod
    def _normalize_tag_filter(raw_tags: Optional[List[str]]) -> set[str]:
        if not isinstance(raw_tags, list):
            return set()
        out: set[str] = set()
        for item in raw_tags:
            clean = str(item or "").strip().lower()
            if clean:
                out.add(clean)
        return out

    def _normalize_vector(self, values: List[float], *, size_hint: int) -> List[float]:
        norm = math.sqrt(sum(float(v) * float(v) for v in values))
        if norm <= 1e-9:
            return [0.0] * max(1, size_hint)
        return [round(float(v) / norm, 8) for v in values]

    @staticmethod
    def _cosine(a: List[float], b: List[float]) -> float:
        if not a or not b:
            return 0.0
        length = min(len(a), len(b))
        if length <= 0:
            return 0.0
        dot = sum(float(a[idx]) * float(b[idx]) for idx in range(length))
        return max(-1.0, min(1.0, dot))

    def _lexical_overlap(self, query_tokens: List[str], row: Dict[str, Any]) -> float:
        if not query_tokens:
            return 0.0
        row_tokens = set(self._tokens(str(row.get("text", ""))))
        row_tokens.update(self._tokens(str(row.get("summary", ""))))
        row_tokens.update(str(item).lower() for item in row.get("actions", []) if isinstance(item, str))
        overlap = len(set(query_tokens).intersection(row_tokens))
        if overlap <= 0:
            return 0.0
        return min(1.0, overlap / max(1.0, len(set(query_tokens))))

    @staticmethod
    def _recency_bonus(now_ts: float, created_at: str) -> float:
        try:
            created = datetime.fromisoformat(created_at)
            created_ts = created.timestamp()
        except Exception:
            return 0.0
        age_s = max(0.0, now_ts - created_ts)
        age_days = age_s / 86400.0
        return max(0.0, math.exp(-age_days / 14.0))

    def _public_record(self, row: Dict[str, Any], *, score: Optional[float]) -> Dict[str, Any]:
        output = {
            "memory_id": str(row.get("memory_id", "")),
            "goal_id": str(row.get("goal_id", "")),
            "created_at": str(row.get("created_at", "")),
            "text": str(row.get("text", "")),
            "status": str(row.get("status", "")),
            "source": str(row.get("source", "")),
            "actions": [str(item) for item in row.get("actions", []) if isinstance(item, str)],
            "result_count": int(row.get("result_count", 0)),
            "failure": str(row.get("failure", "")),
            "summary": str(row.get("summary", "")),
            "tags": [str(item) for item in row.get("tags", []) if isinstance(item, str)],
            "memory_type": "episodic_semantic",
        }
        if score is not None:
            output["memory_score"] = round(float(score), 6)
        return output
