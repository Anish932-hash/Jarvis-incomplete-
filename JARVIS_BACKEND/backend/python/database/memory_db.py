import time
import math
from typing import Any, Dict, Iterable, List, Optional, Set


class MemoryDB:
    """
    In-memory hybrid memory index with semantic + lexical retrieval.
    Features:
    - Embedding storage
    - Relevance ranking with recency/access weighting
    - Memory aging (decay)
    - TTL expiry
    - Tag indexing
    - Hybrid search with tag/ID filters
    """

    def __init__(self, *, max_entries: int = 10_000, max_events: int = 5_000):
        self.entries: Dict[str, Dict[str, Any]] = {}
        self.tags: Dict[str, Set[str]] = {}
        self.events: List[Dict[str, Any]] = []
        self.max_entries = max(100, min(int(max_entries), 500_000))
        self.max_events = max(100, min(int(max_events), 1_000_000))
        self._feedback_state: Dict[str, float] = {
            "count": 0.0,
            "success_ema": 0.5,
            "reward_ema": 0.0,
            "updated_at": 0.0,
        }
        self._adaptive_weights: Dict[str, float] = {
            "semantic_weight": 0.72,
            "lexical_weight": 0.14,
            "recency_weight": 0.09,
            "access_weight": 0.05,
        }
        self._search_journal: List[Dict[str, Any]] = []
        self._max_search_journal = 400

    # ------------------------
    # STORE MEMORY
    # ------------------------
    def store(
        self,
        memory_id: str,
        content: str,
        embedding: List[float],
        tags: Optional[List[str]] = None,
        ttl: Optional[int] = None,
        priority: float = 0.0,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        clean_id = str(memory_id or "").strip()
        if not clean_id:
            raise ValueError("memory_id is required")
        now = time.time()
        normalized_tags = self._normalize_tags(tags or [])
        vector = [float(item) for item in embedding] if isinstance(embedding, list) else []
        text = str(content or "").strip()
        if not vector:
            raise ValueError("embedding is required")

        existing = self.entries.get(clean_id)
        if isinstance(existing, dict):
            self._drop_tags_for_id(clean_id, existing.get("tags", []))

        self.entries[clean_id] = {
            "id": clean_id,
            "content": text,
            "embed": vector,
            "tags": normalized_tags,
            "created": now,
            "updated": now,
            "ttl": now + ttl if ttl else None,
            "access_count": 0,
            "last_access": 0.0,
            "priority": max(-1.0, min(float(priority), 1.0)),
            "metadata": metadata if isinstance(metadata, dict) else {},
        }

        for tag in normalized_tags:
            self.tags.setdefault(tag, set()).add(clean_id)

        self._cleanup_expired()
        self._enforce_capacity()

    # ------------------------
    # RETRIEVAL
    # ------------------------
    @staticmethod
    def _cosine(a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a * norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def search(
        self,
        query_embed: List[float],
        top_k: int = 5,
        *,
        lexical_query: str = "",
        must_tags: Optional[List[str]] = None,
        exclude_ids: Optional[Iterable[str]] = None,
        min_score: float = -1.0,
        semantic_weight: float = 0.72,
        lexical_weight: float = 0.14,
        recency_weight: float = 0.09,
        access_weight: float = 0.05,
        strategy: str = "",
        diversify_by: str = "",
        max_per_group: int = 0,
        prefer_tags: Optional[List[str]] = None,
        use_adaptive_weights: bool = False,
        include_breakdown: bool = False,
    ) -> List[Dict[str, Any]]:
        if not isinstance(query_embed, list) or not query_embed:
            return []
        started = time.monotonic()
        now = time.time()
        required_tags = self._normalize_tags(must_tags or [])
        preferred_tags = self._normalize_tags(prefer_tags or [])
        excluded = {str(item).strip() for item in (exclude_ids or []) if str(item).strip()}
        qlex = str(lexical_query or "").strip().lower()
        bounded_top_k = max(1, min(int(top_k), 1000))
        strategy_name = str(strategy or "").strip().lower()
        weights = self._resolve_weight_profile(
            semantic_weight=semantic_weight,
            lexical_weight=lexical_weight,
            recency_weight=recency_weight,
            access_weight=access_weight,
            strategy=strategy_name,
            use_adaptive=bool(use_adaptive_weights),
        )
        semantic_w = float(weights.get("semantic_weight", 0.72))
        lexical_w = float(weights.get("lexical_weight", 0.14))
        recency_w = float(weights.get("recency_weight", 0.09))
        access_w = float(weights.get("access_weight", 0.05))

        results = []
        for mid, data in self.entries.items():
            if mid in excluded:
                continue
            if data["ttl"] and now > data["ttl"]:
                continue
            tags = data.get("tags", [])
            if required_tags and not required_tags.issubset(set(tags)):
                continue

            score = self._cosine(query_embed, data["embed"])
            age = now - data["created"]
            decay = 1.0 / (1.0 + (age / 86400.0))
            lex = self._lexical_similarity(qlex, str(data.get("content", "")))
            access_count = max(0, int(data.get("access_count", 0)))
            access_boost = min(1.0, math.log1p(access_count) / 8.0)
            priority = max(-1.0, min(float(data.get("priority", 0.0) or 0.0), 1.0))
            tag_overlap = len(preferred_tags.intersection(set(tags))) if preferred_tags else 0
            tag_affinity = min(1.0, tag_overlap / max(1.0, float(len(preferred_tags)))) if preferred_tags else 0.0
            combined = (
                (semantic_w * score)
                + (lexical_w * lex)
                + (recency_w * decay)
                + (access_w * access_boost)
                + (0.02 * priority)
                + (0.04 * tag_affinity)
            )
            if combined < float(min_score):
                continue

            row: Dict[str, Any] = {
                "id": mid,
                "content": data["content"],
                "score": combined,
                "semantic_score": score,
                "lexical_score": lex,
                "recency_score": decay,
                "access_score": access_boost,
                "tag_affinity_score": tag_affinity,
                "priority": priority,
                "tags": list(tags),
                "metadata": dict(data.get("metadata", {})),
            }
            if include_breakdown:
                row["score_components"] = {
                    "semantic": round(semantic_w * score, 6),
                    "lexical": round(lexical_w * lex, 6),
                    "recency": round(recency_w * decay, 6),
                    "access": round(access_w * access_boost, 6),
                    "priority": round(0.02 * priority, 6),
                    "tag_affinity": round(0.04 * tag_affinity, 6),
                }
            results.append(row)

        results.sort(key=lambda x: x["score"], reverse=True)
        selected = results[:bounded_top_k]
        if str(diversify_by or "").strip():
            selected = self._diversify_results(
                selected,
                diversify_by=str(diversify_by or "").strip().lower(),
                top_k=bounded_top_k,
                max_per_group=max_per_group,
            )
        for row in selected:
            self._record_access(row["id"])
        self._record_search_journal(
            lexical_query=qlex,
            top_k=bounded_top_k,
            strategy=strategy_name or ("adaptive" if use_adaptive_weights else "manual"),
            weights=weights,
            result_count=len(selected),
            latency_ms=max(0.0, (time.monotonic() - started) * 1000.0),
            diversify_by=str(diversify_by or "").strip().lower(),
            prefer_tags=sorted(preferred_tags),
        )
        return selected

    def hybrid_search(
        self,
        *,
        query_text: str,
        query_embed: Optional[List[float]] = None,
        top_k: int = 5,
        must_tags: Optional[List[str]] = None,
        exclude_ids: Optional[List[str]] = None,
        min_score: float = 0.0,
        strategy: str = "",
        diversify_by: str = "",
        max_per_group: int = 0,
        prefer_tags: Optional[List[str]] = None,
        use_adaptive_weights: bool = False,
        include_breakdown: bool = False,
    ) -> List[Dict[str, Any]]:
        if query_embed is None:
            query_embed = self._proxy_embedding(query_text)
        return self.search(
            query_embed=query_embed,
            top_k=top_k,
            lexical_query=query_text,
            must_tags=must_tags,
            exclude_ids=exclude_ids,
            min_score=min_score,
            strategy=strategy,
            diversify_by=diversify_by,
            max_per_group=max_per_group,
            prefer_tags=prefer_tags,
            use_adaptive_weights=use_adaptive_weights,
            include_breakdown=include_breakdown,
        )

    # ------------------------
    # TAG SEARCH
    # ------------------------
    def by_tag(self, tag: str) -> List[Dict[str, Any]]:
        clean_tag = str(tag or "").strip().lower()
        if not clean_tag:
            return []
        ids = sorted(self.tags.get(clean_tag, set()))
        now = time.time()
        rows: List[Dict[str, Any]] = []
        for mid in ids:
            row = self.entries.get(mid)
            if not isinstance(row, dict):
                continue
            ttl = row.get("ttl")
            if ttl is not None and now > float(ttl):
                continue
            self._record_access(mid)
            rows.append(dict(row))
        return rows

    def record_feedback(self, memory_id: str, *, success: bool, reward: float = 0.0) -> Dict[str, Any]:
        clean_id = str(memory_id or "").strip()
        row = self.entries.get(clean_id)
        if not isinstance(row, dict):
            return {"status": "error", "message": "memory not found", "memory_id": clean_id}
        current_priority = float(row.get("priority", 0.0) or 0.0)
        delta = (0.08 if success else -0.12) + max(-0.2, min(float(reward), 0.2))
        next_priority = max(-1.0, min(current_priority + delta, 1.0))
        row["priority"] = next_priority
        row["updated"] = time.time()
        feedback_row = row.get("feedback", {})
        if not isinstance(feedback_row, dict):
            feedback_row = {}
        alpha = 0.24
        signal = 1.0 if bool(success) else 0.0
        bounded_reward = max(-1.0, min(float(reward), 1.0))
        feedback_row["count"] = float(feedback_row.get("count", 0.0) or 0.0) + 1.0
        feedback_row["success_ema"] = ((1.0 - alpha) * float(feedback_row.get("success_ema", signal) or signal)) + (alpha * signal)
        feedback_row["reward_ema"] = ((1.0 - alpha) * float(feedback_row.get("reward_ema", bounded_reward) or bounded_reward)) + (
            alpha * bounded_reward
        )
        feedback_row["updated_at"] = row["updated"]
        row["feedback"] = feedback_row
        self.entries[clean_id] = row

        global_state = self._feedback_state
        global_state["count"] = float(global_state.get("count", 0.0) or 0.0) + 1.0
        global_state["success_ema"] = ((1.0 - alpha) * float(global_state.get("success_ema", signal) or signal)) + (alpha * signal)
        global_state["reward_ema"] = ((1.0 - alpha) * float(global_state.get("reward_ema", bounded_reward) or bounded_reward)) + (
            alpha * bounded_reward
        )
        global_state["updated_at"] = row["updated"]
        self._retune_adaptive_weights()
        return {
            "status": "success",
            "memory_id": clean_id,
            "priority": round(next_priority, 6),
            "adaptive_profile": self.adaptive_profile(),
        }

    def adaptive_profile(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "weights": {
                "semantic_weight": round(float(self._adaptive_weights.get("semantic_weight", 0.72)), 6),
                "lexical_weight": round(float(self._adaptive_weights.get("lexical_weight", 0.14)), 6),
                "recency_weight": round(float(self._adaptive_weights.get("recency_weight", 0.09)), 6),
                "access_weight": round(float(self._adaptive_weights.get("access_weight", 0.05)), 6),
            },
            "feedback": {
                "count": int(float(self._feedback_state.get("count", 0.0) or 0.0)),
                "success_ema": round(float(self._feedback_state.get("success_ema", 0.5) or 0.5), 6),
                "reward_ema": round(float(self._feedback_state.get("reward_ema", 0.0) or 0.0), 6),
                "updated_at": float(self._feedback_state.get("updated_at", 0.0) or 0.0),
            },
        }

    def search_with_diagnostics(
        self,
        *,
        query_text: str,
        query_embed: Optional[List[float]] = None,
        top_k: int = 5,
        must_tags: Optional[List[str]] = None,
        exclude_ids: Optional[List[str]] = None,
        min_score: float = 0.0,
        strategy: str = "adaptive",
        diversify_by: str = "tag",
        max_per_group: int = 2,
        prefer_tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        started = time.monotonic()
        rows = self.hybrid_search(
            query_text=query_text,
            query_embed=query_embed,
            top_k=top_k,
            must_tags=must_tags,
            exclude_ids=exclude_ids,
            min_score=min_score,
            strategy=strategy,
            diversify_by=diversify_by,
            max_per_group=max_per_group,
            prefer_tags=prefer_tags,
            use_adaptive_weights=True,
            include_breakdown=True,
        )
        elapsed_ms = max(0.0, (time.monotonic() - started) * 1000.0)
        return {
            "status": "success",
            "query_text": str(query_text or "").strip(),
            "count": len(rows),
            "items": rows,
            "strategy": str(strategy or "").strip().lower() or "adaptive",
            "diversify_by": str(diversify_by or "").strip().lower(),
            "latency_ms": round(elapsed_ms, 6),
            "adaptive_profile": self.adaptive_profile(),
            "search_journal_tail": self.search_journal(limit=10).get("items", []),
        }

    def search_journal(self, *, limit: int = 40) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), self._max_search_journal))
        rows = self._search_journal[-bounded:]
        return {"status": "success", "count": len(rows), "items": list(rows)}

    # ------------------------
    # COMPAT WRAPPERS (legacy agents)
    # ------------------------
    def insert_entry(self, entry: Dict[str, Any]) -> None:
        entry = dict(entry)
        entry["timestamp"] = entry.get("timestamp", time.time())
        self.events.append(entry)
        # Keep event buffer bounded for runtime stability.
        if len(self.events) > self.max_events:
            self.events = self.events[-self.max_events:]

    def get_recent(self, limit: int = 10) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        return list(self.events[-limit:])

    def _record_access(self, memory_id: str) -> None:
        row = self.entries.get(memory_id)
        if not isinstance(row, dict):
            return
        row["access_count"] = max(0, int(row.get("access_count", 0))) + 1
        row["last_access"] = time.time()
        self.entries[memory_id] = row

    def _cleanup_expired(self) -> int:
        now = time.time()
        expired = [
            memory_id
            for memory_id, row in self.entries.items()
            if isinstance(row, dict) and row.get("ttl") is not None and now > float(row.get("ttl") or 0.0)
        ]
        if not expired:
            return 0
        for memory_id in expired:
            row = self.entries.pop(memory_id, {})
            if isinstance(row, dict):
                self._drop_tags_for_id(memory_id, row.get("tags", []))
        return len(expired)

    def _enforce_capacity(self) -> None:
        if len(self.entries) <= self.max_entries:
            return
        now = time.time()
        ranked: List[tuple[float, str]] = []
        for memory_id, row in self.entries.items():
            created = float(row.get("created", now) or now)
            age_hours = max(0.0, (now - created) / 3600.0)
            access = max(0, int(row.get("access_count", 0)))
            priority = max(-1.0, min(float(row.get("priority", 0.0) or 0.0), 1.0))
            retention_score = (0.35 * (1.0 / (1.0 + age_hours / 24.0))) + (0.45 * min(1.0, math.log1p(access) / 8.0)) + (0.2 * (priority + 1.0) / 2.0)
            ranked.append((retention_score, memory_id))
        ranked.sort(key=lambda item: item[0])
        overflow = len(self.entries) - self.max_entries
        for _, memory_id in ranked[:overflow]:
            row = self.entries.pop(memory_id, {})
            if isinstance(row, dict):
                self._drop_tags_for_id(memory_id, row.get("tags", []))

    def _drop_tags_for_id(self, memory_id: str, tags: Iterable[str]) -> None:
        for raw_tag in tags:
            tag = str(raw_tag or "").strip().lower()
            if not tag:
                continue
            members = self.tags.get(tag)
            if not isinstance(members, set):
                continue
            members.discard(memory_id)
            if not members:
                self.tags.pop(tag, None)

    @staticmethod
    def _normalize_tags(tags: Iterable[str]) -> Set[str]:
        return {str(tag or "").strip().lower() for tag in tags if str(tag or "").strip()}

    @staticmethod
    def _lexical_similarity(query: str, text: str) -> float:
        q = str(query or "").strip().lower()
        t = str(text or "").strip().lower()
        if not q or not t:
            return 0.0
        if q in t:
            return min(1.0, 0.75 + (len(q) / max(1.0, len(t)) * 0.25))
        q_tokens = {token for token in q.split() if token}
        t_tokens = {token for token in t.split() if token}
        if not q_tokens or not t_tokens:
            return 0.0
        overlap = len(q_tokens.intersection(t_tokens))
        return overlap / float(max(1, len(q_tokens)))

    @staticmethod
    def _proxy_embedding(text: str, *, dim: int = 64) -> List[float]:
        clean = str(text or "").strip().lower()
        if not clean:
            return [0.0] * dim
        vector = [0.0] * dim
        for idx, token in enumerate(clean.split()):
            bucket = (hash(token) + idx) % dim
            vector[bucket] += 1.0
        norm = math.sqrt(sum(item * item for item in vector))
        if norm <= 0:
            return vector
        return [item / norm for item in vector]

    @staticmethod
    def _normalize_weight_map(weights: Dict[str, float]) -> Dict[str, float]:
        semantic = max(0.0, float(weights.get("semantic_weight", 0.0) or 0.0))
        lexical = max(0.0, float(weights.get("lexical_weight", 0.0) or 0.0))
        recency = max(0.0, float(weights.get("recency_weight", 0.0) or 0.0))
        access = max(0.0, float(weights.get("access_weight", 0.0) or 0.0))
        total = semantic + lexical + recency + access
        if total <= 0.0:
            return {
                "semantic_weight": 0.72,
                "lexical_weight": 0.14,
                "recency_weight": 0.09,
                "access_weight": 0.05,
            }
        return {
            "semantic_weight": semantic / total,
            "lexical_weight": lexical / total,
            "recency_weight": recency / total,
            "access_weight": access / total,
        }

    def _resolve_weight_profile(
        self,
        *,
        semantic_weight: float,
        lexical_weight: float,
        recency_weight: float,
        access_weight: float,
        strategy: str,
        use_adaptive: bool,
    ) -> Dict[str, float]:
        profile = {
            "semantic_weight": float(semantic_weight),
            "lexical_weight": float(lexical_weight),
            "recency_weight": float(recency_weight),
            "access_weight": float(access_weight),
        }
        clean_strategy = str(strategy or "").strip().lower()
        if clean_strategy == "semantic":
            profile.update({"semantic_weight": 0.84, "lexical_weight": 0.08, "recency_weight": 0.05, "access_weight": 0.03})
        elif clean_strategy == "lexical":
            profile.update({"semantic_weight": 0.52, "lexical_weight": 0.32, "recency_weight": 0.1, "access_weight": 0.06})
        elif clean_strategy == "recency":
            profile.update({"semantic_weight": 0.48, "lexical_weight": 0.15, "recency_weight": 0.28, "access_weight": 0.09})
        elif clean_strategy == "exploration":
            profile.update({"semantic_weight": 0.55, "lexical_weight": 0.2, "recency_weight": 0.17, "access_weight": 0.08})
        elif clean_strategy == "priority":
            profile.update({"semantic_weight": 0.66, "lexical_weight": 0.14, "recency_weight": 0.08, "access_weight": 0.12})
        elif clean_strategy == "adaptive" or bool(use_adaptive):
            profile.update({key: float(value) for key, value in self._adaptive_weights.items() if key in profile})
        return self._normalize_weight_map(profile)

    def _retune_adaptive_weights(self) -> None:
        success_ema = max(0.0, min(float(self._feedback_state.get("success_ema", 0.5) or 0.5), 1.0))
        reward_ema = max(-1.0, min(float(self._feedback_state.get("reward_ema", 0.0) or 0.0), 1.0))
        count = max(0.0, float(self._feedback_state.get("count", 0.0) or 0.0))

        semantic = 0.72
        lexical = 0.14
        recency = 0.09
        access = 0.05

        if count >= 3.0:
            if success_ema < 0.45:
                lexical += 0.07
                recency += 0.05
                semantic -= 0.09
            elif success_ema > 0.72:
                semantic += 0.06
                recency -= 0.02
                lexical -= 0.02
            if reward_ema < -0.12:
                recency += 0.04
                access += 0.03
                semantic -= 0.05
            elif reward_ema > 0.16:
                semantic += 0.04
                lexical += 0.01
                access -= 0.02
        self._adaptive_weights = self._normalize_weight_map(
            {
                "semantic_weight": semantic,
                "lexical_weight": lexical,
                "recency_weight": recency,
                "access_weight": access,
            }
        )

    def _diversify_results(
        self,
        rows: List[Dict[str, Any]],
        *,
        diversify_by: str,
        top_k: int,
        max_per_group: int,
    ) -> List[Dict[str, Any]]:
        bounded_top_k = max(1, min(int(top_k), 1000))
        cap = max(1, min(int(max_per_group or 2), 50))
        selected: List[Dict[str, Any]] = []
        counts: Dict[str, int] = {}
        leftovers: List[Dict[str, Any]] = []
        for row in rows:
            group = self._result_group_key(row, diversify_by=diversify_by)
            current = int(counts.get(group, 0))
            if current < cap:
                selected.append(row)
                counts[group] = current + 1
            else:
                leftovers.append(row)
            if len(selected) >= bounded_top_k:
                return selected[:bounded_top_k]
        if len(selected) < bounded_top_k and leftovers:
            selected.extend(leftovers[: bounded_top_k - len(selected)])
        return selected[:bounded_top_k]

    @staticmethod
    def _result_group_key(row: Dict[str, Any], *, diversify_by: str) -> str:
        mode = str(diversify_by or "").strip().lower()
        if mode in {"tag", "tags"}:
            tags = row.get("tags", [])
            if isinstance(tags, list) and tags:
                normalized = [str(item).strip().lower() for item in tags if str(item).strip()]
                if normalized:
                    return normalized[0]
            return "__tag:none__"
        metadata = row.get("metadata", {})
        if mode.startswith("metadata:") and isinstance(metadata, dict):
            key = mode.split("metadata:", 1)[1].strip()
            value = str(metadata.get(key, "")).strip().lower()
            return value or f"__meta:{key}:none__"
        if mode and isinstance(metadata, dict):
            value = str(metadata.get(mode, "")).strip().lower()
            if value:
                return value
        return "__group:default__"

    def _record_search_journal(
        self,
        *,
        lexical_query: str,
        top_k: int,
        strategy: str,
        weights: Dict[str, float],
        result_count: int,
        latency_ms: float,
        diversify_by: str,
        prefer_tags: List[str],
    ) -> None:
        row = {
            "at": time.time(),
            "query": str(lexical_query or "")[:320],
            "top_k": int(top_k),
            "strategy": str(strategy or "").strip().lower() or "manual",
            "weights": {
                "semantic_weight": round(float(weights.get("semantic_weight", 0.0) or 0.0), 6),
                "lexical_weight": round(float(weights.get("lexical_weight", 0.0) or 0.0), 6),
                "recency_weight": round(float(weights.get("recency_weight", 0.0) or 0.0), 6),
                "access_weight": round(float(weights.get("access_weight", 0.0) or 0.0), 6),
            },
            "result_count": int(max(0, result_count)),
            "latency_ms": round(max(0.0, float(latency_ms)), 6),
            "diversify_by": str(diversify_by or "").strip().lower(),
            "prefer_tags": list(prefer_tags[:12]),
            "adaptive_success_ema": round(float(self._feedback_state.get("success_ema", 0.5) or 0.5), 6),
            "adaptive_reward_ema": round(float(self._feedback_state.get("reward_ema", 0.0) or 0.0), 6),
        }
        self._search_journal.append(row)
        if len(self._search_journal) > self._max_search_journal:
            self._search_journal = self._search_journal[-self._max_search_journal :]
