from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, List, Literal, Optional

from .task_state import GoalStatus, StepStatus

ActionOutcome = Literal["success", "failed", "blocked", "skipped"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class ActionRequest:
    action: str
    args: Dict[str, Any] = field(default_factory=dict)
    source: str = "planner"
    correlation_id: str = ""
    requested_at: str = field(default_factory=utc_now_iso)
    metadata: Dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""
    deadline_at: str = ""
    trace: Dict[str, Any] = field(default_factory=dict)

    def normalized_action(self) -> str:
        return str(self.action or "").strip().lower()

    def args_fingerprint(self) -> str:
        payload = self.args if isinstance(self.args, dict) else {}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def dedupe_key(self) -> str:
        if str(self.idempotency_key or "").strip():
            return str(self.idempotency_key).strip()
        action = self.normalized_action()
        source = str(self.source or "").strip().lower()
        return f"{source}:{action}:{self.args_fingerprint()[:20]}"

    def is_expired(self, *, now_iso: str = "") -> bool:
        deadline = str(self.deadline_at or "").strip()
        if not deadline:
            return False
        try:
            now = datetime.fromisoformat(str(now_iso or utc_now_iso()))
            target = datetime.fromisoformat(deadline)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            return now.astimezone(timezone.utc) >= target.astimezone(timezone.utc)
        except Exception:
            return False

    def remaining_budget_ms(self, *, now_iso: str = "") -> Optional[int]:
        deadline = str(self.deadline_at or "").strip()
        if not deadline:
            return None
        try:
            now = datetime.fromisoformat(str(now_iso or utc_now_iso()))
            target = datetime.fromisoformat(deadline)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            remaining = (target.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds() * 1000.0
            return max(0, int(round(remaining)))
        except Exception:
            return None


@dataclass(slots=True)
class ActionResult:
    action: str
    status: ActionOutcome
    output: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    attempt: int = 1
    completed_at: str = field(default_factory=utc_now_iso)
    retryable: bool = False
    risk_level: str = ""
    cost_units: float = 0.0

    def is_success(self) -> bool:
        return str(self.status or "").strip().lower() == "success"

    def is_failure(self) -> bool:
        return str(self.status or "").strip().lower() in {"failed", "blocked"}

    def summary(self) -> Dict[str, Any]:
        return {
            "action": str(self.action or "").strip(),
            "status": str(self.status or "").strip().lower(),
            "attempt": int(self.attempt),
            "duration_ms": int(self.duration_ms),
            "retryable": bool(self.retryable),
            "risk_level": str(self.risk_level or "").strip().lower(),
            "has_error": bool(str(self.error or "").strip()),
            "cost_units": round(float(self.cost_units or 0.0), 6),
        }

    def error_code(self) -> str:
        direct = str(self.output.get("error_code", "")) if isinstance(self.output, dict) else ""
        if direct.strip():
            return direct.strip().lower()
        ev = self.evidence if isinstance(self.evidence, dict) else {}
        code = str(ev.get("error_code", "")).strip().lower()
        if code:
            return code
        text = str(self.error or "").strip().lower()
        if not text:
            return ""
        token = text.replace(" ", "_").split(":")[0]
        return token[:72]


@dataclass(slots=True)
class PlanStep:
    step_id: str
    action: str
    args: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    verify: Dict[str, Any] = field(default_factory=dict)
    status: StepStatus = StepStatus.PENDING
    can_retry: bool = True
    max_retries: int = 2
    timeout_s: int = 30
    priority: int = 0
    preconditions: Dict[str, Any] = field(default_factory=dict)
    postconditions: Dict[str, Any] = field(default_factory=dict)
    rollback: Dict[str, Any] = field(default_factory=dict)
    guardrails: Dict[str, Any] = field(default_factory=dict)
    budget: Dict[str, Any] = field(default_factory=dict)

    def is_ready(self, completed_step_ids: set[str] | List[str]) -> bool:
        completed = set(completed_step_ids)
        return all(dep in completed for dep in (self.depends_on or []))

    def effective_timeout_s(self, *, default_timeout_s: int = 30) -> int:
        value = int(self.timeout_s or 0)
        if value > 0:
            return max(1, min(value, 3600))
        return max(1, min(int(default_timeout_s), 3600))

    def to_runtime_dict(self) -> Dict[str, Any]:
        return {
            "step_id": str(self.step_id or "").strip(),
            "action": str(self.action or "").strip(),
            "args": dict(self.args or {}),
            "depends_on": list(self.depends_on or []),
            "verify": dict(self.verify or {}),
            "status": str(self.status.value if hasattr(self.status, "value") else self.status),
            "can_retry": bool(self.can_retry),
            "max_retries": int(self.max_retries),
            "timeout_s": int(self.timeout_s),
            "priority": int(self.priority),
            "preconditions": dict(self.preconditions or {}),
            "postconditions": dict(self.postconditions or {}),
            "rollback": dict(self.rollback or {}),
            "guardrails": dict(self.guardrails or {}),
            "budget": dict(self.budget or {}),
        }

    def risk_weight(self) -> float:
        guardrails = self.guardrails if isinstance(self.guardrails, dict) else {}
        verify = self.verify if isinstance(self.verify, dict) else {}
        level = str(guardrails.get("risk_level", "")).strip().lower()
        strict = str(verify.get("mode", verify.get("strictness", ""))).strip().lower()
        base = 0.15
        if level in {"medium", "moderate"}:
            base = 0.45
        elif level in {"high", "critical"}:
            base = 0.78
        if strict in {"strict", "paranoid", "high"}:
            base = min(1.0, base + 0.08)
        if bool(guardrails.get("requires_approval", False)):
            base = min(1.0, base + 0.12)
        return round(max(0.0, min(1.0, base)), 6)

    def estimated_cost_units(self) -> float:
        budget = self.budget if isinstance(self.budget, dict) else {}
        explicit = budget.get("cost_units")
        try:
            if explicit is not None:
                value = float(explicit)
                return round(max(0.0, value), 6)
        except Exception:
            pass
        timeout = float(self.effective_timeout_s(default_timeout_s=30))
        retry_factor = 1.0 + max(0.0, float(self.max_retries) * 0.32 if bool(self.can_retry) else 0.0)
        risk_factor = 1.0 + float(self.risk_weight()) * 1.8
        cost = (timeout / 30.0) * retry_factor * risk_factor
        return round(max(0.05, min(1000.0, cost)), 6)


@dataclass(slots=True)
class ExecutionPlan:
    plan_id: str
    goal_id: str
    intent: str
    steps: List[PlanStep]
    context: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)
    profile: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> Dict[str, Any]:
        errors: List[str] = []
        step_ids: List[str] = []
        for step in self.steps or []:
            step_id = str(step.step_id or "").strip()
            if not step_id:
                errors.append("step_id_missing")
                continue
            step_ids.append(step_id)
        duplicates = sorted({item for item in step_ids if step_ids.count(item) > 1})
        if duplicates:
            errors.append(f"duplicate_step_ids:{','.join(duplicates)}")

        known = set(step_ids)
        for step in self.steps or []:
            step_id = str(step.step_id or "").strip()
            for dep in step.depends_on or []:
                clean_dep = str(dep or "").strip()
                if clean_dep and clean_dep not in known:
                    errors.append(f"missing_dependency:{step_id}->{clean_dep}")

        if not errors:
            cycle = self._detect_cycle()
            if cycle:
                errors.append(f"cycle_detected:{'->'.join(cycle)}")
        return {
            "status": "success" if not errors else "error",
            "plan_id": str(self.plan_id or "").strip(),
            "step_count": len(self.steps or []),
            "errors": errors,
            "valid": not bool(errors),
        }

    def step_index(self) -> Dict[str, PlanStep]:
        rows: Dict[str, PlanStep] = {}
        for step in self.steps or []:
            step_id = str(step.step_id or "").strip()
            if step_id and step_id not in rows:
                rows[step_id] = step
        return rows

    def dependency_layers(self) -> List[List[str]]:
        rows = self.step_index()
        if not rows:
            return []
        indegree: Dict[str, int] = {step_id: 0 for step_id in rows}
        forward: Dict[str, List[str]] = {step_id: [] for step_id in rows}
        for step_id, step in rows.items():
            for dep in step.depends_on or []:
                dep_id = str(dep or "").strip()
                if dep_id in rows:
                    indegree[step_id] = int(indegree.get(step_id, 0)) + 1
                    forward.setdefault(dep_id, []).append(step_id)
        ready = sorted([sid for sid, value in indegree.items() if value <= 0])
        visited: set[str] = set()
        layers: List[List[str]] = []
        while ready:
            layer = list(ready)
            layers.append(layer)
            ready = []
            for node in layer:
                visited.add(node)
                for nxt in forward.get(node, []):
                    indegree[nxt] = max(0, int(indegree.get(nxt, 0)) - 1)
                    if indegree[nxt] == 0 and nxt not in visited and nxt not in ready:
                        ready.append(nxt)
            ready.sort()
        if len(visited) < len(rows):
            leftovers = sorted([sid for sid in rows if sid not in visited])
            if leftovers:
                layers.append(leftovers)
        return layers

    def critical_path_timeout_s(self, *, default_timeout_s: int = 30) -> int:
        rows = self.step_index()
        if not rows:
            return 0
        layers = self.dependency_layers()
        if not layers:
            return 0
        distances: Dict[str, int] = {}
        for layer in layers:
            for step_id in layer:
                step = rows.get(step_id)
                if step is None:
                    continue
                own = int(step.effective_timeout_s(default_timeout_s=default_timeout_s))
                deps = [str(dep or "").strip() for dep in (step.depends_on or []) if str(dep or "").strip() in rows]
                if not deps:
                    distances[step_id] = own
                    continue
                best_dep = max([int(distances.get(dep, 0)) for dep in deps] or [0])
                distances[step_id] = own + best_dep
        return max([int(value) for value in distances.values()] or [0])

    def risk_summary(self) -> Dict[str, Any]:
        steps = self.steps or []
        if not steps:
            return {
                "step_count": 0,
                "avg_risk_weight": 0.0,
                "high_risk_count": 0,
                "approval_required_count": 0,
                "estimated_total_cost_units": 0.0,
            }
        weights = [float(step.risk_weight()) for step in steps]
        high_risk = [step for step in steps if float(step.risk_weight()) >= 0.72]
        approval = [
            step
            for step in steps
            if isinstance(step.guardrails, dict) and bool(step.guardrails.get("requires_approval", False))
        ]
        total_cost = sum(float(step.estimated_cost_units()) for step in steps)
        return {
            "step_count": len(steps),
            "avg_risk_weight": round(sum(weights) / float(len(weights)), 6),
            "high_risk_count": len(high_risk),
            "approval_required_count": len(approval),
            "estimated_total_cost_units": round(total_cost, 6),
        }

    def runtime_contract(self) -> Dict[str, Any]:
        validity = self.validate()
        layers = self.dependency_layers()
        risk = self.risk_summary()
        return {
            "status": "success" if bool(validity.get("valid", False)) else "error",
            "plan_id": str(self.plan_id or "").strip(),
            "validity": validity,
            "execution_layers": layers,
            "execution_depth": len(layers),
            "critical_path_timeout_s": int(self.critical_path_timeout_s(default_timeout_s=30)),
            "risk": risk,
        }

    def _detect_cycle(self) -> List[str]:
        graph: Dict[str, List[str]] = {}
        for step in self.steps or []:
            step_id = str(step.step_id or "").strip()
            if not step_id:
                continue
            graph[step_id] = [str(dep or "").strip() for dep in (step.depends_on or []) if str(dep or "").strip()]

        visited: set[str] = set()
        stack: set[str] = set()

        def dfs(node: str, trail: List[str]) -> List[str]:
            if node in stack:
                try:
                    idx = trail.index(node)
                except ValueError:
                    idx = 0
                return trail[idx:] + [node]
            if node in visited:
                return []
            visited.add(node)
            stack.add(node)
            for dep in graph.get(node, []):
                if dep not in graph:
                    continue
                cycle = dfs(dep, trail + [dep])
                if cycle:
                    return cycle
            stack.remove(node)
            return []

        for node in graph.keys():
            cycle = dfs(node, [node])
            if cycle:
                return cycle
        return []


@dataclass(slots=True)
class GoalRequest:
    text: str
    source: str = "user"
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class GoalRecord:
    goal_id: str
    request: GoalRequest
    status: GoalStatus = GoalStatus.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    plan: Optional[ExecutionPlan] = None
    results: List[ActionResult] = field(default_factory=list)
    failure_reason: Optional[str] = None

    def is_terminal(self) -> bool:
        return self.status in {
            GoalStatus.COMPLETED,
            GoalStatus.FAILED,
            GoalStatus.BLOCKED,
            GoalStatus.CANCELLED,
        }

    def to_summary(self) -> Dict[str, Any]:
        return {
            "goal_id": str(self.goal_id or "").strip(),
            "status": str(self.status.value if hasattr(self.status, "value") else self.status),
            "source": str(self.request.source or "").strip(),
            "text": str(self.request.text or "").strip(),
            "created_at": str(self.request.created_at or ""),
            "started_at": str(self.started_at or ""),
            "completed_at": str(self.completed_at or ""),
            "failure_reason": str(self.failure_reason or ""),
            "result_count": len(self.results or []),
            "terminal": self.is_terminal(),
        }
