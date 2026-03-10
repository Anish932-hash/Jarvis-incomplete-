import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence

from .contracts import ActionRequest, ActionResult
from backend.python.utils.logger import Logger

ToolHandler = Callable[[Dict[str, Any]], Any]


@dataclass(slots=True)
class ToolDefinition:
    name: str
    handler: ToolHandler
    description: str = ""
    risk: str = "low"
    requires_confirmation: bool = False
    required_args: tuple[str, ...] = ()


class ToolRegistry:
    def __init__(self) -> None:
        self.logger = Logger.get_logger("ToolRegistry")
        self._tools: Dict[str, ToolDefinition] = {}

    def register(
        self,
        name: str,
        handler: ToolHandler,
        *,
        description: str = "",
        risk: str = "low",
        requires_confirmation: bool = False,
        required_args: Sequence[str] | None = None,
    ) -> None:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Tool name cannot be empty.")
        if not callable(handler):
            raise TypeError(f"Tool handler for '{normalized_name}' must be callable.")

        normalized_required: list[str] = []
        if required_args:
            for item in required_args:
                arg_name = str(item or "").strip()
                if not arg_name:
                    continue
                if arg_name not in normalized_required:
                    normalized_required.append(arg_name)

        if normalized_name in self._tools:
            self.logger.warning(f"Overwriting existing tool registration: {normalized_name}")

        self._tools[normalized_name] = ToolDefinition(
            name=normalized_name,
            handler=handler,
            description=description,
            risk=self._normalize_risk(risk),
            requires_confirmation=requires_confirmation,
            required_args=tuple(normalized_required),
        )

    def has(self, name: str) -> bool:
        return str(name or "").strip() in self._tools

    def get(self, name: str) -> Optional[ToolDefinition]:
        return self._tools.get(str(name or "").strip())

    def list_tools(self) -> Dict[str, ToolDefinition]:
        return dict(self._tools)

    async def execute(self, request: ActionRequest, timeout_s: int = 30) -> ActionResult:
        action_name = str(request.action or "").strip()
        tool = self._tools.get(action_name)
        if tool is None:
            return ActionResult(
                action=action_name or request.action,
                status="failed",
                error=f"Tool not registered: {action_name or request.action}",
            )

        started = time.perf_counter()
        if not isinstance(request.args, dict):
            duration_ms = int((time.perf_counter() - started) * 1000)
            return ActionResult(
                action=action_name,
                status="failed",
                error="Tool arguments must be a JSON object.",
                duration_ms=duration_ms,
                evidence={"tool": tool.name},
            )

        args = dict(request.args)
        if tool.required_args:
            missing = [key for key in tool.required_args if key not in args]
            if missing:
                duration_ms = int((time.perf_counter() - started) * 1000)
                missing_csv = ", ".join(missing)
                return ActionResult(
                    action=action_name,
                    status="failed",
                    error=f"Missing required args for {tool.name}: {missing_csv}",
                    duration_ms=duration_ms,
                    evidence={"tool": tool.name, "missing_args": missing},
                )

        try:
            result = await self._run_handler(tool.handler, args, timeout_s)
            duration_ms = int((time.perf_counter() - started) * 1000)

            if isinstance(result, dict):
                raw_status = str(result.get("status", "success")).strip().lower()
                if raw_status in {"blocked", "skipped"}:
                    status = raw_status
                elif raw_status in {"error", "failed"}:
                    status = "failed"
                else:
                    status = "success"
                error = str(result.get("message") or result.get("error") or "").strip() or None
                if status == "success":
                    error = None
                return ActionResult(
                    action=action_name,
                    status=status,
                    output=result,
                    error=error,
                    duration_ms=duration_ms,
                    evidence={"tool": tool.name},
                )

            return ActionResult(
                action=action_name,
                status="success",
                output={"result": result},
                duration_ms=duration_ms,
                evidence={"tool": tool.name},
            )
        except Exception as exc:  # noqa: BLE001
            duration_ms = int((time.perf_counter() - started) * 1000)
            return ActionResult(
                action=action_name,
                status="failed",
                error=str(exc),
                duration_ms=duration_ms,
                evidence={"tool": tool.name},
            )

    async def _run_handler(self, handler: ToolHandler, args: Dict[str, Any], timeout_s: int) -> Any:
        if inspect.iscoroutinefunction(handler):
            return await asyncio.wait_for(handler(args), timeout=timeout_s)

        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(loop.run_in_executor(None, handler, args), timeout=timeout_s)

    def _normalize_risk(self, risk: str) -> str:
        normalized = str(risk or "").strip().lower()
        if normalized in {"low", "medium", "high", "critical"}:
            return normalized
        if normalized:
            self.logger.warning(f"Unknown risk level '{risk}' registered; defaulting to 'low'.")
        return "low"
