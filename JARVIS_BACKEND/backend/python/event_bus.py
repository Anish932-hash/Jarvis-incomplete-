import asyncio
import inspect
import itertools
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.python.utils.logger import Logger

EventHandler = Callable[[Dict[str, Any]], Any]


class EventBus:
    """
    Async event bus with optional router dispatch fallback.
    """

    def __init__(self, autostart: bool = False, max_queue_size: int = 2048) -> None:
        self.logger = Logger.get_logger("EventBus")
        bounded_size = max(32, int(max_queue_size))
        self.queue: asyncio.PriorityQueue[Tuple[int, int, str, Dict[str, Any]]] = asyncio.PriorityQueue(maxsize=bounded_size)
        self._counter = itertools.count(1)
        self.running = False
        self.worker_task: Optional[asyncio.Task] = None
        self.subscribers: Dict[str, List[EventHandler]] = defaultdict(list)
        self.router: Any = None
        self._autostart = autostart

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.worker_task = asyncio.create_task(self._worker(), name="event-bus-worker")
        self.logger.info("EventBus worker online.")

    def set_router(self, router: Any) -> None:
        self.router = router

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        name = str(event_type or "").strip()
        if not name:
            raise ValueError("event_type is required")
        if not callable(handler):
            raise TypeError("handler must be callable")
        handlers = self.subscribers[name]
        if handler in handlers:
            return
        handlers.append(handler)

    def unsubscribe(self, event_type: str, handler: EventHandler) -> bool:
        name = str(event_type or "").strip()
        if not name:
            return False
        handlers = self.subscribers.get(name, [])
        if handler not in handlers:
            return False
        handlers.remove(handler)
        if not handlers and name in self.subscribers:
            del self.subscribers[name]
        return True

    async def emit(self, *args: Any, priority: int = 50) -> str:
        """
        Backward-compatible signatures:
        - emit(priority, event_type, payload)
        - emit(event_type, payload, priority=?)
        """
        if len(args) == 3 and isinstance(args[0], int):
            event_priority = int(args[0])
            event_type = str(args[1]).strip()
            payload = self._normalize_payload(args[2])
        elif len(args) >= 2:
            event_priority = priority
            event_type = str(args[0]).strip()
            payload = self._normalize_payload(args[1])
        else:
            raise ValueError("emit requires (event_type, payload) or (priority, event_type, payload)")
        if not event_type:
            raise ValueError("event_type is required")

        if self._autostart and not self.running:
            await self.start()
        elif not self.running:
            await self.start()

        event_id = str(next(self._counter))
        try:
            self.queue.put_nowait((event_priority, int(event_id), event_type, payload))
        except asyncio.QueueFull:
            self.logger.warning("EventBus queue is full; waiting for worker capacity.")
            await self.queue.put((event_priority, int(event_id), event_type, payload))
        return event_id

    async def request(self, event_type: str, payload: Dict[str, Any] | None = None) -> Any:
        """
        Direct request/response dispatch bypassing queue.
        Returns first handler result, all handler results, or router result.
        Raises when no route is available.
        """
        name = str(event_type or "").strip()
        if not name:
            raise ValueError("event_type is required")
        return await self._dispatch(name, self._normalize_payload(payload), strict=True)

    async def _worker(self) -> None:
        while self.running:
            try:
                _, _, event_type, payload = await self.queue.get()
                await self._dispatch(event_type, payload)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                self.logger.error(f"EventBus worker error: {exc}")

    async def _dispatch(self, event_type: str, payload: Dict[str, Any], strict: bool = False) -> Any:
        handlers = self.subscribers.get(event_type, [])
        if handlers:
            results: list[Any] = []
            for handler in handlers:
                results.append(await self._run_handler(handler, payload))
            return results[0] if len(results) == 1 else results

        if self.router is not None:
            try:
                return await self.router.dispatch(event_type, payload)
            except Exception as exc:  # noqa: BLE001
                self.logger.error(f"Router dispatch failed for {event_type}: {exc}")
                if strict:
                    raise
                return None

        message = f"No subscribers/router for event: {event_type}"
        if strict:
            raise ValueError(message)
        self.logger.warning(message)
        return None

    async def _run_handler(self, handler: EventHandler, payload: Dict[str, Any]) -> Any:
        if inspect.iscoroutinefunction(handler):
            return await handler(payload)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, handler, payload)

    @staticmethod
    def _normalize_payload(payload: Any) -> Dict[str, Any]:
        if payload is None:
            return {}
        if isinstance(payload, dict):
            return payload
        return {"value": payload}

    async def join(self) -> None:
        await self.queue.join()

    def pending_count(self) -> int:
        return self.queue.qsize()

    async def shutdown(self) -> None:
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        self.logger.info("EventBus stopped.")
