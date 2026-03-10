from __future__ import annotations

import asyncio

import pytest

from backend.python.event_bus import EventBus


def test_request_returns_subscriber_result() -> None:
    async def _handler(payload: dict) -> dict:
        return {"status": "success", "echo": payload.get("value")}

    bus = EventBus()
    bus.subscribe("ping", _handler)

    result = asyncio.run(bus.request("ping", {"value": 7}))
    assert result["status"] == "success"
    assert result["echo"] == 7


def test_emit_respects_priority_order() -> None:
    observed: list[str] = []

    async def _handler(payload: dict) -> None:
        observed.append(str(payload.get("name")))

    async def _scenario() -> None:
        bus = EventBus()
        bus.subscribe("evt", _handler)
        await bus.start()
        await bus.emit(80, "evt", {"name": "low-priority"})
        await bus.emit(10, "evt", {"name": "high-priority"})
        await bus.join()
        await bus.shutdown()

    asyncio.run(_scenario())
    assert observed == ["high-priority", "low-priority"]


def test_unsubscribe_and_missing_request_route() -> None:
    bus = EventBus()

    def _handler(_payload: dict) -> dict:
        return {"status": "ok"}

    bus.subscribe("evt", _handler)
    removed = bus.unsubscribe("evt", _handler)
    assert removed is True

    with pytest.raises(ValueError):
        asyncio.run(bus.request("evt", {"value": 1}))


def test_request_uses_router_fallback() -> None:
    class _Router:
        async def dispatch(self, event_type: str, payload: dict) -> dict:
            return {"event": event_type, "payload": payload, "status": "success"}

    bus = EventBus()
    bus.set_router(_Router())
    result = asyncio.run(bus.request("agent.action", {"action": "time_now"}))
    assert result["status"] == "success"
    assert result["event"] == "agent.action"
