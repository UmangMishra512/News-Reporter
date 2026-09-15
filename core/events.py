"""
core/events.py — Asyncio-based internal event bus.

Agents publish events and subscribe to topics. No direct function calls
between agents; everything flows through the bus for full decoupling.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  Event topic constants
# ─────────────────────────────────────────────────────────────
class Topics:
    # Lifecycle
    AGENT_STARTED      = "agent.started"
    AGENT_STOPPED      = "agent.stopped"
    AGENT_HEARTBEAT    = "agent.heartbeat"
    AGENT_FAILED       = "agent.failed"
    AGENT_STALE        = "agent.stale"

    # Task lifecycle
    TASK_CREATED       = "task.created"
    TASK_ASSIGNED      = "task.assigned"
    TASK_STARTED       = "task.started"
    TASK_DONE          = "task.done"
    TASK_FAILED        = "task.failed"
    TASK_RETRY         = "task.retry"
    TASK_CANCELLED     = "task.cancelled"
    TASK_ESCALATED     = "task.escalated"

    # Data pipeline
    ARTICLES_FETCHED   = "articles.fetched"
    ARTICLES_DEDUPED   = "articles.deduped"
    ARTICLES_VERIFIED  = "articles.verified"
    ARTICLES_SUMMARIZED= "articles.summarized"
    ARTICLES_RANKED    = "articles.ranked"
    ARTICLES_QA_PASSED = "articles.qa_passed"

    # Publishing
    RUN_STARTED        = "run.started"
    RUN_COMPLETED      = "run.completed"
    PUBLISHED          = "published"

    # Health
    HEALTH_CHECK_DONE  = "health.check_done"
    RESOURCE_UNHEALTHY = "resource.unhealthy"
    RESOURCE_RECOVERED = "resource.recovered"

    # Incidents
    INCIDENT_CREATED   = "incident.created"
    INCIDENT_RESOLVED  = "incident.resolved"

    # Watchdog
    WATCHDOG_ALERT     = "watchdog.alert"
    BROWSER_FROZEN     = "browser.frozen"

    # Recovery
    RECOVERY_STARTED   = "recovery.started"
    RECOVERY_DONE      = "recovery.done"
    RECOVERY_FAILED    = "recovery.failed"

    # Director alerts
    NOTIFY_DIRECTOR    = "director.notify"


class Event:
    """A single event flowing through the bus."""
    __slots__ = ("topic", "payload", "source", "timestamp", "id")

    def __init__(
        self,
        topic: str,
        payload: Optional[Dict[str, Any]] = None,
        source: str = "unknown",
    ) -> None:
        import uuid
        self.id        = str(uuid.uuid4())
        self.topic     = topic
        self.payload   = payload or {}
        self.source    = source
        self.timestamp = datetime.utcnow()

    def __repr__(self) -> str:
        return f"<Event topic={self.topic!r} source={self.source!r}>"


class EventBus:
    """
    Asyncio-native pub/sub event bus.

    Usage:
        bus = EventBus()

        # Subscribe
        async def my_handler(event: Event):
            ...
        bus.subscribe(Topics.TASK_FAILED, my_handler)

        # Publish
        await bus.publish(Event(Topics.TASK_FAILED, payload={"task_id": "123"}, source="rss_worker"))
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._wildcard_subscribers: List[Callable] = []
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def subscribe(self, topic: str, handler: Callable) -> None:
        """Subscribe a coroutine handler to a topic."""
        self._subscribers[topic].append(handler)
        log.debug(f"EventBus: subscribed {handler.__qualname__!r} to {topic!r}")

    def subscribe_all(self, handler: Callable) -> None:
        """Subscribe to ALL events (useful for loggers, monitors)."""
        self._wildcard_subscribers.append(handler)

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        handlers = self._subscribers.get(topic, [])
        if handler in handlers:
            handlers.remove(handler)

    async def publish(self, event: Event) -> None:
        """Non-blocking enqueue. Returns immediately."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            log.warning(f"EventBus queue full — dropping event: {event.topic}")

    async def publish_many(self, events: List[Event]) -> None:
        for event in events:
            await self.publish(event)

    async def start(self) -> None:
        """Start the dispatch loop."""
        self._running = True
        self._task = asyncio.create_task(self._dispatch_loop(), name="event_bus")
        log.info("EventBus started.")

    async def stop(self) -> None:
        """Graceful shutdown — drain the queue first."""
        try:
            # Drain queue while dispatch loop is still running
            await asyncio.wait_for(self._queue.join(), timeout=5.0)
        except asyncio.TimeoutError:
            log.warning("EventBus stop: queue drain timed out after 5.0s, draining remaining events")
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except (asyncio.QueueEmpty, ValueError):
                    break
        finally:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
        log.info("EventBus stopped.")

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await self._dispatch(event)
            except Exception as exc:
                log.exception(f"EventBus dispatch error: {exc}")
            finally:
                self._queue.task_done()

    async def _dispatch(self, event: Event) -> None:
        """Fire all handlers for this event."""
        handlers = (
            self._subscribers.get(event.topic, [])
            + self._wildcard_subscribers
        )
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as exc:
                log.exception(
                    f"EventBus handler error in {handler.__qualname__!r} "
                    f"for topic {event.topic!r}: {exc}"
                )


# Singleton — shared across all agents
_bus: Optional[EventBus] = None


def get_bus() -> EventBus:
    global _bus
    if _bus is None:
        _bus = EventBus()
    return _bus
