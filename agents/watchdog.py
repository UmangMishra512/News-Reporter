"""
agents/watchdog.py — Watchdog AI.

Observes the browser_worker in real time and detects pathological
states: infinite loops, frozen pages, captcha loops, etc.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus

log = AgentLogger("watchdog")

MAX_SAME_URL_SECONDS   = 45   # if browser stuck on same URL > 45s → alert
MAX_ACTION_COUNT       = 50   # if >50 actions without completing → loop
CHECK_INTERVAL_SECONDS = 5    # how often watchdog checks


class WatchdogAgent:
    """
    Observes the BrowserWorker via shared state.
    Triggers emergency stop and incident reporting on anomalies.
    """

    AGENT_NAME = "watchdog"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus           = bus or get_bus()
        self._db            = get_db()
        self._browser_worker = None   # set via register()
        self._running        = False
        self._last_url: Optional[str]     = None
        self._same_url_since: Optional[datetime] = None
        self._last_action_count = 0
        self._stagnant_count    = 0
        self._alerts_sent       = 0
        self._watch_task:      Optional[asyncio.Task] = None
        self._heartbeat_task:  Optional[asyncio.Task] = None

    def register_browser_worker(self, worker) -> None:
        """Call this after browser_worker is created."""
        self._browser_worker = worker
        log.info("Watchdog registered with BrowserWorker.")

    async def start(self) -> None:
        self._running = True
        self._watch_task     = asyncio.create_task(self._watch_loop(), name="watchdog_watch")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="watchdog_hb")
        log.info("Watchdog started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        for t in (self._watch_task, self._heartbeat_task):
            if t:
                t.cancel()

    # ── Watch loop ────────────────────────────────────────────

    async def _watch_loop(self) -> None:
        while self._running:
            try:
                if self._browser_worker:
                    await self._check_browser()
            except Exception as e:
                log.error(f"Watchdog check error: {e}")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

    async def _check_browser(self) -> None:
        worker = self._browser_worker
        current_url    = worker.current_url
        action_count   = worker.action_count
        now            = datetime.now(timezone.utc)

        # ── Frozen URL detection ──────────────────────────────
        if current_url:
            if current_url != self._last_url:
                # URL changed — healthy
                self._last_url       = current_url
                self._same_url_since = now
                self._stagnant_count = 0
            else:
                # Same URL — check how long
                elapsed = (now - self._same_url_since).total_seconds()
                if elapsed > MAX_SAME_URL_SECONDS:
                    log.warning(
                        f"Browser frozen on URL for {elapsed:.0f}s: {current_url[:80]}"
                    )
                    await self._alert(
                        alert_type="frozen_browser",
                        url=current_url,
                        message=(
                            f"Browser stuck on same URL for {elapsed:.0f}s. "
                            f"Triggering emergency stop."
                        ),
                    )
                    await worker.emergency_stop()
                    self._same_url_since = now  # reset timer
        else:
            # No URL = not active
            self._last_url       = None
            self._same_url_since = None
            self._stagnant_count = 0

        # ── Infinite action loop detection ────────────────────
        if action_count > 0:
            if action_count == self._last_action_count:
                self._stagnant_count += 1
                if self._stagnant_count >= 6:  # 6 × 5s = 30s of no progress
                    log.warning(
                        f"Action count stagnant at {action_count} for 30s. Possible loop."
                    )
                    await self._alert(
                        alert_type="action_loop",
                        url=current_url or "",
                        message=f"Browser action count stagnant at {action_count}",
                    )
                    await worker.emergency_stop()
                    self._stagnant_count = 0
            elif action_count > MAX_ACTION_COUNT:
                log.warning(f"Excessive browser actions: {action_count}")
                await self._alert(
                    alert_type="excessive_actions",
                    url=current_url or "",
                    message=f"Browser performed {action_count} actions in one task",
                )
                await worker.emergency_stop()
            else:
                self._stagnant_count = 0

        self._last_action_count = action_count

    async def _alert(self, alert_type: str, url: str, message: str) -> None:
        self._alerts_sent += 1
        log.incident(
            f"Watchdog alert: {alert_type}",
            alert_type=alert_type,
            url=url,
            message=message,
        )
        await self._bus.publish(Event(
            Topics.WATCHDOG_ALERT,
            payload={
                "agent":      self.AGENT_NAME,
                "alert_type": alert_type,
                "url":        url,
                "message":    message,
                "timestamp":  datetime.utcnow().isoformat(),
            },
            source=self.AGENT_NAME,
        ))

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    metadata={"alerts_sent": self._alerts_sent},
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(30)
