"""
agents/supervisor.py — Executive Supervisor AI.

The CEO of the platform. Monitors all agent heartbeats,
detects failures, triggers recovery, generates incident reports,
and DMs the Director only when human intervention is truly required.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from config.settings import get_settings
from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Severity

from agents.recovery_engine import get_recovery_engine

log = AgentLogger("supervisor")

KNOWN_AGENTS = [
    "ops_manager", "source_discovery", "rss_worker", "api_worker",
    "browser_worker", "dedup_agent", "fact_verifier", "summarizer",
    "ranker", "qa_agent", "health_monitor", "watchdog",
    "failure_analyzer", "website_knowledge",
]


class ExecutiveSupervisor:
    """
    Monitors all agents via heartbeats. Acts if any agent is stale.
    """

    AGENT_NAME = "supervisor"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus       = bus or get_bus()
        self._db        = get_db()
        self._settings  = get_settings()
        self._recovery  = get_recovery_engine()
        self._running   = False
        self._stale_counts: Dict[str, int] = {}  # agent → consecutive stale count
        self._director_notified: Dict[str, int] = {}  # to avoid spam
        self._monitor_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True

        # Subscribe to important events
        self._bus.subscribe(Topics.TASK_ESCALATED,   self._on_escalated)
        self._bus.subscribe(Topics.RECOVERY_FAILED,  self._on_recovery_failed)
        self._bus.subscribe(Topics.RESOURCE_UNHEALTHY, self._on_unhealthy)
        self._bus.subscribe(Topics.NOTIFY_DIRECTOR,  self._on_notify_director)

        self._monitor_task = asyncio.create_task(
            self._monitor_loop(), name="supervisor_monitor"
        )
        log.info("Executive Supervisor started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()

    # ── Heartbeat Monitor ────────────────────────────────────

    async def _monitor_loop(self) -> None:
        stale_threshold = self._settings.agent_stale_threshold_seconds
        check_interval  = self._settings.heartbeat_check_interval

        while self._running:
            try:
                await self._check_all_heartbeats(stale_threshold)
                await self._check_open_incidents()
            except Exception as e:
                log.error(f"Supervisor monitor error: {e}")
            await asyncio.sleep(check_interval)

    async def _check_all_heartbeats(self, stale_threshold: float) -> None:
        try:
            heartbeats = await self._db.get_all_heartbeats()
        except Exception as e:
            log.error(f"Failed to fetch heartbeats: {e}")
            return

        now = datetime.now(timezone.utc)
        known = {hb["agent_name"]: hb for hb in heartbeats}

        for agent_name in KNOWN_AGENTS:
            hb = known.get(agent_name)
            if not hb:
                # Never registered — might still be starting up
                continue

            try:
                last_ts = datetime.fromisoformat(
                    hb["timestamp"].replace("Z", "+00:00")
                )
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            age = (now - last_ts).total_seconds()

            if age > stale_threshold:
                count = self._stale_counts.get(agent_name, 0) + 1
                self._stale_counts[agent_name] = count
                log.warning(
                    f"Agent STALE: {agent_name} (last seen {age:.0f}s ago, stale #{count})"
                )
                if count <= 3:
                    await self._recovery.handle_agent_stale(agent_name)
                else:
                    # Recovery exhausted — notify Director
                    await self._notify_director(
                        f"⚠️ **{agent_name}** is unresponsive after {count} recovery attempts. "
                        f"Last heartbeat: {age:.0f}s ago. Manual intervention may be needed.",
                        severity="critical",
                    )
            else:
                self._stale_counts[agent_name] = 0  # reset

    async def _check_open_incidents(self) -> None:
        try:
            incidents = await self._db.get_open_incidents()
            if len(incidents) > 20:
                log.warning(f"High open incident count: {len(incidents)}")
                await self._notify_director(
                    f"📊 **Incident backlog alert**: {len(incidents)} unresolved incidents. "
                    f"Review `storage/logs/incidents.log` for details.",
                    severity="high",
                )
        except Exception as e:
            log.error(f"Incident check error: {e}")

    # ── Event Handlers ───────────────────────────────────────

    async def _on_escalated(self, event: Event) -> None:
        payload = event.payload
        agent   = payload.get("agent", "unknown")
        error   = payload.get("error_message", "unknown error")
        log.error(f"Escalated failure from {agent}: {error}")
        await self._notify_director(
            f"🚨 **Escalated failure**: Agent `{agent}` encountered an unrecoverable error.\n"
            f"Error: {error}\nCheck `storage/logs/incidents.log` for details.",
            severity="high",
        )

    async def _on_recovery_failed(self, event: Event) -> None:
        agent  = event.payload.get("agent", "unknown")
        reason = event.payload.get("reason", "unknown")
        await self._notify_director(
            f"🔴 **Recovery failed**: Could not restart `{agent}`. Reason: {reason}",
            severity="critical",
        )

    async def _on_unhealthy(self, event: Event) -> None:
        payload  = event.payload
        resource = payload.get("resource_name", "unknown")
        rtype    = payload.get("resource_type", "")
        error    = payload.get("error", "")
        log.warning(f"Resource unhealthy: [{rtype}] {resource} — {error}")
        # Only notify Director for critical resources
        if rtype in ("network", "database"):
            await self._notify_director(
                f"⚠️ **{rtype.upper()} health alert**: `{resource}` is unhealthy.\nError: {error}",
                severity="high",
            )

    async def _on_notify_director(self, event: Event) -> None:
        msg      = event.payload.get("message", "System alert")
        severity = event.payload.get("severity", "medium")
        await self._notify_director(msg, severity)

    # ── Director Alert ──────────────────────────────────────

    async def _notify_director(self, message: str, severity: str = "medium") -> None:
        # Throttle: same severity key not more than once per 10 minutes
        key = message[:50]
        last = self._director_notified.get(key, 0)
        import time
        if time.time() - last < 600:
            return
        self._director_notified[key] = time.time()

        log.incident(f"Director notification: {message[:120]}")

        # Publish alert to event bus (dashboard can pick this up)
        await self._bus.publish(Event(
            Topics.PUBLISHED,
            payload={"type": "alert", "message": message, "severity": severity},
            source=self.AGENT_NAME,
        ))
