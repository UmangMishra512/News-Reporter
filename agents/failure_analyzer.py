"""
agents/failure_analyzer.py — Failure Analyzer AI.

Every failure becomes a permanent, structured incident record.
No error is ever silently ignored.
"""
from __future__ import annotations

import asyncio
import traceback
from datetime import datetime
from typing import Optional

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Incident, Severity

from agents.recovery_engine import classify_error, get_recovery_engine

log = AgentLogger("failure_analyzer")


class FailureAnalyzer:
    """
    Subscribes to all TASK_FAILED and AGENT_FAILED events.
    Persists every incident to the DB and triggers recovery.
    """

    AGENT_NAME = "failure_analyzer"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus = bus or get_bus()
        self._db  = get_db()
        self._recovery = get_recovery_engine()
        self._tasks_failed = 0
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        # Subscribe to failure events
        self._bus.subscribe(Topics.TASK_FAILED,  self._on_task_failed)
        self._bus.subscribe(Topics.AGENT_FAILED, self._on_agent_failed)
        self._bus.subscribe(Topics.WATCHDOG_ALERT, self._on_watchdog_alert)

        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("Failure Analyzer started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        await self._bus.publish(Event(
            Topics.AGENT_STOPPED,
            payload={"agent": self.AGENT_NAME},
            source=self.AGENT_NAME,
        ))

    # ── Event Handlers ───────────────────────────────────────

    async def _on_task_failed(self, event: Event) -> None:
        payload = event.payload
        task_dict   = payload.get("task", {})
        error_msg   = payload.get("error", "unknown error")
        status_code = payload.get("status_code")
        agent_name  = payload.get("agent", task_dict.get("agent", "unknown"))
        exc_str     = payload.get("traceback", "")

        error_type = classify_error(error_msg, status_code)
        severity   = self._infer_severity(error_type, task_dict.get("retry_count", 0))

        incident = Incident(
            timestamp=datetime.utcnow(),
            agent_name=agent_name,
            task_id=task_dict.get("id"),
            website=payload.get("website"),
            url=payload.get("url"),
            severity=severity,
            error_type=error_type,
            error_message=error_msg,
            stack_trace=exc_str or None,
            retry_count=task_dict.get("retry_count", 0),
            execution_time_seconds=payload.get("execution_time"),
        )

        await self._save_incident(incident)
        self._tasks_failed += 1
        log.incident(
            f"Task failed: {error_type}",
            incident_id=incident.id,
            agent=agent_name,
            error=error_msg,
        )

    async def _on_agent_failed(self, event: Event) -> None:
        payload    = event.payload
        agent_name = payload.get("agent", "unknown")
        error_msg  = payload.get("error", "agent failure")

        incident = Incident(
            timestamp=datetime.utcnow(),
            agent_name=agent_name,
            severity=Severity.HIGH,
            error_type="agent_failed",
            error_message=error_msg,
            stack_trace=payload.get("traceback"),
        )
        await self._save_incident(incident)
        log.incident(f"Agent failure recorded", agent=agent_name, incident_id=incident.id)

    async def _on_watchdog_alert(self, event: Event) -> None:
        payload = event.payload
        incident = Incident(
            timestamp=datetime.utcnow(),
            agent_name=payload.get("agent", "browser_worker"),
            severity=Severity.HIGH,
            error_type=payload.get("alert_type", "watchdog_alert"),
            error_message=payload.get("message", "Watchdog detected anomaly"),
            url=payload.get("url"),
        )
        await self._save_incident(incident)
        log.incident(f"Watchdog alert recorded", incident_id=incident.id)

    # ── Helpers ──────────────────────────────────────────────

    async def _save_incident(self, incident: Incident) -> None:
        try:
            await self._db.save_incident(incident.model_dump(mode="json"))
            await self._bus.publish(Event(
                Topics.INCIDENT_CREATED,
                payload={"incident_id": incident.id, "severity": incident.severity},
                source=self.AGENT_NAME,
            ))
        except Exception as e:
            log.error(f"Failed to save incident: {e}")

    def _infer_severity(self, error_type: str, retry_count: int) -> Severity:
        critical_types = {"captcha", "agent_failed", "loop_detected", "browser_crash"}
        if error_type in critical_types or retry_count >= 3:
            return Severity.HIGH
        if retry_count >= 2:
            return Severity.MEDIUM
        return Severity.LOW

    async def get_open_incidents(self):
        return await self._db.get_open_incidents()

    async def resolve_incident(self, incident_id: str, resolution: str) -> None:
        await self._db.resolve_incident(incident_id, resolution)
        await self._bus.publish(Event(
            Topics.INCIDENT_RESOLVED,
            payload={"incident_id": incident_id, "resolution": resolution},
            source=self.AGENT_NAME,
        ))

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    tasks_failed=self._tasks_failed,
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
                await self._bus.publish(Event(
                    Topics.AGENT_HEARTBEAT,
                    payload=hb.model_dump(mode="json"),
                    source=self.AGENT_NAME,
                ))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(30)
