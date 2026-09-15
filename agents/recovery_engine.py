"""
agents/recovery_engine.py — Recovery Engine.

Applies the platform's retry policy rules and coordinates automatic
recovery for every failed task or agent without human intervention.
Only escalates when all automatic options are exhausted.

Retry Policy (from spec):
  Network Error  → Retry 3 times
  Timeout        → Retry 2 times
  429 Rate Limit → Exponential backoff
  Captcha        → Stop, switch source
  403 Forbidden  → Switch source
  404            → Stop
  500            → Retry
  Unknown        → Report and escalate
"""
from __future__ import annotations

import asyncio
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import Incident, RecoveryAction, Severity, TaskItem, TaskStatus

log = AgentLogger("recovery_engine")


# ─────────────────────────────────────────────────────────────
#  Error Classification
# ─────────────────────────────────────────────────────────────

class ErrorType:
    NETWORK         = "network_error"
    TIMEOUT         = "timeout"
    RATE_LIMIT      = "rate_limit_429"
    CAPTCHA         = "captcha"
    FORBIDDEN       = "forbidden_403"
    NOT_FOUND       = "not_found_404"
    SERVER_ERROR    = "server_error_5xx"
    PARSE_ERROR     = "parse_error"
    EMPTY_RESPONSE  = "empty_response"
    BROWSER_CRASH   = "browser_crash"
    LOOP_DETECTED   = "loop_detected"
    AGENT_STALE     = "agent_stale"
    UNKNOWN         = "unknown"


# Maps error type → (max_retries, backoff_seconds, recovery_action)
RETRY_POLICY: Dict[str, Dict[str, Any]] = {
    ErrorType.NETWORK:      {"max_retries": 3, "backoff": 5,   "action": RecoveryAction.RETRY},
    ErrorType.TIMEOUT:      {"max_retries": 2, "backoff": 10,  "action": RecoveryAction.RETRY},
    ErrorType.RATE_LIMIT:   {"max_retries": 5, "backoff": 60,  "action": RecoveryAction.RETRY,  "exponential": True},
    ErrorType.CAPTCHA:      {"max_retries": 0, "backoff": 0,   "action": RecoveryAction.SWITCH_SOURCE},
    ErrorType.FORBIDDEN:    {"max_retries": 0, "backoff": 0,   "action": RecoveryAction.SWITCH_SOURCE},
    ErrorType.NOT_FOUND:    {"max_retries": 0, "backoff": 0,   "action": RecoveryAction.SKIP_SOURCE},
    ErrorType.SERVER_ERROR: {"max_retries": 2, "backoff": 15,  "action": RecoveryAction.RETRY},
    ErrorType.PARSE_ERROR:  {"max_retries": 1, "backoff": 5,   "action": RecoveryAction.RETRY},
    ErrorType.EMPTY_RESPONSE: {"max_retries": 1, "backoff": 5, "action": RecoveryAction.SWITCH_SOURCE},
    ErrorType.BROWSER_CRASH: {"max_retries": 1, "backoff": 5,  "action": RecoveryAction.RESTART_BROWSER},
    ErrorType.LOOP_DETECTED: {"max_retries": 0, "backoff": 0,  "action": RecoveryAction.RESTART_AGENT},
    ErrorType.AGENT_STALE:  {"max_retries": 1, "backoff": 10,  "action": RecoveryAction.RESTART_AGENT},
    ErrorType.UNKNOWN:      {"max_retries": 1, "backoff": 30,  "action": RecoveryAction.ESCALATE},
}


def classify_error(error_msg: str, status_code: Optional[int] = None) -> str:
    """Classify an error string/HTTP status code into an ErrorType."""
    msg = error_msg.lower()
    if status_code == 429 or "429" in msg or "rate limit" in msg or "too many" in msg:
        return ErrorType.RATE_LIMIT
    if status_code == 403 or "403" in msg or "forbidden" in msg:
        return ErrorType.FORBIDDEN
    if status_code == 404 or "404" in msg or "not found" in msg:
        return ErrorType.NOT_FOUND
    if status_code and 500 <= status_code < 600 or "500" in msg or "server error" in msg:
        return ErrorType.SERVER_ERROR
    if "captcha" in msg or "recaptcha" in msg or "challenge" in msg:
        return ErrorType.CAPTCHA
    if "timeout" in msg or "timed out" in msg:
        return ErrorType.TIMEOUT
    if "connection" in msg or "network" in msg or "ssl" in msg or "socket" in msg:
        return ErrorType.NETWORK
    if "parse" in msg or "decode" in msg or "xml" in msg or "json" in msg:
        return ErrorType.PARSE_ERROR
    if "empty" in msg or "no articles" in msg or "no content" in msg:
        return ErrorType.EMPTY_RESPONSE
    if "browser" in msg or "playwright" in msg or "chromium" in msg:
        return ErrorType.BROWSER_CRASH
    if "loop" in msg or "infinite" in msg or "stuck" in msg:
        return ErrorType.LOOP_DETECTED
    return ErrorType.UNKNOWN


# ─────────────────────────────────────────────────────────────
#  Recovery Engine
# ─────────────────────────────────────────────────────────────

class RecoveryEngine:
    """
    Determines and executes the correct recovery strategy for any failure.

    Used by: failure_analyzer.py, watchdog.py, supervisor.py
    """

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus = bus or get_bus()
        self._agent_restart_callbacks: Dict[str, Callable] = {}

    def register_restart_callback(self, agent_name: str, callback: Callable) -> None:
        """Agents register their own restart functions here."""
        self._agent_restart_callbacks[agent_name] = callback
        log.debug(f"Registered restart callback for {agent_name!r}")

    async def handle_task_failure(
        self,
        task: TaskItem,
        error_msg: str,
        agent_name: str,
        status_code: Optional[int] = None,
        exc: Optional[Exception] = None,
    ) -> RecoveryAction:
        """
        Main entry point. Given a failed task, decide and execute recovery.
        Returns the action taken.
        """
        error_type = classify_error(error_msg, status_code)
        policy     = RETRY_POLICY.get(error_type, RETRY_POLICY[ErrorType.UNKNOWN])
        action     = policy["action"]
        max_retries = policy["max_retries"]

        log.warning(
            f"Handling failure",
            task_id=task.id,
            agent=agent_name,
            error_type=error_type,
            retry_count=task.retry_count,
            action=action,
        )

        # ── Retry logic ────────────────────────────────────────
        if action == RecoveryAction.RETRY and task.retry_count < max_retries:
            backoff = policy["backoff"]
            if policy.get("exponential"):
                backoff = backoff * (2 ** task.retry_count)

            log.info(
                f"Scheduling retry in {backoff}s",
                task_id=task.id,
                attempt=task.retry_count + 1,
                max=max_retries,
            )
            await asyncio.sleep(min(backoff, 300))  # cap at 5 minutes

            await self._bus.publish(Event(
                Topics.TASK_RETRY,
                payload={"task": task.model_dump(), "error_type": error_type},
                source="recovery_engine",
            ))
            return RecoveryAction.RETRY

        # ── Switch source ──────────────────────────────────────
        if action == RecoveryAction.SWITCH_SOURCE:
            await self._bus.publish(Event(
                Topics.TASK_CANCELLED,
                payload={"task": task.model_dump(), "reason": f"switch_source:{error_type}"},
                source="recovery_engine",
            ))
            return RecoveryAction.SWITCH_SOURCE

        # ── Skip source ────────────────────────────────────────
        if action == RecoveryAction.SKIP_SOURCE:
            await self._bus.publish(Event(
                Topics.TASK_CANCELLED,
                payload={"task": task.model_dump(), "reason": f"skip:{error_type}"},
                source="recovery_engine",
            ))
            return RecoveryAction.SKIP_SOURCE

        # ── Restart agent ──────────────────────────────────────
        if action == RecoveryAction.RESTART_AGENT:
            await self._restart_agent(agent_name)
            return RecoveryAction.RESTART_AGENT

        # ── Restart browser ────────────────────────────────────
        if action == RecoveryAction.RESTART_BROWSER:
            await self._bus.publish(Event(
                Topics.BROWSER_FROZEN,
                payload={"agent": agent_name, "task_id": task.id},
                source="recovery_engine",
            ))
            return RecoveryAction.RESTART_BROWSER

        # ── Escalate ───────────────────────────────────────────
        await self._escalate(task, agent_name, error_type, error_msg)
        return RecoveryAction.ESCALATE

    async def handle_agent_stale(self, agent_name: str) -> None:
        """Called by the supervisor when an agent's heartbeat is overdue."""
        log.warning(f"Agent is stale, attempting restart", agent=agent_name)
        await self._restart_agent(agent_name)

    async def _restart_agent(self, agent_name: str) -> None:
        callback = self._agent_restart_callbacks.get(agent_name)
        if callback:
            try:
                await self._bus.publish(Event(
                    Topics.RECOVERY_STARTED,
                    payload={"agent": agent_name, "action": "restart"},
                    source="recovery_engine",
                ))
                await callback()
                await self._bus.publish(Event(
                    Topics.RECOVERY_DONE,
                    payload={"agent": agent_name},
                    source="recovery_engine",
                ))
                log.info(f"Agent restarted successfully", agent=agent_name)
            except Exception as e:
                log.error(f"Agent restart failed: {e}", agent=agent_name)
                await self._escalate_agent(agent_name, str(e))
        else:
            log.warning(f"No restart callback registered for {agent_name!r}")
            await self._escalate_agent(agent_name, "no_restart_callback")

    async def _escalate(
        self,
        task: TaskItem,
        agent_name: str,
        error_type: str,
        error_msg: str,
    ) -> None:
        log.error(
            f"Escalating unresolved failure to Supervisor",
            task_id=task.id,
            agent=agent_name,
            error_type=error_type,
        )
        await self._bus.publish(Event(
            Topics.TASK_ESCALATED,
            payload={
                "task": task.model_dump(),
                "agent": agent_name,
                "error_type": error_type,
                "error_message": error_msg,
            },
            source="recovery_engine",
        ))

    async def _escalate_agent(self, agent_name: str, reason: str) -> None:
        await self._bus.publish(Event(
            Topics.RECOVERY_FAILED,
            payload={"agent": agent_name, "reason": reason},
            source="recovery_engine",
        ))
        await self._bus.publish(Event(
            Topics.NOTIFY_DIRECTOR,
            payload={
                "message": f"⚠️ Agent **{agent_name}** could not be restarted. Reason: {reason}",
                "severity": "high",
            },
            source="recovery_engine",
        ))


# Singleton
_engine: Optional[RecoveryEngine] = None


def get_recovery_engine() -> RecoveryEngine:
    global _engine
    if _engine is None:
        _engine = RecoveryEngine()
    return _engine
