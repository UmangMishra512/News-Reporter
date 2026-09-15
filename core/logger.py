"""
core/logger.py — Structured logging setup for the entire platform.

Outputs:
  - Human-readable colored logs to console (INFO+)
  - Structured JSON logs to storage/logs/agent.log (all levels)
  - Separate incident log at storage/logs/incidents.log
  - Performance log at storage/logs/performance.log
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger as loguru_logger

# ─────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────

LOG_DIR = Path(os.getenv("LOG_DIR", "storage/logs"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def setup_logging() -> None:
    """Call once at startup to configure loguru for the whole platform."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Remove default handler
    loguru_logger.remove()

    # Patch every log record to ensure 'agent' always exists
    def _ensure_agent(record: dict) -> bool:
        if "agent" not in record["extra"]:
            record["extra"]["agent"] = record.get("name", "system")
        return True

    # ── Console: colorful, human-readable ──────────────────
    loguru_logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        colorize=True,
        filter=_ensure_agent,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[agent]: <20}</cyan> | "
            "<level>{message}</level>"
        ),
    )

    # ── File: structured JSON for every log entry ───────────
    loguru_logger.add(
        LOG_DIR / "agent.log",
        level="DEBUG",
        rotation="50 MB",
        retention="90 days",
        compression="gz",
        serialize=True,          # JSON format
        enqueue=True,            # thread-safe async write
    )

    # ── Incidents log ────────────────────────────────────────
    loguru_logger.add(
        LOG_DIR / "incidents.log",
        level="WARNING",
        filter=lambda r: r["extra"].get("incident") is True,
        rotation="10 MB",
        retention="365 days",
        serialize=True,
        enqueue=True,
    )

    # ── Performance log ──────────────────────────────────────
    loguru_logger.add(
        LOG_DIR / "performance.log",
        level="INFO",
        filter=lambda r: r["extra"].get("performance") is True,
        rotation="10 MB",
        retention="180 days",
        serialize=True,
        enqueue=True,
    )

    # Bridge stdlib logging → loguru so third-party libs are captured
    class _InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                level = loguru_logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            frame, depth = sys._getframe(6), 6
            while frame and frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back  # type: ignore
                depth += 1
            loguru_logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    loguru_logger.info("Logging system initialised.", agent="logger")


# ─────────────────────────────────────────────────────────────
#  Agent-scoped logger factory
# ─────────────────────────────────────────────────────────────

class AgentLogger:
    """
    A logger bound to a specific agent name.

    Usage:
        log = AgentLogger("rss_worker")
        log.info("RSS loaded", source="the_hindu", articles=42)
        log.error("Fetch failed", url="https://...", error=str(e))
    """

    # Standard log action labels used in the platform
    ACTIONS = {
        "task_created":       "TASK CREATED",
        "task_assigned":      "TASK ASSIGNED",
        "task_started":       "TASK STARTED",
        "rss_loaded":         "RSS LOADED",
        "api_request":        "API REQUEST",
        "article_extracted":  "ARTICLE EXTRACTED",
        "duplicate_removed":  "DUPLICATE REMOVED",
        "verification_done":  "VERIFICATION COMPLETE",
        "summary_generated":  "SUMMARY GENERATED",
        "published":          "PUBLISHED",
        "failed":             "FAILED",
        "recovered":          "RECOVERED",
        "cancelled":          "CANCELLED",
        "incident_created":   "INCIDENT CREATED",
        "heartbeat":          "HEARTBEAT",
    }

    def __init__(self, agent_name: str) -> None:
        self._agent = agent_name
        self._log = loguru_logger.bind(agent=agent_name)

    def debug(self, msg: str, **kw: Any) -> None:
        self._log.bind(**kw).debug(msg)

    def info(self, msg: str, action: Optional[str] = None, **kw: Any) -> None:
        label = self.ACTIONS.get(action, action) if action else None
        if label:
            msg = f"[{label}] {msg}"
        self._log.bind(**kw).info(msg)

    def warning(self, msg: str, **kw: Any) -> None:
        self._log.bind(**kw).warning(msg)

    def error(self, msg: str, **kw: Any) -> None:
        self._log.bind(**kw).error(msg)

    def critical(self, msg: str, **kw: Any) -> None:
        self._log.bind(**kw).critical(msg)

    def exception(self, msg: str, **kw: Any) -> None:
        self._log.bind(**kw).exception(msg)

    def incident(self, msg: str, **kw: Any) -> None:
        """Log an incident — also routed to incidents.log."""
        self._log.bind(incident=True, **kw).warning(f"[INCIDENT] {msg}")

    def perf(self, msg: str, **kw: Any) -> None:
        """Log a performance metric — also routed to performance.log."""
        self._log.bind(performance=True, **kw).info(f"[PERF] {msg}")
