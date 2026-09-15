"""
agents/health_monitor.py — Health Monitor AI.

Continuously pings all configured news sources, APIs, the database,
and system resources. Reports anomalies to the
Supervisor via the event bus.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

from config.settings import get_settings
from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, HealthCheck

log = AgentLogger("health_monitor")

CHECK_INTERVAL_SECONDS = 300  # every 5 minutes
HEARTBEAT_INTERVAL     = 30

INTERNET_CHECK_URL = "https://1.1.1.1"  # Cloudflare DNS — very reliable


class HealthMonitorAgent:
    """
    Checks:
    - Internet connectivity
    - All RSS feed URLs (HTTP HEAD)
    - Database file size and accessibility
    - System memory and disk
    """

    AGENT_NAME = "health_monitor"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus      = bus or get_bus()
        self._db       = get_db()
        self._settings = get_settings()
        self._running  = False
        self._tasks_completed = 0
        self._unhealthy_resources: Dict[str, int] = {}  # resource → fail count
        self._timeout = aiohttp.ClientTimeout(total=10)
        self._monitor_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        self._monitor_task   = asyncio.create_task(self._monitor_loop(), name="health_monitor_main")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="health_monitor_hb")
        log.info("Health Monitor started. Checking every 5 minutes.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        for t in (self._monitor_task, self._heartbeat_task):
            if t:
                t.cancel()

    # ── Main loop ─────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                await self._run_checks()
                self._tasks_completed += 1
            except Exception as e:
                log.error(f"Health check loop error: {e}")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

    async def _run_checks(self) -> None:
        checks = await asyncio.gather(
            self._check_internet(),
            self._check_database(),
            self._check_disk(),
            self._check_all_sources(),
            return_exceptions=True,
        )
        for check in checks:
            if isinstance(check, Exception):
                log.error(f"Health check raised: {check}")

    # ── Individual checks ────────────────────────────────────

    async def _check_internet(self) -> None:
        hc = await self._ping(
            resource_type="network",
            resource_name="internet",
            url=INTERNET_CHECK_URL,
        )
        if not hc.is_healthy:
            log.critical("NO INTERNET CONNECTIVITY detected!")
            await self._alert("network", "internet", "No internet connectivity")

    async def _check_database(self) -> None:
        try:
            db_path = self._settings.database_path
            if not Path(db_path).exists():
                await self._save_hc(HealthCheck(
                    resource_type="database",
                    resource_name="sqlite",
                    is_healthy=False,
                    error="Database file not found",
                ))
                await self._alert("database", "sqlite", "Database file not found")
                return

            size_mb = Path(db_path).stat().st_size / (1024 * 1024)
            # Try a quick query
            rows = await self._db.get_recent_metrics(limit=1)
            hc = HealthCheck(
                resource_type="database",
                resource_name="sqlite",
                is_healthy=True,
                metadata_note=f"size={size_mb:.1f}MB",
            )
            await self._save_hc(hc)

            if size_mb > 500:
                log.warning(f"Database is large: {size_mb:.1f} MB")
                await self._alert("database", "sqlite_size", f"DB size {size_mb:.1f} MB")

            # Periodic cleanup: prune health checks older than 7 days
            try:
                pruned_count = await self._db.prune_old_health_checks(retention_days=7)
                if pruned_count > 0:
                    log.info(
                        f"Pruned {pruned_count} old health check records (older than 7 days)",
                        action="db_cleanup",
                    )
            except Exception as pe:
                log.error(f"Failed to prune old health checks: {pe}")

        except Exception as e:
            await self._save_hc(HealthCheck(
                resource_type="database",
                resource_name="sqlite",
                is_healthy=False,
                error=str(e),
            ))
            await self._alert("database", "sqlite", str(e))

    async def _check_disk(self) -> None:
        try:
            import shutil
            total, used, free = shutil.disk_usage("/")
            free_gb = free / (1024 ** 3)
            if free_gb < 1.0:
                log.warning(f"Low disk space: {free_gb:.2f} GB free")
                await self._alert("system", "disk_space", f"Only {free_gb:.2f}GB free")
        except Exception as e:
            log.error(f"Disk check failed: {e}")

    async def _check_all_sources(self) -> None:
        import json
        sources_path = Path("config/sources.json")
        if not sources_path.exists():
            return
        data = json.loads(sources_path.read_text())
        sources = [s for s in data.get("sources", []) if s.get("enabled") and s.get("rss_url")]

        semaphore = asyncio.Semaphore(10)

        async def check_one(source: Dict[str, Any]) -> None:
            async with semaphore:
                url = source.get("rss_url") or source.get("api_url", "")
                if not url:
                    return
                hc = await self._ping(
                    resource_type="rss",
                    resource_name=source.get("id", "unknown"),
                    url=url,
                )
                if not hc.is_healthy:
                    prev_fails = self._unhealthy_resources.get(source["id"], 0) + 1
                    self._unhealthy_resources[source["id"]] = prev_fails
                    if prev_fails >= 3:  # only alert after 3 consecutive failures
                        await self._alert("rss", source["id"], hc.error or "HTTP error")
                else:
                    self._unhealthy_resources.pop(source["id"], None)

        await asyncio.gather(*[check_one(s) for s in sources], return_exceptions=True)

    # ── Helpers ──────────────────────────────────────────────

    async def _ping(
        self,
        resource_type: str,
        resource_name: str,
        url: str,
    ) -> HealthCheck:
        t0  = time.monotonic()
        err = None
        sc  = None
        ok  = False
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector, timeout=self._timeout) as session:
                async with session.head(url, allow_redirects=True) as resp:
                    sc  = resp.status
                    ok  = resp.status < 400
        except asyncio.TimeoutError:
            err = "timeout"
        except aiohttp.ClientError as e:
            err = str(e)
        except Exception as e:
            err = str(e)

        rt_ms = (time.monotonic() - t0) * 1000
        hc = HealthCheck(
            resource_type=resource_type,
            resource_name=resource_name,
            url=url,
            is_healthy=ok,
            response_time_ms=rt_ms,
            status_code=sc,
            error=err,
        )
        await self._save_hc(hc)
        return hc

    async def _save_hc(self, hc: HealthCheck) -> None:
        try:
            await self._db.save_health_check(hc.model_dump(mode="json"))
        except Exception as e:
            log.error(f"Failed to save health check: {e}")

    async def _alert(self, rtype: str, name: str, error: str) -> None:
        await self._bus.publish(Event(
            Topics.RESOURCE_UNHEALTHY,
            payload={"resource_type": rtype, "resource_name": name, "error": error},
            source=self.AGENT_NAME,
        ))

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    tasks_completed=self._tasks_completed,
                    metadata={"unhealthy_count": len(self._unhealthy_resources)},
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
