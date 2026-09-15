"""
agents/source_discovery.py — Source Discovery Orchestrator.

Coordinates RSS, API, and (as last resort) browser workers to
collect the maximum number of articles from all configured sources.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import List, Optional

from config.settings import get_settings
from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Article

from agents.api_worker     import APIWorker
from agents.browser_worker import BrowserWorker
from agents.rss_worker     import RSSWorker
from agents.website_knowledge import get_knowledge_agent

log = AgentLogger("source_discovery")


class SourceDiscoveryAgent:
    """
    Orchestrates all collection workers.
    Priority order: RSS/Google News RSS → API → Browser (last resort).
    """

    AGENT_NAME = "source_discovery"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus       = bus or get_bus()
        self._db        = get_db()
        self._settings  = get_settings()
        self._knowledge = get_knowledge_agent()
        self._rss       = RSSWorker(
            bus=self._bus,
            timeout=self._settings.request_timeout_seconds,
            concurrency=self._settings.rss_worker_concurrency,
        )
        self._api       = APIWorker(bus=self._bus)
        self._browser   = BrowserWorker(bus=self._bus)
        self._running   = False
        self._runs_completed = 0
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        await self._rss.start()
        await self._api.start()
        await self._browser.start()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("Source Discovery Agent started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        await self._rss.stop()
        await self._api.stop()
        await self._browser.stop()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    @property
    def browser_worker(self) -> BrowserWorker:
        return self._browser

    async def collect(self, run_id: str) -> List[Article]:
        """
        Run a full collection cycle. Returns all raw articles
        before dedup/verification/ranking.
        """
        log.info(f"Starting collection cycle (run_id={run_id})", action="task_started")
        all_articles: List[Article] = []

        sources = self._knowledge.get_all_sources()

        # ── 1. RSS feeds (parallel) ───────────────────────────
        try:
            rss_articles = await self._rss.fetch_all_sources(sources, run_id)
            all_articles.extend(rss_articles)
            log.info(f"RSS collected: {len(rss_articles)} articles")
        except Exception as e:
            log.error(f"RSS collection failed: {e}")

        # ── 2. APIs ───────────────────────────────────────────
        try:
            api_articles = await self._api.fetch_all(run_id)
            all_articles.extend(api_articles)
            if api_articles:
                log.info(f"API collected: {len(api_articles)} articles")
        except Exception as e:
            log.error(f"API collection failed: {e}")

        # ── 3. Browser (only if <50 articles and browser enabled) ──
        if len(all_articles) < 50 and self._settings.enable_browser_automation:
            log.info("Article count low — attempting browser fallback")
            browser_sources = [s for s in sources if not s.get("rss_url")]
            for source in browser_sources[:3]:  # max 3 browser sources
                url = source.get("url") or source.get("api_url", "")
                if url:
                    try:
                        browser_articles = await self._browser.fetch_page(source, url, run_id)
                        all_articles.extend(browser_articles)
                    except Exception as e:
                        log.error(f"Browser fallback failed for {source.get('id')}: {e}")

        # ── Limit total ──────────────────────────────────────
        max_articles = self._settings.max_articles_per_run
        if len(all_articles) > max_articles:
            # Sort by source priority before trimming
            all_articles.sort(
                key=lambda a: a.metadata.get("source_priority", 5)
            )
            all_articles = all_articles[:max_articles]

        self._runs_completed += 1
        log.info(
            f"Collection complete. Total raw articles: {len(all_articles)}",
            action="articles_fetched",
            run_id=run_id,
            count=len(all_articles),
        )
        await self._bus.publish(Event(
            Topics.ARTICLES_FETCHED,
            payload={"count": len(all_articles), "run_id": run_id},
            source=self.AGENT_NAME,
        ))
        return all_articles

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    tasks_completed=self._runs_completed,
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(30)
