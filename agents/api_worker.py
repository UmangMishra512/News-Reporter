"""
agents/api_worker.py — Official News API Worker.

Fetches articles from NewsAPI.org and other structured APIs
when available and configured.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from config.settings import get_settings
from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import (
    AgentHeartbeat, AgentStatus, Article, Category, ExtractionMethod
)

log = AgentLogger("api_worker")

HEADERS = {
    "User-Agent": "IndiaNewsIntelligence/1.0",
    "Accept": "application/json",
}


class APIWorker:
    """Fetches news from structured JSON APIs (NewsAPI, etc.)."""

    AGENT_NAME = "api_worker"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus      = bus or get_bus()
        self._db       = get_db()
        self._settings = get_settings()
        self._timeout  = aiohttp.ClientTimeout(total=20)
        self._completed = 0
        self._failed    = 0
        self._running   = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("API Worker ready.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def fetch_newsapi(self, run_id: str) -> List[Article]:
        """Fetch top India headlines from NewsAPI.org."""
        api_key = self._settings.news_api_key
        if not api_key or api_key.startswith("your_"):
            log.debug("NewsAPI key not configured, skipping.")
            return []

        url = (
            f"https://newsapi.org/v2/top-headlines"
            f"?country=in&pageSize=30&apiKey={api_key}"
        )
        try:
            t0 = time.monotonic()
            async with aiohttp.ClientSession(headers=HEADERS, timeout=self._timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 429:
                        log.warning("NewsAPI rate limited.")
                        return []
                    resp.raise_for_status()
                    data = await resp.json()
                    rt_ms = (time.monotonic() - t0) * 1000

            articles = []
            for item in data.get("articles", []):
                try:
                    published_at = None
                    if item.get("publishedAt"):
                        published_at = datetime.fromisoformat(
                            item["publishedAt"].replace("Z", "+00:00")
                        )
                    article = Article(
                        title=item.get("title", "").strip(),
                        url=item.get("url", ""),
                        source="newsapi",
                        publisher=item.get("source", {}).get("name", "Unknown"),
                        published_at=published_at,
                        category=Category.GENERAL,
                        raw_content=(
                            (item.get("description") or "") + " " +
                            (item.get("content") or "")
                        )[:2000],
                        extraction_method=ExtractionMethod.API,
                        run_id=run_id,
                        metadata={"response_time_ms": rt_ms},
                    )
                    if article.title and article.url:
                        articles.append(article)
                except Exception as e:
                    log.warning(f"Failed to parse NewsAPI item: {e}")

            self._completed += 1
            log.info(
                f"NewsAPI returned {len(articles)} articles",
                action="api_request",
                articles=len(articles),
            )
            return articles

        except Exception as e:
            self._failed += 1
            log.error(f"NewsAPI fetch failed: {e}")
            await self._bus.publish(Event(
                Topics.TASK_FAILED,
                payload={"agent": self.AGENT_NAME, "error": str(e), "source": "newsapi"},
                source=self.AGENT_NAME,
            ))
            return []

    async def fetch_all(self, run_id: str) -> List[Article]:
        """Run all configured API workers."""
        results = await asyncio.gather(
            self.fetch_newsapi(run_id),
            return_exceptions=True,
        )
        articles = []
        for r in results:
            if isinstance(r, list):
                articles.extend(r)
        return articles

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    tasks_completed=self._completed,
                    tasks_failed=self._failed,
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(60)
