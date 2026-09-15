"""
agents/rss_worker.py — RSS Feed Worker.

Fetches news articles from RSS/Atom feeds and Google News RSS
using aiohttp with full error handling, retry logic, and heartbeats.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
import feedparser

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import (
    AgentHeartbeat, AgentStatus, Article, Category, ExtractionMethod, TaskItem
)
from agents.website_knowledge import get_knowledge_agent

log = AgentLogger("rss_worker")

# Google News RSS redirects to actual article URLs
GOOGLE_NEWS_DOMAINS = {"news.google.com"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; IndiaNewsBot/1.0; "
        "+https://github.com/india-news-platform)"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


class RSSWorker:
    """
    Fetches one or more RSS feeds concurrently and returns Article objects.
    """

    AGENT_NAME = "rss_worker"

    def __init__(
        self,
        bus: Optional[EventBus] = None,
        timeout: int = 15,
        concurrency: int = 8,
    ) -> None:
        self._bus         = bus or get_bus()
        self._db          = get_db()
        self._knowledge   = get_knowledge_agent()
        self._timeout     = aiohttp.ClientTimeout(total=timeout)
        self._semaphore   = asyncio.Semaphore(concurrency)
        self._running     = False
        self._completed   = 0
        self._failed      = 0
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("RSS Worker ready.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    # ── Main fetch method ────────────────────────────────────

    async def fetch_all_sources(
        self,
        sources: List[Dict[str, Any]],
        run_id: str,
    ) -> List[Article]:
        """
        Fetch all RSS sources concurrently.
        Returns combined list of Article objects.
        """
        rss_sources = [
            s for s in sources
            if s.get("method") in ("rss", "google_news_rss") and s.get("enabled", True)
        ]

        log.info(
            f"Starting parallel RSS fetch for {len(rss_sources)} sources.",
            action="task_started",
            run_id=run_id,
        )

        tasks = [
            self._fetch_source(source, run_id)
            for source in rss_sources
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_articles: List[Article] = []
        sources_ok   = 0
        sources_fail = 0

        for source, result in zip(rss_sources, results):
            source_id = source.get("id", "unknown")
            if isinstance(result, Exception):
                log.error(
                    f"Source fetch raised exception: {result}",
                    source=source_id,
                )
                sources_fail += 1
            elif isinstance(result, list) and len(result) > 0:
                all_articles.extend(result)
                sources_ok += 1
            else:
                log.warning(
                    f"Source {source_id!r} returned empty or failed fetch",
                    source=source_id,
                )
                sources_fail += 1

        log.info(
            f"RSS fetch complete. "
            f"Articles: {len(all_articles)} | OK: {sources_ok} | Failed: {sources_fail}",
            action="rss_loaded",
            run_id=run_id,
        )
        return all_articles

    async def _fetch_source(
        self, source: Dict[str, Any], run_id: str
    ) -> List[Article]:
        async with self._semaphore:
            source_id = source.get("id", "unknown")
            rss_url   = source.get("rss_url", "")
            if not rss_url:
                return []

            domain    = self._domain(rss_url)
            strategy  = self._knowledge.get_strategy(domain)

            if not strategy.get("robots_ok", True):
                log.warning(f"Skipping {source_id!r} — robots.txt disallows.", source=source_id)
                return []
            if strategy.get("has_captcha"):
                log.warning(f"Skipping {source_id!r} — captcha detected.", source=source_id)
                return []

            max_retries = strategy.get("max_retries", 3)
            backoff     = strategy.get("backoff", 5)

            for attempt in range(max_retries + 1):
                try:
                    return await self._do_fetch(source, rss_url, run_id, domain)
                except asyncio.TimeoutError:
                    log.warning(f"Timeout fetching {source_id!r} (attempt {attempt+1})")
                    if attempt < max_retries:
                        await asyncio.sleep(backoff)
                        continue
                    await self._emit_failure(source, rss_url, "timeout", run_id)
                    await self._knowledge.record_failure(domain, "timeout")
                    self._failed += 1
                    return []
                except aiohttp.ClientResponseError as e:
                    if e.status in (429,):
                        wait = backoff * (2 ** attempt)
                        log.warning(f"Rate limited {source_id!r}. Waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    await self._emit_failure(source, rss_url, str(e), run_id, e.status)
                    await self._knowledge.record_failure(domain, str(e))
                    self._failed += 1
                    return []
                except Exception as e:
                    await self._emit_failure(source, rss_url, str(e), run_id)
                    await self._knowledge.record_failure(domain, str(e))
                    self._failed += 1
                    return []

            return []

    async def _do_fetch(
        self,
        source: Dict[str, Any],
        url: str,
        run_id: str,
        domain: str,
    ) -> List[Article]:
        t0 = time.monotonic()
        async with aiohttp.ClientSession(
            headers=HEADERS,
            timeout=self._timeout,
            connector=aiohttp.TCPConnector(ssl=False),
        ) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.read()
                rt_ms   = (time.monotonic() - t0) * 1000.0

        # feedparser is synchronous — run in thread pool
        loop = asyncio.get_event_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, content)

        articles = self._parse_feed(feed, source, run_id)

        await self._knowledge.record_success(domain, rt_ms)
        self._completed += 1
        log.info(
            f"Fetched {len(articles)} articles from {source['name']}",
            action="rss_loaded",
            source=source.get("id"),
            articles=len(articles),
        )
        return articles

    def _parse_feed(
        self,
        feed: Any,
        source: Dict[str, Any],
        run_id: str,
    ) -> List[Article]:
        articles = []
        for entry in feed.entries[:50]:  # cap per source
            try:
                title = (entry.get("title") or "").strip()
                url   = entry.get("link") or entry.get("url", "")
                if not title or not url:
                    continue

                # Deduplicate by URL within the same fetch
                published_at = None
                for date_field in ("published_parsed", "updated_parsed"):
                    parsed = entry.get(date_field)
                    if parsed:
                        try:
                            published_at = datetime(*parsed[:6], tzinfo=timezone.utc)
                        except Exception:
                            pass
                        break

                raw_content = (
                    entry.get("summary")
                    or entry.get("description")
                    or entry.get("content", [{}])[0].get("value", "")
                    or ""
                )

                # Category inference from source config
                cats = source.get("categories", ["general"])
                cat  = Category(cats[0]) if cats else Category.GENERAL

                article = Article(
                    title=title,
                    url=url,
                    source=source.get("id", "unknown"),
                    publisher=source.get("publisher", source.get("name", "Unknown")),
                    published_at=published_at,
                    category=cat,
                    raw_content=raw_content[:2000],  # cap content size
                    extraction_method=ExtractionMethod(
                        source.get("method", "rss")
                    ),
                    run_id=run_id,
                    metadata={
                        "source_id": source.get("id"),
                        "source_priority": source.get("priority", 5),
                        "source_reliability": source.get("reliability", 0.8),
                    },
                )
                articles.append(article)
            except Exception as e:
                log.warning(f"Failed to parse feed entry: {e}")
                continue

        return articles

    async def _emit_failure(
        self,
        source: Dict[str, Any],
        url: str,
        error: str,
        run_id: str,
        status_code: Optional[int] = None,
    ) -> None:
        await self._bus.publish(Event(
            Topics.TASK_FAILED,
            payload={
                "agent": self.AGENT_NAME,
                "source": source.get("id"),
                "domain": self._domain(url),
                "url": url,
                "error": error,
                "status_code": status_code,
                "run_id": run_id,
                "task": {"id": f"rss_{source.get('id')}", "retry_count": 0},
            },
            source=self.AGENT_NAME,
        ))

    def _domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc
        except Exception:
            return url

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING if self._running else AgentStatus.IDLE,
                    tasks_completed=self._completed,
                    tasks_failed=self._failed,
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(30)
