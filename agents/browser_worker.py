"""
agents/browser_worker.py — Playwright Browser Worker (last resort).

Only used when RSS and API methods fail. Fully monitored by the
Watchdog agent. Strict timeouts on every operation.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import (
    AgentHeartbeat, AgentStatus, Article, Category, ExtractionMethod
)

log = AgentLogger("browser_worker")

PAGE_TIMEOUT_MS    = 30_000   # 30s max page load
EXTRACT_TIMEOUT_MS = 15_000   # 15s max content extraction
MAX_ARTICLES_PER_PAGE = 20


class BrowserWorker:
    """
    Playwright-based scraper used as last resort.
    Requires: playwright install chromium
    """

    AGENT_NAME = "browser_worker"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus       = bus or get_bus()
        self._db        = get_db()
        self._browser   = None
        self._playwright = None
        self._running    = False
        self._completed  = 0
        self._failed     = 0
        self._current_url: Optional[str] = None  # for watchdog monitoring
        self._action_count = 0                   # for loop detection
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("Browser Worker ready (Playwright). Will start browser on first use.")

    async def stop(self) -> None:
        self._running = False
        await self._close_browser()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def fetch_page(
        self,
        source: Dict[str, Any],
        url: str,
        run_id: str,
    ) -> List[Article]:
        """
        Open URL in browser and extract article links/titles.
        Enforces strict timeouts to prevent hangs.
        """
        from config.settings import get_settings
        settings = get_settings()
        if not settings.enable_browser_automation:
            log.debug("Browser automation disabled. Skipping.")
            return []

        try:
            await self._ensure_browser()
            return await asyncio.wait_for(
                self._do_fetch(source, url, run_id),
                timeout=60.0,
            )
        except asyncio.TimeoutError:
            log.error(f"Browser fetch timed out for {url}")
            await self._emit_watchdog_alert("timeout", url)
            await self._restart_browser()
            return []
        except Exception as e:
            log.error(f"Browser fetch error: {e}", url=url)
            await self._bus.publish(Event(
                Topics.TASK_FAILED,
                payload={
                    "agent": self.AGENT_NAME,
                    "url": url,
                    "error": str(e),
                    "source": source.get("id"),
                },
                source=self.AGENT_NAME,
            ))
            self._failed += 1
            return []

    async def _do_fetch(
        self,
        source: Dict[str, Any],
        url: str,
        run_id: str,
    ) -> List[Article]:
        self._current_url = url
        self._action_count = 0

        page = await self._browser.new_page()
        try:
            await page.set_extra_http_headers({
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            })
            self._action_count += 1

            await page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            self._action_count += 1

            # Check for CAPTCHA
            content = await page.content()
            if any(k in content.lower() for k in ["captcha", "recaptcha", "challenge"]):
                log.warning(f"CAPTCHA detected at {url}")
                await self._emit_watchdog_alert("captcha", url)
                return []

            # Extract article headlines and links
            articles_data = await asyncio.wait_for(
                page.evaluate("""() => {
                    const items = [];
                    const selectors = ['article', 'h2 a', 'h3 a', '.headline a', '.title a', 'a[data-testid]'];
                    for (const sel of selectors) {
                        const els = document.querySelectorAll(sel);
                        for (const el of els) {
                            const a = el.tagName === 'A' ? el : el.querySelector('a');
                            if (!a) continue;
                            const title = (a.textContent || '').trim();
                            const href = a.href;
                            if (title.length > 20 && href && href.startsWith('http')) {
                                items.push({title, url: href});
                            }
                        }
                        if (items.length >= 20) break;
                    }
                    return items.slice(0, 20);
                }"""),
                timeout=EXTRACT_TIMEOUT_MS / 1000,
            )

            articles = []
            for item in (articles_data or []):
                if not item.get("title") or not item.get("url"):
                    continue
                article = Article(
                    title=item["title"],
                    url=item["url"],
                    source=source.get("id", "browser"),
                    publisher=source.get("publisher", "Unknown"),
                    published_at=datetime.now(timezone.utc),
                    category=Category.GENERAL,
                    extraction_method=ExtractionMethod.BROWSER,
                    run_id=run_id,
                )
                articles.append(article)

            self._completed += 1
            log.info(
                f"Browser extracted {len(articles)} articles from {url}",
                action="article_extracted",
                source=source.get("id"),
            )
            return articles

        finally:
            await page.close()
            self._current_url = None

    async def _ensure_browser(self) -> None:
        if self._browser and self._browser.is_connected():
            return
        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            log.info("Chromium browser launched.")
        except Exception as e:
            raise RuntimeError(f"Failed to launch browser: {e}") from e

    async def _restart_browser(self) -> None:
        await self._close_browser()
        await self._ensure_browser()
        log.info("Browser restarted.")

    async def _close_browser(self) -> None:
        try:
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        self._browser    = None
        self._playwright = None

    async def _emit_watchdog_alert(self, alert_type: str, url: str) -> None:
        await self._bus.publish(Event(
            Topics.WATCHDOG_ALERT,
            payload={
                "agent":      self.AGENT_NAME,
                "alert_type": alert_type,
                "url":        url,
                "message":    f"Browser worker detected: {alert_type}",
            },
            source=self.AGENT_NAME,
        ))

    # ── Public watchdog interface ─────────────────────────────

    @property
    def current_url(self) -> Optional[str]:
        return self._current_url

    @property
    def action_count(self) -> int:
        return self._action_count

    async def emergency_stop(self) -> None:
        """Called by Watchdog if anomaly detected."""
        log.warning("Emergency stop triggered by Watchdog!")
        await self._restart_browser()

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    current_task=self._current_url,
                    tasks_completed=self._completed,
                    tasks_failed=self._failed,
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(20)
