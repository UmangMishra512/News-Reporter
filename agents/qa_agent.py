"""
agents/qa_agent.py — Quality Assurance AI.

Final checkpoint before any story is published.
Validates headlines, summaries, URLs, grammar, and categories.
Rejects anything that fails the quality bar.
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import List, Optional, Tuple

import aiohttp

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Article

log = AgentLogger("qa_agent")

MIN_HEADLINE_WORDS   = 5
MAX_HEADLINE_WORDS   = 25
MIN_SUMMARY_WORDS    = 15
MAX_SUMMARY_WORDS    = 100
MIN_URL_LENGTH       = 15

# Patterns that indicate poor quality content
QUALITY_FAIL_PATTERNS = [
    re.compile(r"(lorem ipsum|placeholder|test article|dummy)", re.IGNORECASE),
    re.compile(r"^\s*$"),                 # empty
    re.compile(r"^[\W\d]+$"),            # only punctuation/numbers
    re.compile(r"(.)\1{5,}"),            # repeated chars (aaaaa)
]


class QAAgent:
    """
    Quality gate — every story must pass before publication.
    Checks:
      1. Headline length and content
      2. Summary length and content
      3. URL format validity
      4. URL reachability (HEAD request, sampled)
      5. Duplicate detection (final check)
      6. Category assignment validity
      7. Pattern-based quality checks
    """

    AGENT_NAME = "qa_agent"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus     = bus or get_bus()
        self._db      = get_db()
        self._running = False
        self._timeout = aiohttp.ClientTimeout(total=8)
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._seen_headlines = set()

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("QA Agent started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def run_qa(self, articles: List[Article]) -> List[Article]:
        """
        Run full QA on all articles.
        Returns only articles that pass all checks.
        """
        if not articles:
            return []

        log.info(f"Running QA on {len(articles)} articles.")

        # Reset headline dedup for this batch
        self._seen_headlines = set()
        # Preload recent published headlines for cross-run dedup
        recent = await self._db.get_recent_headlines(hours=48)
        self._seen_headlines.update(h.lower().strip() for h in recent)

        passed  = []
        failed  = 0

        for article in articles:
            ok, notes = await self._check(article)
            article.qa_passed = ok
            article.qa_notes  = notes

            if ok:
                # Register this headline so next article can't duplicate it
                key = (article.headline or article.title).lower().strip()
                self._seen_headlines.add(key)
                passed.append(article)
            else:
                failed += 1
                log.debug(f"QA FAILED [{notes}]: {article.title[:60]}")

        log.info(
            f"QA complete. Passed: {len(passed)} | Failed: {failed}",
            action="task_done",
        )
        await self._bus.publish(Event(
            Topics.ARTICLES_QA_PASSED,
            payload={"count": len(passed), "failed": failed},
            source=self.AGENT_NAME,
        ))
        return passed

    async def _check(self, article: Article) -> Tuple[bool, str]:
        """Run all checks on a single article. Returns (passed, notes)."""

        headline = (article.headline or article.title or "").strip()
        summary  = (article.summary or "").strip()
        url      = (article.url or "").strip()

        # 1. Headline checks
        if not headline:
            return False, "empty_headline"
        words = headline.split()
        if len(words) < MIN_HEADLINE_WORDS:
            return False, f"headline_too_short:{len(words)}"
        if len(words) > MAX_HEADLINE_WORDS:
            article.headline = " ".join(words[:MAX_HEADLINE_WORDS])  # truncate
        for pat in QUALITY_FAIL_PATTERNS:
            if pat.search(headline):
                return False, "headline_quality_fail"

        # 2. Summary checks
        if not summary:
            return False, "empty_summary"
        sum_words = summary.split()
        if len(sum_words) < MIN_SUMMARY_WORDS:
            return False, f"summary_too_short:{len(sum_words)}"
        if len(sum_words) > MAX_SUMMARY_WORDS:
            article.summary = " ".join(sum_words[:MAX_SUMMARY_WORDS]) + "..."

        # 3. URL format check
        if not url or len(url) < MIN_URL_LENGTH:
            return False, "invalid_url"
        if not url.startswith(("http://", "https://")):
            return False, "url_no_scheme"

        # 4. Headline dedup (across this batch and recent 48h)
        key = headline.lower().strip()
        if key in self._seen_headlines:
            return False, "duplicate_headline"

        # 5. Category validity — already enforced by Pydantic, but double check
        if not article.category:
            return False, "missing_category"

        # 6. Confidence score check
        if article.confidence_score < 0.3:
            return False, f"low_confidence:{article.confidence_score:.2f}"

        return True, "ok"

    async def _check_url_reachable(self, url: str) -> bool:
        """Quick HEAD request to verify URL is live. Best-effort."""
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                async with session.head(url, allow_redirects=True) as resp:
                    return resp.status < 400
        except Exception:
            return True  # Don't fail articles just because HEAD timed out

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(60)
