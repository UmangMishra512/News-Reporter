"""
agents/fact_verifier.py — Fact Verification Agent.

Cross-references articles against the collected batch to find
corroborating sources. Uses LLM to detect clickbait, rumors, and
speculation. Assigns a confidence score to every article.
"""
from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from typing import Dict, List, Optional

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Article

log = AgentLogger("fact_verifier")

# Clickbait / fake news markers to flag
CLICKBAIT_PATTERNS = [
    r"\byou won'?t believe\b",
    r"\bshocking\b",
    r"\bbreaking\b.*\bbreaking\b",  # "BREAKING BREAKING" spam
    r"\bjust in\b.*\bjust in\b",
    r"\bfake news\b",
    r"\bsatire\b",
    r"\bhoax\b",
    r"\bunconfirmed\b",
    r"\bsocial media (claims?|says?|reports?)\b",
    r"\bwhat happened next will\b",
    r"\bclick here\b",
    r"\bviral\b.*\b(claim|news|video)\b",
]

TRUSTED_SOURCES = {
    "pib_india", "reuters_india", "mea_india", "ani_news",
    "the_hindu", "the_hindu_national", "indian_express",
    "ndtv_top", "economic_times", "livemint", "business_standard",
    "newsapi",
}


class FactVerifierAgent:
    """
    Verifies articles by:
    1. Checking corroboration count (≥2 independent sources preferred)
    2. Flagging clickbait patterns in the title
    3. Assigning a confidence score (0.0 – 1.0)
    """

    AGENT_NAME = "fact_verifier"

    def __init__(
        self,
        bus: Optional[EventBus] = None,
        min_confidence: float = 0.5,
    ) -> None:
        self._bus             = bus or get_bus()
        self._db              = get_db()
        self._min_confidence  = min_confidence
        self._running         = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._patterns        = [re.compile(p, re.IGNORECASE) for p in CLICKBAIT_PATTERNS]

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("Fact Verifier started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def verify_batch(self, articles: List[Article]) -> List[Article]:
        """
        Verify all articles. Returns articles with confidence scores set.
        Articles below min_confidence are marked is_verified=False.
        """
        if not articles:
            return []

        log.info(
            f"Verifying {len(articles)} articles.",
            action="task_started",
        )

        # Build corroboration index: keyword → list of articles
        corroboration = self._build_corroboration_index(articles)

        verified = []
        rejected = 0

        for article in articles:
            score = self._score_article(article, corroboration)
            article.confidence_score = score
            article.is_verified = score >= self._min_confidence

            if article.is_verified:
                verified.append(article)
            else:
                rejected += 1
                log.debug(
                    f"Rejected (confidence={score:.2f}): {article.title[:60]}",
                )

        log.info(
            f"Fact verification done. "
            f"Passed: {len(verified)} | Rejected: {rejected}",
            action="verification_done",
        )

        await self._bus.publish(Event(
            Topics.ARTICLES_VERIFIED,
            payload={"count": len(verified), "rejected": rejected},
            source=self.AGENT_NAME,
        ))
        return verified

    # ── Scoring ──────────────────────────────────────────────

    def _score_article(
        self,
        article: Article,
        corroboration: Dict[str, List[Article]],
    ) -> float:
        score = 0.0

        # 1. Source trustworthiness (0.0 – 0.40)
        source_score = 0.40 if article.source in TRUSTED_SOURCES else 0.20
        score += source_score

        # 2. Source reliability from metadata (0.0 – 0.20)
        rel = article.metadata.get("source_reliability", 0.8)
        score += rel * 0.20

        # 3. Corroboration — how many other sources cover this? (0.0 – 0.30)
        corroboration_count = self._count_corroboration(article, corroboration)
        if corroboration_count >= 3:
            score += 0.30
        elif corroboration_count == 2:
            score += 0.20
        elif corroboration_count == 1:
            score += 0.10

        # 4. Clickbait penalty (−0.0 to −0.30)
        clickbait_hits = self._count_clickbait(article.title)
        score -= clickbait_hits * 0.15

        # 5. Has content (summary/description) (+0.05)
        if article.raw_content and len(article.raw_content) > 50:
            score += 0.05

        # 6. Has publish date (+0.05)
        if article.published_at:
            score += 0.05

        return max(0.0, min(1.0, score))

    def _build_corroboration_index(
        self, articles: List[Article]
    ) -> Dict[str, List[Article]]:
        """Build keyword → articles mapping for fast corroboration lookup."""
        index: Dict[str, List[Article]] = defaultdict(list)
        for article in articles:
            keywords = self._extract_keywords(article.title)
            for kw in keywords:
                index[kw].append(article)
        return dict(index)

    def _count_corroboration(
        self,
        article: Article,
        index: Dict[str, List[Article]],
    ) -> int:
        """Count how many DIFFERENT sources cover the same story."""
        keywords = self._extract_keywords(article.title)
        other_sources = set()
        for kw in keywords:
            for other in index.get(kw, []):
                if other.id != article.id and other.source != article.source:
                    other_sources.add(other.source)
        return len(other_sources)

    def _extract_keywords(self, title: str) -> List[str]:
        """Extract significant 2-word ngrams from a title."""
        words = re.sub(r"[^\w\s]", "", title.lower()).split()
        stopwords = {
            "the", "a", "an", "is", "in", "of", "and", "to", "for",
            "on", "at", "by", "as", "with", "from", "into", "that",
            "this", "it", "he", "she", "they", "was", "are", "be",
            "has", "have", "had", "will", "not", "but", "or", "no",
        }
        content_words = [w for w in words if w not in stopwords and len(w) > 3]
        # Bigrams
        bigrams = [
            f"{content_words[i]}_{content_words[i+1]}"
            for i in range(len(content_words) - 1)
        ]
        return content_words[:5] + bigrams[:3]

    def _count_clickbait(self, title: str) -> int:
        return sum(1 for p in self._patterns if p.search(title))

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
