"""
agents/ranker.py — Ranking Agent.

Scores articles on 9 dimensions and returns the Top N most important
India news stories. Pure algorithmic ranking — no LLM needed.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Article, Category

log = AgentLogger("ranker")

CATEGORIES_PATH = Path("config/categories.json")

# Category weights (from categories.json)
DEFAULT_CATEGORY_WEIGHTS: Dict[str, float] = {
    "national_security": 10.0,
    "politics":          9.0,
    "finance":           8.0,
    "government":        8.0,
    "economy":           8.0,
    "ai":                8.0,
    "technology":        7.0,
    "crime":             7.0,
    "business":          7.0,
    "accidents":         8.0,
    "natural_disasters": 9.0,
    "international":     7.0,
    "stock_market":      7.0,
    "startups":          6.0,
    "sports":            6.0,
    "general":           3.0,
}

# Number patterns in title suggest concrete, factual news
NUMBER_PATTERN = re.compile(r'\b\d+(?:\.\d+)?(?:%|cr|lakh|billion|million|crore)?\b')

BREAKING_KEYWORDS = {
    "breaking", "just in", "live", "urgent", "developing",
    "major", "massive", "historic", "unprecedented", "emergency",
}

HIGH_IMPACT_KEYWORDS = {
    "died", "killed", "injured", "explosion", "earthquake", "flood",
    "cyclone", "war", "attack", "arrested", "verdict", "elected",
    "resignation", "nuclear", "missile", "ceasefire", "budget",
    "interest rate", "rbi", "sensex", "nifty", "gdp", "inflation",
    "strike", "protest", "riot",
}


class RankerAgent:
    """
    Scores every article on 9 dimensions and returns the top N stories.
    """

    AGENT_NAME = "ranker"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus      = bus or get_bus()
        self._db       = get_db()
        self._running  = False
        self._cat_weights = self._load_category_weights()
        self._heartbeat_task: Optional[asyncio.Task] = None

    def _load_category_weights(self) -> Dict[str, float]:
        try:
            data = json.loads(CATEGORIES_PATH.read_text())
            return {k: v.get("weight", 5.0) for k, v in data.get("categories", {}).items()}
        except Exception:
            return DEFAULT_CATEGORY_WEIGHTS

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("Ranking Agent started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def rank(self, articles: List[Article], top_n: int = 10) -> List[Article]:
        """
        Score and rank articles. Returns top_n with rank field set.
        """
        if not articles:
            return []

        log.info(f"Ranking {len(articles)} articles, selecting top {top_n}.")

        now = datetime.now(timezone.utc)

        scored = []
        for article in articles:
            score = self._compute_score(article, now)
            scored.append((score, article))

        # Sort descending by score
        scored.sort(key=lambda x: x[0], reverse=True)

        top = []
        for rank, (score, article) in enumerate(scored[:top_n], start=1):
            article.rank = rank
            article.metadata["ranking_score"] = round(score, 4)
            top.append(article)
            log.debug(
                f"Rank {rank}: {article.title[:60]} (score={score:.3f})",
            )

        log.info(
            f"Ranking complete. Top {len(top)} stories selected.",
            action="task_done",
        )

        await self._bus.publish(Event(
            Topics.ARTICLES_RANKED,
            payload={"count": len(top), "top_n": top_n},
            source=self.AGENT_NAME,
        ))
        return top

    # ── Scoring Dimensions ────────────────────────────────────

    def _compute_score(self, article: Article, now: datetime) -> float:
        title = (article.headline or article.title).lower()

        score = 0.0

        # 1. Category weight (0 – 10, normalised to 0 – 0.25)
        cat_w = self._cat_weights.get(article.category.value, 3.0)
        score += (cat_w / 10.0) * 0.25

        # 2. National impact — high-impact keywords (0 – 0.20)
        hits = sum(1 for kw in HIGH_IMPACT_KEYWORDS if kw in title)
        score += min(hits / 3.0, 1.0) * 0.20

        # 3. Breaking status (0 – 0.15)
        is_breaking = any(kw in title for kw in BREAKING_KEYWORDS)
        score += 0.15 if is_breaking else 0.0

        # 4. Source reliability from metadata (0 – 0.10)
        rel = article.metadata.get("source_reliability", 0.8)
        score += rel * 0.10

        # 5. Confidence score from fact verifier (0 – 0.10)
        score += article.confidence_score * 0.10

        # 6. Freshness — penalise old articles (0 – 0.10)
        freshness = 1.0
        if article.published_at:
            try:
                pub = article.published_at
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=timezone.utc)
                age_hours = (now - pub).total_seconds() / 3600
                freshness = max(0.0, 1.0 - (age_hours / 24.0))  # full score if <1h, 0 if >24h
            except Exception:
                freshness = 0.5
        score += freshness * 0.10

        # 7. Has concrete numbers (0 – 0.05)
        has_numbers = bool(NUMBER_PATTERN.search(title))
        score += 0.05 if has_numbers else 0.0

        # 8. Source priority bonus (0 – 0.05)
        prio = article.metadata.get("source_priority", 5)
        score += (1.0 / prio) * 0.05

        return min(1.0, score)

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
