"""
agents/dedup_agent.py — Deduplication Agent.

Uses local sentence-transformers for semantic similarity to cluster
articles about the same event. Keeps only the most credible source
per cluster — no API calls, no cost, fully offline.
"""
from __future__ import annotations

import asyncio
import hashlib
import uuid
from typing import Dict, List, Optional, Set, Tuple

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Article

log = AgentLogger("dedup_agent")

# Source priority for cluster representative selection (lower = better)
SOURCE_PRIORITY: Dict[str, int] = {
    "pib_india":       1,
    "reuters_india":   2,
    "mea_india":       2,
    "ani_news":        3,
    "the_hindu":       4,
    "the_hindu_national": 4,
    "indian_express":  5,
    "ndtv_top":        5,
    "ndtv_india":      5,
    "economic_times":  5,
    "livemint":        6,
    "business_standard": 6,
    "hindustan_times": 7,
    "times_of_india":  7,
    "google_news_india": 6,
}


class DeduplicationAgent:
    """
    Removes near-duplicate articles using:
    1. URL exact-match (fast)
    2. Title hash (fast)
    3. Semantic cosine similarity via sentence-transformers (thorough)
    """

    AGENT_NAME = "dedup_agent"

    def __init__(
        self,
        similarity_threshold: float = 0.82,
        bus: Optional[EventBus] = None,
    ) -> None:
        self._bus       = bus or get_bus()
        self._db        = get_db()
        self._threshold = similarity_threshold
        self._model     = None   # lazy-loaded
        self._running   = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info("Deduplication Agent started.", action="task_started")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def deduplicate(self, articles: List[Article]) -> List[Article]:
        """
        Remove duplicates and return the best representative per cluster.
        """
        if not articles:
            return []

        log.info(
            f"Starting deduplication on {len(articles)} articles.",
            action="task_started",
        )

        # ── Step 1: Remove already-published URLs ─────────────
        articles = await self._filter_already_published(articles)
        log.debug(f"After published-URL filter: {len(articles)} articles")

        # ── Step 2: URL dedup ─────────────────────────────────
        articles = self._url_dedup(articles)
        log.debug(f"After URL dedup: {len(articles)} articles")

        # ── Step 3: Title hash dedup ──────────────────────────
        articles = self._title_hash_dedup(articles)
        log.debug(f"After title-hash dedup: {len(articles)} articles")

        # ── Step 4: Semantic similarity dedup ─────────────────
        articles = await self._semantic_dedup(articles)
        log.info(
            f"Deduplication complete. Final: {len(articles)} articles.",
            action="duplicate_removed",
        )

        await self._bus.publish(Event(
            Topics.ARTICLES_DEDUPED,
            payload={"count": len(articles)},
            source=self.AGENT_NAME,
        ))
        return articles

    # ── Step implementations ─────────────────────────────────

    async def _filter_already_published(self, articles: List[Article]) -> List[Article]:
        """Remove articles whose URLs have already been published."""
        filtered = []
        for a in articles:
            if not await self._db.url_exists(a.url):
                filtered.append(a)
            else:
                a.is_duplicate = True
                log.debug(f"Filtered already-published: {a.url[:80]}")
        return filtered

    def _url_dedup(self, articles: List[Article]) -> List[Article]:
        seen_urls: Set[str] = set()
        unique = []
        for a in articles:
            canonical = self._canonical_url(a.url)
            if canonical not in seen_urls:
                seen_urls.add(canonical)
                unique.append(a)
            else:
                a.is_duplicate = True
        return unique

    def _title_hash_dedup(self, articles: List[Article]) -> List[Article]:
        seen_hashes: Set[str] = set()
        unique = []
        for a in articles:
            h = hashlib.md5(a.title.lower().strip().encode()).hexdigest()
            if h not in seen_hashes:
                seen_hashes.add(h)
                unique.append(a)
            else:
                a.is_duplicate = True
        return unique

    async def _semantic_dedup(self, articles: List[Article]) -> List[Article]:
        """Cluster articles by semantic similarity and keep best per cluster."""
        if len(articles) < 2:
            return articles

        model = await self._load_model()
        if model is None:
            # Fallback: no semantic dedup if model unavailable
            log.warning("Sentence-transformer unavailable. Skipping semantic dedup.")
            return articles

        titles = [a.title for a in articles]
        loop = asyncio.get_event_loop()

        # Encode in thread pool (CPU-bound)
        embeddings = await loop.run_in_executor(
            None, lambda: model.encode(titles, normalize_embeddings=True, show_progress_bar=False)
        )

        # Greedy clustering
        n = len(articles)
        cluster_ids = [-1] * n
        next_cluster = 0

        for i in range(n):
            if cluster_ids[i] != -1:
                continue
            cluster_ids[i] = next_cluster
            for j in range(i + 1, n):
                if cluster_ids[j] != -1:
                    continue
                # Dot product of normalized vectors == cosine similarity
                sim = float(embeddings[i] @ embeddings[j])
                if sim >= self._threshold:
                    cluster_ids[j] = next_cluster
                    articles[j].is_duplicate = True
                    articles[j].cluster_id   = str(next_cluster)
            next_cluster += 1

        # Assign cluster IDs
        for i, a in enumerate(articles):
            if a.cluster_id is None:
                a.cluster_id = str(cluster_ids[i])

        # Pick best representative per cluster
        clusters: Dict[int, List[Tuple[int, Article]]] = {}
        for i, (cid, a) in enumerate(zip(cluster_ids, articles)):
            clusters.setdefault(cid, []).append((i, a))

        representatives = []
        for cid, members in clusters.items():
            best = self._pick_best(members)
            best.is_duplicate = False  # the winner is not a dup
            representatives.append(best)

        removed = len(articles) - len(representatives)
        log.info(f"Semantic dedup removed {removed} near-duplicates.")
        return representatives

    def _pick_best(self, members: List[Tuple[int, Article]]) -> Article:
        """Pick the article from the most credible source in a cluster."""
        def score(article: Article) -> int:
            return SOURCE_PRIORITY.get(article.source, 99)
        return min(members, key=lambda x: score(x[1]))[1]

    async def _load_model(self):
        """Lazy-load the sentence-transformer model. Disabled for Render Free Tier (512MB RAM)."""
        if getattr(self, "_model_warning_printed", False) is False:
            log.warning("Semantic dedup disabled to prevent out-of-memory errors on Render Free Tier.")
            self._model_warning_printed = True
        return None

    # ── Helpers ──────────────────────────────────────────────

    def _canonical_url(self, url: str) -> str:
        """Strip tracking params for URL comparison."""
        try:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(url.rstrip("/"))
            # Remove common tracking params
            remove = {"utm_source", "utm_medium", "utm_campaign", "utm_term",
                      "utm_content", "ref", "source", "fbclid", "gclid"}
            qs = {k: v for k, v in parse_qs(parsed.query).items() if k not in remove}
            clean = parsed._replace(query=urlencode(qs, doseq=True))
            return urlunparse(clean).lower()
        except Exception:
            return url.lower()

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
