"""
agents/website_knowledge.py — Website Knowledge Agent.

Maintains a continuously-updated profile for every news source.
Worker agents consult this before accessing any website to get
the best extraction strategy and retry config.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, ExtractionMethod, WebsiteProfile

log = AgentLogger("website_knowledge")

SOURCES_PATH = Path("config/sources.json")


class WebsiteKnowledgeAgent:
    """
    Loads source configs, maintains reliability scores, and provides
    strategy recommendations to all worker agents.
    """

    AGENT_NAME = "website_knowledge"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus = bus or get_bus()
        self._db  = get_db()
        self._profiles: Dict[str, WebsiteProfile] = {}
        self._sources: List[Dict[str, Any]] = []
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        await self._load_sources()
        await self._load_profiles_from_db()

        # Subscribe to success/failure events to update scores
        self._bus.subscribe(Topics.TASK_DONE,   self._on_task_done)
        self._bus.subscribe(Topics.TASK_FAILED, self._on_task_failed)

        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info(
            f"Website Knowledge Agent started. Loaded {len(self._sources)} sources.",
            action="task_started",
        )

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    # ── Public API ───────────────────────────────────────────

    def get_all_sources(self) -> List[Dict[str, Any]]:
        """Return all enabled news source configs."""
        return [s for s in self._sources if s.get("enabled", True)]

    def get_strategy(self, domain: str) -> Dict[str, Any]:
        """
        Return the best extraction strategy for a domain.
        Falls back to defaults if no profile exists.
        """
        profile = self._profiles.get(domain)
        if not profile:
            return {
                "method": ExtractionMethod.RSS,
                "max_retries": 3,
                "backoff": 5,
                "reliability": 1.0,
            }
        return {
            "method":      profile.preferred_method,
            "max_retries": 3 if profile.reliability_score > 0.7 else 1,
            "backoff":     5 if profile.reliability_score > 0.5 else 15,
            "reliability": profile.reliability_score,
            "has_captcha": profile.has_captcha,
            "robots_ok":   profile.robots_txt_allows,
        }

    def get_profile(self, domain: str) -> Optional[WebsiteProfile]:
        return self._profiles.get(domain)

    async def record_success(
        self, domain: str, response_time_ms: float = 0.0
    ) -> None:
        profile = self._get_or_create_profile(domain)
        profile.success_count += 1
        profile.last_accessed = datetime.utcnow()
        profile.last_success  = datetime.utcnow()
        # Exponential moving average for response time
        profile.avg_response_time_ms = (
            0.8 * profile.avg_response_time_ms + 0.2 * response_time_ms
            if profile.avg_response_time_ms > 0
            else response_time_ms
        )
        # Reliability decays slowly and recovers on success
        profile.reliability_score = min(
            1.0, profile.reliability_score + 0.02
        )
        await self._persist(profile)

    async def record_failure(self, domain: str, error: str = "") -> None:
        profile = self._get_or_create_profile(domain)
        profile.failure_count += 1
        profile.last_accessed = datetime.utcnow()
        # Reliability decays on failure
        profile.reliability_score = max(
            0.0, profile.reliability_score - 0.10
        )
        if "captcha" in error.lower():
            profile.has_captcha = True
        await self._persist(profile)

    # ── Internal helpers ─────────────────────────────────────

    async def _load_sources(self) -> None:
        try:
            data = json.loads(SOURCES_PATH.read_text())
            self._sources = data.get("sources", [])
            log.info(f"Loaded {len(self._sources)} source configs.")
        except Exception as e:
            log.error(f"Failed to load sources.json: {e}")
            self._sources = []

    async def _load_profiles_from_db(self) -> None:
        try:
            rows = await self._db.get_all_profiles()
            for row in rows:
                domain = row["domain"]
                # Parse JSON fields
                try:
                    row["retry_strategy"] = json.loads(row.get("retry_strategy", "{}"))
                except Exception:
                    row["retry_strategy"] = {}
                profile = WebsiteProfile(**{
                    k: v for k, v in row.items()
                    if k in WebsiteProfile.model_fields
                })
                self._profiles[domain] = profile
            log.info(f"Loaded {len(self._profiles)} website profiles from DB.")
        except Exception as e:
            log.error(f"Failed to load profiles from DB: {e}")

        # Seed profiles from sources.json if not in DB
        for source in self._sources:
            rss_url = source.get("rss_url", "")
            api_url = source.get("api_url", "")
            url = rss_url or api_url
            if not url:
                continue
            domain = self._extract_domain(url)
            if domain and domain not in self._profiles:
                method = ExtractionMethod(source.get("method", "rss"))
                profile = WebsiteProfile(
                    domain=domain,
                    has_rss=bool(rss_url),
                    rss_url=rss_url or None,
                    has_api=bool(api_url),
                    api_url=api_url or None,
                    preferred_method=method,
                    reliability_score=source.get("reliability", 0.90),
                )
                self._profiles[domain] = profile
                await self._persist(profile)

    def _get_or_create_profile(self, domain: str) -> WebsiteProfile:
        if domain not in self._profiles:
            self._profiles[domain] = WebsiteProfile(domain=domain)
        return self._profiles[domain]

    async def _persist(self, profile: WebsiteProfile) -> None:
        try:
            data = profile.model_dump(mode="json")
            await self._db.upsert_website_profile(data)
        except Exception as e:
            log.error(f"Failed to persist profile for {profile.domain}: {e}")

    def _extract_domain(self, url: str) -> str:
        try:
            import tldextract
            ext = tldextract.extract(url)
            return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain
        except Exception:
            from urllib.parse import urlparse
            return urlparse(url).netloc

    # ── Event Handlers ───────────────────────────────────────

    async def _on_task_done(self, event: Event) -> None:
        domain = event.payload.get("domain") or event.payload.get("source")
        rt     = event.payload.get("response_time_ms", 0.0)
        if domain:
            await self.record_success(domain, rt)

    async def _on_task_failed(self, event: Event) -> None:
        domain = event.payload.get("domain") or event.payload.get("source")
        error  = event.payload.get("error", "")
        if domain:
            await self.record_failure(domain, error)

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    metadata={"profiles_loaded": len(self._profiles)},
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(60)


# Singleton
_agent: Optional[WebsiteKnowledgeAgent] = None


def get_knowledge_agent() -> WebsiteKnowledgeAgent:
    global _agent
    if _agent is None:
        _agent = WebsiteKnowledgeAgent()
    return _agent
