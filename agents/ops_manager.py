"""
agents/ops_manager.py — Operations Manager AI.

Manages the task queue, assigns work to agents, schedules collection
runs, tracks task state, and prevents duplicate work.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import get_settings
from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import (
    AgentHeartbeat, AgentStatus, Article, PerformanceMetric,
    PublishedStory, RunContext, TaskItem, TaskStatus
)

from agents.dedup_agent      import DeduplicationAgent
from agents.fact_verifier    import FactVerifierAgent
from agents.qa_agent         import QAAgent
from agents.ranker           import RankerAgent
from agents.recovery_engine  import get_recovery_engine
from agents.source_discovery import SourceDiscoveryAgent
from agents.summarizer       import SummarizerAgent

log = AgentLogger("ops_manager")


class OperationsManager:
    """
    The central coordinator of the news pipeline.

    Lifecycle of a run:
      collect → dedup → verify → summarize → rank → qa → publish
    """

    AGENT_NAME = "ops_manager"

    def __init__(
        self,
        bus: Optional[EventBus] = None,
    ) -> None:
        self._bus        = bus or get_bus()
        self._db         = get_db()
        self._settings   = get_settings()
        self._recovery   = get_recovery_engine()
        self._scheduler  = AsyncIOScheduler(timezone="Asia/Kolkata")
        self._running    = False
        self._active_run: Optional[RunContext] = None
        self._runs_done  = 0
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Pipeline stages
        self._discovery  = SourceDiscoveryAgent(bus=self._bus)
        self._dedup      = DeduplicationAgent(
            similarity_threshold=self._settings.dedup_similarity_threshold,
            bus=self._bus,
        )
        self._verifier   = FactVerifierAgent(
            bus=self._bus,
            min_confidence=self._settings.min_confidence_score,
        )
        self._summarizer = SummarizerAgent(bus=self._bus)
        self._ranker     = RankerAgent(bus=self._bus)
        self._qa         = QAAgent(bus=self._bus)

    @property
    def discovery_agent(self) -> SourceDiscoveryAgent:
        return self._discovery

    async def start(self, run_on_start: bool = True) -> None:
        self._running = True

        # Prevent duplicate heartbeat loops on restart
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

        # Register restart callbacks with RecoveryEngine
        self._register_restart_callbacks()

        # Start all pipeline agents
        await self._discovery.start()
        await self._dedup.start()
        await self._verifier.start()
        await self._summarizer.start()
        await self._ranker.start()
        await self._qa.start()

        # Auto-scheduling disabled per user request
        # interval_hours = self._settings.publish_interval_hours
        # self._scheduler.add_job(
        #     self._run_pipeline,
        #     IntervalTrigger(hours=interval_hours),
        #     id="news_pipeline",
        #     name=f"News Pipeline (every {interval_hours}h)",
        #     max_instances=1,
        #     replace_existing=True,
        # )
        # self._scheduler.start()

        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )

        log.info(
            "Operations Manager started. Awaiting manual chat trigger (/news).",
            action="task_started",
        )

        # Initial run disabled so it only starts on chat trigger
        # if run_on_start:
        #     asyncio.create_task(self._run_pipeline_safe(), name="initial_run")

    def _register_restart_callbacks(self) -> None:
        """Register self and managed child worker restart callbacks with RecoveryEngine."""
        recovery = self._recovery or get_recovery_engine()
        recovery.register_restart_callback(self.AGENT_NAME, self.restart)
        recovery.register_restart_callback("ops_manager", self.restart)
        recovery.register_restart_callback("source_discovery", self._restart_discovery)
        recovery.register_restart_callback("rss_worker", self._restart_rss)
        recovery.register_restart_callback("api_worker", self._restart_api)
        recovery.register_restart_callback("browser_worker", self._restart_browser)
        recovery.register_restart_callback("dedup_agent", self._restart_dedup)
        recovery.register_restart_callback("fact_verifier", self._restart_verifier)
        recovery.register_restart_callback("summarizer", self._restart_summarizer)
        recovery.register_restart_callback("ranker", self._restart_ranker)
        recovery.register_restart_callback("qa_agent", self._restart_qa)
        log.info("Registered agent restart callbacks with RecoveryEngine.")

    async def restart(self) -> None:
        """Cleanly restart OperationsManager without triggering an initial pipeline run."""
        log.info("Restarting OperationsManager...")
        await self.stop()
        await self.start(run_on_start=False)

    async def _restart_discovery(self) -> None:
        await self._discovery.stop()
        await self._discovery.start()

    async def _restart_rss(self) -> None:
        await self._discovery._rss.stop()
        await self._discovery._rss.start()

    async def _restart_api(self) -> None:
        await self._discovery._api.stop()
        await self._discovery._api.start()

    async def _restart_browser(self) -> None:
        await self._discovery._browser.stop()
        await self._discovery._browser.start()

    async def _restart_dedup(self) -> None:
        await self._dedup.stop()
        await self._dedup.start()

    async def _restart_verifier(self) -> None:
        await self._verifier.stop()
        await self._verifier.start()

    async def _restart_summarizer(self) -> None:
        await self._summarizer.stop()
        await self._summarizer.start()

    async def _restart_ranker(self) -> None:
        await self._ranker.stop()
        await self._ranker.start()

    async def _restart_qa(self) -> None:
        await self._qa.stop()
        await self._qa.start()

    async def stop(self) -> None:
        self._running = False
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        await self._discovery.stop()
        await self._dedup.stop()
        await self._verifier.stop()
        await self._summarizer.stop()
        await self._ranker.stop()
        await self._qa.stop()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    # ── Pipeline ──────────────────────────────────────────────

    async def _run_pipeline_safe(self) -> None:
        """Wrapper with top-level exception handling."""
        try:
            await self._run_pipeline()
        except Exception as e:
            log.exception(f"Pipeline run crashed: {e}")
            await self._bus.publish(Event(
                Topics.AGENT_FAILED,
                payload={
                    "agent": self.AGENT_NAME,
                    "error": str(e),
                },
                source=self.AGENT_NAME,
            ))

    async def _run_pipeline(self) -> None:
        """Execute the full news pipeline."""
        if self._active_run is not None:
            log.warning("Pipeline already running — skipping this trigger.")
            return

        run = RunContext()
        self._active_run = run
        t0 = datetime.now(timezone.utc)

        await self._bus.publish(Event(
            Topics.RUN_STARTED,
            payload={"run_id": run.run_id},
            source=self.AGENT_NAME,
        ))
        log.info(f"=== Pipeline Run Started: {run.run_id} ===", action="task_started")

        try:
            # Stage 1: Collect
            t1 = datetime.now(timezone.utc)
            raw_articles = await self._discovery.collect(run.run_id)
            fetch_dur = (datetime.now(timezone.utc) - t1).total_seconds()

            if not raw_articles:
                log.warning("No articles collected. Aborting run.")
                return

            # Stage 2: Dedup
            deduped_articles = await self._dedup.deduplicate(raw_articles)
            if not deduped_articles:
                log.warning("All articles were duplicates. Aborting run.")
                return

            # Stage 3: Fact verify
            verified_articles = await self._verifier.verify_batch(deduped_articles)
            if not verified_articles:
                log.warning("All articles failed verification. Aborting run.")
                return

            # Stage 4: Summarize (LLM)
            summarized_articles = await self._summarizer.summarize_batch(verified_articles)

            # Stage 5: Rank
            top_n = self._settings.top_stories_to_publish
            ranked_articles = await self._ranker.rank(summarized_articles, top_n=top_n * 2)  # rank 2× to leave QA buffer

            # Stage 6: QA
            qa_articles = await self._qa.run_qa(ranked_articles)
            final_articles = qa_articles[:top_n]  # final cap

            if not final_articles:
                log.warning("All articles failed QA. Nothing to publish.")
                return

            # Stage 7: Persist articles to DB
            for a in final_articles:
                try:
                    await self._db.upsert_article(a.model_dump(mode="json"))
                except Exception as e:
                    log.error(f"Failed to save article {a.id}: {e}")

            # Stage 8: Publish
            run.articles = final_articles
            published = await self._publish(final_articles, run.run_id)
            run.published = published

            # Record metrics
            total_dur = (datetime.now(timezone.utc) - t0).total_seconds()
            metric = PerformanceMetric(
                run_id=run.run_id,
                articles_fetched=len(raw_articles),
                articles_after_dedup=len(deduped_articles),
                articles_verified=len(verified_articles),
                articles_published=len(published),
                articles_rejected=max(0, len(raw_articles) - len(published)),
                fetch_duration_seconds=fetch_dur,
                total_duration_seconds=total_dur,
                llm_calls=len(verified_articles),
            )
            await self._db.save_metric(metric.model_dump(mode="json"))

            AgentLogger("ops_manager").perf(
                f"Pipeline run {run.run_id} completed: fetched={len(raw_articles)}, published={len(published)}",
                run_id=run.run_id,
                duration=total_dur,
            )

            run.status = "done"
            self._runs_done += 1

            await self._bus.publish(Event(
                Topics.RUN_COMPLETED,
                payload={
                    "run_id":    run.run_id,
                    "published": len(published),
                    "duration":  round(total_dur, 1),
                },
                source=self.AGENT_NAME,
            ))
            log.info(
                f"=== Pipeline Run Complete: {len(published)} stories published "
                f"in {total_dur:.1f}s ===",
                action="published",
            )

        except Exception as e:
            run.status = "failed"
            log.exception(f"Pipeline run failed: {e}")
            raise
        finally:
            self._active_run = None

    async def _publish(
        self, articles: List[Article], run_id: str
    ) -> List[PublishedStory]:
        """Save published records to DB."""
        published = []
        for a in articles:
            ps = PublishedStory(
                article_id=a.id,
                run_id=run_id,
                rank=a.rank or 99,
                category=a.category.value,
                headline=a.headline or a.title,
                summary=a.summary or "",
                source_url=a.url,
                publisher=a.publisher,
                confidence_score=a.confidence_score,
            )
            try:
                await self._db.save_published_story(ps.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Failed to save published story: {e}")

            published.append(ps)

        # Log published stories to console/file
        log.info(f"Published {len(published)} stories to dashboard.")
        for ps in published:
            log.info(f"  [{ps.rank}] {ps.headline[:80]}")

        return published

    async def trigger_run(self) -> None:
        """Manually trigger a pipeline run (e.g. from /news slash command)."""
        asyncio.create_task(self._run_pipeline_safe(), name="manual_run")

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING if not self._active_run else AgentStatus.RUNNING,
                    tasks_completed=self._runs_done,
                    current_task=self._active_run.run_id if self._active_run else None,
                    metadata={"active_run": bool(self._active_run)},
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(30)
