"""
core/database.py — Async SQLite database layer via aiosqlite.

Schema: articles, incidents, agent_heartbeats, website_profiles,
        published_stories, performance_metrics, health_checks
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

DB_PATH = Path(os.getenv("DATABASE_PATH", "storage/news.db"))


# ─────────────────────────────────────────────────────────────
#  Schema DDL
# ─────────────────────────────────────────────────────────────

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS articles (
    id                  TEXT PRIMARY KEY,
    run_id              TEXT,
    title               TEXT NOT NULL,
    url                 TEXT NOT NULL,
    source              TEXT NOT NULL,
    publisher           TEXT NOT NULL,
    published_at        TEXT,
    fetched_at          TEXT NOT NULL,
    category            TEXT DEFAULT 'general',
    raw_content         TEXT,
    summary             TEXT,
    headline            TEXT,
    confidence_score    REAL DEFAULT 0.0,
    rank                INTEGER,
    extraction_method   TEXT DEFAULT 'rss',
    is_verified         INTEGER DEFAULT 0,
    is_duplicate        INTEGER DEFAULT 0,
    duplicate_of        TEXT,
    cluster_id          TEXT,
    tags                TEXT DEFAULT '[]',
    metadata            TEXT DEFAULT '{}',
    qa_passed           INTEGER DEFAULT 0,
    qa_notes            TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_articles_run_id ON articles(run_id);
CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);

CREATE TABLE IF NOT EXISTS incidents (
    id                      TEXT PRIMARY KEY,
    timestamp               TEXT NOT NULL,
    agent_name              TEXT NOT NULL,
    task_id                 TEXT,
    website                 TEXT,
    url                     TEXT,
    severity                TEXT DEFAULT 'medium',
    error_type              TEXT NOT NULL,
    error_message           TEXT NOT NULL,
    stack_trace             TEXT,
    retry_count             INTEGER DEFAULT 0,
    recovery_method         TEXT,
    resolution              TEXT,
    final_status            TEXT DEFAULT 'open',
    execution_time_seconds  REAL,
    created_at              TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_incidents_agent ON incidents(agent_name);
CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(final_status);
CREATE INDEX IF NOT EXISTS idx_incidents_status_time ON incidents(final_status, timestamp DESC);

CREATE TABLE IF NOT EXISTS agent_heartbeats (
    agent_name          TEXT PRIMARY KEY,
    status              TEXT NOT NULL,
    timestamp           TEXT NOT NULL,
    current_task        TEXT,
    tasks_completed     INTEGER DEFAULT 0,
    tasks_failed        INTEGER DEFAULT 0,
    metadata            TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS website_profiles (
    domain              TEXT PRIMARY KEY,
    has_rss             INTEGER DEFAULT 0,
    rss_url             TEXT,
    has_api             INTEGER DEFAULT 0,
    api_url             TEXT,
    requires_login      INTEGER DEFAULT 0,
    has_captcha         INTEGER DEFAULT 0,
    robots_txt_allows   INTEGER DEFAULT 1,
    preferred_method    TEXT DEFAULT 'rss',
    success_count       INTEGER DEFAULT 0,
    failure_count       INTEGER DEFAULT 0,
    reliability_score   REAL DEFAULT 1.0,
    avg_response_time_ms REAL DEFAULT 0.0,
    last_accessed       TEXT,
    last_success        TEXT,
    retry_strategy      TEXT DEFAULT '{}',
    notes               TEXT DEFAULT '',
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS published_stories (
    id                  TEXT PRIMARY KEY,
    article_id          TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    published_at        TEXT NOT NULL,
    discord_message_id  TEXT,
    rank                INTEGER NOT NULL,
    category            TEXT NOT NULL,
    headline            TEXT NOT NULL,
    summary             TEXT NOT NULL,
    source_url          TEXT NOT NULL,
    publisher           TEXT NOT NULL,
    confidence_score    REAL DEFAULT 0.0,
    FOREIGN KEY (article_id) REFERENCES articles(id)
);

CREATE INDEX IF NOT EXISTS idx_published_run ON published_stories(run_id);
CREATE INDEX IF NOT EXISTS idx_published_at ON published_stories(published_at);
CREATE INDEX IF NOT EXISTS idx_published_url ON published_stories(source_url);

CREATE TABLE IF NOT EXISTS performance_metrics (
    id                          TEXT PRIMARY KEY,
    timestamp                   TEXT NOT NULL,
    run_id                      TEXT UNIQUE NOT NULL,
    articles_fetched            INTEGER DEFAULT 0,
    articles_after_dedup        INTEGER DEFAULT 0,
    articles_verified           INTEGER DEFAULT 0,
    articles_published          INTEGER DEFAULT 0,
    articles_rejected           INTEGER DEFAULT 0,
    fetch_duration_seconds      REAL DEFAULT 0.0,
    processing_duration_seconds REAL DEFAULT 0.0,
    total_duration_seconds      REAL DEFAULT 0.0,
    sources_successful          INTEGER DEFAULT 0,
    sources_failed              INTEGER DEFAULT 0,
    incidents_created           INTEGER DEFAULT 0,
    llm_calls                   INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_perf_timestamp ON performance_metrics(timestamp DESC);

CREATE TABLE IF NOT EXISTS health_checks (
    id              TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_name   TEXT NOT NULL,
    url             TEXT,
    is_healthy      INTEGER NOT NULL,
    response_time_ms REAL,
    status_code     INTEGER,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_health_resource ON health_checks(resource_name);
CREATE INDEX IF NOT EXISTS idx_health_time ON health_checks(timestamp);
CREATE INDEX IF NOT EXISTS idx_health_prune ON health_checks(timestamp);
"""


# ─────────────────────────────────────────────────────────────
#  Database class
# ─────────────────────────────────────────────────────────────

class Database:
    def __init__(self, path: Path = DB_PATH) -> None:
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA busy_timeout = 5000;")
        await self._conn.execute("PRAGMA foreign_keys = ON;")
        await self._conn.executescript(SCHEMA_SQL)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    # ── Articles ─────────────────────────────────────────────

    async def upsert_article(self, a: Dict[str, Any]) -> None:
        sql = """
        INSERT OR REPLACE INTO articles
            (id, run_id, title, url, source, publisher, published_at, fetched_at,
             category, raw_content, summary, headline, confidence_score, rank,
             extraction_method, is_verified, is_duplicate, duplicate_of, cluster_id,
             tags, metadata, qa_passed, qa_notes)
        VALUES
            (:id, :run_id, :title, :url, :source, :publisher, :published_at, :fetched_at,
             :category, :raw_content, :summary, :headline, :confidence_score, :rank,
             :extraction_method, :is_verified, :is_duplicate, :duplicate_of, :cluster_id,
             :tags, :metadata, :qa_passed, :qa_notes)
        """
        row = {**a}
        row["tags"]     = json.dumps(row.get("tags", []))
        row["metadata"] = json.dumps(row.get("metadata", {}))
        row["is_verified"]  = int(row.get("is_verified",  False))
        row["is_duplicate"] = int(row.get("is_duplicate", False))
        row["qa_passed"]    = int(row.get("qa_passed",    False))
        await self._conn.execute(sql, row)
        await self._conn.commit()

    async def get_articles_by_run(self, run_id: str) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM articles WHERE run_id = ? ORDER BY rank ASC NULLS LAST",
            (run_id,)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def url_exists(self, url: str) -> bool:
        async with self._conn.execute(
            "SELECT 1 FROM published_stories WHERE source_url = ? LIMIT 1", (url,)
        ) as cur:
            return await cur.fetchone() is not None

    async def get_recent_headlines(self, hours: int = 48) -> List[str]:
        cutoff = datetime.utcnow().isoformat()
        async with self._conn.execute(
            """SELECT headline FROM published_stories
               WHERE published_at >= datetime('now', ?)
               ORDER BY published_at DESC""",
            (f"-{hours} hours",)
        ) as cur:
            rows = await cur.fetchall()
        return [r["headline"] for r in rows if r["headline"]]

    # ── Incidents ────────────────────────────────────────────

    async def save_incident(self, inc: Dict[str, Any]) -> None:
        sql = """
        INSERT OR REPLACE INTO incidents
            (id, timestamp, agent_name, task_id, website, url, severity,
             error_type, error_message, stack_trace, retry_count,
             recovery_method, resolution, final_status, execution_time_seconds)
        VALUES
            (:id, :timestamp, :agent_name, :task_id, :website, :url, :severity,
             :error_type, :error_message, :stack_trace, :retry_count,
             :recovery_method, :resolution, :final_status, :execution_time_seconds)
        """
        await self._conn.execute(sql, inc)
        await self._conn.commit()

    async def get_open_incidents(self) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM incidents WHERE final_status = 'open' ORDER BY timestamp DESC"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def resolve_incident(self, incident_id: str, resolution: str) -> None:
        await self._conn.execute(
            "UPDATE incidents SET final_status='resolved', resolution=? WHERE id=?",
            (resolution, incident_id)
        )
        await self._conn.commit()

    # ── Heartbeats ───────────────────────────────────────────

    async def upsert_heartbeat(self, hb: Dict[str, Any]) -> None:
        sql = """
        INSERT OR REPLACE INTO agent_heartbeats
            (agent_name, status, timestamp, current_task,
             tasks_completed, tasks_failed, metadata)
        VALUES
            (:agent_name, :status, :timestamp, :current_task,
             :tasks_completed, :tasks_failed, :metadata)
        """
        row = {**hb}
        row["metadata"] = json.dumps(row.get("metadata", {}))
        await self._conn.execute(sql, row)
        await self._conn.commit()

    async def get_all_heartbeats(self) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM agent_heartbeats ORDER BY timestamp DESC"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ── Website Profiles ─────────────────────────────────────

    async def upsert_website_profile(self, wp: Dict[str, Any]) -> None:
        sql = """
        INSERT OR REPLACE INTO website_profiles
            (domain, has_rss, rss_url, has_api, api_url, requires_login,
             has_captcha, robots_txt_allows, preferred_method, success_count,
             failure_count, reliability_score, avg_response_time_ms,
             last_accessed, last_success, retry_strategy, notes, updated_at)
        VALUES
            (:domain, :has_rss, :rss_url, :has_api, :api_url, :requires_login,
             :has_captcha, :robots_txt_allows, :preferred_method, :success_count,
             :failure_count, :reliability_score, :avg_response_time_ms,
             :last_accessed, :last_success, :retry_strategy, :notes, datetime('now'))
        """
        row = {**wp}
        row["retry_strategy"] = json.dumps(row.get("retry_strategy", {}))
        row["has_rss"]              = int(row.get("has_rss", False))
        row["has_api"]              = int(row.get("has_api", False))
        row["requires_login"]       = int(row.get("requires_login", False))
        row["has_captcha"]          = int(row.get("has_captcha", False))
        row["robots_txt_allows"]    = int(row.get("robots_txt_allows", True))
        await self._conn.execute(sql, row)
        await self._conn.commit()

    async def get_website_profile(self, domain: str) -> Optional[Dict]:
        async with self._conn.execute(
            "SELECT * FROM website_profiles WHERE domain = ?", (domain,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def get_all_profiles(self) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM website_profiles ORDER BY reliability_score DESC"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ── Published Stories ────────────────────────────────────

    async def save_published_story(self, ps: Dict[str, Any]) -> None:
        sql = """
        INSERT OR REPLACE INTO published_stories
            (id, article_id, run_id, published_at, discord_message_id, rank,
             category, headline, summary, source_url, publisher, confidence_score)
        VALUES
            (:id, :article_id, :run_id, :published_at, :discord_message_id, :rank,
             :category, :headline, :summary, :source_url, :publisher, :confidence_score)
        """
        await self._conn.execute(sql, ps)
        await self._conn.commit()

    async def get_published_today(self) -> List[Dict]:
        async with self._conn.execute(
            """SELECT * FROM published_stories
               WHERE published_at >= datetime('now', 'start of day')
               ORDER BY published_at DESC"""
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_published_by_category(self, category: str, limit: int = 10) -> List[Dict]:
        async with self._conn.execute(
            """SELECT * FROM published_stories WHERE category = ?
               ORDER BY published_at DESC LIMIT ?""",
            (category, limit)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def search_published(self, keyword: str, limit: int = 10) -> List[Dict]:
        pattern = f"%{keyword}%"
        async with self._conn.execute(
            """SELECT * FROM published_stories
               WHERE headline LIKE ? OR summary LIKE ?
               ORDER BY published_at DESC LIMIT ?""",
            (pattern, pattern, limit)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ── Performance Metrics ──────────────────────────────────

    async def save_metric(self, m: Dict[str, Any]) -> None:
        sql = """
        INSERT OR REPLACE INTO performance_metrics
            (id, timestamp, run_id, articles_fetched, articles_after_dedup,
             articles_verified, articles_published, articles_rejected,
             fetch_duration_seconds, processing_duration_seconds,
             total_duration_seconds, sources_successful, sources_failed,
             incidents_created, llm_calls)
        VALUES
            (:id, :timestamp, :run_id, :articles_fetched, :articles_after_dedup,
             :articles_verified, :articles_published, :articles_rejected,
             :fetch_duration_seconds, :processing_duration_seconds,
             :total_duration_seconds, :sources_successful, :sources_failed,
             :incidents_created, :llm_calls)
        """
        await self._conn.execute(sql, m)
        await self._conn.commit()

    async def get_recent_metrics(self, limit: int = 10) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM performance_metrics ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ── Health Checks ────────────────────────────────────────

    async def save_health_check(self, hc: Dict[str, Any]) -> None:
        sql = """
        INSERT INTO health_checks
            (id, timestamp, resource_type, resource_name, url,
             is_healthy, response_time_ms, status_code, error)
        VALUES
            (:id, :timestamp, :resource_type, :resource_name, :url,
             :is_healthy, :response_time_ms, :status_code, :error)
        """
        hc["is_healthy"] = int(hc.get("is_healthy", True))
        await self._conn.execute(sql, hc)
        await self._conn.commit()

    async def get_unhealthy_resources(self, hours: int = 1) -> List[Dict]:
        async with self._conn.execute(
            """SELECT resource_name, resource_type, COUNT(*) as fail_count
               FROM health_checks
               WHERE is_healthy = 0
               AND timestamp >= datetime('now', ?)
               GROUP BY resource_name
               ORDER BY fail_count DESC""",
            (f"-{hours} hours",)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def prune_old_health_checks(self, retention_days: int = 7) -> int:
        """
        Delete health check records older than retention_days (default: 7).
        Returns the count of deleted rows.
        """
        cursor = await self._conn.execute(
            "DELETE FROM health_checks WHERE timestamp < datetime('now', ?)",
            (f"-{retention_days} days",),
        )
        await self._conn.commit()
        return cursor.rowcount


# ─────────────────────────────────────────────────────────────
#  Singleton accessor
# ─────────────────────────────────────────────────────────────

_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


async def init_db() -> Database:
    db = get_db()
    await db.connect()
    index_migrations = [
        "CREATE INDEX IF NOT EXISTS idx_published_url ON published_stories(source_url);",
        "CREATE INDEX IF NOT EXISTS idx_perf_timestamp ON performance_metrics(timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_incidents_status_time ON incidents(final_status, timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_health_prune ON health_checks(timestamp);",
    ]
    for migration_sql in index_migrations:
        await db._conn.execute(migration_sql)
    await db._conn.commit()
    return db
