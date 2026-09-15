"""
core/models.py — Pydantic data models for the India News Intelligence Platform.

Every piece of data flowing through the system is typed here.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────
#  Enumerations
# ─────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING      = "pending"
    IN_PROGRESS  = "in_progress"
    DONE         = "done"
    FAILED       = "failed"
    CANCELLED    = "cancelled"
    RETRY        = "retry"
    ESCALATED    = "escalated"


class AgentStatus(str, Enum):
    STARTING  = "starting"
    RUNNING   = "running"
    IDLE      = "idle"
    PAUSED    = "paused"
    FAILED    = "failed"
    STOPPED   = "stopped"


class Severity(str, Enum):
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"


class ExtractionMethod(str, Enum):
    API              = "api"
    RSS              = "rss"
    GOOGLE_NEWS_RSS  = "google_news_rss"
    HTML             = "html"
    BROWSER          = "browser"


class Category(str, Enum):
    POLITICS          = "politics"
    FINANCE           = "finance"
    ECONOMY           = "economy"
    BUSINESS          = "business"
    STOCK_MARKET      = "stock_market"
    TECHNOLOGY        = "technology"
    AI                = "ai"
    STARTUPS          = "startups"
    SPORTS            = "sports"
    NATIONAL_SECURITY = "national_security"
    GOVERNMENT        = "government"
    CRIME             = "crime"
    ACCIDENTS         = "accidents"
    NATURAL_DISASTERS = "natural_disasters"
    INTERNATIONAL     = "international"
    GENERAL           = "general"


class RecoveryAction(str, Enum):
    RETRY            = "retry"
    RESTART_AGENT    = "restart_agent"
    RESTART_BROWSER  = "restart_browser"
    SWITCH_SOURCE    = "switch_source"
    SWITCH_RSS       = "switch_rss"
    SWITCH_API       = "switch_api"
    SKIP_SOURCE      = "skip_source"
    REASSIGN_TASK    = "reassign_task"
    ESCALATE         = "escalate"
    NOTIFY_DIRECTOR  = "notify_director"


# ─────────────────────────────────────────────────────────────
#  Core Data Models
# ─────────────────────────────────────────────────────────────

class Article(BaseModel):
    """A news article collected from any source."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str
    url: str
    source: str                        # e.g. "the_hindu"
    publisher: str                     # e.g. "The Hindu"
    published_at: Optional[datetime] = None
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    category: Category = Category.GENERAL
    raw_content: Optional[str] = None
    summary: Optional[str] = None
    headline: Optional[str] = None     # cleaned headline after QA
    confidence_score: float = 0.0      # 0.0 – 1.0
    rank: Optional[int] = None         # 1 = most important
    extraction_method: ExtractionMethod = ExtractionMethod.RSS
    is_verified: bool = False
    is_duplicate: bool = False
    duplicate_of: Optional[str] = None
    cluster_id: Optional[str] = None   # dedup cluster
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    qa_passed: bool = False
    qa_notes: Optional[str] = None
    run_id: Optional[str] = None


class TaskItem(BaseModel):
    """A unit of work assigned to an agent by the Operations Manager."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str                     # e.g. "fetch_rss", "summarize", "publish"
    agent: Optional[str] = None        # assigned agent name
    priority: int = 5                  # 1 = highest, 10 = lowest
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class Incident(BaseModel):
    """A structured failure record — every error becomes an incident."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    agent_name: str
    task_id: Optional[str] = None
    website: Optional[str] = None
    url: Optional[str] = None
    severity: Severity = Severity.MEDIUM
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    retry_count: int = 0
    recovery_method: Optional[RecoveryAction] = None
    resolution: Optional[str] = None
    final_status: str = "open"        # open | resolved | escalated
    execution_time_seconds: Optional[float] = None


class WebsiteProfile(BaseModel):
    """Continuously-updated profile of a news source's reliability and access strategy."""
    domain: str
    has_rss: bool = False
    rss_url: Optional[str] = None
    has_api: bool = False
    api_url: Optional[str] = None
    requires_login: bool = False
    has_captcha: bool = False
    robots_txt_allows: bool = True
    preferred_method: ExtractionMethod = ExtractionMethod.RSS
    success_count: int = 0
    failure_count: int = 0
    reliability_score: float = 1.0    # decays on failures, grows on success
    avg_response_time_ms: float = 0.0
    last_accessed: Optional[datetime] = None
    last_success: Optional[datetime] = None
    retry_strategy: Dict[str, Any] = Field(default_factory=dict)
    notes: str = ""


class AgentHeartbeat(BaseModel):
    """Periodic heartbeat emitted by every agent so the supervisor can detect crashes."""
    agent_name: str
    status: AgentStatus
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class HealthCheck(BaseModel):
    """Result of a single health-check ping against a resource."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    resource_type: str                 # api | rss | network | database
    resource_name: str
    url: Optional[str] = None
    is_healthy: bool = True
    response_time_ms: Optional[float] = None
    status_code: Optional[int] = None
    error: Optional[str] = None


class PerformanceMetric(BaseModel):
    """End-of-run analytics snapshot."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    run_id: str
    articles_fetched: int = 0
    articles_after_dedup: int = 0
    articles_verified: int = 0
    articles_published: int = 0
    articles_rejected: int = 0
    fetch_duration_seconds: float = 0.0
    processing_duration_seconds: float = 0.0
    total_duration_seconds: float = 0.0
    sources_successful: int = 0
    sources_failed: int = 0
    incidents_created: int = 0
    llm_calls: int = 0


class PublishedStory(BaseModel):
    """A record of a story that was published to the dashboard."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    article_id: str
    run_id: str
    published_at: datetime = Field(default_factory=datetime.utcnow)
    discord_message_id: Optional[str] = None  # kept for DB schema compatibility
    rank: int
    category: str
    headline: str
    summary: str
    source_url: str
    publisher: str
    confidence_score: float = 0.0


class RunContext(BaseModel):
    """Shared context object for a single collection-to-publish run."""
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    articles: List[Article] = Field(default_factory=list)
    published: List[PublishedStory] = Field(default_factory=list)
    metrics: Optional[PerformanceMetric] = None
    status: str = "running"           # running | done | failed
