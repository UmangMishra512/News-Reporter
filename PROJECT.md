# Project: India News Intelligence Platform - System Audit & E2E Dry-Run Pipeline Verification

## Architecture
The platform is an autonomous news intelligence system consisting of:
1. **Entry Point & CLI (`main.py`)**: Parses `--dry-run` and `--once` flags; manages startup, credential validation, agent orchestration via `OperationsManager`, and graceful shutdown.
2. **Configuration & Settings (`config/settings.py`, `config/sources.json`, `config/categories.json`)**: Environment-based configuration, 25 Indian news RSS/API sources, 16 categories with ranking weights.
3. **Decoupled Event Bus (`core/events.py`)**: Asynchronous pub/sub event distribution across agents using standard event topics.
4. **Pipeline Stages (`agents/ops_manager.py`)**:
   - Stage 1: Discovery & Collection (`agents/source_discovery.py`, `agents/rss_worker.py`)
   - Stage 2: Deduplication (`agents/dedup_agent.py` - URL, title hash, and SentenceTransformer semantic cosine similarity)
   - Stage 3: Fact Verification (`agents/fact_verifier.py` - algorithmic corroboration and source trustworthiness)
   - Stage 4: Summarization (`agents/summarizer.py` - LLM generation with fallback to keyword extraction)
   - Stage 5: Algorithmic Ranking (`agents/ranker.py` - 9-dimensional scoring)
   - Stage 6: Quality Assurance Gate (`agents/qa_agent.py` - headline/summary/length checks and cross-run duplicate avoidance)
   - Stage 7: Article Persistence (`core/database.py` - `articles` table)
   - Stage 8: Story Publication & Metrics (`core/database.py` - `published_stories`, `performance_metrics` tables)
5. **Observability & Error Recovery (`agents/failure_analyzer.py`, `agents/recovery_engine.py`, `core/logger.py`)**: Multi-target logging (`agent.log`, `incidents.log`, `performance.log`, console) and SQLite persistence for incidents, heartbeats, and health checks.

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | CLI Flag Parsing (`--dry-run`, `--once`) | Supports running a single standalone execution without launching Discord bot | M1, M2 | survey |
| 2 | Environment & Credential Validation | Configures environment (`.env`, `LLM_PROVIDER`) and validates credentials | M1 | survey |
| 3 | Pre-Run DB Baseline Capture | Captures pre-run baseline counts from `storage/news.db` | M1 | survey |
| 4 | Standalone Pipeline Execution | Executes pipeline once without automatic scheduler (`AsyncIOScheduler`) | M2 | survey |
| 5 | Source Discovery & RSS Ingestion | Concurrent collection of articles from Indian news sources | M2 | survey |
| 6 | URL & Semantic Deduplication | Multi-pass dedup (URL canonicalization, title MD5, semantic clustering) | M2 | survey |
| 7 | Algorithmic Fact Verification | Corroboration scoring and source trust evaluation | M2 | survey |
| 8 | Summarization with Fallback | LLM summarization with heuristic fallback on API failure | M2 | survey |
| 9 | 9-Dimensional Ranking | Composite scoring and top-story ranking | M2 | survey |
| 10 | QA Filter Gate | Quality checks and cross-run duplicate rejection | M2 | survey |
| 11 | `published_stories` DB Persistence | In dry-run mode, stories are persisted to SQLite table with NULL discord_id | M3 | survey |
| 12 | `performance_metrics` DB Persistence | Captures run metrics (fetched, published, duration) in SQLite | M3 | survey |
| 13 | Structured Log Verification | Multi-target logs (`agent.log`, `incidents.log`, stdout) record run lifecycle | M3 | survey |
| 14 | Forensic Integrity & Verification | Forensic verification of zero cheating, genuine execution, and exit code 0 | M4 | survey |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| 1 | Pre-flight & Baseline Snapshot | Ensure `.env`/runtime readiness and record baseline counts in `storage/news.db` | none | DONE |
| 2 | End-to-End Dry Run Execution (R1) | Execute `python3 main.py --dry-run --once`, monitor execution, verify exit code 0 and exception-free logs | M1 | IN_PROGRESS |
| 3 | Core System Health & DB Audit (R2) | Audit `storage/news.db` tables (`published_stories`, `performance_metrics`, `articles`, `incidents`) | M2 | PLANNED |
| 4 | Multi-Agent Review & Forensic Integrity Audit | Reviewers, Challengers, and Forensic Auditor inspect integrity, logs, and acceptance criteria | M3 | PLANNED |

## Interface Contracts
### CLI & Entry Point ↔ Pipeline
- Invocation: `.venv/bin/python3 main.py --dry-run --once` (or `python3 main.py --dry-run --once` with venv activated).
- Exit Code: 0 on successful pipeline completion.
- Flags: `--dry-run` (skips Discord connection), `--once` (executes single pipeline pass and shuts down).

### Pipeline ↔ SQLite Database (`storage/news.db`)
- `articles`: Schema with `id`, `run_id`, `title`, `url`, `category`, `confidence_score`, `rank`, `qa_passed`.
- `published_stories`: Schema with `id`, `article_id`, `run_id`, `published_at`, `rank`, `category`, `headline`, `summary`, `source_url`, `publisher`, `confidence_score`, `discord_message_id`.
  - In dry run: `discord_message_id` is NULL.
- `performance_metrics`: Schema with `id`, `timestamp`, `run_id`, `articles_fetched`, `articles_published`, `total_duration_seconds`.

## Code Layout
- `main.py`: Platform entry point and CLI runner.
- `config/settings.py`: Pydantic settings management.
- `config/sources.json`: News source definitions.
- `config/categories.json`: Categories and ranking weights.
- `core/database.py`: SQLite connection and query helper methods.
- `core/events.py`: EventBus publish/subscribe system.
- `core/models.py`: Pydantic models for Article, PublishedStory, PerformanceMetric, Incident.
- `core/logger.py`: Structured logger configuration.
- `agents/ops_manager.py`: OperationsManager coordinating pipeline execution.
- `agents/source_discovery.py`, `agents/rss_worker.py`: Stage 1 Discovery.
- `agents/dedup_agent.py`: Stage 2 Deduplication.
- `agents/fact_verifier.py`: Stage 3 Fact Verification.
- `agents/summarizer.py`: Stage 4 Summarization.
- `agents/ranker.py`: Stage 5 Ranking.
- `agents/qa_agent.py`: Stage 6 QA Gate.
- `storage/news.db`: SQLite database file.
- `storage/logs/`: Application logs (`agent.log`, `incidents.log`, `performance.log`).
