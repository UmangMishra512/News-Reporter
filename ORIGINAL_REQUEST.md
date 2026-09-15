# Original User Request

## Initial Request — 2026-09-11T17:05:13+05:30

Perform a comprehensive system audit of the India News Intelligence Platform to ensure it is working properly. The audit should include running a full end-to-end dry-run pipeline test.

Requirements:
R1. Execute End-to-End Pipeline Dry Run
Trigger a dry-run execution of the pipeline (e.g., using `--dry-run` and `--once` flags on `main.py`) to verify that all components from discovery to ranking execute without unhandled exceptions. Ensure that the pipeline can run successfully even if the automatic scheduler is currently disabled.

R2. Audit Core System Health
Inspect the output logs and the SQLite database (`storage/news.db`) to ensure that articles successfully move through the pipeline (discovery -> dedup -> fact verification -> summarization -> ranking) and that metrics/incidents are being logged correctly.

Acceptance Criteria:
- Running `python3 main.py --dry-run --once` completes successfully (exit code 0).
- No critical unhandled Python exceptions occur during the dry run.
- The `storage/news.db` database contains new entries in the `published_stories` table resulting from the dry run.
- The performance metrics table reflects a completed run with fetched and published article counts.

## Follow-up — 2026-09-14T20:37:16Z

Perform a comprehensive system audit of the News agent platform to identify why the dashboard is failing to open and uncover any other underlying stability, configuration, or architectural issues. Please use a full team of agents for this task.

Working directory: /Users/umangmishra21/News agent
Integrity mode: demo

## Requirements

### R1. Root Cause Analysis for Dashboard
Investigate and document the exact reason the web dashboard fails to open, looking at port conflicts, startup logs, or task crashes.

### R2. Comprehensive System Audit
Review the system's architecture, recent runtime logs, and database state to identify any other stability, performance, or configuration issues.

### R3. Audit Report Only
Produce a detailed Markdown report (`audit_report.md`) of all findings. Do not make any modifications to the application's source code or configuration files.

## Acceptance Criteria

### Verification Rubric
- [ ] The report clearly identifies why the dashboard fails to start and provides the exact log snippet or code reference as evidence.
- [ ] The report includes at least one systemic or architectural observation beyond just the dashboard issue.
- [ ] No files in the codebase (apart from the report itself and temporary diagnostic scripts) have been modified.

## Follow-up — 2026-09-14T21:32:25Z

Implement the remediation steps identified in `audit_report.md` to fix the dashboard crash, resolve the event bus and recovery engine deadlocks, correct the database storage runaway, and fix the false-positive health monitor alarms. Please use a full team of agents for this task.

Working directory: /Users/umangmishra21/News agent
Integrity mode: benchmark

## Requirements

### R1. Dashboard Port Remediation (Priority 1)
Fix the port 8080 collision by implementing a dynamic port auto-fallback loop (e.g., trying up to 10 ports). Disable the dashboard on `--once` dry runs. Fix the relative path loading to use `__file__`.

### R2. Architectural & Recovery Fixes (Priority 1 & 4)
Wire the agent restart callbacks into the `RecoveryEngine` so stale agents can actually restart rather than causing infinite incident escalation. Fix the `EventBus` shutdown deadlock.

### R3. Analytics & Ingestion Fixes (Priority 2)
Fix the `PerformanceMetric` truncation bug where `articles` was overwritten in place, restoring accurate funnel tracking. Add `ssl=False` to the `health_monitor`'s `ClientSession`. Fix the RSS worker success accounting.

### R4. Database Cleanup & Optimization (Priority 3)
Execute the SQL migrations to add missing indexes (`idx_published_url`, `idx_perf_timestamp`, etc.). Set `PRAGMA busy_timeout = 5000` and `foreign_keys = ON`. Add TTL pruning for `health_checks`.

## Acceptance Criteria

### Automated Verification
- [ ] Running `python main.py --dry-run --once` executes completely without throwing `[Errno 48]` port collisions or database locking exceptions.

### Verification Rubric (Agent Review)
- [ ] Dashboard port fallback logic is successfully implemented.
- [ ] `register_restart_callback` is now actively used by the core agents.
- [ ] Database connection logic correctly executes the `busy_timeout` pragma.
- [ ] Pipeline metrics correctly record the exact funnel counts before truncation.
