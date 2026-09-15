"""
main.py — India News Intelligence Platform Entry Point.

Starts all agents and the web dashboard.
Handles graceful shutdown on SIGINT/SIGTERM.

Usage:
    python main.py             # Normal mode (publishes to dashboard)
    python main.py --dry-run   # Test mode (no actual publishing)
    python main.py --once      # Run pipeline once and exit
"""
from __future__ import annotations

import argparse
import asyncio
import errno
import logging
import os
import signal
import sys
from pathlib import Path

# ── Ensure project root is on path ────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# ── Load .env BEFORE importing settings ───────────────────────
from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=False)

# ── Core imports ──────────────────────────────────────────────
from core.logger   import setup_logging, AgentLogger
from core.database import init_db
from core.events   import get_bus, Topics, Event

setup_logging()
log = AgentLogger("main")


async def main(args: argparse.Namespace) -> None:
    from config.settings import get_settings
    settings = get_settings()

    # Override dry_run from CLI flag
    if args.dry_run:
        os.environ["DRY_RUN"] = "true"
        settings = get_settings.__wrapped__() if hasattr(get_settings, "__wrapped__") else settings

    log.info("=" * 60)
    log.info("  India News Intelligence Platform  —  Starting Up")
    log.info(f"  LLM Provider : {settings.llm_provider.upper()}")
    log.info(f"  Schedule     : every {settings.publish_interval_hours}h")
    log.info(f"  Dry Run      : {settings.dry_run or args.dry_run}")
    log.info("=" * 60)

    # ── Validate config ────────────────────────────────────────

    try:
        settings.validate_llm()
    except RuntimeError as e:
        log.critical(str(e))
        sys.exit(1)

    # ── Init database ──────────────────────────────────────────
    log.info("Initialising database...")
    db = await init_db()
    log.info(f"Database ready at: {settings.database_path}")

    # ── Init event bus ─────────────────────────────────────────
    bus = get_bus()
    await bus.start()
    log.info("Event bus started.")

    # ── Start agents ───────────────────────────────────────────
    from agents.website_knowledge import get_knowledge_agent
    from agents.failure_analyzer  import FailureAnalyzer
    from agents.health_monitor    import HealthMonitorAgent
    from agents.watchdog          import WatchdogAgent
    from agents.supervisor        import ExecutiveSupervisor
    from agents.ops_manager       import OperationsManager
    from agents.recovery_engine   import get_recovery_engine

    knowledge    = get_knowledge_agent()
    failure_an   = FailureAnalyzer(bus=bus)
    health_mon   = HealthMonitorAgent(bus=bus)
    watchdog     = WatchdogAgent(bus=bus)
    supervisor   = ExecutiveSupervisor(bus=bus)
    ops_manager  = OperationsManager(bus=bus)

    await knowledge.start()
    await failure_an.start()
    await health_mon.start()
    await watchdog.start()
    await supervisor.start()

    # Wire watchdog to browser worker (inside ops_manager's discovery agent)
    browser_worker = ops_manager.discovery_agent.browser_worker
    watchdog.register_browser_worker(browser_worker)

    # Register top-level agent restart callbacks with RecoveryEngine
    from agents.recovery_engine import get_recovery_engine
    recovery = get_recovery_engine()
    recovery.register_restart_callback("website_knowledge", knowledge.start)
    recovery.register_restart_callback("failure_analyzer", failure_an.start)
    recovery.register_restart_callback("health_monitor", health_mon.start)
    recovery.register_restart_callback("watchdog", watchdog.start)
    recovery.register_restart_callback("supervisor", supervisor.start)

    # ── Start operations manager (pipeline + scheduler) ───────
    await ops_manager.start(run_on_start=not args.once)

    # ── Optional: start web dashboard ─────────────────────────
    dashboard_task = None
    if settings.enable_dashboard and not args.once:
        dashboard_task = asyncio.create_task(
            _start_dashboard(
                port=settings.dashboard_port,
                ops_manager=ops_manager,
                host=getattr(settings, "dashboard_host", "127.0.0.1"),
            ),
            name="dashboard",
        )

    # ── If --once: run pipeline and exit ──────────────────────
    if args.once:
        log.info("--once mode: running pipeline once then exiting.")
        await ops_manager._run_pipeline()
        await _shutdown(
            bus, ops_manager, knowledge, failure_an,
            health_mon, watchdog, supervisor, db,
            dashboard_task=dashboard_task,
        )
        return

    # ── Wait for shutdown signal ───────────────────────────────
    stop_event = asyncio.Event()

    def _handle_signal(sig: signal.Signals) -> None:
        log.info(f"Received signal {sig.name} — initiating graceful shutdown.")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s))

    log.info("Platform running. Press Ctrl+C to stop.")
    await stop_event.wait()

    await _shutdown(
        bus, ops_manager, knowledge, failure_an,
        health_mon, watchdog, supervisor, db,
        dashboard_task=dashboard_task,
    )


async def _shutdown(
    bus, ops_manager, knowledge, failure_an,
    health_mon, watchdog, supervisor, db,
    dashboard_task=None,
) -> None:
    log.info("Shutting down all agents...")
    await ops_manager.stop()
    await supervisor.stop()
    await watchdog.stop()
    await health_mon.stop()
    await failure_an.stop()
    await knowledge.stop()
    if dashboard_task:
        dashboard_task.cancel()
    await bus.stop()
    await db.close()
    log.info("Platform shutdown complete.")


async def _start_dashboard(port: int, ops_manager=None, host: str = "127.0.0.1") -> None:
    """Serve the web dashboard as a static file server."""
    try:
        import aiohttp
        from aiohttp import web
        import aiohttp_cors

        log = AgentLogger("dashboard")
        dashboard_dir = Path(__file__).resolve().parent / "dashboard"
        if not dashboard_dir.exists():
            log.error(f"Dashboard directory not found at: {dashboard_dir}")
            return

        app = web.Application()

        # API routes
        from core.database import get_db
        _db = get_db()

        async def api_stats(request: web.Request) -> web.Response:
            import json as _json
            metrics = await _db.get_recent_metrics(limit=10)
            return web.json_response(metrics)

        async def api_stories(request: web.Request) -> web.Response:
            stories = await _db.get_published_today()
            return web.json_response(stories)

        async def api_incidents(request: web.Request) -> web.Response:
            incidents = await _db.get_open_incidents()
            return web.json_response(incidents)

        async def api_health(request: web.Request) -> web.Response:
            heartbeats = await _db.get_all_heartbeats()
            return web.json_response(heartbeats)

        async def api_command(request: web.Request) -> web.Response:
            try:
                data = await request.json()
                command = data.get("command", "").strip().lower()
                if not command:
                    return web.json_response({"status": "error", "message": "Command is empty."}, status=400)
                
                # Command routing
                if command in ["/news", "news", "trigger news", "find news"]:
                    if ops_manager:
                        await ops_manager.trigger_run()
                        return web.json_response({"status": "success", "message": "✅ News pipeline triggered! Stories will be published in a few minutes."})
                    else:
                        return web.json_response({"status": "error", "message": "Operations Manager not ready yet."}, status=503)
                
                # Mock handling for other categories
                elif any(command.startswith(c) for c in ["/breaking", "/politics", "/finance", "/economy", "/business", "/technology", "/ai", "/startups", "/sports", "/latest", "/today", "/summary"]):
                    category = command.lstrip("/").split()[0]
                    return web.json_response({"status": "success", "message": f"Command '{category}' received. You can see the latest published stories in the dashboard panels."})
                
                else:
                    return web.json_response({"status": "error", "message": f"Unknown command: '{command}'"}, status=400)
            except Exception as e:
                AgentLogger("dashboard").error(f"Error executing command: {e}")
                return web.json_response({"status": "error", "message": str(e)}, status=500)

        app.router.add_get("/api/stats",     api_stats)
        app.router.add_get("/api/stories",   api_stories)
        app.router.add_get("/api/incidents", api_incidents)
        app.router.add_get("/api/health",    api_health)
        app.router.add_post("/api/command",  api_command)

        # CORS for local dev
        try:
            cors = aiohttp_cors.setup(app, defaults={
                "*": aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_headers="*")
            })
            for route in list(app.router.routes()):
                cors.add(route)
        except Exception:
            pass

        # Static files
        async def index_handler(request: web.Request) -> web.Response:
            return web.FileResponse(dashboard_dir / "index.html")

        app.router.add_get("/", index_handler)
        app.router.add_static("/", dashboard_dir)

        runner = web.AppRunner(app)
        await runner.setup()
        bound_port = None
        max_retries = 10
        for offset in range(max_retries):
            candidate_port = port + offset
            try:
                site = web.TCPSite(runner, host, candidate_port)
                await site.start()
                bound_port = candidate_port
                break
            except OSError as e:
                if e.errno in (getattr(errno, "EADDRINUSE", 48), 48, 98):
                    log.warning(f"Dashboard port {candidate_port} occupied (errno {e.errno}), attempting fallback to {candidate_port + 1}...")
                    continue
                raise

        if bound_port is None:
            log.error(f"Failed to bind dashboard across ports {port}-{port + max_retries - 1}.")
            await runner.cleanup()
            return

        log.info(f"Web dashboard running at http://{host}:{bound_port}")
        try:
            await asyncio.sleep(float("inf"))  # keep running until cancelled
        except asyncio.CancelledError:
            pass
        finally:
            await runner.cleanup()

    except ImportError:
        pass
    except asyncio.CancelledError:
        pass
    except Exception as e:
        AgentLogger("dashboard").error(f"Dashboard error: {e}")


# ── Entry point ────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="India News Intelligence Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                  # Normal operation — publishes to dashboard
  python main.py --dry-run        # Test run — prints stories to console only
  python main.py --once           # Run pipeline once then exit (useful for cron)
        """,
    )
    p.add_argument("--dry-run", action="store_true", help="Run without publishing stories")
    p.add_argument("--once",    action="store_true", help="Run pipeline once and exit")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        pass
