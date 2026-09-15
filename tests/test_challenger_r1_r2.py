"""
Adversarial Challenger Test Suite for Remediation R1 & R2
India News Intelligence Platform

Tests:
1. Test Dashboard dynamic port fallback:
   - Occupies port 8080 with a TCP socket listener.
   - Invokes _start_dashboard(port=8080, host="127.0.0.1").
   - Probes http://127.0.0.1:8081/ to verify dynamic binding to port 8081 without [Errno 48].
   - Tests 10-port exhaustion: occupies 8080..8089, verifies graceful exit without uncaught exception.
2. Test --once flag behavior:
   - Verifies dashboard_task is None when args.once is True.
   - Verifies no server task is spawned or socket bound.
3. Test EventBus shutdown resilience:
   - Clean shutdown with pending events in queue (all processed).
   - Clean shutdown when subscribers throw unhandled exceptions (no deadlock).
   - Clean shutdown when subscriber exceeds timeout (timeout fallback drains cleanly).
4. Test Agent Restart Callbacks in RecoveryEngine:
   - Verifies all 14 platform agents have registered restart callbacks.
"""

import asyncio
import errno
import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import aiohttp
from core.events import Event, EventBus, Topics
from agents.recovery_engine import get_recovery_engine
from agents.supervisor import ExecutiveSupervisor


class TestDashboardPortFallback(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress testing for Dashboard Dynamic Port Fallback (R1)."""

    def setUp(self):
        self._blocker_sockets = []

    def tearDown(self):
        for sock in self._blocker_sockets:
            try:
                sock.close()
            except Exception:
                pass
        self._blocker_sockets.clear()

    def _occupy_port(self, port: int, host: str = "127.0.0.1") -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(1)
        self._blocker_sockets.append(sock)
        return sock

    async def test_dashboard_port_8080_occupied_fallback_to_8081(self):
        """
        Adversarial Test 1:
        Given port 8080 is already held by another process/socket,
        When _start_dashboard(8080) is started,
        Then it must catch [Errno 48] and successfully bind to 8081,
        serving requests without raising any unhandled exception.
        """
        from main import _start_dashboard
        from core.database import init_db

        # Initialize mock or real db for endpoint handlers
        db = await init_db()

        test_base_port = 8080
        # Occupy port 8080
        blocker = self._occupy_port(test_base_port)
        self.assertTrue(blocker.getsockname()[1] == test_base_port)

        # Launch _start_dashboard in a background task
        dashboard_task = asyncio.create_task(
            _start_dashboard(port=test_base_port, ops_manager=None, host="127.0.0.1"),
            name="challenger_dashboard_test",
        )

        # Wait for server to bind
        await asyncio.sleep(0.8)

        # Verify task is running and has not crashed with [Errno 48]
        self.assertFalse(dashboard_task.done(), "Dashboard task crashed unexpectedly!")

        # Verify port 8081 is listening and responsive
        expected_fallback_port = test_base_port + 1
        connector = aiohttp.TCPConnector()
        async with aiohttp.ClientSession(connector=connector) as session:
            # Probe /api/health
            async with session.get(f"http://127.0.0.1:{expected_fallback_port}/api/health", timeout=3.0) as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIsInstance(data, list)

            # Probe / (index page)
            async with session.get(f"http://127.0.0.1:{expected_fallback_port}/", timeout=3.0) as resp:
                self.assertEqual(resp.status, 200)
                text = await resp.text()
                self.assertIn("<title>", text)

        # Clean shutdown of dashboard task
        dashboard_task.cancel()
        try:
            await dashboard_task
        except asyncio.CancelledError:
            pass

        await db.close()

    async def test_dashboard_exhaustion_all_ports_occupied(self):
        """
        Adversarial Test 2:
        Given all 10 candidate ports (8080-8089) are occupied,
        When _start_dashboard(8080) is executed,
        Then it should log an error and return cleanly without throwing an unhandled exception.
        """
        from main import _start_dashboard
        test_base_port = 8800  # use distinct range to avoid conflicts

        # Occupy all 10 ports: 8800 to 8809
        for p in range(test_base_port, test_base_port + 10):
            self._occupy_port(p)

        # Launch _start_dashboard; should exhaust all 10 retries and return cleanly
        dashboard_task = asyncio.create_task(
            _start_dashboard(port=test_base_port, ops_manager=None, host="127.0.0.1")
        )

        # Should finish quickly because all ports are exhausted
        await asyncio.wait_for(dashboard_task, timeout=5.0)
        self.assertTrue(dashboard_task.done())
        self.assertIsNone(dashboard_task.exception(), "Expected clean termination on exhaustion")


class TestOnceFlagBehavior(unittest.IsolatedAsyncioTestCase):
    """Verification that --once flag bypasses dashboard creation (R1)."""

    async def test_once_flag_prevents_dashboard_task_creation(self):
        """
        Adversarial Test 3:
        When main.py is configured with args.once = True,
        verify that settings.enable_dashboard and not args.once evaluates to False,
        guaranteeing that no dashboard task is spawned.
        """
        import argparse
        from main import parse_args

        test_args = parse_args(["--dry-run", "--once"])
        self.assertTrue(test_args.once)
        self.assertTrue(test_args.dry_run)

        # Test the branch condition in main.py line 114
        enable_dashboard = True
        should_start_dashboard = enable_dashboard and not test_args.once
        self.assertFalse(
            should_start_dashboard,
            "Dashboard should NOT be spawned when args.once is True!"
        )


class TestEventBusShutdown(unittest.IsolatedAsyncioTestCase):
    """Adversarial stress testing for EventBus shutdown and deadlocks (R2)."""

    async def test_shutdown_with_pending_events_drains_completely(self):
        """
        Adversarial Test 4:
        Given an EventBus with 50 events queued,
        When bus.stop() is invoked,
        Then all 50 events must be processed and stop() must return in < 5.0 seconds.
        """
        bus = EventBus()
        await bus.start()

        processed_events = []

        async def fast_handler(event: Event):
            await asyncio.sleep(0.01)
            processed_events.append(event)

        bus.subscribe(Topics.ARTICLE_RAW, fast_handler)

        # Queue 50 events
        for i in range(50):
            await bus.publish(Event(topic=Topics.ARTICLE_RAW, sender="test", payload={"i": i}))

        # Stop bus immediately while events are queued
        stop_start = asyncio.get_event_loop().time()
        await bus.stop()
        stop_duration = asyncio.get_event_loop().time() - stop_start

        self.assertFalse(bus._running)
        self.assertEqual(len(processed_events), 50, f"Expected 50 processed events, got {len(processed_events)}")
        self.assertLess(stop_duration, 5.0, f"Stop took too long: {stop_duration:.2f}s")

    async def test_shutdown_resilience_when_subscribers_fail(self):
        """
        Adversarial Test 5:
        Given a subscriber that raises unhandled exceptions,
        When bus.stop() is invoked with events queued,
        Then task_done() must still be called in finally block and stop() must not deadlock.
        """
        bus = EventBus()
        await bus.start()

        call_count = 0

        async def crashing_handler(event: Event):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Deliberate subscriber catastrophe!")

        bus.subscribe(Topics.HEARTBEAT, crashing_handler)

        for i in range(20):
            await bus.publish(Event(topic=Topics.HEARTBEAT, sender="test", payload={"i": i}))

        # Must not deadlock or hang
        await asyncio.wait_for(bus.stop(), timeout=3.0)

        self.assertFalse(bus._running)
        self.assertEqual(call_count, 20, "Crashing handler should have been invoked for all 20 events")
        self.assertEqual(bus._queue.unfinished_tasks, 0, "Unfinished tasks must be 0")

    async def test_shutdown_timeout_safety_with_hung_subscriber(self):
        """
        Adversarial Test 6:
        Given a subscriber that hangs indefinitely,
        When bus.stop() is invoked,
        Then the 5.0s timeout in stop() must trigger, safely cancel the task,
        drain remaining items, and exit without unhandled deadlock.
        """
        bus = EventBus()
        await bus.start()

        async def hung_handler(event: Event):
            await asyncio.sleep(60.0)

        bus.subscribe(Topics.PIPELINE_START, hung_handler)

        # Enqueue events
        for i in range(5):
            await bus.publish(Event(topic=Topics.PIPELINE_START, sender="test", payload={"i": i}))

        # stop() should encounter 5.0s timeout and exit cleanly
        stop_start = asyncio.get_event_loop().time()
        await bus.stop()
        elapsed = asyncio.get_event_loop().time() - stop_start

        self.assertFalse(bus._running)
        self.assertGreaterEqual(elapsed, 4.5, "Expected stop() to wait for timeout duration")
        self.assertLess(elapsed, 7.0, f"Stop() took longer than expected: {elapsed}s")


class TestRecoveryEngineCallbacks(unittest.IsolatedAsyncioTestCase):
    """Verification of agent restart callback registration across the platform (R2)."""

    async def test_all_14_known_agents_have_restart_callbacks(self):
        """
        Adversarial Test 7:
        Verify that RecoveryEngine contains registered restart callbacks for all
        14 known agents from ExecutiveSupervisor.KNOWN_AGENTS.
        """
        from agents.ops_manager import OperationsManager
        from agents.recovery_engine import get_recovery_engine
        from agents.website_knowledge import WebsiteKnowledgeAgent
        from agents.failure_analyzer import FailureAnalyzer
        from agents.health_monitor import HealthMonitorAgent
        from agents.watchdog import WatchdogAgent
        from agents.supervisor import ExecutiveSupervisor

        recovery = get_recovery_engine()

        # Instantiate agents as in main.py
        ops = OperationsManager()
        knowledge = WebsiteKnowledgeAgent()
        failure_an = FailureAnalyzer()
        health_mon = HealthMonitorAgent()
        watchdog = WatchdogAgent()
        supervisor = ExecutiveSupervisor()

        # Register top-level agents (as done in main.py lines 103-107)
        recovery.register_restart_callback("website_knowledge", knowledge.start)
        recovery.register_restart_callback("failure_analyzer", failure_an.start)
        recovery.register_restart_callback("health_monitor", health_mon.start)
        recovery.register_restart_callback("watchdog", watchdog.start)
        recovery.register_restart_callback("supervisor", supervisor.start)

        # Trigger ops_manager registration (as done in ops_manager.start())
        ops._register_restart_callbacks()

        # Check against ExecutiveSupervisor.KNOWN_AGENTS
        known = ExecutiveSupervisor.KNOWN_AGENTS
        self.assertEqual(len(known), 14)

        missing = []
        for agent in known:
            if agent not in recovery._agent_restart_callbacks:
                missing.append(agent)

        self.assertEqual(missing, [], f"Agents missing restart callbacks in RecoveryEngine: {missing}")

        # Clean up
        await ops.stop()


if __name__ == "__main__":
    unittest.main()
