"""
Process watchdog for automatic decoder restart.

Monitors active decoder subprocesses and detects unexpected exits.
When a process dies unexpectedly, it logs the event and invokes an
optional restart callback with exponential backoff.
"""

import logging
import threading
import time
from typing import Callable, Optional
import subprocess

logger = logging.getLogger('intercept.watchdog')


class WatchedProcess:
    """Tracks a single watched subprocess."""

    def __init__(self, name: str, process: subprocess.Popen,
                 on_exit: Optional[Callable[[str, int], None]] = None,
                 max_restarts: int = 5,
                 backoff_base: float = 2.0,
                 backoff_cap: float = 60.0):
        self.name = name
        self.process = process
        self.on_exit = on_exit  # callback(name, returncode)
        self.max_restarts = max_restarts
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.restart_count = 0
        self.last_start_time = time.time()
        self.intentional_stop = False


class ProcessWatchdog:
    """Monitors subprocess health and triggers recovery on unexpected exit.

    Usage:
        watchdog = ProcessWatchdog(check_interval=5.0)
        watchdog.start()

        # When a mode starts a process:
        watchdog.watch('sensor', process, on_exit=my_restart_callback)

        # When a mode intentionally stops:
        watchdog.unwatch('sensor')

        # On app shutdown:
        watchdog.stop()
    """

    def __init__(self, check_interval: float = 5.0):
        self._check_interval = check_interval
        self._watched: dict[str, WatchedProcess] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the watchdog monitoring thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            name='process-watchdog',
            daemon=True
        )
        self._thread.start()
        logger.info("Process watchdog started (interval=%.1fs)", self._check_interval)

    def stop(self) -> None:
        """Stop the watchdog monitoring thread."""
        with self._lock:
            self._running = False
        if self._thread:
            self._thread.join(timeout=self._check_interval + 1)
        logger.info("Process watchdog stopped")

    def watch(self, name: str, process: subprocess.Popen,
              on_exit: Optional[Callable[[str, int], None]] = None,
              max_restarts: int = 5) -> None:
        """Register a process for monitoring."""
        with self._lock:
            self._watched[name] = WatchedProcess(
                name=name, process=process, on_exit=on_exit,
                max_restarts=max_restarts
            )
        logger.debug("Watching process '%s' (PID %d)", name, process.pid)

    def unwatch(self, name: str) -> None:
        """Remove a process from monitoring (intentional stop)."""
        with self._lock:
            wp = self._watched.pop(name, None)
            if wp:
                wp.intentional_stop = True
        logger.debug("Unwatched process '%s'", name)

    def get_status(self) -> dict:
        """Return status of all watched processes."""
        with self._lock:
            result = {}
            for name, wp in self._watched.items():
                alive = wp.process.poll() is None
                result[name] = {
                    'pid': wp.process.pid,
                    'alive': alive,
                    'restart_count': wp.restart_count,
                    'uptime': time.time() - wp.last_start_time if alive else 0,
                }
            return result

    def _monitor_loop(self) -> None:
        """Main monitoring loop -- runs in daemon thread."""
        while self._running:
            time.sleep(self._check_interval)
            if not self._running:
                break
            self._check_processes()

    def _check_processes(self) -> None:
        """Check all watched processes and handle unexpected exits."""
        with self._lock:
            to_notify = []
            for name, wp in list(self._watched.items()):
                if wp.intentional_stop:
                    continue
                returncode = wp.process.poll()
                if returncode is not None:
                    # Process died unexpectedly
                    logger.warning(
                        "Watched process '%s' (PID %d) exited unexpectedly "
                        "with code %d (restarts: %d/%d)",
                        name, wp.process.pid, returncode,
                        wp.restart_count, wp.max_restarts
                    )
                    if wp.on_exit and wp.restart_count < wp.max_restarts:
                        backoff = min(
                            wp.backoff_base ** wp.restart_count,
                            wp.backoff_cap
                        )
                        wp.restart_count += 1
                        to_notify.append((name, wp, returncode, backoff))
                    else:
                        if wp.restart_count >= wp.max_restarts:
                            logger.error(
                                "Process '%s' exceeded max restarts (%d). "
                                "Giving up. Manual intervention required.",
                                name, wp.max_restarts
                            )
                        del self._watched[name]

        # Call callbacks outside the lock to prevent deadlocks
        for name, wp, returncode, backoff in to_notify:
            logger.info(
                "Scheduling restart for '%s' in %.1fs (attempt %d/%d)",
                name, backoff, wp.restart_count, wp.max_restarts
            )
            # Run callback in a separate thread to avoid blocking the monitor
            threading.Thread(
                target=self._delayed_callback,
                args=(name, wp, returncode, backoff),
                daemon=True,
                name=f'watchdog-restart-{name}'
            ).start()

    def _delayed_callback(self, name: str, wp: WatchedProcess,
                          returncode: int, backoff: float) -> None:
        """Execute restart callback after backoff delay."""
        time.sleep(backoff)
        if not self._running:
            return
        try:
            wp.on_exit(name, returncode)
        except Exception:
            logger.exception("Restart callback failed for '%s'", name)


# Module-level singleton
_watchdog: Optional[ProcessWatchdog] = None
_watchdog_lock = threading.Lock()


def get_watchdog() -> ProcessWatchdog:
    """Get or create the global ProcessWatchdog singleton."""
    global _watchdog
    with _watchdog_lock:
        if _watchdog is None:
            _watchdog = ProcessWatchdog()
        return _watchdog
