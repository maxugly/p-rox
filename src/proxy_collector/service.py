"""Proxy control service providing CLI and API coordination, status snapshots, and daemon mode."""

import fcntl
import json
import logging
import os
import signal
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from .collector import ProxyCollector
from .monitor import ProxyMonitor
from .pool import DEFAULT_CACHE_DIR, DEFAULT_PROXY_FILE, ProxyPool, site_key

logger = logging.getLogger("proxy_collector.service")


class ProxyService:
    """Coordinates proxy ingestion, multi-stage qualification, IPC file locking, and background monitoring."""

    def __init__(
        self,
        cache: Optional[str | Path] = None,
        proxy_file: Optional[str | Path] = DEFAULT_PROXY_FILE,
        site: Optional[str] = None,
        route_workers: int = 8,
        site_workers: int = 8,
        echo_url: str = "https://api.ipify.org/",
    ):
        self.cache = Path(cache or DEFAULT_CACHE_DIR).expanduser().resolve()
        self.cache.mkdir(parents=True, exist_ok=True)
        self.proxy_file = proxy_file
        self.site = site
        self.route_workers = route_workers
        self.site_workers = site_workers
        self.echo_url = echo_url

        self.snapshot = self.cache / "proxy_status.json"
        self.lock_file = self.cache / "proxy_service.lock"
        self.stop = threading.Event()
        self.pool: Optional[ProxyPool] = None

    def _load(self) -> ProxyPool:
        self.pool = ProxyPool(path=self.proxy_file, cache_dir=self.cache, refresh=False)
        return self.pool

    @contextmanager
    def writer(self):
        """Exclusive file lock ensuring only one coordinator process mutates state at a time."""
        with open(self.lock_file, "a") as lock_fh:
            try:
                fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Proxy service is busy; stop any running daemon before manual update/check.")
            yield

    def _monitor(self, refresh: bool = False) -> ProxyMonitor:
        sites = {}
        validators = {}
        if self.site:
            parsed = urlsplit(self.site if "://" in self.site else f"http://{self.site}")
            target = self.site
            if not parsed.path or parsed.path == "/":
                target = f"{self.site.rstrip('/')}/robots.txt"
            sites[self.site] = target
            if target.endswith("/robots.txt"):
                validators[self.site] = lambda r: "user-agent" in r.text.lower()

        return ProxyMonitor(
            self.pool,
            sites=sites,
            route_workers=self.route_workers,
            site_workers=self.site_workers,
            echo_url=self.echo_url,
            refresh=refresh,
            stop=self.stop,
            validators=validators,
        )

    def _publish(self, phase: str, **extra) -> Dict[str, Any]:
        """Atomically publish real-time status snapshot to disk."""
        summary = self.pool.summary(self.site) if self.pool else {}
        state = {
            "pid": os.getpid(),
            "updated_at": time.time(),
            "phase": phase,
            "site": self.site,
            **summary,
            **extra,
        }
        temp_path = self.snapshot.with_suffix(f".tmp.{os.getpid()}")
        try:
            temp_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
            temp_path.replace(self.snapshot)
        except Exception as exc:
            logger.warning("Failed to publish status snapshot: %s", exc)
        return state

    def update(self, limit: int = 10000) -> Dict[str, Any]:
        """Ingest fresh, deduplicated web proxies from collector into the pool."""
        with self.writer():
            self._load()
            collector = ProxyCollector(cache_dir=self.cache / "sources")
            added = collector.ingest_into_pool(
                self.pool,
                limit=limit,
                protocols={"http", "https", "socks4", "socks5"},
            )
            self.pool.save_manifest()
            self.pool.flush_health()
            return self._publish("updated", added=added)

    def status(self) -> Dict[str, Any]:
        """Query real-time pool metrics and daemon process state."""
        pool = ProxyPool(path=self.proxy_file, cache_dir=self.cache, refresh=False)
        state = pool.summary(self.site)
        previous = {}
        if self.snapshot.is_file():
            try:
                previous = json.loads(self.snapshot.read_text(encoding="utf-8"))
            except Exception:
                pass

        running = False
        if self.lock_file.exists():
            with open(self.lock_file, "a") as lock_fh:
                try:
                    fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    running = False
                except BlockingIOError:
                    running = True

        if running:
            return {**previous, "service_running": True}

        for key in ["failed_session", "session_started_at"]:
            if key in previous:
                state[key] = previous[key]

        return {**state, "service_running": False, "last_phase": previous.get("phase")}

    def check(self, limit: int = 200) -> Dict[str, Any]:
        """Execute synchronous batch qualification across pending proxies."""
        with self.writer():
            self._load()
            monitor = self._monitor()
            checked = monitor.check_pending(limit)
            return self._publish(
                "checked",
                selected=monitor.selected,
                checked=checked,
                deferred=max(0, monitor.selected - checked),
            )

    def daemon(self, interval: float = 2.0):
        """Run foreground qualification monitor loop with signal handling."""
        with self.writer():
            self._load()
            monitor = self._monitor(refresh=True).start()
            old_handlers = {}

            if threading.current_thread() is threading.main_thread():
                for sig in [signal.SIGINT, signal.SIGTERM]:
                    old_handlers[sig] = signal.signal(sig, lambda *_: self.stop.set())

            try:
                while not self.stop.is_set():
                    self.pool.flush_health()
                    status_data = self._publish("running", checks=monitor.stats)
                    print(json.dumps(status_data), flush=True)
                    self.stop.wait(interval)
            finally:
                self.stop.set()
                monitor.close()
                self._publish("stopped", checks=monitor.stats)
                for sig, handler in old_handlers.items():
                    signal.signal(sig, handler)
