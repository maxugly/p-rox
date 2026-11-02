"""Independent multi-stage proxy acquisition, route-testing, and site-qualification workers."""

import ipaddress
import logging
import queue
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
from urllib3.util import Timeout

from .pool import ProxyPool, site_key

logger = logging.getLogger("proxy_collector.monitor")

FALLBACK_ECHO_URLS = [
    "https://api.ipify.org/",
    "https://icanhazip.com/",
    "https://ifconfig.me/ip",
]


class ProxyMonitor:
    """Manages independent background worker pools for Layer 1 route-echo and Layer 2 site-qualification."""

    def __init__(
        self,
        pool: ProxyPool,
        sites: Optional[Dict[str, str]] = None,
        *,
        route_workers: int = 8,
        site_workers: int = 8,
        echo_url: str = "https://api.ipify.org/",
        refresh: bool = False,
        stop: Optional[threading.Event] = None,
        validators: Optional[Dict[str, Callable[[requests.Response], bool]]] = None,
    ):
        self.pool = pool
        self.sites = sites or {}
        self.echo_url = echo_url
        self.refresh = refresh
        self.validators = validators or {}
        self.stop = stop or threading.Event()
        self.route_workers = max(1, route_workers)
        self.site_workers = max(1, site_workers)

        self.route_jobs: queue.Queue = queue.Queue(maxsize=max(1, self.route_workers * 2))
        self.site_jobs: queue.Queue = queue.Queue(maxsize=max(1, self.site_workers * 2))
        self.pending: Set[Any] = set()
        self.lock = threading.Lock()

        self.stats = {"route_ok": 0, "site_ok": 0, "failed": 0}
        self.attempted: Set[str] = set()
        self.selected: int = 0
        self.route_cursor: int = 0
        self.threads: List[threading.Thread] = []

    def start(self) -> "ProxyMonitor":
        """Start background feed, route, and site qualification threads."""
        targets = (
            [self._feed]
            + [self._route] * self.route_workers
            + [self._site] * self.site_workers
        )
        for target in targets:
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self.threads.append(t)
        return self

    def close(self):
        """Stop all background worker threads and flush health."""
        self.stop.set()
        for t in self.threads:
            t.join(timeout=5)
        self.pool.flush_health()

    def _enqueue(self, q: queue.Queue, key: Any, item: Any) -> bool:
        with self.lock:
            if key in self.pending:
                return True
            try:
                q.put_nowait((key, item))
            except queue.Full:
                return False
            self.pending.add(key)
            return True

    def _feed(self):
        """Background feed loop continuously matching candidates to route or site check queues."""
        while not self.stop.is_set():
            now = time.time()

            # 1. Feed already routed proxies to site qualification
            if self.sites and self.pool.leases:
                with self.pool.leases.condition:
                    routed = list(self.pool.leases.routed.values())

                for p in routed:
                    key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
                    with self.pool.lock:
                        h = self.pool.health.get(key, {})
                        if p["benched_until"] > now or h.get("checked_at", 0) <= now - self.pool.ROUTE_TTL:
                            continue
                        targets = dict(h.get("sites", {}))

                    for site in self.sites:
                        s_token = site_key(site)
                        target = targets.get(s_token, {})
                        if (
                            max(target.get("checked_at", 0), target.get("transfer_at", 0)) <= now - self.pool.SITE_TTL
                            and target.get("blocked_until", 0) <= now
                        ):
                            if not self._enqueue(self.site_jobs, (site, key), (p, site)):
                                break
                    if self.site_jobs.full():
                        break

            # 2. Feed cold proxies to route check
            with self.pool.lock:
                size = len(self.pool.proxies)

            if size > 0:
                scan_batch = min(size, max(64, self.route_workers * 8))
                for _ in range(scan_batch):
                    if self.stop.is_set() or self.route_jobs.full():
                        break
                    with self.pool.lock:
                        p = self.pool.proxies[self.route_cursor % size]
                        self.route_cursor = (self.route_cursor + 1) % size
                        key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
                        h = self.pool.health.get(key, {})
                        needed = p["benched_until"] <= now and h.get("checked_at", 0) <= now - self.pool.ROUTE_TTL

                    if needed:
                        self._enqueue(self.route_jobs, ("route", key), p)

            self.stop.wait(0.2)

    def _jobs(self, q: queue.Queue, callback: Callable[[Any], None]):
        """Worker loop reading jobs from queue."""
        while not self.stop.is_set():
            try:
                key, item = q.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                callback(item)
            finally:
                with self.lock:
                    self.pending.discard(key)
                q.task_done()

    def _test_route(self, p: Dict[str, Any]):
        """Layer 1: Probe candidate proxy against IP echo endpoint within strict 8s budget."""
        try:
            with self.pool.leases.lease(self.echo_url, specific=p, timeout=0.2, stop=self.stop):
                route = ProxyPool.requests_url(p)
                start = time.monotonic()
                key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
                with self.lock:
                    self.attempted.add(key)

                egress = None
                with requests.Session() as session:
                    session.trust_env = False
                    timeout = Timeout(total=8.0, connect=3.0, read=5.0)
                    with session.get(self.echo_url, proxies={"http": route, "https": route}, timeout=timeout) as r:
                        if r.status_code == 429:
                            self.pool.fail(p, "ratelimit")
                            return
                        r.raise_for_status()
                        egress = str(ipaddress.ip_address(r.text.strip()))

                latency = time.monotonic() - start
                self.pool.ok(p)
                with self.pool.lock:
                    self.pool.health[key].update(egress=egress, route_latency=latency)
                self.pool.leases.notify(p)
                with self.lock:
                    self.stats["route_ok"] += 1

        except (TimeoutError, RuntimeError):
            pass
        except Exception:
            self.pool.fail(p, "conn")
            with self.lock:
                self.stats["failed"] += 1

    def _route(self):
        self._jobs(self.route_jobs, self._test_route)

    def _test_site(self, job: Tuple[Dict[str, Any], str]):
        """Layer 2: Probe proxy against target site lightweight endpoint."""
        p, site = job
        try:
            with self.pool.leases.lease(site, specific=p, min_stage=1, timeout=0.2, stop=self.stop):
                route = ProxyPool.requests_url(p)
                start = time.monotonic()
                key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
                with self.lock:
                    self.attempted.add(key)

                target_url = self.sites.get(site, site)
                with requests.Session() as session:
                    session.trust_env = False
                    timeout = Timeout(total=8.0, connect=3.0, read=5.0)
                    with session.get(target_url, proxies={"http": route, "https": route}, timeout=timeout) as r:
                        validator = self.validators.get(site)
                        if r.status_code in {429, 403} or (r.ok and validator and not validator(r)):
                            self.pool.site_fail(p, site, f"filter-or-HTTP-{r.status_code}")
                            with self.lock:
                                self.stats["failed"] += 1
                            return
                        r.raise_for_status()

                latency = time.monotonic() - start
                self.pool.site_ok(p, site, latency=latency)
                with self.lock:
                    self.stats["site_ok"] += 1

        except (TimeoutError, RuntimeError):
            pass
        except Exception:
            self.pool.fail(p, "conn")
            with self.lock:
                self.stats["failed"] += 1

    def _site(self):
        self._jobs(self.site_jobs, self._test_site)

    def check_pending(self, limit: int = 200) -> int:
        """Run batch qualification across pending candidates with identity token management."""
        now = time.time()
        candidates = []

        with self.pool.lock:
            for p in self.pool.proxies:
                key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
                h = self.pool.health.get(key, {})
                if p["benched_until"] > now:
                    continue

                routed = h.get("checked_at", 0) > now - self.pool.ROUTE_TTL
                needed = not routed or any(
                    max(
                        h.get("sites", {}).get(site_key(s), {}).get("checked_at", 0),
                        h.get("sites", {}).get(site_key(s), {}).get("transfer_at", 0),
                    ) < now - self.pool.SITE_TTL
                    for s in self.sites
                )
                if needed:
                    candidates.append(p)
                if limit and len(candidates) >= limit:
                    break

        self.selected = len(candidates)

        def route_fn(p):
            key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
            if self.pool.health.get(key, {}).get("checked_at", 0) <= time.time() - self.pool.ROUTE_TTL:
                self._test_route(p)
            return p

        pending_routes = deque(candidates)
        pending_sites: deque = deque()
        active: Set[Tuple[str, str]] = set()
        running: Dict[Any, Tuple[str, Tuple[str, str], Any]] = {}

        with ThreadPoolExecutor(max_workers=self.route_workers) as routes_exec, ThreadPoolExecutor(max_workers=self.site_workers) as targets_exec:
            def dispatch(pending, executor, stage, capacity):
                available = capacity - sum(v[0] == stage for v in running.values())
                for _ in range(len(pending)):
                    if available <= 0 or self.stop.is_set():
                        break
                    job = pending.popleft()
                    p = job if stage == "route" else job[0]
                    dest = self.echo_url if stage == "route" else job[1]
                    identity = self.pool.leases._identity(p)
                    token = (site_key(dest), identity)

                    if token in active:
                        pending.append(job)
                        continue

                    active.add(token)
                    cb = route_fn if stage == "route" else self._test_site
                    running[executor.submit(cb, job)] = (stage, token, job)
                    available -= 1

            while pending_routes or pending_sites or running:
                if self.stop.is_set():
                    pending_routes.clear()
                    pending_sites.clear()

                dispatch(pending_routes, routes_exec, "route", self.route_workers)
                dispatch(pending_sites, targets_exec, "site", self.site_workers)

                if not running:
                    break

                completed, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in completed:
                    stage, token, job = running.pop(future)
                    active.discard(token)
                    try:
                        future.result()
                    except Exception:
                        pass

                    if stage == "route" and not self.stop.is_set():
                        p = job
                        key = self.pool._key(p["scheme"], p["host"], p["port"], p["user"])
                        now_check = time.time()
                        if (
                            self.pool.health.get(key, {}).get("checked_at", 0) > now_check - self.pool.ROUTE_TTL
                            and p["benched_until"] <= now_check
                        ):
                            pending_sites.extend((p, site) for site in self.sites)

        self.pool.flush_health()
        return len(self.attempted)
