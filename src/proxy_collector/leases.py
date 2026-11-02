"""Mutual-exclusion proxy leases by resolved egress identity per target site."""

import contextlib
import threading
import time
from collections import defaultdict
from typing import Any, Dict, Optional, Set, Tuple

from .pool import site_key


class ProxyLeases:
    """Manages concurrent proxy leases ensuring no two workers access the same site from the same egress IP.

    Key mechanics:
    - Token = (site_key(site), egress_identity)
    - If multiple proxy endpoints share the same public outbound IP, only ONE can access that site at a time.
    - Leases support staged requirements: min_stage=0 (any), min_stage=1 (L1 route-qualified), min_stage=2 (L2 site-qualified).
    """

    def __init__(self, pool: Any):
        self.pool = pool
        self.condition = threading.Condition()
        self.busy: Set[Tuple[str, str]] = set()
        self.position: Dict[str, int] = defaultdict(int)
        self.routed: Dict[str, Dict[str, Any]] = {}
        self.ready: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)

        with self.pool.lock:
            for proxy in self.pool.proxies:
                self._register(proxy)

    def _register(self, proxy: Dict[str, Any]):
        """Index proxy into routed and per-site ready dictionaries."""
        key = self.pool._key(proxy["scheme"], proxy["host"], proxy["port"], proxy["user"])
        health = self.pool.health.get(key, {})
        now = time.time()

        if health.get("checked_at", 0) > now - self.pool.ROUTE_TTL and proxy["benched_until"] <= now:
            self.routed[key] = proxy
        else:
            self.routed.pop(key, None)

        for site, target in health.get("sites", {}).items():
            if (
                max(target.get("checked_at", 0), target.get("transfer_at", 0)) > now - self.pool.SITE_TTL
                and target.get("blocked_until", 0) <= now
                and proxy["benched_until"] <= now
            ):
                self.ready[site][key] = proxy
            else:
                self.ready[site].pop(key, None)

    def notify(self, proxy: Dict[str, Any]):
        """Notify lease manager of updated proxy status."""
        with self.condition:
            self._register(proxy)
            self.condition.notify_all()

    def _identity(self, proxy: Dict[str, Any]) -> str:
        """Resolve public egress IP or fallback to proxy host."""
        key = self.pool._key(proxy["scheme"], proxy["host"], proxy["port"], proxy["user"])
        health = self.pool.health.get(key, {})
        return health.get("egress") or proxy["host"]

    @contextlib.contextmanager
    def lease(
        self,
        site: str,
        *,
        min_stage: int = 0,
        timeout: float = 10.0,
        stop: Optional[threading.Event] = None,
        specific: Optional[Dict[str, Any]] = None,
    ):
        """Acquire an exclusive proxy lease for a given target site."""
        site_token = site_key(site)
        deadline = time.time() + timeout

        while not (stop and stop.is_set()):
            with self.condition:
                now = time.time()

                if specific is not None:
                    candidates = [specific]
                elif min_stage >= 2:
                    site_ready = self.ready.get(site_token, {})
                    candidates = [site_ready[k] for k in sorted(site_ready.keys()) if k in site_ready]
                elif min_stage == 1:
                    candidates = [self.routed[k] for k in sorted(self.routed.keys()) if k in self.routed]
                else:
                    candidates = self.pool.proxies

                if candidates:
                    start_idx = self.position[site_token] % len(candidates)
                    for offset in range(len(candidates)):
                        proxy = candidates[(start_idx + offset) % len(candidates)]
                        key = self.pool._key(proxy["scheme"], proxy["host"], proxy["port"], proxy["user"])

                        # Verify eligibility
                        if proxy["benched_until"] > now:
                            continue

                        health = self.pool.health.get(key, {})
                        if min_stage >= 1 and health.get("checked_at", 0) <= now - self.pool.ROUTE_TTL:
                            continue

                        target = health.get("sites", {}).get(site_token, {})
                        if target.get("blocked_until", 0) > now:
                            continue
                        if min_stage >= 2 and max(target.get("checked_at", 0), target.get("transfer_at", 0)) <= now - self.pool.SITE_TTL:
                            continue

                        # Check identity reservation token
                        identity = self._identity(proxy)
                        token = (site_token, identity)
                        if token not in self.busy:
                            self.busy.add(token)
                            self.position[site_token] = (start_idx + offset + 1) % len(candidates)
                            try:
                                yield proxy
                                return
                            finally:
                                with self.condition:
                                    self.busy.discard(token)
                                    self.condition.notify_all()

                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self.condition.wait(timeout=min(0.2, remaining))

        raise TimeoutError(f"No available proxy lease for site '{site_token}' within {timeout}s (min_stage={min_stage})")
