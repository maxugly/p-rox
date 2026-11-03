"""High-performance proxy pool with staged qualification, circuit-breaker health, and persistence."""

import copy
import json
import logging
import os
import random
import re
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urlsplit

from .models import GeoLocation, LatencyMetrics, Proxy, ProxyProfile, SecurityMetrics

logger = logging.getLogger("proxy_collector.pool")

DEFAULT_PROXY_FILE = Path.home() / ".proxies"
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "p-rox"


def site_key(url: str) -> str:
    """Normalize a URL or domain into a consistent site key for per-site scoping."""
    if not url:
        return "default"
    if "://" not in url:
        url = f"http://{url}"
    try:
        host = urlsplit(url).hostname or url
    except Exception:
        host = url
    host = host.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


class ProxyPool:
    """Thread-safe proxy pool maintaining cold, L1-routed, L2-qualified, and L3-elite proxies.

    Features:
    - Tiered storage: premium authenticated proxies + high-volume free pool.
    - Escalating circuit breaker quarantine: transient (2m), rate-limit (10m), auth (1h), and dead (7d).
    - Multi-stage health persistence: records egress IP, latencies, throughputs, and failure counters.
    - Deterministic round-robin rotation avoiding timeouts on dead sockets.
    """

    PRUNE_AFTER: int = 5
    ROUTE_TTL: float = 43200.0   # 12 hours
    SITE_TTL: float = 86400.0    # 24 hours

    def __init__(
        self,
        path: Optional[str | Path] = DEFAULT_PROXY_FILE,
        cache_dir: Optional[str | Path] = None,
        refresh: bool = False,
    ):
        self.lock = threading.RLock()
        self.proxies: List[Dict[str, Any]] = []
        self._proxy_keys: Set[str] = set()

        self.cache_dir = Path(cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.health_path = self.cache_dir / "proxy_health.json"

        self._health_dirty = False
        self._persist_lock = threading.Lock()
        self.failed_session: Set[str] = set()
        self.started_at = time.time()
        self.idx = 0

        # Load persisted health snapshot
        self.health: Dict[str, Dict[str, Any]] = {}
        if self.health_path.is_file():
            try:
                self.health = json.loads(self.health_path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning("Could not read proxy health cache: %s", exc)
                self.health = {}

        # Initialize leases
        self.leases: Any = None
        self._init_leases()

        # Tier 1: Load premium proxies if file exists
        if path:
            self._load_premium_file(Path(path))

        # Tier 2: Load cached pool manifest if exists
        manifest_path = self.cache_dir / "pool_manifest.txt"
        if manifest_path.is_file():
            try:
                for line in manifest_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        self.ingest([line])
            except Exception as exc:
                logger.warning("Error reading pool manifest: %s", exc)

    def _init_leases(self):
        from .leases import ProxyLeases
        with self.lock:
            self.leases = ProxyLeases(self)

    @staticmethod
    def _key(scheme: str, host: str, port: str | int, user: str = "") -> str:
        """Canonical key for tracking a proxy endpoint in health storage."""
        auth = f"{user}@" if user else ""
        return f"{scheme.lower()}://{auth}{host.lower()}:{port}"

    @classmethod
    def _proxy_key(cls, p: Dict[str, Any]) -> str:
        """Canonical key from a proxy dict."""
        return cls._key(p.get("scheme", "http"), p.get("host", ""), p.get("port", ""), p.get("user", ""))

    def _load_premium_file(self, path: Path):
        """Load user-supplied authenticated proxies (tier: premium)."""
        if not path.is_file():
            return
        try:
            for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "://" in line:
                    parsed = urlsplit(line)
                    if parsed.scheme in {"http", "https", "socks4", "socks5", "socks5h", "socks4a"} and parsed.hostname and parsed.port:
                        self._add(
                            scheme=parsed.scheme,
                            host=parsed.hostname,
                            port=str(parsed.port),
                            user=unquote(parsed.username or ""),
                            pw=unquote(parsed.password or ""),
                            tier="premium",
                        )
                else:
                    parts = line.split(":")
                    if len(parts) == 4:
                        host, port, user, pw = parts
                        self._add("http", host, port, user, pw, "premium")
                    elif len(parts) == 2:
                        host, port = parts
                        self._add("http", host, port, "", "", "premium")
        except Exception as exc:
            logger.warning("Error loading premium proxy file %s: %s", path, exc)

    def _add(
        self,
        scheme: str,
        host: str,
        port: str | int,
        user: str = "",
        pw: str = "",
        tier: str = "free",
        country: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Add a proxy endpoint to the pool. Returns True if newly added."""
        scheme = scheme.lower()
        host = host.lower()
        port = str(port)
        key = self._key(scheme, host, port, user)

        with self.lock:
            if key in self._proxy_keys:
                return False

            h = self.health.get(key, {})
            now = time.time()

            # Check if permanently pruned/dead free proxy
            if h.get("dead") and tier == "free":
                dead_at = h.get("dead_at", 0)
                if now - dead_at < 7 * 86400:
                    h["benched_until"] = dead_at + 7 * 86400
                else:
                    # Expired dead entry after 7 days; allow re-testing
                    self.health.pop(key, None)
                    h = {}

            p = {
                "scheme": scheme,
                "host": host,
                "port": port,
                "user": user or "",
                "pass": pw or "",
                "tier": tier,
                "fails": 0,
                "consec": h.get("consec", 0),
                "benched_until": h.get("benched_until", 0.0),
                "label": f":{port}",
                "last_reason": "",
                "country": country or h.get("country", ""),
                "extra": extra or {},
            }
            self.proxies.append(p)
            self._proxy_keys.add(key)
            if self.leases is not None:
                self.leases._register(p)
            return True

    def ingest(
        self,
        entries: Any,
        default_scheme: str = "http",
        tier: str = "free",
    ) -> int:
        """Ingest lines, URLs, Proxy objects, or dicts into the pool."""
        added = 0
        with self.lock:
            for item in entries:
                if isinstance(item, Proxy):
                    if self._add(
                        scheme=item.scheme,
                        host=item.host,
                        port=item.port,
                        user=item.username or "",
                        pw=item.password or "",
                        tier=tier,
                        country=item.country or "",
                        extra=item.extra,
                    ):
                        added += 1
                elif isinstance(item, dict):
                    if self._add(
                        scheme=item.get("scheme", default_scheme),
                        host=item.get("host", ""),
                        port=item.get("port", ""),
                        user=item.get("user") or item.get("username") or "",
                        pw=item.get("pass") or item.get("password") or "",
                        tier=item.get("tier", tier),
                        country=item.get("country", ""),
                    ):
                        added += 1
                elif isinstance(item, str):
                    line = item.strip()
                    if not line or line.startswith("#"):
                        continue
                    m = re.match(r"^((?:https?|socks[45]))://([^:/]+):(\d+)$", line)
                    if m:
                        if self._add(m.group(1), m.group(2), m.group(3), tier=tier):
                            added += 1
                    else:
                        m2 = re.match(r"^([^:/\s]+):(\d+)$", line)
                        if m2:
                            if self._add(default_scheme, m2.group(1), m2.group(2), tier=tier):
                                added += 1
        return added

    def save_manifest(self, path: Optional[Path] = None):
        """Save active pool endpoints to a persistent manifest file."""
        target = path or (self.cache_dir / "pool_manifest.txt")
        temp = target.with_suffix(f".tmp.{os.getpid()}")
        try:
            with open(temp, "w", encoding="utf-8") as fh:
                for p in self.proxies:
                    auth = f"{p['user']}:{p['pass']}@" if p.get("user") else ""
                    fh.write(f"{p['scheme']}://{auth}{p['host']}:{p['port']}\n")
            temp.replace(target)
        except Exception as exc:
            logger.warning("Error saving pool manifest: %s", exc)

    def flush_health(self):
        """Atomically persist health metadata to disk without serializing unprobed deadweight."""
        with self._persist_lock:
            with self.lock:
                if not self._health_dirty:
                    return
                snapshot = copy.deepcopy(self.health)
                self._health_dirty = False

            temp_path = self.health_path.with_suffix(f".tmp.{os.getpid()}")
            try:
                temp_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
                temp_path.replace(self.health_path)
            except Exception as exc:
                logger.warning("Failed to flush health cache: %s", exc)

    def _mark_dirty(self):
        self._health_dirty = True

    # -------------------------------------------------------------------------
    # Qualification & Health Transition Handlers
    # -------------------------------------------------------------------------

    def ok(self, p: Dict[str, Any]):
        """Mark proxy as passed Layer 1 (route-qualified via IP echo)."""
        if not p:
            return
        with self.lock:
            p["fails"] = 0
            p["consec"] = 0
            p["last_reason"] = ""
            key = self._key(p["scheme"], p["host"], p["port"], p["user"])
            h = self.health.setdefault(key, {})
            h.update(consec=0, checked_at=time.time())
            self._mark_dirty()
        if self.leases:
            self.leases.notify(p)

    def site_ok(
        self,
        p: Dict[str, Any],
        site: str,
        transfer: bool = False,
        latency: Optional[float] = None,
        throughput: Optional[float] = None,
    ):
        """Mark proxy as qualified on a target site (L2 target-verified or L3 transfer-verified)."""
        if not p:
            return
        site_token = site_key(site)
        with self.lock:
            key = self._key(p["scheme"], p["host"], p["port"], p["user"])
            h = self.health.setdefault(key, {})
            target = h.setdefault("sites", {}).setdefault(site_token, {})
            now = time.time()

            if transfer:
                target["transfer_at"] = now
                h["mix_ok_at"] = now
            else:
                target["checked_at"] = now
                h["l2_at"] = now

            if latency is not None:
                target["latency"] = latency
            if throughput is not None:
                target["bytes_per_second"] = throughput

            p["consec"] = 0
            p["fails"] = 0
            self._mark_dirty()

        if self.leases:
            self.leases.notify(p)

    def fail(self, p: Dict[str, Any], reason: str = "conn"):
        """Record proxy connection or tunnel failure with escalating circuit breaker penalty."""
        if not p:
            return
        with self.lock:
            now = time.time()
            key = self._key(p["scheme"], p["host"], p["port"], p["user"])
            self.failed_session.add(key)
            p["fails"] += 1
            p["consec"] = p.get("consec", 0) + 1
            p["last_reason"] = reason

            # Escalating quarantine durations
            if reason in ("payment", "auth", "407"):
                # Account/auth rejected: bench 1 hour
                p["benched_until"] = now + 3600
            elif p["tier"] == "free" and p["consec"] >= self.PRUNE_AFTER:
                # 5 consecutive failures on public proxy: mark dead for 7 days
                p["benched_until"] = now + 7 * 86400
                self.health.setdefault(key, {}).update({
                    "consec": p["consec"],
                    "dead": True,
                    "dead_at": now,
                    "reason": reason,
                    "checked_at": 0,
                })
            elif reason in ("ratelimit", "429") or p["consec"] >= 3:
                # Rate limit or 3+ fails: bench 10 minutes
                p["benched_until"] = now + 600
            else:
                # Transient network error: bench 2 minutes
                p["benched_until"] = now + 120

            h = self.health.setdefault(key, {})
            h.update(benched_until=p["benched_until"], checked_at=0)
            for target in h.get("sites", {}).values():
                target.update(checked_at=0, transfer_at=0)

            self._mark_dirty()

        if self.leases:
            self.leases.notify(p)

    def site_fail(self, p: Dict[str, Any], site: str, reason: str, cooldown: float = 60.0):
        """Apply site-specific cooldown without benching the proxy globally."""
        if not p:
            return
        site_token = site_key(site)
        with self.lock:
            key = self._key(p["scheme"], p["host"], p["port"], p["user"])
            self.failed_session.add(key)
            target = self.health.setdefault(key, {}).setdefault("sites", {}).setdefault(site_token, {})
            target.update(
                blocked_until=time.time() + cooldown,
                last_failure=reason,
                checked_at=0,
                transfer_at=0,
            )
            self._mark_dirty()

        if self.leases:
            self.leases.notify(p)

    def apply_profile(self, profile: ProxyProfile):
        """Record a completed ProxyProfile into the health store and promote tiers."""
        p_dict = profile.proxy.as_pool_dict()
        key = self._key(profile.proxy.scheme, profile.proxy.host, profile.proxy.port, profile.proxy.username)
        with self.lock:
            h = self.health.setdefault(key, {})
            if profile.egress_ip:
                h["egress"] = profile.egress_ip
            if profile.geo:
                h["geo"] = profile.geo.to_dict()
            if profile.latency:
                h["latency"] = profile.latency.to_dict()
            if profile.security:
                h["security"] = profile.security.to_dict()

            h["checked_at"] = time.time()
            if profile.tier >= 1:
                h["consec"] = 0
                h["benched_until"] = 0.0

            self._mark_dirty()

        # Update in-memory entry if present
        for p in self.proxies:
            if self._key(p["scheme"], p["host"], p["port"], p.get("user")) == key:
                if profile.geo and profile.geo.country_code:
                    p["country"] = profile.geo.country_code
                if profile.tier >= 1:
                    p["fails"] = 0
                    p["consec"] = 0
                break

    def get_profile(self, p: Dict[str, Any]) -> ProxyProfile:
        """Construct a structured ProxyProfile from a pool proxy dictionary."""
        proxy_obj = Proxy(
            scheme=p.get("scheme", "http"),
            host=p.get("host", ""),
            port=int(p.get("port", 0)),
            username=p.get("user") or None,
            password=p.get("pass") or None,
            country=p.get("country") or None,
        )
        key = self._key(proxy_obj.scheme, proxy_obj.host, proxy_obj.port, proxy_obj.username)
        with self.lock:
            h = self.health.get(key, {})
            geo = GeoLocation.from_dict(h.get("geo")) if h.get("geo") else None
            latency = LatencyMetrics.from_dict(h.get("latency")) if h.get("latency") else None
            security = SecurityMetrics.from_dict(h.get("security")) if h.get("security") else None

            # Calculate effective tier
            r_tier, _ = self._rank(p)
            effective_tier = 0
            if r_tier == -1:
                effective_tier = 3
            elif r_tier == 0:
                effective_tier = 2
            elif r_tier == 1:
                effective_tier = 1

            return ProxyProfile(
                proxy=proxy_obj,
                egress_ip=h.get("egress"),
                geo=geo,
                latency=latency,
                security=security,
                tier=effective_tier,
                fails=p.get("fails", 0),
                consec=h.get("consec", 0),
                benched_until=h.get("benched_until", 0.0),
                last_tested=h.get("checked_at", 0.0),
                last_reason=h.get("reason", ""),
            )

    # -------------------------------------------------------------------------
    # Ranking, Rotation, and Selection
    # -------------------------------------------------------------------------

    def _rank(self, p: Dict[str, Any], site: Optional[str] = None) -> Tuple[int, int]:
        """Rank proxy:
        - Tier -1: L3 live stream verified within 24h
        - Tier 0:  L2 target site verified within 24h
        - Tier 1:  L1 route echo verified within 12h
        - Tier 2:  Untested candidate
        - Tier 3:  Suspects with consecutive failures
        """
        key = self._key(p["scheme"], p["host"], p["port"], p["user"])
        h = self.health.get(key, {})
        if h.get("consec", 0) > 0:
            return (3, h.get("consec", 0))

        now = time.time()
        if site:
            target = h.get("sites", {}).get(site_key(site), {})
            if target.get("transfer_at", 0) > now - self.SITE_TTL:
                return (-1, 0)
            if target.get("checked_at", 0) > now - self.SITE_TTL:
                return (0, 0)

        if h.get("mix_ok_at", 0) > now - self.SITE_TTL:
            return (-1, 0)
        if h.get("l2_at", 0) > now - self.SITE_TTL:
            return (0, 0)
        if h.get("checked_at", 0) > now - self.ROUTE_TTL:
            return (1, 0)

        return (2, 0)

    def _live(self, site: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return non-benched proxies (and non-blocked on target site if specified)."""
        now = time.time()
        site_token = site_key(site) if site else None

        candidates = []
        for p in self.proxies:
            if p["benched_until"] > now:
                continue
            if site_token:
                key = self._key(p["scheme"], p["host"], p["port"], p["user"])
                target = self.health.get(key, {}).get("sites", {}).get(site_token, {})
                if target.get("blocked_until", 0) > now:
                    continue
            candidates.append(p)
        return candidates

    def pick(self, site: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Round-robin pick within the best available qualified tier."""
        with self.lock:
            live = self._live(site)
            if not live:
                return None

            # Sort by rank
            live.sort(key=lambda p: self._rank(p, site))
            best_tier = self._rank(live[0], site)[0]
            best_pool = [p for p in live if self._rank(p, site)[0] == best_tier]

            self.idx = (self.idx + 1) % len(best_pool)
            return best_pool[self.idx]

    def validate(self, p: Optional[Dict[str, Any]], site: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Validate proxy is still live; jump immediately to next live candidate if benched."""
        with self.lock:
            now = time.time()
            if p is not None and p["benched_until"] <= now:
                if site:
                    key = self._key(p["scheme"], p["host"], p["port"], p["user"])
                    target = self.health.get(key, {}).get("sites", {}).get(site_key(site), {})
                    if target.get("blocked_until", 0) <= now:
                        return p
                else:
                    return p
            return self.pick(site)

    def summary(self, site: Optional[str] = None) -> Dict[str, Any]:
        """Aggregate real-time metrics across all pools."""
        now = time.time()
        site_token = site_key(site) if site else None
        res = {
            "total": len(self.proxies),
            "working": 0,
            "active": 0,
            "failed_session": len(self.failed_session),
            "pending_testing": 0,
            "quarantined": 0,
            "route_qualified": 0,
            "site_qualified": 0,
            "session_started_at": self.started_at,
        }

        with self.lock:
            seen = set()
            for p in self.proxies:
                key = self._key(p["scheme"], p["host"], p["port"], p["user"])
                if key in seen:
                    continue
                seen.add(key)

                h = self.health.get(key, {})
                routed = h.get("checked_at", 0) > now - self.ROUTE_TTL

                targets = h.get("sites", {})
                if site_token:
                    selected = [targets.get(site_token, {})]
                else:
                    selected = list(targets.values())

                qualified = any(
                    max(t.get("checked_at", 0), t.get("transfer_at", 0)) > now - self.SITE_TTL
                    and t.get("blocked_until", 0) <= now
                    for t in selected
                )

                if p["benched_until"] > now or (site_token and targets.get(site_token, {}).get("blocked_until", 0) > now):
                    res["quarantined"] += 1
                else:
                    res["route_qualified"] += int(routed)
                    res["site_qualified"] += int(qualified)
                    is_working = (routed and qualified) if site else routed
                    res["working"] += int(is_working)
                    res["pending_testing"] += int(not is_working)

        if self.leases is not None:
            with self.leases.condition:
                res["active"] = len(self.leases.busy)

        return res

    def __len__(self) -> int:
        with self.lock:
            return len(self.proxies)

    @staticmethod
    def requests_url(p: Dict[str, Any]) -> str:
        """Format proxy dict into requests/urllib3 compatible URL."""
        user = p.get("user", "")
        pw = p.get("pass", "")
        auth = f"{quote(user, safe='')}:{quote(pw, safe='')}@" if user or pw else ""
        scheme = p["scheme"]
        if scheme == "socks5":
            scheme = "socks5h"
        elif scheme == "socks4":
            scheme = "socks4a"
        return f"{scheme}://{auth}{p['host']}:{p['port']}"

    @staticmethod
    def playwright(p: Dict[str, Any]) -> Dict[str, str]:
        """Format proxy dict into Playwright server parameter."""
        d = {"server": f"{p['scheme']}://{p['host']}:{p['port']}"}
        if p.get("user"):
            d["username"] = p["user"]
        if p.get("pass"):
            d["password"] = p["pass"]
        return d
