"""SQLite database layer for p-rox proxy storage, status tracking, and rotation."""

import collections
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_DB_PATH = Path.home() / ".p-rox" / "proxies.db"


class ProxyDB:
    """Manages SQLite storage for public proxy candidates, testing state, and metrics."""

    def __init__(self, db_path: Optional[str | Path] = None):
        self.db_path = Path(db_path or DEFAULT_DB_PATH).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        """Return a SQLite connection configured for concurrent WAL access."""
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def init_db(self):
        """Create tables and indexes if they do not exist."""
        with self.get_connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS proxies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheme TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    username TEXT,
                    password TEXT,
                    url TEXT UNIQUE,
                    status TEXT DEFAULT 'untested',
                    latency_ms REAL,
                    ttfb_ms REAL,
                    egress_ip TEXT,
                    country TEXT,
                    city TEXT,
                    asn TEXT,
                    anonymity TEXT,
                    fails INTEGER DEFAULT 0,
                    first_seen_alive REAL,
                    last_checked_at REAL,
                    created_at REAL DEFAULT (strftime('%s', 'now'))
                );

                CREATE INDEX IF NOT EXISTS idx_status ON proxies(status);
                CREATE INDEX IF NOT EXISTS idx_latency ON proxies(latency_ms);
                CREATE INDEX IF NOT EXISTS idx_last_checked ON proxies(last_checked_at);
                CREATE INDEX IF NOT EXISTS idx_country ON proxies(country);
                CREATE INDEX IF NOT EXISTS idx_country_null ON proxies(id) WHERE country IS NULL;
                """
            )
            # Migration: add first_seen_alive column if upgrading existing database
            try:
                conn.execute("ALTER TABLE proxies ADD COLUMN first_seen_alive REAL;")
            except sqlite3.OperationalError:
                pass

    def upsert_batch(
        self,
        proxies: Iterable[Any],
        batch_size: int = 10000,
        resolve_geoip: bool = True,
        geoip_resolver: Optional[Any] = None,
    ) -> int:
        """Bulk insert proxy candidates. Skips existing URLs.

        Calculates country, city, and ASN immediately on insertion if resolve_geoip=True.
        Returns total newly inserted rows.
        """
        resolver = None
        if resolve_geoip:
            if geoip_resolver is not None:
                resolver = geoip_resolver if getattr(geoip_resolver, "is_available", False) else None
            else:
                try:
                    from .geoip import GeoIPResolver

                    r = GeoIPResolver()
                    if r.is_available:
                        resolver = r
                except Exception:
                    resolver = None

        inserted_total = 0
        batch = []
        host_cache: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}

        with self.get_connection() as conn:
            for p in proxies:
                scheme = getattr(p, "scheme", None) or (p.get("scheme", "http") if isinstance(p, dict) else "http")
                host = getattr(p, "host", None) or (p.get("host", "") if isinstance(p, dict) else "")
                port = int(getattr(p, "port", None) or (p.get("port", 0) if isinstance(p, dict) else 0))
                user = getattr(p, "username", None) or (p.get("user") if isinstance(p, dict) else None)
                pw = getattr(p, "password", None) or (p.get("pass") if isinstance(p, dict) else None)
                url = getattr(p, "url", None) or (p.get("url") if isinstance(p, dict) else None)
                if not url:
                    auth = f"{user}:{pw}@" if user else ""
                    url = f"{scheme}://{auth}{host}:{port}"

                country = getattr(p, "country", None) or (p.get("country") if isinstance(p, dict) else None)
                city = getattr(p, "city", None) or (p.get("city") if isinstance(p, dict) else None)
                asn = getattr(p, "asn", None) or (p.get("asn") if isinstance(p, dict) else None)

                if resolver and not country and host:
                    cached = host_cache.get(host)
                    if cached is not None:
                        country, city, asn = cached
                    else:
                        loc = resolver.lookup(host)
                        if loc.country_code:
                            country = f"{loc.country_code} ({loc.city})" if loc.city else loc.country_code
                        else:
                            country = "??"
                        city = loc.city or None
                        asn = loc.org or None
                        if len(host_cache) < 50000:
                            host_cache[host] = (country, city, asn)

                batch.append((scheme, host, port, user, pw, url, country, city, asn))
                if len(batch) >= batch_size:
                    cursor = conn.executemany(
                        """
                        INSERT OR IGNORE INTO proxies (scheme, host, port, username, password, url, country, city, asn)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    inserted_total += cursor.rowcount if cursor.rowcount > 0 else 0
                    batch.clear()

            if batch:
                cursor = conn.executemany(
                    """
                    INSERT OR IGNORE INTO proxies (scheme, host, port, username, password, url, country, city, asn)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                inserted_total += cursor.rowcount if cursor.rowcount > 0 else 0

        return inserted_total

    def get_unresolved_geoip_batch(self, limit: int = 5000) -> List[Dict[str, Any]]:
        """Fetch candidate proxies that have not yet had their GeoIP resolved."""
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, host
                FROM proxies
                WHERE country IS NULL OR country = ''
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_geoip_batch(self, updates: List[Tuple[Optional[str], Optional[str], Optional[str], int]]):
        """Bulk update resolved country, city, and asn metadata for proxy candidates."""
        with self.get_connection() as conn:
            conn.executemany(
                """
                UPDATE proxies
                SET country = ?, city = ?, asn = ?
                WHERE id = ?
                """,
                updates,
            )

    def get_unresolved_geoip_count(self) -> int:
        """Return count of proxies currently missing country geolocation."""
        with self.get_connection() as conn:
            row = conn.execute("SELECT COUNT(*) FROM proxies WHERE country IS NULL OR country = ''").fetchone()
            return row[0] if row else 0

    def get_candidates_for_testing(
        self,
        limit: int = 100,
        keepalive_seconds: float = 180.0,
        include_all_alive: bool = False,
        country: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch candidates prioritized for health checks:
        1. Known ALIVE proxies (last live proxies first if include_all_alive=True, else due for heartbeat).
        2. Fresh UNTESTED candidates (newest scraped IPs first).
        3. DEAD proxies due for retry (2-hour exponential backoff).

        Optionally filters candidates by country prefix (e.g. 'US', 'DE').
        """
        now = time.time()
        stale_threshold = now - keepalive_seconds
        dead_threshold = now - 7200.0  # 2 hours backoff for dead nodes
        results = []

        country_filter = ""
        c_param: Tuple[Any, ...] = ()
        if country:
            country_filter = " AND country LIKE ?"
            c_param = (f"{country.upper().strip()}%",)

        with self.get_connection() as conn:
            # 1. Alive proxies:
            if include_all_alive:
                alive_due = conn.execute(
                    f"""
                    SELECT id, scheme, host, port, username, password, url, status, fails, last_checked_at
                    FROM proxies
                    WHERE status = 'alive'{country_filter}
                    ORDER BY last_checked_at DESC
                    LIMIT ?
                    """,
                    c_param + (limit,),
                ).fetchall()
            else:
                alive_due = conn.execute(
                    f"""
                    SELECT id, scheme, host, port, username, password, url, status, fails, last_checked_at
                    FROM proxies
                    WHERE status = 'alive' AND (last_checked_at IS NULL OR last_checked_at < ?){country_filter}
                    ORDER BY last_checked_at ASC
                    LIMIT ?
                    """,
                    (stale_threshold,) + c_param + (limit,),
                ).fetchall()
            results.extend([dict(r) for r in alive_due])

            # 2. Fresh untested candidates
            if len(results) < limit:
                remaining = limit - len(results)
                untested = conn.execute(
                    f"""
                    SELECT id, scheme, host, port, username, password, url, status, fails, last_checked_at
                    FROM proxies
                    WHERE status = 'untested'{country_filter}
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    c_param + (remaining,),
                ).fetchall()
                results.extend([dict(r) for r in untested])

            # 3. Dead candidates due for retry
            if len(results) < limit:
                remaining = limit - len(results)
                dead = conn.execute(
                    f"""
                    SELECT id, scheme, host, port, username, password, url, status, fails, last_checked_at
                    FROM proxies
                    WHERE status = 'dead' AND fails < 5 AND (last_checked_at IS NULL OR last_checked_at < ?){country_filter}
                    ORDER BY last_checked_at ASC
                    LIMIT ?
                    """,
                    (dead_threshold,) + c_param + (remaining,),
                ).fetchall()
                results.extend([dict(r) for r in dead])

            return results

    def record_test_result(
        self,
        url: str,
        is_alive: bool,
        latency_ms: Optional[float] = None,
        ttfb_ms: Optional[float] = None,
        egress_ip: Optional[str] = None,
        country: Optional[str] = None,
        city: Optional[str] = None,
        asn: Optional[str] = None,
        anonymity: Optional[str] = None,
    ):
        """Record the outcome of a proxy test."""
        now = time.time()
        with self.get_connection() as conn:
            if is_alive:
                conn.execute(
                    """
                    UPDATE proxies
                    SET status = 'alive',
                        latency_ms = ?,
                        ttfb_ms = ?,
                        egress_ip = ?,
                        country = ?,
                        city = ?,
                        asn = ?,
                        anonymity = ?,
                        fails = 0,
                        first_seen_alive = COALESCE(first_seen_alive, ?),
                        last_checked_at = ?
                    WHERE url = ?
                    """,
                    (latency_ms, ttfb_ms, egress_ip, country, city, asn, anonymity, now, now, url),
                )
            else:
                conn.execute(
                    """
                    UPDATE proxies
                    SET status = 'dead',
                        fails = fails + 1,
                        first_seen_alive = NULL,
                        last_checked_at = ?
                    WHERE url = ?
                    """,
                    (now, url),
                )

    def get_alive_proxies(self, limit: int = 10, country: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch the freshest and fastest verified working proxies (Rule 1: Freshness first, then ping).

        Optionally filters by country prefix (e.g. 'US', 'DE').
        """
        with self.get_connection() as conn:
            query = """
                SELECT id, scheme, host, port, username, password, url, latency_ms, ttfb_ms, egress_ip, country, city, asn, anonymity, first_seen_alive, last_checked_at
                FROM proxies
                WHERE status = 'alive'
            """
            params: List[Any] = []
            if country:
                query += " AND country LIKE ?"
                params.append(f"{country.upper().strip()}%")
            query += " ORDER BY last_checked_at DESC, latency_ms ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]

    def reset_pool(self, clear_all: bool = False):
        """Reset proxy health stats to untested or completely wipe the database."""
        if clear_all:
            if self.db_path.exists():
                self.db_path.unlink()
            self.init_db()
        else:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    UPDATE proxies
                    SET status = 'untested',
                        latency_ms = NULL,
                        ttfb_ms = NULL,
                        egress_ip = NULL,
                        country = NULL,
                        city = NULL,
                        asn = NULL,
                        anonymity = NULL,
                        fails = 0,
                        first_seen_alive = NULL,
                        last_checked_at = NULL
                    """
                )

    def mark_proxy_failed(self, url: str):
        """Mark a proxy as failed when dropped by a gateway slot."""
        now = time.time()
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE proxies
                SET status = 'dead',
                    fails = fails + 1,
                    first_seen_alive = NULL,
                    last_checked_at = ?
                WHERE url = ?
                """,
                (now, url),
            )

    def get_stats(self) -> Dict[str, Any]:
        """Return counts of total, untested, alive, dead, and GeoIP resolved proxies in the database."""
        with self.get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
            untested = conn.execute("SELECT COUNT(*) FROM proxies WHERE status = 'untested'").fetchone()[0]
            alive = conn.execute("SELECT COUNT(*) FROM proxies WHERE status = 'alive'").fetchone()[0]
            dead = conn.execute("SELECT COUNT(*) FROM proxies WHERE status = 'dead'").fetchone()[0]
            unresolved_geoip = conn.execute("SELECT COUNT(*) FROM proxies WHERE country IS NULL OR country = ''").fetchone()[0]

        file_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0

        return {
            "total": total,
            "untested": untested,
            "alive": alive,
            "dead": dead,
            "unresolved_geoip": unresolved_geoip,
            "resolved_geoip": max(0, total - unresolved_geoip),
            "db_path": str(self.db_path),
            "db_size_mb": round(file_size_bytes / (1024 * 1024), 2),
        }

    def get_pool_analytics(self) -> Dict[str, Any]:
        """Compute aggregate pool metrics: ping distribution, age/lifetimes, countries, anonymity."""
        stats = self.get_stats()
        now = time.time()

        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT latency_ms, first_seen_alive, country, anonymity
                FROM proxies
                WHERE status = 'alive'
                """
            ).fetchall()

        latencies = [r["latency_ms"] for r in rows if r["latency_ms"] is not None]
        avg_ping = statistics.mean(latencies) if latencies else 0.0
        stdev_ping = statistics.stdev(latencies) if len(latencies) > 1 else 0.0

        lifetimes = [max(0.0, now - r["first_seen_alive"]) for r in rows if r["first_seen_alive"] is not None]
        avg_life_s = statistics.mean(lifetimes) if lifetimes else 0.0
        oldest_alive_s = max(lifetimes) if lifetimes else 0.0

        country_counts: collections.Counter = collections.Counter()
        for r in rows:
            c = r["country"]
            if c:
                code = c.strip().split()[0].upper()[:2]
                country_counts[code] += 1
            else:
                country_counts["??"] += 1

        countries = sorted(country_counts.items(), key=lambda x: (-x[1], x[0]))

        anon_counts: collections.Counter = collections.Counter()
        for r in rows:
            a = r["anonymity"]
            anon_counts[a.upper() if a else "UNKNOWN"] += 1

        return {
            "total": stats["total"],
            "untested": stats["untested"],
            "alive": stats["alive"],
            "dead": stats["dead"],
            "unresolved_geoip": stats.get("unresolved_geoip", 0),
            "resolved_geoip": stats.get("resolved_geoip", 0),
            "db_path": stats["db_path"],
            "db_size_mb": stats["db_size_mb"],
            "avg_ping": round(avg_ping, 1),
            "stdev_ping": round(stdev_ping, 1),
            "avg_life_s": round(avg_life_s, 1),
            "oldest_alive_s": round(oldest_alive_s, 1),
            "countries": countries,
            "anonymity": dict(anon_counts),
        }
