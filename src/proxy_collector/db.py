"""SQLite database layer for p-rox proxy storage, status tracking, and rotation."""

import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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
                    last_checked_at REAL,
                    created_at REAL DEFAULT (strftime('%s', 'now'))
                );

                CREATE INDEX IF NOT EXISTS idx_status ON proxies(status);
                CREATE INDEX IF NOT EXISTS idx_latency ON proxies(latency_ms);
                CREATE INDEX IF NOT EXISTS idx_last_checked ON proxies(last_checked_at);
                """
            )

    def upsert_batch(self, proxies: Iterable[Any], batch_size: int = 10000) -> int:
        """Bulk insert proxy candidates. Skips existing URLs.

        Returns total newly inserted rows.
        """
        inserted_total = 0
        batch = []

        with self.get_connection() as conn:
            for p in proxies:
                scheme = getattr(p, "scheme", None) or p.get("scheme", "http")
                host = getattr(p, "host", None) or p.get("host", "")
                port = int(getattr(p, "port", None) or p.get("port", 0))
                user = getattr(p, "username", None) or p.get("user") or None
                pw = getattr(p, "password", None) or p.get("pass") or None
                url = getattr(p, "url", None) or p.get("url")
                if not url:
                    auth = f"{user}:{pw}@" if user else ""
                    url = f"{scheme}://{auth}{host}:{port}"

                batch.append((scheme, host, port, user, pw, url))
                if len(batch) >= batch_size:
                    cursor = conn.executemany(
                        """
                        INSERT OR IGNORE INTO proxies (scheme, host, port, username, password, url)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    inserted_total += cursor.rowcount if cursor.rowcount > 0 else 0
                    batch.clear()

            if batch:
                cursor = conn.executemany(
                    """
                    INSERT OR IGNORE INTO proxies (scheme, host, port, username, password, url)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                inserted_total += cursor.rowcount if cursor.rowcount > 0 else 0

        return inserted_total

    def get_candidates_for_testing(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch candidates prioritized for health checks (untested first, then oldest checked)."""
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, scheme, host, port, username, password, url, status, fails, last_checked_at
                FROM proxies
                WHERE status = 'untested' OR (status = 'alive' AND last_checked_at < ?)
                ORDER BY CASE status WHEN 'untested' THEN 0 ELSE 1 END, last_checked_at ASC
                LIMIT ?
                """,
                (time.time() - 3600.0, limit),
            ).fetchall()

            if len(rows) < limit:
                # Fill remaining capacity with benched proxies that have failed < 5 times
                remaining = limit - len(rows)
                more_rows = conn.execute(
                    """
                    SELECT id, scheme, host, port, username, password, url, status, fails, last_checked_at
                    FROM proxies
                    WHERE status = 'dead' AND fails < 5 AND (last_checked_at IS NULL OR last_checked_at < ?)
                    ORDER BY last_checked_at ASC
                    LIMIT ?
                    """,
                    (time.time() - 7200.0, remaining),
                ).fetchall()
                rows = list(rows) + list(more_rows)

            return [dict(r) for r in rows]

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
                        last_checked_at = ?
                    WHERE url = ?
                    """,
                    (latency_ms, ttfb_ms, egress_ip, country, city, asn, anonymity, now, url),
                )
            else:
                conn.execute(
                    """
                    UPDATE proxies
                    SET status = 'dead',
                        fails = fails + 1,
                        last_checked_at = ?
                    WHERE url = ?
                    """,
                    (now, url),
                )

    def get_alive_proxies(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch the fastest verified working proxies for gateway routing."""
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, scheme, host, port, username, password, url, latency_ms, ttfb_ms, egress_ip, country, city, asn, anonymity
                FROM proxies
                WHERE status = 'alive'
                ORDER BY latency_ms ASC, last_checked_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_proxy_failed(self, url: str):
        """Mark a proxy as failed when dropped by a gateway slot."""
        now = time.time()
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE proxies
                SET status = 'dead',
                    fails = fails + 1,
                    last_checked_at = ?
                WHERE url = ?
                """,
                (now, url),
            )

    def get_stats(self) -> Dict[str, Any]:
        """Return counts of total, untested, alive, and dead proxies in the database."""
        with self.get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
            untested = conn.execute("SELECT COUNT(*) FROM proxies WHERE status = 'untested'").fetchone()[0]
            alive = conn.execute("SELECT COUNT(*) FROM proxies WHERE status = 'alive'").fetchone()[0]
            dead = conn.execute("SELECT COUNT(*) FROM proxies WHERE status = 'dead'").fetchone()[0]

        file_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0

        return {
            "total": total,
            "untested": untested,
            "alive": alive,
            "dead": dead,
            "db_path": str(self.db_path),
            "db_size_mb": round(file_size_bytes / (1024 * 1024), 2),
        }
