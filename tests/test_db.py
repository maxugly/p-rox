"""Tests for ProxyDB SQLite persistence and operations."""

from proxy_collector.db import ProxyDB
from proxy_collector.models import Proxy


def test_proxydb_init_and_stats(tmp_path):
    db_file = tmp_path / "test_proxies.db"
    db = ProxyDB(db_path=db_file)
    stats = db.get_stats()

    assert stats["total"] == 0
    assert stats["untested"] == 0
    assert stats["alive"] == 0
    assert stats["dead"] == 0
    assert db_file.is_file()


def test_proxydb_upsert_and_dedup(tmp_path):
    db = ProxyDB(db_path=tmp_path / "test.db")
    proxies = [
        Proxy(scheme="http", host="1.1.1.1", port=8080),
        Proxy(scheme="http", host="2.2.2.2", port=3128),
        Proxy(scheme="http", host="1.1.1.1", port=8080),  # Duplicate
    ]

    added = db.upsert_batch(proxies)
    assert added == 2

    stats = db.get_stats()
    assert stats["total"] == 2
    assert stats["untested"] == 2


def test_proxydb_test_recording_and_alive_retrieval(tmp_path):
    db = ProxyDB(db_path=tmp_path / "test.db")
    p1 = Proxy(scheme="http", host="1.1.1.1", port=8080)
    p2 = Proxy(scheme="http", host="2.2.2.2", port=3128)
    db.upsert_batch([p1, p2])

    candidates = db.get_candidates_for_testing(limit=10)
    assert len(candidates) == 2

    # Record p1 as alive, p2 as dead
    db.record_test_result(
        url=p1.url,
        is_alive=True,
        latency_ms=45.5,
        ttfb_ms=120.0,
        egress_ip="1.1.1.1",
        country="US (Ashburn)",
        city="Ashburn",
        asn="Amazon",
        anonymity="elite",
    )
    db.record_test_result(url=p2.url, is_alive=False)

    stats = db.get_stats()
    assert stats["total"] == 2
    assert stats["untested"] == 0
    assert stats["alive"] == 1
    assert stats["dead"] == 1

    alive = db.get_alive_proxies(limit=5)
    assert len(alive) == 1
    assert alive[0]["url"] == p1.url
    assert alive[0]["latency_ms"] == 45.5
    assert alive[0]["country"] == "US (Ashburn)"


def test_proxydb_mark_proxy_failed(tmp_path):
    db = ProxyDB(db_path=tmp_path / "test.db")
    p = Proxy(scheme="http", host="1.1.1.1", port=8080)
    db.upsert_batch([p])
    db.record_test_result(url=p.url, is_alive=True, latency_ms=50.0)

    assert len(db.get_alive_proxies()) == 1

    db.mark_proxy_failed(p.url)
    assert len(db.get_alive_proxies()) == 0
    assert db.get_stats()["dead"] == 1


def test_proxydb_keepalive_priority_and_reset(tmp_path):
    import time
    db = ProxyDB(db_path=tmp_path / "test.db")
    p1 = Proxy(scheme="http", host="1.1.1.1", port=8080)
    p2 = Proxy(scheme="http", host="2.2.2.2", port=8080)
    db.upsert_batch([p1, p2])

    # Mark p1 as alive checked 4 minutes ago (> 180s keepalive)
    past_time = time.time() - 240.0
    with db.get_connection() as conn:
        conn.execute(
            "UPDATE proxies SET status = 'alive', last_checked_at = ?, latency_ms = 100.0 WHERE url = ?",
            (past_time, p1.url),
        )

    # get_candidates_for_testing must return p1 FIRST because it's due for 3-minute heartbeat
    candidates = db.get_candidates_for_testing(limit=10, keepalive_seconds=180.0)
    assert len(candidates) == 2
    assert candidates[0]["url"] == p1.url  # Heartbeat priority!
    assert candidates[1]["url"] == p2.url  # Fresh untested candidate

    # Test reset_pool
    db.reset_pool(clear_all=False)
    stats = db.get_stats()
    assert stats["total"] == 2
    assert stats["untested"] == 2
    assert stats["alive"] == 0

