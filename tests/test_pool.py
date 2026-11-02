"""Tests for ProxyPool, circuit breaker quarantine, and ranking."""

import time
from pathlib import Path
from proxy_collector.pool import ProxyPool, site_key


def test_site_key_normalization():
    assert site_key("https://www.example.com/path?query=1") == "example.com"
    assert site_key("http://assets.mixkit.co/audio/file.mp3") == "assets.mixkit.co"
    assert site_key("example.org") == "example.org"
    assert site_key("") == "default"


def test_pool_tiers_and_deduplication(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    assert pool._add("http", "1.1.1.1", "80", tier="free")
    assert not pool._add("http", "1.1.1.1", "80", tier="free")  # duplicate dropped
    assert pool._add("http", "1.1.1.2", "8080", user="user1", pw="pass1", tier="premium")
    assert len(pool) == 2


def test_pool_circuit_breaker_quarantine(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "80", tier="free")
    p = pool.proxies[0]

    # Transient error: 2 min quarantine
    pool.fail(p, reason="conn")
    assert p["fails"] == 1
    assert p["consec"] == 1
    assert p["benched_until"] > time.time() + 100

    # Rate limit error: 10 min quarantine
    pool.fail(p, reason="ratelimit")
    assert p["consec"] == 2
    assert p["benched_until"] > time.time() + 550

    # Auth error: 1 hr quarantine
    pool.fail(p, reason="auth")
    assert p["consec"] == 3
    assert p["benched_until"] > time.time() + 3500

    # After 5 consecutive failures: 7 days dead
    pool.fail(p, reason="conn")
    pool.fail(p, reason="conn")
    assert p["consec"] == 5
    assert p["benched_until"] > time.time() + 6 * 86400
    key = pool._key(p["scheme"], p["host"], p["port"], p["user"])
    assert pool.health[key].get("dead") is True


def test_pool_ranking_and_pick(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "80")  # Untested: tier 2
    pool._add("http", "1.1.1.2", "80")  # L1 route qualified: tier 1
    pool._add("http", "1.1.1.3", "80")  # L2 site qualified: tier 0
    pool._add("http", "1.1.1.4", "80")  # L3 mix verified: tier -1

    p1, p2, p3, p4 = pool.proxies
    pool.ok(p2)
    pool.site_ok(p3, site="example.com", transfer=False)
    pool.site_ok(p4, site="example.com", transfer=True)

    assert pool._rank(p4, "example.com")[0] == -1
    assert pool._rank(p3, "example.com")[0] == 0
    assert pool._rank(p2, "example.com")[0] == 1
    assert pool._rank(p1, "example.com")[0] == 2

    # Pick should choose the best available tier (-1)
    picked = pool.pick(site="example.com")
    assert picked["host"] == "1.1.1.4"


def test_pool_validate_fast_jump(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "80")
    pool._add("http", "1.1.1.2", "80")
    p1, p2 = pool.proxies

    # Validate returns p1 while live
    assert pool.validate(p1) == p1

    # When p1 fails, validate immediately jumps to live p2
    pool.fail(p1)
    jumped = pool.validate(p1)
    assert jumped["host"] == "1.1.1.2"


def test_site_fail_isolation(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "80")
    p = pool.proxies[0]

    # Site-specific block on siteA
    pool.site_fail(p, site="siteA.com", reason="HTTP-403", cooldown=120)

    # Proxy is blocked on siteA
    summaryA = pool.summary(site="siteA.com")
    assert summaryA["quarantined"] == 1

    # But proxy is NOT quarantined for siteB!
    summaryB = pool.summary(site="siteB.com")
    assert summaryB["quarantined"] == 0
