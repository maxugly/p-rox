"""Tests for ProxyCollector orchestrator and loadDowner pool bridge."""

import threading
from pathlib import Path

from proxy_collector.collector import ProxyCollector
from proxy_collector.models import Feed, Proxy


class DummyProxyPool:
    """Mock simulating loadDowner ProxyPool."""

    def __init__(self):
        self.lock = threading.RLock()
        self.proxies = []

    def _add(self, scheme, host, port, user, pw, tier):
        self.proxies.append({
            "scheme": scheme,
            "host": host,
            "port": port,
            "user": user,
            "pass": pw,
            "tier": tier,
        })


def test_collector_local_cache(tmp_path):
    cache_dir = tmp_path / "sources"
    cache_dir.mkdir()

    # Create dummy source files
    (cache_dir / "http_list.txt").write_text("1.1.1.1:8080\n1.1.1.2:8080\n")
    (cache_dir / "socks5_list.txt").write_text("socks5://2.2.2.1:1080\n")

    collector = ProxyCollector(cache_dir=cache_dir)
    proxies = collector.collect(fetch=False)

    assert len(proxies) == 3
    schemes = {p.scheme for p in proxies}
    assert "http" in schemes
    assert "socks5" in schemes


def test_collector_ingest_into_pool(tmp_path):
    cache_dir = tmp_path / "sources"
    cache_dir.mkdir()

    (cache_dir / "proxies.txt").write_text(
        "http://1.1.1.1:8080\n"
        "http://1.1.1.1:8080\n"  # duplicate
        "socks5://alice:pwd@2.2.2.2:1080\n"
        "vless://uuid@3.3.3.3:443\n"  # tunnel protocol (should be filtered out by default for pool)
    )

    collector = ProxyCollector(cache_dir=cache_dir)
    pool = DummyProxyPool()

    added = collector.ingest_into_pool(pool)
    assert added == 2
    assert len(pool.proxies) == 2
    assert pool.proxies[0]["host"] == "1.1.1.1"
    assert pool.proxies[1]["user"] == "alice"
    assert pool.proxies[1]["pass"] == "pwd"
