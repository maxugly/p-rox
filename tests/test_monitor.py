"""Tests for ProxyMonitor staged checking and token dispatching."""

import io
import threading
import time
from unittest.mock import patch
import requests
from proxy_collector.pool import ProxyPool
from proxy_collector.monitor import ProxyMonitor


def test_monitor_route_and_site_check(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    for i in range(1, 4):
        pool._add("http", f"10.1.0.{i}", "8080")

    def mock_get(session, url, **kwargs):
        resp = requests.Response()
        resp.status_code = 200
        if "echo" in url or "ipify" in url:
            resp._content = b"198.51.100.1"
        else:
            resp._content = b"User-agent: *\nDisallow: /admin"
        resp.raw = io.BytesIO(resp._content)
        return resp

    monitor = ProxyMonitor(
        pool,
        sites={"fixture.site": "https://fixture.site/robots.txt"},
        route_workers=3,
        site_workers=2,
        echo_url="https://echo.test/",
    )

    with patch.object(requests.Session, "get", new=mock_get):
        attempted = monitor.check_pending(limit=3)
        assert attempted == 3

    # All 3 proxies should now be route-qualified and site-qualified
    summary = pool.summary(site="fixture.site")
    assert summary["route_qualified"] == 3
    assert summary["site_qualified"] == 3
    assert summary["working"] == 3
