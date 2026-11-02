"""Tests for RoutedSession leased request execution and streaming throughput."""

import io
from unittest.mock import MagicMock, patch
import pytest
import requests
from proxy_collector.pool import ProxyPool
from proxy_collector.routing import RoutedSession


def test_routed_session_injects_proxy_and_leases(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "8080")
    p = pool.proxies[0]
    pool.ok(p)
    pool.site_ok(p, "target.com")

    mock_resp = requests.Response()
    mock_resp.status_code = 200
    mock_resp._content = b"OK"

    with patch.object(requests.Session, "request", return_value=mock_resp) as mock_req:
        session = RoutedSession(pool, site="target.com", min_stage=2)
        resp = session.get("http://target.com/page")

        assert resp.status_code == 200
        assert resp.route_proxy["host"] == "1.1.1.1"

        # Verify proxy argument was injected
        _, kwargs = mock_req.call_args
        assert kwargs["proxies"]["http"] == "http://1.1.1.1:8080"
        assert kwargs["proxies"]["https"] == "http://1.1.1.1:8080"


def test_routed_session_streaming_lease_hold(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "8080")
    p = pool.proxies[0]
    pool.ok(p)
    pool.site_ok(p, "target.com")

    mock_resp = requests.Response()
    mock_resp.status_code = 200
    mock_resp.raw = io.BytesIO(b"streaming-data-payload")

    with patch.object(requests.Session, "request", return_value=mock_resp):
        session = RoutedSession(pool, site="target.com", min_stage=2)
        resp = session.get("http://target.com/stream", stream=True)

        # While stream is active, lease must be held
        with pytest.raises(TimeoutError):
            with pool.leases.lease("target.com", specific=p, timeout=0.02):
                pass

        # Read stream
        chunks = list(resp.iter_content(chunk_size=4))
        assert len(chunks) > 0

        # After stream finishes, lease must be released
        with pool.leases.lease("target.com", specific=p, timeout=0.02) as released_p:
            assert released_p["host"] == "1.1.1.1"

        # Verified throughput was calculated and recorded
        key = pool._key(p["scheme"], p["host"], p["port"], p["user"])
        assert "bytes_per_second" in pool.health[key]["sites"]["target.com"]


def test_routed_session_error_handling(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "8080")
    p = pool.proxies[0]
    pool.ok(p)
    pool.site_ok(p, "target.com")

    # Connection error benches the proxy
    with patch.object(requests.Session, "request", side_effect=requests.ConnectionError("Connection dropped")):
        session = RoutedSession(pool, site="target.com", min_stage=2)
        with pytest.raises(requests.ConnectionError):
            session.get("http://target.com/fail")

        assert p["fails"] == 1
        assert p["last_reason"] == "conn"
        # Lease was cleanly released
        assert len(pool.leases.busy) == 0
