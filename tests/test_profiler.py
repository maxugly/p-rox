"""Tests for ProxyProfiler: dual-ping, anonymity levels, and TLS tampering."""

import socket
import threading
from unittest.mock import MagicMock, patch

import requests

from proxy_collector.models import Proxy
from proxy_collector.profiler import ProxyProfiler


def test_measure_tcp_handshake():
    # Spin up local echo TCP listener
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def _accept():
        try:
            conn, _ = srv.accept()
            conn.close()
        except Exception:
            pass

    t = threading.Thread(target=_accept, daemon=True)
    t.start()

    latency_ms = ProxyProfiler.measure_tcp_handshake("127.0.0.1", port, timeout=1.0)
    assert latency_ms > 0
    assert latency_ms < 500
    srv.close()


def test_determine_anonymity_elite():
    profiler = ProxyProfiler(my_ip="203.0.113.50")
    p = Proxy(scheme="http", host="1.1.1.1", port=8080)

    # Elite: no proxy headers, real IP hidden
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {
        "origin": "185.220.101.4",
        "headers": {
            "Host": "httpbin.org",
            "User-Agent": "p-rox/0.0.1",
            "Accept": "*/*",
        },
    }

    with patch("requests.get", return_value=mock_resp):
        anon, egress, leaked = profiler.determine_anonymity(p)
        assert anon == "elite"
        assert egress == "185.220.101.4"
        assert leaked is None


def test_determine_anonymity_anonymous():
    profiler = ProxyProfiler(my_ip="203.0.113.50")
    p = Proxy(scheme="http", host="1.1.1.1", port=8080)

    # Anonymous: proxy headers present (Via), but real IP hidden
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {
        "origin": "185.220.101.4",
        "headers": {
            "Host": "httpbin.org",
            "Via": "1.1 squid",
            "X-Proxy-Id": "node-42",
        },
    }

    with patch("requests.get", return_value=mock_resp):
        anon, egress, leaked = profiler.determine_anonymity(p)
        assert anon == "anonymous"
        assert egress == "185.220.101.4"
        assert leaked is None


def test_determine_anonymity_transparent():
    profiler = ProxyProfiler(my_ip="203.0.113.50")
    p = Proxy(scheme="http", host="1.1.1.1", port=8080)

    # Transparent: real client IP leaked in X-Forwarded-For
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {
        "origin": "185.220.101.4, 203.0.113.50",
        "headers": {
            "Host": "httpbin.org",
            "X-Forwarded-For": "203.0.113.50",
        },
    }

    with patch("requests.get", return_value=mock_resp):
        anon, egress, leaked = profiler.determine_anonymity(p)
        assert anon == "transparent"
        assert leaked == "203.0.113.50"


def test_check_tls_integrity_clean():
    profiler = ProxyProfiler()
    p = Proxy(scheme="http", host="1.1.1.1", port=8080)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch("requests.get", return_value=mock_resp):
        tampered, msg = profiler.check_tls_integrity(p)
        assert tampered is False
        assert "Valid CA" in msg


def test_check_tls_integrity_mitm():
    profiler = ProxyProfiler()
    p = Proxy(scheme="http", host="1.1.1.1", port=8080)

    with patch("requests.get", side_effect=requests.exceptions.SSLError("Self-signed certificate")):
        tampered, msg = profiler.check_tls_integrity(p)
        assert tampered is True
        assert "Self-signed" in msg
