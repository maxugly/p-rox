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


def test_concurrent_profiling():
    import concurrent.futures

    profiler = ProxyProfiler()
    proxies = [Proxy(scheme="http", host=f"10.0.0.{i}", port=8080) for i in range(5)]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {"origin": "185.220.101.4", "headers": {}}

    with patch("requests.get", return_value=mock_resp), patch.object(
        ProxyProfiler, "measure_tcp_handshake", return_value=12.5
    ):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            profiles = list(executor.map(profiler.profile, proxies))

    assert len(profiles) == 5
    for prof in profiles:
        assert prof.egress_ip == "185.220.101.4"
        assert prof.latency.tcp_handshake_ms == 12.5
        assert prof.security.anonymity == "elite"


def test_cmd_profile_cli(tmp_path, capsys):
    from proxy_collector.cli import cmd_test

    args = MagicMock()
    args.proxy = "http://1.2.3.4:8080"
    args.cache_dir = str(tmp_path)
    args.db = str(tmp_path / "test.db")
    args.timeout = 2.0
    args.limit = 1
    args.workers = 1

    mock_profile = MagicMock()
    mock_profile.proxy = Proxy(scheme="http", host="1.2.3.4", port=8080)
    mock_profile.egress_ip = "1.2.3.4"
    mock_profile.geo = None
    mock_profile.latency.tcp_handshake_ms = 15.0
    mock_profile.latency.http_ttfb_ms = 80.0
    mock_profile.security.anonymity = "elite"
    mock_profile.security.tls_tampering_detected = False
    mock_profile.tier = 1
    mock_profile.last_reason = "OK"

    with patch.object(ProxyProfiler, "profile", return_value=mock_profile):
        cmd_test(args)

    out = capsys.readouterr().out
    assert "Testing proxy http://1.2.3.4:8080" in out
    assert "1.2.3.4" in out


