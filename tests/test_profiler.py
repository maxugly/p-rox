"""Tests for ProxyProfiler: dual-ping, anonymity levels, and TLS tampering."""

import socket
import threading
from unittest.mock import MagicMock, patch

import requests

from proxy_collector.dashboard import format_anonymity_badge
from proxy_collector.models import Proxy
from proxy_collector.profiler import (
    ProxyProfiler,
    discover_local_public_ip,
    reset_cached_local_ip,
)


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


def test_classify_anonymity_tier_matrix():
    # 1. Elite: clean headers, clean TLS, TCP <= 300ms, TTFB <= 1500ms
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=150.0, http_ttfb_ms=600.0) == "elite"
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=300.0, http_ttfb_ms=1500.0) == "elite"

    # 2. Anonymous: clean headers, but moderate latency (TCP <= 800ms, TTFB <= 4000ms)
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=450.0, http_ttfb_ms=600.0) == "anonymous"
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=150.0, http_ttfb_ms=2000.0) == "anonymous"
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=800.0, http_ttfb_ms=4000.0) == "anonymous"

    # 3. Anonymous: proxy headers present (e.g. Via), fast or moderate latency
    assert ProxyProfiler.classify_anonymity_tier("anonymous", tcp_handshake_ms=50.0, http_ttfb_ms=200.0) == "anonymous"
    assert ProxyProfiler.classify_anonymity_tier("anonymous", tcp_handshake_ms=750.0, http_ttfb_ms=3500.0) == "anonymous"

    # 4. Slow: clean headers or anonymous headers, but high latency (TCP > 800ms or TTFB > 4000ms)
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=850.0, http_ttfb_ms=500.0) == "slow"
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=200.0, http_ttfb_ms=4200.0) == "slow"
    assert ProxyProfiler.classify_anonymity_tier("anonymous", tcp_handshake_ms=900.0, http_ttfb_ms=500.0) == "slow"
    assert ProxyProfiler.classify_anonymity_tier("anonymous", tcp_handshake_ms=100.0, http_ttfb_ms=5000.0) == "slow"

    # 5. Transparent: leaked client IP or transparent base status
    assert (
        ProxyProfiler.classify_anonymity_tier(
            "elite",
            tcp_handshake_ms=50.0,
            http_ttfb_ms=100.0,
            leaked_ip="203.0.113.1",
        )
        == "transparent"
    )
    assert (
        ProxyProfiler.classify_anonymity_tier(
            "transparent",
            tcp_handshake_ms=50.0,
            http_ttfb_ms=100.0,
        )
        == "transparent"
    )

    # 6. Tampered: TLS MitM interception detected (takes precedence over latency)
    assert (
        ProxyProfiler.classify_anonymity_tier(
            "elite",
            tcp_handshake_ms=20.0,
            http_ttfb_ms=50.0,
            tls_tampered=True,
        )
        == "tampered"
    )

    # 7. Unknown / Unreachable
    assert ProxyProfiler.classify_anonymity_tier("elite", tcp_handshake_ms=float("inf"), http_ttfb_ms=float("inf")) == "unknown"
    assert ProxyProfiler.classify_anonymity_tier("unknown", tcp_handshake_ms=100.0, http_ttfb_ms=float("inf")) == "unknown"


def test_profiler_profile_latency_demotion():
    profiler = ProxyProfiler()
    p = Proxy(scheme="http", host="1.2.3.4", port=8080)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {"origin": "185.220.101.4", "headers": {}}

    # Scenario A: Moderate TCP (400ms) -> classified as anonymous despite clean headers
    with patch("requests.get", return_value=mock_resp), patch.object(
        ProxyProfiler, "measure_tcp_handshake", return_value=400.0
    ), patch.object(
        ProxyProfiler, "measure_http_ttfb", return_value=(200.0, "ok", {})
    ), patch.object(
        ProxyProfiler, "check_tls_integrity", return_value=(False, "Valid CA")
    ):
        profile = profiler.profile(p)
        assert profile.security.anonymity == "anonymous"

    # Scenario B: High TTFB (4500ms) -> classified as slow
    with patch("requests.get", return_value=mock_resp), patch.object(
        ProxyProfiler, "measure_tcp_handshake", return_value=100.0
    ), patch.object(
        ProxyProfiler, "measure_http_ttfb", return_value=(4500.0, "ok", {})
    ), patch.object(
        ProxyProfiler, "check_tls_integrity", return_value=(False, "Valid CA")
    ):
        profile = profiler.profile(p)
        assert profile.security.anonymity == "slow"


def test_discover_local_public_ip_sources():
    reset_cached_local_ip()

    # 1. Plain text response (e.g. api.ipify.org)
    mock_resp1 = MagicMock()
    mock_resp1.status_code = 200
    mock_resp1.text = "198.51.100.22\n"

    with patch("requests.get", return_value=mock_resp1):
        ip = discover_local_public_ip(timeout=1.0, force=True)
        assert ip == "198.51.100.22"

    # 2. Cloudflare trace format (ip=...)
    reset_cached_local_ip()
    mock_resp2 = MagicMock()
    mock_resp2.status_code = 200
    mock_resp2.text = "fl=123\nip=203.0.113.99\nts=123456\n"

    with patch("requests.get", return_value=mock_resp2):
        ip = discover_local_public_ip(timeout=1.0, force=True)
        assert ip == "203.0.113.99"

    # 3. JSON response format ({"origin": "..."})
    reset_cached_local_ip()
    mock_resp3 = MagicMock()
    mock_resp3.status_code = 200
    mock_resp3.text = '{"origin": "192.0.2.77"}'
    mock_resp3.json.return_value = {"origin": "192.0.2.77"}

    with patch("requests.get", return_value=mock_resp3):
        ip = discover_local_public_ip(timeout=1.0, force=True)
        assert ip == "192.0.2.77"

    # 4. Failure fallback returns None safely
    reset_cached_local_ip()
    with patch("requests.get", side_effect=Exception("Network down")):
        ip = discover_local_public_ip(timeout=1.0, force=True)
        assert ip is None

    reset_cached_local_ip()


def test_format_anonymity_badge():
    b_elite = format_anonymity_badge("elite")
    assert "ELITE" in b_elite
    assert "\033[1;92m" in b_elite

    b_anon = format_anonymity_badge("anonymous")
    assert "ANON" in b_anon
    assert "\033[93m" in b_anon

    b_slow = format_anonymity_badge("slow")
    assert "SLOW" in b_slow
    assert "\033[35m" in b_slow

    b_trans = format_anonymity_badge("transparent")
    assert "TRANS" in b_trans
    assert "\033[91m" in b_trans

    b_tamper = format_anonymity_badge("tampered")
    assert "TAMPER" in b_tamper
    assert "\033[1;91m" in b_tamper



