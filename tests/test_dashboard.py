"""Tests for TerminalDashboard formatting and rendering."""

from unittest.mock import MagicMock

from proxy_collector.dashboard import TerminalDashboard
from proxy_collector.models import (
    GeoLocation,
    LatencyMetrics,
    Proxy,
    ProxyProfile,
    SecurityMetrics,
)


def test_render_gateway_dashboard():
    mock_slot = MagicMock()
    mock_slot.slot_id = 0
    mock_slot.port = 3030
    mock_slot.stats.status = "healthy"
    mock_slot.stats.country = "DE Frankfurt"
    mock_slot.stats.anonymity = "elite"
    mock_slot.stats.tcp_ms = 45.2
    mock_slot.stats.ttfb_ms = 120.5
    mock_slot.stats.connections_active = 2
    mock_slot.stats.bytes_received = 10240
    mock_slot.stats.bytes_sent = 5120
    mock_slot.stats.upstream = "http://185.220.101.4:9050"

    output = TerminalDashboard.render_gateway_dashboard(
        slots=[mock_slot],
        host="127.0.0.1",
        target_site="https://example.com",
        pool_stats={"L0": 1000, "L1": 50},
    )

    assert "p-rox Local Proxy Gateway" in output
    assert "3030" in output
    assert "DE Frankfurt" in output
    assert "45ms / 120ms" in output
    assert "http://185.220.101.4:9050" in output


def test_print_profile_card(capsys):
    p = Proxy(scheme="http", host="1.2.3.4", port=8080)
    profile = ProxyProfile(
        proxy=p,
        egress_ip="5.6.7.8",
        geo=GeoLocation(
            country_code="US",
            country_name="United States",
            city="Ashburn",
            asn=16509,
            org="Amazon.com, Inc.",
            is_datacenter=True,
        ),
        latency=LatencyMetrics(tcp_handshake_ms=25.0, http_ttfb_ms=110.0),
        security=SecurityMetrics(anonymity="elite", tls_tampering_detected=False),
        tier=1,
    )

    TerminalDashboard.print_profile_card(profile)
    captured = capsys.readouterr().out

    assert "Proxy Profile: http://1.2.3.4:8080" in captured
    assert "True Egress IP:" in captured
    assert "5.6.7.8" in captured
    assert "Ashburn" in captured
    assert "Amazon.com, Inc." in captured
    assert "ELITE" in captured
    assert "25.0ms" in captured
    assert "Clean (Valid CA)" in captured
