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


def test_format_helpers():
    from proxy_collector.dashboard import (
        country_code_to_flag,
        format_duration,
        format_uptime,
        visible_len,
    )

    assert format_duration(45) == "45s"
    assert format_duration(125) == "2m 05s"
    assert format_duration(3665) == "1h 01m"

    assert format_uptime(45) == "00:00:45"
    assert format_uptime(125) == "00:02:05"
    assert format_uptime(3665) == "01:01:05"

    assert visible_len("\033[1;92mALIVE\033[0m") == 5
    assert visible_len("plain text") == 10

    # Flag emoji conversion
    assert country_code_to_flag("US") == "🇺🇸"
    assert country_code_to_flag("de") == "🇩🇪"
    assert country_code_to_flag("FR") == "🇫🇷"
    assert country_code_to_flag("jp") == "🇯🇵"
    assert country_code_to_flag("GB") == "🇬🇧"
    assert country_code_to_flag("??") == ""
    assert country_code_to_flag("") == ""
    assert country_code_to_flag(None) == ""


def test_render_sticky_header_empty():
    from proxy_collector.dashboard import ANSI_REGEX, render_sticky_header

    analytics = {
        "total": 100,
        "untested": 100,
        "alive": 0,
        "dead": 0,
        "avg_ping": 0.0,
        "stdev_ping": 0.0,
        "avg_life_s": 0.0,
        "oldest_alive_s": 0.0,
        "countries": [],
        "anonymity": {},
    }

    lines = render_sticky_header(
        analytics=analytics,
        session_died=0,
        uptime_s=10.0,
        speed=5.0,
        workers=10,
        heartbeat_s=180.0,
        width=80,
    )

    assert len(lines) == 6
    assert lines[0].startswith("┌─ p-rox pool monitor")
    assert lines[-1].startswith("└")

    plain_lines = [ANSI_REGEX.sub("", l) for l in lines]
    assert "100 Total" in plain_lines[2]
    assert "0 ALIVE" in plain_lines[2]
    assert "0 Dead (0 Died)" in plain_lines[2]
    assert "Ping: -" in plain_lines[3]
    assert "Avg Life: -" in plain_lines[3]
    assert "Waiting for verified proxies" in plain_lines[4]


def test_render_sticky_header_populated():
    from proxy_collector.dashboard import ANSI_REGEX, render_sticky_header

    analytics = {
        "total": 2050404,
        "untested": 2050300,
        "alive": 14,
        "dead": 90,
        "avg_ping": 118.4,
        "stdev_ping": 24.1,
        "avg_life_s": 750.0,
        "oldest_alive_s": 2892.0,
        "countries": [("US", 8), ("DE", 3), ("CA", 2), ("GB", 1)],
        "anonymity": {"ELITE": 13, "ANONYMOUS": 1},
    }

    lines = render_sticky_header(
        analytics=analytics,
        session_died=12,
        uptime_s=252.0,
        speed=18.2,
        workers=25,
        heartbeat_s=180.0,
        width=80,
    )

    assert len(lines) == 7
    plain_lines = [ANSI_REGEX.sub("", l) for l in lines]
    assert "00:04:12" in plain_lines[1]
    assert "18.2/s" in plain_lines[1]
    assert "2,050,404 Total" in plain_lines[2]
    assert "14 ALIVE" in plain_lines[2]
    assert "90 Dead (12 Died)" in plain_lines[2]
    assert "Ping: 118ms (±24ms)" in plain_lines[3]
    assert "Avg Life: 12m 30s" in plain_lines[3]
    assert "Oldest: 48m 12s" in plain_lines[3]
    assert "Elite: 92% (13)" in plain_lines[3]
    assert "US: 8" in plain_lines[4]
    assert "DE: 3" in plain_lines[4]
    assert "🇺🇸" in plain_lines[4]
    assert "🇩🇪" in plain_lines[4]
    assert "%" in plain_lines[4]


def test_render_sticky_header_multiline_countries():
    from proxy_collector.dashboard import ANSI_REGEX, render_sticky_header

    # 14 countries that cannot fit in a single 80-character width line
    countries = [
        ("US", 50), ("DE", 20), ("CA", 15), ("GB", 10),
        ("FR", 9), ("NL", 8), ("JP", 7), ("KR", 6),
        ("SG", 5), ("IN", 4), ("BR", 3), ("AU", 2),
        ("IT", 2), ("ES", 1),
    ]

    analytics = {
        "total": 1000,
        "untested": 800,
        "alive": 152,
        "dead": 48,
        "avg_ping": 95.0,
        "stdev_ping": 15.0,
        "avg_life_s": 300.0,
        "oldest_alive_s": 900.0,
        "countries": countries,
        "anonymity": {"ELITE": 140, "ANONYMOUS": 12},
    }

    lines = render_sticky_header(
        analytics=analytics,
        session_died=2,
        uptime_s=60.0,
        speed=10.0,
        workers=20,
        heartbeat_s=180.0,
        width=80,
    )

    # Should have wrapped into at least 7 lines (more than 1 country line)
    assert len(lines) >= 7
    plain_lines = [ANSI_REGEX.sub("", l) for l in lines]
    all_text = " ".join(plain_lines)

    # Every country must be visible in the output (no truncation like "+ 2 more")
    assert "+ " not in all_text and "more" not in all_text
    for code, _ in countries:
        assert f"{code}:" in all_text


def test_sticky_terminal_disabled_mode(capsys):
    from proxy_collector.dashboard import StickyTerminal

    term = StickyTerminal(header_lines=6, enabled=False)
    assert not term.enabled

    term.init_terminal()
    term.update_header(["line 1", "line 2"])
    term.print_log("hello log")
    term.restore()

    captured = capsys.readouterr().out
    assert "hello log" in captured

