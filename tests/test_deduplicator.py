"""Tests for proxy deduplication and filtering."""

from proxy_collector.deduplicator import Deduplicator
from proxy_collector.models import Proxy


def test_deduplicator_basic():
    p1 = Proxy(scheme="http", host="1.1.1.1", port=80)
    p2 = Proxy(scheme="HTTP", host="1.1.1.1", port=80)
    p3 = Proxy(scheme="http", host="1.1.1.2", port=80)

    dedup = Deduplicator()
    results = dedup.dedup_list([p1, p2, p3])

    assert len(results) == 2
    assert dedup.total_seen == 3
    assert dedup.duplicates_dropped == 1
    assert dedup.counts_by_protocol["http"] == 2


def test_deduplicator_web_only():
    p_http = Proxy(scheme="http", host="1.1.1.1", port=80)
    p_socks5 = Proxy(scheme="socks5", host="1.1.1.2", port=1080)
    p_vless = Proxy(scheme="vless", host="1.1.1.3", port=443, raw_uri="vless://xyz@1.1.1.3:443")

    dedup = Deduplicator(web_only=True)
    results = dedup.dedup_list([p_http, p_socks5, p_vless])

    assert len(results) == 2
    assert all(p.scheme in {"http", "socks5"} for p in results)


def test_deduplicator_protocol_filtering():
    p1 = Proxy(scheme="http", host="1.1.1.1", port=80)
    p2 = Proxy(scheme="socks5", host="1.1.1.2", port=1080)
    p3 = Proxy(scheme="socks4", host="1.1.1.3", port=1080)

    dedup = Deduplicator(protocols=["socks5"])
    results = dedup.dedup_list([p1, p2, p3])

    assert len(results) == 1
    assert results[0].scheme == "socks5"


def test_write_to_file(tmp_path):
    output_file = tmp_path / "proxies.txt"
    p1 = Proxy(scheme="http", host="2.2.2.2", port=80)
    p2 = Proxy(scheme="http", host="1.1.1.1", port=80)
    p3 = Proxy(scheme="http", host="2.2.2.2", port=80)

    dedup = Deduplicator()
    count = dedup.write_to_file([p1, p2, p3], output_file, sort=True)

    assert count == 2
    lines = output_file.read_text().splitlines()
    assert lines == ["http://1.1.1.1:80", "http://2.2.2.2:80"]
