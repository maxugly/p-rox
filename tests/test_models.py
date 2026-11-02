"""Tests for proxy models and representations."""

import pytest
from proxy_collector.models import Feed, Proxy, FetchResult, CollectionStats


def test_proxy_initialization():
    p = Proxy(scheme="HTTP", host="192.168.1.1", port=8080)
    assert p.scheme == "http"
    assert p.host == "192.168.1.1"
    assert p.port == 8080
    assert p.username is None
    assert p.password is None
    assert p.url == "http://192.168.1.1:8080"
    assert not p.is_tunnel


def test_proxy_with_auth():
    p = Proxy(scheme="socks5", host="example.com", port=1080, username="user", password="pwd")
    assert p.url == "socks5://user:pwd@example.com:1080"
    assert p.requests_url == "socks5h://user:pwd@example.com:1080"
    assert p.requests_proxies_dict == {
        "http": "socks5h://user:pwd@example.com:1080",
        "https": "socks5h://user:pwd@example.com:1080",
    }
    assert p.playwright_dict == {
        "server": "socks5://example.com:1080",
        "username": "user",
        "password": "pwd",
    }


def test_proxy_as_pool_dict():
    p = Proxy(scheme="http", host="10.0.0.1", port=3128, username="u", password="p", country="Germany")
    pool_dict = p.as_pool_dict(tier="premium")
    assert pool_dict["scheme"] == "http"
    assert pool_dict["host"] == "10.0.0.1"
    assert pool_dict["port"] == "3128"
    assert pool_dict["user"] == "u"
    assert pool_dict["pass"] == "p"
    assert pool_dict["tier"] == "premium"
    assert pool_dict["country"] == "Germany"


def test_tunnel_proxy():
    raw = "vless://uuid-1234@server.com:443?security=tls#node1"
    p = Proxy(scheme="vless", host="server.com", port=443, raw_uri=raw)
    assert p.is_tunnel
    assert p.url == raw
    assert p.dedup_key == raw.lower()


def test_proxy_dedup_key_and_equality():
    p1 = Proxy(scheme="http", host="1.2.3.4", port=80)
    p2 = Proxy(scheme="HTTP", host="1.2.3.4", port=80)
    p3 = Proxy(scheme="http", host="1.2.3.4", port=8080)
    assert p1 == p2
    assert hash(p1) == hash(p2)
    assert p1 != p3
    assert len({p1, p2}) == 1
