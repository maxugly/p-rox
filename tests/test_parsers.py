"""Tests for line and JSON parsers."""

import json
from proxy_collector.parsers import (
    is_valid_ipv4,
    is_valid_host,
    is_feed_url,
    parse_line,
    parse_json_content,
)


def test_host_validation():
    assert is_valid_ipv4("1.2.3.4")
    assert not is_valid_ipv4("999.1.1.1")
    assert not is_valid_ipv4("1.2.3")
    assert not is_valid_ipv4("example.com")

    assert is_valid_host("1.2.3.4")
    assert is_valid_host("proxy.example.com")
    assert is_valid_host("localhost")
    assert not is_valid_host("not-a-domain")
    assert not is_valid_host("")


def test_is_feed_url():
    assert is_feed_url("https://raw.githubusercontent.com/user/repo/main/http.txt")
    assert is_feed_url("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt")
    assert not is_feed_url("http://1.2.3.4:8080")
    assert not is_feed_url("http://1.2.3.4:8080/")
    assert not is_feed_url("socks5://user:pass@proxy.example.com:1080")


def test_parse_line_standard():
    # Plain scheme
    results = list(parse_line("http://1.2.3.4:8080"))
    assert len(results) == 1
    assert results[0].scheme == "http"
    assert results[0].host == "1.2.3.4"
    assert results[0].port == 8080

    # With credentials
    results = list(parse_line("socks5://alice:secret123@45.67.89.10:1080"))
    assert len(results) == 1
    assert results[0].scheme == "socks5"
    assert results[0].username == "alice"
    assert results[0].password == "secret123"

    # Bare host:port with default scheme
    results = list(parse_line("104.248.63.15:3128", default_scheme="https"))
    assert len(results) == 1
    assert results[0].scheme == "https"
    assert results[0].host == "104.248.63.15"
    assert results[0].port == 3128

    # Colon separated host:port:Country (hideip.me)
    results = list(parse_line("8.219.97.248:80:Singapore", default_scheme="http"))
    assert len(results) == 1
    assert results[0].scheme == "http"
    assert results[0].host == "8.219.97.248"
    assert results[0].port == 80
    assert results[0].country == "Singapore"


def test_parse_line_tunnels():
    vless_line = "vless://b0138d55-a493-4dc3-9932@outline-vpn.cloud:443?security=tls#node"
    results = list(parse_line(vless_line))
    assert len(results) == 1
    assert results[0].scheme == "vless"
    assert results[0].is_tunnel
    assert results[0].host == "outline-vpn.cloud"
    assert results[0].port == 443
    assert results[0].url == vless_line


def test_parse_line_concatenated_br():
    line = "socks5://1.1.1.1:1080<br/>socks5://2.2.2.2:1080%3Cbr%3Esocks5://3.3.3.3:1080"
    results = list(parse_line(line))
    assert len(results) == 3
    assert [p.host for p in results] == ["1.1.1.1", "2.2.2.2", "3.3.3.3"]


def test_parse_line_comments_and_invalid():
    assert list(parse_line("# this is a comment")) == []
    assert list(parse_line("")) == []
    assert list(parse_line("   ")) == []
    assert list(parse_line("invalid_host:notaport")) == []
    assert list(parse_line("1.2.3.4:999999")) == []  # port out of range


def test_parse_json_array_monosans():
    content = json.dumps([
        {
            "protocol": "socks5",
            "host": "186.246.31.197",
            "port": 9050,
            "username": "u1",
            "password": "p1",
            "geolocation": {
                "country": {"names": {"en": "Germany"}, "iso_code": "DE"}
            },
            "timeout": 2.5
        }
    ])
    proxies = list(parse_json_content(content))
    assert len(proxies) == 1
    p = proxies[0]
    assert p.scheme == "socks5"
    assert p.host == "186.246.31.197"
    assert p.port == 9050
    assert p.username == "u1"
    assert p.country == "Germany"
    assert p.extra.get("timeout") == 2.5


def test_parse_json_dict_jetkai():
    content = json.dumps({
        "http": ["1.0.205.87:8080", "1.1.189.58:8080"],
        "socks5": ["2.2.2.2:1080"]
    })
    proxies = list(parse_json_content(content))
    assert len(proxies) == 3
    assert proxies[0].scheme == "http"
    assert proxies[2].scheme == "socks5"


def test_parse_json_flags_vakhov():
    content = json.dumps([
        {
            "host": "test.example.com",
            "ip": "72.195.101.99",
            "port": "4145",
            "country_name": "United States",
            "http": "0",
            "ssl": "0",
            "socks4": "1",
            "socks5": "0"
        }
    ])
    proxies = list(parse_json_content(content))
    assert len(proxies) == 1
    assert proxies[0].scheme == "socks4"
    assert proxies[0].host == "72.195.101.99"
    assert proxies[0].port == 4145
    assert proxies[0].country == "United States"
