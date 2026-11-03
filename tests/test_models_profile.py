"""Tests for profile, geolocation, and network metrics models."""

from proxy_collector.models import (
    GeoLocation,
    LatencyMetrics,
    Proxy,
    ProxyProfile,
    SecurityMetrics,
)


def test_geolocation_serialization():
    geo = GeoLocation(
        country_code="DE",
        country_name="Germany",
        city="Frankfurt",
        region="Hesse",
        latitude=50.1109,
        longitude=8.6821,
        asn=16509,
        org="Amazon.com, Inc.",
        is_datacenter=True,
        source="maxmind",
    )
    d = geo.to_dict()
    assert d["country_code"] == "DE"
    assert d["city"] == "Frankfurt"
    assert d["is_datacenter"] is True

    restored = GeoLocation.from_dict(d)
    assert restored.country_code == "DE"
    assert restored.latitude == 50.1109
    assert restored.is_datacenter is True
    assert restored.source == "maxmind"


def test_latency_metrics_serialization():
    lat = LatencyMetrics(tcp_handshake_ms=24.567, http_ttfb_ms=112.35, measured_at=1000.0)
    d = lat.to_dict()
    assert d["tcp_handshake_ms"] == 24.57
    assert d["http_ttfb_ms"] == 112.35

    restored = LatencyMetrics.from_dict(d)
    assert restored.tcp_handshake_ms == 24.57
    assert restored.http_ttfb_ms == 112.35


def test_security_metrics_serialization():
    sec = SecurityMetrics(
        tls_tampering_detected=False,
        cert_issuer="Cloudflare Inc",
        anonymity="elite",
        leaked_ip=None,
    )
    d = sec.to_dict()
    assert d["anonymity"] == "elite"
    assert d["tls_tampering_detected"] is False

    restored = SecurityMetrics.from_dict(d)
    assert restored.anonymity == "elite"
    assert restored.cert_issuer == "Cloudflare Inc"


def test_proxy_profile_to_dict():
    p = Proxy(scheme="http", host="1.2.3.4", port=8080)
    profile = ProxyProfile(
        proxy=p,
        egress_ip="5.6.7.8",
        geo=GeoLocation(country_code="US", org="Cloudflare, Inc.", is_datacenter=True),
        latency=LatencyMetrics(tcp_handshake_ms=15.0, http_ttfb_ms=90.0),
        security=SecurityMetrics(anonymity="elite"),
        tier=1,
    )
    d = profile.to_dict()
    assert d["egress_ip"] == "5.6.7.8"
    assert d["geo"]["country_code"] == "US"
    assert d["geo"]["is_datacenter"] is True
    assert d["latency"]["tcp_handshake_ms"] == 15.0
    assert d["security"]["anonymity"] == "elite"
    assert d["tier"] == 1
