"""Data models representing proxies, feeds, and collection metrics."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urlsplit


STANDARD_SCHEMES = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
TUNNEL_SCHEMES = {
    "vless", "vmess", "trojan", "ss", "ssr", "tg", "hy", "hy2",
    "hysteria", "hysteria2", "tuic", "wireguard", "anytls", "reality", "direct"
}
ALL_SUPPORTED_SCHEMES = STANDARD_SCHEMES | TUNNEL_SCHEMES


@dataclass
class Proxy:
    """Represents a normalized proxy server or tunnel endpoint."""

    scheme: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    raw_uri: Optional[str] = None
    country: Optional[str] = None
    anonymity: Optional[str] = None
    source: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.scheme = self.scheme.lower().strip()
        self.host = self.host.lower().strip()
        self.port = int(self.port)
        if self.username:
            self.username = self.username.strip()
        if self.password:
            self.password = self.password.strip()

    @property
    def is_tunnel(self) -> bool:
        """True if this proxy uses a tunnel protocol (VLESS, VMESS, SS, etc.)."""
        return self.scheme in TUNNEL_SCHEMES

    @property
    def url(self) -> str:
        """Canonical URI representation."""
        if self.raw_uri and self.is_tunnel:
            # Preserve full tunnel parameters, normalize scheme
            if "://" in self.raw_uri:
                _, rest = self.raw_uri.split("://", 1)
                return f"{self.scheme}://{rest}"
            return self.raw_uri

        auth = ""
        if self.username or self.password:
            u = quote(self.username or "", safe="")
            p = quote(self.password or "", safe="")
            auth = f"{u}:{p}@" if p or u else ""
        return f"{self.scheme}://{auth}{self.host}:{self.port}"

    @property
    def requests_url(self) -> str:
        """Formatted URL for requests/urllib3 sessions (supports socks5h/socks4a for DNS resolution)."""
        scheme = self.scheme
        if scheme == "socks5":
            scheme = "socks5h"
        elif scheme == "socks4":
            scheme = "socks4a"
        auth = ""
        if self.username or self.password:
            u = quote(self.username or "", safe="")
            p = quote(self.password or "", safe="")
            auth = f"{u}:{p}@" if p or u else ""
        return f"{scheme}://{auth}{self.host}:{self.port}"

    @property
    def requests_proxies_dict(self) -> Dict[str, str]:
        """Dictionary ready for requests `proxies={'http': ..., 'https': ...}`."""
        url = self.requests_url
        return {"http": url, "https": url}

    @property
    def playwright_dict(self) -> Dict[str, str]:
        """Dictionary ready for Playwright browser launch proxy parameter."""
        d = {"server": f"{self.scheme}://{self.host}:{self.port}"}
        if self.username:
            d["username"] = self.username
        if self.password:
            d["password"] = self.password
        return d

    def as_pool_dict(self, tier: str = "free") -> Dict[str, Any]:
        """Convert to dict matching loadDowner ProxyPool internal format."""
        return {
            "scheme": self.scheme,
            "host": self.host,
            "port": str(self.port),
            "user": self.username or "",
            "pass": self.password or "",
            "tier": tier,
            "fails": 0,
            "consec": 0,
            "benched_until": 0.0,
            "label": f":{self.port}",
            "last_reason": "",
            "country": self.country or "",
            "extra": self.extra,
        }

    @property
    def dedup_key(self) -> str:
        """Canonical key for exact deduplication."""
        if self.is_tunnel and self.raw_uri:
            # Preserve tunnel UUID / params in dedup key
            return self.url.lower()
        u = self.username or ""
        p = self.password or ""
        auth = f"{u}:{p}@" if u or p else ""
        return f"{self.scheme}://{auth}{self.host}:{self.port}".lower()

    def __str__(self) -> str:
        return self.url

    def __hash__(self) -> int:
        return hash(self.dedup_key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Proxy):
            return False
        return self.dedup_key == other.dedup_key


@dataclass
class Feed:
    """Configuration for an upstream proxy list feed."""

    url: str
    source_name: str
    format: str = "auto"  # auto, scheme_prefixed, host_port, host_port_country, json_array, json_dict, json_flags
    default_scheme: Optional[str] = None
    cached_as: Optional[str] = None
    expected_entries: Optional[int] = None
    enabled: bool = True
    tags: List[str] = field(default_factory=list)

    @property
    def safe_cached_name(self) -> str:
        """Sanitized cache filename."""
        if self.cached_as and not self.cached_as.startswith("(") and "." in self.cached_as:
            return self.cached_as
        # Derive clean filename from source and url
        stem = self.url.rstrip("/").split("/")[-1].split("?")[0]
        if not stem or "." not in stem:
            stem = f"{stem}.txt" if stem else "feed.txt"
        clean_source = "".join(c if c.isalnum() else "_" for c in self.source_name.lower())
        return f"{clean_source}_{stem}"


@dataclass
class FetchResult:
    """Result of fetching a single feed."""

    feed: Feed
    status: str  # cached, downloaded, failed, skipped
    cached_path: Optional[str] = None
    bytes_downloaded: int = 0
    entries_count: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class CollectionStats:
    """Summary statistics for a proxy collection run."""

    total_lines_parsed: int = 0
    unique_entries: int = 0
    duplicates_dropped: int = 0
    by_protocol: Dict[str, int] = field(default_factory=dict)
    by_source: Dict[str, int] = field(default_factory=dict)
    feed_results: List[FetchResult] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass
class GeoLocation:
    """Geographic and ASN information for a proxy or egress IP."""

    country_code: str = ""
    country_name: str = ""
    city: str = ""
    region: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    asn: Optional[int] = None
    org: str = ""
    is_datacenter: bool = False
    source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "country_code": self.country_code,
            "country_name": self.country_name,
            "city": self.city,
            "region": self.region,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "asn": self.asn,
            "org": self.org,
            "is_datacenter": self.is_datacenter,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "GeoLocation":
        if not data:
            return cls()
        return cls(
            country_code=str(data.get("country_code", "") or ""),
            country_name=str(data.get("country_name", "") or ""),
            city=str(data.get("city", "") or ""),
            region=str(data.get("region", "") or ""),
            latitude=float(data["latitude"]) if data.get("latitude") is not None else None,
            longitude=float(data["longitude"]) if data.get("longitude") is not None else None,
            asn=int(data["asn"]) if data.get("asn") is not None else None,
            org=str(data.get("org", "") or ""),
            is_datacenter=bool(data.get("is_datacenter", False)),
            source=str(data.get("source", "") or ""),
        )


@dataclass
class LatencyMetrics:
    """Network latency benchmarks for a proxy."""

    tcp_handshake_ms: float = 0.0
    http_ttfb_ms: float = 0.0
    measured_at: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tcp_handshake_ms": round(self.tcp_handshake_ms, 2),
            "http_ttfb_ms": round(self.http_ttfb_ms, 2),
            "measured_at": self.measured_at,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "LatencyMetrics":
        if not data:
            return cls()
        return cls(
            tcp_handshake_ms=float(data.get("tcp_handshake_ms", 0.0) or 0.0),
            http_ttfb_ms=float(data.get("http_ttfb_ms", 0.0) or 0.0),
            measured_at=float(data.get("measured_at", 0.0) or 0.0),
        )


@dataclass
class SecurityMetrics:
    """Security and integrity checks."""

    tls_tampering_detected: bool = False
    cert_issuer: Optional[str] = None
    anonymity: str = "unknown"  # "elite", "anonymous", "transparent", "unknown"
    leaked_ip: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tls_tampering_detected": self.tls_tampering_detected,
            "cert_issuer": self.cert_issuer,
            "anonymity": self.anonymity,
            "leaked_ip": self.leaked_ip,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "SecurityMetrics":
        if not data:
            return cls()
        return cls(
            tls_tampering_detected=bool(data.get("tls_tampering_detected", False)),
            cert_issuer=data.get("cert_issuer"),
            anonymity=str(data.get("anonymity", "unknown") or "unknown"),
            leaked_ip=data.get("leaked_ip"),
        )


@dataclass
class ProxyProfile:
    """Rich telemetry, qualification, and operational profile for a proxy."""

    proxy: Proxy
    egress_ip: Optional[str] = None
    geo: Optional[GeoLocation] = None
    latency: Optional[LatencyMetrics] = None
    security: Optional[SecurityMetrics] = None
    tier: int = 0
    fails: int = 0
    consec: int = 0
    benched_until: float = 0.0
    last_tested: float = 0.0
    last_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proxy": self.proxy.as_pool_dict(),
            "egress_ip": self.egress_ip,
            "geo": self.geo.to_dict() if self.geo else None,
            "latency": self.latency.to_dict() if self.latency else None,
            "security": self.security.to_dict() if self.security else None,
            "tier": self.tier,
            "fails": self.fails,
            "consec": self.consec,
            "benched_until": self.benched_until,
            "last_tested": self.last_tested,
            "last_reason": self.last_reason,
        }

