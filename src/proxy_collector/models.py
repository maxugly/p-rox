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
