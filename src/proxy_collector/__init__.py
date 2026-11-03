"""p-rox: High-performance proxy acquisition, deduplication, staged qualification, and leased routing."""

from .models import (
    CollectionStats,
    Feed,
    FetchResult,
    GeoLocation,
    LatencyMetrics,
    Proxy,
    ProxyProfile,
    SecurityMetrics,
)
from .sources import DEFAULT_FEEDS, load_catalog
from .fetcher import ProxyFetcher
from .parsers import parse_file, parse_line
from .deduplicator import Deduplicator
from .collector import ProxyCollector
from .pool import ProxyPool, site_key
from .leases import ProxyLeases
from .monitor import ProxyMonitor
from .routing import RoutedSession
from .service import ProxyService
from .gateway import GatewayServer, ProxySlot, SlotStats
from .geoip import GeoIPResolver, download_dbip_lite, download_maxmind_geolite2
from .profiler import ProxyProfiler
from .dashboard import TerminalDashboard

__all__ = [
    "Proxy",
    "Feed",
    "FetchResult",
    "CollectionStats",
    "GeoLocation",
    "LatencyMetrics",
    "SecurityMetrics",
    "ProxyProfile",
    "load_catalog",
    "DEFAULT_FEEDS",
    "ProxyFetcher",
    "parse_line",
    "parse_file",
    "Deduplicator",
    "ProxyCollector",
    "ProxyPool",
    "ProxyLeases",
    "ProxyMonitor",
    "RoutedSession",
    "ProxyService",
    "GatewayServer",
    "ProxySlot",
    "SlotStats",
    "GeoIPResolver",
    "download_dbip_lite",
    "download_maxmind_geolite2",
    "ProxyProfiler",
    "TerminalDashboard",
    "site_key",
]

__version__ = "0.0.1"
__repository__ = "https://github.com/maxugly/p-rox"
