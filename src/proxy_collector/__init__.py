"""p-rox: High-performance proxy acquisition, deduplication, staged qualification, and leased routing."""

from .models import Proxy, Feed, FetchResult, CollectionStats
from .sources import load_catalog, DEFAULT_FEEDS
from .fetcher import ProxyFetcher
from .parsers import parse_line, parse_file
from .deduplicator import Deduplicator
from .collector import ProxyCollector
from .pool import ProxyPool, site_key
from .leases import ProxyLeases
from .monitor import ProxyMonitor
from .routing import RoutedSession
from .service import ProxyService

__all__ = [
    "Proxy",
    "Feed",
    "FetchResult",
    "CollectionStats",
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
    "site_key",
]

__version__ = "0.1.0"
