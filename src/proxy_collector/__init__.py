"""Proxy Collector: High-performance multi-source proxy acquisition, normalization, and deduplication."""

from .models import Proxy, Feed, FetchResult, CollectionStats
from .sources import load_catalog, DEFAULT_FEEDS
from .fetcher import ProxyFetcher
from .parsers import parse_line, parse_file
from .deduplicator import Deduplicator
from .collector import ProxyCollector

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
]

__version__ = "0.1.0"
