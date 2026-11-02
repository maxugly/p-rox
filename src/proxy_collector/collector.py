"""High-level ProxyCollector orchestrator."""

import logging
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, List, Optional, Set

from .deduplicator import Deduplicator
from .fetcher import ProxyFetcher
from .models import CollectionStats, Feed, FetchResult, Proxy
from .parsers import parse_file
from .sources import load_catalog

logger = logging.getLogger("proxy_collector")


class ProxyCollector:
    """Orchestrates proxy acquisition, caching, parsing, deduplication, and export."""

    def __init__(
        self,
        cache_dir: Optional[str | Path] = None,
        catalog_path: Optional[str | Path] = None,
        feeds: Optional[List[Feed]] = None,
        concurrency: int = 10,
        ttl_seconds: float = 86400.0,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
    ):
        self.fetcher = ProxyFetcher(
            cache_dir=cache_dir,
            ttl_seconds=ttl_seconds,
            concurrency=concurrency,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self.catalog = feeds if feeds is not None else load_catalog(catalog_path)
        self.cache_dir = self.fetcher.cache_dir
        self.last_fetch_results: List[FetchResult] = []

    def fetch(
        self,
        force: bool = False,
        feeds: Optional[List[Feed]] = None,
        on_progress: Optional[Callable[[FetchResult], None]] = None,
    ) -> List[FetchResult]:
        """Fetch feeds into cache."""
        target_feeds = feeds if feeds is not None else self.catalog
        self.last_fetch_results = self.fetcher.fetch_all(
            target_feeds,
            force=force,
            progress_callback=on_progress,
        )
        return self.last_fetch_results

    def stream_all_sources(
        self,
        fetch: bool = False,
        force: bool = False,
        feeds: Optional[List[Feed]] = None,
    ) -> Iterator[Proxy]:
        """Stream raw proxies from all configured feeds."""
        target_feeds = feeds if feeds is not None else self.catalog

        if fetch:
            self.fetch(force=force, feeds=target_feeds)

        # First iterate over known feed cache destinations
        processed_files: Set[Path] = set()
        for feed in target_feeds:
            if not feed.enabled:
                continue
            cached_path = self.fetcher.get_cached_path(feed)
            if cached_path.is_file():
                processed_files.add(cached_path.resolve())
                yield from parse_file(cached_path, feed=feed)

        # Also parse any additional files existing in cache_dir not mapped to a feed
        if self.cache_dir.is_dir():
            for p in sorted(self.cache_dir.iterdir()):
                if p.is_file() and p.resolve() not in processed_files and not p.name.startswith("."):
                    if p.suffix in (".txt", ".json", ".csv"):
                        yield from parse_file(p)

    def collect_stream(
        self,
        fetch: bool = False,
        force: bool = False,
        protocols: Optional[Iterable[str]] = None,
        web_only: bool = False,
        feeds: Optional[List[Feed]] = None,
    ) -> Iterator[Proxy]:
        """Stream deduplicated proxies matching protocol criteria."""
        dedup = Deduplicator(protocols=protocols, web_only=web_only)
        raw_stream = self.stream_all_sources(fetch=fetch, force=force, feeds=feeds)
        yield from dedup.filter_and_dedup_stream(raw_stream)

    def collect(
        self,
        fetch: bool = False,
        force: bool = False,
        protocols: Optional[Iterable[str]] = None,
        web_only: bool = False,
        limit: Optional[int] = None,
        sort: bool = True,
        feeds: Optional[List[Feed]] = None,
    ) -> List[Proxy]:
        """Collect and return a list of unique proxies in memory."""
        results: List[Proxy] = []
        for p in self.collect_stream(
            fetch=fetch,
            force=force,
            protocols=protocols,
            web_only=web_only,
            feeds=feeds,
        ):
            results.append(p)
            if limit and len(results) >= limit:
                break

        if sort and not limit:
            from .deduplicator import proxy_sort_key
            results.sort(key=proxy_sort_key)

        return results

    def collect_to_file(
        self,
        output_path: str | Path,
        fetch: bool = False,
        force: bool = False,
        protocols: Optional[Iterable[str]] = None,
        web_only: bool = False,
        sort: bool = True,
        on_progress: Optional[Callable[[int], None]] = None,
    ) -> CollectionStats:
        """Collect all proxies, deduplicate, and write directly to a text file."""
        start_time = time.monotonic()
        dedup = Deduplicator(protocols=protocols, web_only=web_only)
        raw_stream = self.stream_all_sources(fetch=fetch, force=force)

        by_source_counter: Counter[str] = Counter()

        def counted_stream():
            for p in raw_stream:
                if p.source:
                    by_source_counter[p.source] += 1
                yield p

        unique_count = dedup.write_to_file(
            counted_stream(),
            output_path=output_path,
            sort=sort,
            on_progress=on_progress,
        )

        duration = time.monotonic() - start_time
        return CollectionStats(
            total_lines_parsed=dedup.total_seen,
            unique_entries=unique_count,
            duplicates_dropped=dedup.duplicates_dropped,
            by_protocol=dict(dedup.counts_by_protocol),
            by_source=dict(by_source_counter),
            feed_results=self.last_fetch_results,
            duration_seconds=duration,
        )

    def ingest_into_pool(
        self,
        pool: Any,
        limit: Optional[int] = None,
        protocols: Optional[Iterable[str]] = None,
        tier: str = "free",
    ) -> int:
        """Direct bridge to loadDowner ProxyPool. Ingests collected proxies into the pool.

        Returns count of newly added proxies.
        """
        # loadDowner ProxyPool supports http, https, socks4, socks5
        default_protocols = {"http", "https", "socks4", "socks5"}
        target_protocols = set(protocols) if protocols else default_protocols

        added = 0
        seen_existing = set()
        if hasattr(pool, "proxies"):
            with pool.lock:
                seen_existing = {
                    (p.get("scheme"), p.get("host"), str(p.get("port")))
                    for p in pool.proxies
                }

        for proxy in self.collect_stream(protocols=target_protocols):
            if (proxy.scheme, proxy.host, str(proxy.port)) in seen_existing:
                continue

            if hasattr(pool, "_add"):
                with pool.lock:
                    pool._add(
                        proxy.scheme,
                        proxy.host,
                        str(proxy.port),
                        proxy.username or "",
                        proxy.password or "",
                        tier,
                    )
            elif hasattr(pool, "proxies"):
                with pool.lock:
                    pool.proxies.append(proxy.as_pool_dict(tier=tier))

            seen_existing.add((proxy.scheme, proxy.host, str(proxy.port)))
            added += 1
            if limit and added >= limit:
                break

        return added
