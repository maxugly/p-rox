"""High-performance concurrent proxy list fetcher with caching and atomic writes."""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, List, Optional
import requests
from urllib3.util import Timeout

from .models import Feed, FetchResult


logger = logging.getLogger("proxy_collector.fetcher")

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class ProxyFetcher:
    """Fetches proxy lists from remote URLs with streaming, caching, and concurrency."""

    def __init__(
        self,
        cache_dir: Optional[str | Path] = None,
        ttl_seconds: float = 86400.0,
        concurrency: int = 10,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
        user_agent: str = DEFAULT_USER_AGENT,
    ):
        if cache_dir is None:
            # Check if local 'sources' directory exists in working tree
            local_sources = Path("sources")
            if local_sources.is_dir():
                self.cache_dir = local_sources.resolve()
            else:
                self.cache_dir = Path.home() / ".cache" / "proxyCollector" / "sources"
        else:
            self.cache_dir = Path(cache_dir).expanduser().resolve()

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_seconds
        self.concurrency = max(1, concurrency)
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.user_agent = user_agent

    def get_cached_path(self, feed: Feed) -> Path:
        """Return the destination path in cache for a given feed."""
        return self.cache_dir / feed.safe_cached_name

    def is_stale(self, path: Path) -> bool:
        """Check if cached file is missing or older than TTL."""
        if not path.is_file():
            return True
        try:
            age = time.time() - path.stat().st_mtime
            return age > self.ttl_seconds
        except OSError:
            return True

    def fetch_feed(
        self,
        feed: Feed,
        force: bool = False,
        session: Optional[requests.Session] = None,
    ) -> FetchResult:
        """Fetch a single feed or reuse cached copy if valid."""
        if not feed.enabled:
            return FetchResult(feed=feed, status="skipped")

        dest_path = self.get_cached_path(feed)
        stale = self.is_stale(dest_path)

        if not stale and not force:
            return FetchResult(
                feed=feed,
                status="cached",
                cached_path=str(dest_path),
                bytes_downloaded=0,
            )

        start_time = time.monotonic()
        temp_path = dest_path.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
        req_session = session or requests.Session()
        req_session.trust_env = False

        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*",
        }

        try:
            timeout = Timeout(connect=self.connect_timeout, read=self.read_timeout)
            with req_session.get(feed.url, headers=headers, timeout=timeout, stream=True) as resp:
                resp.raise_for_status()

                bytes_count = 0
                with open(temp_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)
                            bytes_count += len(chunk)

                # Atomic replace
                os.replace(temp_path, dest_path)
                duration = time.monotonic() - start_time
                return FetchResult(
                    feed=feed,
                    status="downloaded",
                    cached_path=str(dest_path),
                    bytes_downloaded=bytes_count,
                    duration_seconds=duration,
                )

        except Exception as exc:
            # Clean up temp file on failure
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

            duration = time.monotonic() - start_time
            # If cached file exists, fallback to cached copy
            if dest_path.is_file():
                logger.warning(
                    "Download failed for %s (%s), falling back to existing cache: %s",
                    feed.url,
                    type(exc).__name__,
                    exc,
                )
                return FetchResult(
                    feed=feed,
                    status="cached",
                    cached_path=str(dest_path),
                    error=f"Download failed ({type(exc).__name__}), used existing cache",
                    duration_seconds=duration,
                )

            return FetchResult(
                feed=feed,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
                duration_seconds=duration,
            )

    def fetch_all(
        self,
        feeds: List[Feed],
        force: bool = False,
        progress_callback: Optional[Callable[[FetchResult], None]] = None,
    ) -> List[FetchResult]:
        """Fetch all enabled feeds concurrently."""
        results: List[FetchResult] = []
        enabled_feeds = [f for f in feeds if f.enabled]

        if not enabled_feeds:
            return results

        workers = min(self.concurrency, len(enabled_feeds))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_feed = {
                executor.submit(self.fetch_feed, feed, force): feed
                for feed in enabled_feeds
            }

            for future in as_completed(future_to_feed):
                try:
                    res = future.result()
                except Exception as exc:
                    feed = future_to_feed[future]
                    res = FetchResult(
                        feed=feed,
                        status="failed",
                        error=f"Worker exception: {exc}",
                    )
                results.append(res)
                if progress_callback:
                    progress_callback(res)

        return results
