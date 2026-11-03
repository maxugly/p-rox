"""Core operational workflows: Syncing feeds into SQLite and testing live candidates."""

import concurrent.futures
import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .collector import ProxyCollector
from .db import ProxyDB
from .geoip import GeoIPResolver
from .models import Proxy
from .profiler import ProxyProfiler

logger = logging.getLogger(__name__)


def sync_candidates(
    db: ProxyDB,
    force: bool = False,
    sources_dir: Optional[Path] = None,
    concurrency: int = 10,
    on_feed_progress: Optional[Callable[[str, str, int, str], None]] = None,
    on_import_progress: Optional[Callable[[int], None]] = None,
) -> Dict[str, Any]:
    """Fetch public feeds and bulk-load deduplicated candidate proxies into SQLite."""
    start_time = time.monotonic()

    # Determine sources cache location
    if sources_dir is None:
        local_sources = Path("sources").resolve()
        if local_sources.is_dir() and any(local_sources.glob("*.txt")):
            sources_dir = local_sources
        else:
            sources_dir = Path.home() / ".p-rox" / "sources"

    collector = ProxyCollector(
        cache_dir=sources_dir,
        concurrency=concurrency,
    )

    # 1. Fetch remote feeds if stale or requested
    def _feed_cb(res):
        if on_feed_progress:
            on_feed_progress(res.status, res.feed.source_name, res.bytes_downloaded, res.error or "")

    fetch_results = collector.fetch(force=force, on_progress=_feed_cb)
    fetch_duration = time.monotonic() - start_time

    # 2. Stream candidates through canonical deduplicator directly into SQLite
    stream_start = time.monotonic()
    dedup_stream = collector.collect_stream(web_only=True)

    batch: List[Proxy] = []
    total_streamed = 0
    newly_inserted = 0

    for p in dedup_stream:
        batch.append(p)
        total_streamed += 1
        if len(batch) >= 10000:
            newly_inserted += db.upsert_batch(batch)
            batch.clear()
            if on_import_progress:
                on_import_progress(total_streamed)

    if batch:
        newly_inserted += db.upsert_batch(batch)
        batch.clear()
        if on_import_progress:
            on_import_progress(total_streamed)

    import_duration = time.monotonic() - stream_start
    total_duration = time.monotonic() - start_time

    stats = db.get_stats()

    return {
        "feeds_downloaded": sum(1 for r in fetch_results if r.status == "downloaded"),
        "feeds_cached": sum(1 for r in fetch_results if r.status == "cached"),
        "feeds_failed": sum(1 for r in fetch_results if r.status == "failed"),
        "total_parsed": total_streamed,
        "newly_added": newly_inserted,
        "total_in_db": stats["total"],
        "untested_in_db": stats["untested"],
        "duration_seconds": round(total_duration, 2),
        "fetch_seconds": round(fetch_duration, 2),
        "import_seconds": round(import_duration, 2),
    }


def test_candidates(
    db: ProxyDB,
    limit: int = 50,
    workers: int = 25,
    keepalive: float = 180.0,
    timeout: float = 6.0,
    geoip_resolver: Optional[GeoIPResolver] = None,
    on_result: Optional[Callable[[Dict[str, Any], bool, Any], None]] = None,
) -> Dict[str, Any]:
    """Test a batch of candidates from the database and record live results."""
    candidates = db.get_candidates_for_testing(limit=limit, keepalive_seconds=keepalive)
    if not candidates:
        return {"tested": 0, "alive": 0, "dead": 0, "message": "No candidates available for testing."}

    resolver = geoip_resolver or GeoIPResolver()
    profiler = ProxyProfiler(geoip_resolver=resolver, timeout=timeout)

    tested_count = 0
    alive_count = 0
    dead_count = 0

    def _worker(c: Dict[str, Any]):
        p_obj = Proxy(
            scheme=c.get("scheme", "http"),
            host=c.get("host", ""),
            port=int(c.get("port", 0)),
            username=c.get("username") or None,
            password=c.get("password") or None,
            raw_uri=c.get("url"),
        )
        profile = profiler.profile(p_obj)
        is_alive = bool(profile.tier > 0 and profile.egress_ip and profile.latency.tcp_handshake_ms < float("inf"))

        # Save to SQLite
        geo = profile.geo
        country = f"{geo.country_code} ({geo.city})" if (geo and geo.city) else (geo.country_code if geo else None)
        city = geo.city if geo else None
        asn = geo.org if geo else None
        anonymity = profile.security.anonymity if profile.security else "unknown"

        db.record_test_result(
            url=p_obj.url,
            is_alive=is_alive,
            latency_ms=profile.latency.tcp_handshake_ms if is_alive else None,
            ttfb_ms=profile.latency.http_ttfb_ms if is_alive else None,
            egress_ip=profile.egress_ip if is_alive else None,
            country=country,
            city=city,
            asn=asn,
            anonymity=anonymity,
        )

        return c, is_alive, profile

    workers = max(1, min(workers, len(candidates)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, c): c for c in candidates}
        for fut in concurrent.futures.as_completed(futures):
            try:
                candidate, is_alive, profile = fut.result()
                tested_count += 1
                if is_alive:
                    alive_count += 1
                else:
                    dead_count += 1

                if on_result:
                    on_result(candidate, is_alive, profile)
            except Exception as exc:
                logger.debug("Error testing candidate: %s", exc)

    stats = db.get_stats()
    return {
        "tested": tested_count,
        "alive": alive_count,
        "dead": dead_count,
        "total_alive_in_db": stats["alive"],
        "total_untested_in_db": stats["untested"],
    }


def run_continuous_worker(
    db: ProxyDB,
    workers: int = 25,
    keepalive: float = 180.0,
    timeout: float = 6.0,
    batch_size: int = 50,
    stop_event: Optional[Any] = None,
    on_result: Optional[Callable[[Dict[str, Any], bool, Any], None]] = None,
    on_tick: Optional[Callable[[Dict[str, Any], float], None]] = None,
):
    """Run continuous live testing loop maintaining fresh working proxies in SQLite."""
    import threading

    stop = stop_event or threading.Event()
    resolver = GeoIPResolver()
    total_tested = 0
    start_time = time.monotonic()

    while not stop.is_set():
        res = test_candidates(
            db=db,
            limit=batch_size,
            workers=workers,
            keepalive=keepalive,
            timeout=timeout,
            geoip_resolver=resolver,
            on_result=on_result,
        )

        batch_tested = res.get("tested", 0)
        total_tested += batch_tested
        elapsed = time.monotonic() - start_time
        rate = total_tested / elapsed if elapsed > 0 else 0.0

        if on_tick:
            stats = db.get_stats()
            stats["rate_per_sec"] = round(rate, 1)
            stats["total_tested_session"] = total_tested
            on_tick(stats, rate)

        if batch_tested == 0:
            # Idle sleep if no candidates due for check
            stop.wait(2.0)

