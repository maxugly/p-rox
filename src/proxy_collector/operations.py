"""Core operational workflows: Syncing feeds into SQLite and testing live candidates."""

import concurrent.futures
import logging
import queue
import sys
import threading
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
    resolve_geoip: bool = True,
    geoip_resolver: Optional[GeoIPResolver] = None,
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

    def _notify(streamed: int, inserted: int):
        if on_import_progress:
            try:
                on_import_progress(streamed, inserted)
            except TypeError:
                on_import_progress(streamed)

    for p in dedup_stream:
        batch.append(p)
        total_streamed += 1
        if len(batch) >= 10000:
            newly_inserted += db.upsert_batch(
                batch,
                resolve_geoip=resolve_geoip,
                geoip_resolver=geoip_resolver,
            )
            batch.clear()
            _notify(total_streamed, newly_inserted)

    if batch:
        newly_inserted += db.upsert_batch(
            batch,
            resolve_geoip=resolve_geoip,
            geoip_resolver=geoip_resolver,
        )
        batch.clear()
        _notify(total_streamed, newly_inserted)

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
        "unresolved_geoip": stats.get("unresolved_geoip", 0),
        "resolved_geoip": stats.get("resolved_geoip", 0),
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
    country: Optional[str] = None,
    geoip_resolver: Optional[GeoIPResolver] = None,
    on_result: Optional[Callable[[Dict[str, Any], bool, Any], None]] = None,
) -> Dict[str, Any]:
    """Test a batch of candidates from the database and record live results."""
    candidates = db.get_candidates_for_testing(limit=limit, keepalive_seconds=keepalive, country=country)
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
        return c, is_alive, profile, p_obj.url

    workers = max(1, min(workers, len(candidates)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, c): c for c in candidates}
        for fut in concurrent.futures.as_completed(futures):
            try:
                candidate, is_alive, profile, target_url = fut.result()
                tested_count += 1
                if is_alive:
                    alive_count += 1
                else:
                    dead_count += 1

                # Sequentially record test result in SQLite from the main thread (zero lock contention)
                geo = profile.geo
                country = f"{geo.country_code} ({geo.city})" if (geo and geo.city) else (geo.country_code if geo else None)
                city = geo.city if geo else None
                asn = geo.org if geo else None
                anonymity = profile.security.anonymity if profile.security else "unknown"

                db.record_test_result(
                    url=target_url,
                    is_alive=is_alive,
                    latency_ms=profile.latency.tcp_handshake_ms if is_alive else None,
                    ttfb_ms=profile.latency.http_ttfb_ms if is_alive else None,
                    egress_ip=profile.egress_ip if is_alive else None,
                    country=country,
                    city=city,
                    asn=asn,
                    anonymity=anonymity,
                )

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
    country: Optional[str] = None,
    stop_event: Optional[Any] = None,
    on_result: Optional[Callable[[Dict[str, Any], bool, Any], None]] = None,
    on_tick: Optional[Callable[[Dict[str, Any], float], None]] = None,
):
    """Run continuous live testing loop maintaining fresh working proxies in SQLite.

    Uses a continuous conveyor-belt queue with staggered worker start times
    for steady throughput (eliminating batch pauses, bursts, and socket lockstep).
    """
    stop = stop_event or threading.Event()
    resolver = GeoIPResolver()
    profiler = ProxyProfiler(geoip_resolver=resolver, timeout=timeout)

    q_capacity = max(50, workers * 2)
    task_queue: queue.Queue = queue.Queue(maxsize=q_capacity)
    result_queue: queue.Queue = queue.Queue(maxsize=q_capacity)

    num_workers = max(1, workers)
    total_tested = 0
    total_alive = 0
    total_dead = 0
    session_died = 0
    start_time = time.monotonic()
    last_tick_time = time.monotonic()

    # Track currently queued URLs to avoid feeding duplicates while in-flight
    in_flight: set = set()
    in_flight_lock = threading.Lock()

    def _worker_loop(worker_id: int):
        # Stagger start times so initial socket connections don't fire simultaneously
        jitter = (worker_id % num_workers) * 0.05
        if stop.wait(jitter):
            return

        while not stop.is_set():
            try:
                candidate = task_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                p_obj = Proxy(
                    scheme=candidate.get("scheme", "http"),
                    host=candidate.get("host", ""),
                    port=int(candidate.get("port", 0)),
                    username=candidate.get("username") or None,
                    password=candidate.get("password") or None,
                    raw_uri=candidate.get("url"),
                )
                profile = profiler.profile(p_obj)
                is_alive = bool(
                    profile.tier > 0 and profile.egress_ip and profile.latency.tcp_handshake_ms < float("inf")
                )
                result_queue.put((candidate, is_alive, profile, p_obj.url))
            except Exception as exc:
                logger.debug("Worker %d profiling error: %s", worker_id, exc)
            finally:
                task_queue.task_done()

    # Start persistent worker threads
    for i in range(num_workers):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True)
        t.start()

    # Feeder: start by testing the last live proxies first!
    first_feed = True

    try:
        while not stop.is_set():
            # 1. Replenish task queue when it drops below worker capacity
            if task_queue.qsize() < num_workers:
                needed = q_capacity - task_queue.qsize()
                if needed > 0:
                    candidates = db.get_candidates_for_testing(
                        limit=needed + len(in_flight),
                        keepalive_seconds=keepalive,
                        include_all_alive=first_feed,
                        country=country,
                    )
                    first_feed = False
                    with in_flight_lock:
                        for c in candidates:
                            u = c.get("url")
                            if u and u not in in_flight:
                                in_flight.add(u)
                                try:
                                    task_queue.put_nowait(c)
                                except queue.Full:
                                    break

            # 2. Process results (single SQLite writer = zero lockups)
            try:
                candidate, is_alive, profile, target_url = result_queue.get(timeout=0.05)
                with in_flight_lock:
                    in_flight.discard(target_url)

                total_tested += 1
                if is_alive:
                    total_alive += 1
                else:
                    total_dead += 1
                    session_died += 1

                # Save sequentially in SQLite
                geo = profile.geo
                country = f"{geo.country_code} ({geo.city})" if (geo and geo.city) else (geo.country_code if geo else None)
                city = geo.city if geo else None
                asn = geo.org if geo else None
                anonymity = profile.security.anonymity if profile.security else "unknown"

                db.record_test_result(
                    url=target_url,
                    is_alive=is_alive,
                    latency_ms=profile.latency.tcp_handshake_ms if is_alive else None,
                    ttfb_ms=profile.latency.http_ttfb_ms if is_alive else None,
                    egress_ip=profile.egress_ip if is_alive else None,
                    country=country,
                    city=city,
                    asn=asn,
                    anonymity=anonymity,
                )

                if on_result:
                    on_result(candidate, is_alive, profile)

            except queue.Empty:
                pass

            # 3. Fire on_tick periodically
            now = time.monotonic()
            if on_tick and (now - last_tick_time >= 1.0):
                last_tick_time = now
                elapsed = now - start_time
                rate = total_tested / elapsed if elapsed > 0 else 0.0
                stats = db.get_stats()
                stats["rate_per_sec"] = round(rate, 1)
                stats["total_tested_session"] = total_tested
                stats["session_died"] = session_died
                stats["uptime_s"] = elapsed
                on_tick(stats, rate)

    finally:
        stop.set()
        # Cleanly drain any completed results
        while not result_queue.empty():
            try:
                candidate, is_alive, profile, target_url = result_queue.get_nowait()
                geo = profile.geo
                country = f"{geo.country_code} ({geo.city})" if (geo and geo.city) else (geo.country_code if geo else None)
                db.record_test_result(
                    url=target_url,
                    is_alive=is_alive,
                    latency_ms=profile.latency.tcp_handshake_ms if is_alive else None,
                    ttfb_ms=profile.latency.http_ttfb_ms if is_alive else None,
                    egress_ip=profile.egress_ip if is_alive else None,
                    country=country,
                    city=geo.city if geo else None,
                    asn=geo.org if geo else None,
                    anonymity=profile.security.anonymity if profile.security else "unknown",
                )
            except queue.Empty:
                break


def run_background_geoip_worker(
    db: ProxyDB,
    resolver: Optional[GeoIPResolver] = None,
    stop_event: Optional[Any] = None,
    batch_size: int = 5000,
    on_batch_complete: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Resolve GeoIP metadata in the background for proxies missing country data.

    Processes candidates in batches, caching IP lookups in memory for maximum throughput.
    Returns total proxies resolved during this run.
    """
    stop = stop_event or threading.Event()
    r = resolver or GeoIPResolver()
    if not r.is_available:
        logger.debug("GeoIP resolver unavailable; background GeoIP worker stopped.")
        return 0

    total_resolved = 0
    host_cache: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}

    while not stop.is_set():
        batch = db.get_unresolved_geoip_batch(limit=batch_size)
        if not batch:
            break

        updates: List[Tuple[Optional[str], Optional[str], Optional[str], int]] = []
        for row in batch:
            cand_id = row["id"]
            host = row.get("host", "")
            if not host:
                updates.append(("??", None, None, cand_id))
                continue

            cached = host_cache.get(host)
            if cached is not None:
                country, city, asn = cached
            else:
                loc = r.lookup(host)
                if loc.country_code:
                    country = f"{loc.country_code} ({loc.city})" if loc.city else loc.country_code
                else:
                    country = "??"
                city = loc.city or None
                asn = loc.org or None
                if len(host_cache) < 50000:
                    host_cache[host] = (country, city, asn)

            updates.append((country, city, asn, cand_id))

        if updates:
            db.update_geoip_batch(updates)
            total_resolved += len(updates)
            if on_batch_complete:
                on_batch_complete(len(updates), total_resolved)

        # Micro-yield to prevent SQLite write lock monopolization
        time.sleep(0.01)

    return total_resolved


def start_background_geoip_thread(
    db: ProxyDB,
    resolver: Optional[GeoIPResolver] = None,
    stop_event: Optional[Any] = None,
    batch_size: int = 5000,
    on_batch_complete: Optional[Callable[[int, int], None]] = None,
) -> Optional[threading.Thread]:
    """Start background GeoIP worker thread if unpopulated candidates exist and database is available."""
    r = resolver or GeoIPResolver()
    if not r.is_available:
        return None

    unresolved = db.get_unresolved_geoip_count()
    if unresolved == 0:
        return None

    stop = stop_event or threading.Event()
    t = threading.Thread(
        target=run_background_geoip_worker,
        args=(db, r, stop, batch_size, on_batch_complete),
        name="p-rox-geoip-resolver",
        daemon=True,
    )
    t.start()
    return t


