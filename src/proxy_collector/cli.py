"""Command-line interface for p-rox / proxy-collector."""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from .collector import ProxyCollector
from .models import STANDARD_SCHEMES
from .service import ProxyService


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_fetch(args):
    """Fetch feeds into cache."""
    collector = ProxyCollector(
        cache_dir=args.cache_dir,
        catalog_path=args.catalog,
        concurrency=args.concurrency,
        ttl_seconds=args.ttl,
    )
    print(f"Fetching {len(collector.catalog)} feeds into {collector.cache_dir} (concurrency={args.concurrency})...")
    start = time.monotonic()

    def on_progress(res):
        status_symbol = {
            "downloaded": "✔ [downloaded]",
            "cached": "★ [cached]    ",
            "failed": "✖ [failed]    ",
            "skipped": "⊘ [skipped]   ",
        }.get(res.status, res.status)
        bytes_str = f"{res.bytes_downloaded / 1024 / 1024:.2f} MB" if res.bytes_downloaded else ""
        err_str = f"({res.error})" if res.error else ""
        print(f"  {status_symbol} {res.feed.source_name:20} {res.feed.safe_cached_name:28} {bytes_str} {err_str}")

    results = collector.fetch(force=args.force, on_progress=on_progress)
    duration = time.monotonic() - start

    downloaded = sum(1 for r in results if r.status == "downloaded")
    cached = sum(1 for r in results if r.status == "cached")
    failed = sum(1 for r in results if r.status == "failed")
    total_bytes = sum(r.bytes_downloaded for r in results)

    print("\nFetch Complete:")
    print(f"  Downloaded: {downloaded} feeds ({total_bytes / 1024 / 1024:.2f} MB)")
    print(f"  Cached:     {cached} feeds")
    print(f"  Failed:     {failed} feeds")
    print(f"  Duration:   {duration:.2f}s")


def cmd_collect(args):
    """Collect, deduplicate, and write proxies to output."""
    protocols = None
    if args.web_only:
        protocols = STANDARD_SCHEMES
    elif args.protocol:
        protocols = [p.strip().lower() for p in args.protocol.split(",") if p.strip()]

    collector = ProxyCollector(
        cache_dir=args.cache_dir,
        catalog_path=args.catalog,
        concurrency=args.concurrency,
    )

    output_path = Path(args.output).resolve()
    print(f"Collecting proxies from {collector.cache_dir}...")
    if args.fetch or args.force:
        print("Refreshing feeds first...")
        collector.fetch(force=args.force)

    print(f"Parsing and deduplicating into {output_path}...")

    def progress(count):
        print(f"  ...processed {count:,} unique proxies")

    stats = collector.collect_to_file(
        output_path=output_path,
        protocols=protocols,
        sort=not args.no_sort,
        on_progress=progress,
    )

    print(f"\nCollection Complete in {stats.duration_seconds:.2f}s:")
    print(f"  Total lines parsed:   {stats.total_lines_parsed:,}")
    print(f"  Unique entries saved: {stats.unique_entries:,}")
    print(f"  Duplicates dropped:   {stats.duplicates_dropped:,}")
    print("\nProtocols breakdown:")
    for proto, cnt in sorted(stats.by_protocol.items(), key=lambda x: -x[1]):
        print(f"  {proto:15} {cnt:>10,}")


def cmd_stats(args):
    """Show status of cache and feeds."""
    collector = ProxyCollector(
        cache_dir=args.cache_dir,
        catalog_path=args.catalog,
    )
    print(f"Catalog contains {len(collector.catalog)} configured feeds.")
    print(f"Cache directory: {collector.cache_dir}")

    total_files = 0
    total_size = 0
    if collector.cache_dir.is_dir():
        for p in collector.cache_dir.iterdir():
            if p.is_file() and not p.name.startswith("."):
                total_files += 1
                total_size += p.stat().st_size

    print(f"Cached files: {total_files} ({total_size / 1024 / 1024:.2f} MB)")


def cmd_probe(args):
    """Execute staged qualification probe across pending candidate proxies."""
    service = ProxyService(
        cache=args.cache_dir,
        site=args.site,
        route_workers=args.route_workers,
        site_workers=args.site_workers,
    )
    print(f"Executing staged qualification probe (limit={args.limit}, site={args.site or 'none'})...")
    res = service.check(limit=args.limit)
    print(json.dumps(res, indent=2))


def cmd_daemon(args):
    """Run foreground qualification monitor loop."""
    service = ProxyService(
        cache=args.cache_dir,
        site=args.site,
        route_workers=args.route_workers,
        site_workers=args.site_workers,
    )
    print(f"Starting p-rox qualification daemon (site={args.site or 'none'})...")
    service.daemon(interval=args.interval)


def cmd_service_status(args):
    """Query real-time pool metrics and qualification status."""
    service = ProxyService(cache=args.cache_dir, site=args.site)
    res = service.status()
    print(json.dumps(res, indent=2))


def cmd_update_pool(args):
    """Ingest candidate proxies into the service pool."""
    service = ProxyService(cache=args.cache_dir, site=args.site)
    res = service.update(limit=args.limit)
    print(json.dumps(res, indent=2))


def main():
    parser = argparse.ArgumentParser(description="p-rox: High-performance proxy acquisition, qualification, and routing engine")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # fetch command
    p_fetch = subparsers.add_parser("fetch", help="Fetch feeds into cache")
    p_fetch.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_fetch.add_argument("--catalog", type=str, default=None, help="Path to lists.json")
    p_fetch.add_argument("--concurrency", type=int, default=10, help="Concurrent workers")
    p_fetch.add_argument("--ttl", type=float, default=86400.0, help="Cache TTL in seconds")
    p_fetch.add_argument("--force", action="store_true", help="Force re-download even if fresh")
    p_fetch.set_defaults(func=cmd_fetch)

    # collect command
    p_collect = subparsers.add_parser("collect", help="Collect, deduplicate and export proxies")
    p_collect.add_argument("-o", "--output", type=str, default="all_proxies.txt", help="Output file")
    p_collect.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_collect.add_argument("--catalog", type=str, default=None, help="Path to lists.json")
    p_collect.add_argument("--concurrency", type=int, default=10, help="Concurrency for fetch")
    p_collect.add_argument("--fetch", action="store_true", help="Fetch stale feeds before collecting")
    p_collect.add_argument("--force", action="store_true", help="Force re-fetch all feeds")
    p_collect.add_argument("--web-only", action="store_true", help="Only collect standard web proxies (HTTP, HTTPS, SOCKS)")
    p_collect.add_argument("--protocol", type=str, default=None, help="Comma-separated protocols")
    p_collect.add_argument("--no-sort", action="store_true", help="Do not sort output (faster for streaming)")
    p_collect.set_defaults(func=cmd_collect)

    # stats command
    p_stats = subparsers.add_parser("stats", help="Inspect cache and catalog statistics")
    p_stats.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_stats.add_argument("--catalog", type=str, default=None, help="Path to lists.json")
    p_stats.set_defaults(func=cmd_stats)

    # probe command
    p_probe = subparsers.add_parser("probe", help="Run staged qualification probe across pending proxies")
    p_probe.add_argument("--limit", type=int, default=200, help="Max candidates to probe")
    p_probe.add_argument("--site", type=str, default=None, help="Target site for L2 qualification")
    p_probe.add_argument("--route-workers", type=int, default=8, help="L1 route test workers")
    p_probe.add_argument("--site-workers", type=int, default=8, help="L2 site test workers")
    p_probe.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_probe.set_defaults(func=cmd_probe)

    # daemon command
    p_daemon = subparsers.add_parser("daemon", help="Run foreground qualification monitor daemon")
    p_daemon.add_argument("--site", type=str, default=None, help="Target site for L2 qualification")
    p_daemon.add_argument("--route-workers", type=int, default=8, help="L1 route test workers")
    p_daemon.add_argument("--site-workers", type=int, default=8, help="L2 site test workers")
    p_daemon.add_argument("--interval", type=float, default=2.0, help="Status publish interval")
    p_daemon.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_daemon.set_defaults(func=cmd_daemon)

    # status command
    p_status = subparsers.add_parser("status", help="Query pool and service status")
    p_status.add_argument("--site", type=str, default=None, help="Target site")
    p_status.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_status.set_defaults(func=cmd_service_status)

    # update-pool command
    p_update = subparsers.add_parser("update-pool", help="Ingest proxies into service pool")
    p_update.add_argument("--limit", type=int, default=10000, help="Max proxies to ingest")
    p_update.add_argument("--site", type=str, default=None, help="Target site")
    p_update.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_update.set_defaults(func=cmd_update_pool)

    args = parser.parse_args()
    setup_logging(args.verbose)
    args.func(args)


if __name__ == "__main__":
    main()
