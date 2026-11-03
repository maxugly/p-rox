"""Command-line interface for p-rox / proxy-collector."""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from .dashboard import TerminalDashboard
from .db import DEFAULT_DB_PATH, ProxyDB
from .gateway import GatewayServer
from .geoip import GeoIPResolver, download_dbip_lite, download_maxmind_geolite2
from .models import STANDARD_SCHEMES, Proxy
from .operations import sync_candidates, test_candidates
from .parsers import parse_line
from .pool import DEFAULT_CACHE_DIR, DEFAULT_PROXY_FILE, ProxyPool


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_sync(args):
    """Download public feeds and import candidate proxies into SQLite database."""
    db = ProxyDB(db_path=args.db)
    print("=" * 70)
    print("  p-rox sync — Public Feed Ingestion & Candidate Import")
    print("=" * 70)

    def on_feed(status, name, nbytes, err):
        sym = {
            "downloaded": "✔ [downloaded]",
            "cached": "★ [cached]    ",
            "failed": "✖ [failed]    ",
        }.get(status, status)
        b_str = f"{nbytes / 1024 / 1024:.2f} MB" if nbytes else ""
        err_str = f"({err})" if err else ""
        print(f"  {sym} {name:20} {b_str:10} {err_str}")

    def on_import(count):
        print(f"  ...ingested {count:,} candidates into database...", end="\r", flush=True)

    print("Step 1/2: Fetching 52 public proxy feeds...")
    sources_p = Path(args.sources).resolve() if getattr(args, "sources", None) else None
    res = sync_candidates(
        db=db,
        force=args.force,
        sources_dir=sources_p,
        concurrency=args.concurrency,
        on_feed_progress=on_feed,
        on_import_progress=on_import,
    )
    print()
    print("\nStep 2/2: Candidate import complete.")
    print("-" * 70)
    print(f"  Feeds:              {res['feeds_downloaded']} downloaded, {res['feeds_cached']} cached, {res['feeds_failed']} failed")
    print(f"  Unique Ingested:    {res['newly_added']:,} newly added ({res['total_parsed']:,} parsed)")
    print(f"  Database Total:     {res['total_in_db']:,} candidates ({res['untested_in_db']:,} untested)")
    print(f"  Duration:           {res['duration_seconds']}s")
    print(f"  Database Path:      {db.db_path}")
    print("-" * 70)
    print("Next: Run 'p-rox test' to test live candidates, or 'p-rox gateway' to route traffic.")


def cmd_test(args):
    """Test candidate proxies and record latency, egress IP, and health."""
    db = ProxyDB(db_path=args.db)

    # Single proxy test mode
    if getattr(args, "proxy", None):
        p_obj = next(parse_line(args.proxy), None)
        if not p_obj:
            print(f"Error: Could not parse proxy string '{args.proxy}'")
            sys.exit(1)
        from .profiler import ProxyProfiler

        geoip = GeoIPResolver()
        profiler = ProxyProfiler(geoip_resolver=geoip, timeout=args.timeout)
        print(f"Testing proxy {p_obj.url}...")
        profile = profiler.profile(p_obj)
        TerminalDashboard.print_profile_card(profile)
        return

    stats = db.get_stats()
    if stats["total"] == 0:
        print("Database is empty. Run 'p-rox sync' first to download and import candidates.")
        return

    limit = args.limit
    workers = args.workers
    timeout = args.timeout

    print("=" * 80)
    print(f"  p-rox test — Testing {limit} candidates with {workers} workers (timeout={timeout}s)")
    print("=" * 80)

    def on_result(candidate, is_alive, profile):
        url = candidate.get("url") or f"{candidate.get('scheme')}://{candidate.get('host')}:{candidate.get('port')}"
        if is_alive:
            lat = f"{profile.latency.tcp_handshake_ms:.0f}ms"
            geo = profile.geo
            loc = f"{geo.country_code} {geo.city}" if (geo and geo.city) else (geo.country_code if geo else "??")
            anon = profile.security.anonymity.upper() if profile.security else ""
            egress = profile.egress_ip or ""
            print(f"  \033[92m[ALIVE]\033[0m {url:35} {lat:>6}  {loc:16} {anon:7} (Egress: {egress})")
        else:
            reason = getattr(profile, "last_reason", "") or "failed"
            print(f"  \033[91m[DEAD ]\033[0m {url:35} {reason}")

    res = test_candidates(
        db=db,
        limit=limit,
        workers=workers,
        timeout=timeout,
        on_result=on_result,
    )

    print("-" * 80)
    print(f"Testing Complete: {res['tested']} tested — \033[92m{res['alive']} ALIVE\033[0m, \033[91m{res['dead']} DEAD\033[0m")
    print(f"Database Pool:    \033[92m{res['total_alive_in_db']} alive\033[0m, {res['total_untested_in_db']:,} untested remaining")
    print("-" * 80)


def cmd_status(args):
    """Query transparent database stats and active gateway status."""
    db = ProxyDB(db_path=args.db)
    stats = db.get_stats()
    alive_sample = db.get_alive_proxies(limit=5)

    print("=" * 70)
    print("  p-rox — System Status")
    print("=" * 70)
    print(f"  Database:           {stats['db_path']} ({stats['db_size_mb']} MB)")
    print(f"  Total Candidates:   {stats['total']:,}")
    print(f"  Untested:           {stats['untested']:,}")
    print(f"  Verified Alive:     \033[92m{stats['alive']:,}\033[0m")
    print(f"  Dead / Benched:     \033[91m{stats['dead']:,}\033[0m")

    # Gateway status
    cache_p = Path(args.cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
    status_file = cache_p / "gateway_status.json"
    pid_file = cache_p / "gateway.pid"

    is_running = False
    if pid_file.is_file():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            is_running = True
        except (ProcessLookupError, ValueError, PermissionError):
            pid_file.unlink(missing_ok=True)
            status_file.unlink(missing_ok=True)

    if is_running and status_file.is_file():
        try:
            gw_data = json.loads(status_file.read_text(encoding="utf-8"))
            slots = gw_data.get("slots", [])
            print("-" * 70)
            print(f"  Gateway:            \033[92mRUNNING\033[0m on {gw_data.get('host', '127.0.0.1')} ({len(slots)} slots)")
            for s in slots[:5]:
                loc = s.get("country") or "unknown"
                print(f"    Slot {s['slot_id']} (:{s['port']}) -> {s.get('upstream', 'none')} [{s.get('status', 'healthy')}] ({loc})")
            if len(slots) > 5:
                print(f"    ... and {len(slots) - 5} more slots")
        except Exception:
            pass
    else:
        print(f"  Gateway:            STOPPED")

    if alive_sample:
        print("-" * 70)
        print("  Fastest Working Proxies:")
        for p in alive_sample:
            lat = f"{p['latency_ms']:.0f}ms" if p.get("latency_ms") else "-"
            loc = p.get("country") or "??"
            print(f"    {p['url']:35} {lat:>6}  {loc:16} (Egress: {p.get('egress_ip') or 'unknown'})")

    print("=" * 70)


def cmd_gateway(args):
    """Run local forward proxy gateway exposing static ports mapped to hot pool proxies."""
    cache_p = Path(args.cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
    cache_p.mkdir(parents=True, exist_ok=True)
    status_file = cache_p / "gateway_status.json"
    pid_file = cache_p / "gateway.pid"

    if args.status:
        if status_file.is_file():
            print(status_file.read_text(encoding="utf-8"))
        else:
            print(json.dumps({"status": "stopped", "message": "No gateway is currently running"}))
        return

    if args.stop:
        if pid_file.is_file():
            try:
                pid = int(pid_file.read_text().strip())
                os.kill(pid, signal.SIGTERM)
                print(f"Sent SIGTERM to gateway process {pid}")
                pid_file.unlink(missing_ok=True)
            except Exception as e:
                print(f"Error stopping gateway: {e}")
        else:
            print("No running gateway PID file found.")
        return

    if args.gen_runit:
        target_path = Path(args.gen_runit)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        script = (
            "#!/bin/sh\n"
            "exec 2>&1\n"
            f"exec p-rox gateway --slots {args.slots} --base-port {args.base_port} --host {args.host}\n"
        )
        target_path.write_text(script, encoding="utf-8")
        target_path.chmod(0o755)
        print(f"Generated executable runit service script at {target_path}")
        return

    if args.daemon:
        if hasattr(os, "fork"):
            pid = os.fork()
            if pid > 0:
                print(f"p-rox gateway daemon started in background (PID: {pid})")
                print(f"Listening on {args.host}:{args.base_port}..{args.base_port + args.slots - 1}")
                print(f"To query status: p-rox gateway --status")
                print(f"To stop:         p-rox gateway --stop")
                sys.exit(0)
            os.setsid()
            pid_file.write_text(str(os.getpid()))

    server = GatewayServer(
        slots_count=args.slots,
        base_port=args.base_port,
        host=args.host,
        target_site=args.site,
        cache_dir=cache_p,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop_event = asyncio.Event()

    def signal_handler():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except (NotImplementedError, RuntimeError):
            pass

    async def run_server():
        await server.start(interactive=not args.daemon)
        await stop_event.wait()
        await server.stop()

    try:
        loop.run_until_complete(run_server())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        pid_file.unlink(missing_ok=True)
        print("\nGateway stopped.")


def cmd_update_geoip(args):
    """Download local binary MMDB databases (DB-IP Lite or MaxMind GeoLite2)."""
    cache_dir = Path(args.cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    source = args.source.lower().strip()

    if source == "dbip":
        print(f"Downloading free DB-IP City Lite MMDB database into {cache_dir}...")
        try:
            path = download_dbip_lite(dest_dir=cache_dir)
            print(f"Successfully installed DB-IP City Lite to {path}")
        except Exception as e:
            print(f"Error downloading DB-IP Lite: {e}")
            sys.exit(1)
    elif source == "maxmind":
        if not args.license_key:
            print("Error: MaxMind requires an account license key. Provide with --license-key <KEY>.")
            sys.exit(1)
        print(f"Downloading MaxMind GeoLite2 database into {cache_dir}...")
        try:
            path = download_maxmind_geolite2(license_key=args.license_key, dest_dir=cache_dir)
            print(f"Successfully installed MaxMind GeoLite2 to {path}")
        except Exception as e:
            print(f"Error downloading MaxMind GeoLite2: {e}")
            sys.exit(1)
    else:
        print(f"Unknown GeoIP source: {source}. Choose 'dbip' or 'maxmind'.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog="p-rox",
        description="p-rox: Fast, transparent public proxy acquisition, testing, and gateway rotation",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. sync (aliases: fetch, update)
    p_sync = subparsers.add_parser("sync", aliases=["fetch"], help="Download 52 feeds and import candidates into SQLite")
    p_sync.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_sync.add_argument("--sources", type=str, default=None, help="Directory for cached feed files")
    p_sync.add_argument("--concurrency", type=int, default=10, help="Concurrent workers for feed download")
    p_sync.add_argument("--force", action="store_true", help="Force re-download even if fresh (<24h)")
    p_sync.set_defaults(func=cmd_sync)

    # 2. test (aliases: check, probe, profile)
    p_test = subparsers.add_parser("test", aliases=["probe", "profile"], help="Test candidates (latency, egress IP, country, anonymity)")
    p_test.add_argument("--limit", type=int, default=50, help="Number of candidates to test (default: 50)")
    p_test.add_argument("--workers", type=int, default=10, help="Concurrent test workers (default: 10)")
    p_test.add_argument("--timeout", type=float, default=6.0, help="Socket timeout per test (default: 6.0s)")
    p_test.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_test.add_argument("--proxy", type=str, default=None, help="Test a single specific proxy URI")
    p_test.set_defaults(func=cmd_test)

    # 3. gateway (aliases: serve)
    p_gateway = subparsers.add_parser("gateway", aliases=["serve"], help="Run local forward proxy slots (default: 127.0.0.1:3030..3039)")
    p_gateway.add_argument("--slots", type=int, default=10, help="Number of static port slots (default: 10)")
    p_gateway.add_argument("--base-port", type=int, default=3030, help="Starting port number (default: 3030)")
    p_gateway.add_argument("--host", type=str, default="127.0.0.1", help="Host interface to bind (default: 127.0.0.1)")
    p_gateway.add_argument("--site", type=str, default=None, help="Optional target site for L2 qualification")
    p_gateway.add_argument("--cache-dir", type=str, default=None, help="Cache directory for gateway status")
    p_gateway.add_argument("--daemon", action="store_true", help="Run as background daemon process")
    p_gateway.add_argument("--stop", action="store_true", help="Stop running background gateway daemon")
    p_gateway.add_argument("--status", action="store_true", help="Query status of running gateway slots")
    p_gateway.add_argument("--gen-runit", type=str, default=None, help="Generate a Void Linux runit service script at PATH")
    p_gateway.set_defaults(func=cmd_gateway)

    # 4. status
    p_status = subparsers.add_parser("status", help="Query transparent database stats and active gateway status")
    p_status.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_status.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_status.set_defaults(func=cmd_status)

    # 5. update-geoip
    p_geoip = subparsers.add_parser("update-geoip", help="Download local binary GeoIP database (DB-IP Lite or MaxMind)")
    p_geoip.add_argument("--source", type=str, default="dbip", choices=["dbip", "maxmind"], help="GeoIP source (default: dbip)")
    p_geoip.add_argument("--license-key", type=str, default=None, help="MaxMind account license key (if source is maxmind)")
    p_geoip.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_geoip.set_defaults(func=cmd_update_geoip)

    args = parser.parse_args()
    setup_logging(args.verbose)
    args.func(args)


if __name__ == "__main__":
    main()
