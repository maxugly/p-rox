"""Command-line interface for p-rox / proxy-collector."""

import argparse
import asyncio
import json
import logging
import os
import shutil
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .dashboard import (
    StickyTerminal,
    TerminalDashboard,
    country_code_to_flag,
    render_sticky_header,
)
from .db import DEFAULT_DB_PATH, ProxyDB
from .gateway import GatewayServer
from .geoip import GeoIPResolver, download_dbip_lite, download_maxmind_geolite2
from .models import STANDARD_SCHEMES, Proxy
from .operations import (
    run_continuous_worker,
    start_background_geoip_thread,
    sync_candidates,
    test_candidates,
)
from .parsers import parse_line
from .pool import DEFAULT_CACHE_DIR, DEFAULT_PROXY_FILE, ProxyPool


def setup_logging(verbose: bool = False):
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_run(args):
    """Default interactive runner: continuous live proxy testing and pool maintenance."""
    db = ProxyDB(db_path=args.db)
    stats = db.get_stats()

    # 1. Self-bootstrap if database is empty
    if stats["total"] == 0:
        print("Database is empty. Automatically bootstrapping initial 52 public feeds...")
        cmd_sync(args)
        stats = db.get_stats()

    workers = getattr(args, "workers", 25)
    keepalive = getattr(args, "keepalive", 180.0)
    timeout = getattr(args, "timeout", 6.0)
    serve_enabled = getattr(args, "serve", False)
    country = getattr(args, "country", None)
    no_geoip = getattr(args, "no_geoip", False)

    stop_event = threading.Event()

    def signal_handler(*_):
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, signal_handler)
        except (ValueError, RuntimeError):
            pass

    initial_unresolved = db.get_unresolved_geoip_count()
    geoip_state = {
        "unresolved": initial_unresolved,
        "resolved": max(0, stats["total"] - initial_unresolved),
        "total": stats["total"],
    }

    def _on_geoip_batch(batch_count, total_resolved):
        geoip_state["unresolved"] = max(0, geoip_state["unresolved"] - batch_count)
        geoip_state["resolved"] += batch_count

    # 2. Launch background GeoIP resolver thread for unpopulated candidates if enabled
    if not no_geoip:
        start_background_geoip_thread(
            db=db,
            stop_event=stop_event,
            on_batch_complete=_on_geoip_batch,
        )

    # 3. Start Gateway Server if --serve is requested
    server = None
    gateway_loop = None
    if serve_enabled:
        gw_slots = getattr(args, "slots", 10)
        gw_base_port = getattr(args, "base_port", 3030)
        gw_host = getattr(args, "host", "127.0.0.1")
        gw_site = getattr(args, "site", None)
        gw_cache_dir = getattr(args, "cache_dir", None)

        server = GatewayServer(
            slots_count=gw_slots,
            base_port=gw_base_port,
            host=gw_host,
            target_site=gw_site,
            cache_dir=gw_cache_dir,
            db=db,
            country=country,
        )

        gateway_loop = asyncio.new_event_loop()

        def _run_gateway():
            asyncio.set_event_loop(gateway_loop)
            gateway_loop.run_until_complete(server.start(interactive=False))
            gateway_loop.run_forever()

        gw_thread = threading.Thread(target=_run_gateway, daemon=True)
        gw_thread.start()

    terminal = StickyTerminal(header_lines=6)

    # View Mode Toggle: 's' key toggles between Pool Monitor and Gateway Server views
    VIEW_MONITOR = "monitor"
    VIEW_SERVER = "server"
    VIEW_SLOT_MENU = "slot_menu"
    active_view = [VIEW_MONITOR]
    view_lock = threading.Lock()

    menu_state: Dict[str, Any] = {
        "slot_id": 0,
        "slot_num": 1,
        "cursor_index": 0,
        "query": "",
        "items": [],
        "filtered_items": [],
    }

    start_time = time.monotonic()
    runtime_state = {"session_died": 0, "rate": 0.0, "total_tested": 0}

    def _draw_monitor_header():
        if not terminal.enabled:
            return
        cur_analytics = db.get_pool_analytics()
        cur_analytics["unresolved_geoip"] = geoip_state["unresolved"]
        cur_analytics["resolved_geoip"] = geoip_state["resolved"]
        cur_analytics["no_geoip"] = no_geoip
        elapsed = time.monotonic() - start_time
        cur_header = render_sticky_header(
            analytics=cur_analytics,
            session_died=runtime_state["session_died"],
            uptime_s=elapsed,
            speed=runtime_state["rate"],
            workers=workers,
            heartbeat_s=keepalive,
            gateway_active=serve_enabled,
        )
        terminal.update_header(cur_header)

    def _draw_server_view():
        if not terminal.enabled or not server:
            return
        st = db.get_stats()
        pool_stats = {
            "Alive": st.get("alive", 0),
            "Untested": st.get("untested", 0),
            "Dead": st.get("dead", 0),
        }
        geoip_info = {
            "unresolved": geoip_state["unresolved"],
            "resolved": geoip_state["resolved"],
            "total": geoip_state.get("total", st.get("total", 0)),
            "no_geoip": no_geoip,
        }
        dashboard_str = TerminalDashboard.render_gateway_dashboard(
            slots=server.slots,
            host=server.host,
            target_site=server.target_site,
            pool_stats=pool_stats,
            geoip_stats=geoip_info,
        )
        sys.stdout.write("\033[H" + dashboard_str)
        sys.stdout.flush()

    def _refresh_menu_items():
        all_items = db.get_country_menu_items()
        menu_state["items"] = all_items
        q = str(menu_state["query"]).strip().upper()
        if not q:
            menu_state["filtered_items"] = all_items
        elif q.isdigit():
            menu_state["filtered_items"] = [it for it in all_items if str(it["index"]).startswith(q)]
        else:
            menu_state["filtered_items"] = [it for it in all_items if it["code"].startswith(q)]

    def _draw_slot_menu_view():
        if not terminal.enabled or not server:
            return
        slot_id = menu_state["slot_id"]
        if slot_id >= len(server.slots):
            return
        slot = server.slots[slot_id]
        _refresh_menu_items()
        cols, rows = shutil.get_terminal_size(fallback=(100, 30))
        menu_str = TerminalDashboard.render_country_menu(
            slot_num=menu_state["slot_num"],
            port=slot.port,
            current_filter=slot.country_filter,
            items=menu_state["filtered_items"],
            cursor_index=menu_state["cursor_index"],
            search_query=menu_state["query"],
            term_width=cols,
            term_height=rows,
        )
        sys.stdout.write("\033[H" + menu_str)
        sys.stdout.flush()

    def open_slot_menu(slot_index: int):
        if not serve_enabled or not server or slot_index >= len(server.slots):
            return
        with view_lock:
            active_view[0] = VIEW_SLOT_MENU
            menu_state["slot_id"] = slot_index
            menu_state["slot_num"] = slot_index + 1
            menu_state["cursor_index"] = 0
            menu_state["query"] = ""
            sys.stdout.write("\033[r\033[2J\033[H")
            sys.stdout.flush()
            _draw_slot_menu_view()

    def confirm_menu_selection():
        slot_id = menu_state["slot_id"]
        if server and slot_id < len(server.slots):
            slot = server.slots[slot_id]
            f_items = menu_state["filtered_items"]
            cur_idx = menu_state["cursor_index"]
            if cur_idx == -1:
                slot.set_country_filter(None)
            elif 0 <= cur_idx < len(f_items):
                selected_code = f_items[cur_idx]["code"]
                slot.set_country_filter(selected_code)
        with view_lock:
            active_view[0] = VIEW_SERVER
            sys.stdout.write("\033[r\033[2J\033[H")
            sys.stdout.flush()
            _draw_server_view()

    def exit_slot_menu():
        with view_lock:
            active_view[0] = VIEW_SERVER
            sys.stdout.write("\033[r\033[2J\033[H")
            sys.stdout.flush()
            _draw_server_view()

    def toggle_view():
        if not serve_enabled or not terminal.enabled:
            return
        with view_lock:
            if active_view[0] == VIEW_MONITOR:
                active_view[0] = VIEW_SERVER
                sys.stdout.write("\033[r\033[2J\033[H")
                sys.stdout.flush()
                _draw_server_view()
            else:
                active_view[0] = VIEW_MONITOR
                terminal.init_terminal()
                _draw_monitor_header()

    def handle_add_slot():
        if not serve_enabled or not server or not gateway_loop or not gateway_loop.is_running():
            return
        fut = asyncio.run_coroutine_threadsafe(server.add_slot(), gateway_loop)
        try:
            fut.result(timeout=1.5)
        except Exception:
            pass
        with view_lock:
            if active_view[0] == VIEW_SERVER:
                _draw_server_view()
            elif active_view[0] == VIEW_MONITOR:
                _draw_monitor_header()

    def handle_remove_slot():
        if not serve_enabled or not server or not gateway_loop or not gateway_loop.is_running():
            return
        fut = asyncio.run_coroutine_threadsafe(server.remove_slot(), gateway_loop)
        try:
            fut.result(timeout=1.5)
        except Exception:
            pass
        with view_lock:
            if active_view[0] == VIEW_SERVER:
                _draw_server_view()
            elif active_view[0] == VIEW_MONITOR:
                _draw_monitor_header()

    # Draw initial sticky header
    if terminal.enabled:
        terminal.init_terminal()
        _draw_monitor_header()
    else:
        print("=" * 80)
        print("  p-rox — Continuous Live Proxy Acquisition & Pool Maintenance")
        print(f"  Workers: {workers} | 3-Min Heartbeat: {keepalive}s | Database: {db.db_path}")
        if country:
            print(f"  Country Filter: {country.upper()}")
        if initial_unresolved > 0 and not no_geoip:
            print(f"  GeoIP: Resolving {initial_unresolved:,} candidates in background thread...")
        elif not no_geoip:
            print(f"  GeoIP: 100% mapped ({stats['total']:,} candidates)")
        if serve_enabled and server:
            print(f"  Gateway: {server.slots_count} slots on {server.host}:{server.base_port}..{server.base_port + server.slots_count - 1}")
        print("  Press Ctrl+C to stop cleanly.")
        print("=" * 80)

    # 1Hz background ticker thread ensures real-time clock ticking and telemetry refresh
    def _clock_ticker():
        while not stop_event.is_set():
            stop_event.wait(1.0)
            if stop_event.is_set():
                break
            if terminal.enabled:
                try:
                    with view_lock:
                        if active_view[0] == VIEW_MONITOR:
                            _draw_monitor_header()
                        elif active_view[0] == VIEW_SERVER:
                            _draw_server_view()
                        elif active_view[0] == VIEW_SLOT_MENU:
                            _draw_slot_menu_view()
                except Exception:
                    pass

    ticker_thread = threading.Thread(target=_clock_ticker, daemon=True)
    ticker_thread.start()

    # Interactive keyboard listener for 's' single-button toggle
    def _keyboard_listener():
        if not sys.stdin.isatty():
            return
        try:
            import select
            import termios
            import tty
        except ImportError:
            return

        try:
            old_attr = termios.tcgetattr(sys.stdin)
        except Exception:
            return

        try:
            tty.setcbreak(sys.stdin.fileno())
            while not stop_event.is_set():
                r, _, _ = select.select([sys.stdin], [], [], 0.2)
                if stop_event.is_set():
                    break
                if r:
                    ch = sys.stdin.read(1)
                    if active_view[0] in (VIEW_SERVER, VIEW_MONITOR):
                        if ch in ("s", "S"):
                            toggle_view()
                        elif ch in ("+", "="):
                            handle_add_slot()
                        elif ch in ("-", "_"):
                            handle_remove_slot()
                        elif ch in ("1", "2", "3", "4", "5", "6", "7", "8", "9"):
                            slot_idx = int(ch) - 1
                            open_slot_menu(slot_idx)
                        elif ch == "0":
                            open_slot_menu(9)
                    elif active_view[0] == VIEW_SLOT_MENU:
                        if ch == "\x1b":
                            r2, _, _ = select.select([sys.stdin], [], [], 0.05)
                            if r2:
                                ch2 = sys.stdin.read(1)
                                if ch2 == "[":
                                    r3, _, _ = select.select([sys.stdin], [], [], 0.05)
                                    if r3:
                                        ch3 = sys.stdin.read(1)
                                        cols, _ = shutil.get_terminal_size(fallback=(100, 30))
                                        sep_len = max(80, min(cols, 140))
                                        grid_cols = max(1, min(sep_len // 20, 8))
                                        f_len = len(menu_state["filtered_items"])
                                        cur = menu_state["cursor_index"]
                                        if ch3 == "A":  # Up
                                            if cur == -1:
                                                pass
                                            elif cur < grid_cols:
                                                menu_state["cursor_index"] = -1
                                            else:
                                                menu_state["cursor_index"] = max(0, cur - grid_cols)
                                        elif ch3 == "B":  # Down
                                            if cur == -1:
                                                menu_state["cursor_index"] = 0
                                            else:
                                                menu_state["cursor_index"] = min(f_len - 1, cur + grid_cols)
                                        elif ch3 == "C":  # Right
                                            if cur == -1:
                                                menu_state["cursor_index"] = 0
                                            else:
                                                menu_state["cursor_index"] = min(f_len - 1, cur + 1)
                                        elif ch3 == "D":  # Left
                                            if cur == 0:
                                                menu_state["cursor_index"] = -1
                                            elif cur > 0:
                                                menu_state["cursor_index"] = cur - 1
                                        with view_lock:
                                            _draw_slot_menu_view()
                            else:
                                if menu_state["query"]:
                                    menu_state["query"] = ""
                                    menu_state["cursor_index"] = 0
                                    with view_lock:
                                        _draw_slot_menu_view()
                                else:
                                    exit_slot_menu()
                        elif ch in ("q", "Q"):
                            exit_slot_menu()
                        elif ch in ("\r", "\n"):
                            confirm_menu_selection()
                        elif ch in ("\x7f", "\x08"):
                            if menu_state["query"]:
                                menu_state["query"] = menu_state["query"][:-1]
                                menu_state["cursor_index"] = 0
                                with view_lock:
                                    _draw_slot_menu_view()
                        elif ch == "0" and not menu_state["query"]:
                            menu_state["cursor_index"] = -1
                            with view_lock:
                                _draw_slot_menu_view()
                        elif ch.isalnum():
                            menu_state["query"] += ch.upper()
                            menu_state["cursor_index"] = 0
                            with view_lock:
                                _draw_slot_menu_view()
        except Exception:
            pass
        finally:
            try:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_attr)
            except Exception:
                pass

    if serve_enabled and sys.stdin.isatty():
        kb_thread = threading.Thread(target=_keyboard_listener, daemon=True)
        kb_thread.start()

    def on_result(candidate, is_alive, profile):
        if is_alive and server:
            cand_c = getattr(profile.geo, "country_code", None) if profile.geo else None
            for slot in server.slots:
                if slot.stats.status == "waiting":
                    if not slot.country_filter or (cand_c and cand_c.upper() == slot.country_filter.upper()):
                        cand_dict = {
                            "scheme": candidate.get("scheme", "http"),
                            "host": candidate.get("host", ""),
                            "port": candidate.get("port", 8080),
                            "url": candidate.get("url"),
                            "country": f"{cand_c} ({profile.geo.city})" if (profile.geo and profile.geo.city) else (cand_c or ""),
                            "first_seen_alive": time.time(),
                            "latency_ms": getattr(profile.latency, "tcp_handshake_ms", None),
                            "ttfb_ms": getattr(profile.latency, "http_ttfb_ms", None),
                            "egress_ip": getattr(profile, "egress_ip", ""),
                            "anonymity": getattr(profile.security, "anonymity", "unknown") if profile.security else "unknown",
                        }
                        slot.assign_proxy(cand_dict)
                        slot.stats.status = "healthy"
                        break

        with view_lock:
            if active_view[0] != VIEW_MONITOR:
                return

        url = candidate.get("url") or f"{candidate.get('scheme')}://{candidate.get('host')}:{candidate.get('port')}"
        if is_alive:
            lat = f"{profile.latency.tcp_handshake_ms:.0f}ms"
            geo = profile.geo
            code = geo.country_code if geo else ""
            flag = country_code_to_flag(code)
            f_str = f"{flag} " if flag else ""
            loc = f"{f_str}{geo.country_code} {geo.city}" if (geo and geo.city) else (f"{f_str}{geo.country_code}" if geo else "??")
            anon = profile.security.anonymity.upper() if profile.security else ""
            egress = profile.egress_ip or ""
            line = f"  \033[92m[ALIVE]\033[0m {url:35} {lat:>6}  {loc:18} {anon:7} (Egress: {egress})"
        else:
            reason = getattr(profile, "last_reason", "") or "failed"
            line = f"  \033[91m[DEAD ]\033[0m {url:35} {reason}"
        terminal.print_log(line)

    def on_tick(cur_stats, rate):
        runtime_state["session_died"] = cur_stats.get("session_died", 0)
        runtime_state["rate"] = rate
        runtime_state["total_tested"] = cur_stats.get("total_tested_session", 0)

        with view_lock:
            if active_view[0] == VIEW_MONITOR:
                _draw_monitor_header()
            elif active_view[0] == VIEW_SERVER:
                _draw_server_view()
            elif active_view[0] == VIEW_SLOT_MENU:
                _draw_slot_menu_view()

    try:
        run_continuous_worker(
            db=db,
            workers=workers,
            keepalive=keepalive,
            timeout=timeout,
            batch_size=min(50, workers * 2),
            country=country,
            priority_countries_fn=(lambda: server.get_waiting_countries()) if server else None,
            stop_event=stop_event,
            on_result=on_result,
            on_tick=on_tick,
        )
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        if server and gateway_loop and gateway_loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(server.stop(), gateway_loop)
                fut.result(timeout=2.0)
            except Exception:
                pass
            gateway_loop.call_soon_threadsafe(gateway_loop.stop)
        terminal.restore()

    print("\n" + "-" * 80)
    final_stats = db.get_stats()
    print(f"Session stopped. Pool currently maintains \033[92m{final_stats['alive']} ALIVE\033[0m proxies.")
    print("Run 'p-rox status' anytime to inspect your verified working proxies.")
    print("Source & Documentation: https://github.com/maxugly/p-rox")
    print("-" * 80)


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

    def on_import(streamed, inserted=0):
        if resolve_geoip:
            print(f"  ...ingested {streamed:,} candidates (new: {inserted:,} │ resolving GeoIP)...", end="\r", flush=True)
        else:
            print(f"  ...ingested {streamed:,} candidates (new: {inserted:,})...", end="\r", flush=True)

    print("Step 1/2: Fetching 52 public proxy feeds...")
    sources_p = Path(args.sources).resolve() if getattr(args, "sources", None) else None
    resolve_geoip = not getattr(args, "no_geoip", False)
    res = sync_candidates(
        db=db,
        force=getattr(args, "force", False),
        sources_dir=sources_p,
        concurrency=getattr(args, "concurrency", 10),
        resolve_geoip=resolve_geoip,
        on_feed_progress=on_feed,
        on_import_progress=on_import,
    )
    print()
    print("\nStep 2/2: Candidate import complete.")

    if resolve_geoip:
        unresolved = db.get_unresolved_geoip_count()
        if unresolved > 0:
            total_in_db = res.get("total_in_db", db.get_stats()["total"])
            initial_resolved = max(0, total_in_db - unresolved)
            print(f"\nStep 3/3: Resolving GeoIP metadata for {unresolved:,} candidates...")

            from .operations import run_background_geoip_worker

            def _on_sync_geoip_batch(batch_count, total_resolved):
                remaining = max(0, unresolved - total_resolved)
                current_mapped = initial_resolved + total_resolved
                pct = (current_mapped / total_in_db * 100) if total_in_db > 0 else 0.0
                print(
                    f"  ...GeoIP resolving: \033[1;92m{current_mapped:,}\033[0m mapped │ "
                    f"\033[1;93m{remaining:,} to lookup ↓\033[0m │ \033[1m{pct:.1f}%\033[0m...",
                    end="\r",
                    flush=True,
                )

            run_background_geoip_worker(
                db=db,
                batch_size=5000,
                on_batch_complete=_on_sync_geoip_batch,
            )
            print()

    print("-" * 70)
    print(f"  Feeds:              {res['feeds_downloaded']} downloaded, {res['feeds_cached']} cached, {res['feeds_failed']} failed")
    print(f"  Unique Ingested:    {res['newly_added']:,} newly added ({res['total_parsed']:,} parsed)")
    print(f"  Database Total:     {res['total_in_db']:,} candidates ({res['untested_in_db']:,} untested)")
    final_stats = db.get_stats()
    unres = final_stats.get("unresolved_geoip", 0)
    res_geo = final_stats.get("resolved_geoip", max(0, final_stats["total"] - unres))
    if not resolve_geoip:
        print("  GeoIP Coverage:     disabled (--no-geoip)")
    elif unres > 0:
        pct = (res_geo / final_stats["total"] * 100) if final_stats["total"] > 0 else 0.0
        print(f"  GeoIP Coverage:     {res_geo:,} mapped ({unres:,} to lookup, {pct:.1f}%)")
    else:
        print(f"  GeoIP Coverage:     \033[92m100% mapped\033[0m ({res_geo:,} candidates)")
    print(f"  Duration:           {res['duration_seconds']}s")
    print(f"  Database Path:      {db.db_path}")
    print("-" * 70)
    print("Next: Run 'p-rox' to start testing, or 'p-rox status' to inspect pool health.")


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
    keepalive = getattr(args, "keepalive", 180.0)
    country = getattr(args, "country", None)

    country_info = f" [Country: {country.upper()}]" if country else ""
    print("=" * 80)
    print(f"  p-rox test — Testing {limit} candidates{country_info} with {workers} workers (timeout={timeout}s)")
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
        keepalive=keepalive,
        timeout=timeout,
        country=country,
        on_result=on_result,
    )

    print("-" * 80)
    print(f"Testing Complete: {res['tested']} tested — \033[92m{res['alive']} ALIVE\033[0m, \033[91m{res['dead']} DEAD\033[0m")
    print(f"Database Pool:    \033[92m{res['total_alive_in_db']} alive\033[0m, {res['total_untested_in_db']:,} untested remaining")
    print("-" * 80)


def cmd_status(args):
    """Query transparent database stats and proxy pool maintenance health."""
    db = ProxyDB(db_path=args.db)
    analytics = db.get_pool_analytics()
    alive_sample = db.get_alive_proxies(limit=8)

    print("=" * 70)
    print("  p-rox — Proxy Pool Status")
    print("=" * 70)
    print("  GitHub:             https://github.com/maxugly/p-rox")
    print(f"  Database:           {analytics['db_path']} ({analytics['db_size_mb']} MB)")
    print(f"  Total Candidates:   {analytics['total']:,}")
    print(f"  Untested:           {analytics['untested']:,}")
    print(f"  Verified Alive:     \033[92m{analytics['alive']:,}\033[0m")
    print(f"  Dead / Benched:     \033[91m{analytics['dead']:,}\033[0m")
    unres = analytics.get("unresolved_geoip", 0)
    res_geo = analytics.get("resolved_geoip", max(0, analytics["total"] - unres))
    if unres > 0:
        pct = (res_geo / analytics["total"] * 100) if analytics["total"] > 0 else 0
        print(f"  GeoIP Coverage:     {res_geo:,} mapped ({unres:,} to lookup, {pct:.1f}%)")
    else:
        print(f"  GeoIP Coverage:     \033[92m100% mapped\033[0m ({res_geo:,} candidates)")
    if analytics["alive"] > 0:
        total_alive = analytics["alive"]
        c_parts = []
        for c in analytics["countries"][:10]:
            code, cnt = c[0], c[1]
            flag = country_code_to_flag(code)
            f_str = f"{flag} " if flag else ""
            pct = (cnt / total_alive) * 100
            pct_str = f"{pct:.1f}%" if (pct % 1 != 0) else f"{int(pct)}%"
            c_parts.append(f"{f_str}{code}: {cnt} ({pct_str})")
        print(f"  Ping Performance:   {analytics['avg_ping']:.0f}ms (±{analytics['stdev_ping']:.0f}ms)")
        print(f"  Top Countries:      {', '.join(c_parts)}")

    if alive_sample:
        print("-" * 70)
        print("  Fastest Verified Working Proxies (Freshest First):")
        for p in alive_sample:
            lat = f"{p['latency_ms']:.0f}ms" if p.get("latency_ms") else "-"
            raw_country = p.get("country") or "??"
            code = raw_country.split()[0].upper()[:2] if raw_country else ""
            flag = country_code_to_flag(code)
            f_str = f"{flag} " if flag else ""
            loc = f"{f_str}{raw_country}"
            age_s = int(time.time() - (p.get("last_checked_at") or time.time()))
            age_str = f"{age_s}s ago" if age_s < 120 else f"{age_s // 60}m ago"
            print(f"    {p['url']:35} {lat:>6}  {loc:20} (Egress: {p.get('egress_ip') or 'unknown'}) [{age_str}]")

    print("=" * 70)


def cmd_reset(args):
    """Reset candidate health in database or completely delete cache/database."""
    db = ProxyDB(db_path=args.db)
    if args.all:
        confirm = (
            input("Are you sure you want to completely delete the database and feed cache? (y/N): ")
            if sys.stdin.isatty()
            else "y"
        )
        if confirm.lower().strip() == "y":
            db.reset_pool(clear_all=True)
            sources_dir = Path.home() / ".p-rox" / "sources"
            if sources_dir.exists():
                import shutil

                shutil.rmtree(sources_dir, ignore_errors=True)
            print("Purged entire database and feed cache. Clean slate ready.")
        else:
            print("Aborted.")
    else:
        db.reset_pool(clear_all=False)
        print("Reset all proxy health records. All candidates set back to UNTESTED.")


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
        country=getattr(args, "country", None),
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
        description="p-rox: Fast, transparent public proxy acquisition, filtering, and maintenance engine",
        epilog="Source & Documentation: https://github.com/maxugly/p-rox",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")
    parser.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    parser.add_argument("--workers", "-w", type=int, default=25, help="Concurrent test workers (default: 25)")
    parser.add_argument("--keepalive", type=int, default=180, help="Heartbeat re-check interval in seconds (default: 180s)")
    parser.add_argument("--timeout", type=float, default=6.0, help="Socket timeout per test (default: 6.0s)")
    parser.add_argument("--sources", type=str, default=None, help="Directory for cached feed files")
    parser.add_argument("--country", "-c", type=str, default=None, help="Filter proxies by country code (e.g. US, DE, FR)")
    parser.add_argument("--no-geoip", "--skip-geoip", action="store_true", help="Disable automatic GeoIP calculation")
    parser.add_argument("--serve", action="store_true", help="Start local forward proxy gateway on ports 3030..3039")
    parser.add_argument("--slots", type=int, default=10, help="Number of forward proxy gateway slots (default: 10)")
    parser.add_argument("--base-port", type=int, default=3030, help="Starting base port for gateway slots (default: 3030)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Gateway bind host (default: 127.0.0.1)")
    parser.add_argument("--site", type=str, default=None, help="Target qualification site for gateway proxies")
    parser.set_defaults(func=cmd_run)

    subparsers = parser.add_subparsers(
        dest="command",
        required=False,
        metavar="{run,sync,test,status,reset,update-geoip,gateway}",
    )

    # 1. run (explicit alias for default runner)
    p_run = subparsers.add_parser("run", help="Run continuous live testing and pool maintenance (default)")
    p_run.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH))
    p_run.add_argument("--workers", "-w", type=int, default=25)
    p_run.add_argument("--keepalive", type=int, default=180)
    p_run.add_argument("--timeout", type=float, default=6.0)
    p_run.add_argument("--sources", type=str, default=None)
    p_run.add_argument("--country", "-c", type=str, default=None, help="Filter proxies by country code (e.g. US, DE, FR)")
    p_run.add_argument("--no-geoip", "--skip-geoip", action="store_true", help="Disable automatic GeoIP calculation")
    p_run.add_argument("--serve", action="store_true", help="Start local forward proxy gateway on ports 3030..3039")
    p_run.add_argument("--slots", type=int, default=10, help="Number of forward proxy gateway slots (default: 10)")
    p_run.add_argument("--base-port", type=int, default=3030, help="Starting base port for gateway slots (default: 3030)")
    p_run.add_argument("--host", type=str, default="127.0.0.1", help="Gateway bind host (default: 127.0.0.1)")
    p_run.add_argument("--site", type=str, default=None, help="Target qualification site for gateway proxies")
    p_run.set_defaults(func=cmd_run)

    # 2. sync (aliases: fetch, update)
    p_sync = subparsers.add_parser("sync", aliases=["fetch"], help="Download 52 feeds and import candidates into SQLite")
    p_sync.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_sync.add_argument("--sources", type=str, default=None, help="Directory for cached feed files")
    p_sync.add_argument("--concurrency", type=int, default=10, help="Concurrent workers for feed download")
    p_sync.add_argument("--no-geoip", "--skip-geoip", action="store_true", help="Disable GeoIP calculation during ingestion")
    p_sync.add_argument("--force", action="store_true", help="Force re-download even if fresh (<24h)")
    p_sync.set_defaults(func=cmd_sync)

    # 3. test (aliases: check, probe, profile)
    p_test = subparsers.add_parser("test", aliases=["probe", "profile"], help="Batch test candidates (latency, egress IP, country, anonymity)")
    p_test.add_argument("--limit", type=int, default=50, help="Number of candidates to test (default: 50)")
    p_test.add_argument("--workers", "-w", type=int, default=25, help="Concurrent test workers (default: 25)")
    p_test.add_argument("--keepalive", type=int, default=180, help="Heartbeat re-check interval in seconds (default: 180s)")
    p_test.add_argument("--timeout", type=float, default=6.0, help="Socket timeout per test (default: 6.0s)")
    p_test.add_argument("--country", "-c", type=str, default=None, help="Filter candidates to test by country code (e.g. US, DE)")
    p_test.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_test.add_argument("--proxy", type=str, default=None, help="Test a single specific proxy URI")
    p_test.set_defaults(func=cmd_test)

    # 4. status
    p_status = subparsers.add_parser("status", help="Query database stats, tested proxies, and pool health")
    p_status.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_status.set_defaults(func=cmd_status)

    # 5. reset
    p_reset = subparsers.add_parser("reset", help="Reset test health or completely delete database/cache")
    p_reset.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to SQLite database")
    p_reset.add_argument("--all", action="store_true", help="Completely delete database and feed files")
    p_reset.set_defaults(func=cmd_reset)

    # 6. update-geoip
    p_geoip = subparsers.add_parser("update-geoip", help="Download local binary GeoIP database (DB-IP Lite or MaxMind)")
    p_geoip.add_argument("--source", type=str, default="dbip", choices=["dbip", "maxmind"], help="GeoIP source (default: dbip)")
    p_geoip.add_argument("--license-key", type=str, default=None, help="MaxMind account license key (if source is maxmind)")
    p_geoip.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_geoip.set_defaults(func=cmd_update_geoip)

    # 7. gateway (standalone local proxy server)
    p_gateway = subparsers.add_parser("gateway", aliases=["serve"], help="Run standalone local proxy server")
    p_gateway.add_argument("--slots", type=int, default=10, help="Number of forward proxy slots (default: 10)")
    p_gateway.add_argument("--base-port", type=int, default=3030, help="Starting base port (default: 3030)")
    p_gateway.add_argument("--host", type=str, default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p_gateway.add_argument("--country", "-c", type=str, default=None, help="Filter gateway proxy slots by country code (e.g. US, DE)")
    p_gateway.add_argument("--site", type=str, default=None, help="Target qualification site")
    p_gateway.add_argument("--cache-dir", type=str, default=None, help="Cache directory")
    p_gateway.add_argument("--daemon", action="store_true", help="Run gateway as background daemon")
    p_gateway.add_argument("--stop", action="store_true", help="Stop background daemon")
    p_gateway.add_argument("--status", action="store_true", help="Check gateway status")
    p_gateway.add_argument("--gen-runit", type=str, default=None, help="Generate runit service script")
    p_gateway.set_defaults(func=cmd_gateway)

    args = parser.parse_args()
    setup_logging(args.verbose)
    args.func(args)


if __name__ == "__main__":
    main()
