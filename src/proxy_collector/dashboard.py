"""Terminal dashboard rendering and ANSI layout formatters."""

import re
import shutil
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

ANSI_REGEX = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def visible_len(text: str) -> int:
    """Return visible character width ignoring ANSI escape codes."""
    return len(ANSI_REGEX.sub("", text))


def format_duration(seconds: float) -> str:
    """Format duration in seconds into human-readable compact string down to the second."""
    s = int(max(0, seconds))
    if s < 60:
        return f"{s}s"
    elif s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}m {sec:02d}s"
    else:
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h}h {m:02d}m {sec:02d}s"


def format_bytes(num_bytes: float) -> str:
    """Format bytes into compact human-readable string (B, KB, MB, GB)."""
    b = float(max(0, num_bytes))
    if b < 1024:
        return f"{int(b)}B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.1f}KB"
    elif b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.1f}MB"
    else:
        return f"{b / (1024 * 1024 * 1024):.2f}GB"


def format_rate(bps: float) -> str:
    """Format transfer speed in bytes per second."""
    return f"{format_bytes(bps)}/s"


def format_uptime(seconds: float) -> str:
    """Format uptime seconds into HH:MM:SS format."""
    s = int(max(0, seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def format_compact_num(num: int) -> str:
    """Format large numbers into compact strings (.24M, 45k, 42, 0)."""
    n = int(max(0, num))
    if n >= 100_000:
        val = f"{n / 1_000_000:.2f}"
        if val.startswith("0."):
            val = val[1:]
        elif val.endswith(".00"):
            val = val[:-3]
        return f"{val}M"
    elif n >= 1_000:
        val = f"{n / 1_000:.1f}"
        if val.endswith(".0"):
            val = val[:-2]
        return f"{val}k"
    else:
        return str(n)


ISO_COUNTRY_CODES: Tuple[str, ...] = (
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
    "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS",
    "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN",
    "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE",
    "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF",
    "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM",
    "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM",
    "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC",
    "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK",
    "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA",
    "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG",
    "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW",
    "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS",
    "ST", "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO",
    "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI",
    "VN", "VU", "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW"
)


def country_code_to_flag(code: Optional[str]) -> str:
    """Convert a 2-letter ISO country code into Unicode flag emoji.

    Uses Regional Indicator Symbols U+1F1E6 to U+1F1FF.
    Zero external dependencies.
    """
    if not code or len(code) != 2 or not code.isalpha():
        return ""
    return "".join(chr(0x1F1E6 + ord(c.upper()) - ord("A")) for c in code)


def format_country_lines(
    countries: List[Tuple[str, int]],
    width: int,
    total_alive: Optional[int] = None,
    max_lines: int = 10,
) -> List[str]:
    """Format list of (country_code, count) into multiple wrapped box lines with flags and percentages.

    Places 'Countries (x):' on its own header line, followed by wrapped country lines.
    """
    total_countries = len(countries)
    header_line = f"Countries ({total_countries}):"

    if not countries:
        return [
            _box_line(header_line, width),
            _box_line("(Waiting for verified proxies...)", width),
        ]

    total_pool = total_alive if (total_alive is not None and total_alive > 0) else sum(c[1] for c in countries)
    inner_width = max(20, width - 4)  # space inside │ ... │

    lines: List[str] = [_box_line(header_line, width)]
    current_items: List[str] = []

    for code, cnt in countries:
        flag = country_code_to_flag(code)
        flag_str = f"{flag} " if flag else ""
        if total_pool > 0:
            pct = (cnt / total_pool) * 100
            pct_str = f"{pct:.1f}%" if (pct % 1 != 0) else f"{int(pct)}%"
            item = f"{flag_str}{code}: {cnt} ({pct_str})"
        else:
            item = f"{flag_str}{code}: {cnt}"

        test_line = "  ".join(current_items + [item])

        if visible_len(test_line) <= inner_width:
            current_items.append(item)
        else:
            if current_items:
                lines.append(_box_line("  ".join(current_items), width))
                current_items = [item]
            else:
                lines.append(_box_line(test_line, width))
                current_items = []

            if len(lines) >= max_lines:
                break

    if current_items and len(lines) < max_lines:
        lines.append(_box_line("  ".join(current_items), width))

    return lines


def _box_line(content: str, width: int) -> str:
    """Wrap content in │ borders padded or truncated to target width."""
    inner_width = max(0, width - 4)  # "│ " + content + " │"
    vlen = visible_len(content)
    if vlen > inner_width:
        plain = ANSI_REGEX.sub("", content)
        content = plain[: max(0, inner_width - 1)] + "…"
        vlen = visible_len(content)
    pad = " " * max(0, inner_width - vlen)
    return f"│ {content}{pad} │"


def render_sticky_header(
    analytics: Dict[str, Any],
    session_died: int = 0,
    uptime_s: float = 0.0,
    speed: float = 0.0,
    workers: int = 25,
    heartbeat_s: float = 180.0,
    width: Optional[int] = None,
    gateway_active: bool = False,
    sync_info: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Render the 6-line sticky status box for the top of the terminal."""
    if width is None:
        cols, _ = shutil.get_terminal_size(fallback=(80, 24))
        width = max(70, min(cols, 120))

    title = " p-rox pool monitor [s: Server View | +/-: Slots] " if gateway_active else " p-rox pool monitor "
    bar_len = max(0, width - 2 - len(title))
    top_border = f"┌─{title}{'─' * max(0, bar_len - 1)}┐" if bar_len >= 1 else f"┌{'─' * (width - 2)}┐"

    # Line 1: Session Uptime & Configuration
    line1 = (
        f"Uptime: \033[1m{format_uptime(uptime_s)}\033[0m │ "
        f"Speed: \033[1m{speed:.1f}/s\033[0m │ "
        f"Workers: \033[1m{workers}\033[0m │ "
        f"Heartbeat: \033[1m{int(heartbeat_s)}s\033[0m"
    )

    # Line 2: Pool Counts
    total = analytics.get("total", 0)
    untested = analytics.get("untested", 0)
    alive = analytics.get("alive", 0)
    dead = analytics.get("dead", 0)
    line2 = (
        f"Pool: \033[1m{total:,}\033[0m Total │ "
        f"{untested:,} Untested │ "
        f"\033[1;92m{alive} ALIVE\033[0m │ "
        f"\033[93m{dead} Dead\033[0m (\033[91m{session_died} Died\033[0m)"
    )

    # GeoIP Resolution Progress (numbers reflect active background lookups climbing/dropping)
    unresolved_geoip = analytics.get("unresolved_geoip", 0)
    resolved_geoip = analytics.get("resolved_geoip", max(0, total - unresolved_geoip))
    geoip_line = None
    if unresolved_geoip > 0:
        pct = (resolved_geoip / total * 100) if total > 0 else 0.0
        pct_str = f"{pct:.1f}%" if (pct % 1 != 0) else f"{int(pct)}%"
        geoip_line = (
            f"GeoIP Resolving: \033[1;92m{resolved_geoip:,}\033[0m mapped │ "
            f"\033[1;93m{unresolved_geoip:,} to lookup ↓\033[0m │ "
            f"\033[1m{pct_str}\033[0m"
        )

    # Feed Sync Telemetry
    s_info = sync_info or analytics.get("sync_info")
    sync_line = None
    if s_info:
        st = s_info.get("status", "idle")
        if st == "checking":
            chk = s_info.get("feeds_checked", 0)
            tot = s_info.get("feeds_total", 52)
            sync_line = f"Feed Sync: \033[1;33mChecking feeds... ({chk}/{tot})\033[0m"
        elif s_info.get("last_sync_time", 0) > 0:
            res = s_info.get("last_sync_result", "Up-to-date")
            next_s = max(0.0, s_info.get("next_sync_time", 0) - time.time())
            sync_line = f"Feed Sync: \033[1;92m{res}\033[0m │ Next in: \033[1m{format_duration(next_s)}\033[0m (u: Check now)"
        elif s_info.get("status") == "failed":
            res = s_info.get("last_sync_result", "Failed")
            sync_line = f"Feed Sync: \033[1;91m{res}\033[0m (u: Retry now)"
        else:
            sync_line = "Feed Sync: \033[2mScheduled daily (press 'u' to check now)\033[0m"

    # Line 3: Latency & Lifetimes
    avg_ping = analytics.get("avg_ping", 0.0)
    stdev_ping = analytics.get("stdev_ping", 0.0)
    avg_life = analytics.get("avg_life_s", 0.0)
    oldest_life = analytics.get("oldest_alive_s", 0.0)
    anon_dict = analytics.get("anonymity", {})
    elite_cnt = anon_dict.get("ELITE", 0)

    if alive > 0:
        ping_str = f"Ping: \033[1m{avg_ping:.0f}ms\033[0m (±{stdev_ping:.0f}ms)"
        life_str = f"Avg Life: \033[1m{format_duration(avg_life)}\033[0m"
        oldest_str = f"Oldest: \033[1m{format_duration(oldest_life)}\033[0m"
        elite_pct = int(elite_cnt / alive * 100) if alive else 0
        elite_str = f"Elite: \033[1m{elite_pct}%\033[0m ({elite_cnt})"
    else:
        ping_str = "Ping: -"
        life_str = "Avg Life: -"
        oldest_str = "Oldest: -"
        elite_str = "Elite: -"

    line3 = f"{ping_str} │ {life_str} │ {oldest_str} │ {elite_str}"

    # Country Lines (multi-line wrapping with flags and percentages)
    countries = analytics.get("countries", [])
    country_lines = format_country_lines(countries, width, total_alive=alive)

    bottom_border = f"└{'─' * (width - 2)}┘"

    header = [
        top_border,
        _box_line(line1, width),
        _box_line(line2, width),
    ]
    if geoip_line:
        header.append(_box_line(geoip_line, width))
    if sync_line:
        header.append(_box_line(sync_line, width))
    header.append(_box_line(line3, width))
    header.extend(country_lines)
    header.append(bottom_border)
    return header


class StickyTerminal:
    """Manages DECSTBM terminal scroll margins for a sticky top header with scrolling logs."""

    def __init__(self, header_lines: int = 6, enabled: Optional[bool] = None):
        self.header_lines = header_lines
        self.enabled = sys.stdout.isatty() if enabled is None else enabled
        self._initialized = False
        self._current_header_lines = header_lines

    def init_terminal(self, header_lines: Optional[int] = None):
        """Set DECSTBM scrolling margin below the sticky header."""
        if not self.enabled:
            return
        if header_lines is not None:
            self._current_header_lines = header_lines
        cols, rows = shutil.get_terminal_size(fallback=(80, 24))
        top_margin = min(self._current_header_lines + 2, max(4, rows - 4))
        sys.stdout.write(f"\033[2J\033[{top_margin};{rows}r\033[{top_margin};1H")
        sys.stdout.flush()
        self._initialized = True

    def update_header(self, lines: List[str]):
        """Redraw sticky header at top of terminal without altering log cursor."""
        if not self.enabled:
            return
        n_lines = len(lines)
        if not self._initialized:
            self.init_terminal(header_lines=n_lines)
        elif n_lines != self._current_header_lines:
            cols, rows = shutil.get_terminal_size(fallback=(80, 24))
            self._current_header_lines = n_lines
            top_margin = min(n_lines + 2, max(4, rows - 4))
            sys.stdout.write(f"\033[{top_margin};{rows}r")

        sys.stdout.write("\033[s")  # Save cursor
        for idx, line in enumerate(lines, start=1):
            sys.stdout.write(f"\033[{idx};1H\033[K{line}")
        sys.stdout.write("\033[u")  # Restore cursor
        sys.stdout.flush()

    def print_log(self, text: str):
        """Output a log line into the scrolling margin."""
        if not self.enabled:
            print(text)
            return
        if not self._initialized:
            self.init_terminal()
        sys.stdout.write(text + "\n")
        sys.stdout.flush()

    def restore(self):
        """Reset terminal scrolling margin and restore cursor to normal."""
        if not self.enabled or not self._initialized:
            return
        cols, rows = shutil.get_terminal_size(fallback=(80, 24))
        sys.stdout.write(f"\033[r\033[{rows};1H\n")
        sys.stdout.flush()
        self._initialized = False


class TerminalDashboard:
    """Formats and prints live terminal status matrices for slots, pools, and profiles."""

    render_sticky_header = staticmethod(render_sticky_header)
    format_country_lines = staticmethod(format_country_lines)
    country_code_to_flag = staticmethod(country_code_to_flag)
    format_duration = staticmethod(format_duration)
    format_uptime = staticmethod(format_uptime)
    format_bytes = staticmethod(format_bytes)
    format_rate = staticmethod(format_rate)
    format_compact_num = staticmethod(format_compact_num)

    @staticmethod
    def render_gateway_dashboard(
        slots: List[Any],
        host: str = "127.0.0.1",
        target_site: Optional[str] = None,
        pool_stats: Optional[Dict[str, int]] = None,
        width: Optional[int] = None,
        geoip_stats: Optional[Dict[str, Any]] = None,
        sync_stats: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render ANSI live dashboard for local gateway slots."""
        ports = [s.port for s in slots]
        if ports:
            min_port, max_port = min(ports), max(ports)
            if min_port == max_port:
                port_range_str = str(min_port)
                urls_str = f"http://{host}:{min_port}"
            else:
                port_range_str = f"{min_port}..{max_port}"
                urls_str = f"http://{host}:{min_port} .. http://{host}:{max_port}"
            sample_port = min_port
        else:
            port_range_str = "None"
            urls_str = "None"
            sample_port = 3030

        lines = []
        # Clear screen & cursor to home
        lines.append("\033[2J\033[H")
        lines.append(f"\033[1;36m=== p-rox Local Proxy Gateway ({len(slots)} slots on {host}) ===\033[0m")
        lines.append("\033[2mGitHub: https://github.com/maxugly/p-rox\033[0m")
        lines.append(f"\033[1;92mConnect To Proxy:\033[0m   \033[1m{urls_str}\033[0m  (Ports: \033[1;93m{port_range_str}\033[0m)")
        lines.append(f"\033[2mExample Usage:      curl -x http://{host}:{sample_port} https://httpbin.org/ip\033[0m")
        if target_site:
            lines.append(f"Target Site:        \033[1m{target_site}\033[0m")
        if pool_stats:
            filtered = {k.title() if isinstance(k, str) else k: v for k, v in pool_stats.items() if k not in ("db_path", "db_size_mb")}
            p_str = " │ ".join(f"{k}: {v:,}" if isinstance(v, int) else f"{k}: {v}" for k, v in filtered.items())
            lines.append(f"Pool Summary:       {p_str}")

        if geoip_stats:
            unresolved = geoip_stats.get("unresolved", 0)
            resolved = geoip_stats.get("resolved", 0)
            total = geoip_stats.get("total", 0)
            is_no_geoip = geoip_stats.get("no_geoip", False)
            if is_no_geoip:
                lines.append("GeoIP:              \033[2mdisabled (--no-geoip)\033[0m")
            elif unresolved > 0:
                pct = (resolved / total * 100) if total > 0 else 0.0
                pct_str = f"{pct:.1f}%" if (pct % 1 != 0) else f"{int(pct)}%"
                lines.append(
                    f"GeoIP Resolving:    \033[1;92m{resolved:,}\033[0m mapped │ "
                    f"\033[1;93m{unresolved:,} to lookup ↓\033[0m │ "
                    f"\033[1m{pct_str}\033[0m"
                )
            elif total > 0:
                lines.append(f"GeoIP Coverage:     \033[92m100% mapped\033[0m ({resolved:,} candidates)")

        if sync_stats:
            st = sync_stats.get("status", "idle")
            if st == "checking":
                chk = sync_stats.get("feeds_checked", 0)
                tot = sync_stats.get("feeds_total", 52)
                lines.append(f"Feed Sync:          \033[1;33mChecking feeds... ({chk}/{tot})\033[0m")
            elif sync_stats.get("last_sync_time", 0) > 0:
                res = sync_stats.get("last_sync_result", "Up-to-date")
                next_s = max(0.0, sync_stats.get("next_sync_time", 0) - time.time())
                lines.append(f"Feed Sync:          \033[1;92m{res}\033[0m │ Next daily check in \033[1m{format_duration(next_s)}\033[0m (u: Check now)")
            elif sync_stats.get("status") == "failed":
                res = sync_stats.get("last_sync_result", "Failed")
                lines.append(f"Feed Sync:          \033[1;91m{res}\033[0m (u: Retry now)")
            else:
                lines.append("Feed Sync:          \033[2mScheduled daily (press 'u' to check now)\033[0m")

        lines.append("\033[2mSlot Menu: [1-0]  │  Toggle View: [s]  │  [u] Update Feeds  │  [+] Add Port  │  [-] Remove Port  │  [Ctrl+C] Stop\033[0m")
        lines.append("")
        header = (
            f"\033[1m{'Slot':<5} {'Port':<6} {'Status':<9} {'Location':<16} "
            f"{'Alive':<11} {'Speed (▲/▼)':<18} {'Traffic (▲/▼)':<18} {'Conns':<8} {'Ping (TCP/TTFB)':<16} {'Upstream'}\033[0m"
        )
        lines.append(header)
        sep_len = 118
        lines.append("─" * sep_len)

        for s in slots:
            stats = s.stats
            if hasattr(stats, "update_rates"):
                try:
                    stats.update_rates()
                except Exception:
                    pass

            if stats.status == "healthy":
                status_color = "\033[32m"
            elif stats.status == "waiting":
                status_color = "\033[38;5;208m"
            else:
                status_color = "\033[31m"
            status_str = f"{status_color}{stats.status:<9}\033[0m"

            raw_loc = stats.country or "N/A"
            c_filter_val = getattr(s, "country_filter", None) or getattr(stats, "country_filter", None)
            c_filter = c_filter_val if isinstance(c_filter_val, str) and c_filter_val else None
            code = raw_loc.split()[0].upper()[:2] if raw_loc and raw_loc != "N/A" else (c_filter or "")
            flag = country_code_to_flag(code)
            if c_filter and stats.status == "waiting":
                loc = f"[{c_filter}] {flag} (waiting)" if flag else f"[{c_filter}] waiting"
            elif c_filter:
                loc = f"[{c_filter}] {flag} {raw_loc}" if flag else f"[{c_filter}] {raw_loc}"
            else:
                loc = f"{flag} {raw_loc}" if flag else raw_loc

            vlen = visible_len(loc)
            if vlen < 16:
                loc_padded = loc + " " * (16 - vlen)
            else:
                loc_padded = loc[:15] + "…"

            # Alive Time (down to the second)
            if stats.status == "healthy":
                first_seen = getattr(stats, "first_seen_alive", None)
                last_swapped = getattr(stats, "last_swapped_at", None)
                if isinstance(first_seen, (int, float)):
                    alive_s = max(0.0, time.time() - first_seen)
                    alive_str = format_duration(alive_s)
                elif isinstance(last_swapped, (int, float)):
                    alive_s = max(0.0, time.time() - last_swapped)
                    alive_str = format_duration(alive_s)
                else:
                    alive_str = "-"
            else:
                alive_str = "-"

            bps_up = stats.bps_sent if isinstance(getattr(stats, "bps_sent", None), (int, float)) else 0.0
            bps_down = stats.bps_received if isinstance(getattr(stats, "bps_received", None), (int, float)) else 0.0
            speed_str = f"{format_rate(bps_up)} / {format_rate(bps_down)}"

            b_up = stats.bytes_sent if isinstance(getattr(stats, "bytes_sent", None), (int, float)) else 0.0
            b_down = stats.bytes_received if isinstance(getattr(stats, "bytes_received", None), (int, float)) else 0.0
            traffic_str = f"{format_bytes(b_up)} / {format_bytes(b_down)}"

            c_act = stats.connections_active if isinstance(getattr(stats, "connections_active", None), int) else 0
            c_tot = stats.connections_total if isinstance(getattr(stats, "connections_total", None), int) else 0
            conns_str = f"{c_act} ({c_tot})"

            tcp = getattr(stats, "tcp_ms", None)
            ttfb = getattr(stats, "ttfb_ms", None)
            if isinstance(tcp, (int, float)) and tcp != float("inf"):
                ping_str = f"{int(tcp)}ms / {int(ttfb)}ms" if isinstance(ttfb, (int, float)) and ttfb != float("inf") else f"{int(tcp)}ms TCP"
            else:
                ping_str = "- / -"

            upstream = stats.upstream or "none"
            if len(upstream) > 28:
                upstream = upstream[:27] + "…"

            slot_disp = str(s.slot_id + 1)
            line = (
                f"{slot_disp:<5} {s.port:<6} {status_str} {loc_padded} "
                f"{alive_str:<11} {speed_str:<18} {traffic_str:<18} {conns_str:<8} {ping_str:<16} {upstream}"
            )
            lines.append(line)

        lines.append("─" * sep_len)
        lines.append("\033[2mSelect Slot: [1-0]  │  Toggle: [s]  │  [+] Add Port  │  [-] Remove Port  │  [Ctrl+C] Stop\033[0m\n")
        return "\n".join(lines)

    @staticmethod
    def render_country_menu(
        slot_num: int,
        port: int,
        current_filter: Optional[str],
        items: List[Dict[str, Any]],
        cursor_index: int = 0,
        search_query: str = "",
        term_width: int = 100,
        term_height: int = 30,
    ) -> str:
        """Render full-screen country selection grid for slot configuration."""
        lines = []
        cur_disp = current_filter or "ANY (All Countries)"
        lines.append(f"\033[1;36m=== Slot {slot_num} (Port {port}) Country Selection ===\033[0m  \033[2m[Current: {cur_disp}]\033[0m\033[K")
        lines.append("\033[2mSelect a country to lock this slot. Testing conveyor prioritizes live proxies for selected country.\033[0m\033[K")

        q_disp = f"[{search_query}]" if search_query else "[Type country code or number]"
        is_any_cur = (cursor_index == -1)
        any_style = "\033[1;37;44m" if is_any_cur else "\033[1;33m"
        line3 = (
            f"{any_style}[  0] 🌐 ANY: All Countries (Clear Filter)\033[0m"
            f"   │   Filter: \033[1;97m{q_disp}\033[0m"
            f"   │   Matches: \033[1;32m{len(items)}\033[0m"
        )
        lines.append(line3 + "\033[K")
        sep_len = max(80, min(term_width, 140))
        lines.append("─" * sep_len + "\033[K")

        cell_width = 20
        cols = max(1, min(sep_len // cell_width, 8))
        actual_cell_width = sep_len // cols

        max_grid_rows = max(4, term_height - 7)
        total_rows = (len(items) + cols - 1) // cols if items else 0

        cursor_row = cursor_index // cols if (items and cursor_index >= 0) else 0
        scroll_row = max(0, min(cursor_row - max_grid_rows // 2, max(0, total_rows - max_grid_rows)))

        for r in range(scroll_row, min(scroll_row + max_grid_rows, total_rows)):
            row_cells = []
            for c in range(cols):
                idx = r * cols + c
                if idx < len(items):
                    it = items[idx]
                    tot_str = format_compact_num(it["total"])
                    flag = it["flag"]
                    code = it["code"]
                    alive = it["alive"]
                    core = f"[{it['index']:>3}] {flag} {code}:{alive}/{tot_str}"
                    pad = actual_cell_width - visible_len(core)
                    if pad > 0:
                        core += " " * pad

                    if idx == cursor_index and not is_any_cur:
                        cell_str = f"\033[1;37;44m{core}\033[0m"
                    elif it["tier"] == 1:
                        cell_str = f"\033[1;32m{core}\033[0m"
                    elif it["tier"] == 2:
                        cell_str = f"\033[43;30m{core}\033[0m"
                    else:
                        cell_str = f"\033[2;90m{core}\033[0m"
                    row_cells.append(cell_str)
            lines.append("".join(row_cells) + "\033[K")

        rendered_rows = min(scroll_row + max_grid_rows, total_rows) - scroll_row
        for _ in range(max(0, max_grid_rows - rendered_rows)):
            lines.append("\033[K")

        lines.append("─" * sep_len + "\033[K")
        lines.append("\033[2mNav: [↑/↓/←/→] Move  │  [Enter] Select  │  [0] Clear Filter  │  [Backspace] Edit  │  [Esc/q] Cancel\033[0m\033[K")
        return "\n".join(lines) + "\033[J"

    @staticmethod
    def print_profile_card(profile: Any):
        """Format and print a single ProxyProfile in human-readable card style."""
        p = profile.proxy
        geo = profile.geo
        lat = profile.latency
        sec = profile.security

        print(f"\n\033[1;34m=== Proxy Profile: {p.url} ===\033[0m")
        print(f"  \033[1mEntry Host:\033[0m       {p.host}:{p.port} ({p.scheme})")
        print(f"  \033[1mTrue Egress IP:\033[0m   {profile.egress_ip or 'Undetected'}")

        # Geolocation
        if geo and (geo.country_code or geo.org):
            loc_parts = [p for p in [geo.city, geo.region, geo.country_name, f"({geo.country_code})"] if p]
            loc_str = ", ".join(loc_parts)
            asn_str = f"AS{geo.asn} {geo.org}" if geo.asn else geo.org
            dc_tag = "\033[33m[Datacenter/Cloud]\033[0m" if geo.is_datacenter else "\033[32m[Residential/ISP]\033[0m"
            print(f"  \033[1mLocation:\033[0m         {loc_str}")
            print(f"  \033[1mNetwork/ASN:\033[0m      {asn_str} {dc_tag}")
            if geo.latitude and geo.longitude:
                print(f"  \033[1mCoordinates:\033[0m      {geo.latitude:.4f}, {geo.longitude:.4f}")
        else:
            print(f"  \033[1mLocation:\033[0m         Unknown (GeoIP database not loaded)")

        # Latency
        if lat:
            tcp_s = f"{lat.tcp_handshake_ms}ms" if lat.tcp_handshake_ms != float("inf") else "Failed"
            ttfb_s = f"{lat.http_ttfb_ms}ms" if lat.http_ttfb_ms != float("inf") else "Failed"
            print(f"  \033[1mTCP Handshake:\033[0m    {tcp_s} (SYN-ACK round-trip)")
            print(f"  \033[1mHTTP TTFB:\033[0m        {ttfb_s} (Time-To-First-Byte)")

        # Security & Anonymity
        if sec:
            anon_color = "\033[32m" if sec.anonymity == "elite" else ("\033[33m" if sec.anonymity == "anonymous" else "\033[31m")
            print(f"  \033[1mAnonymity Tier:\033[0m   {anon_color}{sec.anonymity.upper()}\033[0m")
            if sec.leaked_ip:
                print(f"  \033[1;31m[!] Real IP Leaked:\033[0m {sec.leaked_ip}")
            mitm_status = "\033[31m[!] Tampering / MitM Detected\033[0m" if sec.tls_tampering_detected else "\033[32mClean (Valid CA)\033[0m"
            print(f"  \033[1mTLS Integrity:\033[0m    {mitm_status}")

        print(f"  \033[1mPool Tier:\033[0m        Layer {profile.tier}")
        print(f"  \033[1mLast Result:\033[0m      {profile.last_reason or 'None'}")
        print("-" * 50)
