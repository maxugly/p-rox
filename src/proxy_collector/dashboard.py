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
    """Format duration in seconds into human-readable compact string."""
    s = int(max(0, seconds))
    if s < 60:
        return f"{s}s"
    elif s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}m {sec:02d}s"
    else:
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h}h {m:02d}m"


def format_uptime(seconds: float) -> str:
    """Format uptime seconds into HH:MM:SS format."""
    s = int(max(0, seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


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
    """Format list of (country_code, count) into multiple wrapped box lines with flags and percentages."""
    if not countries:
        return [_box_line("Countries: (Waiting for verified proxies...)", width)]

    total_pool = total_alive if (total_alive is not None and total_alive > 0) else sum(c[1] for c in countries)
    total_countries = len(countries)
    prefix_first = f"Countries ({total_countries}): "
    prefix_next = " " * len(prefix_first)
    inner_width = max(20, width - 4)  # space inside │ ... │

    lines: List[str] = []
    current_items: List[str] = []
    is_first = True

    for code, cnt in countries:
        flag = country_code_to_flag(code)
        flag_str = f"{flag} " if flag else ""
        if total_pool > 0:
            pct = (cnt / total_pool) * 100
            pct_str = f"{pct:.1f}%" if (pct % 1 != 0) else f"{int(pct)}%"
            item = f"{flag_str}{code}: {cnt} ({pct_str})"
        else:
            item = f"{flag_str}{code}: {cnt}"

        prefix = prefix_first if is_first else prefix_next
        test_line = prefix + " │ ".join(current_items + [item])

        if visible_len(test_line) <= inner_width:
            current_items.append(item)
        else:
            if current_items:
                line_str = prefix + " │ ".join(current_items)
                lines.append(_box_line(line_str, width))
                current_items = [item]
                is_first = False
            else:
                lines.append(_box_line(test_line, width))
                current_items = []
                is_first = False

            if len(lines) >= max_lines:
                break

    if current_items and len(lines) < max_lines:
        prefix = prefix_first if is_first else prefix_next
        line_str = prefix + " │ ".join(current_items)
        lines.append(_box_line(line_str, width))

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
) -> List[str]:
    """Render the 6-line sticky status box for the top of the terminal."""
    if width is None:
        cols, _ = shutil.get_terminal_size(fallback=(80, 24))
        width = max(70, min(cols, 120))

    title = " p-rox pool monitor "
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
        _box_line(line3, width),
    ]
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

    @staticmethod
    def render_gateway_dashboard(
        slots: List[Any],
        host: str,
        target_site: Optional[str] = None,
        pool_stats: Optional[Dict[str, int]] = None,
    ) -> str:
        """Render ANSI live dashboard for local gateway slots."""
        lines = []
        # Clear screen & cursor to home
        lines.append("\033[2J\033[H")
        lines.append(f"\033[1;36m=== p-rox Local Proxy Gateway ({len(slots)} slots on {host}) ===\033[0m")
        lines.append(f"Target Site: \033[1m{target_site or 'Universal / All Sites'}\033[0m")
        if pool_stats:
            p_str = " | ".join(f"{k}: {v}" for k, v in pool_stats.items())
            lines.append(f"Pool Summary: {p_str}")

        lines.append("")
        header = (
            f"\033[1m{'Slot':<5} {'Port':<7} {'Status':<9} {'Location':<14} "
            f"{'Anon':<7} {'Ping (TCP/TTFB)':<18} {'Active':<7} {'KB':<8} {'Upstream'}\033[0m"
        )
        lines.append(header)
        lines.append("-" * 92)

        for s in slots:
            stats = s.stats
            status_color = "\033[32m" if stats.status == "healthy" else "\033[33m"
            status_str = f"{status_color}{stats.status:<9}\033[0m"

            loc = stats.country or "N/A"
            if len(loc) > 13:
                loc = loc[:12] + "…"

            anon = getattr(stats, "anonymity", "unknown")[:6]
            tcp = getattr(stats, "tcp_ms", None)
            ttfb = getattr(stats, "ttfb_ms", None)

            if tcp and tcp != float("inf"):
                ping_str = f"{int(tcp)}ms / {int(ttfb)}ms" if ttfb and ttfb != float("inf") else f"{int(tcp)}ms TCP"
            else:
                ping_str = "- / -"

            kb = (stats.bytes_received + stats.bytes_sent) / 1024
            upstream = stats.upstream or "None (idle)"
            if len(upstream) > 28:
                upstream = upstream[:27] + "…"

            line = (
                f"{s.slot_id:<5} {s.port:<7} {status_str} {loc:<14} "
                f"{anon:<7} {ping_str:<18} {stats.connections_active:<7} {kb:<8.1f} {upstream}"
            )
            lines.append(line)

        lines.append("-" * 92)
        lines.append("\033[2mPress Ctrl+C to stop.\033[0m\n")
        return "\n".join(lines)

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
