"""Terminal dashboard rendering and ANSI layout formatters."""

import sys
import time
from typing import Any, Dict, List, Optional


class TerminalDashboard:
    """Formats and prints live terminal status matrices for slots, pools, and profiles."""

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
