"""Robust, high-throughput parsers for all proxy list formats."""

import ipaddress
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import unquote, urlsplit

from .models import (
    ALL_SUPPORTED_SCHEMES,
    STANDARD_SCHEMES,
    TUNNEL_SCHEMES,
    Feed,
    Proxy,
)

IPV4_RE = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")
HOST_RE = re.compile(r"^[A-Za-z0-9._-]{1,253}$")
BR_SPLIT_RE = re.compile(r"(?i)<br[^>]*>|%3cbr[^%]*%3e|<br/[^>]*>")


def is_valid_ipv4(host: str) -> bool:
    """Fast IPv4 validation without exception overhead."""
    m = IPV4_RE.match(host)
    if not m:
        return False
    return all(0 <= int(g) <= 255 for g in m.groups())


def is_valid_host(host: str) -> bool:
    """Validate host string (IPv4, IPv6, or valid hostname)."""
    if not host:
        return False
    if is_valid_ipv4(host):
        return True
    if host.startswith("[") and host.endswith("]"):
        try:
            ipaddress.IPv6Address(host[1:-1])
            return True
        except ValueError:
            return False
    if HOST_RE.match(host) and ("." in host or host.lower() == "localhost"):
        return True
    return False


def is_feed_url(line: str) -> bool:
    """Check if line is an upstream feed URL (e.g. gfpcom upstream repo links) rather than a proxy."""
    if not line.lower().startswith(("http://", "https://")):
        return False
    parts = urlsplit(line)
    return bool(parts.path and parts.path != "/")


def _parse_single_entry(
    line: str,
    default_scheme: Optional[str] = None,
    source_name: Optional[str] = None,
) -> Iterator[Proxy]:
    """Parse a single, non-delimited proxy line without recursion."""
    if not line or line.startswith("#") or line.startswith("//"):
        return

    if is_feed_url(line):
        return

    # Check for URI scheme prefix
    if "://" in line:
        scheme_candidate, rest = line.split("://", 1)
        scheme_candidate = scheme_candidate.lower().strip()

        if scheme_candidate in TUNNEL_SCHEMES:
            # Tunnel protocol URI (vless, vmess, trojan, ss, etc.)
            host = ""
            port = 0
            # Try to extract host:port for index / metrics
            try:
                # Format: uuid@host:port?query#frag
                after_at = rest.split("@", 1)[-1]
                host_port_part = after_at.split("?", 1)[0].split("/", 1)[0].split("#", 1)[0]
                if ":" in host_port_part:
                    h, p = host_port_part.rsplit(":", 1)
                    if p.isdigit():
                        host = h
                        port = int(p)
            except Exception:
                pass

            yield Proxy(
                scheme=scheme_candidate,
                host=host or "unknown",
                port=port or 0,
                raw_uri=line,
                source=source_name,
            )
            return

        if scheme_candidate in STANDARD_SCHEMES or scheme_candidate == "socks":
            scheme = "socks5" if scheme_candidate == "socks" else scheme_candidate
            # Parse standard user:pass@host:port
            auth_and_host = rest.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
            username = None
            password = None
            if "@" in auth_and_host:
                auth_part, host_port_part = auth_and_host.rsplit("@", 1)
                if ":" in auth_part:
                    u, p = auth_part.split(":", 1)
                    username, password = unquote(u), unquote(p)
                else:
                    username = unquote(auth_part)
            else:
                host_port_part = auth_and_host

            if ":" in host_port_part:
                h, p = host_port_part.rsplit(":", 1)
                h, p = h.strip(), p.strip()
                if p.isdigit() and (0 < int(p) < 65536) and is_valid_host(h):
                    yield Proxy(
                        scheme=scheme,
                        host=h,
                        port=int(p),
                        username=username,
                        password=password,
                        source=source_name,
                    )
            return

    # Non-URI line: check host:port or host:port:Country
    parts = line.split(":")
    if len(parts) >= 2:
        host, port = parts[0].strip(), parts[1].strip()
        country = ":".join(parts[2:]).strip() if len(parts) >= 3 else None

        if port.isdigit() and (0 < int(port) < 65536) and is_valid_host(host):
            scheme = (default_scheme or "http").lower()
            if scheme == "socks":
                scheme = "socks5"
            yield Proxy(
                scheme=scheme,
                host=host,
                port=int(port),
                country=country if country else None,
                source=source_name,
            )


def parse_line(
    raw_line: str,
    default_scheme: Optional[str] = None,
    source_name: Optional[str] = None,
) -> Iterator[Proxy]:
    """Parse a raw line, handling HTML breaks or concatenated entries, yielding Proxy objects."""
    line = raw_line.strip()
    if not line:
        return

    # Check for multi-URI lines concatenated with <br> or %3cbr
    if "<br" in line.lower() or "%3cbr" in line.lower():
        subbed = BR_SPLIT_RE.sub("\n", line)
        for part in subbed.splitlines():
            part = part.strip()
            if part:
                yield from _parse_single_entry(part, default_scheme, source_name)
        return

    yield from _parse_single_entry(line, default_scheme, source_name)


def parse_json_content(
    content: str,
    default_scheme: Optional[str] = None,
    source_name: Optional[str] = None,
) -> Iterator[Proxy]:
    """Parse JSON proxy data across all known JSON schema variants."""
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        # Fallback to line-by-line text parsing
        for line in content.splitlines():
            yield from parse_line(line, default_scheme, source_name)
        return

    # Variant 1: Jetkai style - dict keyed by protocol {"http": ["host:port", ...], "socks5": [...]}
    if isinstance(data, dict) and not any(isinstance(v, (str, int, float)) for v in data.values()):
        for proto, entries in data.items():
            scheme = str(proto).lower()
            if scheme not in ALL_SUPPORTED_SCHEMES:
                scheme = default_scheme or "http"
            if isinstance(entries, list):
                for item in entries:
                    yield from parse_line(str(item), default_scheme=scheme, source_name=source_name)
        return

    # Variant 2: Monosans or Vakhov style - array of dict objects
    items = data if isinstance(data, list) else data.get("proxies", [])
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue

            host = it.get("ip") or it.get("host") or ""
            port_val = it.get("port")
            if not host or port_val is None:
                continue

            port_str = str(port_val).strip()
            if not port_str.isdigit() or not (0 < int(port_str) < 65536):
                continue
            port = int(port_str)

            if not is_valid_host(host):
                continue

            # Check explicit protocol or flag-based protocol (Vakhov)
            proto = (it.get("protocol") or "").lower()
            if not proto:
                for flag in ("http", "socks5", "socks4", "ssl"):
                    if str(it.get(flag, "0")) in ("1", "true", "True"):
                        proto = "https" if flag == "ssl" else flag
                        break

            scheme = proto if proto in ALL_SUPPORTED_SCHEMES else (default_scheme or "http")
            if scheme == "socks":
                scheme = "socks5"

            username = it.get("username")
            password = it.get("password")

            # Extract geolocation metadata if available
            country = None
            geo = it.get("geolocation")
            if isinstance(geo, dict):
                country = geo.get("country", {}).get("names", {}).get("en") or geo.get("country", {}).get("iso_code")
            elif it.get("country_name"):
                country = it.get("country_name")
            elif it.get("country_code"):
                country = it.get("country_code")

            extra = {}
            for field_name in ("asn", "timeout", "exit_ip", "delay", "anon"):
                if field_name in it:
                    extra[field_name] = it[field_name]

            yield Proxy(
                scheme=scheme,
                host=host,
                port=port,
                username=username or None,
                password=password or None,
                country=country or None,
                anonymity=str(it.get("anon")) if "anon" in it else None,
                source=source_name,
                extra=extra,
            )


def parse_file(
    file_path: str | Path,
    feed: Optional[Feed] = None,
    default_scheme: Optional[str] = None,
    source_name: Optional[str] = None,
) -> Iterator[Proxy]:
    """Parse a file streaming line-by-line or via JSON parser."""
    path = Path(file_path)
    if not path.is_file():
        return

    scheme = default_scheme or (feed.default_scheme if feed else None)
    src_name = source_name or (feed.source_name if feed else None)
    is_json = path.suffix.lower() == ".json" or (feed and "json" in feed.format)

    if is_json:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                content = fh.read()
            yield from parse_json_content(content, default_scheme=scheme, source_name=src_name)
            return
        except Exception:
            pass

    # Line-by-line streaming parse
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            yield from parse_line(line, default_scheme=scheme, source_name=src_name)
