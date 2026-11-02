#!/usr/bin/env python3
"""Normalize every downloaded source in sources/ into one deduplicated proxy list."""

import json
import os
import re
from collections import Counter
from urllib.parse import urlsplit

SOURCES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sources")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_proxies.txt")

URI_SCHEMES = (
    "socks4", "socks4a", "socks5", "socks5h", "http", "https", "socks",
    "ss", "ssr", "vmess", "vless", "trojan", "tg", "hy", "hy2", "hysteria",
    "hysteria2", "tuic", "wireguard", "anytls", "reality", "direct",
)
IPV4 = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
HOST_RE = re.compile(r"^[A-Za-z0-9._-]{1,253}$")
PROTOCOL_HINTS = {
    "http": "http", "https": "https", "socks4": "socks4",
    "socks4a": "socks4a", "socks5": "socks5", "all": "", "connect": "http",
    "combined": "", "proxies": "",
}


def hint_for(filename):
    stem = filename.rsplit(".", 1)[0]
    for part in reversed(stem.split("_")):
        if part in PROTOCOL_HINTS:
            return PROTOCOL_HINTS[part]
    return ""


def entry_from_hostport(line, default_scheme):
    parts = line.split(":")
    if len(parts) < 2:
        return None
    host, port = parts[0].strip(), parts[1].strip()
    if not port.isdigit() or not (0 < int(port) < 65536):
        return None
    if not (IPV4.match(host) or HOST_RE.match(host)):
        return None
    if "." not in host and not IPV4.match(host):
        return None
    body = f"{host}:{port}"
    return f"{default_scheme}://{body}" if default_scheme else body


def is_feed_url(line):
    """Upstream list URLs (gfpcom 'sources' files) are links to other lists, not proxies."""
    if not line.lower().startswith(("http://", "https://")):
        return False
    parts = urlsplit(line)
    return bool(parts.path and parts.path != "/")


def parse_text(path, scheme_hint):
    out = []
    for raw in open(path, encoding="utf-8", errors="replace"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if is_feed_url(line):
            continue
        low = line.lower()
        if any(low.startswith(f"{s}://") for s in URI_SCHEMES):
            out.append(line)
            continue
        e = entry_from_hostport(line, scheme_hint)
        if e and scheme_hint:
            out.append(e)
    return out


def parse_json(path):
    out = []
    try:
        data = json.load(open(path, encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return parse_text(path, "")
    if isinstance(data, dict) and not any(
        isinstance(v, (str, int, float)) for v in data.values()
    ):
        for proto, items in data.items():
            for hp in items or []:
                e = entry_from_hostport(str(hp), str(proto).lower())
                if e:
                    out.append(e)
        return out
    items = data if isinstance(data, list) else data.get("proxies", [])
    for it in items:
        if not isinstance(it, dict):
            continue
        host = it.get("ip") or it.get("host") or ""
        port = it.get("port") or ""
        if not host or not str(port).isdigit():
            continue
        proto = (it.get("protocol") or "").lower()
        if not proto:
            for flag in ("http", "socks5", "socks4", "ssl"):
                if str(it.get(flag, "0")) in ("1", "true", "True"):
                    proto = "https" if flag == "ssl" else flag
                    break
        userinfo = ""
        if it.get("username"):
            userinfo = f"{it['username']}:{it.get('password') or ''}@"
        scheme = proto if proto in URI_SCHEMES else ""
        out.append(f"{scheme}://{userinfo}{host}:{port}" if scheme else f"{host}:{port}")
    return out


def main():
    entries = []
    per_source = {}
    for name in sorted(os.listdir(SOURCES)):
        path = os.path.join(SOURCES, name)
        if not os.path.isfile(path):
            continue
        if name.endswith(".json"):
            lines = parse_json(path)
        else:
            lines = parse_text(path, hint_for(name))
        per_source[name] = len(lines)
        entries.extend(lines)

    seen = set()
    unique = []
    for e in entries:
        key = e.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)

    def sort_key(e):
        scheme = e.split("://", 1)[0] if "://" in e else "zzz"
        return (scheme, e.lower())
    unique.sort(key=sort_key)
    with open(OUTPUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(unique) + "\n")

    by_scheme = Counter(e.split("://", 1)[0] if "://" in e else "none" for e in unique)
    return per_source, by_scheme, len(entries), len(unique)


if __name__ == "__main__":
    per_source, by_scheme, total, unique = main()
    print(f"{'source':32} {'entries':>9}")
    for name, count in per_source.items():
        print(f"{name:32} {count:>9}")
    print(f"\ntotal parsed: {total}  unique: {unique}\n")
    print(f"{'protocol':14} {'count':>9}")
    for scheme, count in by_scheme.most_common():
        print(f"{scheme:14} {count:>9}")
