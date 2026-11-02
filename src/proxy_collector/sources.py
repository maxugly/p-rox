"""Catalog and registry of verified proxy list feeds."""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set

from .models import Feed


PROTOCOL_HINTS: Dict[str, str] = {
    "http": "http",
    "https": "https",
    "socks4": "socks4",
    "socks4a": "socks4a",
    "socks5": "socks5",
    "socks5h": "socks5",
    "connect": "http",
    "vless": "vless",
    "vmess": "vmess",
    "trojan": "trojan",
    "ss": "ss",
    "ssr": "ssr",
    "tg": "tg",
    "hy2": "hy2",
    "tuic": "tuic",
    "wireguard": "wireguard",
}


def infer_format_and_scheme(url: str, cached_as: Optional[str] = None, format_hint: Optional[str] = None) -> tuple[str, Optional[str]]:
    """Infer internal parser format and default protocol scheme."""
    fmt = (format_hint or "").lower()
    name = (cached_as or url.rsplit("/", 1)[-1]).lower()

    # Determine default scheme
    default_scheme = None
    for part in reversed(name.replace(".", "_").split("_")):
        if part in PROTOCOL_HINTS:
            default_scheme = PROTOCOL_HINTS[part]
            break

    # Determine format
    if "json array" in fmt or (name.endswith(".json") and "monosans" in name):
        return "json_array", default_scheme
    if "keyed by protocol" in fmt or (name.endswith(".json") and "jetkai" in name):
        return "json_dict", default_scheme
    if "flag" in fmt or (name.endswith(".json") and "vakhov" in name):
        return "json_flags", default_scheme
    if "country" in fmt:
        return "host_port_country", default_scheme
    if "uri" in fmt or "scheme://" in fmt or "://" in fmt:
        return "scheme_prefixed", default_scheme
    if "no protocol" in fmt or "skipped" in fmt:
        return "unsupported", None
    if "host:port" in fmt or name.endswith(".txt"):
        return "host_port", default_scheme

    return "auto", default_scheme


def load_catalog(json_path: Optional[str | Path] = None) -> List[Feed]:
    """Load proxy sources and feeds catalog from lists.json or search standard paths."""
    candidates = []
    if json_path:
        candidates.append(Path(json_path))
    else:
        # Search cwd, package dir, or parent dirs
        candidates.extend([
            Path("lists.json"),
            Path(__file__).parent / "lists.json",
            Path(__file__).parent.parent.parent / "lists.json",
        ])

    target_path = None
    for cand in candidates:
        if cand.is_file():
            target_path = cand
            break

    feeds: List[Feed] = []
    if target_path and target_path.is_file():
        try:
            with open(target_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for src in data.get("sources", []):
                src_name = src.get("name", "unknown")
                is_stale = "stale" in src.get("status", "").lower()
                for f in src.get("feeds", []):
                    url = f.get("url")
                    if not url:
                        continue
                    cached_as = f.get("cached_as")
                    fmt_hint = f.get("format", "")
                    fmt, default_scheme = infer_format_and_scheme(url, cached_as, fmt_hint)

                    # Determine tags
                    tags: List[str] = [src_name.lower().split("/")[0]]
                    if is_stale:
                        tags.append("stale")
                    if default_scheme:
                        tags.append(default_scheme)
                        if default_scheme in {"http", "https", "socks4", "socks5", "socks4a", "socks5h"}:
                            tags.append("web")
                        else:
                            tags.append("tunnel")
                    elif "vless" in fmt_hint or "vmess" in fmt_hint or "trojan" in fmt_hint:
                        tags.extend(["tunnel", "uri"])

                    enabled = fmt != "unsupported" and not (cached_as and cached_as.startswith("("))
                    feeds.append(
                        Feed(
                            url=url,
                            source_name=src_name,
                            format=fmt,
                            default_scheme=default_scheme,
                            cached_as=cached_as,
                            expected_entries=f.get("entries"),
                            enabled=enabled,
                            tags=tags,
                        )
                    )
            if feeds:
                return feeds
        except Exception:
            pass

    return list(DEFAULT_FEEDS)


# Fallback verified feed list if lists.json is unavailable
DEFAULT_FEEDS: List[Feed] = [
    Feed(
        url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt",
        source_name="Proxifly",
        format="scheme_prefixed",
        cached_as="proxifly_all.txt",
        tags=["proxifly", "web", "all"],
    ),
    Feed(
        url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
        source_name="Proxifly",
        format="scheme_prefixed",
        default_scheme="http",
        cached_as="proxifly_http.txt",
        tags=["proxifly", "web", "http"],
    ),
    Feed(
        url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt",
        source_name="Proxifly",
        format="scheme_prefixed",
        default_scheme="https",
        cached_as="proxifly_https.txt",
        tags=["proxifly", "web", "https"],
    ),
    Feed(
        url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt",
        source_name="Proxifly",
        format="scheme_prefixed",
        default_scheme="socks4",
        cached_as="proxifly_socks4.txt",
        tags=["proxifly", "web", "socks4"],
    ),
    Feed(
        url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
        source_name="Proxifly",
        format="scheme_prefixed",
        default_scheme="socks5",
        cached_as="proxifly_socks5.txt",
        tags=["proxifly", "web", "socks5"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/ProxyScrape/free-proxy-list/main/proxies/all/data.txt",
        source_name="ProxyScrape",
        format="scheme_prefixed",
        cached_as="proxyscrape_all.txt",
        tags=["proxyscrape", "web", "all"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/ProxyScrape/free-proxy-list/main/proxies/protocols/http/data.txt",
        source_name="ProxyScrape",
        format="scheme_prefixed",
        default_scheme="http",
        cached_as="proxyscrape_http.txt",
        tags=["proxyscrape", "web", "http"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/ProxyScrape/free-proxy-list/main/proxies/protocols/https/data.txt",
        source_name="ProxyScrape",
        format="scheme_prefixed",
        default_scheme="https",
        cached_as="proxyscrape_https.txt",
        tags=["proxyscrape", "web", "https"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/ProxyScrape/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        source_name="ProxyScrape",
        format="scheme_prefixed",
        default_scheme="socks4",
        cached_as="proxyscrape_socks4.txt",
        tags=["proxyscrape", "web", "socks4"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/ProxyScrape/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        source_name="ProxyScrape",
        format="scheme_prefixed",
        default_scheme="socks5",
        cached_as="proxyscrape_socks5.txt",
        tags=["proxyscrape", "web", "socks5"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json",
        source_name="monosans/proxy-list",
        format="json_array",
        cached_as="monosans_proxies.json",
        tags=["monosans", "web", "json", "geo"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/json/proxies.json",
        source_name="jetkai/proxy-list",
        format="json_dict",
        cached_as="jetkai_proxies.json",
        tags=["jetkai", "web", "json"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
        source_name="TheSpeedX/PROXY-List",
        format="host_port",
        default_scheme="socks5",
        cached_as="thespeedx_socks5.txt",
        tags=["thespeedx", "web", "socks5"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
        source_name="TheSpeedX/PROXY-List",
        format="host_port",
        default_scheme="socks4",
        cached_as="thespeedx_socks4.txt",
        tags=["thespeedx", "web", "socks4"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
        source_name="TheSpeedX/PROXY-List",
        format="host_port",
        default_scheme="http",
        cached_as="thespeedx_http.txt",
        tags=["thespeedx", "web", "http"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
        source_name="zloi-user/hideip.me",
        format="host_port_country",
        default_scheme="http",
        cached_as="hideip_http.txt",
        tags=["hideip", "web", "http"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt",
        source_name="zloi-user/hideip.me",
        format="host_port_country",
        default_scheme="https",
        cached_as="hideip_https.txt",
        tags=["hideip", "web", "https"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt",
        source_name="zloi-user/hideip.me",
        format="host_port_country",
        default_scheme="socks4",
        cached_as="hideip_socks4.txt",
        tags=["hideip", "web", "socks4"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
        source_name="zloi-user/hideip.me",
        format="host_port_country",
        default_scheme="socks5",
        cached_as="hideip_socks5.txt",
        tags=["hideip", "web", "socks5"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/zloi-user/hideip.me/main/connect.txt",
        source_name="zloi-user/hideip.me",
        format="host_port_country",
        default_scheme="http",
        cached_as="hideip_connect.txt",
        tags=["hideip", "web", "http"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/http.txt",
        source_name="gfpcom/free-proxy-list",
        format="scheme_prefixed",
        default_scheme="http",
        cached_as="gfp_http.txt",
        tags=["gfpcom", "web", "http"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/https.txt",
        source_name="gfpcom/free-proxy-list",
        format="scheme_prefixed",
        default_scheme="https",
        cached_as="gfp_https.txt",
        tags=["gfpcom", "web", "https"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks4.txt",
        source_name="gfpcom/free-proxy-list",
        format="scheme_prefixed",
        default_scheme="socks4",
        cached_as="gfp_socks4.txt",
        tags=["gfpcom", "web", "socks4"],
    ),
    Feed(
        url="https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks5.txt",
        source_name="gfpcom/free-proxy-list",
        format="scheme_prefixed",
        default_scheme="socks5",
        cached_as="gfp_socks5.txt",
        tags=["gfpcom", "web", "socks5"],
    ),
]
