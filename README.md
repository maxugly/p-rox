# proxyCollector

A high-performance Python module and CLI tool for acquiring, caching, parsing, normalizing, and deduplicating proxy lists across 9+ upstream repositories, 52+ feeds, and 2.2M+ unique proxies.

Designed as an independent, reusable library that can be imported anywhere, with native integration hooks for `loadDowner`'s proxy rotation and health monitoring pool.

---

## Features

- **Multi-Source Acquisition Engine**: Concurrent streaming downloader with TTL caching, atomic file writes (`.tmp` -> replace), connect/read timeouts, and network failure resilience.
- **52 Verified Feeds**: Built-in source catalog based on live verified endpoints across Proxifly, ProxyScrape, hideip.me, TheSpeedX, monosans, jetkai, gfpcom, roosterkid, and vakhov.
- **Multi-Format Parsers**: High-throughput parsers supporting:
  - Scheme-prefixed URI lines (`scheme://host:port`, `scheme://user:pass@host:port`)
  - Bare `host:port` lines with protocol hints
  - Colon-delimited metadata (`host:port:Country`)
  - Rich JSON arrays with geolocation and credentials (e.g. `monosans/proxy-list`)
  - JSON objects keyed by protocol (e.g. `jetkai/proxy-list`)
  - JSON flag-based schemas (e.g. `vakhov/fresh-proxy-list`)
  - Tunnel protocols (`vless://`, `vmess://`, `trojan://`, `ss://`, `ssr://`, `tg://`, `hy2://`, `tuic://`, `wireguard://`)
  - HTML `<br>` multi-URI concatenation splitting
  - Feed URL link filtering (automatically drops upstream repo links)
- **Fast Deduplication**: Canonical normalization (lowercased schemes and hostnames, validated ports, preserved tunnel UUIDs/parameters, credentials preserved) deduplicating 2.2M+ lines in ~18 seconds.
- **Protocol Filtering**: Collect all protocols, or restrict to standard web proxies (`--web-only` for HTTP, HTTPS, SOCKS4, SOCKS5), or specific custom subsets.
- **Direct Bridge to `loadDowner`**: Direct `ingest_into_pool(pool)` method that seamlessly populates `ProxyPool` instances.

---

## Installation

```bash
# In your virtual environment
pip install -e .
# Or with PySocks for SOCKS proxy testing
pip install -e ".[socks]"
```

---

## CLI Usage

The package installs the `proxy-collector` command-line utility:

### 1. Show Cache and Catalog Status
```bash
proxy-collector stats
```

### 2. Fetch / Refresh Feeds
Downloads all 52 feeds in parallel with configurable concurrency and cache TTL:
```bash
# Uses cached files if within TTL (24h)
proxy-collector fetch

# Force re-download all feeds
proxy-collector fetch --force --concurrency 12
```

### 3. Collect and Deduplicate Proxies
Parses all cached sources, deduplicates, and saves to file:
```bash
# Collect all unique proxies (web + tunnels)
proxy-collector collect -o all_proxies.txt

# Collect only standard web proxies (HTTP, HTTPS, SOCKS4, SOCKS5)
proxy-collector collect -o web_proxies.txt --web-only

# Filter specific protocols
proxy-collector collect -o socks.txt --protocol socks4,socks5

# Refresh feeds before collecting
proxy-collector collect -o all_proxies.txt --fetch
```

---

## Python API

### Basic Collection
```python
from proxy_collector import ProxyCollector

collector = ProxyCollector()

# Download feeds if stale, or use cache
collector.fetch(force=False)

# Collect deduplicated web proxies (HTTP, HTTPS, SOCKS4, SOCKS5)
proxies = collector.collect(web_only=True, limit=1000)

for p in proxies:
    print(p.scheme, p.host, p.port, p.url, p.requests_url)
```

### Streaming Processing (Low Memory)
```python
from proxy_collector import ProxyCollector

collector = ProxyCollector()

# Memory-efficient streaming generator over 2M+ proxies
for proxy in collector.collect_stream(protocols=["socks5"]):
    process_proxy(proxy)
```

### Export to File with Full Statistics
```python
from proxy_collector import ProxyCollector

collector = ProxyCollector()
stats = collector.collect_to_file("all_proxies.txt", sort=True)

print(f"Parsed {stats.total_lines_parsed:,} lines")
print(f"Unique saved: {stats.unique_entries:,}")
print(f"Dropped {stats.duplicates_dropped:,} duplicates")
print("Breakdown:", stats.by_protocol)
```

### Integration with `loadDowner` ProxyPool
```python
from load_downer.proxies import ProxyPool
from proxy_collector import ProxyCollector

# Initialize loadDowner pool
pool = ProxyPool(refresh=False)

# Ingest fresh, deduplicated web proxies directly into the pool
collector = ProxyCollector()
added = collector.ingest_into_pool(pool, limit=10000, protocols={"http", "https", "socks5"})
print(f"Added {added} proxies into ProxyPool")
```

---

## Verification & Performance

Running on the full 90 MB raw download corpus (63 cached sources):

| Metric | Result |
| :--- | :--- |
| **Total Lines Parsed** | 2,273,962 |
| **Unique Proxies Saved** | 2,203,428 |
| **Duplicates Dropped** | 70,534 |
| **Execution Time** | ~18.5 seconds |

### Protocol Breakdown
- `http`: 551,644
- `socks5`: 506,117
- `https`: 503,568
- `socks4`: 489,075
- `vless`: 94,171
- `trojan`: 24,368
- `vmess`: 14,743
- `ss`: 13,955
- `tg`: 2,666
- `hy2`: 2,463
- `ssr`: 302
- `tuic`: 261
- `wireguard`: 94
- `hysteria2`: 1
