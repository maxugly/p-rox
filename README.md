# p-rox

[![CI](https://github.com/maxugly/p-rox/actions/workflows/ci.yml/badge.svg)](https://github.com/maxugly/p-rox/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: standard](https://img.shields.io/badge/code%20style-pep8-brightgreen.svg)](https://peps.python.org/pep-0008/)

A high-performance Python engine and CLI suite for large-scale proxy acquisition, streaming normalization, deduplication, and staged health qualification.

Built to transform millions of unreliable public proxy listings into a verified, high-throughput rotation pool ready for production scrapers, downloaders, and distributed networking clients.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph S1["1. Acquisition & Deduplication"]
        A["52 Upstream Feeds\n(Proxifly, TheSpeedX, monosans, gfpcom, etc.)"] --> B["Concurrent Streaming Downloader\n(TTL Caching, Atomic Writes)"]
        B --> C["Multi-Format Parsers\n(URIs, Bare IP:Port, Geo-JSON, Tunnels, BR-Split)"]
        C --> D["Streaming Deduplicator\n(Canonical Lowercase Host/Scheme, Preserved Auth & UUIDs)"]
    end

    subgraph S2["2. Staged Qualification Funnel"]
        D --> L0["Layer 0: Cold Candidate Pool\n(2,200,000+ Raw Deduplicated Proxies)"]
        L0 -->|"Strict 8s Budget / IP Echo\n(api.ipify.org)"| L1["Layer 1: Route-Qualified (L1)\n(Verified TCP, Egress IP Detected, ~50,000 Live)"]
        L1 -->|"Lightweight Target Request & Validator\n(robots.txt, WAF / Cloudflare Check)"| L2["Layer 2: Site-Qualified (L2)\n(Unblocked on Target Site, Latency Logged, ~5,000 Ready)"]
        L2 -->|"Production Traffic via RoutedSession\n(Streamed Download Success)"| L3["Layer 3: Live-Stream Qualified (L3)\n(Throughput Verified, Top Rank Priority)"]
    end

    subgraph S3["3. Execution & Leasing"]
        L2 & L3 --> E["ProxyLeases\n(Mutual Exclusion by Egress Identity)"]
        E --> F["RoutedSession / Worker Downloader\n(Exclusive Per-Site Egress Locks)"]
    end
```

---

## Part 1: Acquisition, Normalization & Deduplication

Public proxy lists are notoriously noisy: mixed schemes, dead links, inconsistent formats, duplicate entries across aggregators, and malformed HTML tags. `p-rox` ingests, standardizes, and deduplicates this firehose in seconds.

### 1. Concurrent Streaming Fetcher
- **Parallel Retrieval**: Multi-threaded worker pool (`ThreadPoolExecutor`) downloading up to 52 feeds simultaneously.
- **Smart TTL Caching**: Feeds are cached locally with configurable time-to-live (default 24 hours). Re-downloads are skipped unless stale or forced.
- **Atomic Writes**: Incoming data streams directly to `.tmp` files and swaps via `os.replace`, guaranteeing zero cache corruption on network disconnects.
- **Network Resilience**: Automatic fallback to existing cached copies if an upstream endpoint times out or drops connection.

### 2. Multi-Format Streaming Parsers
The parsing engine processes feeds without loading entire multi-megabyte files into memory at once:
- **Scheme-Prefixed URIs**: `http://`, `https://`, `socks4://`, `socks5://`, `socks5h://`
- **Authenticated Proxies**: `scheme://user:pass@host:port`
- **Bare Host:Port Lines**: Automatic protocol resolution based on feed metadata and filename hints.
- **Delimited Metadata**: Colon-separated lines such as `host:port:Country` (e.g. hideip.me).
- **Rich JSON Arrays**: Object arrays containing host, port, credentials, latency, ASN, and nested geolocation (e.g. `monosans/proxy-list`).
- **Protocol-Keyed JSON**: Dict mapping protocols to host lists (e.g. `jetkai/proxy-list`).
- **Flag-Based JSON**: Dict entries with bitwise/flag protocol indicators (e.g. `vakhov/fresh-proxy-list`).
- **Modern Tunnel Protocols**: Full preservation of parameters and fragments for `vless://`, `vmess://`, `trojan://`, `ss://`, `ssr://`, `tg://`, `hy2://`, `tuic://`, and `wireguard://`.
- **Sanitization Filters**: Automatic detection and removal of upstream repository feed URLs and recursive splitting of multi-URI lines concatenated with HTML `<br>` tags.

### 3. Canonical Deduplication & Normalization
- Schemes and hostnames are normalized to lowercase.
- Ports are validated to integers within `1..65535`.
- IPv4, IPv6, and FQDN hostnames are strictly validated.
- Tunnel UUIDs, queries, and cryptographic fragments are preserved verbatim.
- Deduplication runs in a single pass with minimal memory overhead.

### Verified Benchmark & Protocol Breakdown
Evaluated on the full 90 MB cached corpus (63 source files):

| Metric | Measured Value |
| :--- | :--- |
| **Total Lines Parsed** | **2,273,962** |
| **Unique Proxies Saved** | **2,203,428** |
| **Duplicates Dropped** | **70,534** |
| **Processing Time** | **18.78 seconds** |

```
Protocol Breakdown:
  http               551,644
  socks5             506,117
  https              503,568
  socks4             489,075
  vless               94,171
  trojan              24,368
  vmess               14,743
  ss                  13,955
  tg                   2,666
  hy2                  2,463
  ssr                    302
  tuic                   261
  wireguard               94
  hysteria2                1
```

---

## Part 2: The Staged Qualification Pipeline

Public proxy repositories are **85% to 95% dead**. Without staged qualification, production scrapers burn 90% of their operational runtime waiting for 20-to-30-second socket timeouts.

The staged qualification pipeline eliminates this penalty by filtering candidates through escalating layers of validation:

```
[ Layer 0: Cold Pool ] ──(1) Route Echo──► [ Layer 1: Route-Qualified ] ──(2) Target Probe──► [ Layer 2: Site-Qualified ] ──(3) Real Traffic──► [ Layer 3: Live Elite ]
```

### Layer 0: The Cold Candidate Pool
- Contains the full inventory of 2M+ deduplicated proxies.
- **Rule**: Production scrapers and downloaders are **strictly prohibited** from dispatching traffic directly through Layer 0.

### Layer 1: Route-Qualified & Egress Probing (L1)
- **Target**: Public IP echo endpoint (`https://api.ipify.org/`).
- **Budget**: Strict 8-second total timeout (3s connect timeout, 5s read timeout).
- **Validation Criteria**:
  1. TCP connection and proxy handshake complete within 3 seconds.
  2. HTTP 200 response returned within 8 seconds.
  3. Response body parses as a valid IP: `ipaddress.ip_address(r.text.strip())`.
  4. Real egress IP is recorded and indexed.
- **TTL**: Valid for 12 hours (`checked_at > now - 43200`).
- **Effect**: Eliminates the ~90% dead socket corpses before they touch any target site.

### Layer 2: Target Site Qualification (L2)
- **Target**: Lightweight asset on the specific target domain (e.g. `https://mixkit.co/robots.txt`).
- **Prerequisite**: Candidate must have passed Layer 1 (`min_stage = 1`).
- **Validation Criteria**:
  1. HTTP 200 returned from the target (not 403 Forbidden, 429 Rate Limit, or Cloudflare challenge).
  2. Custom content validator succeeds (e.g. verifying `'user-agent'` exists in `robots.txt` to detect transparent ISP captive portals and interstitial redirectors).
  3. Latency is measured and saved.
- **TTL**: Valid for 24 hours (`max(checked_at, transfer_at) > now - 86400`).
- **Effect**: Eliminates proxies that have working internet access but are IP-banned or geoblocked by the specific target's WAF.

### Layer 3: Live Stream Qualification (L3)
- **Target**: Actual payload downloads dispatched through `RoutedSession`.
- **Validation Criteria**: Full completion of streamed content transfer without disconnect or timeout.
- **Effect**: Measures real-world transfer throughput (`bytes_per_second`). These proven routes are given highest priority (`rank = -1`) in the round-robin selector.

---

## Part 3: Failure Isolation, Quarantine & Pruning

When a proxy fails, the system executes an escalating quarantine policy rather than immediate discarding or blind retries:

| Trigger / Error Condition | Penalty & Quarantine Duration |
| :--- | :--- |
| **`407 Proxy Authentication Required` / Account Failure** | Benched for **1 hour** (`3,600s`). |
| **`429 Rate Limit` or 3 consecutive failures** | Benched for **10 minutes** (`600s`). |
| **1st or 2nd transient connection error** | Benched for **2 minutes** (`120s`). |
| **5 consecutive failures (`PRUNE_AFTER = 5`)** | Marked **`dead: True`**, benched for **7 days** (`604,800s`), and pruned. |
| **Site-Specific Failure (403/429 on target only)** | Cooldown applied **only to that target site** (`60s`); proxy remains available for other domains. |
| **Immediate Failover** | `validate(p)` instantly advances to the next live proxy so no worker idles on a benched socket. |

---

## Part 4: Identity-Aware Mutual Exclusion (`ProxyLeases`)

Many free proxy aggregators list different IP addresses or port combinations that actually terminate at the same outbound machine. Concurrently dispatching multiple requests through different ports of the same egress IP triggers rate limits on target sites.

`ProxyLeases` prevents this via identity-based mutual exclusion:

$$\text{Lease Token} = \Big(\text{site\_key}(\text{url}),\ \text{egress\_identity}\Big)$$

1. When a proxy completes Layer 1 probing, its true public outbound IP (`egress`) is recorded. If unknown, the hostname serves as the conservative identity.
2. Before any request is dispatched, the worker must claim an exclusive lease on the token:
   `busy.add((site_key, identity))`
3. The lease is held **throughout the entire response stream** and released only when the socket closes.
4. **Result**: Zero rate-limit collisions. Multiple worker threads never hit the same target site from the same physical egress IP at the same millisecond.

---

## Part 5: Design Patterns & Terminology

The architecture implements standard distributed systems design patterns:

- **Cascading Health Verification / Multi-Stage Probing Funnel**: Staging lightweight, universal checks (TCP/IP echo) ahead of heavier, site-specific checks (WAF/application verification).
- **Circuit Breaker with Exponential Quarantine**: Automatically isolating failing endpoints, escalating back-off timeouts on repeat offenses, and pruning permanent failures.
- **Egress-Affinity Mutual Exclusion**: Concurrency throttling mapped to underlying network egress rather than surface-level endpoint URLs.
- **Tiered Quality-of-Service (QoS) Pool Rotation**: Round-robin dispatching constrained strictly within the highest qualified pool tier (L3 &rarr; L2 &rarr; L1).

---

## Part 6: Installation & Usage

### Installation

```bash
# In your virtual environment
pip install -e .

# With PySocks for SOCKS4/SOCKS5 support
pip install -e ".[socks]"
```

### CLI Reference

Both `p-rox` and `proxy-collector` can be used interchangeably:

#### 1. Inspect Cache and Feed Catalog
```bash
p-rox stats
```

#### 2. Fetch and Update Upstream Feeds
```bash
# Uses cached feeds if fresh (<24h TTL)
p-rox fetch

# Force re-download all 52 feeds in parallel
p-rox fetch --force --concurrency 12
```

#### 3. Collect, Deduplicate, and Filter Proxies
```bash
# Collect and deduplicate all proxies (web + tunnels)
p-rox collect -o all_proxies.txt

# Collect only standard web proxies (HTTP, HTTPS, SOCKS4, SOCKS5)
p-rox collect -o web_proxies.txt --web-only

# Filter specific protocols
p-rox collect -o socks_proxies.txt --protocol socks4,socks5

# Refresh feeds from the internet before collecting
p-rox collect -o all_proxies.txt --fetch
```

#### 4. Stage Qualification & Health Probing
```bash
# Run batch probe across candidate proxies
p-rox probe --limit 200 --site https://example.com/

# Query real-time qualification and pool status
p-rox status --site example.com

# Start continuous foreground background qualification daemon
p-rox daemon --site https://example.com/ --route-workers 8 --site-workers 8
```

#### 5. Local Forward Proxy Gateway (Static Port Slots)
Exposes fixed local ports mapped to pre-qualified, auto-healing upstream proxies:

```bash
# 1. Interactive mode with live dashboard (slots on ports 8000..8009)
p-rox gateway --slots 10 --base-port 8000

# 2. Bind to 0.0.0.0 for access across home LAN / server
p-rox gateway --host 0.0.0.0 --slots 10 --base-port 8000

# 3. Background daemon mode
p-rox gateway --slots 10 --base-port 8000 --daemon
p-rox gateway --status      # View live slot status
p-rox gateway --stop        # Stop background daemon

# 4. Void Linux runit service generation
sudo p-rox gateway --gen-runit /etc/sv/p-rox/run
sudo ln -s /etc/sv/p-rox /var/service/
```

Use from **any tool or language** without SDK code changes:
```bash
# Slot 0
curl -x http://127.0.0.1:8000 https://ifconfig.me
# Slot 1
curl -x http://127.0.0.1:8001 https://ifconfig.me
```

---

## Part 7: Python API Reference

### 1. Basic Collection & Deduplication

```python
from proxy_collector import ProxyCollector

collector = ProxyCollector()

# Download feeds if stale (uses local cache if fresh)
collector.fetch(force=False)

# Collect deduplicated web proxies
proxies = collector.collect(web_only=True, limit=1000)

for p in proxies:
    # Scheme, host, port, canonical URL, and requests-ready URL
    print(p.scheme, p.host, p.port, p.url, p.requests_url)
```

### 2. Low-Memory Streaming (2M+ Entries)

```python
from proxy_collector import ProxyCollector

collector = ProxyCollector()

for proxy in collector.collect_stream(protocols=["socks5", "https"]):
    handle_proxy(proxy)
```

### 3. File Export with Detailed Statistics

```python
from proxy_collector import ProxyCollector

collector = ProxyCollector()
stats = collector.collect_to_file("all_proxies.txt", sort=True)

print(f"Total lines parsed:   {stats.total_lines_parsed:,}")
print(f"Unique entries saved: {stats.unique_entries:,}")
print(f"Duplicates dropped:   {stats.duplicates_dropped:,}")
print(f"Duration:             {stats.duration_seconds:.2f}s")
print("Protocols breakdown: ", stats.by_protocol)
```

### 4. Staged Qualification Pool (`ProxyPool`)

```python
from proxy_collector import ProxyPool, ProxyCollector

pool = ProxyPool()

# Ingest candidates
collector = ProxyCollector()
collector.ingest_into_pool(pool, limit=5000, protocols={"http", "https", "socks5"})

# Pick highest qualified proxy (L3 mix -> L2 site -> L1 route -> untested)
proxy = pool.pick(site="example.com")
print("Selected:", pool.requests_url(proxy))

# Handle circuit breaker
pool.ok(proxy)                       # Layer 1 route echo succeeded
pool.site_ok(proxy, "example.com")   # Layer 2 target site succeeded
pool.site_fail(proxy, "example.com", "HTTP-403")  # Target site cooldown
pool.fail(proxy, "conn")             # Global socket failure quarantine
```

### 5. Identity-Leased Requests Session (`RoutedSession`)

Subclass of `requests.Session` that automatically manages exclusive per-egress proxy leases across requests:

```python
from proxy_collector import ProxyPool, RoutedSession

pool = ProxyPool()

# RoutedSession automatically leases qualified proxies for the target site
with RoutedSession(pool, site="https://example.com", min_stage=2) as session:
    # 1. Standard GET request (lease held during request)
    resp = session.get("https://example.com/api/data")
    print(resp.status_code, resp.route_proxy["host"])

    # 2. Streaming transfer (lease held across the entire stream; awards L3 qualification on finish)
    with session.get("https://example.com/large-file.zip", stream=True) as stream_resp:
        for chunk in stream_resp.iter_content(chunk_size=65536):
            process_chunk(chunk)
```

### 6. Background Multi-Stage Monitor (`ProxyMonitor`)

```python
from proxy_collector import ProxyPool, ProxyMonitor

pool = ProxyPool()

# Launch background workers: route echo (L1) and site probes (L2)
monitor = ProxyMonitor(
    pool,
    sites={"example.com": "https://example.com/robots.txt"},
    route_workers=8,
    site_workers=8,
)
monitor.start()

# Monitor runs asynchronously in background feeding and qualifying proxies
# To stop:
monitor.close()
```

### 7. Programmatic Proxy Gateway (`GatewayServer`)

```python
import asyncio
from proxy_collector import GatewayServer, ProxyPool

pool = ProxyPool()

async def run_gateway():
    # Maintain 10 static local ports (8000..8009) auto-mapped to healthy proxies
    server = GatewayServer(
        slots_count=10,
        base_port=8000,
        host="127.0.0.1",
        pool=pool,
    )
    await server.start(interactive=True)
    # The server runs asynchronously handling client traffic and hot-swapping
    await asyncio.sleep(3600)
    await server.stop()

asyncio.run(run_gateway())
```
added = collector.ingest_into_pool(
    pool,
    limit=5000,
    protocols={"http", "https", "socks5"},
)
print(f"Ingested {added} fresh, deduplicated proxies into pool.")
```
