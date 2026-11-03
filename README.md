# p-rox

[![Version](https://img.shields.io/badge/version-0.0.1-blue.svg)](https://github.com/maxugly/p-rox)
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

    subgraph S3["3. Execution, Leasing & Local Gateway"]
        L2 & L3 --> E["ProxyLeases\n(Mutual Exclusion by Egress Identity)"]
        E --> F["RoutedSession / Worker Downloader\n(Exclusive Per-Site Egress Locks)"]
        L2 & L3 --> G["Local Gateway Server\n(Static Slots 127.0.0.1:3030..3039,\nAuto-Healing & Hot-Swap)"]
        G --> H["Any HTTP/SOCKS Client\n(curl, scrapers, browsers, Void runit)"]
    end
```

---

## Why P Rox:

Public proxy scraping is notoriously painful:
- **90% of proxies are dead on arrival** before you even send your first HTTP request.
- **8% die mid-scrape**, hanging connections and crashing workers.
- **1.5% are "transparent" honeypots** that leak your real home IP address in `X-Forwarded-For`.
- **Rogue nodes intercept SSL certificates**, trying to decrypt or tamper with your HTTPS requests.
- **Hundreds of different entry IPs share the exact same exit node**, so hitting a website with 20 threads gets your entire batch banned by Cloudflare in 1 second.

`p-rox` was built from the ground up to solve all of this:

* 🚀 **2.2 Million Candidate Ingestion in 18 Seconds**: Ingests, normalizes, and deduplicates 52 upstream public feeds without blowing up system RAM.
* 🛡️ **Auto-Healing Local Gateway (`127.0.0.1:3030..3039`)**: You don't have to rewrite your scraper code or deal with rotation logic. Point your browser, curl, or scraper to a fixed local port (`localhost:3030`), and `p-rox` automatically tests, warms, and hot-swaps proxies behind the scenes without ever dropping the listener!
* 🔒 **Egress-Identity Mutual Exclusion**: Proxies are tracked by their *true remote exit IP* (`ip_echo`). Two workers will never hit the same target domain through the same physical egress IP at the same time.
* ⚡ **Microsecond Offline GeoIP**: Look up Country, City, Region, ASN, and Residential vs. Datacenter classification in 2 microseconds using local binary MMDB files (DB-IP Lite or MaxMind GeoLite2).
* ⏱️ **Dual-Ping Benchmarks**: Separates raw socket connection latency (SYN-ACK) from HTTP Time-To-First-Byte (TTFB) so you always know where bottlenecks lie.
* 🧯 **Exponential Circuit Breakers**: Benches flaky proxies for 2 minutes, rate-limited proxies for 10 minutes, auth blocks for 1 hour, and dead proxies for 7 days.
* 🌐 **Universal Portability**: Zero Docker containers or systemd dependencies. Runs as a clean foreground CLI, interactive dashboard, or background daemon across Linux, macOS, BSD, and Windows, with optional `runit` generation for lightweight servers.

---

## Quickstart for Beginners (Zero to Hero in 60 Seconds)

You don't need a degree in networking to use `p-rox`. Follow this end-to-end workflow:

### 1. Install `p-rox`
```bash
git clone https://github.com/maxugly/p-rox.git
cd p-rox
pip install -e ".[socks]"
```

### 2. Download Free GeoIP Data (1 command, no account needed)
```bash
p-rox update-geoip --source dbip
```

### 3. Ingest Fresh Candidate Proxies
Download 52 public feeds, deduplicate in real time, and load candidates directly into the rotation pool:
```bash
p-rox update-pool --limit 500
```
*(Or use `p-rox collect -o all_proxies.txt --fetch` to export 2.2+ million deduplicated candidates to a file!)*

### 4. Test & Benchmark Live Proxies
Run high-speed dual-ping latency, anonymity tiering, and TLS tamper checks across candidates:
```bash
# Benchmark and display visual profile cards for candidate proxies
p-rox profile --limit 5

# Or run staged qualification (L1 Route IP echo + L2 Site qualification)
p-rox probe --limit 20
```

### 5. Launch the Forward Proxy Gateway
```bash
p-rox gateway --slots 10
```

**That's it!** You now have 10 live, static proxy ports running on `127.0.0.1:3030` through `127.0.0.1:3039`.

Open a second terminal window and test your new IPs:
```bash
# Slot 0
curl -x http://127.0.0.1:3030 https://ifconfig.me

# Slot 1
curl -x http://127.0.0.1:3031 https://ifconfig.me
```

> [!TIP]
> **Why static ports are awesome**: If an upstream public proxy dies, `p-rox` instantly swaps in a new healthy proxy in the background. Your scraper just keeps hitting `http://127.0.0.1:3030` without caring!

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

## Part 6: Installation, Recipes & CLI Reference

### Installation

```bash
# Clone and install in your virtualenv
git clone https://github.com/maxugly/p-rox.git
cd p-rox
pip install -e ".[socks]"
```

Both `p-rox` and `proxy-collector` binary commands can be used interchangeably.

---

### Plug-and-Play Recipes (How to Actually Use It)

Once you run `p-rox gateway --slots 10`, you have 10 forward proxy ports active on `127.0.0.1:3030` through `3039`. Here is how you connect your favorite tools:

#### Recipe 1: Terminal & `curl`
```bash
# Slot 0
curl -x http://127.0.0.1:3030 https://ifconfig.me

# Slot 1
curl -x http://127.0.0.1:3031 https://ifconfig.me
```

#### Recipe 2: Python `requests`
```python
import requests

# Slot 0
proxies = {
    "http": "http://127.0.0.1:3030",
    "https": "http://127.0.0.1:3030",
}
resp = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=10)
print("My Proxy IP:", resp.json()["origin"])
```

#### Recipe 3: Web Scraping with Playwright
```python
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    # Route the entire browser through Slot 0
    browser = p.chromium.launch(
        proxy={"server": "http://127.0.0.1:3030"}
    )
    page = browser.new_page()
    page.goto("https://ifconfig.me")
    print("Page Title:", page.title())
    browser.close()
```

#### Recipe 4: Desktop Web Browser (Firefox / Chrome)
1. In **Firefox**: Settings &rarr; Network Settings &rarr; Manual Proxy Configuration.
2. Set **HTTP Proxy** to `127.0.0.1`, Port `3030`. Check *"Also use this proxy for HTTPS"*.
3. Click OK. Now all your browser traffic routes through Slot 0. If that proxy drops, `p-rox` swaps it silently while you browse!

#### Recipe 5: Running 24/7 on a Home Server or Laptop
```bash
# 1. Start in background
p-rox gateway --host 0.0.0.0 --slots 10 --daemon

# 2. Check live health anytime
p-rox gateway --status

# 3. Stop background process
p-rox gateway --stop
```

For **Void Linux** (or systems using `runit`):
```bash
# Automatically write an executable runit service script
sudo p-rox gateway --gen-runit /etc/sv/p-rox/run
sudo ln -s /etc/sv/p-rox /var/service/
```

---

### Complete CLI Flag & Subcommand Reference

#### 1. `p-rox gateway` — Local Forward Proxy Gateway
Exposes fixed local ports mapped to auto-healing upstream proxies:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--slots` | int | `10` | Number of static local port slots to maintain (e.g. 10 allocates ports 3030..3039). |
| `--base-port` | int | `3030` | Starting port number for the slot allocation. |
| `--host` | string | `127.0.0.1` | Local network interface to bind (`127.0.0.1` for local, `0.0.0.0` for LAN access). |
| `--site` | string | `None` | Optional target website for L2 qualification (e.g. `https://example.com/`). |
| `--daemon` | flag | `False` | Run server as a detached background daemon process. |
| `--status` | flag | `False` | Query status JSON of running background slots. |
| `--stop` | flag | `False` | Send SIGTERM to running background gateway daemon. |
| `--gen-runit` | path | `None` | Generate an executable Void Linux runit service script at PATH. |
| `--cache-dir` | path | `~/.cache/p-rox` | Directory storing health data and status snapshots. |

```bash
# Interactive mode with live terminal dashboard (slots on ports 3030..3039)
p-rox gateway --slots 10

# Bind to LAN for home server
p-rox gateway --host 0.0.0.0 --slots 10 --base-port 3030

# Background daemon mode
p-rox gateway --slots 10 --daemon
p-rox gateway --status
p-rox gateway --stop
```

---

#### 2. `p-rox profile` — Dual-Ping & Deep Telemetry Benchmarking
Benchmark proxies for TCP connect time, HTTP TTFB, anonymity tiering, and TLS tampering:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--proxy` | string | `None` | Single proxy URL to test (e.g. `http://1.2.3.4:8080`). |
| `--limit` | int | `10` | Number of pool candidates to benchmark. |
| `--concurrency` | int | `10` | Parallel worker threads for concurrent testing. |
| `--timeout` | float | `6.0` | Socket timeout in seconds per test phase. |
| `--cache-dir` | path | `~/.cache/p-rox` | Directory storing GeoIP databases and health cache. |

```bash
# Benchmark a single proxy
p-rox profile --proxy http://185.220.101.4:9050

# Benchmark 50 proxies concurrently with 10 worker threads
p-rox profile --limit 50 --concurrency 10
```

---

#### 3. `p-rox update-geoip` — Offline GeoIP Management
Downloads and updates offline binary `.mmdb` databases for microsecond lookups:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--source` | string | `dbip` | Database source: `dbip` (free, no account) or `maxmind`. |
| `--license-key`| string | `None` | MaxMind account license key (required if `--source maxmind`). |
| `--cache-dir` | path | `~/.cache/p-rox` | Directory to save downloaded `.mmdb` file. |

```bash
# 1. Download free DB-IP City Lite database (zero signup required, CC-BY-4.0)
p-rox update-geoip --source dbip

# 2. Download MaxMind GeoLite2 using your account license key
p-rox update-geoip --source maxmind --license-key <YOUR_KEY>
```

---

#### 4. `p-rox collect` — Acquire, Deduplicate & Export Proxies
Streaming canonical deduplication of 2.2M+ candidates into clean text:

| Flag | Type | Default | Description |
|---|---|---|---|
| `-o, --output` | path | `all_proxies.txt` | Target output file for deduplicated proxies. |
| `--web-only` | flag | `False` | Filter to standard web protocols only (HTTP, HTTPS, SOCKS4, SOCKS5). |
| `--protocol` | string | `None` | Comma-separated protocols to include (e.g. `socks5,https`). |
| `--no-sort` | flag | `False` | Stream lines immediately without sorting (faster on huge files). |
| `--fetch` | flag | `False` | Fetch stale feeds from the internet before deduplicating. |
| `--force` | flag | `False` | Force re-download all 52 feeds even if cache is fresh. |
| `--concurrency`| int | `10` | Concurrent download threads for fetching. |
| `--cache-dir` | path | `~/.cache/p-rox` | Cache directory. |

```bash
# Export all 2.2M deduplicated proxies (web + tunnels)
p-rox collect -o all_proxies.txt

# Export only SOCKS5 and HTTPS proxies
p-rox collect -o fast_proxies.txt --protocol socks5,https

# Refresh feeds from internet before collecting
p-rox collect -o all_proxies.txt --fetch
```

---

#### 5. `p-rox fetch` — Parallel Feed Downloader
Fetches 52 upstream proxy feeds with atomic swapping:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--concurrency`| int | `10` | Concurrent worker threads downloading feeds. |
| `--ttl` | float | `86400.0` | Cache time-to-live in seconds (default: 24 hours). |
| `--force` | flag | `False` | Bypass TTL and force redownloading all feeds. |
| `--catalog` | path | `None` | Optional custom `lists.json` feed catalog path. |
| `--cache-dir` | path | `~/.cache/p-rox` | Directory for raw cached feed files. |

```bash
# Uses cached feeds if fresh (<24h TTL)
p-rox fetch

# Force re-download all 52 feeds in parallel with 12 workers
p-rox fetch --force --concurrency 12
```

---

#### 6. `p-rox probe` — Staged Qualification Prober
Runs batch qualification testing across pending pool candidates:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--limit` | int | `200` | Maximum number of candidates to test. |
| `--site` | string | `None` | Target site for L2 qualification (e.g. `https://example.com/`). |
| `--route-workers`| int | `8` | Worker threads for Layer 1 route echo tests. |
| `--site-workers` | int | `8` | Worker threads for Layer 2 target site tests. |
| `--cache-dir` | path | `~/.cache/p-rox` | Cache directory. |

```bash
# Run batch probe across 200 proxies
p-rox probe --limit 200 --site https://example.com/
```

---

#### 7. `p-rox daemon` — Background Monitoring Service
Starts continuous foreground daemon continuously testing and qualifying proxies:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--site` | string | `None` | Target site for L2 qualification. |
| `--route-workers`| int | `8` | L1 route testing threads. |
| `--site-workers` | int | `8` | L2 site testing threads. |
| `--interval` | float | `2.0` | Status publish interval in seconds. |
| `--cache-dir` | path | `~/.cache/p-rox` | Cache directory. |

```bash
p-rox daemon --site https://example.com/ --route-workers 8 --site-workers 8
```

---

#### 8. `p-rox status` & `p-rox stats` — Status & Inspection
Queries live pool health or inspects cache statistics:

| Command | Flags | Description |
|---|---|---|
| `p-rox status` | `--site <URL>`, `--cache-dir <DIR>` | Prints real-time qualification tiers and circuit breaker status. |
| `p-rox stats` | `--catalog <PATH>`, `--cache-dir <DIR>` | Displays feed catalog metrics and local cache file ages. |

```bash
p-rox status --site example.com
p-rox stats
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
    # Maintain 10 static local ports (3030..3039) auto-mapped to healthy proxies
    server = GatewayServer(
        slots_count=10,
        base_port=3030,
        host="127.0.0.1",
        pool=pool,
    )
    await server.start(interactive=True)
    # The server runs asynchronously handling client traffic and hot-swapping
    await asyncio.sleep(3600)
    await server.stop()

asyncio.run(run_gateway())
```

### 8. GeoIP Resolution & Network Profiling

```python
from proxy_collector import GeoIPResolver, ProxyProfiler, Proxy

# 1. Microsecond local GeoIP lookups (supports DB-IP Lite and MaxMind GeoLite2)
geoip = GeoIPResolver()
geo = geoip.lookup("185.220.101.4")
print(geo.country_code, geo.city, geo.org, geo.is_datacenter)

# 2. Deep proxy profiling (Dual-Ping, true egress echo, anonymity tier, TLS tampering)
profiler = ProxyProfiler(geoip_resolver=geoip)
p = Proxy(scheme="http", host="185.220.101.4", port=9050)
profile = profiler.profile(p)

print(f"Discovered Egress IP: {profile.egress_ip}")
print(f"TCP Handshake Latency: {profile.latency.tcp_handshake_ms}ms")
print(f"HTTP TTFB Latency:      {profile.latency.http_ttfb_ms}ms")
print(f"Anonymity Level:        {profile.security.anonymity}")  # elite, anonymous, or transparent
print(f"TLS Tampering / MitM:   {profile.security.tls_tampering_detected}")
```

---

## Part 8: Testing & Development

Run the offline regression test suite (mocked networking, zero outbound calls):

```bash
pytest -v
```

All 60 test units run in `<1s` covering concurrent feed downloads, multi-format stream parsing, canonical deduplication, staged pool progression, identity leasing, circuit breaker quarantine, local gateway transparent tunneling, dual-ping latency, anonymity tiering, and GeoIP resolution.

---

## Agent Directives

For autonomous AI coding agents collaborating on this repository, please consult [AGENTS.md](AGENTS.md) for mandatory architectural constraints, Void Linux / runit conventions, port assignments, and testing protocols.
