# p-rox

`p-rox` is a command-line tool that continuously collects, deduplicates, verifies, and maintains public proxies in a local SQLite database.

It is designed to solve a straightforward problem: public proxy feeds aggregate millions of raw candidate entries, but the vast majority are dead, misconfigured, or short-lived. `p-rox` downloads these public listings, deduplicates them, and runs an asynchronous verification pipeline to track which proxies are actually working, where they exit, how fast they respond, and how long they stay alive.

---

## How It Works

```
Public Feeds (52 feeds, cached 24h)
  ↓
Canonical Deduplicator
  ↓
SQLite Storage (~/.p-rox/proxies.db)
  ↓
Conveyor-Belt Queue (25 persistent workers, staggered start)
  ├── 1. Re-test verified alive proxies first (warm pool on startup)
  ├── 2. Keepalive heartbeat re-checks every 180s
  └── 3. Test untested candidates continuously
  ↓
Single-Threaded SQLite Writer (Zero lock contention)
  ├─ Terminal Sticky Monitor (DECSTBM scroll region + real-time telemetry)
  └─ Local Forward Proxy Gateway (--serve on 127.0.0.1:3030..3039)
```

### 1. Ingestion and Deduplication
- Downloads listings across 52 verified public feed sources.
- Downloaded feeds are cached locally in `~/.p-rox/sources/` with a 24-hour TTL to prevent unnecessary network requests.
- Proxies are normalized into canonical URLs across supported schemes (`http`, `https`, `socks4`, `socks5`) and stripped of duplicates before insertion into SQLite.

### 2. SQLite Database Architecture
Proxy candidates, test outcomes, and telemetry are stored locally in a single SQLite database at `~/.p-rox/proxies.db` using WAL (Write-Ahead Logging) mode.

The database tracks:
- **Proxy Identity**: Scheme, host, port, credentials, and canonical URL.
- **Verification Metrics**: TCP handshake latency (round-trip time), HTTP Time-To-First-Byte (TTFB), and true egress IP.
- **Geolocation**: Country code, city, and ASN (resolved offline using local MMDB databases).
- **Security & Anonymity**: Anonymity tier (`elite`, `anonymous`, `transparent`) and TLS certificate tampering detection.
- **Lifecycle**: `first_seen_alive` timestamp, `last_checked_at`, and cumulative failure counts.

**No-Deletion Policy**: Dead proxies are never silently deleted. When a proxy fails a test, its status is marked `dead`, its failure counter increments, and it is pushed to the back of the queue. If it continues to fail after 5 attempts, it is backed off for 7 days.

### 3. Continuous Conveyor-Belt Pipeline
Traditional batch-based testing (e.g. submitting 50 tasks and waiting for all 50 to finish) suffers from stutter: when workers hit dead proxies, all threads block on socket connection timeouts simultaneously, causing periodic bursts followed by multi-second freezes.

`p-rox` uses a continuous producer-consumer pipeline:
- **Persistent Workers**: Worker threads are created once at startup and remain active throughout the session.
- **Staggered Start Times**: Workers launch with micro-delays (`0.05s * i`) to de-synchronize network socket timeouts and eliminate clustered stalls.
- **Autonomous Task Pickup**: As soon as any worker completes a proxy check (whether in 100ms or 3000ms), it immediately grabs the next candidate from the queue without waiting for other threads.
- **Single SQLite Writer**: Worker threads perform pure network I/O. Results flow into a collector queue drained by a single thread, eliminating multi-threaded SQLite write-lock contention.

### 4. Health Check Prioritization
When selecting candidates for testing, `p-rox` applies the following order:
1. **Startup Warm-up**: On initial launch, all previously verified alive proxies are checked first (newest verified first) to confirm the working pool immediately.
2. **Keepalive Heartbeat**: Working proxies that have not been checked in 180 seconds are re-tested to detect drops and track actual continuous survival lifespan.
3. **Untested Candidates**: Freshly ingested candidates are pulled in sequence.
4. **Dead Retries**: Dead candidates with fewer than 5 failures are retried after a 2-hour backoff window.

### 5. Sticky Terminal Dashboard
When running interactively, `p-rox` uses standard ANSI scrolling margins (`DECSTBM`) to display a fixed 6- to 8-line status box at the top of the terminal while live test results scroll beneath:

```text
┌─ p-rox pool monitor ─────────────────────────────────────────────────────────────────┐
│ Uptime: 00:06:05 │ Speed: 18.4/s │ Workers: 25 │ Heartbeat: 180s                     │
│ Pool: 2,050,404 Total │ 2,045,023 Untested │ 754 ALIVE │ 4,627 Dead (15 Died)        │
│ Ping: 211ms (±245ms) │ Avg Life: 1h 06m │ Oldest: 2h 00m │ Elite: 97% (736)          │
│ Countries (66):                                                                      │
│ 🇺🇸 US: 170 (22.5%)  🇳🇱 NL: 50 (6.6%)  🇩🇪 DE: 49 (6.5%)  🇮🇳 IN: 35 (4.6%)             │
│ ??: 32 (4.2%)  🇱🇻 LV: 30 (4.0%)  🇷🇴 RO: 30 (4.0%)  🇫🇷 FR: 26 (3.4%)                  │
│ 🇬🇧 GB: 25 (3.3%)  🇸🇬 SG: 25 (3.3%)  🇨🇳 CN: 21 (2.8%)  🇯🇵 JP: 19 (2.5%)               │
└──────────────────────────────────────────────────────────────────────────────────────┘
  [ALIVE] socks5://49.13.22.249:10813          157ms  🇵🇰 PK Karachi     ELITE   (Egress: 31.6.58.1)
  [DEAD ] http://198.51.100.24:8080            Unreachable or timeout
```

- **Counts**: Total pool entries, Untested, Verified Alive, Dead, and **Died** (proxies that failed during the current session).
- **Latency & Lifetime**: Average ping with standard deviation (`avg_ping ± stdev`), average lifespan of alive proxies, and oldest surviving proxy.
- **Country Matrix**: Country representation with Unicode flag emojis and percentage share of the alive pool, dynamically wrapping across lines without truncation.
- **Real-Time Clock**: A dedicated 1Hz background ticker ensures the uptime clock and telemetry refresh smoothly every second.
- **Terminal Fallback**: When stdout is not a TTY (such as when redirected to a log file or pipe), ANSI escape codes are omitted.

### 6. Local Forward Proxy Gateway (`--serve`)
When started with the `--serve` flag, `p-rox` opens an asynchronous forward proxy gateway alongside the continuous tester:
- **Static Local Ports**: Binds an array of local ports (default: `127.0.0.1:3030..3039` for 10 slots).
- **Dynamic Upstream Binding**: Each static local port is bound to a verified working upstream proxy (`status = 'alive'`) queried directly from SQLite.
- **Transparent Hot-Swapping**: If an upstream proxy drops or fails mid-session, the gateway immediately fails over and hot-swaps the slot with the next available alive proxy without interrupting the local listening port or requiring client restarts.
- **Truthful Warming State**: If fewer verified proxies exist than requested slots, surplus ports display `waiting` (warming) until the continuous testing pipeline qualifies new nodes.
- **Dynamic Slot Scaling**: Slots can be scaled up or down on the fly (`+` to add ports, `-` to remove ports).
- **Survival Duration**: Tracks exactly how long each assigned proxy has been continuously verified alive (`Alive Time`).

---

## Installation

### Requirements
- Python 3.10, 3.11, 3.12, or 3.13
- Linux, macOS, BSD, or Windows

### Setup
```bash
# Clone the repository
git clone https://github.com/maxugly/p-rox.git
cd p-rox

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install package and dependencies
pip install -e .
```

---

## CLI Usage

### Continuous Mode (Default)
Starts the continuous acquisition and testing pipeline with the live sticky dashboard:
```bash
p-rox
```
Options:
- `--workers <int>`: Number of concurrent test workers (default: `25`).
- `--keepalive <float>`: Heartbeat check interval in seconds for alive proxies (default: `180.0`).
- `--timeout <float>`: Network connect and probe timeout in seconds (default: `6.0`).
- `--db <path>`: Custom SQLite database path (default: `~/.p-rox/proxies.db`).

If the database is empty on first run, `p-rox` automatically downloads and imports the initial feeds.

### Gateway Serving Mode (`--serve`)
Runs continuous pool acquisition and testing while simultaneously serving verified proxies through static local forward proxy ports:
```bash
p-rox --serve
```
By default, this opens 10 slots on `127.0.0.1:3030..3039`.

Options:
- `--slots <int>`: Number of gateway slots/ports (default: `10`).
- `--base-port <int>`: Starting base port for slot allocation (default: `3030`, allocating `3030..3039`).
- `--host <str>`: Local bind address (default: `127.0.0.1`).

#### Real-Time Interactive Controls & Hotkeys
While running with `--serve`, single keystrokes control the engine instantly without interrupting background workers:
- **`s`**: Toggles between the **Pool Monitor View** (sticky header with scrolling test output) and the **Gateway Server View** (live slot matrix).
- **`+` / `=`**: Dynamically scales up the gateway by adding a new proxy slot and binding the next sequential port (e.g. opens `3040`).
- **`-` / `_`**: Dynamically scales down the gateway by stopping and removing the highest numbered slot (minimum 1 slot maintained).
- **`Ctrl+C`**: Cleanly shuts down all socket servers, drains completed work, and restores terminal scrolling margins.

#### Gateway Server Screen
Pressing `s` switches into the live server screen, showing connection URLs, proxy survival times (`Alive`), flags, latencies, and traffic:
```text
=== p-rox Local Proxy Gateway (10 slots on 127.0.0.1) ===
GitHub: https://github.com/maxugly/p-rox
Connect To Proxy:   http://127.0.0.1:3030 .. http://127.0.0.1:3039  (Ports: 3030..3039)
Example Usage:      curl -x http://127.0.0.1:3030 https://httpbin.org/ip
Pool Summary:       Total: 2,050,404 │ Alive: 711 │ Untested: 2,044,451
Toggle View: [s] Switch to Pool Monitor  │  [+] Add Port  │  [-] Remove Port  │  [Ctrl+C] Stop

Slot  Port    Status     Location           Alive     Anon    Ping (TCP/TTFB)  Active  KB       Upstream
────────────────────────────────────────────────────────────────────────────────────────────────────
0     3030    healthy    🇸🇬 SG              2h 16m    elite   242ms / 9677ms   0       0.0      https://23.106.249.54:6550
1     3031    healthy    🇧🇩 BD              1h 36m    elite   305ms TCP        0       0.0      http://115.127.31.66:8080
2     3032    healthy    🇨🇳 CN (Beijing)    34m 31s   elite   229ms / 8139ms   0       0.0      socks5://47.92.219.102:8085
3     3033    healthy    🇨🇳 CN              2h 16m    elite   266ms / 3510ms   0       0.0      https://117.82.138.100:1080
4     3034    healthy    🇺🇸 US (Seattle)    2h 02m    elite   162ms / 3423ms   0       0.0      https://45.150.164.23:8443
5     3035    healthy    🇱🇹 LT              2h 13m    elite   172ms / 3342ms   0       0.0      https://89.124.8.83:443
6     3036    healthy    🇩🇪 DE (Frankfurt … 2h 02m    unknow  243ms / 2583ms   0       0.0      https://64.31.39.228:443
7     3037    healthy    🇱🇻 LV (Riga)       2h 16m    elite   140ms / 1962ms   0       0.0      https://89.111.28.78:443
8     3038    healthy    🇭🇰 HK (Hong Kong)  59m 24s   unknow  268ms TCP        0       0.0      socks5://165.154.20.187:108…
9     3039    healthy    🇺🇸 US (Seattle)    2h 02m    elite   185ms / 2509ms   0       0.0      https://45.150.164.14:8443
────────────────────────────────────────────────────────────────────────────────────────────────────
Hotkeys: [s] toggle monitor/server │ [+] add proxy slot │ [-] remove proxy slot │ Ctrl+C stop
```

#### Routing Requests Through Gateway Slots
Any HTTP/HTTPS or SOCKS-capable application can route through the local ports:

```bash
# cURL
curl -x http://127.0.0.1:3030 https://httpbin.org/ip

# Python Requests
import requests
proxies = {"http": "http://127.0.0.1:3030", "https": "http://127.0.0.1:3030"}
resp = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=10)
print(resp.json())
```

### Pool Status
Inspect verified alive proxies, latency metrics, and country distribution:
```bash
p-rox status
```

Output:
```text
======================================================================
  p-rox — Proxy Pool Status
======================================================================
  Database:           ~/.p-rox/proxies.db (320.5 MB)
  Total Candidates:   2,050,404
  Untested:           2,045,023
  Verified Alive:     754
  Dead / Benched:     4,627
  Ping Performance:   211ms (±245ms)
  Top Countries:      🇺🇸 US: 170 (22.5%), 🇳🇱 NL: 50 (6.6%), 🇩🇪 DE: 49 (6.5%)...
----------------------------------------------------------------------
  Fastest Verified Working Proxies (Freshest First):
    https://89.124.8.77:443              196ms  🇱🇹 LT                (Egress: 89.124.8.77) [2m ago]
    socks5://49.13.22.249:10813          157ms  🇵🇰 PK (Karachi)     (Egress: 31.6.58.1) [3m ago]
    ...
======================================================================
```

### Manual Feed Sync
Download all 52 feeds and import new candidates:
```bash
p-rox sync
```
Use `--force` to bypass the 24-hour feed cache.

### Single-Pass Testing
Test a specific number of candidates without running continuous mode:
```bash
p-rox test --limit 50 --workers 25
```

Or profile a single proxy directly:
```bash
p-rox test --proxy socks5://198.51.100.1:1080
```

### Resetting Data
Reset proxy statuses back to `untested`:
```bash
p-rox reset
```

Or completely wipe the database and cached feed files:
```bash
p-rox reset --all
```

### Offline GeoIP Databases
Download the free binary GeoIP databases for offline country, city, and ASN resolution:
```bash
p-rox update-geoip
```

---

## Testing

The test suite runs completely offline without making outbound network requests:
```bash
pytest -v
```

All 80 tests execute in under 1 second.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
