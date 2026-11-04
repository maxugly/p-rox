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
- **Security & Anonymity**: Anonymity tier (`elite`, `anonymous`, `slow`, `transparent`, `tampered`) and TLS certificate tampering detection.
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

### 7. Daily Background Sync & Delta Merging
To keep candidate pools consistently fresh without restarting or degrading performance:
- **HTTP 304 Conditional Checks**: Daily checks send standard `If-None-Match: <ETag>` and `If-Modified-Since` headers. Unchanged feeds return `HTTP 304 Not Modified`, transferring 0 payload bytes and consuming 0 parse time.
- **Fast Local Delta Merging**: When an upstream feed updates, `p-rox` performs high-speed set difference between the new file and previous cache, streaming only *net-new* proxy candidates through deduplication and SQLite ingestion in under 100 milliseconds.
- **Non-Blocking Background Thread**: Background syncing runs concurrently in WAL mode without pausing test workers or interrupting forward proxy streams.
- **Real-Time Telemetry & Manual Trigger**: Sync progress and countdowns are displayed in both `VIEW_MONITOR` and `VIEW_SERVER`. Pressing `u` at any time triggers an immediate check on demand.

### 8. Multi-Factor Composite Anonymity Qualification
Public proxy lists often label over 95% of entries as "elite" simply because modern SOCKS or Squid proxies strip the `Via` header. `p-rox` enforces strict, multi-factor criteria:
- **`ELITE`**: Clean headers (no `Via`, `X-Forwarded-For`, etc.) + Verified clean TLS (no MitM) + **TCP handshake ≤ 300ms** + **HTTP TTFB ≤ 1,500ms**.
- **`ANON` / `ANONYMOUS`**: Clean headers and clean TLS with moderate latency (TCP ≤ 800ms, TTFB ≤ 4,000ms), OR valid proxy indicator headers without leaking client IP.
- **`SLOW`**: Clean headers and clean TLS, but high latency (TCP > 800ms or TTFB > 4,000ms).
- **`TRANS` / `TRANSPARENT`**: Leaks client IP address in `origin` or headers (`X-Forwarded-For`, `Client-IP`, `X-Real-IP`, etc.).
- **`TAMPER` / `TAMPERED`**: Intercepts or tampers with TLS connections (invalid or self-signed SSL certificates).
- **Host IP Discovery**: Auto-discovers local machine public IP once on startup via fast public echo endpoints for true leak verification. Overridable via `--my-ip <ip>` or disabled with `--no-ip-discovery`.

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
- `--sync-interval <int>`: Daily background feed check interval in seconds (default: `86400` / 24h, `0` to disable).
- `--no-auto-sync`: Disable automatic background feed syncing.
- `--my-ip <ip>`: Explicit host public IP address for client IP leak detection.
- `--no-ip-discovery`: Disable automatic discovery of local public IP.
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
- **`1` - `0`**: Opens the dedicated **Country Selection Menu** for slots 1 through 10.
- **`s`**: Toggles between the **Pool Monitor View** (sticky header with scrolling test output) and the **Gateway Server View** (live slot matrix).
- **`u`**: Manually triggers an immediate background feed update check without waiting for the daily 24h timer.
- **`+` / `=`**: Dynamically scales up the gateway by adding a new proxy slot and binding the next sequential port (e.g. opens `3040`).
- **`-` / `_`**: Dynamically scales down the gateway by stopping and removing the highest numbered slot (minimum 1 slot maintained).
- **`Ctrl+C`**: Cleanly shuts down all socket servers, drains completed work, and restores terminal scrolling margins.

#### Gateway Server Screen
Pressing `s` switches into the live server screen, showing connection URLs, proxy survival times down to the second (`Alive`), transfer speeds (`Speed ▲/▼`), cumulative traffic (`Traffic ▲/▼`), connection counts (`Conns`), flags, latencies, and upstreams:
```text
=== p-rox Local Proxy Gateway (10 slots on 127.0.0.1) ===
GitHub: https://github.com/maxugly/p-rox
Connect To Proxy:   http://127.0.0.1:3030 .. http://127.0.0.1:3039  (Ports: 3030..3039)
Example Usage:      curl -x http://127.0.0.1:3030 https://httpbin.org/ip
Pool Summary:       Total: 2,050,404 │ Alive: 711 │ Untested: 2,044,451
Slot Menu: [1-0]  │  Toggle View: [s]  │  [+] Add Port  │  [-] Remove Port  │  [Ctrl+C] Stop

Slot  Port   Status    Location         Alive       Speed (▲/▼)        Traffic (▲/▼)      Conns    Ping (TCP/TTFB)  Upstream
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
1     3030   healthy   🇸🇬 SG            2h 16m 42s  14.2KB/s / 48.6KB/s 1.2MB / 4.8MB     2 (14)   242ms / 9677ms   https://23.106.249.54:6550
2     3031   healthy   🇧🇩 BD            1h 36m 15s  0B/s / 0B/s        120KB / 450KB      0 (3)    305ms TCP        http://115.127.31.66:8080
3     3032   healthy   🇨🇳 CN (Beijing)  34m 31s     2.1KB/s / 18.4KB/s 45KB / 320KB       1 (5)    229ms / 8139ms   socks5://47.92.219.102:8085
4     3033   healthy   🇨🇳 CN            2h 16m 42s  0B/s / 0B/s        85KB / 512KB       0 (8)    266ms / 3510ms   https://117.82.138.100:1080
5     3034   healthy   🇺🇸 US (Seattle)  2h 02m 09s  5.4KB/s / 62.1KB/s 520KB / 3.4MB      1 (22)   162ms / 3423ms   https://45.150.164.23:8443
6     3035   healthy   🇱🇹 LT            2h 13m 50s  0B/s / 0B/s        12KB / 88KB        0 (2)    172ms / 3342ms   https://89.124.8.83:443
7     3036   healthy   🇩🇪 DE (Frankfurt 2h 02m 09s  0B/s / 0B/s        210KB / 1.1MB      0 (9)    243ms / 2583ms   https://64.31.39.228:443
8     3037   healthy   🇱🇻 LV (Riga)     2h 16m 42s  1.0KB/s / 12.0KB/s 64KB / 480KB       1 (7)    140ms / 1962ms   https://89.111.28.78:443
9     3038   healthy   🇭🇰 HK (Hong Kong 59m 24s     0B/s / 0B/s        0B / 0B            0 (0)    268ms TCP        socks5://165.154.20.187:108…
10    3039   healthy   🇺🇸 US (Seattle)  2h 02m 09s  0B/s / 0B/s        180KB / 950KB      0 (11)   185ms / 2509ms   https://45.150.164.14:8443
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
Hotkeys: [1-0] slot country menu │ [s] toggle monitor/server │ [+] add proxy slot │ [-] remove proxy slot │ Ctrl+C stop
```

#### Dedicated Slot Country Selection Menu (`1`–`0`)
Pressing `1` through `0` opens an interactive, full-screen tiered country selector for that slot:
```text
=== Slot 1 (Port 3030) Country Selection ===  [Current: US]
Select a country to lock this slot. Testing conveyor prioritizes live proxies for selected country.
[  0] 🌐 ANY: All Countries (Clear Filter)   │   Filter: [US                            ]   │   Matches: 1
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
[  1] 🇺🇸 US:14/.24M [  2] 🇩🇪 DE:8/45k   [  3] 🇸🇬 SG:5/12k   [  4] 🇨🇱 CL:0/42    [  5] 🇰🇷 KR:0/380   
[  6] 🇦🇶 AQ:0/0     [  7] 🇦🇮 AI:0/0     [  8] 🇦🇼 AW:0/0     ...
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
Nav: [↑/↓/←/→] Move  │  [Enter] Select  │  [0] Clear Filter  │  [Backspace] Edit  │  [Esc/q] Cancel
```

- **Visual Tiering**:
  - **Tier 1 (Bold Green)**: Tested & alive > 0 proxies.
  - **Tier 2 (Warning Yellow Background)**: Represented in DB candidates, but 0 alive.
  - **Tier 3 (Dimmed Grey)**: 0 candidates in database.
- **Controls**:
  - `↑` / `↓` / `←` / `→`: Navigates cursor through the multi-column full-screen grid.
  - Type letters (`US`, `DE`) or digits (`14`): Instant real-time search filtering.
  - `Enter`: Confirms selection and locks the slot to that country.
  - `0`: Clears filter / unpins slot (`ANY`).
  - `Esc` / `q`: Dismisses menu without changes.
- **Prioritized Conveyor Testing**:
  - When locked to a country with 0 alive proxies, the slot drops current proxy and enters `waiting` (orange, `warming (XX)`).
  - The continuous testing loop prioritizes testing candidates for that country (up to 10 candidates per batch) until a working proxy is verified and immediately assigned.
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

### Country Filtering & Location Selection
Filter live testing or local gateway slot binding by ISO 2-letter country code:
```bash
# Run continuous maintenance and forward gateway for US proxies only
p-rox --serve --country US

# Batch test candidates located in Germany
p-rox test --limit 50 --country DE
```

### Instant GeoIP Calculation & Background Resolution
Country, city, and ASN metadata are computed immediately upon ingestion into SQLite using local microsecond binary MMDB lookups and in-memory host caching.

For existing unpopulated candidates in the database, `p-rox` launches a dedicated daemon thread (`p-rox-geoip-resolver`) that resolves them asynchronously in batches of 5,000 without locking SQLite or degrading runner loops.

To disable automatic GeoIP calculation across any command:
```bash
p-rox --no-geoip
p-rox sync --no-geoip
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

All 97 tests execute in under 1 second.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
