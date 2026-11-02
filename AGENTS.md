# AGENTS.md — p-rox Repository

> Operational briefing, architectural contracts, and development laws for AI coding agents operating in this codebase. Read before reading or modifying code.

---

## 1. Project Identity

| Attribute | Value |
|---|---|
| **Project Name** | `p-rox` (importable module: `proxy_collector`) |
| **Current Version** | `0.0.1` (`.000.001` genesis alpha) |
| **CLI Commands** | `p-rox`, `proxy-collector` |
| **Repository** | [maxugly/p-rox](https://github.com/maxugly/p-rox) (Public) |
| **Runtime** | Python 3.10+ (tested against 3.10, 3.11, 3.12, 3.13) |
| **Package Manager** | `uv` / standard `pip` with `pyproject.toml` |

---

## 2. Core Mental Model & Architecture

`p-rox` solves the reliability gap between raw public proxy listings (2,200,000+ candidates) and production web scraping / downloading infrastructure.

```
Upstream Feeds (52 feeds across 9 providers)
  ↓
Fetcher (Concurrent download, 24h TTL cache, atomic file swaps)
  ↓
Parsers & Deduplicator (Streaming multi-format parse, lowercase canonical dedup)
  ↓
Layer 0: Cold Candidate Pool (~2,200,000 unique proxies)
  ↓ [Layer 1 Probe: Strict 8s budget, route IP echo, TCP verify]
Layer 1: Route-Qualified Pool (~50,000 live, egress IP discovered)
  ↓ [Layer 2 Probe: Target site robots.txt, latency check, WAF detection]
Layer 2: Site-Qualified Pool (~5,000 site-ready proxies)
  ↓ [Layer 3 Probe: Production streamed transfer throughput verified]
Layer 3: Live-Stream Qualified Pool (Premium verified bandwidth)
  ├─ RoutedSession (Per-site mutual exclusion by egress identity)
  └─ GatewayServer (Local forward proxy slots on 127.0.0.1:3030..3039)
```

---

## 3. Mandatory Non-Negotiables & Invariants

When implementing features, refactoring, or writing tests, all AI agents **must strictly adhere** to the following invariants:

### 1. Default Gateway Ports
- **The default base port is `3030`** (allocating `3030..3039` for 10 slots).
- **NEVER** use, default to, or recommend congested ports such as `8000`, `8080`, or `8001`.
- Any new slot-based services must respect the `--base-port` option and default to `3030`.

### 2. Deployment OS & Init System: Void Linux (`runit`)
- The user's continuous server runs **Void Linux**, which uses `runit` as its init system.
- **NEVER** write `systemd` service units or assume `systemctl` is available.
- Services must run as foreground processes or fork-based daemons with standard signal handling (`SIGTERM`/`SIGINT`).
- Runit service generation is built-in: `p-rox gateway --gen-runit /etc/sv/p-rox/run`. Runit template scripts live in [`scripts/runit/p-rox/run`](scripts/runit/p-rox/run).

### 3. Isolated, Offline Unit Tests
- Tests in [`tests/`](tests/) **must never make outbound network requests** to public proxy feeds, echo endpoints, or websites.
- Always mock network calls (`unittest.mock.patch`, `requests.Response` with `io.BytesIO`) or use local `asyncio` loopback socket servers.
- The entire test suite must execute cleanly in **under 1 second** (`pytest -v`).

### 4. Circuit Breaker Quarantine Tiers
Do not bypass or modify the exponential circuit breaker quarantine discipline in [`src/proxy_collector/pool.py`](src/proxy_collector/pool.py):
- **Transient failure** (socket timeout, reset): 120 seconds cooldown.
- **Rate limiting** (HTTP 429): 600 seconds cooldown.
- **Authentication / Forbidden** (HTTP 401, 403, 407): 3600 seconds cooldown.
- **Permanent dead pool** (5 cumulative failures): 7 days quarantine.

### 5. Mutual Exclusion by Egress Identity
- Proxy leasing ([`src/proxy_collector/leases.py`](src/proxy_collector/leases.py)) is keyed on **remote egress IP** (`ip_echo`), not entry host.
- Multiple proxy entry nodes sharing the same exit IP will share remote rate limits. They must never be leased concurrently to the same target domain.

### 6. Public GitHub Hygiene
- This repository is public and showcased on job applications.
- Maintain professional commit messages (`feat:`, `fix:`, `docs:`, `test:`).
- Keep all raw data files (`all_proxies.txt`, `sources/`, `data/`) strictly ignored in `.gitignore`.
- Ensure all CI workflows (`.github/workflows/ci.yml`) remain green across all supported Python versions.

---

## 4. Codebase Directory Map

```text
p-rox/
├── .github/workflows/ci.yml    # GitHub Actions CI matrix (Python 3.10-3.13)
├── scripts/runit/p-rox/run     # Void Linux runit service script template
├── src/proxy_collector/
│   ├── __init__.py             # Public API exports & __version__
│   ├── cli.py                  # CLI argument parser & subcommand actions
│   ├── collector.py            # High-level orchestrator & pool ingestion bridge
│   ├── deduplicator.py         # Zero-allocation streaming canonical deduplicator
│   ├── fetcher.py              # Parallel feed downloader with atomic .tmp swap & cache
│   ├── gateway.py              # Asyncio forward proxy gateway server & slot manager
│   ├── leases.py               # Exclusive per-site proxy lease manager by egress IP
│   ├── models.py               # Data structures: Proxy, Feed, FetchResult, Stats
│   ├── monitor.py              # Background qualification workers (L1 Route & L2 Site)
│   ├── parsers.py              # Multi-format streaming line and JSON parsers
│   ├── pool.py                 # 4-tier pool, circuit breaker, & health persistence
│   ├── routing.py              # Leased RoutedSession (requests.Session subclass)
│   ├── service.py              # Coordination layer with fcntl.flock concurrency locks
│   └── sources.py              # Catalog of 52 verified feeds across 9 providers
├── tests/                      # Fast offline test suite (42 tests, <0.4s)
├── AGENTS.md                   # This file (Agent briefing & architectural rules)
├── pyproject.toml              # Package configuration & entry points
└── README.md                   # Technical reference manual & documentation
```

---

## 5. Development & Verification Workflow

```bash
# 1. Activate development virtualenv
source .venv/bin/activate

# 2. Run full test suite
pytest -v

# 3. Test CLI commands locally
p-rox stats
p-rox gateway --help

# 4. Check git status before committing
git status

# 5. Push and verify GitHub CI
git push origin main
gh run list -L 1
```
