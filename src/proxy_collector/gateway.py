"""High-performance local forward proxy gateway daemon.

Exposes static local ports/IPs mapped to dynamic upstream proxies from the pool,
with automated background health probing, connection tunneling, and transparent hot-swapping.
"""

import asyncio
import ipaddress
import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urlsplit

from .models import Proxy
from .pool import DEFAULT_CACHE_DIR, DEFAULT_PROXY_FILE, ProxyPool, site_key

logger = logging.getLogger("proxy_collector.gateway")


@dataclass
class SlotStats:
    """Real-time metrics for an active gateway slot."""

    port: int
    slot_id: int
    upstream: str
    scheme: str
    host: str
    country: str = ""
    status: str = "warming"  # warming, healthy, degraded, swapping
    connections_total: int = 0
    connections_active: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors_total: int = 0
    last_swapped_at: float = field(default_factory=time.time)
    last_active_at: float = field(default_factory=time.time)


class ProxySlot:
    """Maintains a single local listening port bound to a dynamic upstream proxy."""

    def __init__(
        self,
        slot_id: int,
        port: int,
        host: str,
        pool: ProxyPool,
        target_site: Optional[str] = None,
        on_swap: Optional[Any] = None,
    ):
        self.slot_id = slot_id
        self.port = port
        self.host = host
        self.pool = pool
        self.target_site = target_site
        self.on_swap = on_swap

        self.server: Optional[asyncio.Server] = None
        self.current_proxy: Optional[Dict[str, Any]] = None
        self.stats = SlotStats(
            port=port,
            slot_id=slot_id,
            upstream="none",
            scheme="http",
            host="",
        )
        self.lock = asyncio.Lock()
        self._consecutive_errors = 0

    def assign_proxy(self, proxy: Dict[str, Any]):
        """Bind this slot to a new upstream proxy."""
        self.current_proxy = proxy
        url = ProxyPool.requests_url(proxy)
        self.stats.upstream = url
        self.stats.scheme = proxy.get("scheme", "http")
        self.stats.host = proxy.get("host", "")
        self.stats.country = proxy.get("country", "")
        self.stats.status = "healthy"
        self.stats.last_swapped_at = time.time()
        self._consecutive_errors = 0
        logger.info("Slot %d (:%d) assigned to upstream %s", self.slot_id, self.port, url)

    async def swap_upstream(self, reason: str = "error") -> bool:
        """Hot-swap this slot's upstream proxy with another healthy proxy from the pool."""
        async with self.lock:
            old_p = self.current_proxy
            if old_p:
                self.pool.fail(old_p, reason=reason)

            # Pick next candidate from pool
            new_p = self.pool.pick(site=self.target_site)
            if new_p and new_p != old_p:
                self.assign_proxy(new_p)
                if self.on_swap:
                    self.on_swap(self)
                return True

            self.stats.status = "degraded"
            return False

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming client connection, parse request, and establish tunnel."""
        self.stats.connections_total += 1
        self.stats.connections_active += 1
        self.stats.last_active_at = time.time()

        try:
            # Read client initial request line
            header_bytes = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionResetError):
            writer.close()
            await writer.wait_closed()
            self.stats.connections_active -= 1
            return

        header_text = header_bytes.decode("utf-8", errors="replace")
        lines = header_text.split("\r\n")
        if not lines or not lines[0]:
            writer.close()
            await writer.wait_closed()
            self.stats.connections_active -= 1
            return

        req_line = lines[0].strip()
        parts = req_line.split()
        if len(parts) < 2:
            writer.close()
            await writer.wait_closed()
            self.stats.connections_active -= 1
            return

        method, target = parts[0].upper(), parts[1]

        if not self.current_proxy:
            await self.swap_upstream(reason="init")
            if not self.current_proxy:
                writer.write(b"HTTP/1.1 502 Bad Gateway (No Upstream Proxies Available)\r\n\r\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.stats.connections_active -= 1
                return

        # Attempt connection to upstream proxy with failover
        max_attempts = 2
        for attempt in range(max_attempts):
            upstream_reader = None
            upstream_writer = None
            try:
                p = self.current_proxy
                u_host = p["host"]
                u_port = int(p["port"])
                u_scheme = p["scheme"]
                u_user = p.get("user", "")
                u_pass = p.get("pass", "")

                # Connect to upstream proxy socket
                upstream_reader, upstream_writer = await asyncio.wait_for(
                    asyncio.open_connection(u_host, u_port),
                    timeout=5.0,
                )

                # Perform upstream protocol handshake
                if method == "CONNECT":
                    # HTTPS Tunneling
                    await self._handshake_connect(
                        upstream_reader,
                        upstream_writer,
                        u_scheme,
                        target,
                        u_user,
                        u_pass,
                    )
                    # Notify client tunnel is ready
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await writer.drain()
                else:
                    # Plain HTTP Proxying: forward initial header to upstream
                    await self._handshake_http(
                        upstream_reader,
                        upstream_writer,
                        u_scheme,
                        header_bytes,
                        target,
                        u_user,
                        u_pass,
                    )

                # Successful handshake: splice streams bidirectionally
                self._consecutive_errors = 0
                self.stats.status = "healthy"
                await self._splice(reader, writer, upstream_reader, upstream_writer)
                break

            except Exception as exc:
                self.stats.errors_total += 1
                self._consecutive_errors += 1
                logger.warning(
                    "Slot %d upstream %s failed on attempt %d: %s",
                    self.slot_id,
                    self.stats.upstream,
                    attempt + 1,
                    exc,
                )
                if upstream_writer:
                    try:
                        upstream_writer.close()
                        await upstream_writer.wait_closed()
                    except Exception:
                        pass

                # If handshake failed, hot-swap upstream and retry once if client hasn't disconnected
                if attempt < max_attempts - 1 and not reader.at_eof():
                    swapped = await self.swap_upstream(reason="conn_drop")
                    if swapped:
                        continue

                # Final failure: reply 504 to client
                if not writer.is_closing():
                    try:
                        writer.write(b"HTTP/1.1 504 Gateway Timeout\r\n\r\n")
                        await writer.drain()
                    except Exception:
                        pass
                break

        self.stats.connections_active = max(0, self.stats.connections_active - 1)
        if not writer.is_closing():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handshake_connect(
        self,
        u_reader: asyncio.StreamReader,
        u_writer: asyncio.StreamWriter,
        scheme: str,
        target: str,
        user: str = "",
        pw: str = "",
    ):
        """Establish tunnel to target via HTTP or SOCKS upstream."""
        target_host, target_port_str = target.split(":", 1) if ":" in target else (target, "443")
        target_port = int(target_port_str)

        if scheme in ("socks5", "socks5h"):
            # SOCKS5 Handshake
            await self._socks5_connect(u_reader, u_writer, target_host, target_port, user, pw)
        elif scheme in ("socks4", "socks4a"):
            # SOCKS4 Handshake
            await self._socks4_connect(u_reader, u_writer, target_host, target_port, user)
        else:
            # HTTP CONNECT Handshake
            auth_header = ""
            if user or pw:
                import base64
                token = base64.b64encode(f"{user}:{pw}".encode()).decode()
                auth_header = f"Proxy-Authorization: Basic {token}\r\n"

            connect_req = f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n{auth_header}\r\n"
            u_writer.write(connect_req.encode())
            await u_writer.drain()

            resp = await asyncio.wait_for(u_reader.readuntil(b"\r\n\r\n"), timeout=6.0)
            status_line = resp.split(b"\r\n")[0].decode("latin1", errors="replace")
            if not ("200" in status_line or "Connection established" in status_line):
                raise ConnectionError(f"HTTP CONNECT rejected by upstream: {status_line}")

    async def _handshake_http(
        self,
        u_reader: asyncio.StreamReader,
        u_writer: asyncio.StreamWriter,
        scheme: str,
        initial_headers: bytes,
        target_url: str,
        user: str = "",
        pw: str = "",
    ):
        """Forward plain HTTP request to upstream proxy."""
        if scheme in ("socks5", "socks5h", "socks4", "socks4a"):
            # If upstream is SOCKS, we must tunnel to target host:80 first
            parsed = urlsplit(target_url)
            host = parsed.hostname or "localhost"
            port = parsed.port or 80
            if scheme.startswith("socks5"):
                await self._socks5_connect(u_reader, u_writer, host, port, user, pw)
            else:
                await self._socks4_connect(u_reader, u_writer, host, port, user)

            # Strip full URL from request line: GET http://target.com/path -> GET /path
            path_only = parsed.path or "/"
            if parsed.query:
                path_only += f"?{parsed.query}"
            lines = initial_headers.split(b"\r\n")
            first_parts = lines[0].split(b" ")
            first_parts[1] = path_only.encode()
            lines[0] = b" ".join(first_parts)
            u_writer.write(b"\r\n".join(lines))
            await u_writer.drain()
        else:
            # Plain HTTP upstream: forward headers directly
            u_writer.write(initial_headers)
            await u_writer.drain()

    async def _socks5_connect(
        self,
        u_reader: asyncio.StreamReader,
        u_writer: asyncio.StreamWriter,
        target_host: str,
        target_port: int,
        user: str = "",
        pw: str = "",
    ):
        """Perform client-side SOCKS5 handshake."""
        # Greeting: support no-auth (0x00) and user/pass (0x02)
        if user or pw:
            u_writer.write(b"\x05\x02\x00\x02")
        else:
            u_writer.write(b"\x05\x01\x00")
        await u_writer.drain()

        resp = await asyncio.wait_for(u_reader.readexactly(2), timeout=5.0)
        ver, method = resp[0], resp[1]
        if ver != 5:
            raise ConnectionError(f"Invalid SOCKS version: {ver}")

        if method == 2 and (user or pw):
            # Username/Password auth
            auth_payload = (
                b"\x01"
                + len(user).to_bytes(1, "big")
                + user.encode()
                + len(pw).to_bytes(1, "big")
                + pw.encode()
            )
            u_writer.write(auth_payload)
            await u_writer.drain()
            auth_resp = await asyncio.wait_for(u_reader.readexactly(2), timeout=5.0)
            if auth_resp[1] != 0:
                raise ConnectionError("SOCKS5 authentication failed")
        elif method != 0:
            raise ConnectionError(f"SOCKS5 method {method} not supported")

        # Connect request (CMD 1: CONNECT)
        port_bytes = target_port.to_bytes(2, "big")
        try:
            ip_obj = ipaddress.ip_address(target_host)
            if ip_obj.version == 4:
                addr_payload = b"\x01" + ip_obj.packed
            else:
                addr_payload = b"\x04" + ip_obj.packed
        except ValueError:
            # Domain name (ATYP 0x03)
            host_bytes = target_host.encode("utf-8")
            addr_payload = b"\x03" + len(host_bytes).to_bytes(1, "big") + host_bytes

        connect_payload = b"\x05\x01\x00" + addr_payload + port_bytes
        u_writer.write(connect_payload)
        await u_writer.drain()

        # Reply: read header (4 bytes)
        reply = await asyncio.wait_for(u_reader.readexactly(4), timeout=6.0)
        rep = reply[1]
        if rep != 0:
            raise ConnectionError(f"SOCKS5 connect rejected with status code 0x{rep:02x}")

        atyp = reply[3]
        if atyp == 1:
            await u_reader.readexactly(4 + 2)  # IPv4 + port
        elif atyp == 3:
            domain_len = (await u_reader.readexactly(1))[0]
            await u_reader.readexactly(domain_len + 2)
        elif atyp == 4:
            await u_reader.readexactly(16 + 2)  # IPv6 + port

    async def _socks4_connect(
        self,
        u_reader: asyncio.StreamReader,
        u_writer: asyncio.StreamWriter,
        target_host: str,
        target_port: int,
        user: str = "",
    ):
        """Perform client-side SOCKS4/SOCKS4a handshake."""
        port_bytes = target_port.to_bytes(2, "big")
        user_bytes = user.encode("utf-8") + b"\x00" if user else b"\x00"

        try:
            ip_obj = ipaddress.IPv4Address(target_host)
            payload = b"\x04\x01" + port_bytes + ip_obj.packed + user_bytes
        except ValueError:
            # SOCKS4a domain name extension: IP = 0.0.0.1, append domain after user
            payload = b"\x04\x01" + port_bytes + b"\x00\x00\x00\x01" + user_bytes + target_host.encode() + b"\x00"

        u_writer.write(payload)
        await u_writer.drain()

        resp = await asyncio.wait_for(u_reader.readexactly(8), timeout=6.0)
        if resp[1] != 0x5A:  # 0x5A = request granted
            raise ConnectionError(f"SOCKS4 connect rejected with status code 0x{resp[1]:02x}")

    async def _splice(
        self,
        c_reader: asyncio.StreamReader,
        c_writer: asyncio.StreamWriter,
        u_reader: asyncio.StreamReader,
        u_writer: asyncio.StreamWriter,
    ):
        """Splice client and upstream TCP streams bidirectionally with throughput accounting."""
        async def forward(src: asyncio.StreamReader, dst: asyncio.StreamWriter, is_client_to_upstream: bool):
            try:
                while True:
                    data = await src.read(65536)
                    if not data:
                        break
                    dst.write(data)
                    await dst.drain()
                    if is_client_to_upstream:
                        self.stats.bytes_sent += len(data)
                    else:
                        self.stats.bytes_received += len(data)
            except Exception:
                pass
            finally:
                if not dst.is_closing():
                    try:
                        dst.close()
                    except Exception:
                        pass

        await asyncio.gather(
            forward(c_reader, u_writer, True),
            forward(u_reader, c_writer, False),
            return_exceptions=True,
        )

    async def start(self):
        """Start listening on the designated port."""
        self.server = await asyncio.start_server(
            self.handle_client,
            host=self.host,
            port=self.port,
            reuse_port=hasattr(os, "SO_REUSEPORT"),
        )
        logger.info("Slot %d listening on %s:%d", self.slot_id, self.host, self.port)

    async def stop(self):
        """Stop listening."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()


class GatewayServer:
    """Manages an array of static proxy slots with hot-swapping and background qualification."""

    def __init__(
        self,
        slots_count: int = 10,
        base_port: int = 3030,
        host: str = "127.0.0.1",
        pool: Optional[ProxyPool] = None,
        target_site: Optional[str] = None,
        cache_dir: Optional[str | Path] = None,
    ):
        self.slots_count = max(1, slots_count)
        self.base_port = base_port
        self.host = host
        self.cache_dir = Path(cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.pool = pool or ProxyPool(cache_dir=self.cache_dir)
        self.target_site = target_site

        self.status_file = self.cache_dir / "gateway_status.json"
        self.slots: List[ProxySlot] = []
        self._running = False
        self._bg_tasks: List[asyncio.Task] = []

    def _init_slots(self):
        """Initialize slot instances."""
        self.slots = []
        for i in range(self.slots_count):
            slot = ProxySlot(
                slot_id=i,
                port=self.base_port + i,
                host=self.host,
                pool=self.pool,
                target_site=self.target_site,
                on_swap=lambda s: self._save_status_snapshot(),
            )
            # Pre-assign proxy if pool has one
            candidate = self.pool.pick(site=self.target_site)
            if candidate:
                slot.assign_proxy(candidate)
            self.slots.append(slot)

    def _save_status_snapshot(self):
        """Write live slot status to gateway_status.json."""
        snapshot = {
            "updated_at": time.time(),
            "host": self.host,
            "slots_count": len(self.slots),
            "base_port": self.base_port,
            "target_site": self.target_site,
            "slots": [asdict(s.stats) for s in self.slots],
        }
        temp_file = self.status_file.with_suffix(f".tmp.{os.getpid()}")
        try:
            temp_file.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
            temp_file.replace(self.status_file)
        except Exception as exc:
            logger.warning("Failed to save gateway status snapshot: %s", exc)

    async def _health_keeper_loop(self):
        """Background loop warming and verifying proxies for slot rotation."""
        while self._running:
            try:
                # Save status snapshot periodically
                self._save_status_snapshot()

                # Check if any slot is unassigned or degraded
                for slot in self.slots:
                    if not slot.current_proxy or slot.stats.status == "degraded":
                        candidate = self.pool.pick(site=self.target_site)
                        if candidate:
                            slot.assign_proxy(candidate)

                self.pool.flush_health()
            except Exception as exc:
                logger.debug("Error in health keeper loop: %s", exc)

            await asyncio.sleep(5.0)

    async def _dashboard_loop(self):
        """Live terminal dashboard if running interactively."""
        while self._running:
            if sys.stdout.isatty():
                # ANSI clear screen & move to top
                sys.stdout.write("\033[2J\033[H")
                sys.stdout.write(f"=== p-rox Local Proxy Gateway ({len(self.slots)} slots on {self.host}) ===\n")
                sys.stdout.write(f"Target: {self.target_site or 'All Sites'}\n")
                sys.stdout.write(f"{'Slot':<6} {'Port':<8} {'Status':<10} {'Country':<8} {'Active':<8} {'Traffic (KB)':<14} {'Upstream Proxy'}\n")
                sys.stdout.write("-" * 80 + "\n")
                for s in self.slots:
                    kb = (s.stats.bytes_received + s.stats.bytes_sent) / 1024
                    sys.stdout.write(
                        f"{s.slot_id:<6} {s.port:<8} {s.stats.status:<10} {s.stats.country or 'N/A':<8} "
                        f"{s.stats.connections_active:<8} {kb:<14.1f} {s.stats.upstream}\n"
                    )
                sys.stdout.write("\nPress Ctrl+C to stop.\n")
                sys.stdout.flush()
            await asyncio.sleep(2.0)

    async def start(self, interactive: bool = True):
        """Start all slot listeners and background loops."""
        self._running = True
        self._init_slots()

        # Start all slot servers
        for slot in self.slots:
            await slot.start()

        self._save_status_snapshot()

        # Start background health keeper
        self._bg_tasks.append(asyncio.create_task(self._health_keeper_loop()))

        if interactive and sys.stdout.isatty():
            self._bg_tasks.append(asyncio.create_task(self._dashboard_loop()))

        logger.info(
            "p-rox gateway ready: %d slots listening on %s:%d..%d",
            self.slots_count,
            self.host,
            self.base_port,
            self.base_port + self.slots_count - 1,
        )

    async def stop(self):
        """Stop all listeners and cleanup."""
        self._running = False
        for task in self._bg_tasks:
            task.cancel()
        for slot in self.slots:
            await slot.stop()
        self._save_status_snapshot()
        logger.info("p-rox gateway stopped.")
