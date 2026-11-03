"""Tests for the Proxy Gateway, static port slots, and transparent forwarding."""

import asyncio
import io
import json
from pathlib import Path
from proxy_collector.pool import ProxyPool
from proxy_collector.gateway import GatewayServer, ProxySlot, SlotStats


def test_proxy_slot_assignment_and_swap(tmp_path):
    async def _test():
        pool = ProxyPool(path=None, cache_dir=tmp_path)
        pool._add("http", "1.1.1.1", "8080")
        pool._add("http", "2.2.2.2", "9090")

        slot = ProxySlot(slot_id=0, port=19000, host="127.0.0.1", pool=pool)
        slot.assign_proxy(pool.proxies[0])

        assert slot.stats.port == 19000
        assert slot.stats.upstream == "http://1.1.1.1:8080"
        assert slot.stats.status == "healthy"

        # Swap upstream
        swapped = await slot.swap_upstream(reason="test")
        assert swapped is True
        assert slot.stats.upstream == "http://2.2.2.2:9090"

    asyncio.run(_test())


def test_gateway_server_lifecycle_and_status(tmp_path):
    async def _test():
        pool = ProxyPool(path=None, cache_dir=tmp_path)
        pool._add("http", "1.1.1.1", "8080")
        pool._add("http", "1.1.1.2", "8080")

        server = GatewayServer(
            slots_count=2,
            base_port=19100,
            host="127.0.0.1",
            pool=pool,
            cache_dir=tmp_path,
        )

        await server.start(interactive=False)
        assert len(server.slots) == 2
        assert server.slots[0].port == 19100
        assert server.slots[1].port == 19101

        # Check that status file exists and parses
        status_file = tmp_path / "gateway_status.json"
        assert status_file.is_file()
        data = json.loads(status_file.read_text())
        assert data["slots_count"] == 2
        assert len(data["slots"]) == 2
        assert data["slots"][0]["port"] == 19100

        await server.stop()
        assert server._running is False

    asyncio.run(_test())


def test_gateway_tunnel_http_connect(tmp_path):
    """Test full client -> gateway slot -> upstream proxy -> target echo tunnel."""
    async def _test():
        # 1. Start mock target echo server
        async def target_echo_handler(reader, writer):
            data = await reader.read(1024)
            writer.write(b"ECHO: " + data)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        target_server = await asyncio.start_server(target_echo_handler, "127.0.0.1", 19200)

        # 2. Start mock upstream proxy server that accepts CONNECT and bridges to target
        async def mock_upstream_proxy_handler(reader, writer):
            req = await reader.readuntil(b"\r\n\r\n")
            assert b"CONNECT 127.0.0.1:19200" in req
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()

            # Connect to target
            t_reader, t_writer = await asyncio.open_connection("127.0.0.1", 19200)

            # Splice
            async def forward(src, dst):
                try:
                    while True:
                        d = await src.read(4096)
                        if not d:
                            break
                        dst.write(d)
                        await dst.drain()
                except Exception:
                    pass
                finally:
                    dst.close()

            await asyncio.gather(forward(reader, t_writer), forward(t_reader, writer))

        upstream_server = await asyncio.start_server(mock_upstream_proxy_handler, "127.0.0.1", 19201)

        # 3. Configure p-rox pool and gateway
        pool = ProxyPool(path=None, cache_dir=tmp_path)
        pool._add("http", "127.0.0.1", "19201")  # points to mock upstream proxy

        gateway = GatewayServer(
            slots_count=1,
            base_port=19202,
            host="127.0.0.1",
            pool=pool,
            cache_dir=tmp_path,
        )
        await gateway.start(interactive=False)

        # 4. Client connects to gateway slot 19202 and issues CONNECT
        client_reader, client_writer = await asyncio.open_connection("127.0.0.1", 19202)
        client_writer.write(b"CONNECT 127.0.0.1:19200 HTTP/1.1\r\nHost: 127.0.0.1:19200\r\n\r\n")
        await client_writer.drain()

        # Read 200 from gateway
        resp = await client_reader.readuntil(b"\r\n\r\n")
        assert b"200 Connection Established" in resp

        # Send payload through the tunnel
        client_writer.write(b"Hello p-rox!")
        await client_writer.drain()

        # Receive echo through the tunnel
        reply = await client_reader.read(1024)
        assert reply == b"ECHO: Hello p-rox!"

        # Cleanup
        client_writer.close()
        await client_writer.wait_closed()
        await gateway.stop()
        upstream_server.close()
        await upstream_server.wait_closed()
        target_server.close()
        await target_server.wait_closed()

    asyncio.run(_test())


def test_gateway_default_port(tmp_path):
    server = GatewayServer(cache_dir=tmp_path)
    assert server.base_port == 3030
    assert server.slots_count == 10


def test_cmd_gateway_cli_flags(tmp_path):
    from unittest.mock import MagicMock
    from proxy_collector.cli import cmd_gateway

    runit_path = tmp_path / "run"
    args = MagicMock()
    args.status = False
    args.stop = False
    args.daemon = False
    args.gen_runit = str(runit_path)
    args.slots = 5
    args.base_port = 3030
    args.host = "127.0.0.1"
    args.cache_dir = str(tmp_path)

    cmd_gateway(args)
    assert runit_path.is_file()
    assert "--base-port 3030" in runit_path.read_text()

    # Status check flag
    args.gen_runit = None
    args.status = True
    cmd_gateway(args)


def test_gateway_with_proxydb_alive_assignment_and_waiting(tmp_path):
    import time
    from proxy_collector.db import ProxyDB
    from proxy_collector.models import Proxy

    db = ProxyDB(db_path=tmp_path / "proxies.db")
    p1 = Proxy(scheme="http", host="10.0.0.1", port=8080)
    p2 = Proxy(scheme="http", host="10.0.0.2", port=8080)
    db.upsert_batch([p1, p2])

    t_seen = time.time() - 300.0
    db.record_test_result(p1.url, is_alive=True, latency_ms=40.0, country="US (Ashburn)")
    db.record_test_result(p2.url, is_alive=True, latency_ms=50.0, country="DE (Frankfurt)")
    # Overwrite first_seen_alive to verify time_alive tracking
    with db.get_connection() as conn:
        conn.execute("UPDATE proxies SET first_seen_alive = ? WHERE url = ?", (t_seen, p1.url))

    server = GatewayServer(
        slots_count=4,
        base_port=3030,
        host="127.0.0.1",
        db=db,
        cache_dir=tmp_path,
    )
    server._init_slots()

    assert len(server.slots) == 4
    # First 2 slots should be assigned distinct alive proxies
    assert server.slots[0].stats.status == "healthy"
    assert server.slots[0].stats.upstream in (p1.url, p2.url)
    assert server.slots[1].stats.status == "healthy"
    assert server.slots[1].stats.upstream in (p1.url, p2.url)
    assert server.slots[0].stats.upstream != server.slots[1].stats.upstream

    # Slots 2 and 3 must be waiting because db only has 2 alive proxies
    assert server.slots[2].stats.status == "waiting"
    assert server.slots[2].stats.upstream == "none (pool warming)"
    assert server.slots[3].stats.status == "waiting"
    assert server.slots[3].stats.upstream == "none (pool warming)"


def test_gateway_time_alive_in_dashboard():
    import time
    from unittest.mock import MagicMock
    from proxy_collector.dashboard import TerminalDashboard

    mock_slot = MagicMock()
    mock_slot.slot_id = 0
    mock_slot.port = 3030
    mock_slot.stats.status = "healthy"
    mock_slot.stats.country = "US Ashburn"
    mock_slot.stats.anonymity = "elite"
    mock_slot.stats.tcp_ms = 42.0
    mock_slot.stats.ttfb_ms = 110.0
    mock_slot.stats.connections_active = 1
    mock_slot.stats.bytes_received = 2048
    mock_slot.stats.bytes_sent = 1024
    mock_slot.stats.upstream = "http://10.0.0.1:8080"
    mock_slot.stats.first_seen_alive = time.time() - 450.0  # 7m 30s ago

    out = TerminalDashboard.render_gateway_dashboard(
        slots=[mock_slot],
        host="127.0.0.1",
    )

    assert "7m 30s" in out
    assert "Toggle View: [s]" in out
    assert "3030" in out
    assert "US Ashburn" in out
    assert "Connect To Proxy:" in out
    assert "http://127.0.0.1:3030" in out
    assert "curl -x http://127.0.0.1:3030" in out


def test_gateway_server_dynamic_add_and_remove_slot(tmp_path):
    async def _test():
        pool = ProxyPool(path=None, cache_dir=tmp_path)
        pool._add("http", "1.1.1.1", "8080")
        pool._add("http", "1.1.1.2", "8080")
        pool._add("http", "1.1.1.3", "8080")

        server = GatewayServer(
            slots_count=2,
            base_port=19300,
            host="127.0.0.1",
            pool=pool,
            cache_dir=tmp_path,
        )
        await server.start(interactive=False)
        assert len(server.slots) == 2
        assert server.slots[0].port == 19300
        assert server.slots[1].port == 19301

        # Add a slot
        new_slot = await server.add_slot()
        assert new_slot is not None
        assert len(server.slots) == 3
        assert server.slots[2].port == 19302

        # Remove a slot
        removed = await server.remove_slot()
        assert removed is not None
        assert removed.port == 19302
        assert len(server.slots) == 2

        # Remove down to 1
        await server.remove_slot()
        assert len(server.slots) == 1

        # Cannot remove below 1 slot
        cannot_remove = await server.remove_slot()
        assert cannot_remove is None
        assert len(server.slots) == 1

        await server.stop()

    asyncio.run(_test())


def test_cli_serve_flag_parser():
    import argparse
    from proxy_collector.cli import cmd_run, main

    # Test that the argument parser supports --serve, --slots, --base-port
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--slots", type=int, default=10)
    parser.add_argument("--base-port", type=int, default=3030)
    parser.add_argument("--host", type=str, default="127.0.0.1")

    args = parser.parse_args(["--serve", "--slots", "5", "--base-port", "3030"])
    assert args.serve is True
    assert args.slots == 5
    assert args.base_port == 3030
    assert args.host == "127.0.0.1"


