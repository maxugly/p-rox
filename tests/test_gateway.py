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

