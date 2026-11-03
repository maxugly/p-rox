"""Offline unit tests for operations (sync_candidates and test_candidates)."""

from unittest.mock import MagicMock, patch

from proxy_collector.db import ProxyDB
from proxy_collector.models import LatencyMetrics, Proxy, ProxyProfile, SecurityMetrics
import proxy_collector.operations as ops


def test_sync_candidates_mocked(tmp_path):
    db = ProxyDB(db_path=tmp_path / "test.db")

    mock_proxies = [
        Proxy(scheme="http", host="10.0.0.1", port=8080),
        Proxy(scheme="http", host="10.0.0.2", port=8080),
    ]

    with patch("proxy_collector.operations.ProxyCollector") as mock_col_cls:
        mock_col = MagicMock()
        mock_col.fetch.return_value = []
        mock_col.collect_stream.return_value = iter(mock_proxies)
        mock_col_cls.return_value = mock_col

        res = ops.sync_candidates(db=db, sources_dir=tmp_path)

    assert res["newly_added"] == 2
    assert res["total_in_db"] == 2
    assert db.get_stats()["total"] == 2


def test_test_candidates_mocked(tmp_path):
    db = ProxyDB(db_path=tmp_path / "test.db")
    p1 = Proxy(scheme="http", host="10.0.0.1", port=8080)
    p2 = Proxy(scheme="http", host="10.0.0.2", port=8080)
    db.upsert_batch([p1, p2])

    profile_alive = ProxyProfile(
        proxy=p1,
        egress_ip="10.0.0.1",
        latency=LatencyMetrics(tcp_handshake_ms=25.0, http_ttfb_ms=100.0),
        security=SecurityMetrics(anonymity="elite"),
        tier=1,
    )
    profile_dead = ProxyProfile(
        proxy=p2,
        egress_ip="",
        latency=LatencyMetrics(tcp_handshake_ms=float("inf")),
        security=SecurityMetrics(anonymity="unknown"),
        tier=0,
        last_reason="Timeout",
    )

    def fake_profile(proxy):
        if proxy.host == "10.0.0.1":
            return profile_alive
        return profile_dead

    with patch("proxy_collector.operations.ProxyProfiler") as mock_prof_cls:
        mock_prof = MagicMock()
        mock_prof.profile.side_effect = fake_profile
        mock_prof_cls.return_value = mock_prof

        res = ops.test_candidates(db=db, limit=10, workers=2)

    assert res["tested"] == 2
    assert res["alive"] == 1
    assert res["dead"] == 1
    assert db.get_stats()["alive"] == 1
    assert db.get_stats()["dead"] == 1


def test_run_continuous_worker_tracks_died(tmp_path):
    import threading

    db = ProxyDB(db_path=tmp_path / "test.db")
    stop_event = threading.Event()
    ticks = []

    def on_tick(stats, rate):
        ticks.append((stats, rate))
        stop_event.set()

    with patch("proxy_collector.operations.test_candidates") as mock_test:
        mock_test.return_value = {"tested": 10, "alive": 2, "dead": 8}
        ops.run_continuous_worker(
            db=db,
            workers=2,
            batch_size=10,
            stop_event=stop_event,
            on_tick=on_tick,
        )

    assert len(ticks) >= 1
    stats, rate = ticks[0]
    assert stats["session_died"] == 8
    assert stats["total_tested_session"] == 10
    assert "uptime_s" in stats

