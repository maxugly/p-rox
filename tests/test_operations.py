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
    p1 = Proxy(scheme="http", host="10.0.0.1", port=8080)
    p2 = Proxy(scheme="http", host="10.0.0.2", port=8080)
    db.upsert_batch([p1, p2])

    stop_event = threading.Event()
    results = []

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
    )

    def fake_profile(proxy):
        if proxy.host == "10.0.0.1":
            return profile_alive
        return profile_dead

    def on_result(candidate, is_alive, profile):
        results.append((candidate, is_alive))
        if len(results) >= 2:
            stop_event.set()

    with patch("proxy_collector.operations.ProxyProfiler") as mock_prof_cls:
        mock_prof = MagicMock()
        mock_prof.profile.side_effect = fake_profile
        mock_prof_cls.return_value = mock_prof

        ops.run_continuous_worker(
            db=db,
            workers=2,
            batch_size=10,
            stop_event=stop_event,
            on_result=on_result,
        )

    assert len(results) == 2
    alive_urls = [r[0]["url"] for r in results if r[1]]
    dead_urls = [r[0]["url"] for r in results if not r[1]]
    assert p1.url in alive_urls
    assert p2.url in dead_urls
    assert db.get_stats()["alive"] == 1
    assert db.get_stats()["dead"] == 1


def test_background_geoip_worker(tmp_path):
    class MockResolver:
        is_available = True

        def lookup(self, ip: str):
            from proxy_collector.models import GeoLocation
            if ip == "1.1.1.1":
                return GeoLocation(country_code="US", city="Chicago", org="TestOrg US")
            return GeoLocation(country_code="DE", city="Frankfurt", org="TestOrg DE")

    db = ProxyDB(db_path=tmp_path / "bg_geoip.db")
    p1 = Proxy(scheme="http", host="1.1.1.1", port=8080)
    p2 = Proxy(scheme="http", host="2.2.2.2", port=8080)
    db.upsert_batch([p1, p2], resolve_geoip=False)

    assert db.get_unresolved_geoip_count() == 2

    # Run background worker synchronously
    batches_done = []
    total = ops.run_background_geoip_worker(
        db=db,
        resolver=MockResolver(),
        batch_size=10,
        on_batch_complete=lambda count, tot: batches_done.append((count, tot)),
    )

    assert total == 2
    assert len(batches_done) == 1
    assert db.get_unresolved_geoip_count() == 0

    with db.get_connection() as conn:
        rows = conn.execute("SELECT host, country, city, asn FROM proxies ORDER BY host").fetchall()
        assert rows[0]["country"] == "US (Chicago)"
        assert rows[0]["city"] == "Chicago"
        assert rows[0]["asn"] == "TestOrg US"
        assert rows[1]["country"] == "DE (Frankfurt)"


def test_start_background_geoip_thread(tmp_path):
    import threading
    import time

    class MockResolver:
        is_available = True

        def lookup(self, ip: str):
            from proxy_collector.models import GeoLocation
            return GeoLocation(country_code="JP", city="Tokyo", org="JP ISP")

    db = ProxyDB(db_path=tmp_path / "thread_geoip.db")
    p = Proxy(scheme="http", host="3.3.3.3", port=8080)
    db.upsert_batch([p], resolve_geoip=False)

    stop = threading.Event()
    thread = ops.start_background_geoip_thread(db=db, resolver=MockResolver(), stop_event=stop)
    assert thread is not None
    assert thread.name == "p-rox-geoip-resolver"
    assert thread.daemon is True

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert db.get_unresolved_geoip_count() == 0

    # Calling again when 0 unresolved proxies exist returns None
    assert ops.start_background_geoip_thread(db=db, resolver=MockResolver(), stop_event=stop) is None


def test_sync_candidates_with_geoip_toggle(tmp_path):
    class MockResolver:
        is_available = True

        def lookup(self, ip: str):
            from proxy_collector.models import GeoLocation
            return GeoLocation(country_code="CA", city="Toronto", org="CA Net")

    db = ProxyDB(db_path=tmp_path / "sync_geoip.db")
    proxies = [Proxy(scheme="http", host="4.4.4.4", port=8080)]

    with patch("proxy_collector.operations.ProxyCollector") as mock_col_cls:
        mock_col = MagicMock()
        mock_col.fetch.return_value = []
        mock_col.collect_stream.return_value = iter(proxies)
        mock_col_cls.return_value = mock_col

        ops.sync_candidates(
            db=db,
            sources_dir=tmp_path,
            resolve_geoip=True,
            geoip_resolver=MockResolver(),
        )

    with db.get_connection() as conn:
        row = conn.execute("SELECT country, city FROM proxies WHERE host = '4.4.4.4'").fetchone()
        assert row["country"] == "CA (Toronto)"
        assert row["city"] == "Toronto"


def test_test_candidates_country_filter(tmp_path):
    db = ProxyDB(db_path=tmp_path / "test_country.db")
    p_us = Proxy(scheme="http", host="1.1.1.1", port=8080)
    p_de = Proxy(scheme="http", host="2.2.2.2", port=8080)
    db.upsert_batch([p_us, p_de], resolve_geoip=False)

    with db.get_connection() as conn:
        conn.execute("UPDATE proxies SET country = 'US (Dallas)' WHERE url = ?", (p_us.url,))
        conn.execute("UPDATE proxies SET country = 'DE (Berlin)' WHERE url = ?", (p_de.url,))

    profile_alive = ProxyProfile(
        proxy=p_us,
        egress_ip="1.1.1.1",
        latency=LatencyMetrics(tcp_handshake_ms=30.0, http_ttfb_ms=100.0),
        security=SecurityMetrics(anonymity="elite"),
        tier=1,
    )

    with patch("proxy_collector.operations.ProxyProfiler") as mock_prof_cls:
        mock_prof = MagicMock()
        mock_prof.profile.return_value = profile_alive
        mock_prof_cls.return_value = mock_prof

        res = ops.test_candidates(db=db, limit=10, country="US")

    assert res["tested"] == 1
    assert res["alive"] == 1
    assert db.get_alive_proxies()[0]["url"] == p_us.url


def test_cmd_sync_with_geoip_intake(tmp_path, capsys):
    import argparse
    from proxy_collector.cli import cmd_sync

    class MockResolver:
        is_available = True

        def lookup(self, ip: str):
            from proxy_collector.models import GeoLocation
            return GeoLocation(country_code="NL", city="Amsterdam", org="NL Net")

    db_file = tmp_path / "intake.db"
    args = argparse.Namespace(
        db=str(db_file),
        sources=str(tmp_path),
        force=False,
        concurrency=1,
        no_geoip=False,
    )

    proxies = [
        Proxy(scheme="http", host="5.5.5.5", port=8080),
        Proxy(scheme="http", host="6.6.6.6", port=8080),
    ]

    with patch("proxy_collector.operations.ProxyCollector") as mock_col_cls, \
         patch("proxy_collector.geoip.GeoIPResolver", return_value=MockResolver()), \
         patch("proxy_collector.db.ProxyDB.upsert_batch", wraps=ProxyDB(db_path=db_file).upsert_batch):
        mock_col = MagicMock()
        mock_col.fetch.return_value = []
        mock_col.collect_stream.return_value = iter(proxies)
        mock_col_cls.return_value = mock_col

        cmd_sync(args)

    out = capsys.readouterr().out
    assert "p-rox sync — Public Feed Ingestion & Candidate Import" in out
    assert "Candidate import complete." in out
    assert "GeoIP Coverage:" in out


def test_run_continuous_worker_priority_country(tmp_path):
    import threading
    db = ProxyDB(db_path=tmp_path / "pri_worker.db")
    p_de = Proxy(scheme="http", host="10.0.0.1", port=8080)
    p_us = Proxy(scheme="http", host="10.0.0.2", port=8080)
    db.upsert_batch([p_de, p_us], resolve_geoip=False)

    with db.get_connection() as conn:
        conn.execute("UPDATE proxies SET country = 'DE (Frankfurt)' WHERE url = ?", (p_de.url,))
        conn.execute("UPDATE proxies SET country = 'US (Ashburn)' WHERE url = ?", (p_us.url,))

    stop_event = threading.Event()
    results = []

    profile_alive = ProxyProfile(
        proxy=p_us,
        egress_ip="10.0.0.2",
        latency=LatencyMetrics(tcp_handshake_ms=25.0, http_ttfb_ms=100.0),
        security=SecurityMetrics(anonymity="elite"),
        tier=1,
    )

    def on_result(candidate, is_alive, profile):
        results.append(candidate["url"])
        stop_event.set()

    with patch("proxy_collector.operations.ProxyProfiler") as mock_prof_cls:
        mock_prof = MagicMock()
        mock_prof.profile.return_value = profile_alive
        mock_prof_cls.return_value = mock_prof

        ops.run_continuous_worker(
            db=db,
            workers=1,
            batch_size=2,
            priority_countries_fn=lambda: ["US"],
            stop_event=stop_event,
            on_result=on_result,
        )

    assert len(results) >= 1
    # US proxy was prioritized and tested first even though DE had a lower row ID
    assert results[0] == p_us.url


def test_sync_candidates_all_unchanged_304(tmp_path):
    from proxy_collector.models import Feed, FetchResult

    db = ProxyDB(db_path=tmp_path / "unchanged.db")
    p1 = Proxy(scheme="http", host="1.1.1.1", port=8080)
    db.upsert_batch([p1])

    f1 = Feed(url="https://example.com/f1.txt", source_name="F1")
    fetch_res = [
        FetchResult(feed=f1, status="not_modified", bytes_downloaded=0),
    ]

    with patch("proxy_collector.operations.ProxyCollector") as mock_col_cls:
        mock_col = MagicMock()
        mock_col.fetch.return_value = fetch_res
        mock_col_cls.return_value = mock_col

        msgs = []
        res = ops.sync_candidates(
            db=db,
            check_conditional=True,
            only_updated=True,
            on_status_msg=lambda m: msgs.append(m),
        )

        assert res["feeds_not_modified"] == 1
        assert res["feeds_downloaded"] == 0
        assert res["newly_added"] == 0
        assert any("up-to-date" in m for m in msgs)
        # Verified collect_stream was NOT called because all feeds were 304!
        assert not mock_col.collect_stream.called


def test_sync_candidates_only_updated_delta(tmp_path):
    from proxy_collector.models import Feed, FetchResult

    db = ProxyDB(db_path=tmp_path / "delta.db")
    p1 = Proxy(scheme="http", host="1.1.1.1", port=8080)
    db.upsert_batch([p1])

    f1 = Feed(url="https://example.com/f1.txt", source_name="F1")
    f2 = Feed(url="https://example.com/f2.txt", source_name="F2")
    fetch_res = [
        FetchResult(feed=f1, status="not_modified", bytes_downloaded=0),
        FetchResult(feed=f2, status="downloaded", bytes_downloaded=500),
    ]

    p_new = Proxy(scheme="http", host="2.2.2.2", port=8080)

    with patch("proxy_collector.operations.ProxyCollector") as mock_col_cls:
        mock_col = MagicMock()
        mock_col.fetch.return_value = fetch_res
        mock_col.collect_stream.return_value = iter([p_new])
        mock_col_cls.return_value = mock_col

        msgs = []
        res = ops.sync_candidates(
            db=db,
            check_conditional=True,
            only_updated=True,
            on_status_msg=lambda m: msgs.append(m),
        )

        assert res["feeds_downloaded"] == 1
        assert res["feeds_not_modified"] == 1
        assert res["newly_added"] == 1
        # Verified collect_stream was called ONLY with downloaded feed f2 and only_delta=True
        mock_col.collect_stream.assert_called_once_with(web_only=True, feeds=[f2], only_delta=True)


def test_background_sync_worker_and_trigger(tmp_path):
    import threading
    import time

    db = ProxyDB(db_path=tmp_path / "bg_sync.db")
    sync_state = {}
    stop_event = threading.Event()
    trigger_event = threading.Event()

    fake_res = {
        "feeds_downloaded": 1,
        "feeds_not_modified": 5,
        "feeds_cached": 0,
        "feeds_failed": 0,
        "newly_added": 42,
    }

    with patch("proxy_collector.operations.sync_candidates", return_value=fake_res) as mock_sync:
        t = ops.start_background_sync_thread(
            db=db,
            sync_state=sync_state,
            stop_event=stop_event,
            trigger_event=trigger_event,
            interval_seconds=86400.0,
        )

        # Trigger immediate sync
        trigger_event.set()
        time_limit = time.time() + 2.0
        while time.time() < time_limit:
            if sync_state.get("last_sync_time", 0) > 0:
                break
            time.sleep(0.02)

        stop_event.set()
        t.join(timeout=2.0)

        assert not t.is_alive()
        assert sync_state["status"] == "idle"
        assert sync_state["last_sync_time"] > 0
        assert "5 unchanged (304), 1 updated (+42 new)" in sync_state["last_sync_result"]
        assert sync_state["new_candidates_added"] == 42





