"""Tests for ProxyFetcher."""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from proxy_collector.fetcher import ProxyFetcher
from proxy_collector.models import Feed


def test_fetcher_caching(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    feed = Feed(url="https://example.com/test.txt", source_name="Test", cached_as="test.txt")

    cached_file = cache_dir / "test.txt"
    cached_file.write_text("1.1.1.1:80\n")

    fetcher = ProxyFetcher(cache_dir=cache_dir, ttl_seconds=3600)
    res = fetcher.fetch_feed(feed, force=False)

    assert res.status == "cached"
    assert res.cached_path == str(cached_file)


def test_fetcher_stale_triggers_download(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    feed = Feed(url="https://example.com/test.txt", source_name="Test", cached_as="test.txt")

    cached_file = cache_dir / "test.txt"
    cached_file.write_text("1.1.1.1:80\n")

    # Make cache stale
    old_time = time.time() - 7200
    import os
    os.utime(cached_file, (old_time, old_time))

    fetcher = ProxyFetcher(cache_dir=cache_dir, ttl_seconds=3600)

    # Mock response
    mock_resp = MagicMock()
    mock_resp.iter_content.return_value = [b"2.2.2.2:80\n"]
    mock_resp.raise_for_status.return_value = None
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp):
        res = fetcher.fetch_feed(feed, force=False)
        assert res.status == "downloaded"
        assert cached_file.read_text() == "2.2.2.2:80\n"


def test_fetcher_handles_network_failure_with_existing_cache(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    feed = Feed(url="https://example.com/test.txt", source_name="Test", cached_as="test.txt")

    cached_file = cache_dir / "test.txt"
    cached_file.write_text("1.1.1.1:80\n")

    fetcher = ProxyFetcher(cache_dir=cache_dir, ttl_seconds=10)

    with patch("requests.Session.get", side_effect=ConnectionError("Network down")):
        res = fetcher.fetch_feed(feed, force=True)
        assert res.status == "cached"
        assert "Download failed" in (res.error or "")


def test_fetcher_conditional_304(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    feed = Feed(url="https://example.com/test.txt", source_name="Test", cached_as="test.txt")

    cached_file = cache_dir / "test.txt"
    cached_file.write_text("1.1.1.1:80\n")

    fetcher = ProxyFetcher(cache_dir=cache_dir, ttl_seconds=3600)
    fetcher.set_feed_meta(feed.url, etag="W/\"12345\"", last_modified="Wed, 21 Oct 2026 07:28:00 GMT")

    mock_resp = MagicMock()
    mock_resp.status_code = 304
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp) as mock_get:
        res = fetcher.fetch_feed(feed, check_conditional=True)
        assert res.status == "not_modified"
        assert res.bytes_downloaded == 0
        assert cached_file.read_text() == "1.1.1.1:80\n"

        # Verify conditional headers were sent
        headers_sent = mock_get.call_args[1]["headers"]
        assert headers_sent["If-None-Match"] == "W/\"12345\""
        assert headers_sent["If-Modified-Since"] == "Wed, 21 Oct 2026 07:28:00 GMT"


def test_fetcher_download_saves_meta_and_prev(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    feed = Feed(url="https://example.com/test.txt", source_name="Test", cached_as="test.txt")

    cached_file = cache_dir / "test.txt"
    cached_file.write_text("1.1.1.1:80\n")

    fetcher = ProxyFetcher(cache_dir=cache_dir, ttl_seconds=3600)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"ETag": "\"new-tag\"", "Last-Modified": "Thu, 22 Oct 2026 08:00:00 GMT"}
    mock_resp.iter_content.return_value = [b"2.2.2.2:80\n3.3.3.3:80\n"]
    mock_resp.raise_for_status.return_value = None
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp):
        res = fetcher.fetch_feed(feed, force=True)
        assert res.status == "downloaded"
        assert res.bytes_downloaded > 0
        assert cached_file.read_text() == "2.2.2.2:80\n3.3.3.3:80\n"

        # Verify .prev backup file exists for delta comparison
        prev_file = cache_dir / "test.txt.prev"
        assert prev_file.is_file()
        assert prev_file.read_text() == "1.1.1.1:80\n"

        # Verify metadata was persisted
        meta = fetcher.get_feed_meta(feed.url)
        assert meta["etag"] == "\"new-tag\""
        assert meta["last_modified"] == "Thu, 22 Oct 2026 08:00:00 GMT"

