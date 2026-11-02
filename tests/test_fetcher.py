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
