"""Tests for ProxyService status, locking, and updating."""

from pathlib import Path
from unittest.mock import MagicMock, patch
from proxy_collector.service import ProxyService


def test_proxy_service_status(tmp_path):
    service = ProxyService(cache=tmp_path, proxy_file=None)
    status = service.status()
    assert "total" in status
    assert "working" in status
    assert status["service_running"] is False


def test_proxy_service_update_and_check(tmp_path):
    # Setup dummy source in cache
    sources_dir = tmp_path / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "test_http.txt").write_text("http://1.2.3.4:8080\nhttp://5.6.7.8:8080\n")

    service = ProxyService(cache=tmp_path, proxy_file=None)
    res = service.update(limit=10)
    assert res["added"] == 2

    # Check status
    status = service.status()
    assert status["total"] == 2
