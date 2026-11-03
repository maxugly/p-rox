"""Tests for GeoIP resolution with DB-IP and MaxMind MMDB mocking."""

import gzip
import io
import tarfile
from unittest.mock import MagicMock, patch

from proxy_collector.geoip import (
    GeoIPResolver,
    download_dbip_lite,
    download_maxmind_geolite2,
)


def test_geoip_lookup_mocked():
    resolver = GeoIPResolver()
    # Mock readers
    mock_city_reader = MagicMock()
    mock_city_reader.get.return_value = {
        "country": {"iso_code": "JP", "names": {"en": "Japan"}},
        "city": {"names": {"en": "Tokyo"}},
        "subdivisions": [{"names": {"en": "Tokyo"}}],
        "location": {"latitude": 35.6895, "longitude": 139.6917},
        "traits": {
            "autonomous_system_number": 2516,
            "autonomous_system_organization": "KDDI Corporation",
        },
    }
    resolver._city_reader = mock_city_reader
    resolver._source_name = "dbip"

    loc = resolver.lookup("1.1.1.1")
    assert loc.country_code == "JP"
    assert loc.country_name == "Japan"
    assert loc.city == "Tokyo"
    assert loc.region == "Tokyo"
    assert loc.latitude == 35.6895
    assert loc.asn == 2516
    assert loc.org == "KDDI Corporation"
    assert loc.is_datacenter is False  # Residential/ISP
    assert loc.source == "dbip"


def test_geoip_datacenter_classification():
    resolver = GeoIPResolver()
    mock_city_reader = MagicMock()
    mock_city_reader.get.return_value = {
        "country": {"iso_code": "US", "names": {"en": "United States"}},
        "city": {"names": {"en": "Ashburn"}},
        "traits": {
            "autonomous_system_number": 16509,
            "autonomous_system_organization": "Amazon.com, Inc.",
        },
    }
    resolver._city_reader = mock_city_reader
    loc = resolver.lookup("3.80.0.1")
    assert loc.country_code == "US"
    assert loc.is_datacenter is True


def test_download_dbip_lite_mocked(tmp_path):
    mock_gz_content = b"fake mmdb binary content"
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(mock_gz_content)
    compressed_bytes = buf.getvalue()

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = compressed_bytes
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        result = download_dbip_lite(cache_dir=tmp_path)
        assert result.is_file()
        assert result.read_bytes() == mock_gz_content


def test_download_maxmind_mocked(tmp_path):
    mock_mmdb_content = b"fake maxmind mmdb content"
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w:gz") as tar:
        tar_info = tarfile.TarInfo(name="GeoLite2-City_20261102/GeoLite2-City.mmdb")
        tar_info.size = len(mock_mmdb_content)
        tar.addfile(tar_info, io.BytesIO(mock_mmdb_content))
    tar_bytes = tar_buf.getvalue()

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = tar_bytes
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        result = download_maxmind_geolite2(license_key="testkey", cache_dir=tmp_path)
        assert result.is_file()
        assert result.read_bytes() == mock_mmdb_content
