"""GeoIP resolution and database management supporting DB-IP Lite and MaxMind GeoLite2."""

from datetime import datetime
import gzip
import io
import json
import logging
import os
from pathlib import Path
import re
import tarfile
from typing import Any, Callable, Dict, Optional, Tuple
import urllib.request

from .models import GeoLocation

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser() / "p-rox"

# Well-known datacenter / cloud / VPS providers for heuristic classification
DATACENTER_KEYWORDS = {
    "amazon", "aws", "google", "microsoft", "azure", "digitalocean", "ovh",
    "hetzner", "linode", "akamai", "cloudflare", "fastly", "vultr", "leaseweb",
    "alibaba", "tencent", "oracle", "choopa", "m247", "datapacket", "scaleway",
    "datacenter", "data center", "hosting", "servers", "cloud", "dedicated",
    "colocation", "vps", "cdn", "telecom italia sparkler", "cogent", "level 3",
}

try:
    import maxminddb
except ImportError:
    maxminddb = None


class GeoIPResolver:
    """Fast, local binary MMDB reader supporting DB-IP Lite and MaxMind GeoLite2 databases."""

    def __init__(
        self,
        city_db_path: Optional[str | Path] = None,
        asn_db_path: Optional[str | Path] = None,
        cache_dir: Optional[str | Path] = None,
    ):
        self.cache_dir = Path(cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
        self.city_db_path = Path(city_db_path) if city_db_path else None
        self.asn_db_path = Path(asn_db_path) if asn_db_path else None

        self._city_reader = None
        self._asn_reader = None
        self._source_name = "unknown"

        self._init_readers()

    def _init_readers(self):
        """Auto-discover and open city and ASN MMDB readers."""
        if maxminddb is None:
            logger.debug("maxminddb module not installed; offline GeoIP lookups disabled.")
            return

        # 1. Resolve City DB
        candidate_city_paths = []
        if self.city_db_path and self.city_db_path.is_file():
            candidate_city_paths.append(self.city_db_path)
        if os.environ.get("GEOIP_CITY_PATH"):
            candidate_city_paths.append(Path(os.environ["GEOIP_CITY_PATH"]))

        # Common filenames in cache directory
        candidate_city_paths.extend([
            self.cache_dir / "GeoLite2-City.mmdb",
            self.cache_dir / "dbip-city-lite.mmdb",
            self.cache_dir / "dbip-country-lite.mmdb",
            self.cache_dir / "GeoLite2-Country.mmdb",
        ])

        for p in candidate_city_paths:
            if p and p.is_file():
                try:
                    self._city_reader = maxminddb.open_database(str(p))
                    self._source_name = "maxmind" if "geolite2" in p.name.lower() else "dbip"
                    logger.debug("Loaded GeoIP city database from %s (%s)", p, self._source_name)
                    self.city_db_path = p
                    break
                except Exception as e:
                    logger.warning("Failed opening GeoIP database at %s: %e", p, e)

        # 2. Resolve ASN DB
        candidate_asn_paths = []
        if self.asn_db_path and self.asn_db_path.is_file():
            candidate_asn_paths.append(self.asn_db_path)
        if os.environ.get("GEOIP_ASN_PATH"):
            candidate_asn_paths.append(Path(os.environ["GEOIP_ASN_PATH"]))

        candidate_asn_paths.extend([
            self.cache_dir / "GeoLite2-ASN.mmdb",
            self.cache_dir / "dbip-asn-lite.mmdb",
        ])

        for p in candidate_asn_paths:
            if p and p.is_file():
                try:
                    self._asn_reader = maxminddb.open_database(str(p))
                    logger.debug("Loaded GeoIP ASN database from %s", p)
                    self.asn_db_path = p
                    break
                except Exception as e:
                    logger.warning("Failed opening GeoIP ASN database at %s: %e", p, e)

    @property
    def is_available(self) -> bool:
        """True if at least one local database is loaded."""
        return self._city_reader is not None or self._asn_reader is not None

    def lookup(self, ip: str) -> GeoLocation:
        """Resolve IP address into a rich GeoLocation dataclass in microseconds."""
        if not ip or not self.is_available:
            return GeoLocation()

        ip = ip.strip()
        loc = GeoLocation(source=self._source_name)

        # 1. City / Country lookup
        if self._city_reader:
            try:
                record = self._city_reader.get(ip)
                if record:
                    # Country
                    country_dict = record.get("country") or record.get("registered_country") or {}
                    loc.country_code = country_dict.get("iso_code", "")
                    loc.country_name = country_dict.get("names", {}).get("en", "")

                    # City
                    city_dict = record.get("city") or {}
                    loc.city = city_dict.get("names", {}).get("en", "")

                    # Subdivisions / Region
                    subdivs = record.get("subdivisions") or []
                    if subdivs:
                        loc.region = subdivs[0].get("names", {}).get("en", "")

                    # Coordinates
                    location_dict = record.get("location") or {}
                    if "latitude" in location_dict:
                        loc.latitude = float(location_dict["latitude"])
                    if "longitude" in location_dict:
                        loc.longitude = float(location_dict["longitude"])

                    # DB-IP single database sometimes includes autonomous_system_number / traits
                    traits = record.get("traits") or {}
                    if "autonomous_system_number" in traits:
                        loc.asn = traits.get("autonomous_system_number")
                    if "autonomous_system_organization" in traits:
                        loc.org = traits.get("autonomous_system_organization", "")
            except Exception as e:
                logger.debug("Error in city lookup for %s: %e", ip, e)

        # 2. ASN lookup (if separate ASN database or traits missing)
        if self._asn_reader and (not loc.asn or not loc.org):
            try:
                asn_record = self._asn_reader.get(ip)
                if asn_record:
                    loc.asn = asn_record.get("autonomous_system_number") or loc.asn
                    loc.org = asn_record.get("autonomous_system_organization") or loc.org
            except Exception as e:
                logger.debug("Error in ASN lookup for %s: %e", ip, e)

        # 3. Classify Datacenter vs Residential/ISP
        if loc.org:
            org_lower = loc.org.lower()
            loc.is_datacenter = any(kw in org_lower for kw in DATACENTER_KEYWORDS)

        return loc

    def close(self):
        """Close open database file handles."""
        if self._city_reader:
            try:
                self._city_reader.close()
            except Exception:
                pass
            self._city_reader = None
        if self._asn_reader:
            try:
                self._asn_reader.close()
            except Exception:
                pass
            self._asn_reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def download_dbip_lite(
    cache_dir: Optional[str | Path] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Path:
    """Download and extract free DB-IP City Lite database (CC-BY-4.0, zero account required)."""
    cdir = Path(cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
    cdir.mkdir(parents=True, exist_ok=True)
    target_path = cdir / "dbip-city-lite.mmdb"

    # Current year and month
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    url = f"https://download.db-ip.com/free/dbip-city-lite-{year_month}.mmdb.gz"

    logger.info("Downloading DB-IP Lite database from %s...", url)
    headers = {"User-Agent": "Mozilla/5.0 (p-rox proxy collector)"}
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            compressed_data = io.BytesIO(resp.read())
        with gzip.GzipFile(fileobj=compressed_data) as gz:
            tmp_path = target_path.with_suffix(".tmp")
            with open(tmp_path, "wb") as f_out:
                f_out.write(gz.read())
            os.replace(tmp_path, target_path)
        logger.info("Successfully installed DB-IP Lite to %s", target_path)
        return target_path
    except Exception as e:
        logger.error("Failed downloading DB-IP Lite (%s): %s", url, e)
        # Try previous month in case new month hasn't been published yet
        prev_month = 12 if now.month == 1 else now.month - 1
        prev_year = now.year - 1 if now.month == 1 else now.year
        fallback_url = f"https://download.db-ip.com/free/dbip-city-lite-{prev_year:04d}-{prev_month:02d}.mmdb.gz"
        logger.info("Retrying with fallback url %s...", fallback_url)
        req_prev = urllib.request.Request(fallback_url, headers=headers)
        with urllib.request.urlopen(req_prev, timeout=30) as resp:
            compressed_data = io.BytesIO(resp.read())
        with gzip.GzipFile(fileobj=compressed_data) as gz:
            tmp_path = target_path.with_suffix(".tmp")
            with open(tmp_path, "wb") as f_out:
                f_out.write(gz.read())
            os.replace(tmp_path, target_path)
        logger.info("Successfully installed DB-IP Lite fallback to %s", target_path)
        return target_path


def download_maxmind_geolite2(
    license_key: str,
    edition_id: str = "GeoLite2-City",
    cache_dir: Optional[str | Path] = None,
) -> Path:
    """Download and extract MaxMind GeoLite2 database using a free account license key."""
    if not license_key:
        raise ValueError("MaxMind license key is required.")

    cdir = Path(cache_dir or DEFAULT_CACHE_DIR).expanduser().resolve()
    cdir.mkdir(parents=True, exist_ok=True)
    target_path = cdir / f"{edition_id}.mmdb"

    url = (
        f"https://download.maxmind.com/app/geoip_download?"
        f"edition_id={edition_id}&license_key={license_key}&suffix=tar.gz"
    )

    logger.info("Downloading MaxMind %s using license key...", edition_id)
    headers = {"User-Agent": "p-rox/0.0.1"}
    req = urllib.request.Request(url, headers=headers)

    with urllib.request.urlopen(req, timeout=45) as resp:
        tar_bytes = io.BytesIO(resp.read())

    with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
        # Find the .mmdb member inside the tar archive directory
        for member in tar.getmembers():
            if member.name.endswith(".mmdb") and edition_id in member.name:
                extracted_file = tar.extractfile(member)
                if extracted_file:
                    tmp_path = target_path.with_suffix(".tmp")
                    with open(tmp_path, "wb") as f_out:
                        f_out.write(extracted_file.read())
                    os.replace(tmp_path, target_path)
                    logger.info("Successfully installed MaxMind %s to %s", edition_id, target_path)
                    return target_path

    raise FileNotFoundError(f"Could not find {edition_id}.mmdb inside downloaded archive.")
