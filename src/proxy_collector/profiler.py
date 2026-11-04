"""Network profiler: Dual-ping benchmarking, true egress discovery, anonymity tiering, and TLS tampering checks."""

import logging
import socket
import ssl
import time
from typing import Any, Dict, Optional, Tuple
import urllib.parse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .geoip import GeoIPResolver
from .models import GeoLocation, LatencyMetrics, Proxy, ProxyProfile, SecurityMetrics

logger = logging.getLogger(__name__)

# Headers commonly added by transparent or anonymous proxies
PROXY_INDICATOR_HEADERS = [
    "via",
    "x-forwarded-for",
    "x-real-ip",
    "client-ip",
    "forwarded",
    "proxy-connection",
    "x-proxy-id",
    "x-bluecoat-via",
    "cf-connecting-ip",
]

DEFAULT_ECHO_HTTP = "http://httpbin.org/get"
DEFAULT_ECHO_HTTPS = "https://cloudflare.com/cdn-cgi/trace"

_CACHED_LOCAL_IP: Optional[str] = None


def discover_local_public_ip(timeout: float = 2.0, force: bool = False) -> Optional[str]:
    """Auto-discover the host machine's public egress IP address.

    Tries fast public echo endpoints in sequence. Caches the result in-memory
    so subsequent calls incur 0 network overhead.
    """
    global _CACHED_LOCAL_IP
    if _CACHED_LOCAL_IP is not None and not force:
        return _CACHED_LOCAL_IP

    endpoints = [
        "https://api.ipify.org",
        "https://cloudflare.com/cdn-cgi/trace",
        "https://icanhazip.com",
        "http://httpbin.org/ip",
    ]
    for url in endpoints:
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "p-rox/0.0.1"})
            if resp.status_code == 200:
                text = resp.text.strip()
                if "ip=" in text:
                    for line in text.splitlines():
                        if line.startswith("ip="):
                            cand = line.split("=", 1)[1].strip()
                            if cand and ("." in cand or ":" in cand):
                                _CACHED_LOCAL_IP = cand
                                return _CACHED_LOCAL_IP
                elif "{" in text:
                    try:
                        data = resp.json()
                        cand = (data.get("origin") or data.get("ip") or "").split(",")[0].strip()
                        if cand and ("." in cand or ":" in cand):
                            _CACHED_LOCAL_IP = cand
                            return _CACHED_LOCAL_IP
                    except Exception:
                        pass
                else:
                    cand = text.split("\n")[0].strip()
                    if cand and ("." in cand or ":" in cand):
                        _CACHED_LOCAL_IP = cand
                        return _CACHED_LOCAL_IP
        except Exception:
            continue

    return None


def reset_cached_local_ip() -> None:
    """Reset cached local IP (primarily for unit test isolation)."""
    global _CACHED_LOCAL_IP
    _CACHED_LOCAL_IP = None


class ProxyProfiler:
    """Profiles candidate proxies: latency, true egress identity, anonymity level, and security integrity."""

    def __init__(
        self,
        geoip_resolver: Optional[GeoIPResolver] = None,
        my_ip: Optional[str] = None,
        timeout: float = 8.0,
        auto_discover_ip: bool = False,
    ):
        self.geoip = geoip_resolver
        if my_ip is not None:
            self.my_ip = my_ip
        elif auto_discover_ip:
            self.my_ip = discover_local_public_ip(timeout=min(2.0, timeout))
        else:
            self.my_ip = None
        self.timeout = timeout

    @staticmethod
    def measure_tcp_handshake(host: str, port: int, timeout: float = 4.0) -> float:
        """Measure round-trip time (SYN -> SYN-ACK) in milliseconds to proxy entry host."""
        start = time.perf_counter()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            return round(elapsed_ms, 2)
        except Exception:
            return float("inf")
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def measure_http_ttfb(
        self,
        proxy: Proxy,
        target_url: str = "http://api.ipify.org",
        timeout: float = 6.0,
    ) -> Tuple[float, Optional[str], Dict[str, str]]:
        """Measure HTTP Time-To-First-Byte (TTFB) in ms, returning (ttfb_ms, body_text, headers)."""
        proxies = proxy.requests_proxies_dict
        start = time.perf_counter()
        try:
            with requests.get(
                target_url,
                proxies=proxies,
                timeout=timeout,
                stream=True,
                headers={"User-Agent": "p-rox/0.0.1"},
            ) as resp:
                # Reading the first 1KB captures the first byte
                chunk = next(resp.iter_content(chunk_size=1024), b"")
                ttfb_ms = (time.perf_counter() - start) * 1000.0
                body = chunk.decode("utf-8", errors="ignore")
                headers = {k.lower(): v for k, v in resp.headers.items()}
                return round(ttfb_ms, 2), body, headers
        except Exception:
            return float("inf"), None, {}

    def check_tls_integrity(
        self,
        proxy: Proxy,
        target_url: str = DEFAULT_ECHO_HTTPS,
        timeout: float = 6.0,
    ) -> Tuple[bool, Optional[str]]:
        """Check if proxy intercepts or tampers with TLS certificates (MitM attack / SSL decrypt).

        Returns (is_tampered, issuer_or_error).
        """
        proxies = proxy.requests_proxies_dict
        try:
            # Verify SSL with standard CA certs
            resp = requests.get(
                target_url,
                proxies=proxies,
                timeout=timeout,
                verify=True,
                headers={"User-Agent": "p-rox/0.0.1"},
            )
            if resp.status_code == 200:
                return False, "Valid CA"
            return False, f"HTTP {resp.status_code}"
        except requests.exceptions.SSLError as e:
            # SSL verification failed -> proxy attempted MitM with invalid/self-signed cert
            return True, f"SSLError: {e}"
        except Exception as e:
            return False, str(e)

    def determine_anonymity(
        self,
        proxy: Proxy,
        echo_url: str = DEFAULT_ECHO_HTTP,
        timeout: float = 6.0,
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """Determine proxy anonymity tier (elite, anonymous, transparent).

        Returns (anonymity_level, egress_ip, leaked_ip).
        """
        proxies = proxy.requests_proxies_dict
        try:
            resp = requests.get(
                echo_url,
                proxies=proxies,
                timeout=timeout,
                headers={"User-Agent": "p-rox/0.0.1"},
            )
            if resp.status_code != 200:
                return "unknown", None, None

            data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            headers = {k.lower(): str(v) for k, v in (data.get("headers") or {}).items()}
            origin = data.get("origin", "")
            egress_ip = origin.split(",")[0].strip() if origin else None

            # Check if my real IP is leaked in the origin or headers
            leaked_ip = None
            if self.my_ip:
                if self.my_ip in origin:
                    leaked_ip = self.my_ip
                for h_val in headers.values():
                    if self.my_ip in h_val:
                        leaked_ip = self.my_ip
                        break

            if leaked_ip:
                return "transparent", egress_ip, leaked_ip

            # Check for proxy signature headers
            has_proxy_headers = any(h in headers for h in PROXY_INDICATOR_HEADERS)
            if has_proxy_headers:
                return "anonymous", egress_ip, None

            return "elite", egress_ip, None
        except Exception:
            # Fallback to simple IP echo if json echo fails
            try:
                resp2 = requests.get(
                    "http://api.ipify.org",
                    proxies=proxies,
                    timeout=timeout,
                    headers={"User-Agent": "p-rox/0.0.1"},
                )
                if resp2.status_code == 200:
                    text = resp2.text.strip()
                    if text and ("." in text or ":" in text):
                        if self.my_ip and self.my_ip in text:
                            return "transparent", text, self.my_ip
                        return "unknown", text, None
            except Exception:
                pass
            return "unknown", None, None

    @staticmethod
    def classify_anonymity_tier(
        base_anon: str,
        tcp_handshake_ms: float,
        http_ttfb_ms: float,
        tls_tampered: bool = False,
        leaked_ip: Optional[str] = None,
    ) -> str:
        """Classify multi-factor composite anonymity tier based on headers, TLS integrity, and latency.

        Tiers:
          - 'tampered': TLS interception/MitM detected (SSL cert spoofed or invalid).
          - 'transparent': Client IP leaked in headers or echo origin.
          - 'slow': Clean or anonymous headers, but high latency (TCP > 800ms or TTFB > 4000ms).
          - 'anonymous': Valid proxy headers or moderate latency (TCP <= 800ms, TTFB <= 4000ms).
          - 'elite': Clean headers, no leak, clean TLS, TCP <= 300ms, and HTTP TTFB <= 1500ms.
          - 'unknown': Unreachable or inconclusive echo response.
        """
        if tls_tampered:
            return "tampered"

        if leaked_ip or base_anon == "transparent":
            return "transparent"

        if tcp_handshake_ms == float("inf"):
            return "unknown"

        if base_anon == "unknown":
            if http_ttfb_ms == float("inf"):
                return "unknown"
            return "unknown"

        # Check for high latency
        if tcp_handshake_ms > 800.0 or http_ttfb_ms > 4000.0:
            return "slow"

        # If proxy indicator headers were present, it cannot be elite
        if base_anon == "anonymous":
            return "anonymous"

        # Clean headers & clean TLS: check if latency meets elite criteria
        if tcp_handshake_ms <= 300.0 and http_ttfb_ms <= 1500.0:
            return "elite"

        return "anonymous"

    def profile(self, proxy: Proxy) -> ProxyProfile:
        """Run complete profiling pipeline across a proxy candidate."""
        profile = ProxyProfile(proxy=proxy, last_tested=time.time())

        # 1. TCP Handshake Latency
        tcp_ms = self.measure_tcp_handshake(proxy.host, proxy.port, timeout=self.timeout / 2)
        if tcp_ms == float("inf"):
            profile.tier = 0
            profile.last_reason = "Unreachable or timeout"
            profile.latency = LatencyMetrics(
                tcp_handshake_ms=float("inf"),
                http_ttfb_ms=float("inf"),
                measured_at=time.time(),
            )
            return profile

        # 2. Base Anonymity & Egress Detection
        base_anon, egress_ip, leaked_ip = self.determine_anonymity(proxy, timeout=self.timeout)
        profile.egress_ip = egress_ip or proxy.host

        # 3. HTTP TTFB Latency
        ttfb_ms, _, _ = self.measure_http_ttfb(proxy, timeout=self.timeout)
        profile.latency = LatencyMetrics(
            tcp_handshake_ms=tcp_ms,
            http_ttfb_ms=ttfb_ms,
            measured_at=time.time(),
        )

        # 4. TLS Tampering Integrity
        is_tampered, issuer = self.check_tls_integrity(proxy, timeout=self.timeout)

        # 5. Composite Anonymity Tier Classification
        final_anon = self.classify_anonymity_tier(
            base_anon=base_anon,
            tcp_handshake_ms=tcp_ms,
            http_ttfb_ms=ttfb_ms,
            tls_tampered=is_tampered,
            leaked_ip=leaked_ip,
        )

        profile.security = SecurityMetrics(
            tls_tampering_detected=is_tampered,
            cert_issuer=issuer,
            anonymity=final_anon,
            leaked_ip=leaked_ip,
        )

        # 6. GeoIP Resolution (lookup true egress IP, fallback to entry host)
        lookup_ip = profile.egress_ip or proxy.host
        if self.geoip:
            profile.geo = self.geoip.lookup(lookup_ip)
        else:
            profile.geo = GeoLocation()

        # Update initial qualification tier
        if tcp_ms != float("inf") and egress_ip:
            profile.tier = 1  # Route-qualified
            if not is_tampered and final_anon == "elite":
                profile.last_reason = "L1 route verified, TLS secure, elite low latency"
            elif not is_tampered and final_anon == "anonymous":
                profile.last_reason = "L1 route verified, TLS secure, anonymous"
            elif final_anon == "slow":
                profile.last_reason = "L1 route verified, high latency"
            elif final_anon == "transparent":
                profile.last_reason = "L1 route verified (client IP leaked)"
            elif is_tampered or final_anon == "tampered":
                profile.last_reason = "L1 route verified (TLS tampering detected)"
            else:
                profile.last_reason = "L1 route verified"
        else:
            profile.last_reason = "Unreachable or timeout"

        return profile
