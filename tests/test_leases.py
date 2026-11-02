"""Tests for ProxyLeases identity-based mutual exclusion."""

import threading
import time
import pytest
from proxy_collector.pool import ProxyPool
from proxy_collector.leases import ProxyLeases


def test_lease_mutual_exclusion(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "80")
    pool._add("http", "1.1.1.2", "80")
    p1 = pool.proxies[0]

    # Holding lease on site1 prevents another worker from leasing the same proxy on site1
    with pool.leases.lease("site1.com", specific=p1):
        with pytest.raises(TimeoutError):
            with pool.leases.lease("site1.com", specific=p1, timeout=0.05):
                pass

        # But cross-site reuse on site2 IS allowed!
        with pool.leases.lease("site2.com", specific=p1, timeout=0.05) as other:
            assert other["host"] == "1.1.1.1"


def test_egress_identity_sharing(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    # Two different endpoints sharing the same egress IP
    pool._add("http", "1.1.1.1", "8080")
    pool._add("http", "1.1.1.2", "9090")
    p1, p2 = pool.proxies

    k1 = pool._key(p1["scheme"], p1["host"], p1["port"], p1["user"])
    k2 = pool._key(p2["scheme"], p2["host"], p2["port"], p2["user"])
    pool.health.setdefault(k1, {})["egress"] = "203.0.113.50"
    pool.health.setdefault(k2, {})["egress"] = "203.0.113.50"

    with pool.leases.lease("target.com", specific=p1):
        # Attempting to lease p2 on target.com should block because egress identity is busy
        with pytest.raises(TimeoutError):
            with pool.leases.lease("target.com", specific=p2, timeout=0.05):
                pass


def test_staged_qualification_leasing(tmp_path):
    pool = ProxyPool(path=None, cache_dir=tmp_path)
    pool._add("http", "1.1.1.1", "80")
    p = pool.proxies[0]

    # Without L1 qualification, min_stage=1 raises TimeoutError
    with pytest.raises(TimeoutError):
        with pool.leases.lease("site.com", min_stage=1, timeout=0.05):
            pass

    # Qualify L1
    pool.ok(p)
    with pool.leases.lease("site.com", min_stage=1, timeout=0.05) as leased:
        assert leased["host"] == "1.1.1.1"

    # Without L2 qualification on site.com, min_stage=2 raises TimeoutError
    with pytest.raises(TimeoutError):
        with pool.leases.lease("site.com", min_stage=2, timeout=0.05):
            pass

    # Qualify L2 on site.com
    pool.site_ok(p, "site.com")
    with pool.leases.lease("site.com", min_stage=2, timeout=0.05) as leased2:
        assert leased2["host"] == "1.1.1.1"
