"""Requests session subclass holding an exclusive proxy lease across the entire request/stream lifecycle."""

import time
from typing import Any, Dict, Optional
import requests
from urllib3.util import Timeout

from .pool import ProxyPool, site_key


class RoutedSession(requests.Session):
    """Requests Session holding an exclusive identity-based proxy lease for each request.

    Features:
    - Automatic proxy lease acquisition for the target site from ProxyPool.
    - Strict 8-second total request budget to prevent hung threads.
    - Streaming support: tracks bytes transferred, awards L3 live qualification on completion.
    - Automatic error interception: 407 (auth bench), 403/429 (site cooldown), connection drop (bench).
    - Releases proxy lease only after response body is fully consumed or closed.
    """

    def __init__(
        self,
        pool: Optional[ProxyPool] = None,
        *,
        site: Optional[str] = None,
        min_stage: int = 2,
        stop: Optional[Any] = None,
        connect_timeout: float = 4.0,
        read_timeout: float = 8.0,
        lease_timeout: float = 10.0,
    ):
        super().__init__()
        self.trust_env = False
        self.pool = pool
        self.site = site
        self.min_stage = min_stage
        self.stop = stop
        self.connect_timeout = min(connect_timeout, 8.0)
        self.read_timeout = min(read_timeout, 8.0)
        self.lease_timeout = lease_timeout

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        timeout_arg = kwargs.get("timeout", self.read_timeout)
        if isinstance(timeout_arg, tuple):
            c_to, r_to = timeout_arg
        else:
            c_to, r_to = min(self.connect_timeout, timeout_arg), min(self.read_timeout, timeout_arg)

        kwargs["timeout"] = Timeout(total=8.0, connect=min(c_to, 8.0), read=min(r_to, 8.0))
        self.proxies = {}

        if self.pool is None:
            kwargs["proxies"] = {}
            return super().request(method, url, **kwargs)

        target_site = self.site or url
        lease = self.pool.leases.lease(
            target_site,
            min_stage=self.min_stage,
            timeout=self.lease_timeout,
            stop=self.stop,
        )
        proxy = lease.__enter__()
        route_url = ProxyPool.requests_url(proxy)
        kwargs["proxies"] = {"http": route_url, "https": route_url}

        released = False
        start = time.monotonic()

        def release_lease():
            nonlocal released
            if not released:
                released = True
                lease.__exit__(None, None, None)

        try:
            response = super().request(method, url, **kwargs)
            duration = time.monotonic() - start
            if duration > 8.0:
                response.close()
                raise requests.Timeout("Proxy response exceeded eight seconds")

            if response.status_code == 407:
                self.pool.fail(proxy, "auth")
            elif response.status_code in {403, 429}:
                self.pool.site_fail(proxy, target_site, f"HTTP-{response.status_code}")
            elif response.ok:
                self.pool.site_ok(proxy, target_site, transfer=False, latency=duration)

            orig_close = response.close

            def close_and_release():
                try:
                    orig_close()
                finally:
                    release_lease()

            response.close = close_and_release

            if kwargs.get("stream", False):
                original_iter = response.iter_content

                def tracked_content(*iter_args, **iter_options):
                    count = 0
                    try:
                        for chunk in original_iter(*iter_args, **iter_options):
                            count += len(chunk)
                            yield chunk
                        if response.ok:
                            elapsed = max(0.001, time.monotonic() - start)
                            self.pool.site_ok(
                                proxy,
                                target_site,
                                transfer=True,
                                throughput=count / elapsed,
                            )
                    except requests.RequestException:
                        self.pool.fail(proxy, "conn")
                        raise
                    finally:
                        release_lease()

                response.iter_content = tracked_content
            else:
                release_lease()

            response.route_proxy = proxy
            return response

        except requests.RequestException:
            self.pool.fail(proxy, "conn")
            release_lease()
            raise
        except BaseException:
            release_lease()
            raise
