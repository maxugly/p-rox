"""High-performance proxy normalization, protocol filtering, and deduplication."""

from collections import Counter
from typing import Callable, Iterable, Iterator, List, Optional, Set, TextIO
from pathlib import Path

from .models import (
    ALL_SUPPORTED_SCHEMES,
    STANDARD_SCHEMES,
    Proxy,
)


def proxy_sort_key(p: Proxy) -> tuple:
    """Deterministic sorting key: scheme first, then host, port, and credentials."""
    return (
        p.scheme,
        p.host,
        p.port,
        p.username or "",
        p.raw_uri or "",
    )


class Deduplicator:
    """Deduplicates and filters proxies based on canonical representations."""

    def __init__(
        self,
        protocols: Optional[Iterable[str]] = None,
        web_only: bool = False,
    ):
        if web_only:
            self.protocols: Optional[Set[str]] = set(STANDARD_SCHEMES)
        elif protocols is not None:
            self.protocols = {p.lower().strip() for p in protocols}
        else:
            self.protocols = None

        self.seen_keys: Set[str] = set()
        self.total_seen: int = 0
        self.duplicates_dropped: int = 0
        self.counts_by_protocol: Counter[str] = Counter()

    def reset(self):
        """Reset internal state."""
        self.seen_keys.clear()
        self.total_seen = 0
        self.duplicates_dropped = 0
        self.counts_by_protocol.clear()

    def filter_and_dedup_stream(self, proxies: Iterable[Proxy]) -> Iterator[Proxy]:
        """Streamingly yield unique proxies that match protocol criteria."""
        for p in proxies:
            self.total_seen += 1
            if self.protocols is not None and p.scheme not in self.protocols:
                continue

            key = p.dedup_key
            if key in self.seen_keys:
                self.duplicates_dropped += 1
                continue

            self.seen_keys.add(key)
            self.counts_by_protocol[p.scheme] += 1
            yield p

    def dedup_list(self, proxies: Iterable[Proxy], sort: bool = True) -> List[Proxy]:
        """Deduplicate an iterable of proxies into a unique list, optionally sorted."""
        unique = list(self.filter_and_dedup_stream(proxies))
        if sort:
            unique.sort(key=proxy_sort_key)
        return unique

    def write_to_file(
        self,
        proxies: Iterable[Proxy],
        output_path: str | Path,
        sort: bool = True,
        on_progress: Optional[Callable[[int], None]] = None,
    ) -> int:
        """Write unique proxies to a file, one per line. Returns unique count."""
        out_p = Path(output_path).resolve()
        out_p.parent.mkdir(parents=True, exist_ok=True)

        if sort:
            # Accumulate and sort before writing
            unique_list = self.dedup_list(proxies, sort=True)
            temp_path = out_p.with_suffix(f".tmp.{out_p.name}")
            with open(temp_path, "w", encoding="utf-8") as fh:
                for idx, p in enumerate(unique_list):
                    fh.write(f"{p.url}\n")
                    if on_progress and idx % 50000 == 0:
                        on_progress(idx)
            temp_path.replace(out_p)
            return len(unique_list)

        # Streaming write without sorting
        count = 0
        temp_path = out_p.with_suffix(f".tmp.{out_p.name}")
        with open(temp_path, "w", encoding="utf-8") as fh:
            for p in self.filter_and_dedup_stream(proxies):
                fh.write(f"{p.url}\n")
                count += 1
                if on_progress and count % 50000 == 0:
                    on_progress(count)
        temp_path.replace(out_p)
        return count
