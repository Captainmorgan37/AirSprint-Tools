from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import os
import resource
from typing import Iterable


@dataclass
class FileDescriptorUsage:
    """Snapshot of file descriptor usage for the current process."""

    open_total: int
    soft_limit: int
    hard_limit: int
    usage_pct: float
    counts_by_type: Counter[str]
    top_targets: list[tuple[str, int]]


_FD_DIR = Path("/proc/self/fd")


def _iter_fd_targets(fd_dir: Path = _FD_DIR) -> Iterable[str]:
    """Yield the target path for each open file descriptor.

    If the ``/proc`` entry disappears while iterating we simply skip it to keep the
    snapshot generation resilient.
    """

    if not fd_dir.exists():
        return []

    for entry in fd_dir.iterdir():
        try:
            yield os.readlink(entry)
        except OSError:
            # The descriptor may have closed between ``iterdir`` and ``readlink``.
            continue


def _classify_target(target: str) -> str:
    if target.startswith("socket:"):
        return "socket"
    if target.startswith("pipe:"):
        return "pipe"
    if target.startswith("anon_inode:"):
        return "anon_inode"
    if target.startswith("eventfd:"):
        return "eventfd"
    if target.startswith("/"):
        return "file"
    return "other"


def collect_fd_usage(fd_dir: Path = _FD_DIR) -> FileDescriptorUsage:
    """Return a breakdown of file descriptors and limits for the current process."""

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)

    counts_by_type: Counter[str] = Counter()
    target_counts: Counter[str] = Counter()

    for target in _iter_fd_targets(fd_dir):
        counts_by_type[_classify_target(target)] += 1
        target_counts[target] += 1

    open_total = sum(counts_by_type.values())
    usage_pct = (open_total / soft_limit * 100) if soft_limit else 0.0

    top_targets = target_counts.most_common(20)

    return FileDescriptorUsage(
        open_total=open_total,
        soft_limit=soft_limit,
        hard_limit=hard_limit,
        usage_pct=usage_pct,
        counts_by_type=counts_by_type,
        top_targets=top_targets,
    )
