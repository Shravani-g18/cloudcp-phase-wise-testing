from __future__ import annotations

import fnmatch
import random
from typing import List

import paramiko

from functions.remote_ops import expand_remote_home, run_remote


def list_source_datasets(source_client: paramiko.SSHClient, source_dir: str, pattern: str = "*.yaml") -> List[str]:
    resolved = expand_remote_home(source_client, source_dir)
    cmd = f"find {resolved} -maxdepth 1 -type f -name '*.yaml' -printf '%f\n'"
    code, out, err = run_remote(source_client, cmd)
    if code != 0:
        raise RuntimeError(f"Failed to list datasets on source host: {err.strip() or out.strip()}")
    names = sorted({line.strip() for line in out.splitlines() if line.strip()})
    return [name for name in names if fnmatch.fnmatch(name, pattern)]


def resolve_dataset_selection(
    available: List[str],
    explicit: List[str],
    use_random: bool,
    random_count: int,
    max_datasets: int,
) -> List[str]:
    if explicit:
        missing = sorted(set(explicit) - set(available))
        if missing:
            raise ValueError(f"Explicit dataset(s) not found on source host: {missing}")
        selected = list(explicit)
    else:
        selected = list(available)

    if use_random:
        if random_count <= 0:
            raise ValueError("random_count must be > 0 when random mode is enabled.")
        k = min(random_count, len(selected))
        selected = random.sample(selected, k)

    if max_datasets > 0:
        selected = selected[:max_datasets]

    if not selected:
        raise ValueError("No datasets selected after applying filters.")

    return selected
