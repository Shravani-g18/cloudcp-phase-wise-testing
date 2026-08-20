"""Crash-safe, xattr-free, tier-partitioned batch state tracking.

A batch's **state is the directory it lives in** — no append logs, no xattr.
Batches are partitioned by **size tier** (design §5/§10) so the scheduler gets
O(1) per-tier counts by directory listing:

    <transfer_dir>/batches/pending/<tier>/<name>       created, not yet claimed
    <transfer_dir>/batches/inprogress/<tier>/<name>    claimed by a transfer worker
    <transfer_dir>/batches/completed/<tier>/<name>     fully processed

``<tier>`` ∈ {zero, tiny, small, medium, large}. Every transition is a single
``os.rename`` (atomic within a filesystem on POSIX) that **preserves the tier**,
so a crash can never leave a batch half-moved or in two states.

**Backward compatibility:** a *legacy flat* batch written before the tier upgrade
lives directly under the state dir (``batches/pending/<name>``). All readers here
still see it (labelled tier ``unknown``) and all transitions still work, so an
in-flight transfer created before the upgrade resumes cleanly.

Rationale (challenges #14/#15/#16): resume must not depend on a per-file xattr
probe or a full tree re-walk. The set difference is done by ``listdir`` on a
handful of small directories.

Contract with the pipeline:
  * ``bcloud_src_enum.py`` publishes batches (``publish``, tier from BatchBuilder)
    into ``pending/<tier>/`` and prints the path for the scheduler / GNU parallel.
  * ``aws_transfer.py`` derives the tier from the batch path (``parse_batch_path``),
    ``claim``s the batch, and ``complete``s it after processing.
  * The fallback worker completes a batch by *name* only (it does not know the
    tier), so ``claim``/``complete`` locate a batch across tiers when no tier hint
    is given.
  * On resume the enumerator re-emits ``to_run`` (pending + in-progress).
"""

import os
import re


# --- Batch record framing (design §6.3) ------------------------------------
# Records are NUL-framed. Two layouts, auto-detected on read:
#   * path-only (default / legacy):     <path>\0
#   * size-bearing (BATCH_INCLUDE_SIZE): <size>\0<path>\0
# Size is captured once by the enumerator (it already stat()s every file for
# tiering) so cloudcp and every other consumer can reuse it instead of a
# redundant stat. Detection is unambiguous: the size field is bare ASCII digits
# and a real batch path is always absolute (``/…``) or an ``s3://`` URL, never a
# bare integer — so a batch whose first NUL field is all-digits is size-bearing.
def _encode_record(path, size, include_size):
    pb = os.fsencode(path)
    if include_size and size is not None:
        return os.fsencode(str(int(size))) + b"\0" + pb + b"\0"
    return pb + b"\0"


PENDING = "pending"
INPROGRESS = "inprogress"
COMPLETED = "completed"
_STATES = (PENDING, INPROGRESS, COMPLETED)

# Batches live under this subdir of the per-transfer batch-meta directory.
BATCHES_SUBDIR = "batches"

# On-disk size-tier subdirectories (match BatchBuilder bucket names).
TIERS = ("zero", "tiny", "small", "medium", "large")
# Scheduling label for a legacy flat batch (no tier subdir). The scheduler
# treats it at ``medium`` weight (design §5 reconciliation note).
UNKNOWN_TIER = "unknown"


def batches_root(transfer_dir):
    return os.path.join(transfer_dir, BATCHES_SUBDIR)


def _state_dir(transfer_dir, state):
    return os.path.join(transfer_dir, BATCHES_SUBDIR, state)


def _tier_dir(transfer_dir, state, tier):
    return os.path.join(transfer_dir, BATCHES_SUBDIR, state, tier)


def ensure_dirs(transfer_dir):
    """Create the pending/inprogress/completed state dirs and their tier subdirs."""
    for state in _STATES:
        os.makedirs(_state_dir(transfer_dir, state), exist_ok=True)
        for tier in TIERS:
            os.makedirs(_tier_dir(transfer_dir, state, tier), exist_ok=True)


def state_path(transfer_dir, state, name, tier=None):
    """Path of ``name`` in ``state``. ``tier=None`` → legacy flat location."""
    if tier is None:
        return os.path.join(_state_dir(transfer_dir, state), name)
    return os.path.join(_tier_dir(transfer_dir, state, tier), name)


def parse_batch_path(path):
    """Recover ``(transfer_dir, tier, name)`` from a published batch path.

    Handles both the tier-partitioned layout
    ``<transfer_dir>/batches/<state>/<tier>/<name>`` (``tier`` = the tier) and the
    legacy flat layout ``<transfer_dir>/batches/<state>/<name>`` (``tier`` =
    ``None``). Returns ``(None, None, name)`` if ``path`` is not a managed batch.
    """
    name = os.path.basename(path)
    d1 = os.path.dirname(path)
    b1 = os.path.basename(d1)
    d2 = os.path.dirname(d1)
    b2 = os.path.basename(d2)
    # Flat: <td>/batches/<state>/<name>
    if b1 in _STATES and b2 == BATCHES_SUBDIR:
        return os.path.dirname(d2), None, name
    # Tiered: <td>/batches/<state>/<tier>/<name>
    if b2 in _STATES:
        d3 = os.path.dirname(d2)
        if os.path.basename(d3) == BATCHES_SUBDIR:
            return os.path.dirname(d3), b1, name
    return None, None, name


def transfer_dir_of(batch_path):
    """Recover just the per-transfer dir from a published batch path (any layout)."""
    td, _tier, _name = parse_batch_path(batch_path)
    if td is not None:
        return td
    # Defensive fallback to the old 3-level assumption.
    state_dir = os.path.dirname(batch_path)
    root = os.path.dirname(state_dir)
    return os.path.dirname(root)


def publish(transfer_dir, name, entries, tier=None, include_size=False,
            min_free_check=None):
    """Atomically write a batch into ``pending/<tier>/`` and return its path.

    ``entries`` is an iterable of records, each either a ``str`` path or a
    ``(path, size)`` pair. Each record is written NUL-framed (``<path>\\0`` or,
    when ``include_size`` and a size is present, ``<size>\\0<path>\\0``) so
    cloudcp can frame records even when a path contains a newline, CR or trailing
    space (redesign §4, challenges #9-#12). Paths are encoded with ``os.fsencode``
    so non-UTF-8 / Latin-1 filename bytes round-trip exactly; the size is bare
    ASCII digits.

    Written ``<name>.tmp`` -> ``fsync`` -> ``rename`` so a reader (or a resume)
    never sees a half-written batch (design P5). ``min_free_check``, if given, is
    a no-arg callable that raises when free space is too low; it is invoked
    before writing so we pause rather than emit a truncated batch.
    """
    ensure_dirs(transfer_dir)
    if min_free_check is not None:
        min_free_check()
    final = state_path(transfer_dir, PENDING, name, tier)
    os.makedirs(os.path.dirname(final), exist_ok=True)
    tmp = final + ".tmp"
    with open(tmp, "wb") as f:
        for entry in entries:
            if isinstance(entry, (tuple, list)):
                path, size = entry[0], entry[1]
            else:
                path, size = entry, None
            f.write(_encode_record(path, size, include_size))
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, final)
    return final


def _find(transfer_dir, state, name, tier=None):
    """Locate ``name`` within a single ``state``, returning ``(tier, path)``.

    Checks the hinted tier first (fast path when the caller knows it), then the
    legacy flat location, then every other tier (so the fallback worker, which
    only knows the batch name, can still find it). ``tier`` in the return is the
    tier it was found in (``None`` = legacy flat). ``(None, None)`` if absent.
    """
    if tier is not None:
        p = state_path(transfer_dir, state, name, tier)
        if os.path.exists(p):
            return tier, p
    p = state_path(transfer_dir, state, name, None)  # legacy flat
    if os.path.exists(p):
        return None, p
    for t in TIERS:
        if t == tier:
            continue
        p = state_path(transfer_dir, state, name, t)
        if os.path.exists(p):
            return t, p
    return None, None


def claim(transfer_dir, name, tier=None):
    """Move a batch into ``inprogress/`` (same tier) and return its path.

    Returns ``None`` if the batch is already ``completed`` (resume/dup dispatch).
    Idempotent and race-safe: a batch already ``inprogress`` is re-claimed; a
    lost rename race is resolved by re-inspecting completed/inprogress.
    """
    ensure_dirs(transfer_dir)
    _t, comp = _find(transfer_dir, COMPLETED, name, tier)
    if comp:
        return None
    it, inp = _find(transfer_dir, INPROGRESS, name, tier)
    if inp:
        return inp
    pt, pend = _find(transfer_dir, PENDING, name, tier)
    if pend:
        dest = state_path(transfer_dir, INPROGRESS, name, pt)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        try:
            os.rename(pend, dest)
            return dest
        except OSError:
            # Lost the claim race or pending vanished; settle by re-checking.
            _t2, comp2 = _find(transfer_dir, COMPLETED, name, tier)
            if comp2:
                return None
            _t3, inp2 = _find(transfer_dir, INPROGRESS, name, tier)
            if inp2:
                return inp2
    return None


def requeue(transfer_dir, name, tier=None):
    """Move a batch back into ``pending/`` (same tier) and return its path.

    Used on resume so a batch left ``inprogress`` by a crashed/killed worker is
    put back where the dispatcher looks for work. Idempotent and race-safe: a
    batch already ``pending`` is returned as-is; an already-``completed`` batch
    is left alone (returns ``None``).
    """
    ensure_dirs(transfer_dir)
    _t, comp = _find(transfer_dir, COMPLETED, name, tier)
    if comp:
        return None
    pt, pend = _find(transfer_dir, PENDING, name, tier)
    if pend:
        return pend
    it, inp = _find(transfer_dir, INPROGRESS, name, tier)
    if inp:
        dest = state_path(transfer_dir, PENDING, name, it)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        try:
            os.rename(inp, dest)
            return dest
        except OSError:
            # Lost the race or inprogress vanished; settle by re-checking.
            _t2, comp2 = _find(transfer_dir, COMPLETED, name, tier)
            if comp2:
                return None
            _t3, pend2 = _find(transfer_dir, PENDING, name, tier)
            if pend2:
                return pend2
    return None


def complete(transfer_dir, name, tier=None):
    """Atomically move a batch into ``completed/`` (preserving its tier). Idempotent."""
    ensure_dirs(transfer_dir)
    ct, comp = _find(transfer_dir, COMPLETED, name, tier)
    if comp:
        return comp
    for state in (INPROGRESS, PENDING):
        ft, src = _find(transfer_dir, state, name, tier)
        if src:
            dest = state_path(transfer_dir, COMPLETED, name, ft)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            try:
                os.rename(src, dest)
                return dest
            except OSError:
                _t2, comp2 = _find(transfer_dir, COMPLETED, name, tier)
                if comp2:
                    return comp2
                return dest
    # Nothing to move — return the (hinted) completed path.
    return state_path(transfer_dir, COMPLETED, name, tier)


# Published batch files are named ``batch_<seq>.txt`` by the enumerator.
_BATCH_NAME_RE = re.compile(r"^batch_\d+\.txt$")


def _is_batch_name(name):
    return bool(_BATCH_NAME_RE.match(name))


def _list_dir(d):
    if not os.path.isdir(d):
        return []
    # Only genuine published batch files count as state. aws_transfer writes
    # cloudcp-derived work files and ``*.tmp`` copies alongside; those (and the
    # tier subdirectories themselves) must never be treated as batches.
    return [n for n in os.listdir(d) if _is_batch_name(n)]


def _iter_state(transfer_dir, state):
    """Yield ``(tier_label, name, path)`` for every batch in ``state``.

    Legacy flat batches are yielded with tier ``UNKNOWN_TIER``; tiered batches
    with their tier. Sorted by name within each location for stable dispatch.
    """
    for n in sorted(_list_dir(_state_dir(transfer_dir, state))):
        yield UNKNOWN_TIER, n, state_path(transfer_dir, state, n, None)
    for tier in TIERS:
        for n in sorted(_list_dir(_tier_dir(transfer_dir, state, tier))):
            yield tier, n, state_path(transfer_dir, state, n, tier)


def to_run(transfer_dir):
    """Return ``[(tier, name, path), ...]`` for batches still to run (resume set).

    That is ``pending`` + ``inprogress`` (everything not ``completed``), across
    every tier plus legacy flat. A batch left ``inprogress`` by a crashed worker
    is re-dispatched; the per-file report makes cloudcp skip files it already
    uploaded, so no duplicate uploads.
    """
    out = []
    for state in (PENDING, INPROGRESS):
        for tier, name, path in _iter_state(transfer_dir, state):
            out.append((tier, name, path))
    return out


def counts(transfer_dir):
    """Aggregate ``{'pending': n, 'inprogress': n, 'completed': n}`` (all tiers)."""
    return {state: sum(1 for _ in _iter_state(transfer_dir, state)) for state in _STATES}


def counts_by_tier(transfer_dir):
    """Per-tier ``{tier: {pending, inprogress, completed}}`` for the scheduler."""
    res = {}
    for state in _STATES:
        for tier, _name, _path in _iter_state(transfer_dir, state):
            res.setdefault(tier, {s: 0 for s in _STATES})[state] += 1
    return res


def completed_batches(transfer_dir):
    """Return ``[(name, path), ...]`` for every batch under ``completed/`` (all tiers).

    Used by per-batch verification (design §15.1) to reconcile each terminal
    batch's file list against the durable upload report.
    """
    out = [(name, path) for _tier, name, path in _iter_state(transfer_dir, COMPLETED)]
    return sorted(out, key=lambda x: x[0])


def reset_inprogress_tmp(transfer_dir):
    """Delete leftover ``*.tmp`` batch files in every state / tier dir (crash cleanup)."""
    for state in _STATES:
        dirs = [_state_dir(transfer_dir, state)]
        dirs += [_tier_dir(transfer_dir, state, t) for t in TIERS]
        for d in dirs:
            if not os.path.isdir(d):
                continue
            for n in os.listdir(d):
                if n.endswith(".tmp"):
                    try:
                        os.remove(os.path.join(d, n))
                    except OSError:
                        pass


def read_batch_records(path):
    """Read a batch file into ``[(path, size_or_None), ...]``.

    Auto-detects the two framings (design §6.3): a ``<size>\\0<path>\\0`` file is
    recognised because its first NUL field is bare ASCII digits (a batch path is
    always ``/…`` or ``s3://…``, never a bare integer); otherwise the records are
    path-only and ``size`` is ``None``. ``os.fsdecode`` keeps non-UTF-8 names
    byte-exact so they compare against report ``local_path`` values and open
    correctly. Falls back to newline framing for a legacy batch. ``[]`` on error.
    """
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError:
        return []
    if not data:
        return []
    if b"\0" not in data:  # legacy newline-framed batch
        return [(os.fsdecode(ln), None) for ln in data.split(b"\n") if ln]
    fields = data.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()  # trailing empty after the final NUL
    if not fields:
        return []
    if fields[0].isdigit():
        # size-bearing: (size, path) pairs.
        out = []
        i = 0
        while i + 1 < len(fields):
            sz = int(fields[i]) if fields[i].isdigit() else None
            out.append((os.fsdecode(fields[i + 1]), sz))
            i += 2
        if i < len(fields):  # dangling field (truncated) -> path-only tail
            out.append((os.fsdecode(fields[i]), None))
        return out
    # path-only
    return [(os.fsdecode(f), None) for f in fields if f]


def read_batch_file(path):
    """Read a batch file into a list of surrogateescape path strings.

    Thin wrapper over :func:`read_batch_records` that drops the size, for callers
    (per-batch verification, resume dedup) that only need the paths.
    """
    return [p for p, _size in read_batch_records(path)]
