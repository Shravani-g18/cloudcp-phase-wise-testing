"""Durable upload report + side logs — the xattr-free source of truth.

Design reference: docs/bcloud_redesign_proposal.md §6.2 and
docs/batch_builder_design.md §9 (resume without xattr).

Why this module exists
----------------------
Historically a transfer recorded per-file completion in an xattr
(``user.bryckxferstate``) on the source file.  That broke resumability
whenever the source filesystem was read-only or the file lacked write
permission (challenge #1), and it made resume O(files) with a getxattr
probe per file (challenge #14/#16).  The redesign removes xattr entirely:

* **upload report CSV** — one row per successfully-uploaded object.  This is
  the single source of truth for (a) resume/skip decisions, (b) the boto3
  fallback, and (c) the final end-of-transfer report.  Columns match exactly
  what challenge #8 asked for plus the few fields verification/fallback need::

      local_path,s3path,size,etag,status,attempt,finished_at

  ``status`` is one of ``SUCCESS`` (cloudcp), ``FALLBACK_OK`` (boto3 fallback
  draining a cloudcp rc==2 retry list), ``MP_OK`` (boto3 ProcessPool whole-batch
  retry after cloudcp rc==1, §12.1), or ``SKIPPED`` (already present).  Rows are
  written **only after** the object is confirmed uploaded (cloudcp reports
  size+etag; the boto3 paths do a HeadObject — challenge #13).

* **error log** — human-readable, every transient/permanent error with context.

* **failed_uploads** — machine-readable, NUL-framed, only files that failed
  *terminally* (feeds the fallback queue and the report's failure section).

Concurrency model
------------------
The upload pipeline runs many independent ``aws_transfer.py`` processes (via
GNU parallel) plus one fallback worker process.  To avoid lock contention on a
single file, each *writer process* appends to its **own shard**::

    <LOGS_DIR>/cloud_transfer_<id>/report/
        upload_report.<pid>.csv     # success rows (source of truth)
        error.<pid>.log             # human-readable errors
        failed_uploads.<pid>        # NUL-framed terminal failures

Shards are append-only (crash-safe: a partial tail row is simply ignored on
read).  ``load_completed`` and the final-report generator read *all* shards.
This is the "one CSV per writer, no cross-writer contention" model from the
design, adapted to the process-per-batch pipeline.

Special-character safety (challenges #9-#12)
--------------------------------------------
``local_path`` may contain embedded newlines, trailing spaces, CR, and
non-UTF-8 (Latin-1) bytes.  The CSV shards are opened with
``errors="surrogateescape"`` and written with :mod:`csv`, so every field is
quoted — embedded ``\n``/``\r``/``,``/spaces survive verbatim and readers use
a real CSV parser instead of line-splitting.  ``failed_uploads`` uses NUL
framing (the only byte illegal in a POSIX path) for the same reason.
"""

import os
import csv
import time
import shutil
from datetime import datetime

from bryckcloud.lib.libutils import logger

DEFAULT_LOGS_DIR = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"

# Terminal statuses that mean "do not re-upload this file on resume".
#   SUCCESS      cloudcp uploaded + HeadObject-verified
#   SKIPPED      cloudcp found a matching object already present
#   FALLBACK_OK  boto3 fallback drained a cloudcp rc==2 retry .lst
#   MP_OK        boto3 ProcessPool whole-batch retry after cloudcp rc==1 (§12.1)
COMPLETED_STATUSES = frozenset({"SUCCESS", "FALLBACK_OK", "SKIPPED", "MP_OK"})

# NUL is the only byte illegal in a POSIX filename, so it is the one safe
# delimiter for the machine-readable failed_uploads log.
_NUL = "\0"


def _logs_base(config):
    if config and config.get("LOGS_DIR"):
        return config["LOGS_DIR"]
    return DEFAULT_LOGS_DIR


def transfer_dir(transfer_id, config=None):
    """Per-transfer log directory: <LOGS_DIR>/cloud_transfer_<id>/."""
    return os.path.join(_logs_base(config), "cloud_transfer_{}".format(transfer_id))


def report_dir(transfer_id, config=None):
    """Directory holding the report + side-log shards for a transfer."""
    return os.path.join(transfer_dir(transfer_id, config), "report")


def cloudcp_report_path(transfer_id, config=None):
    """cloudcp's own per-transfer success CSV (redesign §9).

    cloudcp writes ``transfer_report_<id>.csv`` with the SAME columns as our
    shards (``local_path,s3path,size,etag,status,attempt,finished_at``), so it is
    treated as just another report source for resume-skip and the final report.

    Workaround: some cloudcp builds write the report **flat** into the base
    ``LOGS_DIR`` instead of the per-transfer subdir; copy it into the canonical
    location so verification/resume find it (else every file looks "missing").
    """
    canonical = os.path.join(transfer_dir(transfer_id, config),
                             "transfer_report_{}.csv".format(transfer_id))
    flat = os.path.join(_logs_base(config), "transfer_report_{}.csv".format(transfer_id))
    if flat != canonical and os.path.exists(flat):# and not os.path.exists(canonical):
        shutil.copy(flat, canonical)  # one-line workaround: relocate misplaced report
    return canonical


def retry_list_path(transfer_id, batch_stem, config=None):
    """Deterministic per-batch retry list cloudcp writes on partial failure.

    ``<log_dir>/cloudcp_retry_<id>_<batch_stem>.lst`` (redesign §9), NUL-framed
    ``local_path \\0 s3path \\0 size \\0 last_error \\0`` records. The path is
    fully derivable from transfer-id + batch stem, so no socket/message is
    needed to announce it — its appearance (temp+atomic-rename by cloudcp) is
    the "ready" signal.
    """
    return os.path.join(transfer_dir(transfer_id, config),
                        "cloudcp_retry_{}_{}.txt.lst".format(transfer_id, batch_stem))


def _retry_list_prefix(transfer_id):
    return "cloudcp_retry_{}_".format(transfer_id)


def iter_pending_retry_lists(transfer_id, config=None):
    """Yield paths of un-drained ``.lst`` retry lists for this transfer.

    The fallback worker globs these instead of receiving records over a socket.
    A drained list is renamed ``<name>.done`` so it is skipped here (idempotent
    across resume / restarts).
    """
    d = transfer_dir(transfer_id, config)
    prefix = _retry_list_prefix(transfer_id)
    try:
        names = os.listdir(d)
    except OSError:
        return
    for name in names:
        if name.startswith(prefix) and name.endswith(".lst"):
            yield os.path.join(d, name)


def batch_stem_from_retry_list(path, transfer_id):
    """Recover the batch stem (e.g. ``batch_000123``) from a ``.lst`` path."""
    name = os.path.basename(path)
    prefix = _retry_list_prefix(transfer_id)
    if name.startswith(prefix) and name.endswith(".lst"):
        return name[len(prefix):-len(".lst")]
    return None


def read_retry_list(path):
    """Parse a NUL-framed cloudcp retry ``.lst`` into records.

    Returns a list of ``(local_path, s3path, size, last_error)`` tuples. A
    truncated tail (crash mid-write, though cloudcp writes atomically) is
    ignored. Fields preserve exact bytes via ``surrogateescape``.
    """
    try:
        with open(path, "r", newline="", errors="surrogateescape") as f:
            data = f.read()
    except OSError:
        return []
    fields = data.split(_NUL)
    records = []
    for i in range(0, len(fields) - 3, 4):
        records.append((fields[i], fields[i + 1], fields[i + 2], fields[i + 3]))
    return records


def ensure_report_dir(transfer_id, config=None):
    """Create the report directory if needed and return its path."""
    d = report_dir(transfer_id, config)
    os.makedirs(d, exist_ok=True)
    return d


def _shard_path(transfer_id, config, base, ext):
    d = report_dir(transfer_id, config)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "{}.{}{}".format(base, os.getpid(), ext))


def resume_precheck(transfer_id, config=None):
    """Verify we can durably record progress before starting a transfer.

    Challenge #1: if we cannot create/write the report directory we also
    cannot resume later, so the caller should refuse to start rather than
    upload data it can never resume or report on.

    Returns (ok, reason).  ``ok`` is True when the report directory exists and
    is writable; otherwise ``reason`` explains why.
    """
    try:
        d = ensure_report_dir(transfer_id, config)
    except OSError as e:
        return False, "cannot create report directory {}: {}".format(
            report_dir(transfer_id, config), e)
    if not os.access(d, os.W_OK):
        return False, "report directory not writable: {}".format(d)
    # Prove we can actually write (catches read-only mounts that still pass
    # os.access, quota-exhausted volumes, etc.).
    probe = os.path.join(d, ".write_probe.{}".format(os.getpid()))
    try:
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)
    except OSError as e:
        return False, "report directory not writable ({}): {}".format(d, e)
    return True, ""


def append_success(transfer_id, local_path, s3path, size, etag,
                   status="SUCCESS", attempt=1, config=None):
    """Append one completed-upload row to this process's report shard.

    ``status`` must be a terminal-success status (SUCCESS / FALLBACK_OK /
    SKIPPED).  Writes are CSV-quoted and surrogateescape-safe so special
    characters in ``local_path`` are preserved byte-for-byte.
    """
    d = transfer_dir(transfer_id, config)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "transfer_report_{}.csv".format(transfer_id))
    try:
        # newline="" per csv module guidance; surrogateescape round-trips
        # non-UTF-8 filename bytes. QUOTE_ALL keeps every field double-quoted.
        # finished_at is ISO-8601 with milliseconds to match cloudcp's rows
        # (e.g. 2026-07-29T16:36:34.062) rather than a raw epoch integer.
        with open(path, "a", newline="", errors="surrogateescape") as f:
            w = csv.writer(f, quoting=csv.QUOTE_ALL)
            w.writerow([local_path, s3path, size, etag, status, attempt,
                        datetime.now().isoformat(timespec="milliseconds")])
    except OSError:
        # Never let report bookkeeping abort a transfer; the caller logs.
        raise


def log_error(transfer_id, local_path, s3path, message, config=None):
    """Append a human-readable error line to this process's error shard."""
    path = _shard_path(transfer_id, config, "error", ".log")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    # Keep it single-line and readable; local_path may contain newlines, so
    # collapse them for the human log only (the authoritative record of the
    # path is the failed_uploads NUL log below).
    safe_local = local_path.replace("\n", "\\n").replace("\r", "\\r")
    line = "{} src:{} dst:{} error:{}\n".format(ts, safe_local, s3path, message)
    with open(path, "a", errors="surrogateescape") as f:
        f.write(line)


def append_failed(transfer_id, local_path, s3path, size, last_error, config=None):
    """Record a terminally-failed file (NUL-framed, machine-readable).

    Format per record: ``local_path \0 s3path \0 size \0 last_error \0`` — NUL
    framing keeps embedded newlines/spaces/CR in ``local_path`` intact.
    """
    path = _shard_path(transfer_id, config, "failed_uploads", "")
    # last_error may itself contain NUL only in pathological cases; strip it.
    err = (last_error or "").replace(_NUL, " ")
    record = _NUL.join([str(local_path), str(s3path), str(size), err]) + _NUL
    # newline="" disables universal-newline translation so embedded CR/LF bytes
    # in the path survive the round-trip (challenges #9-#12).
    with open(path, "a", newline="", errors="surrogateescape") as f:
        f.write(record)


def _iter_shards(transfer_id, config, base, ext):
    d = report_dir(transfer_id, config)
    if not os.path.isdir(d):
        return
    prefix = base + "."
    for name in os.listdir(d):
        if name.startswith(prefix) and name.endswith(ext):
            yield os.path.join(d, name)


def _report_sources(transfer_id, config=None):
    """All CSV files feeding resume-skip + final report.

    = cloudcp's own ``transfer_report_<id>.csv`` (SUCCESS/SKIPPED rows) plus
    every per-writer ``upload_report.<pid>.csv`` shard (FALLBACK_OK rows from
    the boto3 fallback, and legacy SUCCESS rows). Identical column schema.
    """
    cc = cloudcp_report_path(transfer_id, config)
    if os.path.exists(cc):
        yield cc
    for shard in _iter_shards(transfer_id, config, "upload_report", ".csv"):
        yield shard


def load_completed(transfer_id, config=None):
    """Return the set of local paths already completed (source of truth).

    Reads every ``upload_report.*.csv`` shard and collects ``local_path`` for
    rows whose status is terminal-success.  Used by enumeration to skip files
    already uploaded — replacing the per-file xattr probe (challenges
    #14/#16).  A truncated final row (crash mid-append) is ignored.
    """
    done = set()
    for shard in _report_sources(transfer_id, config):
        try:
            with open(shard, "r", newline="", errors="surrogateescape") as f:
                for row in csv.reader(f):
                    if len(row) < 5:
                        continue  # malformed / truncated tail row
                    local_path, status = row[0], row[4]
                    if status in COMPLETED_STATUSES:
                        done.add(local_path)
        except OSError:
            continue
    return done


def completed_totals(transfer_id, config=None):
    """Return ``(files, bytes)`` already transferred for *transfer_id*.

    Same report sources and row layout as :func:`load_completed`, but also reads
    the ``size`` column (``local_path=row[0]``, ``size=row[2]``, ``status=row[4]``)
    and sums it over terminal-success rows, **de-duplicated by ``local_path``**
    (the report is append-only, so a path may repeat — e.g. SUCCESS then SKIPPED
    on resume — and must be counted once). Returns ``(0, 0)`` when there is no
    report. A row without a parseable size counts as a file but 0 bytes.

    This is the idempotent "bytes/files already transferred" used to seed
    ``CopiedBytes``/``CopiedFiles`` on resume instead of restarting from 0.
    """
    seen = {}
    for shard in _report_sources(transfer_id, config):
        try:
            with open(shard, "r", newline="", errors="surrogateescape") as f:
                for row in csv.reader(f):
                    if len(row) < 5:
                        continue  # malformed / truncated tail row
                    local_path, size, status = row[0], row[2], row[4]
                    if local_path == "local_path":
                        continue  # optional header row
                    if status not in COMPLETED_STATUSES:
                        continue
                    try:
                        seen[local_path] = int(size or 0)
                    except (TypeError, ValueError):
                        seen[local_path] = 0
        except OSError:
            continue
    return len(seen), sum(seen.values())


def iter_report_rows(transfer_id, config=None):
    """Yield dict rows from cloudcp's CSV + all shards for final-report generation."""
    cols = ("local_path", "s3path", "size", "etag", "status", "attempt",
            "finished_at")
    for shard in _report_sources(transfer_id, config):
        try:
            with open(shard, "r", newline="", errors="surrogateescape") as f:
                for row in csv.reader(f):
                    if len(row) < 5:
                        continue
                    if row[0] == "local_path":
                        continue  # skip an optional header row
                    yield dict(zip(cols, row + [""] * (len(cols) - len(row))))
        except OSError:
            continue


def load_failed(transfer_id, config=None):
    """Yield terminally-failed records as (local_path, s3path, size, last_error)."""
    for shard in _iter_shards(transfer_id, config, "failed_uploads", ""):
        try:
            with open(shard, "r", newline="", errors="surrogateescape") as f:
                data = f.read()
        except OSError:
            continue
        fields = data.split(_NUL)
        # records are groups of 4 fields; a trailing "" after the last NUL is
        # expected and dropped.
        for i in range(0, len(fields) - 3, 4):
            yield (fields[i], fields[i + 1], fields[i + 2], fields[i + 3])


def write_final_report(transfer_id, out_path, config=None):
    """Merge all report shards into a single ``final_report.csv`` (challenge #8).

    Columns: ``AbsoluteFilePath,S3Path,FileSize,ETag`` — the same schema the
    verifier produced by slow bucket-listing, but built directly from the
    durable during-upload report so no 300M-object LIST is needed. De-duplicates
    by local path (last terminal-success row wins) and preserves exact filename
    bytes via ``surrogateescape``. Returns the number of rows written.
    """
    seen = {}
    for row in iter_report_rows(transfer_id, config):
        if row.get("status") not in COMPLETED_STATUSES:
            continue
        seen[row["local_path"]] = (row.get("s3path", ""), row.get("size", "0"),
                                   row.get("etag", ""))
    tmp = out_path + ".tmp"
    with open(tmp, "w", newline="", errors="surrogateescape") as f:
        w = csv.writer(f)
        w.writerow(["AbsoluteFilePath", "S3Path", "FileSize", "ETag"])
        for local_path, (s3path, size, etag) in seen.items():
            w.writerow([local_path, s3path, size, etag])
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, out_path)
    return len(seen)
