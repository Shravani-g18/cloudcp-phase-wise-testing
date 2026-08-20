import argparse
import os
import errno
import shutil
import signal
import time
import boto3
import json
import sys
from collections import deque


from logging import DEBUG, INFO, basicConfig
import logging
from systemd.journal import JournalHandler


from bryckcloud.lib.libutils import logger
from bryckcloud.lib.bcloud_sql import update_total_bytes, update_total_files, update_transfer_progress
from bryckcloud.lib.cloud import aws as cloud_aws
from bryckcloud.lib.cloud.BatchBuilder import BatchBuilder, FileEntry
from bryckcloud.lib.cloud import upload_report
from bryckcloud.lib.cloud import batch_state
from bryckcloud.lib.cloud import net_profile


BCLOUD_CFG="/etc/bryck/bryckcloud/config.json"
total_bytes = 0
total_files = 0
transfer_id = None

# Per-tier batch summary, accumulated as batches are published. Feeds the
# batch_summary.csv written in batch-builder-only mode (§20) and is cheap
# observability in a normal run. tier -> {"batches", "files", "bytes"}.
_batch_summary = {}


def _record_batch_summary(batch):
    """Tally one published batch into the per-tier summary."""
    tier = getattr(batch, "bucket", "unknown")
    s = _batch_summary.get(tier)
    if s is None:
        s = {"batches": 0, "files": 0, "bytes": 0}
        _batch_summary[tier] = s
    s["batches"] += 1
    s["files"] += batch.file_count
    s["bytes"] += batch.total_size


def write_batch_summary(output_dir):
    """Write ``batch_summary.csv`` (per-tier batch/file/byte counts).

    Produced for batch-builder-only mode (§20) so batching/throttles/resume can
    be validated on a real tree without moving data; also harmless observability
    in a normal run. Written atomically (tmp -> rename).
    """
    import csv
    path = os.path.join(output_dir, "batch_summary.csv")
    tmp = path + ".tmp"
    tiers = ["zero", "tiny", "small", "medium", "large", "unknown"]
    ordered = [t for t in tiers if t in _batch_summary]
    ordered += [t for t in _batch_summary if t not in tiers]
    try:
        with open(tmp, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["tier", "batch_count", "file_count", "total_bytes", "total_size_mb"])
            tot_b = tot_f = tot_by = 0
            for tier in ordered:
                s = _batch_summary[tier]
                logger.debug(f"Tier-{tier}, summary-{s}")
                w.writerow([tier, s["batches"], s["files"], s["bytes"],
                            "{:.2f}".format(s["bytes"] / (1024 * 1024))])
                tot_b += s["batches"]; tot_f += s["files"]; tot_by += s["bytes"]
            w.writerow(["TOTAL", tot_b, tot_f, tot_by,
                        "{:.2f}".format(tot_by / (1024 * 1024))])
        os.rename(tmp, path)
    except OSError as e:
        logger.error("Failed to write batch_summary.csv: {}".format(e))
        return None
    return path

# Minimum free space (percent) required on the batch-metadata filesystem before
# and during enumeration.  Bugs #17/#18: never start (or continue writing
# batches/logs) when the volume is nearly full — pause and error instead of
# silently losing batch files.  Overridable via config key MIN_FREE_PCT.
DEFAULT_MIN_FREE_PCT = 10.0

# Exit code used when enumeration stops because the filesystem is (nearly) full.
RC_NO_SPACE = 2

# Resume is driven by the durable upload report (docs §6.2 / §9), NOT xattr.
# The report is the set of source paths already uploaded (SUCCESS/FALLBACK_OK/
# SKIPPED) in earlier runs; enumeration skips them so cloudcp batches stay full
# of real work.  Loaded once per process (challenge #14/#16 — no per-file probe,
# and no dependency on writable source files, challenge #1).
_completed_cache = None


def _completed_set(transfer_id, config=None):
    global _completed_cache
    if _completed_cache is None:
        try:
            _completed_cache = upload_report.load_completed(transfer_id, config)
        except Exception as e:
            logger.debug("Failed to load upload report for resume: {}".format(e))
            _completed_cache = set()
    return _completed_cache


def already_transferred(path, transfer_id, config=None):
    """True when *path* was already uploaded for *transfer_id* per the report.

    On restart we re-walk the whole source tree.  Files completed in an earlier
    run appear in the durable upload report; skipping them here keeps the
    cloudcp batches full of real work and stops a serial enumerator from
    starving GNU parallel while it fast-forwards over already-done files.

    Unlike the old xattr scheme this needs no write access to the source files
    (challenge #1) and no per-file syscall probe (challenge #14/#16).
    """
    if transfer_id is None:
        return False
    return path in _completed_set(transfer_id, config)


def get_brycksize(dirname):
    """
    Get the size of a local file or directory (in bytes).
    """
    if os.path.isfile(dirname):
        return os.path.getsize(dirname)

    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(dirname):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
    except Exception as e:
        logger.error("Failed to calculate total size of {}-{}".format(dirname, str(e)))

    return total_size


def check_s3file(s3, bucket_name):
    bucket_name = bucket_name[len("s3://"):]
    if '/' in bucket_name:
        try:
            bucket, key = bucket_name.split('/', 1)
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as E:
            pass
    return False


def get_s3size(bucket_url, endpoint_url, region=None):
    """
    Get the total size of an AWS S3 bucket (in bytes).
    """
    s3 = None
    _kwargs = {}
    if endpoint_url:
        _kwargs['endpoint_url'] = endpoint_url
    if region:
        _kwargs['region_name'] = region
    session = cloud_aws._get_boto3_session()
    s3 = session.client('s3', **_kwargs)

    total_size = 0

    bucket_name, prefix = cloud_aws.url_parse(bucket_url)

    try:
        paginator = s3.get_paginator('list_objects_v2')
        pagination_params = {"Bucket": bucket_name}
        if prefix:
            pagination_params["Prefix"] = prefix + "/"

        if check_s3file(s3, bucket_url):
            pagination_params["Prefix"] = prefix

        for page in paginator.paginate(**pagination_params):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
    except Exception as e:
        logger.debug("Failed to get the S3bucket size {}-{}".format(bucket_name, e))
        return 1

    return total_size


# Calculate and update total_bytes to db
def calculate_size(transfer_id, dirname, region=None, endpoint_url=None, cloud_type=None):
    """
    Calculate the size to be uploaded/downloaded.

    Args:
        copied_bytes (int): Number of bytes to add to CopiedBytes.
        transfer_id (int): ID of the transfer to update.
        dirname (str): Dir name or bucket name
        endpoint_url (str): Customized cloud configuration url
        cloud_type: [None, AWS, GCP, AZURE]
    """
    update_total_bytes(0,transfer_id)
    if dirname.startswith("/"):
        total_bytes = get_brycksize(dirname)
    else:
        total_bytes = get_s3size(dirname, endpoint_url, region)
    update_total_bytes(total_bytes, transfer_id)


def free_pct(path):
    """Return percent free space on the filesystem containing *path*.

    Uses f_bavail (blocks available to non-root) so the check reflects space
    actually usable by the service.  Returns 100.0 if the filesystem reports
    no blocks (avoids spurious no-space aborts).
    """
    try:
        st = os.statvfs(path)
    except OSError:
        # If we cannot stat, don't block enumeration on a space check.
        return 100.0
    return 100.0 * st.f_bavail / st.f_blocks if st.f_blocks else 100.0


def check_free_space(path, min_pct):
    """True when *path* has at least *min_pct* percent free space.

    Logs and writes to stderr when space is below the threshold so the caller
    can pause/abort with a clear "no space" message (bugs #17/#18).
    """
    pct = free_pct(path)
    if pct < min_pct:
        msg = ("No space to continue: {} has only {:.1f}% free, "
               "need >= {:.0f}%".format(path, pct, min_pct))
        logger.error(msg)
        sys.stderr.write(msg + "\n")
        return False
    return True


def write_batch_file(batch, output_path, min_free_pct=DEFAULT_MIN_FREE_PCT):
    file_name = os.path.join(output_path, "batch_{:06d}.txt".format(batch.id))
    # Guard before writing: if the volume is (nearly) full, pause here with a
    # clear no-space error rather than emitting a truncated/partial batch file.
    if not check_free_space(output_path, min_free_pct):
        return RC_NO_SPACE
    try:
        # NUL-framed binary (redesign §4): cloudcp frames records on \0 so paths
        # with embedded newlines/CR/trailing-space survive. os.fsencode keeps
        # non-UTF-8 (Latin-1) filename bytes intact.
        with open(file_name, "wb") as f:
            for entry in batch.files:
                f.write(os.fsencode(entry.path) + b"\0")
        print(file_name, flush=True)
    except OSError as e:
        if e.errno == errno.ENOSPC:
            # Remove the partial file so a resume doesn't consume a truncated batch.
            try:
                os.remove(file_name)
            except OSError:
                pass
            msg = ("No space left on device while writing batch file {}; "
                   "pausing enumeration".format(file_name))
            logger.error(msg)
            sys.stderr.write(msg + "\n")
            return RC_NO_SPACE
        logger.error("Failed to write batch file {}: {}".format(file_name, str(e)))
        sys.stderr.write("Failed to write batch file {}: {}\n".format(file_name, str(e)))
        return 1
    except Exception as e:
        logger.error("Failed to write batch file {}: {}".format(file_name, str(e)))
        sys.stderr.write("Failed to write batch file {}: {}\n".format(file_name, str(e)))
        return 1
    return 0


# ---------------------------------------------------------------------------
# Resume state: manifest + scan frontier journal
# ---------------------------------------------------------------------------
# Resume has two independent layers (challenges #14/#15):
#   * Batch-level (batch_state.py): a batch's on-disk directory is its state
#     (pending/inprogress/completed). O(batches) resume, exact in-progress vs
#     completed accounting; used for BOTH scan cases.
#   * Scan-level (this module): a directory frontier journal (scan.discovered -
#     scan.completed) + manifest.scan_state lets an interrupted *enumeration*
#     resume without re-walking the whole tree (Case 1). When scan_state is
#     "complete" the walk is skipped entirely and only pending/in-progress
#     batches are re-dispatched (Case 2).

MANIFEST_NAME = "manifest.json"
DISCOVERED_LOG = "scan.discovered"
SCAN_COMPLETED_LOG = "scan.completed"

# Name of the active network profile (design §8), stamped into every manifest
# write so a resume knows which profile the transfer was scanned under. Set once
# in __main__ from config.
_active_profile = None

# Whether to write the file size alongside the path in each batch record
# (<size>\0<path>\0). Off by default (path-only, current cloudcp contract);
# enable via BATCH_INCLUDE_SIZE once cloudcp is built to parse it. Set in __main__.
_include_size = False

# Set by the SIGINT/SIGTERM handler; the walk checkpoints at the next dir
# boundary and exits cleanly so a resume starts from a durable point (§9.5).
_stop_requested = False
# Exit code on a clean stop-and-checkpoint (pause), mirroring the standalone.
RC_STOPPED = 130


def _install_stop_handler():
    def _handler(signum, frame):
        global _stop_requested
        _stop_requested = True
    try:
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, OSError):
        # Not in main thread (e.g. under test harness) — best effort.
        pass


def manifest_path(output_dir):
    return os.path.join(output_dir, MANIFEST_NAME)


def read_manifest(output_dir):
    p = manifest_path(output_dir)
    if not os.path.exists(p):
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except (ValueError, OSError):
        return None


def write_manifest(output_dir, scan_state, **fields):
    """Atomically persist the manifest (tmp -> fsync -> rename)."""
    data = {"scan_state": scan_state, "ts": time.time()}
    data.update(fields)
    if _active_profile is not None:
        data.setdefault("active_profile", _active_profile)
    tmp = manifest_path(output_dir) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, manifest_path(output_dir))


def _journal_path(output_dir, name):
    return os.path.join(output_dir, name)


def _read_journal(path):
    s = set()
    if not os.path.exists(path):
        return s
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError:
        return s
    for rec in data.split(b"\x00"):
        if rec:
            s.add(rec)
    return s


def load_frontier(output_dir):
    """Directories still to visit on resume: discovered - completed (bytes)."""
    disc = _read_journal(_journal_path(output_dir, DISCOVERED_LOG))
    comp = _read_journal(_journal_path(output_dir, SCAN_COMPLETED_LOG))
    return disc - comp


def _append_journal(fh, entries):
    if not entries:
        return
    fh.write(b"".join(e + b"\x00" for e in entries))
    fh.flush()
    os.fsync(fh.fileno())


def publish_batch(batch, output_dir, min_free_pct=DEFAULT_MIN_FREE_PCT):
    """Publish a ready batch into ``batches/pending/`` and print its path.

    The batch file is written atomically (tmp -> fsync -> rename) so a reader or
    a resume never sees a partial batch. Returns 0 on success, RC_NO_SPACE when
    the volume is (nearly) full, 1 on other write errors.
    """
    if not check_free_space(output_dir, min_free_pct):
        return RC_NO_SPACE
    name = "batch_{:06d}.txt".format(batch.id)
    try:
        path = batch_state.publish(output_dir, name,
                                   ((entry.path, entry.size) for entry in batch.files),
                                   tier=getattr(batch, "bucket", None),
                                   include_size=_include_size)
    except OSError as e:
        batch_state.reset_inprogress_tmp(output_dir)
        if e.errno == errno.ENOSPC:
            msg = ("No space left on device while writing batch {}; "
                   "pausing enumeration".format(name))
            logger.error(msg)
            sys.stderr.write(msg + "\n")
            return RC_NO_SPACE
        logger.error("Failed to write batch {}: {}".format(name, str(e)))
        sys.stderr.write("Failed to write batch {}: {}\n".format(name, str(e)))
        return 1
    _record_batch_summary(batch)
    print(path, flush=True)
    return 0


def enumerate_s3_bucket(bucket_url, output_path, batch_size, region=None, epoint_url=None, config=None, min_free_pct=DEFAULT_MIN_FREE_PCT):
    global total_bytes
    global total_files
    is_file = False


    bucket_name, prefix = cloud_aws.url_parse(bucket_url)

    s3 = None
    _kwargs = {}
    if epoint_url:
        _kwargs['endpoint_url'] = epoint_url
    if region:
        _kwargs['region_name'] = region
    session = cloud_aws._get_boto3_session()
    s3 = session.client('s3', **_kwargs)

    builder = BatchBuilder(config) if batch_size > 1 else None

    try:
        paginator = s3.get_paginator('list_objects_v2')
        pagination_params = {"Bucket": bucket_name}

        if prefix:
            pagination_params["Prefix"] = prefix + "/"

        if check_s3file(s3, bucket_url):
            pagination_params["Prefix"] = prefix
            is_file = True
        
        for page in paginator.paginate(**pagination_params):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = "s3://" + bucket_name + "/" + obj['Key']
                    total_bytes += obj['Size']
                    total_files += 1
                    if batch_size == 1:
                        print(key, flush=True)
                    else:
                        builder.assign_file(FileEntry(obj['Size'], key))
                        for batch in builder.get_ready_batches():
                            rc = publish_batch(batch, output_path, min_free_pct)
                            if rc:
                                return rc
                if is_file:
                    break
    except Exception as e:
        logger.error("Failed to enumerate bucket {}: {}".format(bucket_url, str(e)))
        return 1

    if builder:
        builder.flush()
        for batch in builder.get_ready_batches():
            rc = publish_batch(batch, output_path, min_free_pct)
            if rc:
                return rc

    return 0


def _checkpoint_scan(builder, output_dir, disc_fh, comp_fh, newly_disc,
                     newly_comp, min_free_pct, transfer_id):
    """Persist a durable resume point mid-scan.

    Ordering (crash-consistency, design §9.2): publish any flushed open batches
    (files become durable) BEFORE recording a directory as completed, then
    append the journal deltas and write the manifest with scan_state=in_progress.
    A directory recorded 'completed' therefore always has its files already on
    disk. Returns 0 or an RC_* to abort (e.g. no space).
    """
    builder.flush_open()
    for batch in builder.get_ready_batches():
        rc = publish_batch(batch, output_dir, min_free_pct)
        if rc:
            return rc
    _append_journal(disc_fh, newly_disc)
    _append_journal(comp_fh, newly_comp)
    write_manifest(output_dir, "in_progress",
                   seq_high_water=builder._batch_counter,
                   records_done=total_files, total_files=total_files,
                   total_bytes=total_bytes, dirs_done=_dirs_done)
    update_total_bytes(total_bytes, transfer_id)
    update_total_files(total_files, transfer_id)
    return 0


# Directory count for manifest bookkeeping/progress.
_dirs_done = 0


def enumerate_dir(dir_path, output_path, batch_size, config=None, transfer_id=None,
                  min_free_pct=DEFAULT_MIN_FREE_PCT, resume=False, start_seq=0,
                  checkpoint_every=100000):
    """Resumable directory walk (BFS, bytes-safe) feeding the BatchBuilder.

    Returns 0 when the scan completed, RC_STOPPED if interrupted by a signal
    (state left resumable), or another RC_* on error/no-space.

    Resume model (challenge #14/#15): the directory frontier is journalled
    (scan.discovered - scan.completed). On resume the frontier is reloaded and
    the walk continues from there instead of re-walking the whole tree. Files
    already in the upload report are skipped (belt-and-suspenders for re-walked
    dirs). ``start_seq`` seeds batch ids from the manifest so published batch
    names never collide across resume.
    """
    global total_bytes
    global total_files
    global _dirs_done

    if os.path.isfile(dir_path):
        total_bytes = os.path.getsize(dir_path)
        total_files = 1
        if not already_transferred(dir_path, transfer_id, config):
            print(dir_path, flush=True)
        write_manifest(output_path, "complete", seq_high_water=start_seq,
                       records_done=total_files, total_files=total_files,
                       total_bytes=total_bytes, dirs_done=0)
        return 0

    if batch_size <= 1:
        builder = None
    else:
        builder = BatchBuilder(config, start_seq=start_seq)

    # On resume, seed the cumulative counters from the prior manifest so the DB
    # totals and progress (challenge #19) reflect the whole transfer, not just
    # the portion scanned in this run.
    if resume:
        prior = read_manifest(output_path) or {}
        total_files = int(prior.get("total_files", 0))
        total_bytes = int(prior.get("total_bytes", 0))
        _dirs_done = int(prior.get("dirs_done", 0))

    root = os.fsencode(dir_path).rstrip(b"/") or b"/"

    disc_fh = open(_journal_path(output_path, DISCOVERED_LOG), "ab")
    comp_fh = open(_journal_path(output_path, SCAN_COMPLETED_LOG), "ab")
    newly_disc, newly_comp = [], []

    if resume:
        frontier = load_frontier(output_path)
        queue = deque(sorted(frontier)) if frontier else deque([root])
    else:
        queue = deque([root])
        newly_disc.append(root)

    last_ckpt_files = total_files
    rc = 0
    try:
        while queue:
            if _stop_requested:
                # Graceful pause: checkpoint and report resumable stop (§9.5).
                if builder:
                    _checkpoint_scan(builder, output_path, disc_fh, comp_fh,
                                     newly_disc, newly_comp, min_free_pct)
                else:
                    _append_journal(disc_fh, newly_disc)
                    _append_journal(comp_fh, newly_comp)
                    write_manifest(output_path, "in_progress", seq_high_water=start_seq,
                                   records_done=total_files, total_files=total_files,
                                   total_bytes=total_bytes, dirs_done=_dirs_done)
                return RC_STOPPED
            current_dir = queue.popleft()
            try:
                it = os.scandir(os.fsdecode(current_dir))
            except OSError as e:
                logger.error("Failed to scandir {}: {}".format(
                    os.fsdecode(current_dir), str(e)))
                # Don't retry an unreadable dir forever — mark it completed.
                newly_comp.append(current_dir)
                continue
            try:
                for entry in it:
                    eb = os.fsencode(entry.path)
                    try:
                        if entry.is_file(follow_symlinks=False):
                            size = entry.stat(follow_symlinks=False).st_size
                            total_bytes += size
                            total_files += 1
                            if already_transferred(entry.path, transfer_id, config):
                                continue
                            if batch_size == 1:
                                print(entry.path, flush=True)
                            else:
                                builder.assign_file(FileEntry(size, entry.path))
                                for batch in builder.get_ready_batches():
                                    rc = publish_batch(batch, output_path, min_free_pct)
                                    if rc:
                                        return rc
                        elif entry.is_dir(follow_symlinks=False):
                            queue.append(eb)
                            newly_disc.append(eb)
                    except OSError as e2:
                        logger.error("Failed on entry {}: {}".format(entry.path, str(e2)))
            finally:
                it.close()
            newly_comp.append(current_dir)
            _dirs_done += 1
            if total_files - last_ckpt_files >= checkpoint_every:
                rc = _checkpoint_scan(builder, output_path, disc_fh, comp_fh,
                                      newly_disc, newly_comp, min_free_pct, transfer_id) if builder \
                    else 0
                if not builder:
                    _append_journal(disc_fh, newly_disc)
                    _append_journal(comp_fh, newly_comp)
                if rc:
                    return rc
                newly_disc, newly_comp = [], []
                last_ckpt_files = total_files

        # Scan finished: flush remaining open batches, persist final journal.
        if builder:
            builder.flush()
            for batch in builder.get_ready_batches():
                rc = publish_batch(batch, output_path, min_free_pct)
                if rc:
                    return rc
        _append_journal(disc_fh, newly_disc)
        _append_journal(comp_fh, newly_comp)
    finally:
        disc_fh.close()
        comp_fh.close()

    seq = builder._batch_counter if builder else start_seq
    write_manifest(output_path, "complete", seq_high_water=seq,
                   records_done=total_files, total_files=total_files,
                   total_bytes=total_bytes, dirs_done=_dirs_done)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="enumerate the source folder or bucket in batches")
    parser.add_argument("source",help="Data source: Directory or Bucket")
    parser.add_argument("-b", "--batch-size", type=int, help="Batch size")
    parser.add_argument("-n", "--num-batches",type=int, help="Number of batches per iteration")
    parser.add_argument("-i", "--transfer-id",type=int, help="Unique id of the transfer")
    parser.add_argument("-o", "--output-dir", help="Output directory for batch files")
    parser.add_argument("--batch-only", action="store_true",
                        help="Batch-builder-only mode (§20): enumerate + build + publish "
                             "batches and write batch_summary.csv, but transfer nothing. "
                             "Overrides the BATCH_BUILDER_ONLY config key.")
    args = parser.parse_args()

    bcloud_cfg = {}
    region = cloud_aws.get_region()

    if os.path.exists(BCLOUD_CFG):
        with open(BCLOUD_CFG, 'r') as file:
            from bryckcloud.lib.config import _normalize_config
            bcloud_cfg = _normalize_config(json.load(file))

    # Network profile (design §8): overlay the active profile's per-tier batch
    # sizing onto the config as flat keys (explicit flat keys still win) so the
    # BatchBuilder packs to the selected profile, and record the profile name in
    # the manifest for resume/audit.
    _active_profile = net_profile.active_profile_name(bcloud_cfg)
    bcloud_cfg = net_profile.overlay_batch_config(bcloud_cfg)
    _include_size = str(bcloud_cfg.get("BATCH_INCLUDE_SIZE", "False")).lower() == "true"
    logger.info("Enumerator using network profile: {} (batch size-in-record: {})".format(
        _active_profile, _include_size))

    endpoint_url = ""
    if "LOCAL_AWS" in bcloud_cfg:
        endpoint_url =bcloud_cfg["LOCAL_AWS"]

    batch_size = 1
    # Enable batching if per-bucket config, BATCH section, or PARALLEL_TRANSFER is set
    has_batch_config = any(k in bcloud_cfg for k in ("ZEROFILE_BATCH_SIZE", "TINYFILE_BATCH_SIZE",
                                      "SMALLFILE_BATCH_SIZE", "MEDIUMFILE_BATCH_SIZE",
                                      "LARGEFILE_BATCH_SIZE", "BATCH_SIZE",
                                      "PARALLEL_TRANSFER", "AWS_PARALLEL"))
    if not has_batch_config and "BATCH" in bcloud_cfg and isinstance(bcloud_cfg["BATCH"], dict):
        has_batch_config = True
    if has_batch_config:
        batch_size = 2  # Any value > 1 enables BatchBuilder

    if args.batch_size:
        batch_size = args.batch_size

    # Batch-builder-only mode (§20): CLI --batch-only overrides the config key.
    # It only changes the *caller's* behaviour (aws.py skips launching the
    # transfer pipeline/fallback); here it just guarantees the BatchBuilder is
    # actually exercised, so force batching on even without any batch config.
    batch_only = args.batch_only or \
        str(bcloud_cfg.get("BATCH_BUILDER_ONLY", "False")).lower() == "true"
    if batch_only and batch_size <= 1:
        batch_size = 2

    transfer_id = 1
    if args.transfer_id:
        transfer_id = args.transfer_id

    num_batches = 1
    if "AWS_THREAD" in bcloud_cfg:
        num_batches = bcloud_cfg["AWS_THREAD"]

    if args.num_batches:
        num_batches  = args.num_batches

    output_dir="/bryck/bcloud_batchmeta"
    if "BATCH_FILE_DIR" in bcloud_cfg:
        output_dir = bcloud_cfg["BATCH_FILE_DIR"]

    if args.output_dir:
        output_dir = args.output_dir

    output_dir=output_dir+"/transfer_{}".format(transfer_id)

    # ---- Resume decision (challenges #14/#15) -----------------------------
    # A resumable manifest means a prior run of THIS transfer left durable
    # state. Two cases:
    #   * scan_state == "complete"  -> Case 2: enumeration is done; skip the
    #     whole tree walk and just re-dispatch pending/in-progress batches.
    #   * scan_state == "in_progress" -> Case 1: resume the walk from the
    #     directory frontier AND re-dispatch already-published pending batches.
    # A truly fresh start (no manifest) wipes stale state for a clean slate.
    prior = read_manifest(output_dir) if os.path.exists(output_dir) else None
    resume = prior is not None and prior.get("scan_state") in ("in_progress", "complete")

    if os.path.exists(output_dir) and not resume:
        # Fresh start: clean batch files/state but preserve the fallback socket.
        for entry in os.listdir(output_dir):
            if entry.startswith("_fallback"):
                continue
            entry_path = os.path.join(output_dir, entry)
            if os.path.isdir(entry_path):
                shutil.rmtree(entry_path)
            else:
                os.remove(entry_path)

    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        logger.error("Failed to create batch output directory {}".format(str(e)))

    batch_state.ensure_dirs(output_dir)
    batch_state.reset_inprogress_tmp(output_dir)

    # Resolve minimum free-space threshold (percent) from config; default 10%.
    min_free_pct = DEFAULT_MIN_FREE_PCT
    if "MIN_FREE_PCT" in bcloud_cfg:
        try:
            min_free_pct = float(bcloud_cfg["MIN_FREE_PCT"])
        except (TypeError, ValueError):
            logger.error("Invalid MIN_FREE_PCT in config; using default {}".format(DEFAULT_MIN_FREE_PCT))

    # Checkpoint frequency (files) for scan-resume durability.
    checkpoint_every = 100000
    if "CHECKPOINT_EVERY_FILES" in bcloud_cfg:
        try:
            checkpoint_every = int(bcloud_cfg["CHECKPOINT_EVERY_FILES"])
        except (TypeError, ValueError):
            pass

    # Preflight (bug #18): refuse to start when the batch-metadata volume is
    # already below the minimum free-space threshold.
    if not check_free_space(output_dir, min_free_pct):
        sys.exit(RC_NO_SPACE)

    # Resume preflight (bug #1): the upload report is the source of truth for
    # resume.  If we cannot durably record progress (report dir not creatable/
    # writable) we could never resume this transfer, so refuse to start rather
    # than upload data we can't resume or report on.
    ok, reason = upload_report.resume_precheck(transfer_id, bcloud_cfg)
    if not ok:
        logger.error("Cannot start transfer {}: {}".format(transfer_id, reason))
        sys.stderr.write("Cannot start transfer {}: {}\n".format(transfer_id, reason))
        sys.exit(RC_NO_SPACE)

    _install_stop_handler()

    # Re-dispatch already-published batches that are not yet completed
    # (batches.created - batches.completed, but tracked as file location:
    # pending + inprogress). Cheap O(batches) — no tree walk. cloudcp skips
    # per-file work already in the report, so partially-done batches only
    # re-run their unfinished files.
    if resume:
        to_run = batch_state.to_run(output_dir)
        c = batch_state.counts(output_dir)
        logger.info("Resuming transfer {}: {} completed, {} pending, {} in-progress "
                    "batches; scan_state={}".format(
                        transfer_id, c["completed"], c["pending"], c["inprogress"],
                        prior.get("scan_state")))
        for _tier, _name, path in to_run:
            print(path, flush=True)

    start_seq = int(prior.get("seq_high_water", 0)) if resume else 0

    if resume and prior.get("scan_state") == "complete":
        # Case 2: enumeration already finished — nothing more to scan.
        rc = 0
        total_bytes = int(prior.get("total_bytes", 0))
        total_files = int(prior.get("total_files", 0))
        # No batch published this run, so rebuild the per-tier tally from the
        # on-disk batches; otherwise batch_summary.csv would be all zeros.
        for _st in (batch_state.PENDING, batch_state.INPROGRESS, batch_state.COMPLETED):
            for _tier, _name, _p in batch_state._iter_state(output_dir, _st):
                _recs = batch_state.read_batch_records(_p)
                _s = _batch_summary.setdefault(_tier, {"batches": 0, "files": 0, "bytes": 0})
                _s["batches"] += 1
                _s["files"] += len(_recs)
                _s["bytes"] += sum(sz for _pp, sz in _recs if sz)
    elif "s3://" in args.source:
        rc = enumerate_s3_bucket(args.source, output_dir, batch_size, region,
                                 endpoint_url, bcloud_cfg, min_free_pct)
    else:
        rc = enumerate_dir(args.source, output_dir, batch_size, bcloud_cfg,
                           transfer_id, min_free_pct, resume=resume,
                           start_seq=start_seq, checkpoint_every=checkpoint_every)

    update_total_bytes(total_bytes, transfer_id)
    update_total_files(total_files, transfer_id)

    # Per-tier batch summary (always cheap; the deliverable of batch-only mode).
    summary_path = write_batch_summary(output_dir)
    if batch_only:
        logger.info("Batch-only mode: enumerated {} files ({} bytes) into batches; "
                    "no transfer performed. Summary: {}".format(
                        total_files, total_bytes, summary_path))
        sys.stderr.write("batch-only: {} files, {} bytes across batches; summary at {}\n".format(
            total_files, total_bytes, summary_path))

    sys.exit(rc)

