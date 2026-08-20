"""Whole-batch boto3 retry for cloudcp rc==1 (design §12.1).

When cloudcp returns **rc==1** for a batch it means the *entire* batch failed
(it could not run to completion, or every object failed). Per requirements.txt
the right response is **not** to hand 100s–1000s of files to the shared,
load-scaled fallback worker (that would spike its backlog and starve rc==2
drains); instead ``aws_transfer`` retries the *whole batch inline* with a
dedicated boto3 ``ProcessPoolExecutor`` — a separate client stack from cloudcp's
C++ SDK — sized from the active network profile.

Model (mirrors ``tests/transfer_mp.py``):
  * ``ProcessPoolExecutor(processes)`` — each worker process owns its **own**
    boto3 client (clients must never cross a fork, per ``transfer_mp.py``).
  * Inside each process a ``ThreadPoolExecutor(threads_per_process)`` overlaps
    I/O so per-request latency is hidden while the process gives a private CPU
    for request signing (sidesteps the GIL).
  * Per file: ``upload_file`` then a **HeadObject size verify** (challenge #13)
    with bounded exponential backoff before success is recorded.

Outputs (durable, xattr-free — P3):
  * success  → ``upload_report.append_success(status=MP_OK)`` (a distinct
    terminal-success status so retried files are observable in the report).
  * failure  → ``upload_report.log_error`` + ``upload_report.append_failed``
    (the global machine-readable failed log).

The batch is **terminal** after this retry regardless of residual failures —
those are surfaced by verification. The caller (``aws_transfer``) performs the
``batch_state.complete`` so batch bookkeeping stays in one place.
"""

import os
import sys
import signal
import time
import random
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from bryckcloud.lib.libutils import logger
from bryckcloud.lib.cloud import upload_report
from bryckcloud.lib.cloud import batch_state
from bryckcloud.lib.cloud import net_profile


# Terminal-success status for an object uploaded by the rc==1 ProcessPool retry.
# Distinct from cloudcp's SUCCESS / the fallback's FALLBACK_OK so the report
# shows which files needed a whole-batch boto3 retry (design §16, §31 Q1).
MP_OK = "MP_OK"

# Pool sizing comes from the active network profile's ``rc1_retry`` block (or
# flat RC1_RETRY_* keys), resolved by net_profile. Defaults are kept small on
# purpose: many ``aws_transfer`` processes can hit rc==1 at once (still under GNU
# parallel until the broker lands), so a large per-batch pool would fork-bomb the
# host; a future broker may also cap concurrent rc==1 retries (§31 Q2).

# Per-worker-process state, populated once by the ProcessPoolExecutor
# initializer so every process owns its own boto3 client.
_G = {}


def clean_s3_key(key):
    """Return a botocore-encodable S3 key from a surrogateescape path string.

    Paths carried as ``surrogateescape`` strings hold lone surrogates for
    non-UTF-8 filename bytes, which botocore cannot sign. Recover the raw bytes
    and decode UTF-8 first (common), else latin-1 (lossless 1:1) so the key
    stays close to the original name (challenge #9). Identical to the helper in
    ``fallback_worker``/``transfer_mp`` — kept local to avoid a cross-import.
    """
    raw = key.encode("utf-8", "surrogateescape")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def compose_s3_key(abspath, fs_prefix, prefix):
    """Compose the S3 key exactly as cloudcp does (design §11.3).

    ``relpath`` = the absolute path with ``fs_prefix`` stripped; the key is
    ``prefix`` (one trailing ``/`` trimmed) joined to ``relpath`` (leading ``/``
    trimmed) with a single ``/``; an empty prefix uses the relpath as-is. String
    ops on surrogateescape strings are byte-safe here (only ASCII ``/`` is
    inspected). ``clean_s3_key`` is applied by the caller.
    """
    fsp = (fs_prefix or "").rstrip("/")
    if fsp and abspath.startswith(fsp):
        rel = abspath[len(fsp):].lstrip("/")
    else:
        rel = os.path.basename(abspath)
    if prefix:
        return prefix.strip("/") + "/" + rel
    return rel


def read_batch_paths(batch_file):
    """Read a NUL-framed batch file into a list of surrogateescape path strings.

    Thin alias over :func:`batch_state.read_batch_file` (the canonical reader) so
    the rc==1 retry and per-batch verification parse batches identically.
    """
    return batch_state.read_batch_file(batch_file)


def _init_worker(cfg):
    """ProcessPoolExecutor initializer — runs once per worker process.

    Children ignore SIGINT (the parent owns shutdown), set the AWS SDK env so
    credential resolution matches the rest of the service (ARN/assumed-role
    aware), and build a boto3 client **local to this process**.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(errors="backslashreplace")
            except Exception:
                pass

    # Import inside the worker so the (already-imported-in-parent) modules are
    # used, but the boto3 client is created fresh in this process.
    from bryckcloud.lib.cloud.aws import (_get_boto3_session, _get_aws_config_path,
                                          _get_aws_credentials_path)
    os.environ["AWS_CONFIG_FILE"] = _get_aws_config_path()
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = _get_aws_credentials_path()
    os.environ["AWS_SDK_LOAD_CONFIG"] = "1"

    _G.update(cfg)
    session = _get_boto3_session()
    client_kwargs = {}
    if cfg.get("region"):
        client_kwargs["region_name"] = cfg["region"]
    if cfg.get("endpoint_url"):
        client_kwargs["endpoint_url"] = cfg["endpoint_url"]
    _G["client"] = session.client("s3", **client_kwargs)


def _transfer_one(record):
    """Upload one file with boto3 + HeadObject verify. Runs in a worker thread.

    ``record`` is ``(abspath, size_or_None)``. When the batch carried the size
    (BATCH_INCLUDE_SIZE) it is reused as the expected size — no ``stat``;
    otherwise we ``stat`` the file. Returns ``(local_path, s3path, size, etag,
    rc, err)`` where ``rc==0`` means verified success.
    """
    abspath, expected_size = record
    s3 = _G["client"]
    bucket = _G["bucket"]
    prefix = _G["prefix"]
    fs_prefix = _G["fs_prefix"]
    max_attempts = _G["max_attempts"]
    backoff_base = _G["backoff_base"]
    backoff_max = _G["backoff_max"]

    key = clean_s3_key(compose_s3_key(abspath, fs_prefix, prefix))
    s3path = "s3://{}/{}".format(bucket, key)

    # TEST: mp failure tests — inject a random failure for a fraction of files
    # (MP_TEST_FAILURE_RATE, 0 = disabled) to exercise the failure path.
    if _G["mp_test_failure_rate"] > 0 and random.random() < _G["mp_test_failure_rate"]:
        return (abspath, s3path, 0, "", 1, "injected test failure")

    last_err = ""
    for attempt in range(1, max_attempts + 1):
        try:
            local_size = expected_size
            if local_size is None:
                try:
                    local_size = os.path.getsize(abspath)
                except OSError as e:
                    # Source vanished/unreadable — terminal, no point retrying.
                    return (abspath, s3path, 0, "", 1, "cannot stat source: {}".format(e))
            s3.upload_file(abspath, bucket, key)
            head = s3.head_object(Bucket=bucket, Key=key)
            s3_size = head.get("ContentLength", 0)
            etag = head.get("ETag", "").strip('"')
            if s3_size != local_size:
                return (abspath, s3path, s3_size, etag, 1,
                        "size mismatch after upload: local={} s3={}".format(local_size, s3_size))
            return (abspath, s3path, s3_size, etag, 0, "")
        except Exception as e:
            last_err = str(e)
            if attempt < max_attempts:
                delay = min(backoff_max, backoff_base * (2 ** (attempt - 1)))
                time.sleep(delay)
    return (abspath, s3path, 0, "", 1, last_err)


def _process_chunk(records):
    """Worker-process entry: fan a chunk of (path, size) records across a thread pool."""
    threads = _G["threads_per_process"]
    results = []
    with ThreadPoolExecutor(max_workers=threads) as tp:
        for res in tp.map(_transfer_one, records):
            results.append(res)
    return results


def _resolve_pool_sizing(config):
    """Resolve (processes, threads_per_process) via the network-profile loader.

    Precedence (flat ``RC1_RETRY_PROCESSES`` / ``RC1_RETRY_THREADS_PER_PROCESS``
    > active profile ``rc1_retry`` > defaults) is handled in
    :func:`net_profile.NetworkProfile.rc1_retry`.
    """
    return net_profile.resolve(config).rc1_retry()


def retry_whole_batch(transfer_id, transfer_type, batch_file, bucket, prefix,
                      fs_prefix, endpoint_url, region, config, txlog=None):
    """Retry an entire cloudcp-rc==1 batch via a boto3 ProcessPool.

    Writes MP_OK report rows for verified successes and error/failed rows for
    terminal failures, then returns ``(ok, failed, ok_bytes)`` so the caller can
    update the stat file / DB progress and complete the batch. Downloads are not
    yet supported here (upload path only, per requirements.txt); a download batch
    is left for the fallback / resume.
    """
    records = batch_state.read_batch_records(batch_file)
    if not records:
        return 0, 0, 0

    if transfer_type != "upload":
        logger.info("rc==1 retry: download batches not handled inline; "
                    "counting all {} records of batch {} as failed".format(
                        len(records), os.path.basename(batch_file)))
        return 0, len(records), 0

    # Skip files already recorded as uploaded in the transfer report (same
    # source of truth as the fallback) so a resume doesn't re-upload them.
    completed = upload_report.load_completed(transfer_id, config)
    if completed:
        records = [r for r in records if r[0] not in completed]
        if not records:
            return 0, 0, 0

    processes, threads = _resolve_pool_sizing(config)
    cfg = config or {}
    worker_cfg = {
        "bucket": bucket,
        "prefix": prefix,
        "fs_prefix": fs_prefix,
        "endpoint_url": endpoint_url or None,
        "region": region or None,
        "threads_per_process": threads,
        "max_attempts": max(1, int(cfg.get("RC1_RETRY_MAX_ATTEMPTS", 3))),
        "backoff_base": float(cfg.get("RC1_RETRY_BACKOFF_BASE_SEC", 1.0)),
        "backoff_max": float(cfg.get("RC1_RETRY_BACKOFF_MAX_SEC", 30.0)),
        # TEST: mp failure tests — fraction of files to fail on purpose (0 = off).
        "mp_test_failure_rate": float(cfg.get("MP_TEST_FAILURE_RATE", 0.0)),
    }

    # Chunk so each future carries a batch of files (amortises pickling) while
    # keeping several in flight per process.
    chunk = max(1, min(200, (len(records) + processes - 1) // processes))
    chunks = [records[i:i + chunk] for i in range(0, len(records), chunk)]

    logger.debug("Retrying failed batch {} ({} files) using {} multi-processes x {} threads".format(
        os.path.basename(batch_file), len(records), processes, threads))

    ok = failed = ok_bytes = 0
    try:
        executor = ProcessPoolExecutor(max_workers=processes, initializer=_init_worker,
                                   initargs=(worker_cfg,))
    
        for results in executor.map(_process_chunk, chunks):
            for local_path, s3path, size, etag, rc, err in results:
                if rc == 0:
                    ok += 1
                    try:
                        ok_bytes += int(size)
                    except (TypeError, ValueError):
                        pass
                    try:
                        upload_report.append_success(
                            transfer_id, local_path, s3path, size, etag,
                            status=MP_OK, attempt=2, config=config)
                    except Exception as e:
                        logger.debug("Could not record the multiprocess-retry upload of {} in report: {}".format(local_path, e))
                    if txlog is not None:
                        try:
                            txlog.info("Uploaded(MP): src:{} dst:{} size:{}".format(
                                local_path, s3path, size))
                        except Exception:
                            pass
                else:
                    failed += 1
                    try:
                        upload_report.log_error(transfer_id, local_path, s3path, err, config=config)
                        upload_report.append_failed(transfer_id, local_path, s3path, size, err, config=config)
                    except Exception as e:
                        logger.debug("rc==1 retry: failed to record failure: {}".format(e))
                    if txlog is not None:
                        try:
                            txlog.info("Failed(MP): src:{} dst:{}".format(local_path, s3path))
                        except Exception:
                            pass
    except Exception as e:
        logger.error("rc==1 retry: ProcessPool failed for batch {}: {}".format(
            os.path.basename(batch_file), e))
    finally:
        executor.shutdown(wait=True)

    logger.debug("MP retry: batch {} done — ok={} failed={}".format(
        os.path.basename(batch_file), ok, failed))
    return ok, failed, ok_bytes
