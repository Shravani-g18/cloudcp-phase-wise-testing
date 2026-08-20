"""Global fallback worker — retries failed uploads/downloads using boto3.

Architecture (socket-free, file-driven):
  Runs as a SEPARATE PROCESS alongside the main transfer pipeline. cloudcp
  writes a per-batch NUL-framed retry list ``cloudcp_retry_<id>_<batch>.lst``
  (redesign §9) atomically at batch end; that file *is* the queue.

  Pipeline (N cloudcp workers)        Fallback Worker (1 process)
  ──────────────────────────          ───────────────────────────
  aws_transfer.py → cloudcp           fallback_worker.py
    exit 0  → complete batch            loop:
    exit 2  → leave inprogress            glob cloudcp_retry_<id>_*.lst
              (.lst on disk)              read NUL records → boto3 pool
                                          on drain: complete batch +
                                                    rename .lst → .lst.done

  Why file-driven instead of a socket:
    - The .lst is durable on disk, so a fallback crash loses nothing — resume
      re-globs and re-drains (crash-safe per-batch completion, design §11).
    - No framing/race/buffer bugs from streaming special-char paths over a
      socket (challenges #9-#12): cloudcp already NUL-frames the .lst.
    - aws_transfer never parses cloudcp output; it only sets batch state.

  A batch is COMPLETED only when cloudcp ran AND the fallback drained its
  retry list. aws_transfer defers completion (leaves the batch in
  ``batches/inprogress/``) whenever cloudcp exits 2; this worker performs the
  ``inprogress → completed`` rename once the .lst is fully drained.

  Worker exits when:
    - the ``_fallback_done`` marker (written by aws.py) exists, AND
    - no un-ingested .lst files remain, AND
    - nothing is queued or in flight.

Usage:
  python3 fallback_worker.py <transfer_id> <transfer_type> --transfer-dir <dir> --pool-size <N>
"""

import argparse
import os
import sys
import time
import queue
import random
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from bryckcloud.lib.libutils import logger, get_filesize
from bryckcloud.lib.bcloud_sql import update_transferred_bytes, update_transfer_progress
from bryckcloud.lib.cloud.aws import (get_region, _get_aws_config_path,
                                      _get_aws_credentials_path,
                                      _get_boto3_session, url_parse)
from bryckcloud.lib.cloud import upload_report
from bryckcloud.lib.cloud import batch_state
from bryckcloud.lib.config import CloudConfig


# NUL is the one byte a POSIX path can never contain, used here only to build a
# composite in-memory batch key ("<transfer_dir>\0<batch_name>").
_NUL = "\0"


def clean_s3_key(key):
    """Return a botocore-encodable S3 key from a possibly-surrogateescaped path.

    Paths read with ``errors='surrogateescape'`` can carry lone surrogates that
    represent non-UTF-8 filename bytes, which botocore cannot sign. Recover the
    raw bytes and decode: UTF-8 first (common case), else latin-1 (lossless 1:1
    byte->codepoint) so the key stays close to the original name (challenge #9).
    """
    raw = key.encode("utf-8", "surrogateescape")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


class FallbackWorker:
    def __init__(self, transfer_id, transfer_type, batch_transfer_dir, pool_size):
        self.transfer_id = transfer_id
        self.transfer_type = transfer_type
        # Batch-state root (<BATCH_FILE_DIR>/transfer_<id>): holds batches/ and
        # the _fallback_done marker aws.py writes when the pipeline is finished.
        self.batch_transfer_dir = batch_transfer_dir
        self.done_marker = os.path.join(batch_transfer_dir, "_fallback_done")
        self.region = get_region()

        local_aws = CloudConfig().bcloud
        self._config = local_aws
        self._endpoint_url = None
        if "LOCAL_AWS" in local_aws and local_aws["LOCAL_AWS"]:
            self._endpoint_url = local_aws["LOCAL_AWS"]

        # --- Dynamic pool sizing (challenge #4/#21, design §4) --------------
        # The fallback drains only the FAILED subset, so concurrency is scaled
        # to the live backlog between a floor and a ceiling instead of pinning a
        # fixed pool. ``pool_size`` (from --pool-size / TM_THREAD_POOL_SIZE) is
        # the default ceiling; a FALLBACK config section overrides min/max and
        # retry/backoff.
        fb = local_aws.get("FALLBACK", {}) or {}
        max_proc = int(fb.get("max_processes", 0) or 0)
        threads_pp = int(fb.get("threads_per_process", 0) or 0)
        computed_max = max_proc * threads_pp if (max_proc and threads_pp) else 0
        self.max_concurrency = max(1, computed_max or int(pool_size))
        min_proc = int(fb.get("min_processes", 0) or 0)
        computed_min = min_proc * threads_pp if (min_proc and threads_pp) else 0
        self.min_concurrency = max(1, min(self.max_concurrency,
                                          computed_min or max(2, self.max_concurrency // 8)))
        self.pool_size = self.max_concurrency
        self.max_attempts = max(1, int(fb.get("max_attempts", 3)))
        self.backoff_base = float(fb.get("backoff_base_sec", 1.0))
        self.backoff_max = float(fb.get("backoff_max_sec", 30.0))
        self.poll_interval = float(fb.get("poll_interval_sec", 1.0))
        # TEST: fallback failure tests — fraction of files to fail on purpose
        # (FALLBACK_TEST_FAILURE_RATE, 0 = off) to exercise the failure path.
        self.test_failure_rate = float(local_aws.get("FALLBACK_TEST_FAILURE_RATE", 0.0))

        # Directory cloudcp writes the per-batch retry .lst files into
        # (== the transfer log dir). We glob this instead of receiving records
        # over a socket — the .lst on disk is the durable, crash-safe queue.
        self.lst_dir = upload_report.transfer_dir(transfer_id, local_aws)
        # .lst paths already ingested this run (so we don't re-read them).
        self._claimed_lsts = set()

        self._transferred = 0
        self._failed = 0
        self._stats_lock = threading.Lock()
        self._work_queue = queue.Queue()
        self._done = threading.Event()

        # Per-batch completion tracking (design §11): a batch handed to the
        # fallback is only marked COMPLETED once every file in its failed-set
        # has been drained here (success or terminal failure). Keyed by
        # "<transfer_dir>\0<batch_name>".
        self._batches = {}
        self._batches_lock = threading.Lock()

        # boto3 S3 client — a *different* client stack from cloudcp's C++ SDK,
        # which is the whole point of the fallback (challenge #4): it insulates
        # against C++ SDK edge cases (multipart stalls, endpoint quirks).
        # botocore clients are thread-safe, so one shared client serves the
        # whole thread pool. ARN/assumed-role creds are honored via the shared
        # session helper.
        session = _get_boto3_session()
        client_kwargs = {}
        if self.region:
            client_kwargs["region_name"] = self.region
        if self._endpoint_url:
            client_kwargs["endpoint_url"] = self._endpoint_url
        self._s3 = session.client("s3", **client_kwargs)

        # Per-file transfer-history logger. cloud_transfer.py treats the
        # existence of this file as proof the transfer was initiated, so it
        # MUST be written even when every file goes through the fallback path
        # (e.g. HI_PERF_OPT disabled).
        self._txlog = self._setup_txhistory_logger(local_aws)

        # Ensure AWS env vars are set for boto3 credential resolution
        # (also set by _get_boto3_session; kept here defensively).
        os.environ["AWS_CONFIG_FILE"] = _get_aws_config_path()
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = _get_aws_credentials_path()
        os.environ["AWS_SDK_LOAD_CONFIG"] = "1"

    def _setup_txhistory_logger(self, local_aws):
        """Create the txhistory logger that writes to the same file aws_transfer.py
        uses: <LOGS_DIR>/cloud_transfer_<id>/cloud_transfer_txhistory_<id>.log."""
        logs_dir = local_aws.get(
            "LOGS_DIR", "/opt/bryck/bryckapi/downloads/cloud_transfer_logs")
        log_dir = os.path.join(logs_dir, "cloud_transfer_{}".format(self.transfer_id))
        tx_logger = logging.getLogger("fallback_txhistory_{}".format(self.transfer_id))
        tx_logger.setLevel(logging.INFO)
        tx_logger.propagate = False
        if not tx_logger.handlers:
            try:
                os.makedirs(log_dir, exist_ok=True)
                log_file = os.path.join(
                    log_dir, "cloud_transfer_txhistory_{}.log".format(self.transfer_id))
                handler = logging.FileHandler(log_file, errors="surrogateescape")
                handler.setFormatter(
                    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                tx_logger.addHandler(handler)
            except Exception as e:
                logger.debug("Fallback: failed to init txhistory logger: {}".format(e))
        return tx_logger

    def _transfer_one(self, src, dst):
        """Transfer a single file using boto3, with bounded retries + HeadObject.

        Uses the boto3 client stack (challenge #4) instead of ``aws s3 cp``,
        retries transient errors up to ``max_attempts`` with exponential backoff
        (design §4), and confirms the object landed with a HeadObject
        (challenge #13) before declaring success. Returns:
            (src, dst, rc, size, etag, err)
        where rc==0 means verified success.
        """
        # TEST: fallback failure tests — inject a random terminal failure.
        if self.test_failure_rate > 0 and random.random() < self.test_failure_rate:
            logger.debug("Fallback: injected test failure for {}".format(src))
            return (src, dst, 1, 0, "", "injected test failure")
        last_err = ""
        for attempt in range(1, self.max_attempts + 1):
            try:
                if self.transfer_type == "upload":
                    logger.debug(f"Uploading via fallback {src} {dst}")
                    local = src.strip('"')
                    bucket, key = url_parse(dst)
                    if not bucket or key is None:
                        return (src, dst, 1, 0, "", "cannot parse s3 destination: {}".format(dst))
                    # Recover a botocore-encodable key from surrogateescape bytes.
                    key = clean_s3_key(key)
                    try:
                        local_size = os.path.getsize(local)
                    except OSError as e:
                        logger.debug("Fallback: cannot stat local file {}: {}".format(local, e))
                        return (src, dst, 1, 0, "", "cannot stat local file: {}".format(e))
                    self._s3.upload_file(local, bucket, key)
                    # HeadObject verification (challenge #13): confirm existence
                    # + size match before recording success.
                    head = self._s3.head_object(Bucket=bucket, Key=key)
                    s3_size = head.get("ContentLength", 0)
                    etag = head.get("ETag", "").strip('"')
                    if s3_size != local_size:
                        return (src, dst, 1, s3_size, etag,
                                "size mismatch after upload: local={} s3={}".format(local_size, s3_size))
                    return (src, dst, 0, s3_size, etag, "")
                else:
                    local = dst.strip('"')
                    bucket, key = url_parse(src)
                    if not bucket or key is None:
                        return (src, dst, 1, 0, "", "cannot parse s3 source: {}".format(src))
                    key = clean_s3_key(key)
                    parent = os.path.dirname(local)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    self._s3.download_file(bucket, key, local)
                    size = os.path.getsize(local)
                    return (src, dst, 0, size, "", "")
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_attempts:
                    delay = min(self.backoff_max, self.backoff_base * (2 ** (attempt - 1)))
                    logger.debug("Fallback attempt {}/{} failed for {}: {} — retrying in {:.1f}s"
                                 .format(attempt, self.max_attempts, src, last_err, delay))
                    time.sleep(delay)
        return (src, dst, 1, 0, "", last_err)

    def _register_batch(self, transfer_dir, batch_name, count, lst_path):
        """Register a batch's retry list; track its outstanding file count.

        The batch is already in ``batches/inprogress/`` (aws_transfer claimed it
        and deferred completion). It stays there until every one of its ``count``
        failed files has drained here, at which point (design §11) it is moved to
        ``completed/`` and the ``.lst`` is renamed ``.lst.done`` — "done = cloudcp
        ran AND fallback drained its failed-set".
        """
        if not transfer_dir or not batch_name or count <= 0:
            return None
        key = transfer_dir + _NUL + batch_name
        with self._batches_lock:
            b = self._batches.get(key)
            if b is None:
                b = {"dir": transfer_dir, "name": batch_name, "remaining": 0,
                     "failed": 0, "lst": lst_path}
                self._batches[key] = b
            b["remaining"] += count
        return key

    def _batch_file_done(self, key, ok=True):
        """Decrement a batch's outstanding count; complete it when it hits zero.

        Only a fully-successful batch is completed + its ``.lst`` retired; any
        terminal failure leaves it inprogress (with the ``.lst``) for a resume.
        """
        if not key:
            return
        with self._batches_lock:
            b = self._batches.get(key)
            if b is None:
                return
            if not ok:
                b["failed"] += 1
            b["remaining"] -= 1
            if b["remaining"] > 0:
                return
            self._batches.pop(key, None)
        if b["failed"]:
            logger.warning("Fallback: batch {} had {} terminal failure(s) — "
                           "leaving inprogress for retry".format(b["name"], b["failed"]))
            return
        # Drained clean: mark the batch completed (idempotent, cross-process safe)
        # and retire its retry list so it is not re-ingested on resume.
        try:
            batch_state.complete(b["dir"], b["name"])
            logger.debug("Fallback drained batch {} — marked completed".format(b["name"]))
        except Exception as e:
            logger.warning("Fallback: failed to complete batch {}: {}".format(b["name"], e))
            return
        lst = b.get("lst")
        if lst:
            try:
                os.rename(lst, lst + ".done")
            except OSError as e:
                logger.debug("Fallback: failed to retire retry list {}: {}".format(lst, e))

    def _handle_result(self, src, dst, rc, size, etag, err, batch_key=None):
        """Process a single verified transfer result — stats, report, txhistory.

        No xattr is written (challenge #14/#16): the durable upload report is
        the source of truth. Successful fallback uploads are appended to the
        report with status FALLBACK_OK (challenge #5/#8); terminal failures go
        to the human error log and the machine-readable failed_uploads log. The
        owning batch's outstanding count is decremented last so batch completion
        only fires after the result is durably recorded.
        """
        # local file is the source for uploads, the destination for downloads
        local_path = src if self.transfer_type == "upload" else dst
        with self._stats_lock:
            if rc == 0:
                self._transferred += 1
                try:
                    update_transfer_progress(size, 1, self.transfer_id)
                except Exception:
                    pass
                if self.transfer_type == "upload":
                    try:
                        upload_report.append_success(
                            self.transfer_id, local_path.strip('"'), dst.strip('"'),
                            size, etag, status="FALLBACK_OK", attempt=2,
                            config=self._config)
                    except Exception as e:
                        logger.debug("Fallback: failed to append report row: {}".format(e))
                verb = "Uploaded" if self.transfer_type == "upload" else "Downloaded"
                self._txlog.info("{}: src:{} dst:{} size:{}".format(verb, src, dst, size))
            else:
                self._failed += 1
                logger.debug(f"Fallback failed: src={src} err={err}")
                self._txlog.info("Failed: src:{} dst: {}".format(src, dst))
                # Two separate side logs (challenge #8): human-readable error +
                # machine-readable terminal-failure record for the report.
                try:
                    upload_report.log_error(self.transfer_id, local_path.strip('"'),
                                            dst.strip('"'), err, config=self._config)
                    upload_report.append_failed(self.transfer_id, local_path.strip('"'),
                                                dst.strip('"'), size, err,
                                                config=self._config)
                except Exception as e:
                    logger.debug("Fallback: failed to record terminal failure: {}".format(e))
        # Outside the stats lock: advance batch completion accounting.
        self._batch_file_done(batch_key, ok=(rc == 0))

    def _ingest_new_lists(self):
        """Glob for new cloudcp retry ``.lst`` files and enqueue their records.

        Socket-free handoff: cloudcp writes ``cloudcp_retry_<id>_<batch>.lst``
        atomically (temp+rename) at batch end, so a file appearing under its
        final name is a complete, ready retry list. Each new list is read once,
        its owning batch registered for completion tracking, and its files
        pushed to the work queue. Crash-safe: on restart we simply re-glob and
        re-ingest any ``.lst`` not yet retired to ``.lst.done``.

        Returns the number of new lists ingested.
        """
        ingested = 0
        for lst_path in upload_report.iter_pending_retry_lists(self.transfer_id, self._config):
            if lst_path in self._claimed_lsts:
                continue
            # New (unclaimed) list: load the source-of-truth completed set
            # (transfer_report_<id>.csv) so already-recorded files are skipped.
            completed = upload_report.load_completed(self.transfer_id, self._config)
            self._claimed_lsts.add(lst_path)
            stem = upload_report.batch_stem_from_retry_list(lst_path, self.transfer_id)
            if not stem:
                continue
            # stem is already the full batch filename (e.g. batch_000123.txt);
            # do NOT re-append .txt or batch_state.complete() won't find it.
            batch_name = stem
            records = upload_report.read_retry_list(lst_path)
            if self.transfer_type == "upload":
                # Cross-check the completed set before queuing: drop files a
                # prior run already recorded so they aren't re-queued/re-counted.
                records = [r for r in records if r[0].strip('"') not in completed]
            if not records:
                # Empty retry list or every file already completed: retire it
                # and complete the batch directly.
                try:
                    batch_state.complete(self.batch_transfer_dir, batch_name)
                    os.rename(lst_path, lst_path + ".done")
                except OSError:
                    pass
                continue
            key = self._register_batch(self.batch_transfer_dir, batch_name,
                                       len(records), lst_path)
            for local_path, s3path, _size, _err in records:
                # cloudcp records local_path + s3path; map to (src, dst) per direction.
                if self.transfer_type == "upload":
                    self._work_queue.put((local_path, s3path, key))
                else:
                    self._work_queue.put((s3path, local_path, key))
            ingested += 1
        return ingested

    def run(self):
        """Main loop: glob retry lists, drain via a load-scaled boto3 pool.

        Exits when the ``_fallback_done`` marker (written by aws.py after the
        pipeline) is present AND there are no un-ingested ``.lst`` files AND
        nothing is queued or in flight.
        """
        logger.info("Fallback worker started: transfer_id={} concurrency={}-{} "
                    "lst_dir={}".format(self.transfer_id, self.min_concurrency,
                                        self.max_concurrency, self.lst_dir))
        if self.test_failure_rate > 0:
            logger.info("Fallback: TEST failure injection ON (rate={})".format(
                self.test_failure_rate))
        # Worker pool sized to the ceiling; in-flight work is bounded dynamically
        # by a target that tracks the live backlog (challenge #4/#21) so we don't
        # hold max concurrency when load is light.
        executor = ThreadPoolExecutor(max_workers=self.max_concurrency,
                                      thread_name_prefix="fb")
        pending = {}
        last_poll = 0.0

        def _target_inflight():
            backlog = self._work_queue.qsize() + len(pending)
            return max(self.min_concurrency, min(self.max_concurrency, backlog))

        try:
            while True:
                # Periodically ingest newly-appeared retry lists.
                now = time.monotonic()
                if now - last_poll >= self.poll_interval:
                    self._ingest_new_lists()
                    last_poll = now

                # Submit while under the load-scaled in-flight target.
                if len(pending) < _target_inflight():
                    try:
                        src, dst, batch_key = self._work_queue.get(timeout=0.2)
                        future = executor.submit(self._transfer_one, src, dst)
                        pending[future] = (src, batch_key)
                    except queue.Empty:
                        pass
                else:
                    time.sleep(0.02)

                # Collect completed futures
                completed = [f for f in list(pending.keys()) if f.done()]
                for future in completed:
                    src_path, batch_key = pending.pop(future)
                    try:
                        src, dst, rc, size, etag, err = future.result()
                        self._handle_result(src, dst, rc, size, etag, err, batch_key)
                    except Exception as e:
                        self._handle_result(src_path, "", 1, 0, "", str(e), batch_key)

                # Exit when the pipeline is done AND everything is drained.
                if (os.path.exists(self.done_marker)
                        and self._work_queue.empty() and not pending):
                    # Final ingest pass in case a .lst appeared just before the
                    # marker was written.
                    if self._ingest_new_lists() == 0 and self._work_queue.empty():
                        break

            # Final drain: wait for any remaining futures
            for future in as_completed(pending):
                src_path, batch_key = pending[future]
                try:
                    src, dst, rc, size, etag, err = future.result()
                    self._handle_result(src, dst, rc, size, etag, err, batch_key)
                except Exception as e:
                    self._handle_result(src_path, "", 1, 0, "", str(e), batch_key)

        finally:
            executor.shutdown(wait=True)

        logger.info(f"Fallback worker done: transferred={self._transferred} "
                    f"failed={self._failed}")
        return self._failed


def write_done_marker(batch_transfer_dir):
    """Signal the fallback worker that the pipeline is finished.

    Socket-free replacement for the old ``signal_done``: aws.py writes a
    ``_fallback_done`` marker file after the enumerate|cloudcp pipeline exits.
    The worker drains any remaining ``.lst`` files and then exits on its own.
    """
    try:
        marker = os.path.join(batch_transfer_dir, "_fallback_done")
        with open(marker, "w") as f:
            f.write(str(time.time()))
        return marker
    except OSError as e:
        logger.debug(f"Failed to write fallback done marker: {e}")
        return None


if __name__ == "__main__":
    # Worker mode (socket-free). Positional: transfer_id, transfer_type,
    # batch_transfer_dir (== <BATCH_FILE_DIR>/transfer_<id>).
    parser = argparse.ArgumentParser(description="Fallback retry worker for failed cloud transfers")
    parser.add_argument("--transfer-id", type=int, help="Transfer ID")
    parser.add_argument("--transfer-type", choices=["upload", "download"], help="Transfer type")
    parser.add_argument("--transfer-dir", required=True,
                        help="Batch-state root: <BATCH_FILE_DIR>/transfer_<id>")
    parser.add_argument("--pool-size", type=int, default=16, help="Max thread pool size")
    args = parser.parse_args()

    try:
        worker = FallbackWorker(args.transfer_id, args.transfer_type,
                                args.transfer_dir, args.pool_size)
        rc = worker.run()
        sys.exit(1 if rc > 0 else 0)
    except Exception as e:
        logger.error(f"Fallback worker crashed: transfer_id={args.transfer_id} error={e}")
        sys.exit(2)
