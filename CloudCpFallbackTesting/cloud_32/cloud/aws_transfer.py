import sys
import os
import hashlib
import logging
import boto3
from botocore.exceptions import ClientError
from json import load, dump
from filelock import FileLock
from os import path, makedirs


from bryckcloud.lib.libutils import run_cmd, logger, get_filesize
from bryckcloud.lib.bcloud_sql import update_transferred_bytes, update_transfer_progress
from bryckcloud.lib.cloud.aws import get_region, _get_boto3_session, url_parse
from bryckcloud.lib.cloud import upload_report
from bryckcloud.lib.cloud import batch_state
from bryckcloud.lib.cloud import mp_batch_retry
from bryckcloud.lib.config import CloudConfig


local_aws = CloudConfig().bcloud
region = get_region()
aws_cmd = "aws s3 cp"
if region:
    aws_cmd = "aws s3 cp --region {}".format(region)

if "AWS_CMD" in local_aws.keys():
    aws_cmd = local_aws["AWS_CMD"]

aws_cp_fallback = "False"
if "AWS_CP_FALLBACK" in local_aws.keys():
    aws_cp_fallback = local_aws["AWS_CP_FALLBACK"]

# Batches carry a per-file size prefix (<size>\0<path>\0) when BATCH_INCLUDE_SIZE
# is on; each record then holds two NULs instead of one, so the raw NUL count is
# 2x the file count and must be halved for live-progress accounting.
batch_include_size = str(local_aws.get("BATCH_INCLUDE_SIZE", "False")).lower() == "true"

# Performance logging: set PERF_STATS=True to log per-batch timing
perf_stats_enabled = local_aws.get("PERF_STATS", "True") == "True"

xattr_xferstate = "user.bryckxferstate"
xattr_ckstate = "user.bryckckchecksum"

log = ""
dbg_log = ""

transferred_bytes = 0


#Log directory
dir_name = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"
if "LOGS_DIR" in local_aws.keys():
    dir_name = local_aws["LOGS_DIR"]
    makedirs(dir_name, exist_ok=True)


#Transfer stats
aws_xfer_stat = "/tmp/aws_xfer_stats"
if "AWS_XFER_STAT" in local_aws.keys():
    aws_xfer_stat = local_aws["AWS_XFER_STAT"]

aws_stat_prefix = "/tmp/aws_bryck_zfer_stat" 
if "AWS_STAT_PREFIX" in local_aws.keys():
    aws_stat_prefix = local_aws["AWS_STAT_PREFIX"]



def get_logger(transfer_id):
    global dir_name
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    logger = logging.getLogger(f"transfer_{transfer_id}")
    logger.setLevel(logging.INFO)

    # Prevent adding multiple handlers if function is called more than once
    if not logger.handlers:
        handler = logging.FileHandler(f"{dir_name}/cloud_transfer_{transfer_id}.log", errors="surrogateescape")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def filename_strip(filename, spl_char):
    filename = filename.strip(spl_char)
    return filename

def get_state(filepath,attr_prop):
    try:
        filepath = filename_strip(filepath, "\"")
        gattr =  os.getxattr(filepath, attr_prop).decode()
        return gattr
    except Exception as e:
        return None

def set_state(filepath,attr_prop,attr_val):
    global dbg_log
    try:
        filepath = filename_strip(filepath, "\"")
        os.setxattr(filepath, attr_prop, attr_val.encode())
    except Exception as e:
        dbg_log.error("SET attr file_path:{} error: {}".format(filepath, str(e)))


def aws_cksum(filename):
    """
    Retrieve the MD5 ETag of an S3 object using boto3.
    """
    try:
        filename = filename.strip().strip('"')
        if not filename.startswith("s3://"):
            return None
        s3_path = filename[len("s3://"):]
        bucket, _, key = s3_path.partition("/")
        session = _get_boto3_session()
        s3_client = session.client("s3", region_name=region if region else None)
        response = s3_client.head_object(Bucket=bucket, Key=key)
        etag = response["ETag"].strip('"')
        return etag
    except ClientError as e:
        logger.error("aws_cksum: Failed to get checksum for {}: {}".format(filename, str(e)))
        return None
    except Exception as e:
        logger.error("aws_cksum: Unexpected error for {}: {}".format(filename, str(e)))
        return None

def get_relativepath(name, basename):
    name = filename_strip(name, "\"")
    basename = filename_strip(basename, "\"")
    prefix = os.path.relpath(name, basename)
    if prefix == ".":
        return os.path.basename(name)
    return prefix

def fs_to_clean_key(path, encoding="cp1252"):
    """Normalize a relative path into a clean-UTF-8 S3 key.

    Valid UTF-8 / ASCII paths are returned unchanged.  Non-UTF-8 names
    (legacy single-byte encodings that os.fsdecode surfaces as
    surrogate-escape code points) are reinterpreted via *encoding* so the
    S3 object key is human-readable UTF-8 and matches the verifier's
    normalized local enumeration.  Idempotent; the local path itself is
    left raw (surrogateescape) so cloudcp can still open the file.
    """
    raw = path if isinstance(path, bytes) else os.fsencode(path)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode(encoding, errors="surrogateescape")

def check_dst_parent(dst):
    """
    Check if the parent directory of dst exists as a file instead of a directory.
    Returns (True, conflicting_path) if a collision is found, else (False, None).
    """
    dst = dst.strip('"')
    parent = os.path.dirname(dst)
    if os.path.exists(parent) and not os.path.isdir(parent):
        return True, parent
    return False, None

def download_postcheck(transfer_id, src, dst, rc, out, err, batch=False, *args, _track_stat=True):
    global xattr_xferstate
    global log
    global dbg_log
    global transferred_bytes
    fallback = 0
    if rc and ("aws s3 cp" not in aws_cmd) and aws_cp_fallback == "True":
        logger.debug("cloudcp failed-falling back to default mode. Transferid:{} src:{} dst:{} rc:{} out:{} err:{}"\
                .format(transfer_id, src, dst, rc, out, err))
        tmp_dst = filename_strip(dst, "\"") + ".bryckawstmp"
        if os.path.exists(tmp_dst):
            os.remove(tmp_dst)

        if os.path.isdir(dst.strip("\"")):
            rc = 1
            err = "Transfer of directory to a file is not supported. Source {} Destination {} is a directory.".format(src, dst)
            logger.error(err)
        else:
            fallback_args = list(*args)
            fallback_args[1] = "\"" + tmp_dst + "\""
            cmd = "aws s3 cp {}".format(" ".join(fallback_args))
            if region:
                cmd += " --region {}".format(region)
            rc, out, err = run_cmd(cmd)
            if not rc and os.path.exists(tmp_dst):
                os.rename(tmp_dst, dst.strip("\""))
        fallback = 1
    if not rc and (len(err.strip()) == 0):
        if _track_stat:
            update_state(transfer_id, transferred=1)
        dst = dst.strip("\"")
        if not fallback:
            src_size, src_cksum = out.split(",")
            f_bytes = int(src_size)
            transferred_bytes += f_bytes
            dbg_log.info("Downloaded: src:{} dst:{} checksum:{} downloadsize:{} size:{}".format(src, dst, src_cksum, src_size, f_bytes))
        else:
            rc_trbytes, f_bytes = get_filesize(dst)
            if rc_trbytes:
                dbg_log.error(f_bytes)
                f_bytes = 0
            else:
                f_bytes = int(f_bytes)
            update_transferred_bytes(f_bytes, transfer_id)
            dbg_log.info("Downloaded: src:{} dst:{} size:{}".format(src, dst, f_bytes))
        if not batch:
            update_transferred_bytes(transferred_bytes, transfer_id)
        set_state(dst, xattr_xferstate, transfer_id)
        return "transferred"
    else:
        if _track_stat:
            update_state(transfer_id, failure=1)
        log.error("Transfer-failure src:{} dst:{} error_msg:\"{}\"".format(src, dst, err))
        logger.debug("Transfer-failure src:{} dst:{} error_msg:\"{}\"".format(src, dst, err))
        dbg_log.info("Failed: src:{} dst: {}".format(src, dst))
        return "failure"


def _batch_record_count(batch_file):
    """Count records in a NUL-framed batch file (records terminated by \\0).

    Cheap live-progress estimate that does not parse cloudcp's report. Returns 0
    on any error. Falls back to newline counting for a legacy newline-framed
    batch (defensive during the cutover).
    """
    try:
        with open(batch_file, "rb") as f:
            data = f.read()
    except OSError:
        return 0
    if not data:
        return 0
    n = data.count(b"\0")
    if n:
        # Size-bearing framing (<size>\0<path>\0) has two NULs per file.
        if batch_include_size:
            return n // 2
        return n
    # Legacy/newline-framed batch: count non-empty lines.
    return sum(1 for ln in data.split(b"\n") if ln.strip())


def _batch_byte_count(batch_file, exclude=None):
    """Sum the local file sizes of a batch — authoritative live-byte progress.

    On a clean upload batch cloudcp has uploaded (and HeadObject size-verified)
    every file, so the local file sizes are the authoritative bytes transferred.
    When the batch was written with sizes (BATCH_INCLUDE_SIZE) we reuse the size
    captured at enumeration — no stat at all; otherwise we stat the paths
    (bounded to one batch, and far cheaper than the HeadObjects cloudcp already
    did). Missing/unreadable files are skipped. ``read_batch_records`` round-trips
    non-UTF-8 names so ``getsize`` opens them correctly. Paths in ``exclude`` are
    skipped (used to drop cloudcp's failed rc==2 subset).
    """
    total = 0
    for p, size in batch_state.read_batch_records(batch_file):
        if exclude and p in exclude:
            continue
        if size is not None:
            total += size
            continue
        try:
            total += os.path.getsize(p)
        except OSError:
            continue
    return total


def _retry_whole_batch(transfer_id, transfer_type, batch_file, bucket, prefix,
                       fs_prefix, endpoint_url, batch_dir, batch_name,
                       batch_tier):
    """Retry a failed batch inline and complete it when every file succeeds."""
    _txlog = dbg_log if dbg_log else None
    endpoint = endpoint_url.split()[-1] if endpoint_url.strip() else ""
    ok, failed, ok_bytes = mp_batch_retry.retry_whole_batch(
        transfer_id, transfer_type, batch_file, bucket, prefix,
        fs_prefix, endpoint, region, local_aws, txlog=_txlog)
    if ok:
        update_state(transfer_id, transferred=ok)
        update_transfer_progress(ok_bytes, ok, transfer_id)
    if failed:
        update_state(transfer_id, failure=failed)
    if ok and failed == 0:
        batch_file = batch_state.complete(batch_dir, batch_name, tier=batch_tier)
    return batch_file, ok, failed, ok_bytes


def batch_transfer(transfer_id, transfer_type, *args):
    global log
    global transferred_bytes

    if perf_stats_enabled:
        import time as _time
        t_start = _time.monotonic()

    base_src = args[0][2]
    del args[0][2]

    src = args[0][0]
    dst = args[0][1]

    endpoint_url = ""
    if len(*args)>4:
        endpoint_url = args[0][4] + " " + args[0][5]
    
    src = src.strip("\'")
    src = src.strip("\"")
    dst = dst.strip("\"")

    # Batch-state claim (resume tracking, xattr-free): a batch published by the
    # enumerator lives under <transfer_dir>/batches/<state>/<tier>/<name> (or a
    # legacy flat <state>/<name>). The tier is derived from the path so the
    # pending -> inprogress -> completed renames stay within the batch's tier.
    # If it is already completed (a duplicate/resume dispatch), skip.
    batch_dir = None
    batch_name = None
    batch_tier = None
    _parsed_dir, _parsed_tier, _parsed_name = batch_state.parse_batch_path(src)
    if _parsed_dir is not None:
        batch_dir = _parsed_dir
        batch_name = _parsed_name
        batch_tier = _parsed_tier
        claimed = batch_state.claim(batch_dir, batch_name, tier=batch_tier)
        if claimed is None:
            return  # already completed in a prior run (resume dedup)
        src = claimed

    if perf_stats_enabled:
        t_upload = _time.monotonic()

    # --- cloudcp-v2 invocation (redesign §2) --------------------------------
    # cloudcp now consumes the RAW batch of absolute paths directly and forms
    # the keys itself; there is no more Python-side pre-processing (rewriting to
    # s3:// URLs) or post-processing (parsing cloudcp's per-line output). cloudcp
    # writes its own three outputs to log_dir: the success CSV, the diagnostic
    # failure log, and a per-batch NUL-framed retry .lst consumed by the boto3
    # fallback.
    #
    #   cloudcp <batch_file> --bucket <b> --prefix <p> --fs-prefix <src_root>
    #           --transfer-id <id> [--endpoint-url <url>]
    batch_file = src
    if transfer_type == "upload":
        bucket, prefix = url_parse(dst)
        fs_prefix = base_src.strip("\"").strip("\'")
    else:
        base_src = base_src.strip("\"")
        bucket, prefix = url_parse(base_src)
        fs_prefix = dst.strip("\"").strip("\'")

    cmd = "{bin} \"{batch}\" --bucket \"{bucket}\" --fs-prefix \"{fs}\" --transfer-id {tid}".format(
        bin=aws_cmd, batch=batch_file, bucket=bucket, fs=fs_prefix, tid=transfer_id)
    if prefix:
        cmd += " --prefix \"{}\"".format(prefix)
    if endpoint_url.strip():
        cmd += " {}".format(endpoint_url.strip())

    rc, out, err = run_cmd(cmd)
    if perf_stats_enabled:
        t_upload_done = _time.monotonic()

    # --- Batch completion coordination (design §12/§14) ---------------------
    # cloudcp exit codes and the authoritative exit-code rule (requirements.txt):
    #   0  -> all objects ok; complete the batch inline.
    #   2  -> partial: some objects failed (per-batch retry .lst written). DEFER:
    #         leave the batch in inprogress; the socket-free fallback globs the
    #         .lst, retries the FAILED SUBSET via boto3, and performs the
    #         completed/ rename itself. (If the fallback is disabled or no .lst
    #         exists, complete inline — the failures are terminal and already
    #         recorded by cloudcp.)
    #   1  -> the WHOLE batch failed (cloudcp couldn't run, or every object
    #         failed). Retry the ENTIRE batch inline with a dedicated boto3
    #         ProcessPool (mp_batch_retry, §12.1) instead of dumping every file
    #         on the shared fallback worker, then complete. Any .lst cloudcp left
    #         is a subset of what we just retried, so retire it.
    fallback_enabled = str(aws_cp_fallback).lower() == "true"
    batch_stem = None
    lst_path = None
    lst_exists = False
    ok = failed = ok_bytes = 0
    if batch_name:
        batch_stem = batch_name[:-4] if batch_name.endswith(".txt") else batch_name
        lst_path = upload_report.retry_list_path(transfer_id, batch_stem, local_aws)
        lst_exists = os.path.exists(lst_path)

    if batch_dir and batch_name:
        if rc == 0:
            batch_file = batch_state.complete(batch_dir, batch_name, tier=batch_tier)
        elif rc == 2:
            if fallback_enabled and lst_exists:
                logger.debug("cloudcp partial for batch {} (rc=2) — deferring "
                             "completion to fallback (retry list present)".format(batch_name))
            else:
                # Nothing (or no one) will retry — the failures are terminal and
                # cloudcp already recorded them; close the batch out.
                batch_file = batch_state.complete(batch_dir, batch_name, tier=batch_tier)
        elif rc == 1:
            # Whole batch failed — retry it inline and keep it exclusive to mp:
            # remove cloudcp's retry .lst up front so the fallback worker (which
            # only globs *.lst) does not also drain the same files.
            if lst_exists:
                try:
                    os.remove(lst_path)
                except OSError:
                    pass
            batch_file, ok, failed, ok_bytes = _retry_whole_batch(
                transfer_id, transfer_type, batch_file, bucket, prefix,
                fs_prefix, endpoint_url, batch_dir, batch_name, batch_tier)
            if perf_stats_enabled:
                t_upload_done = _time.monotonic()
        elif rc == -15:
            if lst_exists:
                try:
                    os.remove(lst_path)
                except OSError:
                    pass
            return
        else:
            # Unexpected exit code: leave inprogress so a resume re-issues it.
            logger.debug("cloudcp unexpected rc for batch {} (rc={}): {}".format(
                            batch_name, rc, (err or "").strip()))
            #if no lst exists then run mp_batch_retry to retry the whole batch
            if not lst_exists:
                batch_file, ok, failed, ok_bytes = _retry_whole_batch(
                    transfer_id, transfer_type, batch_file, bucket, prefix,
                    fs_prefix, endpoint_url, batch_dir, batch_name, batch_tier)
                if perf_stats_enabled:
                    t_upload_done = _time.monotonic()
            

    # Live progress: cloudcp owns the authoritative report, so we do NOT parse
    # it here (that was the old post-processing). On a clean upload batch we
    # advance the transferred-file count by the batch's record count and the
    # transferred-byte count by the batch's summed local file sizes — authoritative
    # (cloudcp uploaded + size-verified every file), bounded to this batch, and
    # cheaper than the HeadObjects cloudcp already did. Skip/failure totals are
    # still reconciled from the report at finalize. (Downloads carry s3:// records,
    # not local paths, so byte progress there stays 0 until finalize.)
    n = 0
    b = 0
    if int(rc) == 0:
        # batch_file was reassigned to the completed/ path by batch_state.complete().
        n = _batch_record_count(batch_file)
        if n:
            b = _batch_byte_count(batch_file)
            update_state(transfer_id, transferred=n)
            update_transfer_progress(b, n, transfer_id)
    elif int(rc) == 1:
        # rc==1 progress was already recorded inline by the mp retry above;
        # surface its counts here so the PERF log reflects them too.
        n = ok
        b = ok_bytes
    elif int(rc) == 2 and lst_exists:
        # Partial: succeeded = batch records minus cloudcp's failed .lst paths.
        # Reuse the batch's own per-file sizes via _batch_byte_count(exclude=...).
        failed_paths = {lp for lp, _s3, _sz, _err in upload_report.read_retry_list(lst_path)}
        n = _batch_record_count(batch_file) - len(failed_paths)
        if n > 0:
            b = _batch_byte_count(batch_file, exclude=failed_paths)
            update_state(transfer_id, transferred=n)
            update_transfer_progress(b, n, transfer_id)

    # Performance timing log (configurable via PERF_STATS in config.json)
    if perf_stats_enabled:
        logger.debug("PERF batch={} files_count:{} total_size:{} upload={:.2f}s total={:.2f}s rc={} batch_file:{}".format(
            os.path.basename(batch_file),
            n, b,
            t_upload_done - t_upload,
            t_upload_done - t_start,
            rc, batch_file))


def upload(transfer_id, *args):
    global log
    global dbg_log
    global dir_name
    src = args[0][0]
    dst = args[0][1]
    dir_name = "{}/cloud_transfer_{}".format(dir_name, transfer_id)
    makedirs(dir_name, exist_ok=True)
    log = get_logger(transfer_id)
    dbg_log = get_logger("txhistory_{}".format(transfer_id))

    if "BATCH_FILE_DIR" in local_aws.keys() and (src.startswith("\""+local_aws["BATCH_FILE_DIR"]) or src.startswith(local_aws["BATCH_FILE_DIR"])):
        batch_transfer(transfer_id, "upload", *args)
        return

    base_src = args[0][2]
    del args[0][2]
   
    dst = filename_strip(dst, "\"")

    if not dst.endswith('/'):
        dst = dst + "/"

    if src.strip("\'") == base_src:
        last_dir = os.path.basename(base_src.strip("\""))
        dst = "\""+dst+fs_to_clean_key(last_dir)+"\""
    else:
        dst = "\""+dst+fs_to_clean_key(get_relativepath(src, base_src))+"\""

    args[0][1] = dst
    cmd_args = " ".join(*args)
    endpoint_url = ""
    if len(*args)>4:
        endpoint_url = args[0][4] + " " + args[0][5]

    src = filename_strip(src, "\'")
    """tmp_src = "r"+"'"+filename_strip(src, "\"")+"'"
    if not os.access(tmp_src, os.R_OK):
        log.error("Transfer-failure src:{} dst:{} error_msg:\"{} doesn't have permission to read\"".format(src, dst, src))
        return"""
    xferval = get_state(src, xattr_xferstate)
    if xferval is None or xferval != transfer_id:
        if aws_cmd == "s5cmd":
            cmd_args = endpoint_url + " cp -p 8 " + src + " " + dst
        rc, out, err = run_cmd("{} {} ".format(aws_cmd, cmd_args))
        if rc and ("aws s3 cp" not in aws_cmd) and  aws_cp_fallback == "True":
            logger.debug("cloudcp failed-falling back to default mode. Transferid:{} src:{} dst:{} rc:{} out:{} err:{}"\
                .format(transfer_id, src, dst, rc, out, err))
            cmd = "aws s3 cp {}".format(" ".join(*args))
            if region:
                cmd += " --region {}".format(region)
            rc, out, err = run_cmd(cmd)
        if not rc:
            update_state(transfer_id, transferred=1)
            src = src.strip("\"")
            rc_trbytes, f_bytes = get_filesize(src)
            if rc_trbytes:
                dbg_log.error(f_bytes)
            else:
                update_transferred_bytes(int(f_bytes), transfer_id)
            dbg_log.info("Uploaded: src:{} dst:{} size:{}".format(src, dst, f_bytes))
            cksum = get_state(src, xattr_ckstate)
            if cksum is not None:
                dst = args[0][1]
                dst_cksum = aws_cksum(dst)
                if cksum.strip("\"") != dst_cksum.strip("\""):
                    log.error("Checksum-mismatch src:{}, dst:{}, cloud_cksum:{}, file_cksum:{}".format(src, dst, dst_cksum, cksum))
                    return
            set_state(src,xattr_xferstate,transfer_id)

        else:
            update_state(transfer_id, failure=1)
            log.error("Transfer-failure src:{} dst:{} error_msg:\"{}\"".format(src, dst, err.strip()))
            logger.debug("Transfer-failure src:{} dst:{} {} {}".format(src, dst, err, out))
            dbg_log.info("Failed: src:{} dst: {}".format(src, dst))
    else:
        update_state(transfer_id, skipped=1)
        rc_trbytes, f_bytes = get_filesize(src.strip("\""))
        if rc_trbytes:
            dbg_log.error(f_bytes)
        else:
            update_transferred_bytes(int(f_bytes), transfer_id)
        dbg_log.info("Skipped: src:{} dst:{} size:{}".format(src, dst, f_bytes))

def download(transfer_id, *args):
    global log
    global dbg_log
    global dir_name
    src = args[0][0]
    dst = args[0][1]
    dir_name = "{}/cloud_transfer_{}".format(dir_name, transfer_id)
    makedirs(dir_name, exist_ok=True)
    log = get_logger(transfer_id)
    dbg_log = get_logger("txhistory_{}".format(transfer_id))

    if "BATCH_FILE_DIR" in local_aws.keys() and (src.startswith(local_aws["BATCH_FILE_DIR"]) or src.startswith("\""+local_aws["BATCH_FILE_DIR"])):
        batch_transfer(transfer_id, "download", *args)
        return

    base_src = args[0][2]
    del args[0][2]

    dst = dst.strip("\"")
    src = src.strip("\'")

    if not dst.endswith('/') and not path.isfile(dst):
        dst = dst + "/"

    if src.strip("\'") == base_src:
        if not path.isfile(dst):
            last_dir = os.path.basename(base_src.strip("\""))
            dst = "\""+dst+last_dir+"\""
    else:
        dst = "\""+dst+get_relativepath(src, base_src)+"\""
    
    args[0][1] = dst

    endpoint_url = ""
    if len(*args)>4:
        endpoint_url = args[0][4] + " " + args[0][5]
    #args[0][1] = args[0][1]+".bryckawstmp"
    cmd_args = " ".join(*args)
    #run_cmd("rm " + args[0][1]+"*")
    if os.path.exists(dst.strip("\"")):
        xferval = get_state(dst, xattr_xferstate)
        if xferval is not None and xferval == transfer_id:
            update_state(transfer_id, skipped=1)
            rc_trbytes, f_bytes = get_filesize(dst.strip("\""))
            if rc_trbytes:
                dbg_log.error(f_bytes)
            else:
                update_transferred_bytes(int(f_bytes), transfer_id)
            dbg_log.info("Skipped: src:{} dst:{} size:{}".format(src, dst, f_bytes))
            return
    collision, conflict_path = check_dst_parent(dst)
    if collision:
        log.error("Transfer-failure transfer_id:{} src:{} dst:{} error_msg:\"Cannot download: {} is a file, expected a directory\"".format(transfer_id, src, dst, conflict_path))
        dbg_log.info("Failed: src:{} dst:{} - {} is a file, not a directory".format(src, dst, conflict_path))
        update_state(transfer_id, failure=1)
        return
    if aws_cmd == "s5cmd":
        dst = filename_strip(dst, "\"")
        cmd_args = endpoint_url + " cp -p 8 " + src + " \"" + dst+".bryckawstmp" + "\""
    rc, out, err = run_cmd("{} {} ".format(aws_cmd, cmd_args))
    download_postcheck(transfer_id, src, dst, int(rc), out, err, False, *args)


def update_state(transfer_id, skipped=0, failure=0, transferred=0):
    if not os.path.exists(aws_xfer_stat):
        return
    stat_file = "{}_{}.json".format(aws_stat_prefix, transfer_id)
    lockfile = "{}_lock_{}".format(aws_stat_prefix, transfer_id)
    if not os.path.exists(lockfile):
        open(lockfile, 'a').close()
    lock = FileLock(lockfile)
    file_state = {}
    try:
        lock.acquire()
        if not os.path.exists(stat_file) or os.path.getsize(stat_file)==0:
            file_state["skipped"] = skipped
            file_state["failure"] = failure
            file_state["transferred"] = transferred
            with open(stat_file, 'w') as f:
                dump(file_state, f, indent=4)
        else:
            with open(stat_file, 'r') as f:
                file_state = load(f)
            file_state["skipped"] += skipped
            file_state["failure"] += failure
            file_state["transferred"] += transferred
            with open(stat_file, 'w') as f:
                dump(file_state, f, indent=4)
    except Exception as e:
        logger.debug("Exception from update stat {}".format(e))
        pass
    finally:
        lock.release()

def main():
    if len(sys.argv) >= 4:
        transfer_id = sys.argv[1]
        dst = sys.argv[3]
        if dst.startswith("\"s3://") or dst.startswith("s3://"):
            upload(transfer_id, sys.argv[2:])
        else:
            download(transfer_id, sys.argv[2:])
    else:
        sys.exit("Usage: python3 script.py <transfer_id> <src> <dst> [args]")

if __name__ == "__main__":
    main()
