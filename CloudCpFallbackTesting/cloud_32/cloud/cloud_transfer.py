from bryckcloud.lib.libutils import run_cmd, logger, get_pid, run_cmd_buf, convert_to_bytes
from bryckcloud.lib.bcloud_sql import cloud_db, get_cloudtransfer
from bryckcloud.lib.bryck_utils import check_bryck_obj_mounted
from bryckcloud.lib.cloud.bcloud_src_enum import calculate_size
from bryckcloud.lib.cloud.verification import verify_transfer, get_cloud_logs_dir
from bryckcloud.lib.config import CloudConfig
from bryckcloud.lib.cloud import aws
from bryckcloud.lib.cloud import gcp
from bryckcloud.lib.cloud import azure
from bryckcloud.lib.cloud import bryck_obj

from datetime import datetime, timezone
from filelock import FileLock
from time import sleep
from threading import Thread
from psutil import pid_exists
from math import floor, ceil
from os import path, makedirs
from json import load
import traceback
import shutil
import ctypes
import re


# Cloud config (config.json) loaded once for this module.
cloud_config = CloudConfig().bcloud
initial_size = 0
transfer_update = "UPDATE CloudTransfer SET CopiedBytes=%s, TotalBytes=%s, LastUpdated=%s, Percentage=%s WHERE id=%s "
OBJ_MOUNT_RETRIES = 5
OBJ_MOUNT_WAIT = 5

def extract_paths(line):
    src_match = re.search(r'Error copying (gs://[^\s:]+)', line)
    dst_match = re.search(r'Local file \((.*?)\)', line)

    if src_match and dst_match:
        source = src_match.group(1)
        destination = dst_match.group(1)
        return source, destination
    else:
        return None, None

def get_dir_size(dirname):
    if not path.exists(dirname):
        return 0
    rc, out, err = run_cmd("du -sb " + dirname + " | awk 'END{print $1}'")
    if rc:
        return None
    return out.strip()


def cal_progress(process, transfer_id, src, dst, cloud_type, progress_pattern, ctx, object_s=False, update=True):
    ctx['line'] = None
    ctx['except_files'] = []
    try:
        for line in process.stdout:
            ctx['line'] = line.strip()
            if ctx['line'].startswith("Error copying "):
                logger.debug(ctx['line'])
                source, destination = extract_paths(ctx['line'])
                ctx['except_files'].append((source, destination))
            try:
                pattern_match = progress_pattern.search(ctx['line'])
                if pattern_match:
                    if object_s:
                        transfer_status = pattern_match.group(1)
                        data_part, percentage = transfer_status.split(',')
                        current_str, total_str = data_part.split('/')
                    else:
                        if len(pattern_match.groups())==4:
                            current_str = pattern_match.group(1)+" "+pattern_match.group(2)
                            total_str = pattern_match.group(3)+" "+pattern_match.group(4)
                        else:
                            current_str, total_str, percentage = pattern_match.groups()
                    copied_bytes = convert_to_bytes(current_str)
                    total_bytes = convert_to_bytes(total_str)
                    percentage = int((copied_bytes / total_bytes) * 100) if total_bytes else 0
                    if update:
                        utc_time = datetime.now().astimezone(timezone.utc)
                        rc, msg = cloud_db(transfer_update, "update", (str(copied_bytes), str(total_bytes), utc_time, str(percentage), transfer_id))
                        if rc:
                            logger.error("Failed to update the copied bytes " + msg)
            except Exception as e:
                continue
    except Exception as e:
        ctx['state'] = "FAILED"
        logger.error("Failed to transfer {}".format(e))
    finally:
        process.wait()

def gcp_transfer(cloud_type, transfer_cmd, transfer_id, src, dst, pattern=None, obj=False, update=True):
    ctx = {'line': None, 'state': 'COMPLETED', 'msg': '', 'except_files': []}
    if obj:
        retry = 0
        while retry < OBJ_MOUNT_RETRIES:
            if check_bryck_obj_mounted():
                break
            sleep(OBJ_MOUNT_WAIT)
            retry += 1
        if retry == OBJ_MOUNT_RETRIES:
            ctx['state'] = "PAUSED"
            ctx['msg'] = f"Object Service not running. Transfer id:{transfer_id} src:{src} dst:{dst} - PAUSED"
            return ctx['state'], ctx['msg']

    # Create logs directory for the transfer
    cloud_logdir = get_cloud_logs_dir(transfer_id)
    if path.exists(cloud_logdir):
        shutil.rmtree(cloud_logdir)
    makedirs(cloud_logdir, exist_ok=True)

    p = run_cmd_buf(transfer_cmd)
    query = "UPDATE CloudTransfer SET transferstate='{}', thread_id=\'{}\' " \
                "WHERE id={} ".format('IN_PROGRESS', p.pid, str(transfer_id))
    rc, ctx['msg'] = cloud_db(query, "update")
    if rc:
        logger.error(ctx['msg'])
    cal_progress_thread = Thread(target=cal_progress, args=(p, transfer_id, src, dst, cloud_type, pattern, ctx, obj, update), daemon=True)
    # Start a daemon thread
    cal_progress_thread.start()
    cal_progress_thread.join()
    if p.returncode:
        # line is defined in cal_progress
        if ctx['line'] and "could not be transferred" in ctx['line']:
            logger.info("Transfer-{}: Retrying transfers {}".format(transfer_id, ctx['except_files']))
            for ex_src, ex_dst in ctx['except_files'][:]:
                if len(ex_src.split('/')[-1])>0:
                    ext_src =ex_src+ex_dst.rsplit(ex_src.split('/')[-1],1)[-1][1:]
                else:
                    ext_src =ex_src+ex_dst.rsplit(ex_src.split('/')[-2],1)[-1][1:]
                logger.info("Transfer-{}: Retrying transfer {} to {}".format(transfer_id, ext_src, ex_dst))
                transfer_cmd = "gsutil -o GSUtil:parallel_composite_upload_threshold=150MiB  -o GSUtil:parallel_thread_count=5 -m cp -Rn -c {} {}".format(ext_src, ex_dst)
                ret_state, ret_msg = gcp_transfer(cloud_type, transfer_cmd, transfer_id, src, dst, pattern, obj, False)
                if ret_state == "FAILED":
                    ctx['state'] = "FAILED"
                    ctx['msg'] = ret_msg
                    logger.error("FAILED TO TRANSFER {}".format(ret_msg))
                else:
                    logger.debug("Removing ({},{})".format(ex_src, ex_dst))
                    ctx['except_files'].remove((ex_src, ex_dst))
        else:
            #elif any(s in line for s in ("Operation completed", "is a file not a directory")):
            ctx['state'] = "FAILED"
            ctx['msg'] = ctx['line']

    if update and not (ctx['state'] == "FAILED"):
            query = "SELECT totalbytes,transferstate FROM CloudTransfer WHERE id={}".format(transfer_id)
            rc, rows = cloud_db(query, "list")
            if rc:
                logger.info("Failed to update the totalbytes of {}".format(transfer_id))
            if not rows[0][1] == "PAUSED":
                if rows[0][0]:
                    copied_bytes = rows[0][0]
                    utc_time = datetime.now().astimezone(timezone.utc)
                    rc, ctx['msg'] = cloud_db(transfer_update, "update", (str(copied_bytes), str(copied_bytes), utc_time, str(100), transfer_id))
                    if rc:
                        logger.error("Failed to update the copied bytes " + ctx['msg'])
                logger.info("{} transfer id:{} src:{} dst:{} successfully completed".format(cloud_type, transfer_id, src, dst))
    if obj and (src.startswith("s3://") or dst.startswith("s3://")):
        # For brycks3 transfers, we need to verify the transfer.
        rc, (bytes_copied, tr_state) = get_cloudtransfer(transfer_id, "copiedbytes", "transferstate")
        if rc:
          logger.debug(f"Failed to fetch current state of object transfer {transfer_id}")
          tr_state = ctx['state']
        if tr_state == "IN_PROGRESS":
            tr_state = ctx['state']
        # Skip verification if transfer was paused/cancelled/stopped
        if tr_state in ("PAUSED", "CANCELLED", "STOPPED"):
            ctx['state'] = tr_state
            return ctx['state'], ctx['msg']
        try:
            verification_state, verification_msg = verify_transfer(transfer_id, src, dst, cloud_type, tr_state)
            if tr_state != "FAILED":
                ctx['state'] = verification_state
                ctx['msg'] = verification_msg
        except Exception as e:
            logger.debug("Verification failed for transfer id:{} src:{} dst:{} with error {}".format(transfer_id, src, dst, str(e)))
            logger.error("Verification failed for transfer id:{} src:{} dst:{}".format(transfer_id, src, dst))
    return ctx['state'], ctx['msg']


def finalize_verify_transfer(transfer_id, src, dst, cloud_type, state, msg):
    """Finalize a completed transfer run and run verification.

    Resolves the terminal state after the transfer subprocess exits:
      * If the transfer was paused/cancelled/stopped, honour that and skip
        verification.
      * A FAILED run is reported as PAUSED so it can be resumed (already-uploaded
        files are skipped on resume).
      * A COMPLETED run is downgraded to PAUSED if the expected history log is
        missing or the failure log is non-empty.
      * Otherwise, re-read the state fresh (to close the race with the pause
        handler) and run verify_transfer unless the transfer was
        cancelled/paused/stopped.

    Returns the final ``(state, msg)`` tuple.
    """
    cloud_logdir = get_cloud_logs_dir(transfer_id)
    rc, (bytes_transferred, tr_state) = get_cloudtransfer(transfer_id, "copiedbytes", "transferstate")
    if rc:
        tr_state = state
    if tr_state in ("PAUSED", "CANCELLED", "STOPPED"):
        state = tr_state
        msg = tr_state
        logger.info("Transfer {} is {} — skipping verification".format(transfer_id, tr_state))
        return state, msg

    if state == "FAILED":
        msg = ("Transfer failed. Some files could not be uploaded. "
               "Please resume the transfer — already-uploaded files are "
               "skipped and only the remaining files are re-uploaded.")
        return "PAUSED", msg

    if state == "COMPLETED":
        log_file = "{}/cloud_transfer_txhistory_{}.log".format(cloud_logdir, transfer_id)
        if not path.exists(log_file):
            state = "PAUSED"
            msg = "ERROR: Failed to initiate cloud transfer {}".format(transfer_id)

        log_file = "{}/cloud_transfer_{}.log".format(cloud_logdir, transfer_id)
        if path.exists(log_file) and path.getsize(log_file) > 0:
            state = "PAUSED"
            msg = "Transfer of one or more files may not have succeeded. Please refer the log file cloud_transfer_{}.log".format(transfer_id)

    # Re-read state fresh before verification to close race with pause handler
    rc, tr_state = get_cloudtransfer(transfer_id, "transferstate")
    if rc:
        tr_state = state
    if not (tr_state in ["CANCELLED", "PAUSED", "STOPPED"]):
        tr_state = state
        logger.info("Verification started for transfer id:{} src:{} dst:{} ".format(transfer_id, src, dst))
        try:
            verification_state, verification_msg = verify_transfer(transfer_id, src, dst, cloud_type, tr_state)
            if state != "FAILED":
                state = verification_state
                msg = verification_msg
        except Exception as e:
            logger.debug("Verification failed for transfer id:{} src:{} dst:{} with error {}".format(transfer_id, src, dst, str(e)))
            logger.error("Verification failed for transfer id:{} src:{} dst:{}".format(transfer_id, src, dst))
    else:
        state = tr_state
        logger.info("Transfer {} is {} — skipping verification".format(transfer_id, tr_state))

    return state, msg


def transfer(cloud_type, transfer_cmd, transfer_id, src, dst, pattern=None, obj=False):
    global initial_size
    state = "COMPLETED"
    try:
        msg = "SUCCESS"
        calculate_src_thread = None

        if cloud_type == "gcp" or obj:
            return gcp_transfer(cloud_type, transfer_cmd, transfer_id, src, dst, pattern, obj)
        else:
            # Reading pid of process and storing in DB
            cloud_logdir = get_cloud_logs_dir(transfer_id)

            p = get_pid(transfer_cmd)
            query = "UPDATE CloudTransfer SET transferstate='{}', thread_id=\'{}\' " \
                "WHERE id={} ".format('IN_PROGRESS', p.pid, str(transfer_id))
            # query = "UPDATE CloudTransfer SET copiedbytes=0, transferstate='{}', thread_id=\'{}\' " \
            #     "WHERE id={} ".format('IN_PROGRESS', p.pid, str(transfer_id))
            rc, msg = cloud_db(query, "update")
            if rc:
                logger.error(msg)

            if not cloud_type == "aws":
                initial_size = get_size(cloud_type, dst)
                if src.startswith("/"):
                    rc, (total_bytes) = get_cloudtransfer(transfer_id, "TotalBytes")
                    if total_bytes is None or int(total_bytes)==0:
                        logger.info("Calculating data transfer size for id-{}...".format(transfer_id))
                        calculate_src_thread = Thread(target=calculate_size, args=(transfer_id, src))
                        calculate_src_thread.setDaemon(True)
                        calculate_src_thread.start()

                # Start a daemon thread
                calculate_progress = Thread(target=progress, args=(transfer_id, src, dst, cloud_type), daemon=True)
                calculate_progress.start()

            logger.info("{} transfer initiated : {} ".format(cloud_type, transfer_id))
            
            rc, out, err = run_cmd(transfer_cmd, p=p)
            if cloud_type == "aws" and str(cloud_config.get("BATCH_BUILDER_ONLY", "False")).lower() == "true":
                if rc == 0:
                    return "COMPLETED", "Batch-only: batches built for {} (no transfer performed)".format(src)
                if rc == 2:
                    return "FAILED", "Batch-only: insufficient space or resume precheck failed"
                if rc == 130:
                    return "PAUSED", "Batch-only: enumeration paused (resumable)"
                return "FAILED", "Batch-only enumeration failed: {}".format((err or out or "").strip())

            if rc or ("cannot resume job" in out):
                state = "FAILED"
                msg = err if len(err)>0 else out
            elif not cloud_type == "aws":
                calculate_progress.join()
                if calculate_src_thread is not None:
                    calculate_src_thread.join()
                rc, (transfer_type, total_bytes) = get_cloudtransfer(transfer_id, "TransferType", "TotalBytes")
                if transfer_type == "upload":
                   total_bytes = abs(int(initial_size) - int(get_size(cloud_type, dst)))
                if not total_bytes == 0:
                    job_id = update_progress(cloud_type, transfer_type, src, dst, transfer_id, total_bytes)
                if cloud_type == "azure":
                   if job_id is not None and len(job_id) > 0:
                      rc, status, files_transfers, total_files, percentage, msg = azure.get_summary(job_id)
                      if status is not None and not (status == "Completed" or status == "InProgress"):
                         state = "FAILED"
            return state, msg

    except Exception as e:
        state = "FAILED"
        msg = str(e)
        logger.error("Transfer_id {} exception with msg {}".format(str(transfer_id), str(e)))
        logger.debug("Transfer_id {} exception with msg {}-{}".format(str(transfer_id), str(e), traceback.format_exc()))
    return state, msg


def get_size(cloud_type, data_path):
    if data_path.startswith("brycks3://") or data_path.startswith("azure://"):
        rc, copied_bytes, err = bryck_obj.get_size(data_path)
    elif cloud_type == "aws":
        rc, copied_bytes, err = aws.get_size(data_path)
    elif cloud_type == "gcp":
        rc, copied_bytes, err = gcp.get_size(data_path)
    elif cloud_type == "azure":
        rc, copied_bytes, err = azure.get_size(data_path)
    else:
        return None
    if rc or len(err)>0:
        return None
    return copied_bytes


def update_progress(cloud_type, transfer_type, src, dst, transfer_id, total_bytes, job_id=None):
    try:
        global initial_size
        copied_bytes = 0
        percentage = 0
        # Query the azure based on job_id
        if cloud_type == "azure" and not(src.startswith("brycks3://") or dst.startswith("brycks3://")):
           if job_id is None:
              rc, options = get_cloudtransfer(transfer_id, "Options")
              if rc or options is None:
                 job_id = azure.get_jobid(src.split('/?')[0], dst.split('/?')[0])
                 if job_id is not None:
                    query = "UPDATE CloudTransfer SET Options='JobId:{}' WHERE id={}".format(job_id, transfer_id)
                    cloud_db(query, "update")
              else:
                 job_id = options.split("JobId:")[-1]
           """if job_id is not None and len(job_id) > 0:
              rc, status, files_transfers, total_files, percentage, msg = azure.get_summary(job_id)
              if status is not None and status in ["Failed", "CompletedWithErrors"]:
                 if transfer_type == "upload":
                    copied_bytes = get_size(cloud_type, dst)
                 else:
                    rc, copied_bytes, err = run_cmd("du -sh " + dst + " | awk 'END{print $1}'")
                    units = {'P' : 1024**5, 'T': 1024**4, 'G': 1024**3, 'M': 1024**2, 'K': 1024}
                    if (copied_bytes is not None and len(copied_bytes.strip()))>0:
                       copied_bytes = int(copied_bytes[:-1])*units[copied_bytes[-1]]
                 if copied_bytes is not None:
                    percentage = round((int(copied_bytes) * 100) / int(total_bytes))
              else:
                 if total_bytes is not None and percentage is not None:
                    copied_bytes = floor(float(total_bytes) * (float(percentage) * 0.01))
                 else:
                    copied_bytes, percentage = 0, 0
        else:"""
        if transfer_type == "upload" or dst.startswith("brycks3://"):
           copied_bytes = get_size(cloud_type, dst)
        else:
            copied_bytes = get_dir_size(dst)
        if copied_bytes is not None:
            copied_bytes = str(abs(int(copied_bytes)-int(initial_size)))
        else:
            copied_bytes = 0

        percentage = 0
        if total_bytes is None or int(total_bytes) == 0:
            rc, (total_bytes) = get_cloudtransfer(transfer_id, "TotalBytes")
        if total_bytes is None:
            total_bytes = 0
        if int(total_bytes)!=0:
            percentage = round((int(copied_bytes) * 100) / int(total_bytes))

        # Update the progress status
        update_db = "UPDATE CloudTransfer SET CopiedBytes=%s, LastUpdated=%s, Percentage=%s WHERE id=%s "
        utc_time = datetime.now().astimezone(timezone.utc)
        rc, msg = cloud_db(update_db, "update", (str(copied_bytes), utc_time, str(percentage), transfer_id))
        if rc:
            logger.error("Failed to update the copied bytes " + msg)
    except ZeroDivisionError:
        logger.error("Update progress Exception {}".format(total_bytes))
    except Exception as e:
        logger.error("Update progress Exception " + str(e))
        pass
    return job_id


def progress(transfer_id, src, dst, cloud_type):
    try:
        global initial_size
        # Query the transfer_type and total_bytes
        rc, (transfer_type, total_bytes) = get_cloudtransfer(transfer_id, "TransferType", "TotalBytes")

        # Calculate the total bytes size of cloud and update DB
        if transfer_type == "download":
            total_bytes = get_size(cloud_type, src)
            utc_time = datetime.now().astimezone(timezone.utc)
            query = "UPDATE CloudTransfer SET TotalBytes={}, LastUpdated=\'{}\' WHERE id={}". \
                format(total_bytes, str(utc_time), transfer_id)
            cloud_db(query, "update")

        # Query the Progress based on job_id for Azure
        job_id = None
        try:
          if cloud_type == "azure" and not(src.startswith("brycks3://") or dst.startswith("brycks3://")):
            rc, options = get_cloudtransfer(transfer_id, "Options")
            if rc:
                job_id = azure.get_jobid(src.split('/?')[0], dst.split('/?')[0])
                query = "UPDATE CloudTransfer SET Options='JobId:{}' WHERE id={}".format(job_id, transfer_id)
                cloud_db(query, "update")
            else:
                job_id = options.split("JobId:")[-1]
        except Exception as e:
            pass

        rc, pid = get_cloudtransfer(transfer_id, "thread_id")
        while pid_exists(int(pid)):
            job_id = update_progress(cloud_type, transfer_type, src, dst, transfer_id, total_bytes, job_id)
            # Progress_interval
            sleep(1)
    except Exception as e:
        logger.error("Progress exception " + str(e))
    except ValueError:
        logger.error("Transfer pid not found, progress thread quits")

