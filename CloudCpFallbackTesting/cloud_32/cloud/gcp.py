from bryckcloud.lib.libutils import run_cmd, logger, get_pid
from json import load
from os import path
from bryckcloud.lib.cloud import cloud_transfer, bryck_obj
import re


def configure(keyfile):
    """Configures a GCP server
        Args:
        keyfile: key file of the GCP
        Returns:
        A tuple: return code, stderr, stdout
    """
    # Authenticate using keyfile
    cmd = "gcloud auth activate-service-account --key-file=\'{}\'".format(keyfile)
    rc, out, err = run_cmd(cmd)
    if rc:
        return 2, out, err

    if rc == 0:
        f = open(keyfile)
        data = load(f)
        out = data["client_email"].split('@')[0]
    return rc, out, err


def transfer(src, dst, transfer_id):
    cmd = "gsutil -o GSUtil:parallel_composite_upload_threshold=150MiB  " \
          "-o GSUtil:parallel_thread_count=5 -m cp -Rn -c " + src + " " + dst
    #rc, msg = cloud_transfer.transfer("gcp", cmd, transfer_id, src, dst)
    progress_pattern = re.compile(r'\[\s*([\d.]+\s+\w+)\s*/\s*([\d.]+\s+\w+)\]\s+(\d+%)')
    rc, msg = cloud_transfer.transfer("gcp", cmd, transfer_id, src, dst, progress_pattern)
    return rc, msg


def get_size(bucket_name):
    return run_cmd("gsutil du -s " + bucket_name + " | awk '{print $1}'")
