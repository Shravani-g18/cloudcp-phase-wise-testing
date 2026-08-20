from bryckcloud.lib.libutils import run_cmd, logger
from bryckcloud.lib.cloud import cloud_transfer

import re
import platform
import configparser
from os import path
from json import dump, load
from bryckcloud.lib.config import CloudConfig


SAFE_RCLONE_VALUE = {
    "S3_UPLOAD_CONCURRENCY": 4,
    "TRANSFERS": 4,
    "MULTI_THREAD_STREAMS": 4,
    "upload_cutoff" : "8Mi",
    "chunk_size" : "8Mi"
    }

clouds = {"aws": "s3", "azure": "azure", "gcp": "gs"}


def check_obj(obj_tool):
    rc, out, err = run_cmd("sudo yum list installed | grep {}".format(obj_tool))
    if "Ubuntu" in  platform.version():
        rc, out, err = run_cmd("sudo dpkg -l | grep {}".format(obj_tool))
    if len(out) > 0:
        return True
    return False


def configure(cloud_type, username=None, keyid=None, region=None, keyfile=None, tenant_id=None):
    if check_obj("mcli") and not cloud_type == "azure":
        if cloud_type == "aws":
            cmd = "mcli alias set s3 https://s3.amazonaws.com {} {}".format(username, keyid)
        elif cloud_type == "azure":
            return False
        elif cloud_type == "gcp":
            cmd = "mcli alias set gcs https://storage.googleapis.com {} {}".format(username, keyid)
        rc, out, err = run_cmd(cmd)
        if rc:
            logger.error("Failed to setup MCLI")

    global clouds
    cmd = "rclone config delete {}".format(clouds[cloud_type])
    run_cmd(cmd)

    cfg_path = r'/home/bryck/.config/rclone/rclone.conf'
    if path.exists(cfg_path):
        with open(cfg_path) as f:
            cfg = f.read()
    else:
        logger.error("rclone configuration is missing")
        return False

    if cloud_type == "gcp":
        with open(keyfile, 'r') as fp:
            data = load(fp)
        cloud_tmp = ["type = google cloud storage", "project_number = {}".format(data["project_id"]),
                         "client_id = {}".format(data["client_id"]), "service_account_file = " + keyfile]
    elif cloud_type == "aws":
        try:
            config = configparser.ConfigParser()
            config.read(cfg_path)
            
            local_aws = CloudConfig().bcloud
            rclone_params = local_aws.get('RCLONE_PARAMS', SAFE_RCLONE_VALUE) or SAFE_RCLONE_VALUE
            default_upload_cutoff = SAFE_RCLONE_VALUE.get('upload_cutoff')
            default_chunk_size = SAFE_RCLONE_VALUE.get('chunk_size')
            upload_cutoff = rclone_params.get('upload_cutoff', default_upload_cutoff)  or default_upload_cutoff
            chunk_size = rclone_params.get('chunk_size', default_chunk_size) or default_chunk_size

            config['brycks3']['upload_cutoff'] = upload_cutoff
            config['brycks3']['chunk_size'] = chunk_size
            config['brycks3']['region'] = region

            config['s3'] = {}
            config['s3']['type'] = 's3'
            config['s3']['provider'] = 'AWS'
            config['s3']['env_auth'] = 'true'
            config['s3']['chunk_size'] = chunk_size
            config['s3']['region'] = region
            config['s3']['upload_cutoff'] = upload_cutoff

            if len(local_aws["LOCAL_AWS"]) > 0:
                config['s3']['provider'] = local_aws["PROVIDER"].capitalize()
                config['s3']['env_auth'] = 'false'
                config['s3']['access_key_id'] = username
                config['s3']['secret_access_key'] = keyid
                config['s3']['endpoint'] = local_aws["LOCAL_AWS"]

            with open(cfg_path, 'w') as configfile:
                config.write(configfile)

        except Exception as e:
            logger.debug("Error: Failed to configure rclone {}".format(e))

        return True

    elif cloud_type == "azure":
        azure_file = '/home/bryck/result.json'
        azure = {"appId": username, "password": keyid, "tenant": tenant_id}
        with open(azure_file, 'w') as fp:
            dump(azure, fp)
        cloud_tmp = ["type = azureblob", "provider = Other", "env_auth = false", "account = bryck",
                         "service_principal_file = {}".format(azure_file)]

    cfg = cfg.split('\n')
    cfg.append("")
    cfg.append("[{}]".format(clouds[cloud_type]))
    cfg.extend(cloud_tmp)
    cfg = ("\n".join(cfg))

    f = open(cfg_path, "w")
    f.write(cfg)
    f.close()
    return True


def de_configure(cloud_type):
    if check_obj("mcli") and not cloud_type == "azure":
        if cloud_type == "aws":
            cmd = "mcli alias remove s3"
        elif cloud_type == "azure":
            logger.error("mcli not supported")
        elif cloud_type == "gcp":
            cmd = "mcli alias remove gcs"
        rc, out, err = run_cmd(cmd)
        if rc:
            logger.error("Failed to remove the MCLI {}".format(cloud_type))

    if check_obj("rclone"):
        global clouds
        cmd = "rclone config delete {}".format(clouds[cloud_type])
        rc, out, err = run_cmd(cmd)
        if rc:
            logger.error("Failed to remove the rclone {}".format(cloud_type))

    return True


def transfer(src, dst, cloud_type, transfer_id, obj_type="seaweedfs"):
    if obj_type == "minio":
        if src.startswith("brycks3://"):
            src = "bryck/" + src.split("brycks3://")[-1]
            if cloud_type == "aws":
                dst = "s3/" + dst.split("s3://")[-1]
        elif dst.startswith("brycks3://"):
            dst = "bryck/" + dst.split("brycks3://")[-1]
            if cloud_type == "aws":
                src = "s3/" + src.split("s3://")[-1]
        cmd = "mcli cp --recursive {} {} ".format(src, dst)
    else:
        if cloud_type == "azure":
           if src.startswith("https://"):
              src = "azure://" + src.split('core.windows.net/')[-1]
           elif dst.startswith("https://"):
              dst = "azure://" + dst.split('core.windows.net/')[-1]
        rclone_params = CloudConfig().bcloud.get('RCLONE_PARAMS', SAFE_RCLONE_VALUE)
        cmd_with_args = [
            "rclone", "copy", "--metadata", "--checksum",
            f"\'{src}\'", f"\'{dst}\'",
            f"--s3-upload-concurrency={rclone_params.get('S3_UPLOAD_CONCURRENCY')}",
            f"--transfers={rclone_params.get('TRANSFERS')}",
            f"--multi-thread-streams={rclone_params.get('MULTI_THREAD_STREAMS')}",
            "-P"
            ]
        cmd=" ".join(cmd_with_args)
    pattern = r'Transferred:\s+([\d.]+\s+(?:KiB|MiB|GiB|TiB|PiB)\s*/\s*[\d.]+\s+(?:KiB|MiB|GiB|TiB|PiB),\s*\d+%)'
    progress_pattern = re.compile(pattern)
    return cloud_transfer.transfer(cloud_type=cloud_type, transfer_cmd=cmd, transfer_id=transfer_id, src=src, dst=dst, pattern=progress_pattern, obj=True)

def get_size(bucket):
    cmd="rclone size " + bucket + " | awk END'{print $5}' | sed 's/(//g'"
    return run_cmd(cmd) 
