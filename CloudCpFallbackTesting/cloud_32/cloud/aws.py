# !usr/bin/env python
# coding: utf-8


from botocore.config import Config
from urllib.parse import urlparse
from os import path, makedirs, remove
from pathlib import Path
import botocore
import boto3
import csv
import os
import time
import signal
import subprocess
import configparser
from json import load, dump
from bryckcloud.lib.libutils import run_cmd, logger, load_json_file
import bryckcloud.lib
from bryckcloud.lib.cloud import cloud_transfer, bryck_obj
from bryckcloud.lib.config import CloudConfig

local_aws = CloudConfig().bcloud


def _get_aws_config_path():
    """Return the path to the AWS config file from bcloud config."""
    return local_aws["AWS_CONFIG_FILE"]


def _get_aws_credentials_path():
    """Derive the credentials file path from the config file path."""
    config_path = _get_aws_config_path()
    return path.join(path.dirname(config_path), "credentials")


def _aws_env():
    """Return environment variables dict ensuring consistent config file paths."""
    env = os.environ.copy()
    env["AWS_CONFIG_FILE"] = _get_aws_config_path()
    env["AWS_SHARED_CREDENTIALS_FILE"] = _get_aws_credentials_path()
    env["AWS_SDK_LOAD_CONFIG"] = "1"
    return env


def get_auth_type():
    """Detect the authentication type from the AWS config file.

    Returns:
        str: 'role' if role_arn is configured in the default section,
             'key' for standard access key authentication,
             None if the config cannot be read.
    """
    aws_config = _get_aws_config_path()
    parser = configparser.ConfigParser()
    try:
        parser.read(aws_config)
    except Exception as e:
        logger.debug("Bcloud: Failed to read AWS config file {}, error: {}".format(aws_config, str(e)))
        return None
    # AWS config uses [default] or [profile default] for the default profile
    for section in ("default", "profile default"):
        if parser.has_section(section) and parser.has_option(section, "role_arn"):
            return "role"
    return "key"


def get_role_arn():
    """Read the role_arn from the default section of the AWS config file.

    Returns:
        str or None: The role ARN if configured, None otherwise.
    """
    aws_config = _get_aws_config_path()
    parser = configparser.ConfigParser()
    try:
        parser.read(aws_config)
    except Exception as e:
        logger.debug("Bcloud: Failed to read AWS config file {}, error: {}".format(aws_config, str(e)))
        return None
    for section in ("default", "profile default"):
        if parser.has_section(section) and parser.has_option(section, "role_arn"):
            return parser.get(section, "role_arn").strip()
    return None


def get_source_profile():
    """Read the source_profile from the default section of the AWS config file.

    Returns:
        str or None: The source profile name if configured, None otherwise.
    """
    aws_config = _get_aws_config_path()
    parser = configparser.ConfigParser()
    try:
        parser.read(aws_config)
    except Exception as e:
        logger.debug("Bcloud: Failed to read AWS config file {}, error: {}".format(aws_config, str(e)))
        return None
    for section in ("default", "profile default"):
        if parser.has_section(section) and parser.has_option(section, "source_profile"):
            return parser.get(section, "source_profile").strip()
    return None


def get_external_id():
    """Read the external_id from the default section of the AWS config file.

    Returns:
        str or None: The external ID if configured, None otherwise.
    """
    aws_config = _get_aws_config_path()
    parser = configparser.ConfigParser()
    try:
        parser.read(aws_config)
    except Exception as e:
        return None
    for section in ("default", "profile default"):
        if parser.has_section(section) and parser.has_option(section, "external_id"):
            return parser.get(section, "external_id").strip()
    return None


def get_temporary_credentials():
    """For ARN auth, assume the role and return temporary credentials.

    Returns:
        dict with keys (aws_access_key_id, aws_secret_access_key, aws_session_token)
        or None if not using role auth or assumption fails.
    """
    if get_auth_type() != "role":
        return None

    role_arn = get_role_arn()
    source_profile = get_source_profile()
    external_id = get_external_id()
    region = get_region()

    if not role_arn or not source_profile:
        return None

    try:
        # Create session using source profile (has the actual access keys)
        source_session = boto3.Session(
            profile_name=source_profile,
            region_name=region,
        )
        sts = source_session.client('sts')

        assume_kwargs = {
            'RoleArn': role_arn,
            'RoleSessionName': 'bryckcloud-verification',
            'DurationSeconds': 3600,
        }
        if external_id:
            assume_kwargs['ExternalId'] = external_id

        response = sts.assume_role(**assume_kwargs)
        creds = response['Credentials']
        return {
            'aws_access_key_id': creds['AccessKeyId'],
            'aws_secret_access_key': creds['SecretAccessKey'],
            'aws_session_token': creds['SessionToken'],
        }
    except Exception as e:
        logger.error(f"Failed to assume role for verification: {e}")
        return None


def get_region():
    aws_config = _get_aws_config_path()
    parser = configparser.ConfigParser()
    try:
        parser.read(aws_config)
    except Exception as e:
        logger.debug("Bcloud: Failed to read AWS config file {}, error: {}".format(aws_config, str(e)))
        return None
    for section in ("default", "profile default"):
        if parser.has_section(section) and parser.has_option(section, "region"):
            return parser.get(section, "region").strip()
    return None


def _get_boto3_session():
    """Create a boto3 session that respects the configured auth type.

    For role-based auth, uses the default profile which triggers automatic
    role assumption via the role_arn + source_profile chain.
    """
    os.environ["AWS_CONFIG_FILE"] = _get_aws_config_path()
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = _get_aws_credentials_path()
    os.environ["AWS_SDK_LOAD_CONFIG"] = "1"

    region = get_region()
    auth_type = get_auth_type()

    if auth_type == "role":
        logger.debug("Bcloud: Using role-based authentication (assumed role)")
        return boto3.Session(profile_name="default", region_name=region)
    return boto3.Session(region_name=region)


def _get_s3_client():
    """Return a configured boto3 S3 client using local overrides or configured region."""
    region = get_region()
    session = _get_boto3_session()
    if len(local_aws["LOCAL_AWS"]) > 0:
        return session.client('s3', endpoint_url=local_aws["LOCAL_AWS"],
                              region_name=region)

    if region:
        return session.client('s3', region_name=region)
    logger.debug("Bcloud: AWS region not found in config, using default region for S3 client")
    return session.client('s3')


def validate_cred(access_key_id=None, access_key=None, region=None, cred_file=None):
    """
    Validate credentials
        Args:
        access_key_id: Access key id for the AWS server
        access_key: Access key for AWS server
        cred_file: Credential file
        Returns:
        A tuple: Status of validation
    """
    if cred_file:
        with open(cred_file, mode='r')as file:
            csvFile = csv.reader(file)
            access_key_id = next(csvFile)[0].split("=")[-1]
            access_key = next(csvFile)[0].split("=")[-1]
    aws_config = Config(region_name=region)
    sts = boto3.client('sts', aws_access_key_id=access_key_id, aws_secret_access_key=access_key, config=aws_config)
    try:
        sts.get_caller_identity()
        return True, access_key_id
    except botocore.exceptions.UnauthorizedSSOTokenError:
        logger.error("Invalid credentials: Authorized token error")
        return False, None
    except botocore.exceptions.ClientError:
        logger.error("Invalid credentials: Client error exception")
        return False, None
    except Exception as e:
        logger.error("Validation exception : " + str(e))
        return False, None


def validate_cred_arn(role_arn, source_profile, region=None):
    """Validate ARN-based credentials by assuming the role via the profile chain.

    This writes the config first, then validates by creating a session with the
    default profile (which triggers role assumption via source_profile).

    Args:
        role_arn: The IAM role ARN to assume.
        source_profile: The profile containing credentials to assume the role.
        region: AWS region (optional).

    Returns:
        A tuple: (success: bool, identity: str or None)
    """
    os.environ["AWS_CONFIG_FILE"] = _get_aws_config_path()
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = _get_aws_credentials_path()
    os.environ["AWS_SDK_LOAD_CONFIG"] = "1"

    try:
        session = boto3.Session(profile_name="default", region_name=region)
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        assumed_arn = identity.get("Arn", "")
        logger.debug("Bcloud: ARN validation successful, identity: {}".format(assumed_arn))
        return True, role_arn
    except botocore.exceptions.UnauthorizedSSOTokenError:
        logger.error("Invalid ARN credentials: Authorized token error")
        return False, None
    except botocore.exceptions.ClientError as e:
        logger.error("Invalid ARN credentials: {}".format(str(e)))
        return False, None
    except botocore.exceptions.NoCredentialsError:
        logger.error("Invalid ARN credentials: Source profile credentials not found")
        return False, None
    except Exception as e:
        logger.error("ARN validation exception: " + str(e))
        return False, None


def set_region(region):
    """
    Args:
        region: Region of the cloud service
    Returns: AWS set region status
    """
    return run_cmd("aws configure set region " + region)


def configure(access_key_id=None, access_key=None, region="us-west-1", cred_file=None,
              role_arn=None, source_profile=None, external_id=None):
    """
    Configures a AWS server
    Args:
        access_key_id: Access key id for the AWS server
        access_key: Access key for AWS server
        region: Region of cloud service default is us-west-1
        cred_file: Credential file
        role_arn: IAM role ARN for assumed role authentication
        source_profile: Profile name containing credentials for role assumption
        external_id: External ID for cross-account trust policy (optional)
    Returns:
        A tuple: return code, status of configure status
    """

    if role_arn:
        return configure_arn(role_arn, source_profile, access_key_id, access_key, region, external_id)

    if len(local_aws["LOCAL_AWS"]) == 0:
       validate, access_key_id = validate_cred(access_key_id, access_key, region, cred_file)
       if not validate:
          return 1, "", "Invalid credentials"
    if cred_file:
        cmd = "aws configure import --csv " + cred_file
        return run_cmd(cmd)
    cmd = "aws configure set aws_access_key_id " + access_key_id
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err
    cmd = "aws configure set aws_secret_access_key " + access_key
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err
    rc, out, err = run_cmd("aws configure set region " + region)
    if rc:
        return rc, out, err
    s3_defaults = [
        "aws configure set default.s3.max_concurrent_requests 20",
        "aws configure set default.s3.max_queue_size 1000",
        "aws configure set default.s3.multipart_threshold 64MB",
        "aws configure set default.s3.multipart_chunksize 64MB",
        "aws configure set default.s3.max_attempts 10",
        "aws configure set default.s3.connect_timeout 300",
        "aws configure set default.s3.read_timeout 300",
    ]
    for cmd in s3_defaults:
        rc, out, err = run_cmd(cmd)
        if rc:
            logger.warning(
                "Failed to apply AWS S3 default config [%s]: %s",
                cmd, err)
    return 0, access_key_id, ""


def configure_arn(role_arn, source_profile, access_key_id=None, access_key=None,
                  region="us-west-1", external_id=None):
    """Configure AWS for ARN-based role assumption.

    Sets up the source profile with credentials and the default profile
    with role_arn and source_profile for automatic role assumption.

    Args:
        role_arn: The IAM role ARN to assume.
        source_profile: The profile name for the source credentials.
        access_key_id: Access key id for the source profile.
        access_key: Secret access key for the source profile.
        region: AWS region (default: us-west-1).
        external_id: External ID required by the trust policy (optional).

    Returns:
        A tuple: (return_code, identifier, error_message)
    """
    if not source_profile:
        return 1, "", "source_profile is required for ARN-based authentication"

    if not role_arn.startswith("arn:aws:iam::"):
        return 1, "", "Invalid role_arn format"

    # Set up source profile credentials
    if access_key_id and access_key:
        cmd = "aws configure set aws_access_key_id {} --profile {}".format(
            access_key_id, source_profile)
        rc, out, err = run_cmd(cmd)
        if rc:
            return rc, out, err

        cmd = "aws configure set aws_secret_access_key {} --profile {}".format(
            access_key, source_profile)
        rc, out, err = run_cmd(cmd)
        if rc:
            return rc, out, err

    # Set source profile region
    cmd = "aws configure set region {} --profile {}".format(region, source_profile)
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err

    # Set default profile with role_arn and source_profile
    cmd = "aws configure set role_arn {} --profile default".format(role_arn)
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err

    cmd = "aws configure set source_profile {} --profile default".format(source_profile)
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err

    cmd = "aws configure set region {} --profile default".format(region)
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err

    # Set external_id if provided (required by some cross-account trust policies)
    if external_id:
        cmd = "aws configure set external_id {} --profile default".format(external_id)
        rc, out, err = run_cmd(cmd)
        if rc:
            return rc, out, err

    # Set S3 transfer tuning on the source profile
    s3_settings = [
        ("max_concurrent_requests", "20"),
        ("max_queue_size", "1000"),
        ("multipart_threshold", "64MB"),
        ("multipart_chunksize", "64MB"),
        ("max_attempts", "10"),
        ("connect_timeout", "300"),
        ("read_timeout", "300"),
    ]
    for key, val in s3_settings:
        cmd = "aws configure set s3.{} {} --profile {}".format(key, val, source_profile)
        rc, out, err = run_cmd(cmd)
        if rc:
            logger.warning(
                "Failed to apply AWS S3 config for profile %s [%s]: %s",
                source_profile, key, err)

    # Validate the role assumption works
    if len(local_aws["LOCAL_AWS"]) == 0:
        validate, _ = validate_cred_arn(role_arn, source_profile, region)
        if not validate:
            return 1, "", "ARN role assumption validation failed"

    return 0, role_arn, ""



def transfer(src, dst, transfer_id):
    # Ensure AWS SDK config env vars are set for cloudcp and aws CLI subprocesses
    os.environ["AWS_CONFIG_FILE"] = _get_aws_config_path()
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = _get_aws_credentials_path()
    os.environ["AWS_SDK_LOAD_CONFIG"] = "1"

    region = get_region()
    if region:
        logger.debug("Bcloud: AWS transfer region: {}".format(region))
    else:
        logger.debug("Bcloud: AWS region not found in config, using default region for transfer command")
    endpoint_url = ""
    dir_path = path.dirname(path.realpath(__file__))
    if len(local_aws["LOCAL_AWS"]) > 0:
       endpoint_url = " --endpoint-url " + local_aws["LOCAL_AWS"]

    #Create a log directory
    logdir = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs" 
    if "LOGS_DIR" in local_aws:
        logdir = local_aws["LOGS_DIR"]
        makedirs(logdir, exist_ok=True)
    cloud_logdir = "{}/cloud_transfer_{}".format(logdir, transfer_id)
    makedirs(cloud_logdir, exist_ok=True)

    #Back up the previous failure log with an incrementing retry number instead of deleting it
    log_file= "{}/cloud_transfer_{}.log".format(cloud_logdir, transfer_id)
    if path.exists(log_file):
        try:
            retry_num = 1
            while path.exists("{}.{}".format(log_file, retry_num)):
                retry_num += 1
            os.rename(log_file, "{}.{}".format(log_file, retry_num))
        except Exception as e:
            logger.debug("Failed to back up cloud transfer failure log file {}: {}".format(log_file, str(e)))

    #File transfer stats transfered, skipped, failure count - Enables to creat stat logs
    xfer_state = "/tmp/aws_xfer_stats"
    if "AWS_XFER_STAT" in local_aws.keys():
        xfer_state = local_aws["AWS_XFER_STAT"]
    if not path.exists(xfer_state):
        try:
            Path(xfer_state).touch()
        except Exception as e:
            logger.debug("Failed to create cloud transfer stat {}: {}".format(xfer_state, str(e)))
   
    #Clear the stat file, for every transfer initiate it'll be update
    stat_prefix = "/tmp/aws_bryck_zfer_stat" 
    if "AWS_STAT_PREFIX" in local_aws.keys():
        stat_prefix = local_aws["AWS_STAT_PREFIX"]

    batch_base_dir = local_aws.get("BATCH_FILE_DIR", "/bryck/bcloud_batchmeta")
    batch_transfer_dir = os.path.join(batch_base_dir, "transfer_{}".format(transfer_id))

    stat_file = "{}_{}.json".format(stat_prefix, transfer_id)
    if path.exists(stat_file):
        stat_data = {
            "skipped": 0,
            "failure": 0,
            "transferred": 0
        }
        with open(stat_file, 'w') as sf:
            dump(stat_data, sf)

    # --- Batch-builder-only mode (design §20) -------------------------------
    # Validate batching / throttles / resume on a real tree WITHOUT moving data:
    # run only the enumerator + BatchBuilder (which publishes batches and writes
    # batch_summary.csv) and skip the cloudcp pipeline, the fallback worker, and
    # verification entirely. Toggled by BATCH_BUILDER_ONLY in config (the
    # enumerator honours the same key / its own --batch-only flag).
    if str(local_aws.get("BATCH_BUILDER_ONLY", "False")).lower() == "true":
        logger.info("Batch-only mode for transfer {} — building batches only, "
                    "no transfer".format(transfer_id))
        os.makedirs(batch_transfer_dir, exist_ok=True)
        cmd = "/opt/bryck/.venv/bryck/bin/python3 {}/bcloud_src_enum.py -i {} \"{}\" --batch-only".format(
            dir_path, transfer_id, src)

        return cloud_transfer.transfer("aws", cmd, transfer_id, src, dst)

    # --- Validate source/destination BEFORE dispatching the transfer ---
    if src.startswith("s3://"):
        if not check_s3bucket(src):
            return "FAILED", "{} bucket not found".format(src)
        if not os.access(dst, os.W_OK):
            return "FAILED", "{} doesn't have write permission".format(dst)
    else:
        if src.endswith("/"):
            src = src[:-1]
        if not check_s3bucket(dst):
            return "FAILED", "{} bucket not found".format(dst)
        if not os.access(src, os.R_OK):
            return "FAILED", "{} doesn't have read permission".format(src)

    if local_aws["AWS_PARALLEL"] == "True":
       thread = local_aws["AWS_THREAD"]
       tm_pool_size = local_aws.get("TM_THREAD_POOL_SIZE", 16)

       transfer_type = "download" if src.startswith("s3://") else "upload"

       # Ensure the batch-transfer dir exists (batch-state + fallback markers).
       os.makedirs(batch_transfer_dir, exist_ok=True)

       # --- Start the socket-free fallback worker -------------------------
       # cloudcp writes a durable per-batch retry list (cloudcp_retry_<id>_*.lst)
       # for any objects it could not upload. The fallback worker globs those
       # lists, retries them via boto3, and owns per-batch completion. There is
       # no socket handshake anymore, so we just launch it and confirm it did
       # not die immediately; it drains until we drop the _fallback_done marker.
       fallback_proc = None
       if str(local_aws.get("AWS_CP_FALLBACK", "True")).lower() == "true":
           # Clear any stale done marker from a previous run.
           stale_marker = os.path.join(batch_transfer_dir, "_fallback_done")
           if os.path.exists(stale_marker):
               os.unlink(stale_marker)

           fallback_cmd_args = [
               "/opt/bryck/.venv/bryck/bin/python3",
               "{}/fallback_worker.py".format(dir_path),
               "--transfer-id", str(transfer_id), "--transfer-type", transfer_type,
               "--transfer-dir", batch_transfer_dir,
               "--pool-size", str(tm_pool_size),
           ]
           fallback_proc = subprocess.Popen(
               fallback_cmd_args,
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
               start_new_session=True,
           )

           # Brief liveness check — confirm the worker didn't crash on startup.
           time.sleep(0.5)
           if fallback_proc.poll() is not None:
               logger.error(f"Fallback worker exited prematurely (rc={fallback_proc.returncode})")
               fallback_proc = None

       # --- Build dispatch command ---
       # TRANSFER_DISPATCH selects the dispatcher: the long-lived weighted
       # broker/scheduler (design §9) or the legacy GNU-parallel pipeline
       # (default, kept as a fallback). Both are run by cloud_transfer.transfer
       # exactly the same way — as one subprocess whose pid drives pause/cancel
       # and whose exit triggers verification — so the fallback-worker handoff
       # and finalize path below are identical for either.
       dispatch = str(local_aws.get("TRANSFER_DISPATCH", "parallel")).lower()
       if dispatch == "broker":
          cmd = "/opt/bryck/.venv/bryck/bin/python3 {}/batch_scheduler.py {} {} \"{}\" \"{}\" \"{}\" --transfer-dir \"{}\" --dir-path \"{}\"".format(
              dir_path, transfer_id, transfer_type, src, dst, src, batch_transfer_dir, dir_path)
          if len(local_aws["LOCAL_AWS"]) > 0:
             cmd += " --endpoint-url {}".format(local_aws["LOCAL_AWS"])
       elif src.startswith("s3://"):
          cmd = "/opt/bryck/.venv/bryck/bin/python3 {}/bcloud_src_enum.py -i {} \"{}\" |".format(dir_path, transfer_id, src)
          cmd += " parallel -j {} \'".format(thread)
          cmd += "/opt/bryck/.venv/bryck/bin/python3 {}/aws_transfer.py \"{}\" ".format(dir_path, transfer_id)
          cmd += '\\"{}\\" '+ '\\"\"{}\"\\" '.format(dst) + '\\"\"{}\"\\" '.format(src.replace("'", "'\"'\"'")) + "--expected-size 54760833024 {}\'".format(endpoint_url)
       else:
          src_head, tail = path.split(src)
          cmd = "/opt/bryck/.venv/bryck/bin/python3 {}/bcloud_src_enum.py -i {} \"{}\" | parallel -j {} \'".format(dir_path, transfer_id, src, thread)
          cmd += " /opt/bryck/.venv/bryck/bin/python3 {}/aws_transfer.py \"{}\" ".format(dir_path, transfer_id)
          cmd += '\\"{}\\" '+ '\\"\"{}\"\\" '.format(dst) + '\\"\"{}\"\\" '.format(src.replace("'", "'\"'\"'")) + "--expected-size 54760833024 {}\'".format(endpoint_url)

       # --- Run dispatch (blocks until enumerate|cloudcp fully completes) ---
       state, msg = cloud_transfer.transfer("aws", cmd, transfer_id, src, dst)
       # --- Signal fallback done + wait for it to drain all retry lists -----
       # Socket-free handoff: the pipeline is finished, so drop the
       # _fallback_done marker. The worker finishes draining any remaining
       # cloudcp_retry_*.lst files (owning per-batch completion) and then exits.
       # We must NOT return to verification until it has fully drained.
       logger.debug("Waiting for fallback to be finished")

       if fallback_proc is not None:
            # Worker may still be draining — signal completion first, THEN wait
            # for it. A non-zero exit (or a wait failure) means the fallback
            # could not finish its retries, so mark the transfer FAILED.
            done_marker = os.path.join(batch_transfer_dir, "_fallback_done")
            try:
                with open(done_marker, "w") as _dm:
                    _dm.write(str(time.time()))
            except OSError as e:
                logger.error(f"Failed to write fallback done marker: {e}")
            try:
                fb_rc = fallback_proc.wait()
                if fb_rc == -15:
                    state = "PAUSED"
                elif fb_rc != 0:
                    logger.debug("Fallback worker failed (rc={})".format(fb_rc))
                    state = "FAILED"
                    msg = "Fallback worker failed (rc={})".format(fb_rc)
            except Exception as e:
                state = "FAILED"
                msg = "Fallback worker did not complete: {}".format(e)
                try:
                    fallback_proc.kill()
                    fallback_proc.wait(timeout=5)
                except Exception:
                    pass
       return cloud_transfer.finalize_verify_transfer(transfer_id, src, dst, "aws", state, msg)

    else:
       if region:
           cmd = "aws s3 cp {} {} --recursive --expected-size 54760833024 {} --region {}".format(src, dst, endpoint_url, region)
       else:
           logger.debug("Bcloud: AWS region not found in config, using default region for S3 transfer command")
           cmd = "aws s3 cp {} {} --recursive --expected-size 54760833024 {}".format(src, dst, endpoint_url)
       return cloud_transfer.transfer("aws", cmd, transfer_id, src, dst)


def get_size(bucket_name):
    cmd="aws s3 ls --summarize --recursive " + bucket_name 
    if len(local_aws["LOCAL_AWS"]) >0 :
       cmd +=  " --endpoint-url " + local_aws["LOCAL_AWS"]
    cmd += " | awk END'{print $3}'"
    return run_cmd(cmd)


def check_s3bucket(bucket_name):
    """Check if an S3 bucket exists and is accessible.

    Args:
        bucket_name (str): s3 URL or bucket name (e.g. 's3://my-bucket' or 'my-bucket')

    Returns:
        bool: True if the bucket exists and is accessible, False otherwise.
    """
    parsed = urlparse(bucket_name)
    bucket = parsed.netloc or parsed.path  # allow passing plain bucket name

    s3 = _get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except Exception as e:
        logger.debug("Bcloud: {} bucket error-{}".format(bucket, str(e)))
    return False


def url_parse(bucket_url):
    # urlparse splits on '#' treating it as a fragment — parse manually to preserve special chars
    _url = bucket_url.strip('/')
    for scheme in ("s3://", "brycks3://"):
        if _url.startswith(scheme):
            _url = _url[len(scheme):]
            break
    _slash = _url.find('/')
    if _slash == -1:
        bucket, prefix = _url, None
    else:
        bucket = _url[:_slash]
        prefix = _url[_slash + 1:].lstrip('/') or None
    return bucket, prefix


def get_s3path_type(bucket_url):
    """Detect whether an S3 path is a file, a directory, or does not exist.

    Args:
        bucket_url (str): s3 URL like 's3://my-bucket/path/to/key'.

    Returns:
        str: ``'file'`` if an exact object exists at the key,
             ``'dir'`` if objects exist under the key as a prefix,
             ``'not_found'`` if neither exists,
             ``None`` if the bucket is inaccessible or an error occurs.
    """
    bucket, prefix = url_parse(bucket_url)

    s3 = _get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
        if not prefix:
            return 'dir'  # bucket root is treated as a directory
        try:
            s3.head_object(Bucket=bucket, Key=prefix)
            logger.debug(f"Bcloud: s3://{bucket}/{prefix} is a file")
            return 'file'
        except Exception:
            pass
        dir_prefix = prefix.rstrip('/') + '/'
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=dir_prefix, MaxKeys=1)
        if resp.get('KeyCount', 0) > 0 or resp.get('Contents'):
            logger.debug(f"Bcloud: s3://{bucket}/{prefix} is a directory")
            return 'dir'
        logger.debug(f"Bcloud: s3://{bucket}/{prefix} does not exist")
        return 'not_found'
    except Exception as e:
        logger.debug("Bcloud: {} bucket error-{}".format(bucket, str(e)))
        return None


def check_s3bucket_prefix(bucket_url, is_file=False):
    """Check if an S3 bucket prefix exists, optionally requiring it to be a file.

    Returns:
        bool: True if the path exists and matches the expected type, False otherwise.
    """
    path_type = get_s3path_type(bucket_url)
    logger.debug(f"{bucket_url} is {path_type}")
    if path_type is None or path_type == 'not_found':
        return False
    if is_file:
        return path_type == 'file'
    return True
