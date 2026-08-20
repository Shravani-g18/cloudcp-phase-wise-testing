from bryckcloud.lib.libutils import run_cmd, logger
from bryckcloud.lib.cloud import cloud_transfer, bryck_obj
from bryckcloud.lib.bcloud_sql import cloud_db
from bryckcloud.lib.config import CloudConfig
from bryckcloud.lib.bcloud_sql import cloud_db
from os.path import exists
from json import load

bcloud_config = CloudConfig()
azure_config = bcloud_config.azure_config

def configure(application_id, tenant_id, secret_key):
    """Configures a Azure server
        Args:
        application_id: Application id of the Azure
        tenant_id: Tenant id of the Azure
        secret_key: Secret key of the Azure
        Returns:
        A tuple: return code, stderr, stdout
    """
    cmd = "az login --service-principal --username \'{}\' --password \'{}\' --tenant \'{}\'".\
        format(application_id, secret_key, tenant_id)
    rc, out, err = run_cmd(cmd)
    if not rc:
        cmd = "export AZCOPY_SPA_CLIENT_SECRET={} && /opt/bryck/.venv/bryck/bin/azcopy login " \
              "--service-principal --application-id \'{}\' --tenant-id \'{}\' ".\
            format(secret_key, application_id, tenant_id)
        rc, out, err = run_cmd(cmd)
    return rc, out, err


def load_azure_env():
    if exists(azure_config):
       with open(azure_config) as f:
         azure_env = load(f)[0]
    else:
       logger.error("Failed to load azure configuration")
       return ""
    az_cmd = "export AZCOPY_AUTO_LOGIN_TYPE=SPN && export AZCOPY_SPA_APPLICATION_ID={} && " \
             "export AZCOPY_SPA_CLIENT_SECRET={} && export AZCOPY_TENANT_ID={} && ". \
             format(azure_env['client_id'], azure_env['client_secret'], azure_env['tenant'])
    return az_cmd


def transfer(src, dst, transfer_id, transfer_type="download", resume=None):
    query = "SELECT TransferType, Resume, Options FROM CloudTransfer WHERE id= {};".format(transfer_id)
    cloud_config = bcloud_config.bcloud

    rc, msg = cloud_db(query, "list")
    transfer_type, resume, options = msg[0]

    az_cmd = load_azure_env()
    cmd = az_cmd + "/opt/bryck/.venv/bryck/bin/azcopy copy --put-md5 \'{}\' \'{}\' --recursive=true".format(src, dst)
    job_id = options 
    if options is not None:
        job_id = options.split('JobId:')[1]
        job_id = validate_jobid(job_id)
    if job_id:
        if cloud_config["AZURE_RESUME"] == "True":
           cmd = az_cmd + "/opt/bryck/.venv/bryck/bin/azcopy jobs resume {} {}".format(job_id, sas)
        else:
           rm_cmd = az_cmd + "/opt/bryck/.venv/bryck/bin/azcopy jobs remove {}".format(job_id)
           run_cmd(rm_cmd)
           query = "UPDATE CloudTransfer SET Options=null WHERE id={}".format(transfer_id)
           cloud_db(query, "update")
    return cloud_transfer.transfer("azure", cmd, transfer_id, src, dst)


def get_size(container):
    az_env = load_azure_env()
    cmd = az_env + "/opt/bryck/.venv/bryck/bin/azcopy list \'" + container + \
          "\' --machine-readable | awk '{sum+=$NF;} END{print sum;}'"
    return run_cmd(cmd)


def get_jobid(src, dst):
    az_env = load_azure_env()
    cmd = az_env + "/opt/bryck/.venv/bryck/bin/azcopy jobs list --with-status=InProgress --log-level=NONE"
    rc, out, err = run_cmd(cmd)
    out = out.split('JobId')[1:]
    for job in out:
        j_src = job.split()[-3].split('/?')[0]
        j_dst = job.split()[-2].split('/?')[0]
        if src == j_src and dst == j_dst:
            return job.split()[1]
    return None


def get_summary(job_id):
    az_env = load_azure_env()
    cmd = az_env + "/opt/bryck/.venv/bryck/bin/azcopy jobs show " + job_id #+ " | grep Percent | awk '{print$4}'"
    rc, out, msg = run_cmd(cmd)
    if not rc:
       msg = out
       out = out.split('\n')
       status = out[-3].split(':')[-1].strip()
       no_files_transfers = int(float(out[6].split(':')[-1].strip())) 
       total_files = int(float(out[5].split(':')[-1].strip()))
       percentage = int(float(out[-4].split(':')[-1].strip())) 
       return rc, status, no_files_transfers, total_files, percentage, msg
    return 1, None, None, None, None, None


def validate_jobid(job_id):
    if len(job_id)==0:
       return None
    az_env = load_azure_env()
    cmd = az_env + "/opt/bryck/.venv/bryck/bin/azcopy jobs show {}".format(job_id)
    rc, out, err = run_cmd(cmd)
    if rc:
        return None
    return job_id
