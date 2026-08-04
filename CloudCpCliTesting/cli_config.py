"""cli_config.py — Shared configuration, paths, and environment helpers.

All host-side paths and defaults for the CloudCP CLI testing package.
Override any value by setting the matching environment variable (listed per
constant), or by patching this module in your test harness.

Config file: /etc/bryck/bryckcloud/config.json
Design ref:  ../docs/config_reference.md
"""

import json
import os
import pathlib

# ---------------------------------------------------------------------------
# Host-side binary / install paths
# (source: tsecb/bryck config.json and run_cloudcp_tests.py defaults)
# ---------------------------------------------------------------------------

CLOUDCP_BIN: str = os.environ.get(
    "CLOUDCP_BIN",
    "/opt/bryck/aws/bin/cloudcp",
)
CLOUDCP_LD_LIBRARY_PATH: str = os.environ.get(
    "CLOUDCP_LD_LIBRARY_PATH",
    "/opt/bryck/aws/lib/",
)

# Full transfer command as the broker uses it (TRANSFER.TRANSFER_CMD in config.json)
CLOUDCP_INVOKE: str = (
    f"export LD_LIBRARY_PATH={CLOUDCP_LD_LIBRARY_PATH}; {CLOUDCP_BIN}"
)

DEFAULT_DATAGEN_BIN: str = os.environ.get(
    "DATAGEN_BIN",
    "/home/bryck/rperiyas/datagen",
)

# ---------------------------------------------------------------------------
# Broker / scheduler paths
# (source: schedular_test.py defaults, tsecb/cloud package layout)
# ---------------------------------------------------------------------------

SCHEDULER_PYTHON: str = os.environ.get(
    "SCHEDULER_PYTHON",
    "/opt/bryck/.venv/bryck/bin/python3",
)
# The scheduler script; contains a minor x.y wildcard — resolve at runtime.
SCHEDULER_SCRIPT_GLOB: str = (
    "/opt/bryck/.venv/bryck/lib/python3.*/site-packages/bryckcloud/lib/cloud/batch_scheduler.py"
)

CONFIG_FILE: str = os.environ.get(
    "BRYCK_CONFIG",
    "/etc/bryck/bryckcloud/config.json",
)

# ---------------------------------------------------------------------------
# Transfer storage paths
# (source: BATCH.BATCH_FILE_DIR and LOGGING.LOGS_DIR in config.json)
# ---------------------------------------------------------------------------

BCLOUD_BATCHMETA: str = os.environ.get(
    "BCLOUD_BATCHMETA",
    "/opt/bryck/bryckapi/downloads/bcloud_batchmeta",
)
TRANSFER_LOGS_DIR: str = os.environ.get(
    "TRANSFER_LOGS_DIR",
    "/opt/bryck/bryckapi/downloads/cloud_transfer_logs",
)
CLOUDCP_LOG: str = os.path.join(TRANSFER_LOGS_DIR, "cloudcp.log")

# Per-transfer sub-directories (constructed at runtime from transfer_id)
def transfer_dir(transfer_id: int) -> pathlib.Path:
    """Return the transfer metadata directory for the given transfer ID."""
    return pathlib.Path(BCLOUD_BATCHMETA) / f"transfer_{transfer_id}"


def transfer_report_path(transfer_id: int) -> pathlib.Path:
    """Return the expected CSV report path for a completed transfer."""
    return (
        pathlib.Path(TRANSFER_LOGS_DIR)
        / f"cloud_transfer_{transfer_id}"
        / f"transfer_report_{transfer_id}.csv"
    )


# ---------------------------------------------------------------------------
# S3 / object-store defaults
# (source: CLOUD section and user-provided config.json)
# ---------------------------------------------------------------------------

DEFAULT_ENDPOINT: str = os.environ.get(
    "S3_ENDPOINT",
    "https://10.10.10.103:9000",
)
DEFAULT_BUCKET: str = os.environ.get(
    "S3_BUCKET",
    "aditya",
)
DEFAULT_PREFIX: str = os.environ.get(
    "S3_PREFIX",
    "cli_test",
)
AWS_CONFIG_FILE: str = os.environ.get(
    "AWS_CONFIG_FILE",
    "/home/bryck/.aws/config",
)

# ---------------------------------------------------------------------------
# Dataset roots
# (source: dataset_cloudcp/spec_files/dataset_map.json)
# ---------------------------------------------------------------------------

REPO_ROOT: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
DATASET_SPEC_DIR: pathlib.Path = REPO_ROOT / "dataset_cloudcp" / "spec_files"
DATASET_MAP_FILE: pathlib.Path = DATASET_SPEC_DIR / "dataset_map.json"

# Where generated test data lives on the bryck host
DATA_ROOT: str = os.environ.get(
    "CLI_TEST_DATA_ROOT",
    "/bryck/cli_test_data",
)

# ---------------------------------------------------------------------------
# Test run output
# ---------------------------------------------------------------------------

CLI_TEST_RUNS_DIR: str = os.environ.get(
    "CLI_TEST_RUNS_DIR",
    str(pathlib.Path(__file__).resolve().parent / "cli_test_runs"),
)

# ---------------------------------------------------------------------------
# Transfer config baseline
# These mirror the CLOUDCP and TRANSFER sections of the production config.json
# (source: user-provided config.json from tsecb/bryck)
# ---------------------------------------------------------------------------

BASELINE_CONFIG: dict = {
    "TRANSFER": {
        "PARALLEL_TRANSFER": "True",
        "PARALLEL_WORKERS": 15,
        "TRANSFER_CMD": CLOUDCP_INVOKE,
        "FALLBACK_ENABLED": "True",
        "TRANSFER_CLIENT_TYPE": "transfermanager",
        "TM_THREAD_POOL_SIZE": 4,
        "CHUNK_SIZE_MB": 64,
        "HI_PERF_OPT": "True",
        "PERF_STATS": "True",
        "TXR_BATCH_VERIFYSIZE": "True",
        "AZURE_RESUME": "False",
        "BATCH_INCLUDE_SIZE": "True",
        "SKIP_EXISTING": False,
        "TRANSFER_DISPATCH": "broker",
    },
    "CLOUDCP": {
        "MULTIPART_THRESHOLD_MB": 64,
        "MULTIPART_CHUNKSIZE_MB": 64,
        "SKIP_EXISTING": True,
        "TRANSFER_STATS": True,
        "STATS_INTERVAL_SEC": 0,
    },
    "CLOUD": {
        "LOCAL_AWS": DEFAULT_ENDPOINT,
        "PROVIDER": "minio",
        "AWS_CONFIG_FILE": AWS_CONFIG_FILE,
    },
    "NETWORK_PROFILE": "dt2_100gbe",
}

# Valid transfer-report status values (from bcloud_final_design.md §16)
VALID_REPORT_STATUSES: frozenset = frozenset(
    {"SUCCESS", "SKIPPED", "FAILED", "MISMATCH", "PARTIAL"}
)

# Required CSV headers for transfer_report_<id>.csv
REQUIRED_REPORT_HEADERS: list = [
    "file_path",
    "size",
    "status",
    "s3_key",
    "transfer_id",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config_json() -> dict:
    """Load and return the live config.json from the bryck host."""
    with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_dataset_map() -> dict:
    """Load and return the dataset map JSON from this repository."""
    with open(DATASET_MAP_FILE, "r", encoding="utf-8") as fh:
        return json.load(fh)


def resolve_scheduler_script() -> str:
    """Resolve the batch_scheduler.py path (handles the python3.x wildcard)."""
    import glob as _glob

    matches = _glob.glob(SCHEDULER_SCRIPT_GLOB)
    if not matches:
        raise FileNotFoundError(
            f"batch_scheduler.py not found matching: {SCHEDULER_SCRIPT_GLOB}"
        )
    return sorted(matches)[-1]


def next_transfer_id() -> int:
    """Return the next available transfer ID from the batchmeta directory."""
    bm = pathlib.Path(BCLOUD_BATCHMETA)
    if not bm.exists():
        return 1
    existing = [
        int(p.name.split("_")[1])
        for p in bm.iterdir()
        if p.is_dir() and p.name.startswith("transfer_") and p.name.split("_")[1].isdigit()
    ]
    return max(existing, default=0) + 1
