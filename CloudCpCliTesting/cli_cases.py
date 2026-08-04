"""cli_cases.py — Case catalogue, dataset mapping, tags, and skip logic.

Each entry in CASES is a dict describing one CLI test case from cli_test_plan.md.

Fields
------
id          : str   — unique case identifier (e.g. "CLI-SMOKE-01")
group       : str   — logical group name (smoke/boundary/encoding/config/rerun/report/mixed/perf)
tags        : list  — free-form tags used for --tag filtering (group name is always included)
title       : str   — one-line description
datasets    : list  — list of dataset IDs from dataset_map.json to use (first is primary)
config_overrides : dict  — config.json overrides for this case (empty = baseline)
priority    : str   — "P0" or "P2"
pass_criteria : str — plain-text pass criteria (see cli_test_plan.md for full detail)
notes       : str   — optional implementation notes / assumptions
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Case catalogue
# ---------------------------------------------------------------------------

CASES: list[dict[str, Any]] = [

    # ------------------------------------------------------------------ SMOKE
    {
        "id": "CLI-SMOKE-01",
        "group": "smoke",
        "tags": ["smoke", "p0"],
        "title": "Basic end-to-end transfer completes",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "Broker exits 0; transfer_report CSV has 91,320 SUCCESS rows; "
            "no FAILED or MISMATCH rows."
        ),
        "notes": "DS-P7-01 ~300 GB, all tiers; fastest mixed gate.",
    },
    {
        "id": "CLI-SMOKE-02",
        "group": "smoke",
        "tags": ["smoke", "p0", "report"],
        "title": "Transfer report is well-formed",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "CSV has required headers [file_path, size, status, s3_key, transfer_id]; "
            "all status values are valid enum members."
        ),
        "notes": "",
    },
    {
        "id": "CLI-SMOKE-03",
        "group": "smoke",
        "tags": ["smoke", "p0", "s3"],
        "title": "Uploaded objects reachable via HeadObject (sample)",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "Random sample of 100 keys: HeadObject HTTP 200; "
            "Content-Length matches source file size."
        ),
        "notes": "Uses boto3 HeadObject via report_validator.py --sample 100.",
    },
    {
        "id": "CLI-SMOKE-04",
        "group": "smoke",
        "tags": ["smoke", "p0", "config"],
        "title": "Transfer completes with PARALLEL_WORKERS=15",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"PARALLEL_WORKERS": 15}},
        "priority": "P0",
        "pass_criteria": "All rows SUCCESS; no worker deadlock; broker exits 0.",
        "notes": "15 is the production default (config.json TRANSFER.PARALLEL_WORKERS).",
    },
    {
        "id": "CLI-SMOKE-05",
        "group": "smoke",
        "tags": ["smoke", "p0", "logging"],
        "title": "Broker log captures per-batch timing",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"PERF_STATS": "True"}},
        "priority": "P0",
        "pass_criteria": (
            "cloudcp.log contains preprocess/upload/postprocess timing lines per batch."
        ),
        "notes": "",
    },

    # --------------------------------------------------------------- BOUNDARY
    {
        "id": "CLI-BOUND-01",
        "group": "boundary",
        "tags": ["boundary", "p0", "zero-byte"],
        "title": "Zero-byte file transfers as empty S3 object",
        "datasets": ["DS-P8-02"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "HeadObject returns Content-Length: 0; status SUCCESS; no error."
        ),
        "notes": "DS-P8-02: single 0-byte file with Unicode emoji name (FN-08).",
    },
    {
        "id": "CLI-BOUND-02",
        "group": "boundary",
        "tags": ["boundary", "p0", "tiny"],
        "title": "1-byte file (absolute minimum tiny) uses single-part PUT",
        "datasets": ["DS-P9-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "HeadObject 200; Content-Length: 1; single-part PUT (not multipart)."
        ),
        "notes": "DS-P9-01: one 1-byte file (FN-04 embedded newline).",
    },
    {
        "id": "CLI-BOUND-03",
        "group": "boundary",
        "tags": ["boundary", "p0", "multipart"],
        "title": "63 MB file uses single-part PUT (below 64 MB threshold)",
        "datasets": ["DS-P9-03"],
        "config_overrides": {"CLOUDCP": {"MULTIPART_THRESHOLD_MB": 64}},
        "priority": "P0",
        "pass_criteria": (
            "S3 access log shows PutObject, NOT CreateMultipartUpload."
        ),
        "notes": "DS-P9-03: one 63 MB file (FN-16 leading dash).",
    },
    {
        "id": "CLI-BOUND-04",
        "group": "boundary",
        "tags": ["boundary", "p0", "multipart"],
        "title": "64 MB file triggers multipart upload (first multipart)",
        "datasets": ["DS-P9-04"],
        "config_overrides": {"CLOUDCP": {"MULTIPART_THRESHOLD_MB": 64}},
        "priority": "P0",
        "pass_criteria": (
            "S3 access log shows CreateMultipartUpload; "
            "no incomplete multipart uploads after transfer."
        ),
        "notes": "DS-P9-04: one 64 MB file (FN-13 Windows-reserved chars).",
    },
    {
        "id": "CLI-BOUND-05",
        "group": "boundary",
        "tags": ["boundary", "p0", "multipart", "medium"],
        "title": "100 MB file (small→medium boundary) uses multipart",
        "datasets": ["DS-P9-05"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "Multipart confirmed; HeadObject Content-Length matches source.",
        "notes": "DS-P9-05: one 100 MB file (FN-18 zero-width Unicode).",
    },
    {
        "id": "CLI-BOUND-06",
        "group": "boundary",
        "tags": ["boundary", "p0", "multipart", "large"],
        "title": "1 GB file (medium→large boundary) completes cleanly",
        "datasets": ["DS-P9-06"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "Status SUCCESS; Content-Length matches; no orphaned multipart.",
        "notes": "DS-P9-06: one 1 GB file (FN-08 Unicode emoji).",
    },
    {
        "id": "CLI-BOUND-07",
        "group": "boundary",
        "tags": ["boundary", "p0", "multipart", "large"],
        "title": "100 GB single file (large-tier extreme) completes",
        "datasets": ["DS-P9-07"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "Transfer completes; Content-Length matches; "
            "zero incomplete multipart uploads."
        ),
        "notes": "DS-P9-07: one 100 GB file (FN-07 240-char name). Long-running.",
    },
    {
        "id": "CLI-BOUND-08",
        "group": "boundary",
        "tags": ["boundary", "p0", "multipart"],
        "title": "All 11 boundary sizes transfer correctly",
        "datasets": ["DS-P2-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "All 110 boundary files SUCCESS; "
            "files ≥64 MB use multipart; files <64 MB use single-part."
        ),
        "notes": (
            "DS-P2-01: 10 files at each of 11 exact boundaries "
            "(0B, 1B, 10KB, ~1MB, 1MB, 63MB, 64MB, 99MB, 100MB, 999MB, 1GB)."
        ),
    },

    # --------------------------------------------------------------- ENCODING
    {
        "id": "CLI-ENC-01",
        "group": "encoding",
        "tags": ["encoding", "p0", "unicode"],
        "title": "Unicode filenames (emoji, CJK, Cyrillic) round-trip",
        "datasets": ["DS-P4-01", "DS-P4-05"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "HeadObject key matches source filename byte-for-byte (UTF-8); "
            "status SUCCESS for all variant rows."
        ),
        "notes": "DS-P4-01: 20,000 tiny files, all 20 filename variants.",
    },
    {
        "id": "CLI-ENC-02",
        "group": "encoding",
        "tags": ["encoding", "p0", "special-chars"],
        "title": "ASCII special chars round-trip",
        "datasets": ["DS-P4-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "No mangling in S3 key; Content-Length correct.",
        "notes": "FN-11 (shell metacharacters), FN-12 (shell metacharacters).",
    },
    {
        "id": "CLI-ENC-03",
        "group": "encoding",
        "tags": ["encoding", "p0", "spaces"],
        "title": "Filename with embedded spaces round-trips",
        "datasets": ["DS-P4-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "S3 key contains literal space (URL-encoded in HTTP, raw in key); "
            "HeadObject 200."
        ),
        "notes": "FN-02: names with embedded spaces.",
    },
    {
        "id": "CLI-ENC-04",
        "group": "encoding",
        "tags": ["encoding", "p0", "long-name"],
        "title": "Long filename (~240 chars) produces correct key",
        "datasets": ["DS-P8-03"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "Key length ≤1024 bytes; HeadObject 200; no truncation."
        ),
        "notes": "DS-P8-03: single 100 GB file with 240-char name (FN-07).",
    },
    {
        "id": "CLI-ENC-05",
        "group": "encoding",
        "tags": ["encoding", "p0", "deep-tree"],
        "title": "Deep path (~14 levels) yields correct key",
        "datasets": ["DS-P8-04"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "Key = PREFIX + strip(FS_PREFIX, path); "
            "no path separator lost; HeadObject 200."
        ),
        "notes": "DS-P8-04: ~700 files at every level of a 14-deep tree.",
    },
    {
        "id": "CLI-ENC-06",
        "group": "encoding",
        "tags": ["encoding", "p0", "unicode", "cross-tier"],
        "title": "Cross-tier encoding (all 20 variants, all tiers)",
        "datasets": ["DS-P4-05"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "12,550 SUCCESS rows; sample of each variant at each tier passes HeadObject."
        ),
        "notes": "DS-P4-05: 12,550 files, all 20 variants × all tiers + 50 zero seeds.",
    },

    # -------------------------------------------------------------- CONFIG
    {
        "id": "CLI-CFG-01",
        "group": "config",
        "tags": ["config", "p0", "multipart", "chunk"],
        "title": "CHUNK_SIZE_MB=8 produces ≤8 MiB parts",
        "datasets": ["DS-P1-04"],
        "config_overrides": {
            "TRANSFER": {"CHUNK_SIZE_MB": 8},
            "CLOUDCP": {"MULTIPART_CHUNKSIZE_MB": 8},
        },
        "priority": "P0",
        "pass_criteria": (
            "S3 access log shows parts ≤8 MiB for medium files; "
            "transfer completes; all SUCCESS."
        ),
        "notes": "DS-P1-04: medium-tier pure (~5 TB); subset recommended for speed.",
    },
    {
        "id": "CLI-CFG-02",
        "group": "config",
        "tags": ["config", "p0", "multipart", "chunk"],
        "title": "CHUNK_SIZE_MB=128 produces ≤128 MiB parts",
        "datasets": ["DS-P1-04"],
        "config_overrides": {
            "TRANSFER": {"CHUNK_SIZE_MB": 128},
            "CLOUDCP": {"MULTIPART_CHUNKSIZE_MB": 128},
        },
        "priority": "P0",
        "pass_criteria": "Parts ≤128 MiB; transfer completes; same file count and sizes.",
        "notes": "",
    },
    {
        "id": "CLI-CFG-03",
        "group": "config",
        "tags": ["config", "p0", "parallelism"],
        "title": "PARALLEL_WORKERS=1 (serial transfer) completes correctly",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"PARALLEL_WORKERS": 1}},
        "priority": "P0",
        "pass_criteria": "Transfer completes; no concurrency errors; all SUCCESS.",
        "notes": "",
    },
    {
        "id": "CLI-CFG-04",
        "group": "config",
        "tags": ["config", "p0", "parallelism"],
        "title": "PARALLEL_WORKERS=32 (high concurrency) completes correctly",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"PARALLEL_WORKERS": 32}},
        "priority": "P0",
        "pass_criteria": "Transfer completes; log shows ≤32 concurrent workers; all SUCCESS.",
        "notes": "",
    },
    {
        "id": "CLI-CFG-05",
        "group": "config",
        "tags": ["config", "p0"],
        "title": "HI_PERF_OPT=False still produces correct results",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"HI_PERF_OPT": "False"}},
        "priority": "P0",
        "pass_criteria": "Transfer completes correctly; no crash or hang.",
        "notes": "",
    },
    {
        "id": "CLI-CFG-06",
        "group": "config",
        "tags": ["config", "p0", "logging"],
        "title": "PERF_STATS=False suppresses timing log lines",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"PERF_STATS": "False"}},
        "priority": "P0",
        "pass_criteria": (
            "cloudcp.log does NOT contain preprocess/upload/postprocess timing lines; "
            "transfer still succeeds."
        ),
        "notes": "",
    },
    {
        "id": "CLI-CFG-07",
        "group": "config",
        "tags": ["config", "p0", "threadpool"],
        "title": "TM_THREAD_POOL_SIZE=4 (cloudcp thread pool) completes correctly",
        "datasets": ["DS-P1-04"],
        "config_overrides": {"TRANSFER": {"TM_THREAD_POOL_SIZE": 4}},
        "priority": "P0",
        "pass_criteria": "Transfer completes; parts correctly assembled; no errors.",
        "notes": "",
    },
    {
        "id": "CLI-CFG-08",
        "group": "config",
        "tags": ["config", "p0", "endpoint"],
        "title": "LOCAL_AWS MinIO endpoint used for all S3 calls",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"CLOUD": {"LOCAL_AWS": "https://10.10.10.103:9000"}},
        "priority": "P0",
        "pass_criteria": (
            "All S3 API calls hit MinIO endpoint; "
            "no calls to public AWS S3; all SUCCESS."
        ),
        "notes": "Verify via network traffic or MinIO access log.",
    },
    {
        "id": "CLI-CFG-09",
        "group": "config",
        "tags": ["config", "p0", "network-profile"],
        "title": "Network profile dt2_100gbe allocates large-tier workers",
        "datasets": ["DS-P3-01"],
        "config_overrides": {"NETWORK_PROFILE": "dt2_100gbe"},
        "priority": "P0",
        "pass_criteria": (
            "Large-tier worker slots allocated proportionally; "
            "log confirms tier weights."
        ),
        "notes": "DS-P3-01: large exhausts first; validates work-stealing.",
    },
    {
        "id": "CLI-CFG-10",
        "group": "config",
        "tags": ["config", "p0", "concurrency"],
        "title": "MAX_CONCURRENT_TRANSFERS=3 caps active cloudcp processes",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"SERVICE": {"MAX_CONCURRENT_TRANSFERS": 3}},
        "priority": "P0",
        "pass_criteria": "Never >3 active cloudcp processes; transfer completes.",
        "notes": "",
    },

    # --------------------------------------------------------------- RERUN/SKIP
    {
        "id": "CLI-SKIP-01",
        "group": "rerun",
        "tags": ["rerun", "skip", "p0"],
        "title": "SKIP_EXISTING=true skips already-uploaded files on second run",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"CLOUDCP": {"SKIP_EXISTING": True}},
        "priority": "P0",
        "pass_criteria": (
            "Second run report shows ≥90% rows SKIPPED; no FAILED; all files on S3."
        ),
        "notes": "Requires a first successful run (CLI-SMOKE-01) beforehand.",
    },
    {
        "id": "CLI-SKIP-02",
        "group": "rerun",
        "tags": ["rerun", "skip", "p0"],
        "title": "SKIP_EXISTING=false re-uploads all files",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"CLOUDCP": {"SKIP_EXISTING": False}},
        "priority": "P0",
        "pass_criteria": "Report shows SUCCESS for all rows; no SKIPPED; sizes match.",
        "notes": "",
    },
    {
        "id": "CLI-SKIP-03",
        "group": "rerun",
        "tags": ["rerun", "resume", "p0"],
        "title": "Interrupt mid-transfer; resume completes remainder",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "First+second-run combined reports cover 100% of files "
            "with SUCCESS or SKIPPED."
        ),
        "notes": "Kill broker at ~50% progress (SIGTERM); then restart.",
    },
    {
        "id": "CLI-SKIP-04",
        "group": "rerun",
        "tags": ["rerun", "idempotency", "p0"],
        "title": "Re-run with identical source is idempotent",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"CLOUDCP": {"SKIP_EXISTING": True}},
        "priority": "P0",
        "pass_criteria": (
            "Report row count matches file count; no duplicates; "
            "all SKIPPED or SUCCESS."
        ),
        "notes": "",
    },
    {
        "id": "CLI-SKIP-05",
        "group": "rerun",
        "tags": ["rerun", "resume", "p0"],
        "title": "AZURE_RESUME=False forces clean restart",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"TRANSFER": {"AZURE_RESUME": "False"}},
        "priority": "P0",
        "pass_criteria": "Restart processes all files; SKIPPED count depends on SKIP_EXISTING.",
        "notes": "",
    },

    # ----------------------------------------------------------- REPORT
    {
        "id": "CLI-RPT-01",
        "group": "report",
        "tags": ["report", "p0"],
        "title": "Report CSV is well-formed (headers, encoding, row count)",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "UTF-8; headers present; row count = 91,320; no BOM issues.",
        "notes": "",
    },
    {
        "id": "CLI-RPT-02",
        "group": "report",
        "tags": ["report", "p0"],
        "title": "All status values are valid enum members",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "status column contains only: SUCCESS, SKIPPED, FAILED, MISMATCH, PARTIAL."
        ),
        "notes": "",
    },
    {
        "id": "CLI-RPT-03",
        "group": "report",
        "tags": ["report", "p0", "key-composition"],
        "title": "s3_key matches expected composition rule",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "Sample of 200 rows: s3_key == PREFIX + strip(FS_PREFIX, file_path)."
        ),
        "notes": "Key rule from bcloud_final_design.md §11.",
    },
    {
        "id": "CLI-RPT-04",
        "group": "report",
        "tags": ["report", "p0", "s3"],
        "title": "size in report matches HeadObject Content-Length",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "Random 50-row sample: size field == actual S3 object size.",
        "notes": "",
    },
    {
        "id": "CLI-RPT-05",
        "group": "report",
        "tags": ["report", "p0", "path"],
        "title": "Report written to documented path",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "CSV at "
            "/opt/bryck/bryckapi/downloads/cloud_transfer_logs/"
            "cloud_transfer_<id>/transfer_report_<id>.csv"
        ),
        "notes": "",
    },
    {
        "id": "CLI-RPT-06",
        "group": "report",
        "tags": ["report", "p0", "json"],
        "title": "JSON summary file is well-structured",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": (
            "transfer_summary_files.json present; "
            "contains transfer_id, status, total, success, failed fields."
        ),
        "notes": "VERIFICATION.TRANSFER_SUMMARY_FILES path from config.",
    },
    {
        "id": "CLI-RPT-07",
        "group": "report",
        "tags": ["report", "p0", "empty"],
        "title": "Zero-file source produces empty-but-valid report",
        "datasets": ["DS-P8-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "CSV has headers only; scan_state=complete; no batch files.",
        "notes": "DS-P8-01: empty source directory with 3 empty subdirs.",
    },

    # ----------------------------------------------------------------- MIXED
    {
        "id": "CLI-MIX-01",
        "group": "mixed",
        "tags": ["mixed", "p0", "regression"],
        "title": "Mixed 4 GB transfer (all tiers) completes correctly",
        "datasets": ["DS-P7-01"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "All rows SUCCESS; report well-formed; no FAILED.",
        "notes": (
            "Use a ~4 GB subset or a custom mixed dataset. "
            "See dataset_prep.py --suggest mixed --total-gb 4"
        ),
    },
    {
        "id": "CLI-MIX-02",
        "group": "mixed",
        "tags": ["mixed", "p0", "encoding"],
        "title": "Mixed dataset with all 20 filename variants",
        "datasets": ["DS-P4-05"],
        "config_overrides": {},
        "priority": "P0",
        "pass_criteria": "All 12,550 rows SUCCESS; encoding round-trip correct.",
        "notes": "",
    },
    {
        "id": "CLI-MIX-03",
        "group": "mixed",
        "tags": ["mixed", "p0", "network-profile"],
        "title": "Mixed dataset under low-bandwidth profile",
        "datasets": ["DS-P7-01"],
        "config_overrides": {"NETWORK_PROFILE": "wan_lowbw"},
        "priority": "P0",
        "pass_criteria": (
            "Transfer completes; batch hashes identical to dt2_100gbe run of same dataset."
        ),
        "notes": "",
    },

    # ------------------------------------------------------------------ PERF
    {
        "id": "CLI-PERF-01",
        "group": "perf",
        "tags": ["perf", "p2", "tiny"],
        "title": "Tiny-file throughput baseline (files/sec)",
        "datasets": ["DS-P1-02"],
        "config_overrides": {"TRANSFER": {"PARALLEL_WORKERS": 15}},
        "priority": "P2",
        "pass_criteria": "Files/sec and PUT/sec logged; baseline recorded.",
        "notes": "DS-P1-02: ~500 GB, 1M tiny files.",
    },
    {
        "id": "CLI-PERF-02",
        "group": "perf",
        "tags": ["perf", "p2", "large"],
        "title": "Large-file bandwidth saturation",
        "datasets": ["DS-P1-06"],
        "config_overrides": {"TRANSFER": {"PARALLEL_WORKERS": 15}},
        "priority": "P2",
        "pass_criteria": "Sustained MB/s recorded; target ≥80% of 100 GbE link.",
        "notes": "DS-P1-06: ~10 TB, 200 large files.",
    },
    {
        "id": "CLI-PERF-03",
        "group": "perf",
        "tags": ["perf", "p2", "mixed"],
        "title": "Mixed full-pipeline scale baseline",
        "datasets": ["DS-P7-03"],
        "config_overrides": {"TRANSFER": {"PARALLEL_WORKERS": 15}},
        "priority": "P2",
        "pass_criteria": "Wall time and per-tier completion order recorded.",
        "notes": "DS-P7-03: ~10 TB, 1.17M files.",
    },
]

# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

_CASE_BY_ID: dict[str, dict] = {c["id"]: c for c in CASES}
_CASES_BY_GROUP: dict[str, list[dict]] = {}
_CASES_BY_TAG: dict[str, list[dict]] = {}

for _case in CASES:
    _CASES_BY_GROUP.setdefault(_case["group"], []).append(_case)
    for _tag in _case["tags"]:
        _CASES_BY_TAG.setdefault(_tag, []).append(_case)


def get_case(case_id: str) -> dict:
    """Return the case dict for the given ID, or raise KeyError."""
    return _CASE_BY_ID[case_id]


def get_group(group_name: str) -> list[dict]:
    """Return all cases in the named group."""
    return list(_CASES_BY_GROUP.get(group_name, []))


def get_tag(tag: str) -> list[dict]:
    """Return all cases matching the given tag."""
    return list(_CASES_BY_TAG.get(tag, []))


def filter_cases(
    *,
    case_id: str | None = None,
    group: str | None = None,
    tag: str | None = None,
    priority: str | None = None,
    exclude_tags: list[str] | None = None,
) -> list[dict]:
    """Return a filtered, deduplicated list of cases.

    Filters are ANDed together. case_id, group, and tag are mutually exclusive
    selectors; priority is an additional AND filter.
    """
    if case_id:
        pool = [get_case(case_id)]
    elif group:
        pool = get_group(group)
    elif tag:
        pool = get_tag(tag)
    else:
        pool = list(CASES)

    if priority:
        pool = [c for c in pool if c["priority"] == priority]

    if exclude_tags:
        exclude_set = set(exclude_tags)
        pool = [c for c in pool if not set(c["tags"]) & exclude_set]

    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[dict] = []
    for c in pool:
        if c["id"] not in seen:
            seen.add(c["id"])
            result.append(c)
    return result


def list_cases(verbose: bool = False) -> None:
    """Print a formatted case listing to stdout."""
    header = f"{'ID':<18}  {'GROUP':<10}  {'PRI':<4}  {'TITLE'}"
    print(header)
    print("-" * len(header))
    for c in CASES:
        print(f"{c['id']:<18}  {c['group']:<10}  {c['priority']:<4}  {c['title']}")
        if verbose:
            print(f"  datasets : {', '.join(c['datasets'])}")
            print(f"  tags     : {', '.join(c['tags'])}")
            print(f"  overrides: {c['config_overrides'] or '(none)'}")
            print(f"  pass     : {c['pass_criteria']}")
            if c["notes"]:
                print(f"  notes    : {c['notes']}")
            print()


# ---------------------------------------------------------------------------
# Dataset-to-case reverse mapping (useful for report validation)
# ---------------------------------------------------------------------------

def datasets_for_case(case_id: str) -> list[str]:
    """Return the list of dataset IDs required by the given case."""
    return get_case(case_id)["datasets"]


def cases_for_dataset(dataset_id: str) -> list[dict]:
    """Return all cases that reference the given dataset ID."""
    return [c for c in CASES if dataset_id in c["datasets"]]
