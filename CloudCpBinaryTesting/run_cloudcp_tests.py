#!/usr/bin/env python3
"""End-to-end test orchestrator for the cloudcp binary.

Pipeline per dataset (spec file in data/specs/):

  1. datagen  --spec <spec>            -> materialize real files under the spec `root`
  2. make_batches.py <root> --single   -> one NUL-framed batch_000000.txt
  3. stage into transfer_<id>/batches/inprogress/zero/ (new incrementing id)
  4. cloudcp <batch> ...               -> upload every file to the object store
  5. validate transfer_report_<id>.csv -> all SUCCESS, row count + size match
  6. clear the bucket (aws s3 rm --recursive)   [without deleting the bucket]
  7. record a per-run report (logs + JSON + Markdown summary)

Selection is modular: drop a new `NN_name.yaml` spec into data/specs/ and it is
auto-discovered and orderable by its numeric prefix.

Examples
--------
    # one dataset by name or by spec number
    python run_cloudcp_tests.py --dataset tiny_2million
    python run_cloudcp_tests.py --dataset 3

    # an inclusive range of spec numbers
    python run_cloudcp_tests.py --from 1 --to 4

    # the negative / malformed-batch suite (B01-B12)
    python run_cloudcp_tests.py --negative

    # only the extended-attribute (xattr) case (N12-N16)
    python run_cloudcp_tests.py --xattr

    # everything: all positive datasets + the negative suite
    python run_cloudcp_tests.py --all

    # list what would run, or preview commands without touching anything
    python run_cloudcp_tests.py --list
    python run_cloudcp_tests.py --all --dry-run

This script is intended to run on the Linux host where datagen, cloudcp, aws and
the /bryck & /opt paths exist. --dry-run works anywhere (prints commands only).
"""

import argparse
import csv
import datetime as _dt
import json
import os
import posixpath as pp
import re
import shutil
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# Fixed configuration (server-side, hard-coded per the test environment).
# Anything a caller might reasonably override is exposed as a CLI flag below.
# ---------------------------------------------------------------------------

DEFAULT_DATAGEN_BIN = "/home/bryck/rperiyas/datagen"
CLOUDCP_BIN = "/opt/bryck/aws/bin/cloudcp"
CLOUDCP_LD_LIBRARY_PATH = "/opt/bryck/aws/lib/"

BCLOUD_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
TRANSFER_LOGS_DIR = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"

# The tier sub-directory under .../batches/inprogress/ is hard-coded to "zero".
INPROGRESS_TIER = "zero"

DEFAULT_BUCKET = "aditya"
DEFAULT_ENDPOINT = "https://10.10.10.103:9000"

# fs-prefix per dataset == the spec `root` itself (e.g. /bryck/cloudcp_test/<dataset>);
# --prefix == the dataset name. Both derived at runtime from each spec.

# ---------------------------------------------------------------------------
# Business-facing descriptions.
#
# These drive the human-readable report (report.md) so it can be shared with
# non-engineering stakeholders. Each entry explains, in plain language, what the
# dataset contains and *why* it matters for validating the cloudcp uploader.
# ---------------------------------------------------------------------------

DATASET_INFO = {
    "zero_byte": {
        "title": "Zero-byte files",
        "summary": "200 completely empty (0-byte) files.",
        "purpose": "Confirms the uploader correctly creates empty objects "
                   "(zero-length uploads) without error or hanging.",
    },
    "tiny_files": {
        "title": "Tiny files",
        "summary": "5,400 small files (each under 1 MiB) spread across a "
                   "3-level directory tree.",
        "purpose": "Exercises high file-count throughput and the large number "
                   "of single-part uploads a real small-file workload produces.",
    },
    "small_files": {
        "title": "Small files",
        "summary": "120 files sized between 1 MiB and 64 MiB.",
        "purpose": "Straddles the multipart upload cutoff, so both single-part "
                   "and multipart upload paths are exercised in one dataset.",
    },
    "medium_files": {
        "title": "Medium files",
        "summary": "Files sized between 64 MiB and 1 GiB.",
        "purpose": "Validates multi-chunk multipart uploads and the integrity "
                   "(checksum) of each uploaded part.",
    },
    "large_files": {
        "title": "Large files",
        "summary": "5 very large files of 1 GiB or more.",
        "purpose": "Stresses long-running large multipart transfers and the "
                   "resume/retry behaviour on big objects.",
    },
    "sparse_files": {
        "title": "Sparse files",
        "summary": "60 sparse (hole-punched) files spanning small to medium "
                   "logical sizes.",
        "purpose": "Validates correct handling of logical-vs-physical file size "
                   "and reads across empty regions (holes).",
    },
    "fill_files": {
        "title": "Deterministic content files",
        "summary": "Files filled with a fixed, repeatable byte pattern.",
        "purpose": "Produces stable, verifiable checksums so uploaded content "
                   "can be proven byte-for-byte identical to the source.",
    },
    "deep_tree": {
        "title": "Deep directory tree",
        "summary": "4,096 files at the leaves of a 12-level-deep folder tree.",
        "purpose": "Stresses construction of very long object keys from deeply "
                   "nested paths while keeping the file count exact.",
    },
    "unicode_names": {
        "title": "Unicode filenames",
        "summary": "Files whose names use emoji, CJK, Cyrillic and accented "
                   "characters, plus spaces.",
        "purpose": "Validates byte-exact round-trip of international filenames "
                   "from disk into object-store keys.",
    },
    "special_char_names": {
        "title": "Special-character filenames",
        "summary": "Files whose names contain ASCII special characters "
                   "(spaces, parentheses, ampersands, brackets, etc.).",
        "purpose": "Stresses shell-quoting and object-key escaping without "
                   "relying on Unicode.",
    },
    "mixed_realistic": {
        "title": "Mixed realistic workload",
        "summary": "A weighted mix of file types and sizes biased toward small "
                   "files, mimicking a real-world directory tree.",
        "purpose": "Provides an end-to-end sanity check against a workload that "
                   "resembles genuine customer data.",
    },
    "tiny_2million": {
        "title": "Scale test — two million tiny files",
        "summary": "Approximately 2,000,000 tiny files.",
        "purpose": "Large-batch enumeration and sustained-throughput stress "
                   "test at production scale.",
    },
}

# Malformed-batch (Scenario A) descriptions, keyed by the batch file stem.
NEGATIVE_BATCH_INFO = {
    "bad_batch_empty": "An empty batch file (0 bytes) with no records at all.",
    "bad_batch_no_terminator": "The final record is missing its trailing "
                               "terminator byte.",
    "bad_batch_double_nul": "Two consecutive terminators, creating an empty "
                            "record in the middle of the batch.",
    "bad_batch_leading_nul": "A leading terminator, creating an empty first "
                             "record before any real path.",
    "bad_batch_only_nuls": "A file containing nothing but terminator bytes.",
    "bad_batch_dangling_paths": "A well-formed batch in which every listed file "
                                "does not exist on disk.",
    "bad_batch_directory_entry": "A folder path supplied where a file path is "
                                 "expected.",
    "bad_batch_crlf_paths": "Paths containing embedded carriage-return / "
                            "line-feed characters.",
    "bad_batch_nonutf8": "A path containing invalid (non-UTF-8) bytes.",
    "bad_batch_very_long_path": "A single path far exceeding the operating "
                                "system path-length limit.",
    "bad_batch_whitespace_only": "A record consisting only of whitespace.",
    "bad_batch_mixed_valid_invalid": "Alternating valid and non-existent paths, "
                                     "testing the partial-success contract.",
    "batch_xattr": "Valid files carrying hostile / edge extended attributes "
                   "(valid, oversized >64 KiB, binary, many, bad-checksum); "
                   "tests the xattr-to-object-metadata policy, not batch framing.",
}

# Human summary of the two negative scenarios for the report narrative.
NEGATIVE_SCENARIO_INFO = {
    "negative_A_no_data": {
        "title": "Scenario A — Malformed upload batches",
        "summary": "Twelve deliberately broken batch files are fed to the "
                   "uploader with no matching data present.",
        "purpose": "Proves the uploader rejects corrupt input safely: it must "
                   "not hang and must not crash. A clean error is the expected, "
                   "correct outcome.",
    },
    "negative_B_with_data": {
        "title": "Scenario B — Corrupted batch over real data",
        "summary": "Six batches built from a real dataset but with corrupted "
                   "framing injected at known points.",
        "purpose": "Proves that every valid file listed before the corruption "
                   "still uploads successfully, and the uploader handles the "
                   "corruption gracefully instead of failing the whole job.",
    },
    "negative_C_xattr": {
        "title": "Scenario C — Extended-attribute metadata",
        "summary": "Valid files carrying hostile / edge extended attributes "
                   "(valid, oversized, binary, many, bad-checksum) are uploaded "
                   "via batch_xattr.txt.",
        "purpose": "Proves the uploader handles user.* xattr metadata safely: it "
                   "must not hang or crash reading the attributes. The "
                   "preserve-vs-drop policy and byte-exact round-trip are then "
                   "confirmed against the stored object metadata.",
    },
}


# ---------------------------------------------------------------------------
# Pause / resume test cases (PR01 – PR06)
# ---------------------------------------------------------------------------

# Path that cloudcp appends [Batch]...done lines to.
CLOUDCP_LOG_PATH = pp.join(TRANSFER_LOGS_DIR, "cloudcp.log")

PAUSE_RESUME_CASE_INFO = {
    "PR01": {
        "title": "PR01 — Basic pause/resume (tiny, 5 s kill)",
        "dataset": "tiny_files",
        "kill_after_sec": 5,
        "cycles": 1,
        "summary": "Kill cloudcp after ~5 s (~32 % progress) on the tiny-files dataset.",
        "purpose": "Confirms the basic resume path: cloudcp reads cloudcp.log, "
                   "skips already-uploaded files, and completes the remainder.",
    },
    "PR02": {
        "title": "PR02 — Multipart-cutoff resume (small, 8 s kill)",
        "dataset": "small_files",
        "kill_after_sec": 8,
        "cycles": 1,
        "summary": "Kill cloudcp after ~8 s on the small-files dataset which "
                   "straddles the 8 MiB multipart cutoff.",
        "purpose": "Verifies that resume handles both single-part and multipart "
                   "partially-uploaded objects correctly.",
    },
    "PR03": {
        "title": "PR03 — Immediate kill (tiny, 2 s kill)",
        "dataset": "tiny_files",
        "kill_after_sec": 2,
        "cycles": 1,
        "summary": "Kill cloudcp after only ~2 s (~13 % progress) on tiny_files.",
        "purpose": "Proves that a resume from very early in the transfer still "
                   "completes the full dataset.",
    },
    "PR04": {
        "title": "PR04 — Late kill (tiny, 12 s kill)",
        "dataset": "tiny_files",
        "kill_after_sec": 12,
        "cycles": 1,
        "summary": "Kill cloudcp after ~12 s (~75 % progress) on tiny_files.",
        "purpose": "Verifies that only the tail of the batch is uploaded on "
                   "resume and the overall report is still complete.",
    },
    "PR05": {
        "title": "PR05 — Double kill/resume (tiny, 2 cycles)",
        "dataset": "tiny_files",
        "kill_after_sec": 5,
        "cycles": 2,
        "summary": "Kill and resume twice; cloudcp.log is checked after each kill.",
        "purpose": "Confirms that cloudcp.log accumulates state across multiple "
                   "kill-resume cycles and never re-uploads already-committed files.",
    },
    "PR06": {
        "title": "PR06 — Unicode filenames resume",
        "dataset": "unicode_names",
        "kill_after_sec": 5,
        "cycles": 1,
        "summary": "Kill cloudcp after ~5 s on the unicode-filenames dataset.",
        "purpose": "Ensures non-ASCII paths are recorded and re-read from "
                   "cloudcp.log byte-exactly on resume.",
    },
}


def _count_ok_in_cloudcp_log(tid):
    """Sum ok=N values from [Batch]...done lines for transfer *tid* in cloudcp.log."""
    if not os.path.isfile(CLOUDCP_LOG_PATH):
        return 0
    marker = "cloud_transfer_{}/".format(tid)
    total = 0
    try:
        with open(CLOUDCP_LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if marker not in line:
                    continue
                m = re.search(r"\[Batch\].*\bdone\b.*\bok=(\d+)", line)
                if m:
                    total += int(m.group(1))
    except OSError:
        pass
    return total


def dataset_info(name):
    """Return the business description for *name* (with a safe fallback)."""
    return DATASET_INFO.get(name, {
        "title": name,
        "summary": "Custom dataset.",
        "purpose": "Validates cloudcp upload behaviour for this dataset.",
    })

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SPECS_DIR = os.path.join(HERE, "data", "specs")
MAKE_BATCHES = os.path.join(HERE, "make_batches.py")
RUNS_ROOT = os.path.join(HERE, "runs")

# Dedicated spec for Scenario B ("data present + corrupted batch"). Kept outside
# data/specs/ so it is never picked up as a positive dataset.
NEG_BASE_SPEC = os.path.join(HERE, "data", "negative", "neg_base.yaml")

# Detect hangs on the (intentionally hostile) negative batches.
NEGATIVE_TIMEOUT_SEC = 600


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

class Logger:
    """Tee log lines to stdout and a run.log file."""

    def __init__(self, log_path=None):
        self._fh = open(log_path, "a", encoding="utf-8") if log_path else None

    def log(self, msg, level="INFO"):
        ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = "{} [{}] {}".format(ts, level, msg)
        print(line, flush=True)
        if self._fh:
            self._fh.write(line + "\n")
            self._fh.flush()

    def close(self):
        if self._fh:
            self._fh.close()
            self._fh = None


def _coerce_scalar(val):
    """Best-effort scalar coercion: int, bool, else stripped string."""
    v = val.strip().strip("'\"")
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(v)
    except ValueError:
        return v


def load_spec(spec_path):
    """Parse the subset of a datagen spec we need (dependency-free).

    Handles top-level scalars plus one level of nested mappings (enough for the
    `tree` / `flat` blocks we compute an expected file count from). Lists and
    deeper nesting are ignored. Avoids requiring PyYAML on the host.
    """
    data = {}
    # stack of (indent, container) so nested mappings attach to their parent
    stack = [(-1, data)]
    with open(spec_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip(" "))
            content = line.strip()
            if content.startswith("- "):        # ignore list items
                continue
            m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$", content)
            if not m:
                continue
            key, val = m.group(1), m.group(2)
            # strip inline comment on unquoted scalars
            if val and val.strip()[:1] not in ("'", '"'):
                val = val.split(" #", 1)[0]
            val = val.strip()
            while stack and indent <= stack[-1][0]:
                stack.pop()
            parent = stack[-1][1]
            if val == "":                        # opens a nested mapping
                child = {}
                parent[key] = child
                stack.append((indent, child))
            else:
                parent[key] = _coerce_scalar(val)
    return data


def expected_file_count(spec):
    """Compute how many files datagen *should* create from the spec geometry.

    Returns None for modes where the count isn't derivable from the spec
    (`list` / `csv-list`, which depend on external input files).
    """
    mode = spec.get("mode")
    if mode == "flat":
        flat = spec.get("flat") or {}
        return int(flat.get("num_files", 1))
    if mode == "tree":
        tree = spec.get("tree") or {}
        fanout = int(tree.get("fanout", 1))
        depth = int(tree.get("depth", 1))
        fpd = int(tree.get("files_per_dir", 1))
        each = tree.get("files_in_each_dir", False)
        if each:
            # files at every node (root + all intermediate + leaves)
            if fanout == 1:
                dirs = depth + 1
            else:
                dirs = (fanout ** (depth + 1) - 1) // (fanout - 1)
            return dirs * fpd
        # files only at leaf directories
        return (fanout ** depth) * fpd
    return None


class Dataset:
    def __init__(self, number, spec_path, name, root, mode, expected_count):
        self.number = number
        self.spec_path = spec_path
        self.name = name
        self.root = root
        self.mode = mode
        self.expected_count = expected_count

    def __repr__(self):
        return "Dataset(#{} {} root={})".format(self.number, self.name, self.root)


def discover_datasets(specs_dir):
    """Return datasets discovered from *specs_dir*, ordered by numeric prefix."""
    if not os.path.isdir(specs_dir):
        raise SystemExit("specs dir not found: {}".format(specs_dir))
    datasets = []
    for fname in sorted(os.listdir(specs_dir)):
        if not fname.lower().endswith((".yaml", ".yml")):
            continue
        spec_path = os.path.join(specs_dir, fname)
        m = re.match(r"^(\d+)", fname)
        number = int(m.group(1)) if m else 0
        spec = load_spec(spec_path)
        root = spec.get("root")
        mode = spec.get("mode")
        if not root:
            print("WARNING: spec has no top-level root, skipping: {}".format(spec_path),
                  file=sys.stderr)
            continue
        name = pp.basename(str(root).rstrip("/"))
        datasets.append(Dataset(number, spec_path, name, root, mode,
                                expected_file_count(spec)))
    datasets.sort(key=lambda d: (d.number, d.name))
    return datasets


def select_datasets(all_datasets, args):
    """Apply --dataset / --from / --to selection."""
    if args.dataset:
        sel = args.dataset
        for d in all_datasets:
            if d.name == sel or str(d.number) == str(sel):
                return [d]
        raise SystemExit("no dataset matches --dataset {!r}. Use --list.".format(sel))

    if args.from_ is not None or args.to is not None:
        lo = args.from_ if args.from_ is not None else -(10 ** 9)
        hi = args.to if args.to is not None else (10 ** 9)
        return [d for d in all_datasets if lo <= d.number <= hi]

    return list(all_datasets)


def next_transfer_id(dry_run):
    """Allocate the next transfer id (max existing transfer_<N> + 1)."""
    max_id = 0
    if os.path.isdir(BCLOUD_BATCHMETA):
        for entry in os.listdir(BCLOUD_BATCHMETA):
            m = re.match(r"^transfer_(\d+)$", entry)
            if m:
                max_id = max(max_id, int(m.group(1)))
    return max_id + 1


def run_cmd(cmd, logger, env=None, cwd=None, timeout=None, dry_run=False,
            stdout_path=None, stderr_path=None):
    """Run *cmd* (list), streaming a summary to the logger.

    Returns dict: rc, timed_out, signaled, stdout, stderr.
    """
    printable = " ".join(cmd)
    logger.log("$ " + printable)
    if dry_run:
        return {"rc": 0, "timed_out": False, "signaled": False,
                "stdout": "", "stderr": "", "dry_run": True}

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    try:
        proc = subprocess.run(
            cmd, env=run_env, cwd=cwd, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode("utf-8", "replace")
        err = (e.stderr or b"").decode("utf-8", "replace")
        logger.log("command TIMED OUT after {}s".format(timeout), "ERROR")
        _dump(stdout_path, out)
        _dump(stderr_path, err)
        return {"rc": None, "timed_out": True, "signaled": False,
                "stdout": out, "stderr": err}
    except FileNotFoundError as e:
        logger.log("command not found: {}".format(e), "ERROR")
        return {"rc": 127, "timed_out": False, "signaled": False,
                "stdout": "", "stderr": str(e)}

    out = proc.stdout.decode("utf-8", "replace")
    err = proc.stderr.decode("utf-8", "replace")
    _dump(stdout_path, out)
    _dump(stderr_path, err)
    signaled = proc.returncode is not None and proc.returncode < 0
    level = "INFO" if proc.returncode == 0 else "WARN"
    logger.log("exit code: {}{}".format(
        proc.returncode, " (killed by signal {})".format(-proc.returncode) if signaled else ""),
        level)
    if err.strip():
        tail = "\n".join(err.strip().splitlines()[-10:])
        logger.log("stderr tail:\n" + tail, level)
    return {"rc": proc.returncode, "timed_out": False, "signaled": signaled,
            "stdout": out, "stderr": err}


def _dump(path, text):
    if path and text is not None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)


def count_batch_records(batch_path):
    """Count NUL-terminated records in a batch file."""
    with open(batch_path, "rb") as f:
        return f.read().count(b"\0")


def transfer_report_path(tid):
    """Locate the transfer report CSV for transfer *tid*.

    Newer cloudcp writes the report into a per-transfer subdirectory
    (cloud_transfer_<id>/transfer_report_<id>.csv); older builds wrote it flat
    directly under TRANSFER_LOGS_DIR. Prefer the nested layout, fall back to the
    flat one, and default to the nested path (for logging) if neither exists.
    """
    fname = "transfer_report_{}.csv".format(tid)
    nested = pp.join(TRANSFER_LOGS_DIR, "cloud_transfer_{}".format(tid), fname)
    flat = pp.join(TRANSFER_LOGS_DIR, fname)
    if os.path.isfile(nested):
        return nested
    if os.path.isfile(flat):
        return flat
    return nested


# ---------------------------------------------------------------------------
# Positive dataset run
# ---------------------------------------------------------------------------

def run_positive_dataset(ds, args, run_ctx, logger):
    info = dataset_info(ds.name)
    result = {
        "kind": "positive",
        "dataset": ds.name,
        "spec": ds.spec_path,
        "number": ds.number,
        "root": ds.root,
        "mode": ds.mode,
        "title": info["title"],
        "summary": info["summary"],
        "purpose": info["purpose"],
        "status": "PENDING",
        "steps": {},
        "errors": [],
        "transfer_id": None,
        "counts": {},
    }
    ds_dir = os.path.join(run_ctx["dir"], "datasets", ds.name)
    os.makedirs(ds_dir, exist_ok=True)

    logger.log("=" * 70)
    logger.log("DATASET #{}: {}  (spec={})".format(ds.number, ds.name, ds.spec_path))
    result["counts"]["expected"] = ds.expected_count
    if ds.expected_count is not None:
        logger.log("spec expects {} file(s)".format(ds.expected_count))

    # --- Step 0: validate spec present ------------------------------------
    if not os.path.isfile(ds.spec_path):
        result["status"] = "FAIL"
        result["errors"].append("spec file missing: {}".format(ds.spec_path))
        logger.log("spec file missing, aborting dataset", "ERROR")
        return result

    # --- Step 1: datagen ---------------------------------------------------
    r = run_cmd([args.datagen_bin, "--spec", ds.spec_path], logger,
                dry_run=args.dry_run,
                stdout_path=os.path.join(ds_dir, "datagen.stdout"),
                stderr_path=os.path.join(ds_dir, "datagen.stderr"))
    result["steps"]["datagen"] = {"rc": r["rc"], "timed_out": r["timed_out"]}
    if not args.dry_run and r["rc"] != 0:
        result["status"] = "FAIL"
        result["errors"].append("datagen failed (rc={})".format(r["rc"]))
        return result

    # --- Step 2: make_batches (single) ------------------------------------
    batches_out = os.path.join(ds_dir, "batches")
    r = run_cmd([sys.executable, MAKE_BATCHES, ds.root, "-o", batches_out, "--single"],
                logger, dry_run=args.dry_run,
                stdout_path=os.path.join(ds_dir, "make_batches.stdout"),
                stderr_path=os.path.join(ds_dir, "make_batches.stderr"))
    result["steps"]["make_batches"] = {"rc": r["rc"]}
    if not args.dry_run and r["rc"] != 0:
        result["status"] = "FAIL"
        result["errors"].append("make_batches failed (rc={})".format(r["rc"]))
        return result

    local_batch = os.path.join(batches_out, "batch_000000.txt")
    record_count = None
    if not args.dry_run:
        if not os.path.isfile(local_batch):
            result["status"] = "FAIL"
            result["errors"].append("batch file not produced: {}".format(local_batch))
            return result
        record_count = count_batch_records(local_batch)
        result["counts"]["batch_records"] = record_count
        logger.log("batch records (files datagen created): {}".format(record_count))
        # datagen sanity: files actually created must match the spec geometry
        if ds.expected_count is not None and record_count != ds.expected_count:
            result["status"] = "FAIL"
            msg = ("datagen produced {} file(s) but spec expects {} "
                   "(diff {:+d})").format(record_count, ds.expected_count,
                                          record_count - ds.expected_count)
            result["errors"].append(msg)
            logger.log(msg, "ERROR")
            return result

    # --- Step 3: allocate transfer id + stage batch -----------------------
    tid = next_transfer_id(args.dry_run)
    result["transfer_id"] = tid
    staged_dir = pp.join(BCLOUD_BATCHMETA, "transfer_{}".format(tid),
                         "batches", "inprogress", INPROGRESS_TIER)
    staged_batch = pp.join(staged_dir, "batch_000000.txt")
    logger.log("transfer id: {}  ->  {}".format(tid, staged_batch))
    if not args.dry_run:
        os.makedirs(staged_dir, exist_ok=True)
        shutil.copy2(local_batch, staged_batch)

    # --- Step 4: cloudcp ---------------------------------------------------
    fs_prefix = ds.root                     # /bryck/cloudcp_test/<dataset>
    key_prefix = ds.name                    # <dataset>
    cloudcp_cmd = [
        CLOUDCP_BIN, staged_batch,
        "--bucket", args.bucket,
        "--fs-prefix", fs_prefix,
        "--transfer-id", str(tid),
        "--prefix", key_prefix,
        "--endpoint-url", args.endpoint_url,
    ]
    r = run_cmd(cloudcp_cmd, logger,
                env={"LD_LIBRARY_PATH": CLOUDCP_LD_LIBRARY_PATH},
                dry_run=args.dry_run,
                stdout_path=os.path.join(ds_dir, "cloudcp.stdout"),
                stderr_path=os.path.join(ds_dir, "cloudcp.stderr"))
    result["steps"]["cloudcp"] = {"rc": r["rc"], "signaled": r["signaled"],
                                  "timed_out": r["timed_out"]}

    # --- Step 5: validate the transfer report -----------------------------
    csv_path = transfer_report_path(tid)
    result["report_csv"] = csv_path
    if args.dry_run:
        result["status"] = "DRY_RUN"
    else:
        # keep a copy of the report alongside the run artifacts
        if os.path.isfile(csv_path):
            try:
                shutil.copy2(csv_path, os.path.join(ds_dir, os.path.basename(csv_path)))
            except OSError:
                pass
        ok, val = validate_positive_csv(csv_path, record_count, logger)
        result["counts"].update(val)
        if ok:
            result["status"] = "PASS"
        else:
            result["status"] = "FAIL"
            result["errors"].extend(val.get("problems", []))

    # --- Step 6: clear the bucket -----------------------------------------
    if args.no_clear:
        logger.log("--skip-delete set: leaving bucket contents in place")
    else:
        clear_bucket(args, logger)

    # --- Step 7: remove the materialized dataset from the mount -----------
    if args.no_clear:
        logger.log("--skip-delete set: keeping local dataset {} on the mount".format(ds.root))
    else:
        delete_dataset_dir(ds.root, args, logger)

    logger.log("dataset {} -> {}".format(ds.name, result["status"]),
               "INFO" if result["status"] in ("PASS", "DRY_RUN") else "ERROR")
    return result


def validate_positive_csv(csv_path, expected_records, logger):
    """Validate a positive transfer report.

    PASS when: CSV exists, every row status==SUCCESS, row count == expected
    record count (when known), and each row's `size` matches the source file.
    """
    val = {"problems": []}
    if not os.path.isfile(csv_path):
        val["problems"].append("transfer report not found: {}".format(csv_path))
        logger.log("transfer report not found: {}".format(csv_path), "ERROR")
        return False, val

    total = 0
    non_success = 0
    size_mismatch = 0
    missing_local = 0
    examples = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            status = (row.get("status") or "").strip().upper()
            if status != "SUCCESS":
                non_success += 1
                if len(examples) < 5:
                    examples.append("{}={}".format(row.get("local_path"), status))
                continue
            # size verification against the source file on disk
            local_path = row.get("local_path")
            size_str = (row.get("size") or "").strip()
            try:
                reported = int(size_str)
            except (TypeError, ValueError):
                reported = None
            if local_path and os.path.isfile(local_path):
                actual = os.path.getsize(local_path)
                if reported is not None and reported != actual:
                    size_mismatch += 1
                    if len(examples) < 5:
                        examples.append("size {} reported={} actual={}".format(
                            local_path, reported, actual))
            elif local_path:
                missing_local += 1

    val["rows"] = total
    val["non_success"] = non_success
    val["size_mismatch"] = size_mismatch
    val["missing_local_files"] = missing_local
    logger.log("report rows={} non_success={} size_mismatch={} missing_local={}".format(
        total, non_success, size_mismatch, missing_local))

    ok = True
    if total == 0:
        ok = False
        val["problems"].append("transfer report has 0 rows")
    if non_success:
        ok = False
        val["problems"].append("{} row(s) not SUCCESS (e.g. {})".format(
            non_success, "; ".join(examples[:5])))
    if size_mismatch:
        ok = False
        val["problems"].append("{} size mismatch(es) (e.g. {})".format(
            size_mismatch, "; ".join(examples[:5])))
    if expected_records is not None and total != expected_records:
        ok = False
        val["problems"].append("row count {} != expected {} files".format(
            total, expected_records))
    return ok, val


# ---------------------------------------------------------------------------
# Negative suite: Scenario A (no data present, malformed batches B01-B12)
#                 Scenario B (data present, corrupted batch C01-C06)
# ---------------------------------------------------------------------------

def run_negative_suite(args, run_ctx, logger):
    """Run every negative scenario and return their combined result records."""
    results = []
    results.extend(run_negative_scenario_a(args, run_ctx, logger))
    results.extend(run_negative_scenario_b(args, run_ctx, logger))
    results.extend(run_negative_scenario_c(args, run_ctx, logger))
    return results


def run_negative_scenario_a(args, run_ctx, logger):
    logger.log("=" * 70)
    logger.log("NEGATIVE SCENARIO A: no data present, malformed batches (B01-B12)")

    neg_root = os.path.join(run_ctx["dir"], "negative")
    os.makedirs(neg_root, exist_ok=True)

    r = run_cmd([sys.executable, MAKE_BATCHES, "--negative", "-o", neg_root],
                logger, dry_run=args.dry_run,
                stdout_path=os.path.join(neg_root, "make_batches_negative.stdout"),
                stderr_path=os.path.join(neg_root, "make_batches_negative.stderr"))
    if not args.dry_run and r["rc"] != 0:
        return [{
            "kind": "negative", "dataset": "negative_A_no_data", "status": "FAIL",
            "errors": ["make_batches --negative failed (rc={})".format(r["rc"])],
            "cases": [],
        }]

    neg_data = os.path.join(neg_root, "negative_data")
    neg_batches = os.path.join(neg_root, "negative_batches")

    if args.dry_run:
        logger.log("(dry-run) would run cloudcp against each bad_batch_*.txt in "
                   + neg_batches)
        return [{"kind": "negative", "dataset": "negative_A_no_data",
                 "status": "DRY_RUN", "errors": [], "cases": []}]

    batch_files = sorted(
        os.path.join(neg_batches, n) for n in os.listdir(neg_batches)
        if n.startswith("bad_batch_") and n.endswith(".txt")
    ) if os.path.isdir(neg_batches) else []

    if not batch_files:
        return [{"kind": "negative", "dataset": "negative_A_no_data", "status": "FAIL",
                 "errors": ["no malformed batch files found in " + neg_batches],
                 "cases": []}]

    cases = []
    for bf in batch_files:
        cases.append(run_negative_case(bf, neg_data, args, run_ctx, logger))

    passed = sum(1 for c in cases if c["status"] == "PASS")
    overall = "PASS" if passed == len(cases) else "FAIL"
    scn = NEGATIVE_SCENARIO_INFO["negative_A_no_data"]
    return [{
        "kind": "negative", "dataset": "negative_A_no_data",
        "title": scn["title"], "summary": scn["summary"],
        "purpose": scn["purpose"],
        "status": overall,
        "errors": [],
        "counts": {"cases": len(cases), "passed": passed,
                   "failed": len(cases) - passed},
        "cases": cases,
    }]


def run_negative_case(batch_file, neg_data, args, run_ctx, logger):
    case_id = os.path.splitext(os.path.basename(batch_file))[0]
    logger.log("-" * 60)
    logger.log("NEGATIVE CASE: {}".format(case_id))
    case_dir = os.path.join(run_ctx["dir"], "negative", "cases", case_id)
    os.makedirs(case_dir, exist_ok=True)

    tid = next_transfer_id(args.dry_run)
    staged_dir = pp.join(BCLOUD_BATCHMETA, "transfer_{}".format(tid),
                         "batches", "inprogress", INPROGRESS_TIER)
    staged_batch = pp.join(staged_dir, os.path.basename(batch_file))
    os.makedirs(staged_dir, exist_ok=True)
    shutil.copy2(batch_file, staged_batch)

    cmd = [
        CLOUDCP_BIN, staged_batch,
        "--bucket", args.bucket,
        "--fs-prefix", neg_data,
        "--transfer-id", str(tid),
        "--prefix", "negative",
        "--endpoint-url", args.endpoint_url,
    ]
    r = run_cmd(cmd, logger, env={"LD_LIBRARY_PATH": CLOUDCP_LD_LIBRARY_PATH},
                timeout=NEGATIVE_TIMEOUT_SEC,
                stdout_path=os.path.join(case_dir, "cloudcp.stdout"),
                stderr_path=os.path.join(case_dir, "cloudcp.stderr"))

    # Pass criteria for a hostile batch: cloudcp must not hang (timeout) and must
    # not be killed by a signal (segfault/abort). A clean non-zero exit that
    # reports the bad records is an acceptable, expected outcome.
    problems = []
    if r["timed_out"]:
        problems.append("cloudcp hung (timeout {}s)".format(NEGATIVE_TIMEOUT_SEC))
    if r["signaled"]:
        problems.append("cloudcp killed by signal {}".format(-r["rc"]))

    status = "PASS" if not problems else "FAIL"
    logger.log("case {} -> {} (exit={})".format(case_id, status, r["rc"]),
               "INFO" if status == "PASS" else "ERROR")

    # negative cases shouldn't populate the bucket, but clear defensively
    if not args.no_clear:
        clear_bucket(args, logger)

    return {
        "case": case_id,
        "transfer_id": tid,
        "rc": r["rc"],
        "timed_out": r["timed_out"],
        "signaled": r["signaled"],
        "status": status,
        "errors": problems,
        "description": NEGATIVE_BATCH_INFO.get(case_id, "Malformed batch input."),
    }


# ---------------------------------------------------------------------------
# Scenario C: extended-attribute (xattr) metadata (N12-N16)
# ---------------------------------------------------------------------------

def run_negative_scenario_c(args, run_ctx, logger):
    logger.log("=" * 70)
    logger.log("NEGATIVE SCENARIO C: extended-attribute metadata (N12-N16)")

    c_root = os.path.join(run_ctx["dir"], "negative_xattr")
    os.makedirs(c_root, exist_ok=True)

    r = run_cmd([sys.executable, MAKE_BATCHES, "--negative", "-o", c_root],
                logger, dry_run=args.dry_run,
                stdout_path=os.path.join(c_root, "make_batches_negative.stdout"),
                stderr_path=os.path.join(c_root, "make_batches_negative.stderr"))
    if not args.dry_run and r["rc"] != 0:
        return [{"kind": "negative", "dataset": "negative_C_xattr", "status": "FAIL",
                 "errors": ["make_batches --negative failed (rc={})".format(r["rc"])],
                 "cases": []}]

    neg_data = os.path.join(c_root, "negative_data")
    xattr_batch = os.path.join(c_root, "negative_batches", "batch_xattr.txt")
    scn = NEGATIVE_SCENARIO_INFO["negative_C_xattr"]

    if args.dry_run:
        logger.log("(dry-run) would run cloudcp against " + xattr_batch)
        return [{"kind": "negative", "dataset": "negative_C_xattr",
                 "status": "DRY_RUN", "errors": [], "cases": []}]

    # xattr is Linux-only on an xattr-capable fs; make_batches skips it elsewhere.
    if not os.path.isfile(xattr_batch):
        logger.log("batch_xattr.txt not produced; xattr unsupported on this fs/OS", "ERROR")
        return [{"kind": "negative", "dataset": "negative_C_xattr",
                 "title": scn["title"], "summary": scn["summary"], "purpose": scn["purpose"],
                 "status": "SKIP",
                 "errors": ["batch_xattr.txt not produced (no xattr-capable fs / non-Linux)"],
                 "counts": {"cases": 0, "passed": 0, "failed": 0}, "cases": []}]

    cases = [run_negative_case(xattr_batch, neg_data, args, run_ctx, logger)]
    passed = sum(1 for c in cases if c["status"] == "PASS")
    overall = "PASS" if passed == len(cases) else "FAIL"
    return [{
        "kind": "negative", "dataset": "negative_C_xattr",
        "title": scn["title"], "summary": scn["summary"], "purpose": scn["purpose"],
        "status": overall, "errors": [],
        "counts": {"cases": len(cases), "passed": passed,
                   "failed": len(cases) - passed},
        "cases": cases,
    }]


# ---------------------------------------------------------------------------
# Scenario B: data present + corrupted batch (C01-C06)
# ---------------------------------------------------------------------------

def run_negative_scenario_b(args, run_ctx, logger):
    logger.log("=" * 70)
    logger.log("NEGATIVE SCENARIO B: data present, corrupted batch (C01-C06)")

    if not os.path.isfile(NEG_BASE_SPEC):
        msg = "neg-base spec not found: {}".format(NEG_BASE_SPEC)
        logger.log(msg, "ERROR")
        return [{"kind": "negative", "dataset": "negative_B_with_data",
                 "status": "FAIL", "errors": [msg], "cases": []}]

    spec = load_spec(NEG_BASE_SPEC)
    base_root = spec.get("root")
    base_name = pp.basename(str(base_root).rstrip("/")) if base_root else "neg_base"
    b_dir = os.path.join(run_ctx["dir"], "negative_with_data")
    os.makedirs(b_dir, exist_ok=True)

    # Step 1: materialize the real base dataset on the mount.
    r = run_cmd([args.datagen_bin, "--spec", NEG_BASE_SPEC], logger,
                dry_run=args.dry_run,
                stdout_path=os.path.join(b_dir, "datagen.stdout"),
                stderr_path=os.path.join(b_dir, "datagen.stderr"))
    if not args.dry_run and r["rc"] != 0:
        return [{"kind": "negative", "dataset": "negative_B_with_data",
                 "status": "FAIL",
                 "errors": ["datagen (neg-base) failed (rc={})".format(r["rc"])],
                 "cases": []}]

    # Step 2: build corrupted batch variants + manifest from the real files.
    r = run_cmd([sys.executable, MAKE_BATCHES, "--corrupt-from", base_root,
                 "-o", b_dir], logger, dry_run=args.dry_run,
                stdout_path=os.path.join(b_dir, "make_batches_corrupt.stdout"),
                stderr_path=os.path.join(b_dir, "make_batches_corrupt.stderr"))
    if not args.dry_run and r["rc"] != 0:
        return [{"kind": "negative", "dataset": "negative_B_with_data",
                 "status": "FAIL",
                 "errors": ["make_batches --corrupt-from failed (rc={})".format(r["rc"])],
                 "cases": []}]

    if args.dry_run:
        logger.log("(dry-run) would run cloudcp against each corrupt batch "
                   "described in {}/corrupt_manifest.json".format(b_dir))
        return [{"kind": "negative", "dataset": "negative_B_with_data",
                 "status": "DRY_RUN", "errors": [], "cases": []}]

    manifest_path = os.path.join(b_dir, "corrupt_manifest.json")
    if not os.path.isfile(manifest_path):
        _cleanup_base_dataset(base_root, args, logger)
        return [{"kind": "negative", "dataset": "negative_B_with_data",
                 "status": "FAIL",
                 "errors": ["corrupt manifest not produced: " + manifest_path],
                 "cases": []}]

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    fs_prefix = manifest.get("fs_prefix") or base_root
    cases = []
    try:
        for case in manifest.get("cases", []):
            cases.append(run_corrupt_case(case, b_dir, fs_prefix, base_name,
                                          args, run_ctx, logger))
    finally:
        # remove the materialized base dataset once all cases are done
        _cleanup_base_dataset(base_root, args, logger)

    passed = sum(1 for c in cases if c["status"] == "PASS")
    overall = "PASS" if passed == len(cases) else "FAIL"
    scn = NEGATIVE_SCENARIO_INFO["negative_B_with_data"]
    return [{
        "kind": "negative", "dataset": "negative_B_with_data",
        "title": scn["title"], "summary": scn["summary"],
        "purpose": scn["purpose"],
        "status": overall, "errors": [],
        "counts": {"cases": len(cases), "passed": passed,
                   "failed": len(cases) - passed},
        "cases": cases,
    }]


def _cleanup_base_dataset(base_root, args, logger):
    if args.no_clear:
        logger.log("--skip-delete set: keeping neg-base dataset {} on the mount".format(
            base_root))
    else:
        delete_dataset_dir(base_root, args, logger)


def run_corrupt_case(case, b_dir, fs_prefix, key_prefix, args, run_ctx, logger):
    case_id = case["id"]
    logger.log("-" * 60)
    logger.log("SCENARIO B CASE: {} ({})".format(case_id, case.get("corruption", "")))
    case_dir = os.path.join(run_ctx["dir"], "negative_with_data", "cases", case_id)
    os.makedirs(case_dir, exist_ok=True)

    local_batch = os.path.join(b_dir, case["batch"])
    tid = next_transfer_id(args.dry_run)
    staged_dir = pp.join(BCLOUD_BATCHMETA, "transfer_{}".format(tid),
                         "batches", "inprogress", INPROGRESS_TIER)
    staged_batch = pp.join(staged_dir, os.path.basename(local_batch))
    os.makedirs(staged_dir, exist_ok=True)
    shutil.copy2(local_batch, staged_batch)

    cmd = [
        CLOUDCP_BIN, staged_batch,
        "--bucket", args.bucket,
        "--fs-prefix", fs_prefix,
        "--transfer-id", str(tid),
        "--prefix", key_prefix,
        "--endpoint-url", args.endpoint_url,
    ]
    r = run_cmd(cmd, logger, env={"LD_LIBRARY_PATH": CLOUDCP_LD_LIBRARY_PATH},
                timeout=NEGATIVE_TIMEOUT_SEC,
                stdout_path=os.path.join(case_dir, "cloudcp.stdout"),
                stderr_path=os.path.join(case_dir, "cloudcp.stderr"))

    problems = []
    if r["timed_out"]:
        problems.append("cloudcp hung (timeout {}s)".format(NEGATIVE_TIMEOUT_SEC))
    if r["signaled"]:
        problems.append("cloudcp killed by signal {}".format(-r["rc"]))

    # Validate: every real record that precedes the corruption (the manifest's
    # expected-success prefix) must be SUCCESS in the transfer report. Records
    # at/after the corruption may fail -- that is expected, not a test failure.
    csv_path = transfer_report_path(tid)
    if os.path.isfile(csv_path):
        try:
            shutil.copy2(csv_path, os.path.join(case_dir, os.path.basename(csv_path)))
        except OSError:
            pass
    success_paths = _read_success_paths(csv_path)
    expected = case.get("expected_success", [])
    missing = [p for p in expected if p not in success_paths]

    if expected and missing:
        problems.append("{}/{} expected-success record(s) not SUCCESS (e.g. {})".format(
            len(missing), len(expected), "; ".join(missing[:3])))

    status = "PASS" if not problems else "FAIL"
    logger.log("case {} -> {} (exit={}, expected_success={}, got_success={})".format(
        case_id, status, r["rc"], len(expected), len(success_paths)),
        "INFO" if status == "PASS" else "ERROR")

    if not args.no_clear:
        clear_bucket(args, logger)

    return {
        "case": case_id,
        "transfer_id": tid,
        "rc": r["rc"],
        "timed_out": r["timed_out"],
        "signaled": r["signaled"],
        "expected_success": len(expected),
        "got_success": len(success_paths),
        "status": status,
        "errors": problems,
        "description": case.get("corruption", "Corrupted batch framing."),
        "note": case.get("note", ""),
    }


def _read_success_paths(csv_path):
    """Return the set of local_path values with status==SUCCESS in a report."""
    paths = set()
    if not os.path.isfile(csv_path):
        return paths
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip().upper() == "SUCCESS":
                lp = row.get("local_path")
                if lp is not None:
                    paths.add(lp)
    return paths


# ---------------------------------------------------------------------------
# Pause / resume suite
# ---------------------------------------------------------------------------

def run_pause_resume_suite(args, run_ctx, logger):
    """Run every PR case and return their result records."""
    results = []
    for case_id, case_info in PAUSE_RESUME_CASE_INFO.items():
        results.append(run_pause_resume_case(case_id, case_info, args, run_ctx, logger))
    return results


def run_pause_resume_case(case_id, case_info, args, run_ctx, logger):
    """Run one pause/resume cycle test.

    Pipeline:
      1. datagen + make_batches (single batch) + stage under inprogress/
      2. Launch cloudcp via Popen; SIGKILL after kill_after_sec (× cycles)
      3. Snapshot cloudcp.log ok-count as pre-resume baseline
      4. Final cloudcp run to completion (same transfer-id)
      5. Validate transfer report
    """
    dataset_name = case_info["dataset"]
    kill_after = (args.pr_kill_after
                  if args.pr_kill_after is not None
                  else case_info["kill_after_sec"])
    cycles = case_info["cycles"]

    all_datasets = discover_datasets(args.specs_dir)
    ds = next((d for d in all_datasets if d.name == dataset_name), None)
    if ds is None:
        return {
            "kind": "pause_resume",
            "case": case_id,
            "dataset": dataset_name,
            "status": "FAIL",
            "errors": ["dataset '{}' not found in specs".format(dataset_name)],
            "counts": {}, "pr_baseline_count": 0, "pr_final_count": 0,
        }

    result = {
        "kind": "pause_resume",
        "case": case_id,
        "dataset": dataset_name,
        "title": case_info["title"],
        "summary": case_info["summary"],
        "purpose": case_info["purpose"],
        "status": "PENDING",
        "errors": [],
        "counts": {},
        "kill_after_sec": kill_after,
        "cycles": cycles,
        "pr_baseline_count": 0,
        "pr_final_count": 0,
    }

    case_dir = os.path.join(run_ctx["dir"], "pause_resume", case_id)
    os.makedirs(case_dir, exist_ok=True)

    logger.log("=" * 70)
    logger.log("PAUSE/RESUME {}: {}  kill_after={}s  cycles={}".format(
        case_id, case_info["title"], kill_after, cycles))

    # Step 1 — datagen
    r = run_cmd([args.datagen_bin, "--spec", ds.spec_path], logger,
                dry_run=args.dry_run,
                stdout_path=os.path.join(case_dir, "datagen.stdout"),
                stderr_path=os.path.join(case_dir, "datagen.stderr"))
    if not args.dry_run and r["rc"] != 0:
        result["status"] = "FAIL"
        result["errors"].append("datagen failed (rc={})".format(r["rc"]))
        return result

    # Step 2 — make_batches (single)
    batches_out = os.path.join(case_dir, "batches")
    r = run_cmd([sys.executable, MAKE_BATCHES, ds.root, "-o", batches_out, "--single"],
                logger, dry_run=args.dry_run,
                stdout_path=os.path.join(case_dir, "make_batches.stdout"),
                stderr_path=os.path.join(case_dir, "make_batches.stderr"))
    if not args.dry_run and r["rc"] != 0:
        result["status"] = "FAIL"
        result["errors"].append("make_batches failed (rc={})".format(r["rc"]))
        return result

    local_batch = os.path.join(batches_out, "batch_000000.txt")
    record_count = None
    if not args.dry_run:
        if not os.path.isfile(local_batch):
            result["status"] = "FAIL"
            result["errors"].append("batch not produced: {}".format(local_batch))
            return result
        record_count = count_batch_records(local_batch)
        result["counts"]["batch_records"] = record_count
        logger.log("batch records: {}".format(record_count))

    # Step 3 — stage under inprogress/ (same directory cloudcp expects)
    tid = next_transfer_id(args.dry_run)
    result["transfer_id"] = tid
    staged_dir = pp.join(BCLOUD_BATCHMETA, "transfer_{}".format(tid),
                         "batches", "inprogress", INPROGRESS_TIER)
    staged_batch = pp.join(staged_dir, "batch_000000.txt")
    logger.log("transfer id: {}  ->  {}".format(tid, staged_batch))
    if not args.dry_run:
        os.makedirs(staged_dir, exist_ok=True)
        shutil.copy2(local_batch, staged_batch)

    cloudcp_cmd = [
        CLOUDCP_BIN, staged_batch,
        "--bucket", args.bucket,
        "--fs-prefix", ds.root,
        "--transfer-id", str(tid),
        "--prefix", ds.name,
        "--endpoint-url", args.endpoint_url,
    ]
    run_env = os.environ.copy()
    run_env["LD_LIBRARY_PATH"] = CLOUDCP_LD_LIBRARY_PATH

    if args.dry_run:
        logger.log("(dry-run) would launch cloudcp, kill after {}s ({} cycle(s)), "
                   "then resume and validate".format(kill_after, cycles))
        result["status"] = "DRY_RUN"
        if not args.no_clear:
            clear_bucket(args, logger)
        delete_dataset_dir(ds.root, args, logger)
        return result

    try:
        # Kill-resume cycles
        for cycle in range(cycles):
            logger.log("-" * 60)
            logger.log("KILL CYCLE {}/{}  kill_after={}s".format(
                cycle + 1, cycles, kill_after))

            before_ok = _count_ok_in_cloudcp_log(tid)
            logger.log("cloudcp.log ok before launch: {}".format(before_ok))

            stdout_f = open(os.path.join(case_dir,
                                         "cloudcp_kill{}.stdout".format(cycle + 1)), "w")
            stderr_f = open(os.path.join(case_dir,
                                         "cloudcp_kill{}.stderr".format(cycle + 1)), "w")
            proc = subprocess.Popen(cloudcp_cmd, env=run_env,
                                    stdout=stdout_f, stderr=stderr_f)
            logger.log("cloudcp pid={} started; sleeping {}s".format(proc.pid, kill_after))

            time.sleep(kill_after)

            if proc.poll() is None:
                logger.log("SIGKILL -> pid={}".format(proc.pid))
                proc.kill()
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    logger.log("process did not die within 15 s after SIGKILL", "WARN")
            else:
                logger.log("process already exited (rc={}) before kill — "
                           "try a shorter --pr-kill-after".format(proc.returncode), "WARN")

            stdout_f.close()
            stderr_f.close()
            after_ok = _count_ok_in_cloudcp_log(tid)
            logger.log("cloudcp.log ok after kill: {}  (delta: {})".format(
                after_ok, after_ok - before_ok))

        result["pr_baseline_count"] = _count_ok_in_cloudcp_log(tid)
        logger.log("pre-resume baseline (total ok from log): {}".format(
            result["pr_baseline_count"]))

        # Final resume run to completion (same transfer-id, same batch path)
        logger.log("-" * 60)
        logger.log("FINAL RESUME RUN  transfer_id={}".format(tid))
        with open(os.path.join(case_dir, "cloudcp_resume.stdout"), "w") as so, \
             open(os.path.join(case_dir, "cloudcp_resume.stderr"), "w") as se:
            try:
                proc_final = subprocess.run(cloudcp_cmd, env=run_env,
                                            stdout=so, stderr=se)
                resume_rc = proc_final.returncode
            except FileNotFoundError as exc:
                result["status"] = "FAIL"
                result["errors"].append("cloudcp binary not found: {}".format(exc))
                return result

        logger.log("resume exit code: {}".format(resume_rc))
        result["pr_final_count"] = _count_ok_in_cloudcp_log(tid)
        logger.log("post-resume ok-count from log: {}".format(result["pr_final_count"]))

        # Validate final transfer report
        csv_path = transfer_report_path(tid)
        result["report_csv"] = csv_path
        if os.path.isfile(csv_path):
            try:
                shutil.copy2(csv_path, os.path.join(case_dir, os.path.basename(csv_path)))
            except OSError:
                pass

        ok, val = validate_positive_csv(csv_path, record_count, logger)
        result["counts"].update(val)

        problems = list(val.get("problems", []))
        if result["pr_baseline_count"] == 0 and cycles > 0 and record_count:
            problems.append(
                "pr_baseline_count=0: no completed files found in cloudcp.log "
                "before resume — process may have been killed before any batch finished")

        if ok and not problems:
            result["status"] = "PASS"
        else:
            result["status"] = "FAIL"
            result["errors"].extend(problems)

    except Exception as exc:
        result["status"] = "FAIL"
        result["errors"].append("unexpected exception: {}".format(exc))
    finally:
        if not args.no_clear:
            clear_bucket(args, logger)
        delete_dataset_dir(ds.root, args, logger)

    logger.log("case {} -> {}".format(case_id, result["status"]),
               "INFO" if result["status"] in ("PASS", "DRY_RUN") else "ERROR")
    return result


# ---------------------------------------------------------------------------
# Bucket clearing
# ---------------------------------------------------------------------------

def clear_bucket(args, logger):
    cmd = ["aws", "s3", "rm", "s3://{}".format(args.bucket), "--recursive",
           "--endpoint-url", args.endpoint_url]
    logger.log("clearing bucket s3://{} (objects only, bucket preserved)".format(args.bucket))
    r = run_cmd(cmd, logger, dry_run=args.dry_run)
    if not args.dry_run and r["rc"] not in (0, None):
        logger.log("bucket clear returned rc={} (continuing)".format(r["rc"]), "WARN")


def delete_dataset_dir(root, args, logger):
    """Remove the materialized dataset directory from the mount.

    Guarded so it only ever deletes an absolute path under the expected
    /bryck/cloudcp_test root -- never something shorter or unexpected.
    """
    root = str(root).rstrip("/")
    logger.log("deleting local dataset from mount: {}".format(root))
    if args.dry_run:
        logger.log("$ rm -rf {}".format(root))
        return
    expected_base = "/bryck/cloudcp_test"
    if not (root.startswith(expected_base + "/") and len(root) > len(expected_base) + 1):
        logger.log("refusing to delete unexpected path (not under {}): {}".format(
            expected_base, root), "ERROR")
        return
    if not os.path.isdir(root):
        logger.log("local dataset dir not present, nothing to delete: {}".format(root))
        return
    try:
        shutil.rmtree(root)
        logger.log("removed {}".format(root))
    except OSError as e:
        logger.log("failed to remove {}: {} (continuing)".format(root, e), "WARN")


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def write_reports(run_ctx, args, results, logger):
    report = {
        "run_id": run_ctx["run_id"],
        "started_at": run_ctx["started_at"],
        "finished_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "dry_run": args.dry_run,
        "bucket": args.bucket,
        "endpoint_url": args.endpoint_url,
        "datagen_bin": args.datagen_bin,
        "results": results,
    }

    json_path = os.path.join(run_ctx["dir"], "report.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    md_path = os.path.join(run_ctx["dir"], "report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(render_markdown(report))

    logger.log("report.json -> {}".format(json_path))
    logger.log("report.md   -> {}".format(md_path))
    return json_path, md_path


def render_markdown(report):
    results = report["results"]
    positives = [r for r in results if r["kind"] == "positive"]
    negatives = [r for r in results if r["kind"] == "negative"]
    pause_resumes = [r for r in results if r["kind"] == "pause_resume"]

    total = len(results)
    passed = sum(1 for r in results if r["status"] in ("PASS", "DRY_RUN", "SKIP"))
    failed = total - passed
    overall = "PASSED" if failed == 0 else "ATTENTION REQUIRED"
    is_dry = report.get("dry_run")

    def status_badge(s):
        return {
            "PASS": "PASS",
            "DRY_RUN": "PREVIEW",
            "FAIL": "FAIL",
        }.get(s, s)

    L = []
    L.append("# CloudCP Upload — Test Report")
    L.append("")
    L.append("_Automated end-to-end validation of the CloudCP file-upload "
             "utility against the object store._")
    L.append("")
    L.append("| | |")
    L.append("|---|---|")
    L.append("| **Overall result** | **{}** |".format(overall))
    L.append("| Run identifier | `{}` |".format(report["run_id"]))
    L.append("| Started | {} |".format(report["started_at"]))
    L.append("| Finished | {} |".format(report["finished_at"]))
    L.append("| Mode | {} |".format(
        "Preview only (no data transferred)" if is_dry else "Live run"))
    L.append("| Target bucket | `{}` |".format(report["bucket"]))
    L.append("| Object-store endpoint | `{}` |".format(report["endpoint_url"]))
    L.append("")

    # -- Executive summary --------------------------------------------------
    L.append("## Executive summary")
    L.append("")
    L.append("CloudCP is the utility that copies files from the on-premises "
             "storage mount into the S3-compatible object store. This report "
             "documents a full automated test run of that utility.")
    L.append("")
    L.append("For every dataset below the test harness performs the same "
             "end-to-end pipeline:")
    L.append("")
    L.append("1. **Generate** a representative set of real files on the storage mount.")
    L.append("2. **Package** those files into an upload batch.")
    L.append("3. **Upload** them with CloudCP to the live object store.")
    L.append("4. **Verify** the resulting transfer report — every file must be "
             "reported as successfully uploaded and its recorded size must match "
             "the file on disk.")
    L.append("5. **Clean up** the bucket and the generated data.")
    L.append("")
    L.append("In addition, a **resilience suite** feeds the uploader "
             "deliberately broken and corrupted input to confirm it fails "
             "safely — without hanging or crashing — and still uploads every "
             "valid file it can.")
    L.append("")
    L.append("**Result: {} of {} test items passed ({} failed).**".format(
        passed, total, failed))
    if is_dry:
        L.append("")
        L.append("> Note: this was a **preview (dry) run** — commands were "
                 "planned but no data was actually generated, uploaded, or "
                 "deleted.")
    L.append("")

    # -- At-a-glance table --------------------------------------------------
    L.append("## Results at a glance")
    L.append("")
    L.append("| # | Test item | What it covers | Result | Files verified |")
    L.append("|---|-----------|----------------|--------|----------------|")
    for r in positives:
        c = r.get("counts", {})
        verified = c.get("rows", "—")
        if is_dry:
            verified = "—"
        L.append("| {} | {} | {} | {} | {} |".format(
            r.get("number", "—"), r.get("title", r["dataset"]),
            r.get("summary", ""), status_badge(r["status"]), verified))
    for r in negatives:
        c = r.get("counts", {})
        cases = c.get("cases", "—")
        L.append("| — | {} | {} | {} | {} |".format(
            r.get("title", r["dataset"]), r.get("summary", ""),
            status_badge(r["status"]),
            "{} cases".format(cases) if cases != "—" else "—"))
    for r in pause_resumes:
        L.append("| — | {} | {} | {} | — |".format(
            r.get("title", r["case"]), r.get("summary", ""),
            status_badge(r["status"])))
    L.append("")

    # -- Positive datasets in detail ---------------------------------------
    if positives:
        L.append("## Datasets validated")
        L.append("")
        L.append("Each dataset targets a specific real-world characteristic of "
                 "customer data. All must upload with a 100% success rate.")
        L.append("")
        for r in positives:
            c = r.get("counts", {})
            L.append("### {}. {}  —  {}".format(
                r.get("number", "—"), r.get("title", r["dataset"]),
                status_badge(r["status"])))
            L.append("")
            L.append("- **What it is:** {}".format(r.get("summary", "")))
            L.append("- **Why it matters:** {}".format(r.get("purpose", "")))
            if not is_dry:
                created = c.get("batch_records")
                rows = c.get("rows")
                expected = c.get("expected")
                if expected is not None:
                    L.append("- **Files expected:** {}".format(expected))
                if created is not None:
                    L.append("- **Files generated & uploaded:** {}".format(created))
                if rows is not None:
                    L.append("- **Rows in transfer report verified:** {}".format(rows))
                if c.get("size_mismatch") is not None:
                    L.append("- **Size mismatches:** {}".format(
                        c.get("size_mismatch", 0)))
            if r.get("errors"):
                L.append("- **Issues found:**")
                for e in r["errors"]:
                    L.append("    - {}".format(e))
            L.append("")

    # -- Negative / resilience suite ---------------------------------------
    if negatives:
        L.append("## Resilience testing")
        L.append("")
        L.append("These scenarios verify that CloudCP behaves safely when given "
                 "bad input. A **pass** means the uploader neither hung nor "
                 "crashed — reporting a clean error is the correct, expected "
                 "behaviour.")
        L.append("")
        for r in negatives:
            c = r.get("counts", {})
            L.append("### {}  —  {}".format(
                r.get("title", r["dataset"]), status_badge(r["status"])))
            L.append("")
            if r.get("summary"):
                L.append("- **What it does:** {}".format(r["summary"]))
            if r.get("purpose"):
                L.append("- **Why it matters:** {}".format(r["purpose"]))
            if c:
                L.append("- **Cases:** {} total, {} passed, {} failed".format(
                    c.get("cases", "—"), c.get("passed", "—"),
                    c.get("failed", "—")))
            for e in r.get("errors", []):
                L.append("- {}".format(e))
            L.append("")
            cases = r.get("cases", [])
            if cases:
                has_success = any("expected_success" in cs for cs in cases)
                if has_success:
                    L.append("| Case | Scenario | Files that must upload "
                             "(uploaded / expected) | Result |")
                    L.append("|------|----------|--------------------------------------|--------|")
                    for cs in cases:
                        succ = "{} / {}".format(cs.get("got_success", "—"),
                                                cs.get("expected_success", "—"))
                        L.append("| {} | {} | {} | {} |".format(
                            cs["case"], cs.get("description", ""), succ,
                            status_badge(cs["status"])))
                else:
                    L.append("| Case | Scenario | Result |")
                    L.append("|------|----------|--------|")
                    for cs in cases:
                        L.append("| {} | {} | {} |".format(
                            cs["case"], cs.get("description", ""),
                            status_badge(cs["status"])))
                L.append("")

    # -- Pause / resume suite ----------------------------------------------
    if pause_resumes:
        L.append("## Pause / resume testing")
        L.append("")
        L.append("These cases verify that `cloudcp` correctly resumes an "
                 "interrupted transfer using `cloudcp.log` as the source of "
                 "truth for already-uploaded files.")
        L.append("")
        L.append("| Case | Dataset | Kill after | Cycles | "
                 "Pre-resume ok | Post-resume ok | Expected | Result |")
        L.append("|------|---------|-----------|--------|"
                 "---------------|----------------|----------|--------|")
        for r in pause_resumes:
            c = r.get("counts", {})
            expected = c.get("batch_records", "—")
            pre = r.get("pr_baseline_count", "—")
            post = r.get("pr_final_count", "—")
            if is_dry:
                pre = post = expected = "—"
            L.append("| {} | {} | {}s | {} | {} | {} | {} | {} |".format(
                r["case"], r["dataset"],
                r.get("kill_after_sec", "—"), r.get("cycles", "—"),
                pre, post, expected, status_badge(r["status"])))
        L.append("")
        for r in pause_resumes:
            if r.get("errors"):
                L.append("**{} issues:**".format(r["case"]))
                for e in r["errors"]:
                    L.append("- {}".format(e))
                L.append("")

    # -- Failures -----------------------------------------------------------
    fails = [r for r in results if r["status"] == "FAIL"]
    if fails:
        L.append("## Items needing attention")
        L.append("")
        for r in fails:
            L.append("### {} ({})".format(
                r.get("title", r["dataset"]), r["kind"]))
            for e in r.get("errors", []):
                L.append("- {}".format(e))
            for case in r.get("cases", []):
                if case["status"] != "PASS":
                    L.append("- Case `{}`: {}".format(
                        case["case"],
                        "; ".join(case.get("errors", [])) or "failed"))
            L.append("")
    else:
        L.append("## Items needing attention")
        L.append("")
        L.append("None — all test items completed successfully.")
        L.append("")

    L.append("---")
    L.append("")
    L.append("_Generated automatically by run_cloudcp_tests.py. "
             "Detailed logs and per-file transfer reports are preserved under "
             "the run directory `{}`._".format(report["run_id"]))

    return "\n".join(L) + "\n"



# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="End-to-end cloudcp binary test orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sel = p.add_argument_group("selection")
    sel.add_argument("--dataset", help="Run one dataset by name or spec number.")
    sel.add_argument("--from", dest="from_", type=int,
                     help="Start of an inclusive spec-number range.")
    sel.add_argument("--to", type=int, help="End of an inclusive spec-number range.")
    sel.add_argument("--negative", action="store_true",
                     help="Run the negative / malformed-batch suite (B01-B12).")
    sel.add_argument("--xattr", action="store_true",
                     help="Run only the extended-attribute (xattr) case (N12-N16).")
    sel.add_argument("--all", action="store_true",
                     help="Run every positive dataset, the negative suite, AND "
                          "the pause/resume suite.")
    sel.add_argument("--pause-resume", action="store_true",
                     help="Run the pause/resume test suite (PR01-PR06).")
    sel.add_argument("--list", action="store_true",
                     help="List discovered datasets and exit.")

    cfg = p.add_argument_group("configuration")
    cfg.add_argument("--specs-dir", default=DEFAULT_SPECS_DIR,
                     help="Directory of datagen spec files (default: data/specs).")
    cfg.add_argument("--datagen-bin", default=DEFAULT_DATAGEN_BIN,
                     help="Path to the datagen binary.")
    cfg.add_argument("--bucket", default=DEFAULT_BUCKET, help="Target bucket.")
    cfg.add_argument("--endpoint-url", default=DEFAULT_ENDPOINT,
                     help="Object-store endpoint URL.")

    beh = p.add_argument_group("behaviour")
    beh.add_argument("--dry-run", action="store_true",
                     help="Print commands only; no datagen/cloudcp/aws side effects.")
    beh.add_argument("--skip-delete", "--no-clear", dest="no_clear",
                     action="store_true",
                     help="After a dataset, do NOT clear the MinIO bucket and do "
                          "NOT delete the local dataset from the mount. "
                          "Alias: --no-clear.")
    beh.add_argument("--yes", action="store_true",
                     help="Skip the confirmation prompt for real (non-dry) runs.")
    beh.add_argument("--pr-kill-after", type=int, default=None, metavar="SEC",
                     help="Override kill-after seconds for ALL pause/resume cases.")
    return p


def confirm_or_abort(datasets, args):
    if args.dry_run or args.yes or args.list:
        return
    names = ", ".join(d.name for d in datasets) or "(none)"
    extra = " + negative suite" if (args.negative or args.all) else ""
    if args.xattr and not extra:
        extra = " + xattr case"
    print("About to run REAL datagen + cloudcp + bucket clears for:")
    print("  {}{}".format(names, extra))
    print("  bucket=s3://{}  endpoint={}".format(args.bucket, args.endpoint_url))
    ans = input("Proceed? [y/N] ").strip().lower()
    if ans not in ("y", "yes"):
        raise SystemExit("aborted by user (use --yes to skip this prompt).")


def main(argv=None):
    args = build_parser().parse_args(argv)

    all_datasets = discover_datasets(args.specs_dir)

    if args.list:
        print("Discovered datasets in {}:".format(args.specs_dir))
        for d in all_datasets:
            exp = d.expected_count if d.expected_count is not None else "?"
            print("  #{:<3} {:<22} mode={:<8} expected={:<9} root={}".format(
                d.number, d.name, d.mode or "?", exp, d.root))
        print("\nNegative suite:      --negative (malformed batches B01-B12)")
        print("Xattr case:          --xattr (extended-attribute metadata N12-N16)")
        print("Pause/resume suite:  --pause-resume (PR01-PR06)")
        for cid, ci in PAUSE_RESUME_CASE_INFO.items():
            print("  {} dataset={} kill_after={}s cycles={}".format(
                cid, ci["dataset"], ci["kill_after_sec"], ci["cycles"]))
        return 0

    # Decide what runs.
    run_negative = args.negative or args.all or args.xattr
    run_pr = args.pause_resume or args.all
    negative_only = (args.negative or args.xattr) and not (
        args.all or args.dataset or args.from_ is not None or args.to is not None)
    pr_only = args.pause_resume and not (
        args.all or args.dataset or args.from_ is not None or args.to is not None
        or args.negative or args.xattr)
    if negative_only or pr_only:
        positive = []
    else:
        positive = select_datasets(all_datasets, args)

    if not positive and not run_negative and not run_pr:
        raise SystemExit("nothing selected. Use --list to see datasets.")

    if os.name != "posix" and not args.dry_run:
        print("WARNING: this host is not POSIX; real runs expect the Linux test "
              "box. Use --dry-run to preview safely.", file=sys.stderr)

    confirm_or_abort(positive, args)

    # --- set up the run workspace -----------------------------------------
    run_id = _dt.datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = os.path.join(RUNS_ROOT, run_id)
    os.makedirs(run_dir, exist_ok=True)
    logger = Logger(os.path.join(run_dir, "run.log"))
    run_ctx = {
        "run_id": run_id,
        "dir": run_dir,
        "started_at": _dt.datetime.now().isoformat(timespec="seconds"),
    }

    logger.log("run id: {}".format(run_id))
    logger.log("run dir: {}".format(run_dir))
    logger.log("mode: {}".format("DRY-RUN" if args.dry_run else "LIVE"))
    logger.log("positive datasets: {}".format(
        ", ".join(d.name for d in positive) or "(none)"))
    logger.log("negative suite: {}".format("yes" if run_negative else "no"))
    logger.log("pause/resume suite: {}".format("yes" if run_pr else "no"))

    results = []
    t0 = time.time()
    try:
        for ds in positive:
            results.append(run_positive_dataset(ds, args, run_ctx, logger))
        if args.xattr and not (args.negative or args.all):
            results.extend(run_negative_scenario_c(args, run_ctx, logger))
        elif run_negative:
            results.extend(run_negative_suite(args, run_ctx, logger))
        if run_pr:
            results.extend(run_pause_resume_suite(args, run_ctx, logger))
    finally:
        write_reports(run_ctx, args, results, logger)

    elapsed = time.time() - t0
    passed = sum(1 for r in results if r["status"] in ("PASS", "DRY_RUN", "SKIP"))
    failed = len(results) - passed
    logger.log("=" * 70)
    logger.log("DONE in {:.1f}s: {} passed, {} failed, {} total".format(
        elapsed, passed, failed, len(results)),
        "INFO" if failed == 0 else "ERROR")
    logger.close()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
