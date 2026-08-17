#!/usr/bin/env python3
"""Generate one dataset, submit a bryckcloud CLI transfer, and validate results.

This runner is intended for the Linux Bryck host where these paths exist:

- /opt/bryck/.venv/bryck/bin/bryckcloud
- /home/bryck/rperiyas/datagen
- /opt/bryck/bryckapi/downloads/bcloud_batchmeta
- /opt/bryck/bryckapi/downloads/cloud_transfer_logs

Workflow:
1. Load one dataset from dataset_cloudcp/spec_files/manifest.json.
2. Rewrite each spec's root under a caller-provided output base.
3. Run datagen for every spec in that dataset.
4. Validate local file counts against the manifest.
5. Submit bryckcloud transfer add aws --src <dataset_root> --dst <s3://...>.
6. Detect the transfer id, wait for transfer artifacts, and validate reports.

Typical usage:

    python3 cloudcpclitesting.py --list
    python3 cloudcpclitesting.py \
      --dataset DS-P1-03 \
      --output-base /bryck/cloudcp_cli_data \
      --dst s3://aditya/cloudcp-cli/DS-P1-03 \
      --yes
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import pathlib
import re
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


HERE = pathlib.Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
SPEC_ROOT = REPO_ROOT / "dataset_cloudcp" / "spec_files"
MANIFEST_PATH = SPEC_ROOT / "manifest.json"
RUNS_DIR = HERE / "runs"

DEFAULT_DATAGEN = "/home/bryck/rperiyas/datagen"
DEFAULT_BRYCKCLOUD = "/opt/bryck/.venv/bryck/bin/bryckcloud"
DEFAULT_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
DEFAULT_TRANSFER_LOGS = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"

TRANSFER_ID_RE = re.compile(r"transfer(?:[_ ]id)?[^0-9]*(\d+)", re.IGNORECASE)
TRANSFER_DIR_RE = re.compile(r"^transfer_(\d+)$")
ROOT_LINE_RE = re.compile(r"^root:\s*(.+?)\s*$", re.MULTILINE)
TERMINAL_SUCCESS = {"SUCCESS", "SKIPPED", "FALLBACK_OK"}
BATCH_STATES = ("pending", "inprogress", "completed")


@dataclass
class DatasetSelection:
    dataset_id: str
    phase: int
    name: str
    expected_files: int
    root_base: str
    specs: List[dict]


@dataclass
class BatchFileRecord:
    path: pathlib.Path
    state: str
    tier: str
    records: int
    examples: List[str]


def setup_logging(verbose: bool) -> logging.Logger:
    logger = logging.getLogger("cloudcpclitesting")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a dataset, run bryckcloud transfer add aws, and validate the reports.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dataset", help="Dataset id from manifest.json, e.g. DS-P1-03.")
    parser.add_argument("--list", action="store_true", help="List dataset ids and exit.")
    parser.add_argument(
        "--spec-root",
        default=str(SPEC_ROOT),
        help="Directory containing manifest.json and DS-P* spec folders.",
    )
    parser.add_argument(
        "--output-base",
        help="Linux directory under which rewritten specs will generate files, e.g. /bryck/cloudcp_cli_data.",
    )
    parser.add_argument("--dst", help="Destination S3 URI, e.g. s3://bucket/prefix.")
    parser.add_argument("--datagen-bin", default=DEFAULT_DATAGEN, help="Path to the datagen binary.")
    parser.add_argument(
        "--bryckcloud-bin",
        default=DEFAULT_BRYCKCLOUD,
        help="Path to the bryckcloud CLI.",
    )
    parser.add_argument(
        "--batchmeta-dir",
        default=DEFAULT_BATCHMETA,
        help="Batch metadata root used to detect created transfer ids.",
    )
    parser.add_argument(
        "--transfer-logs-dir",
        default=DEFAULT_TRANSFER_LOGS,
        help="Transfer logs root used to validate reports.",
    )
    parser.add_argument(
        "--transfer-id",
        type=int,
        help="Use an explicit transfer id instead of auto-detecting a newly created one.",
    )
    parser.add_argument(
        "--transfer-arg",
        action="append",
        default=[],
        help="Extra argument to append after 'bryckcloud transfer add aws'. Repeat as needed.",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip datagen and use an already materialized dataset under --output-base/<dataset>.",
    )
    parser.add_argument(
        "--skip-transfer",
        action="store_true",
        help="Skip bryckcloud submission and validate an existing transfer id.",
    )
    parser.add_argument(
        "--validate-batches",
        action="store_true",
        default=True,
        help="Validate transfer_<id>/batches state and record counts after the run.",
    )
    parser.add_argument(
        "--no-validate-batches",
        dest="validate_batches",
        action="store_false",
        help="Skip transfer_<id>/batches validation.",
    )
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=1800,
        help="Seconds to wait for report artifacts before failing.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=10,
        help="Polling interval in seconds while waiting for reports.",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Keep the materialized dataset after the run. Default is to leave it in place anyway; this flag documents intent.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands and planned paths without executing them.")
    parser.add_argument("--yes", action="store_true", help="Skip the real-run confirmation prompt.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    if args.list:
        return
    if not args.dataset:
        raise SystemExit("--dataset is required unless --list is used.")
    if not args.output_base:
        raise SystemExit("--output-base is required.")
    if not args.skip_transfer and not args.dst:
        raise SystemExit("--dst is required unless --skip-transfer is used.")
    if args.skip_transfer and args.transfer_id is None:
        raise SystemExit("--transfer-id is required with --skip-transfer.")
    if args.wait_timeout < 1:
        raise SystemExit("--wait-timeout must be >= 1.")
    if args.poll_interval < 1:
        raise SystemExit("--poll-interval must be >= 1.")
    if args.dst and not args.dst.startswith("s3://"):
        raise SystemExit("--dst must be a plain S3 URI like s3://bucket/prefix.")


def normalize_dst_with_dataset(dst: str, dataset_id: str) -> str:
    """Ensure destination prefix keeps the dataset id as the last path segment."""
    cleaned = dst.rstrip("/")
    if cleaned.endswith("/" + dataset_id) or cleaned == "s3://" + dataset_id:
        return cleaned
    return cleaned + "/" + dataset_id


def load_manifest(spec_root: pathlib.Path) -> Tuple[dict, Dict[str, dict]]:
    manifest_path = spec_root / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"manifest not found: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    dataset_map = {entry["id"]: entry for entry in manifest.get("datasets", [])}
    return manifest, dataset_map


def list_datasets(spec_root: pathlib.Path) -> int:
    manifest, dataset_map = load_manifest(spec_root)
    print(f"Datasets in {spec_root}:")
    for dataset_id in sorted(dataset_map):
        entry = dataset_map[dataset_id]
        print(
            f"  {dataset_id:<9} phase={entry.get('phase')} "
            f"specs={entry.get('spec_count')} expected={entry.get('emitted_files')} "
            f"name={entry.get('name')}"
        )
    print(f"root_base={manifest.get('root_base')}")
    return 0


def select_dataset(spec_root: pathlib.Path, dataset_id: str) -> DatasetSelection:
    manifest, dataset_map = load_manifest(spec_root)
    try:
        entry = dataset_map[dataset_id]
    except KeyError as exc:
        raise SystemExit(f"dataset not found: {dataset_id}") from exc
    expected_files = entry.get("emitted_files")
    if expected_files is None:
        expected_files = sum(int(spec.get("count", 0)) for spec in entry.get("specs", []))
    return DatasetSelection(
        dataset_id=dataset_id,
        phase=int(entry.get("phase", 0)),
        name=entry.get("name", dataset_id),
        expected_files=int(expected_files),
        root_base=str(manifest.get("root_base", "")),
        specs=list(entry.get("specs", [])),
    )


def rewrite_spec_root(spec_path: pathlib.Path, root_base: str, output_base: str) -> Tuple[pathlib.Path, pathlib.Path]:
    text = spec_path.read_text(encoding="utf-8")
    match = ROOT_LINE_RE.search(text)
    if not match:
        raise ValueError(f"spec has no root: {spec_path}")
    original_root = match.group(1).strip()

    if root_base and original_root.startswith(root_base):
        rel = original_root[len(root_base):].lstrip("/")
    else:
        rel = original_root.lstrip("/")

    rel_parts = [part for part in rel.replace("\\", "/").split("/") if part]
    yaml_root = output_base.replace("\\", "/").rstrip("/") + "/" + "/".join(rel_parts)
    fs_root = pathlib.Path(output_base, *rel_parts)
    new_text = ROOT_LINE_RE.sub(f"root: {yaml_root}", text, count=1)

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        prefix=f"cli_{spec_path.stem}_",
        delete=False,
        encoding="utf-8",
    )
    tmp.write(new_text)
    tmp.close()
    return pathlib.Path(tmp.name), fs_root


def shell_join(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def run_cmd(
    cmd: Sequence[str],
    logger: logging.Logger,
    dry_run: bool,
    cwd: Optional[pathlib.Path] = None,
) -> subprocess.CompletedProcess[str] | None:
    logger.info("$ %s", shell_join(cmd))
    if dry_run:
        return None
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True)


def check_completed(proc: subprocess.CompletedProcess[str], what: str) -> None:
    if proc.returncode != 0:
        stderr = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"{what} failed with exit {proc.returncode}: {stderr[:800]}")


def count_files_recursive(root: pathlib.Path) -> int:
    if not root.is_dir():
        return 0
    total = 0
    for _dirpath, _dirnames, filenames in os.walk(root):
        total += len(filenames)
    return total


def generate_dataset(
    args: argparse.Namespace,
    dataset: DatasetSelection,
    spec_root: pathlib.Path,
    logger: logging.Logger,
) -> Tuple[pathlib.Path, dict]:
    dataset_root = pathlib.Path(args.output_base) / dataset.dataset_id
    summary = {
        "dataset_root": str(dataset_root),
        "expected_files": dataset.expected_files,
        "spec_results": [],
    }
    if args.skip_generate:
        actual = count_files_recursive(dataset_root)
        summary["actual_files"] = actual
        if actual != dataset.expected_files:
            raise RuntimeError(
                f"existing dataset file count mismatch: actual={actual} expected={dataset.expected_files}"
            )
        return dataset_root, summary

    tmp_specs: List[pathlib.Path] = []
    try:
        for spec_meta in dataset.specs:
            spec_file = spec_meta["file"]
            expected = int(spec_meta.get("count", 0))
            original_spec = spec_root / dataset.dataset_id / spec_file
            tmp_spec, spec_output_root = rewrite_spec_root(original_spec, dataset.root_base, args.output_base)
            tmp_specs.append(tmp_spec)
            spec_output_root.mkdir(parents=True, exist_ok=True)
            proc = run_cmd([args.datagen_bin, "--spec", str(tmp_spec)], logger, args.dry_run)
            if proc is not None:
                check_completed(proc, f"datagen for {spec_file}")
                if proc.stdout.strip() and args.verbose:
                    logger.info(proc.stdout.strip())
                if proc.stderr.strip():
                    logger.info(proc.stderr.strip())
            actual = expected if args.dry_run else count_files_recursive(spec_output_root)
            if actual != expected:
                raise RuntimeError(
                    f"spec count mismatch for {spec_file}: actual={actual} expected={expected}"
                )
            summary["spec_results"].append(
                {
                    "spec_file": spec_file,
                    "expected_files": expected,
                    "actual_files": actual,
                    "materialized_root": str(spec_output_root),
                }
            )
        total_actual = dataset.expected_files if args.dry_run else count_files_recursive(dataset_root)
        summary["actual_files"] = total_actual
        if total_actual != dataset.expected_files:
            raise RuntimeError(
                f"dataset file count mismatch: actual={total_actual} expected={dataset.expected_files}"
            )
        return dataset_root, summary
    finally:
        for tmp_spec in tmp_specs:
            try:
                tmp_spec.unlink()
            except OSError:
                pass


def collect_transfer_ids(batchmeta_dir: pathlib.Path, transfer_logs_dir: pathlib.Path) -> set[int]:
    ids: set[int] = set()
    for root in (batchmeta_dir, transfer_logs_dir):
        if not root.is_dir():
            continue
        for entry in root.iterdir():
            match = TRANSFER_DIR_RE.match(entry.name)
            if match:
                ids.add(int(match.group(1)))
    return ids


def parse_transfer_id_from_output(text: str) -> Optional[int]:
    candidates = [int(match.group(1)) for match in TRANSFER_ID_RE.finditer(text)]
    if not candidates:
        return None
    return candidates[-1]


def detect_transfer_id(
    args: argparse.Namespace,
    before_ids: set[int],
    command_output: str,
    batchmeta_dir: pathlib.Path,
    transfer_logs_dir: pathlib.Path,
) -> int:
    if args.transfer_id is not None:
        return args.transfer_id

    parsed = parse_transfer_id_from_output(command_output)
    if parsed is not None:
        return parsed

    deadline = time.time() + max(args.poll_interval, 5)
    while time.time() < deadline:
        after_ids = collect_transfer_ids(batchmeta_dir, transfer_logs_dir)
        new_ids = sorted(after_ids - before_ids)
        if new_ids:
            return new_ids[-1]
        time.sleep(1)

    raise RuntimeError("could not detect transfer id; rerun with --transfer-id <id>")


def transfer_log_dir(transfer_logs_dir: pathlib.Path, transfer_id: int) -> pathlib.Path:
    return transfer_logs_dir / f"cloud_transfer_{transfer_id}"


def transfer_batchmeta_dir(batchmeta_dir: pathlib.Path, transfer_id: int) -> pathlib.Path:
    return batchmeta_dir / f"transfer_{transfer_id}"


def batch_state_dir(batchmeta_dir: pathlib.Path, transfer_id: int, state: str) -> pathlib.Path:
    return transfer_batchmeta_dir(batchmeta_dir, transfer_id) / "batches" / state


def transfer_report_path(transfer_logs_dir: pathlib.Path, transfer_id: int) -> pathlib.Path:
    nested = transfer_log_dir(transfer_logs_dir, transfer_id) / f"transfer_report_{transfer_id}.csv"
    flat = transfer_logs_dir / f"transfer_report_{transfer_id}.csv"
    if nested.is_file():
        return nested
    if flat.is_file():
        return flat
    return nested


def final_report_path(transfer_logs_dir: pathlib.Path, transfer_id: int) -> pathlib.Path:
    return transfer_log_dir(transfer_logs_dir, transfer_id) / "final_report.csv"


def report_dir(transfer_logs_dir: pathlib.Path, transfer_id: int) -> pathlib.Path:
    return transfer_log_dir(transfer_logs_dir, transfer_id) / "report"


def load_csv_rows(path: pathlib.Path) -> List[dict]:
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def iter_upload_shards(report_root: pathlib.Path) -> Iterable[pathlib.Path]:
    if not report_root.is_dir():
        return []
    return sorted(report_root.glob("upload_report.*.csv"))


def count_nul_records(path: pathlib.Path) -> int:
    data = path.read_bytes()
    return data.count(b"\0")


def decode_path_samples(path: pathlib.Path, limit: int = 3) -> List[str]:
    data = path.read_bytes()
    samples: List[str] = []
    for raw in data.split(b"\0"):
        if not raw:
            continue
        samples.append(os.fsdecode(raw))
        if len(samples) >= limit:
            break
    return samples


def collect_batch_files(batchmeta_dir: pathlib.Path, transfer_id: int) -> List[BatchFileRecord]:
    records: List[BatchFileRecord] = []
    for state in BATCH_STATES:
        state_dir = batch_state_dir(batchmeta_dir, transfer_id, state)
        if not state_dir.is_dir():
            continue
        for tier_dir in sorted(path for path in state_dir.iterdir() if path.is_dir()):
            for batch_file in sorted(path for path in tier_dir.iterdir() if path.is_file()):
                records.append(
                    BatchFileRecord(
                        path=batch_file,
                        state=state,
                        tier=tier_dir.name,
                        records=count_nul_records(batch_file),
                        examples=decode_path_samples(batch_file),
                    )
                )
    return records


def load_json_if_exists(path: pathlib.Path) -> Optional[dict]:
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def summarize_batch_files(batch_files: List[BatchFileRecord]) -> dict:
    state_counts = {state: 0 for state in BATCH_STATES}
    state_records = {state: 0 for state in BATCH_STATES}
    tier_counts: Dict[str, int] = {}
    tier_records: Dict[str, int] = {}
    file_rows: List[dict] = []

    for batch_file in batch_files:
        state_counts[batch_file.state] = state_counts.get(batch_file.state, 0) + 1
        state_records[batch_file.state] = state_records.get(batch_file.state, 0) + batch_file.records
        tier_counts[batch_file.tier] = tier_counts.get(batch_file.tier, 0) + 1
        tier_records[batch_file.tier] = tier_records.get(batch_file.tier, 0) + batch_file.records
        file_rows.append(
            {
                "path": str(batch_file.path),
                "state": batch_file.state,
                "tier": batch_file.tier,
                "records": batch_file.records,
                "examples": batch_file.examples,
            }
        )

    return {
        "batch_files": file_rows,
        "state_batch_counts": state_counts,
        "state_record_counts": state_records,
        "tier_batch_counts": tier_counts,
        "tier_record_counts": tier_records,
        "total_batches": len(batch_files),
        "total_records": sum(item.records for item in batch_files),
    }


def validate_batch_summary_csv(batch_summary_path: pathlib.Path, expected_files: int) -> dict:
    if not batch_summary_path.is_file():
        return {"exists": False, "problems": [f"batch summary not found: {batch_summary_path}"]}

    rows = load_csv_rows(batch_summary_path)
    total_files = 0
    rows_with_files = 0
    file_key: Optional[str] = None
    for row in rows:
        if file_key is None:
            for candidate in ("num_files", "file_count", "files", "count", "total_files"):
                if candidate in row:
                    file_key = candidate
                    break
        if file_key is None:
            continue
        value = parse_int(row.get(file_key))
        if value is None:
            continue
        total_files += value
        rows_with_files += 1

    problems: List[str] = []
    if rows and rows_with_files == 0:
        problems.append("batch_summary.csv exists but no recognizable file-count column was found")
    if rows_with_files and total_files != expected_files:
        problems.append(f"batch_summary.csv total files {total_files} != expected {expected_files}")

    return {
        "exists": True,
        "rows": len(rows),
        "file_count_column": file_key,
        "total_files": total_files,
        "rows_with_files": rows_with_files,
        "problems": problems,
    }


def validate_batch_artifacts(
    batchmeta_dir: pathlib.Path,
    transfer_id: int,
    dataset_root: pathlib.Path,
    expected_files: int,
) -> dict:
    transfer_dir = transfer_batchmeta_dir(batchmeta_dir, transfer_id)
    problems: List[str] = []
    if not transfer_dir.is_dir():
        return {
            "exists": False,
            "transfer_dir": str(transfer_dir),
            "problems": [f"batch metadata dir not found: {transfer_dir}"],
        }

    batch_files = collect_batch_files(batchmeta_dir, transfer_id)
    summary = summarize_batch_files(batch_files)
    transfer_manifest_path = transfer_dir / "manifest.json"
    transfer_manifest = load_json_if_exists(transfer_manifest_path) or {}
    batch_summary_path = transfer_dir / "batch_summary.csv"
    batch_summary = validate_batch_summary_csv(batch_summary_path, expected_files)

    total_batches = summary["total_batches"]
    total_records = summary["total_records"]
    completed_records = summary["state_record_counts"].get("completed", 0)
    pending_batches = summary["state_batch_counts"].get("pending", 0)
    inprogress_batches = summary["state_batch_counts"].get("inprogress", 0)
    completed_batches = summary["state_batch_counts"].get("completed", 0)

    if total_batches == 0:
        problems.append("no batch files were found under transfer_<id>/batches")
    if total_records != expected_files:
        problems.append(f"batch files contain {total_records} records but expected {expected_files}")
    if completed_records != expected_files:
        problems.append(f"completed batches contain {completed_records} records but expected {expected_files}")
    if pending_batches:
        problems.append(f"pending batches remain after transfer completion: {pending_batches}")
    if inprogress_batches:
        problems.append(f"inprogress batches remain after transfer completion: {inprogress_batches}")
    if completed_batches == 0:
        problems.append("no completed batches were found")

    manifest_total_files = parse_int(transfer_manifest.get("total_files"))
    if manifest_total_files is not None and manifest_total_files != expected_files:
        problems.append(f"transfer manifest total_files {manifest_total_files} != expected {expected_files}")

    scan_state = str(transfer_manifest.get("scan_state", "")).strip().lower()
    if scan_state and scan_state != "complete":
        problems.append(f"transfer manifest scan_state is {scan_state!r}, expected 'complete'")

    seq_high_water = parse_int(transfer_manifest.get("seq_high_water"))
    if seq_high_water is not None and seq_high_water < completed_batches:
        problems.append(
            f"transfer manifest seq_high_water {seq_high_water} is smaller than completed batch count {completed_batches}"
        )

    for batch_file in batch_files:
        if batch_file.records <= 0:
            problems.append(f"empty batch file found: {batch_file.path}")
            continue
        for sample in batch_file.examples:
            if not sample.startswith(str(dataset_root)):
                problems.append(f"batch file path outside dataset root: {sample}")
                break

    problems.extend(batch_summary.get("problems", []))

    return {
        "exists": True,
        "transfer_dir": str(transfer_dir),
        "manifest_path": str(transfer_manifest_path),
        "batch_summary_path": str(batch_summary_path),
        "transfer_manifest": transfer_manifest,
        "batch_summary": batch_summary,
        "batch_files": summary,
        "problems": problems,
    }


def count_failed_upload_records(report_root: pathlib.Path) -> int:
    total = 0
    if not report_root.is_dir():
        return 0
    for path in sorted(report_root.glob("failed_uploads.*")):
        if not path.is_file():
            continue
        data = path.read_bytes()
        if data:
            total += data.count(b"\0") // 4
    return total


def count_live_retry_lists(log_dir: pathlib.Path, transfer_id: int) -> int:
    if not log_dir.is_dir():
        return 0
    pattern = f"cloudcp_retry_{transfer_id}_*.lst"
    return len(list(log_dir.glob(pattern)))


def validate_transfer_success_rows(
    transfer_logs_dir: pathlib.Path,
    transfer_id: int,
    expected_files: int,
) -> dict:
    successes: Dict[str, dict] = {}
    non_terminal: List[str] = []

    for row in load_csv_rows(transfer_report_path(transfer_logs_dir, transfer_id)):
        status = (row.get("status") or "").strip().upper()
        local_path = row.get("local_path") or ""
        if status in TERMINAL_SUCCESS:
            successes[local_path] = row
        else:
            non_terminal.append(f"{local_path}:{status}")

    for shard in iter_upload_shards(report_dir(transfer_logs_dir, transfer_id)):
        for row in load_csv_rows(shard):
            status = (row.get("status") or "").strip().upper()
            local_path = row.get("local_path") or ""
            if status in TERMINAL_SUCCESS:
                successes[local_path] = row
            else:
                non_terminal.append(f"{local_path}:{status}")

    return {
        "merged_success_rows": len(successes),
        "non_terminal_rows": non_terminal,
        "expected_files": expected_files,
    }


def validate_final_report(
    final_report: pathlib.Path,
    dataset_root: pathlib.Path,
    expected_files: int,
    dst: str,
) -> dict:
    if not final_report.is_file():
        return {"exists": False, "rows": 0, "problems": [f"final report not found: {final_report}"]}

    rows = load_csv_rows(final_report)
    problems: List[str] = []
    expected_prefix = dst.rstrip("/")
    local_root = str(dataset_root)
    bad_prefix = 0
    size_mismatch = 0
    missing_local = 0

    for row in rows:
        local_path = row.get("AbsoluteFilePath") or row.get("local_path") or ""
        s3_path = row.get("S3Path") or row.get("s3path") or ""
        size_value = row.get("FileSize") or row.get("size") or ""
        if local_path and not local_path.startswith(local_root):
            problems.append(f"local path outside dataset root: {local_path}")
            break
        if s3_path and not s3_path.startswith(expected_prefix):
            bad_prefix += 1
        try:
            reported_size = int(size_value)
        except (TypeError, ValueError):
            reported_size = None
        if local_path and os.path.isfile(local_path):
            actual_size = os.path.getsize(local_path)
            if reported_size is not None and actual_size != reported_size:
                size_mismatch += 1
        elif local_path:
            missing_local += 1

    if len(rows) != expected_files:
        problems.append(f"final report row count {len(rows)} != expected {expected_files}")
    if bad_prefix:
        problems.append(f"{bad_prefix} final report row(s) have an unexpected S3Path prefix")
    if size_mismatch:
        problems.append(f"{size_mismatch} final report row(s) have a size mismatch")
    if missing_local:
        problems.append(f"{missing_local} final report row(s) reference missing local files")

    return {
        "exists": True,
        "rows": len(rows),
        "bad_prefix": bad_prefix,
        "size_mismatch": size_mismatch,
        "missing_local": missing_local,
        "problems": problems,
    }


def wait_for_transfer_artifacts(
    transfer_logs_dir: pathlib.Path,
    transfer_id: int,
    expected_files: int,
    timeout: int,
    poll_interval: int,
    logger: logging.Logger,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        report = transfer_report_path(transfer_logs_dir, transfer_id)
        final_report = final_report_path(transfer_logs_dir, transfer_id)
        success_summary = validate_transfer_success_rows(transfer_logs_dir, transfer_id, expected_files)
        merged_success = int(success_summary["merged_success_rows"])
        live_retries = count_live_retry_lists(transfer_log_dir(transfer_logs_dir, transfer_id), transfer_id)
        final_exists = final_report.is_file()
        logger.info(
            "waiting for transfer %s: report=%s final=%s merged_success=%s/%s live_retry_lists=%s",
            transfer_id,
            "yes" if report.is_file() else "no",
            "yes" if final_exists else "no",
            merged_success,
            expected_files,
            live_retries,
        )
        if final_exists and merged_success >= expected_files and live_retries == 0:
            return
        time.sleep(poll_interval)
    raise TimeoutError(f"timed out waiting for transfer {transfer_id} artifacts")


def build_transfer_command(args: argparse.Namespace, dataset_root: pathlib.Path, dst: str) -> List[str]:
    cmd = [
        args.bryckcloud_bin,
        "transfer",
        "add",
        "aws",
        "--src",
        str(dataset_root),
        "--dst",
        dst,
    ]
    cmd.extend(args.transfer_arg)
    return cmd


def confirm_or_abort(args: argparse.Namespace, dataset_root: pathlib.Path, dst: str) -> None:
    if args.dry_run or args.yes or args.skip_transfer:
        return
    print("About to generate and transfer:")
    print(f"  dataset={args.dataset}")
    print(f"  src={dataset_root}")
    print(f"  dst={dst}")
    print(f"  validate_batches={args.validate_batches}")
    answer = input("Proceed? [y/N] ").strip().lower()
    if answer not in {"y", "yes"}:
        raise SystemExit("aborted by user")


def write_report(run_dir: pathlib.Path, payload: dict) -> pathlib.Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.json"
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return report_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    validate_args(args)
    spec_root = pathlib.Path(args.spec_root).resolve()
    if args.list:
        return list_datasets(spec_root)

    logger = setup_logging(args.verbose)
    dataset = select_dataset(spec_root, args.dataset)
    run_dir = RUNS_DIR / f"run_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{dataset.dataset_id}"
    payload = {
        "dataset": dataset.dataset_id,
        "name": dataset.name,
        "phase": dataset.phase,
        "dst": args.dst,
        "dry_run": args.dry_run,
        "validate_batches": args.validate_batches,
    }

    try:
        dataset_root, generation_summary = generate_dataset(args, dataset, spec_root, logger)
        payload["generation"] = generation_summary
        normalized_dst = normalize_dst_with_dataset(args.dst, dataset.dataset_id)
        payload["normalized_dst"] = normalized_dst
        if normalized_dst != args.dst.rstrip("/"):
            logger.info("destination normalized to include dataset id: %s", normalized_dst)
        confirm_or_abort(args, dataset_root, normalized_dst)

        batchmeta_dir = pathlib.Path(args.batchmeta_dir)
        transfer_logs_dir = pathlib.Path(args.transfer_logs_dir)
        transfer_id = args.transfer_id
        transfer_cmd = build_transfer_command(args, dataset_root, normalized_dst)
        payload["transfer_command"] = transfer_cmd

        if not args.skip_transfer:
            before_ids = collect_transfer_ids(batchmeta_dir, transfer_logs_dir)
            proc = run_cmd(transfer_cmd, logger, args.dry_run)
            command_output = ""
            if proc is not None:
                payload["transfer_stdout"] = proc.stdout
                payload["transfer_stderr"] = proc.stderr
                payload["transfer_rc"] = proc.returncode
                check_completed(proc, "bryckcloud transfer add aws")
                command_output = (proc.stdout or "") + "\n" + (proc.stderr or "")
            if args.dry_run:
                transfer_id = transfer_id if transfer_id is not None else 0
            else:
                transfer_id = detect_transfer_id(args, before_ids, command_output, batchmeta_dir, transfer_logs_dir)
        elif transfer_id is None:
            raise RuntimeError("transfer id is required when --skip-transfer is used")

        payload["transfer_id"] = transfer_id

        if not args.dry_run:
            wait_for_transfer_artifacts(
                pathlib.Path(args.transfer_logs_dir),
                transfer_id,
                dataset.expected_files,
                args.wait_timeout,
                args.poll_interval,
                logger,
            )
            merged = validate_transfer_success_rows(pathlib.Path(args.transfer_logs_dir), transfer_id, dataset.expected_files)
            final_validation = validate_final_report(
                final_report_path(pathlib.Path(args.transfer_logs_dir), transfer_id),
                dataset_root,
                dataset.expected_files,
                normalized_dst,
            )
            failures = count_failed_upload_records(report_dir(pathlib.Path(args.transfer_logs_dir), transfer_id))
            live_retries = count_live_retry_lists(transfer_log_dir(pathlib.Path(args.transfer_logs_dir), transfer_id), transfer_id)
            payload["validation"] = {
                "merged_success": merged,
                "final_report": final_validation,
                "failed_upload_records": failures,
                "live_retry_lists": live_retries,
                "transfer_report_path": str(transfer_report_path(pathlib.Path(args.transfer_logs_dir), transfer_id)),
                "final_report_path": str(final_report_path(pathlib.Path(args.transfer_logs_dir), transfer_id)),
            }
            batch_validation = None
            if args.validate_batches:
                batch_validation = validate_batch_artifacts(
                    pathlib.Path(args.batchmeta_dir),
                    transfer_id,
                    dataset_root,
                    dataset.expected_files,
                )
                payload["validation"]["batch_validation"] = batch_validation
            problems = []
            if merged["merged_success_rows"] != dataset.expected_files:
                problems.append(
                    f"merged terminal success rows {merged['merged_success_rows']} != expected {dataset.expected_files}"
                )
            if merged["non_terminal_rows"]:
                problems.append(f"non-terminal status rows found: {len(merged['non_terminal_rows'])}")
            problems.extend(final_validation.get("problems", []))
            if failures:
                problems.append(f"failed_uploads entries present: {failures}")
            if live_retries:
                problems.append(f"live retry lists still present: {live_retries}")
            if batch_validation is not None:
                problems.extend(batch_validation.get("problems", []))
            payload["status"] = "PASS" if not problems else "FAIL"
            payload["problems"] = problems
            if problems:
                raise RuntimeError("; ".join(problems))
        else:
            payload["status"] = "DRY_RUN"

        report_path = write_report(run_dir, payload)
        logger.info("report written to %s", report_path)
        logger.info("dataset=%s transfer_id=%s status=%s", dataset.dataset_id, transfer_id, payload["status"])
        return 0
    except Exception as exc:  # noqa: BLE001
        payload["status"] = "FAIL"
        payload["error"] = str(exc)
        report_path = write_report(run_dir, payload)
        logger.error(str(exc))
        logger.error("report written to %s", report_path)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())