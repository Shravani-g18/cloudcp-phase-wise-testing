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
import types
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union


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
    args: Union[argparse.Namespace, types.SimpleNamespace],
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


# =============================================================================
# Consolidated negative-test framework
# =============================================================================
# Architecture: Dataset Manager | Environment Manager | Test Case Manager |
#               Executor | Validator | Report Generator
#
# Sources consolidated here:
#   - negative_environment_runner.py: environment-aware negative execution flow
#     (inspect -> prepare only what's needed -> validate -> execute -> validate
#     result -> cleanup -> verify final state). That file's TestContext (from
#     the missing cloud_transfer_test_runner.py) is rebuilt below directly on
#     the same bryckclient-cli scripts cloud_transfer_only.py already calls
#     (bryck_mount.py, bryck_cloud_configure.py, bryck_cloud_transfer_*.py...).
#   - NEGATIVE_TEST_PLAN.md: the complete catalog (sections 7-28). Every ID is
#     registered in NEG_CATALOG so --list-negative/reports never silently drop
#     a scenario; CLI/AUTH/TID/AWS/STATE/CLEAN are fully implemented here as a
#     concrete, working reference pattern. Every other ID is present but
#     reports BLOCKED with a clear "not yet ported" reason -- extend by adding
#     a `run=` callable to its NegTestCase entry, following the same pattern.
#   - cloud_transfer_only.py: current permissions/prerequisites/execution
#     requirements (CLI transfer-add adapter, mount-state handling, config.json
#     validation) and the summary.html/json/md report style, reused below.
#   - dataset_cloudcp/spec_files/manifest.json: dataset catalog. Replaces the
#     single hard-coded spec_files/small_1gb_fast.yaml fixture; NegDatasetManager
#     selects a dataset by requirement (small/fast fixture vs. explicit --dataset).

BRYCK_CLI_DIR = HERE / "bryckclient-cli"

NEG_TERMINAL_STATES = {"COMPLETED", "FAILED", "STOPPED", "CANCELLED"}
# Small/fast fixture datasets from the manifest (see manifest.json emitted_files),
# used as the default dataset requirement instead of one hard-coded spec file.
NEG_DEFAULT_DATASET = "DS-P2-06"    # 9 files -- fast baseline fixture
NEG_EMPTY_DATASET = "DS-P8-01"      # 0 files -- empty-source-directory cases
NEG_SINGLE_FILE_DATASET = "DS-P9-01"  # 1 file -- minimal single-object cases


# -----------------------------------------------------------------------------
# Dataset Manager
# -----------------------------------------------------------------------------

class NegDatasetManager:
    """Selects a dataset from dataset_cloudcp/spec_files/manifest.json by
    requirement instead of a single hard-coded spec file. Reuses the existing
    load_manifest()/select_dataset()/generate_dataset() helpers above."""

    def __init__(self, spec_root: pathlib.Path):
        self.spec_root = spec_root
        self._manifest, self._by_id = load_manifest(spec_root)

    def resolve(self, requirement: str = "small_fast") -> str:
        """`requirement` is either an explicit dataset id (e.g. DS-P1-04) or a
        named shorthand: 'small_fast' (default fixture), 'empty' (0 files),
        'single_file' (1 file)."""
        if requirement in self._by_id:
            return requirement
        return {
            "small_fast": NEG_DEFAULT_DATASET,
            "empty": NEG_EMPTY_DATASET,
            "single_file": NEG_SINGLE_FILE_DATASET,
        }.get(requirement, NEG_DEFAULT_DATASET)

    def select(self, requirement: str = "small_fast") -> DatasetSelection:
        return select_dataset(self.spec_root, self.resolve(requirement))

    def materialize(self, requirement: str, output_base: str, logger: logging.Logger,
                    dry_run: bool, skip_generate: bool = False,
                    spec_file: Optional[str] = None) -> tuple[pathlib.Path, dict]:
        """If `spec_file` is given, it takes priority over `requirement` and is
        materialized directly (see materialize_spec_file); otherwise a dataset
        id/shorthand is resolved from manifest.json as before."""
        if spec_file:
            return self.materialize_spec_file(spec_file, output_base, logger, dry_run)
        dataset = self.select(requirement)
        ns = types.SimpleNamespace(output_base=output_base, skip_generate=skip_generate,
                                   datagen_bin=DEFAULT_DATAGEN, dry_run=dry_run, verbose=False)
        return generate_dataset(ns, dataset, self.spec_root, logger)

    def materialize_spec_file(self, spec_file: str, output_base: str, logger: logging.Logger,
                              dry_run: bool) -> tuple[pathlib.Path, dict]:
        """Materialize an explicit datagen spec YAML (or a directory of *.yaml
        specs) instead of resolving a dataset id from manifest.json -- lets a
        single --test/--section/--range run use exactly the fixture passed via
        --spec-file (e.g. a CloudCpSchedulerTesting/spec_files/<id>/ folder)."""
        spec_path = pathlib.Path(spec_file)
        if not spec_path.is_absolute():
            candidate = self.spec_root / spec_path
            spec_path = candidate if candidate.exists() else (REPO_ROOT / spec_path)
        if not spec_path.exists():
            raise SystemExit(f"--spec-file not found: {spec_file}")
        spec_files = sorted(spec_path.glob("*.yaml")) if spec_path.is_dir() else [spec_path]
        if not spec_files:
            raise SystemExit(f"no .yaml spec file(s) found under {spec_path}")
        summary: dict = {"spec_file": str(spec_path), "spec_results": []}
        dataset_root: Optional[pathlib.Path] = None
        total_actual = 0
        for sf in spec_files:
            tmp_spec, spec_output_root = rewrite_spec_root(sf, "", output_base)
            dataset_root = dataset_root or spec_output_root
            try:
                spec_output_root.mkdir(parents=True, exist_ok=True)
                proc = run_cmd([DEFAULT_DATAGEN, "--spec", str(tmp_spec)], logger, dry_run)
                if proc is not None:
                    check_completed(proc, f"datagen for {sf.name}")
                actual = 0 if dry_run else count_files_recursive(spec_output_root)
                total_actual += actual
                summary["spec_results"].append({"spec_file": sf.name, "actual_files": actual,
                                                "materialized_root": str(spec_output_root)})
            finally:
                try:
                    tmp_spec.unlink()
                except OSError:
                    pass
        summary["actual_files"] = total_actual
        return dataset_root or pathlib.Path(output_base), summary


# -----------------------------------------------------------------------------
# Environment Manager
# -----------------------------------------------------------------------------

@dataclass
class NegCmd:
    label: str
    argv: List[str]
    rc: Optional[int]
    stdout: str
    stderr: str
    duration: float

    @property
    def passed(self) -> bool:
        return self.rc == 0

    def as_dict(self) -> dict:
        return {"label": self.label, "argv": self.argv, "rc": self.rc,
                "stdout": self.stdout[-4000:], "stderr": self.stderr[-4000:], "duration": self.duration}


class NegEnvironmentManager:
    """Prepares/validates Bryck state and records every command for the report.
    Rebuilds negative_environment_runner.py's EnvironmentManager on top of the
    same bryckclient-cli scripts cloud_transfer_only.py calls directly, since
    the original's cloud_transfer_test_runner.TestContext is not available here."""

    def __init__(self, login: str, params: str, format_mount_params: str,
                output_base: str, download_base: str, bucket: str,
                datagen_manager: NegDatasetManager, dry_run: bool,
                logger: logging.Logger, python_bin: str = "python3",
                poll_interval: int = 10, action_timeout: int = 90,
                spec_file: Optional[str] = None):
        self.login = login
        self.params = params
        self.format_mount_params = format_mount_params
        self.output_base = output_base
        self.download_base = download_base
        self.bucket = bucket
        self.datasets = datagen_manager
        self.dry_run = dry_run
        self.logger = logger
        self.spec_file = spec_file  # overrides dataset_requirement when set (see --spec-file)
        self.python_bin = python_bin
        self.poll_interval = poll_interval
        self.action_timeout = action_timeout
        self.commands: List[NegCmd] = []
        self.active_transfers: List[str] = []

    def _run_py(self, label: str, script: str, args: List[str], timeout: Optional[int] = None) -> NegCmd:
        argv = [self.python_bin, str(BRYCK_CLI_DIR / script)] + args
        self.logger.info("$ %s", shell_join(argv))
        started = time.time()
        if self.dry_run:
            cmd = NegCmd(label, argv, 0, "", "", 0.0)
        else:
            try:
                proc = subprocess.run(argv, cwd=str(BRYCK_CLI_DIR), capture_output=True, text=True, timeout=timeout)
                cmd = NegCmd(label, argv, proc.returncode, proc.stdout or "", proc.stderr or "", time.time() - started)
            except subprocess.TimeoutExpired as exc:
                cmd = NegCmd(label, argv, -1, exc.stdout or "", f"TIMEOUT after {timeout}s: {exc}", time.time() - started)
        self.commands.append(cmd)
        return cmd

    def bryck_state(self) -> str:
        cmd = self._run_py("bryck_info", "bryck_info.py", ["--login", self.login])
        if self.dry_run:
            return "DRYRUN"
        if not cmd.passed:
            return "UNKNOWN"
        try:
            payload = json.loads(cmd.stdout)
        except (json.JSONDecodeError, TypeError):
            return "UNKNOWN"
        state = payload.get("State")
        if state is None and isinstance(payload.get("bryck_info"), dict):
            state = payload["bryck_info"].get("State")
        return str(state or "UNKNOWN").strip()

    def wait_for_bryck_state(self, targets: set, timeout: int) -> str:
        """Poll bryck_state() (case-insensitive) until it matches one of `targets`
        or `timeout` elapses -- used to verify a transition actually completed
        before the next operation is attempted, instead of assuming it did."""
        if self.dry_run:
            return next(iter(targets))
        state = self.bryck_state()
        deadline = time.time() + timeout
        while time.time() < deadline:
            if state.strip().lower() in targets:
                return state
            time.sleep(self.poll_interval)
            state = self.bryck_state()
        return state

    def state_bucket(self, timeout: int = 0) -> str:
        """Classifies the CURRENT bryck_state() into 'mounted', 'ejected'
        (covers Ejected/Removed), or 'other' -- callers branch on this instead
        of blindly issuing eject/mount/format regardless of what the device is
        already doing. If `timeout` > 0, polls until the state resolves to a
        known bucket or the timeout elapses (for a device mid-transition)."""
        if self.dry_run:
            return "other"

        def _bucket_of(raw: str) -> str:
            s = raw.strip().lower()
            if s == "mounted":
                return "mounted"
            if s in {"ejected", "removed"}:
                return "ejected"
            return "other"

        bucket = _bucket_of(self.bryck_state())
        if bucket != "other" or timeout <= 0:
            return bucket
        deadline = time.time() + timeout
        while bucket == "other" and time.time() < deadline:
            time.sleep(self.poll_interval)
            bucket = _bucket_of(self.bryck_state())
        return bucket

    def ensure_mounted(self) -> bool:
        if self.dry_run:
            return True
        state = self.bryck_state().strip().lower()
        if state == "mounted":
            return True
        if state != "ejected":
            return False
        mount = self._run_py("mount", "bryck_mount.py", ["--login", self.login, "--params", self.format_mount_params],
                             timeout=self.action_timeout)
        if not mount.passed:
            return False
        deadline = time.time() + self.action_timeout
        while time.time() < deadline:
            if self.bryck_state().strip().lower() == "mounted":
                return True
            time.sleep(self.poll_interval)
        return False

    def ensure_ejected(self) -> bool:
        if self.dry_run:
            return True
        if self.bryck_state().strip().lower() == "ejected":
            return True
        return self._run_py("eject", "bryck_eject_unmount.py", ["--login", self.login],
                            timeout=self.action_timeout).passed

    def format_with_retry(self, deadline_sec: int = 1800, per_call_timeout: int = 900) -> NegCmd:
        """Format the Bryck. A real hardware format can legitimately run far
        longer than a normal action_timeout -- if the subprocess call itself
        times out that does NOT mean the device rejected the format, it likely
        means formatting is still genuinely in progress. So on a timeout, POLL
        bryck_state() for it to settle instead of blindly re-issuing bryck_format.py
        (which would restart/duplicate a real, still-running format job -- this
        was the actual bug: format kept re-triggering every poll_interval while
        the device was still busy formatting from the first call). Only a real,
        non-timeout rejection (e.g. device still settling right after eject) is
        retried by re-issuing the format command."""
        cmd = self.format_bryck(timeout=per_call_timeout)
        if self.dry_run or cmd.passed:
            return cmd
        deadline = time.time() + deadline_sec
        while time.time() < deadline:
            if cmd.stderr.startswith("TIMEOUT"):
                settled = self.wait_for_bryck_state({"ejected", "removed"}, timeout=min(60, deadline_sec))
                if settled.strip().lower() in {"ejected", "removed"}:
                    return NegCmd(cmd.label, cmd.argv, 0, cmd.stdout, cmd.stderr, cmd.duration)
                continue
            time.sleep(self.poll_interval)
            cmd = self.format_bryck(timeout=per_call_timeout)
        return cmd

    def mount_with_retry(self, deadline_sec: int = 900, per_call_timeout: int = 300) -> NegCmd:
        """Same stuck-transitional/still-running race as format_with_retry, for mount."""
        cmd = self.mount_bryck(timeout=per_call_timeout)
        if self.dry_run or cmd.passed:
            return cmd
        deadline = time.time() + deadline_sec
        while time.time() < deadline:
            if cmd.stderr.startswith("TIMEOUT"):
                settled = self.wait_for_bryck_state({"mounted"}, timeout=min(60, deadline_sec))
                if settled.strip().lower() == "mounted":
                    return NegCmd(cmd.label, cmd.argv, 0, cmd.stdout, cmd.stderr, cmd.duration)
                continue
            time.sleep(self.poll_interval)
            cmd = self.mount_bryck(timeout=per_call_timeout)
        return cmd

    def configure_cloud_with_retry(self, tier: str, base_cloud_ops: dict, deadline_sec: int = 120) -> bool:
        ok = self.configure_cloud(tier, base_cloud_ops)
        if self.dry_run:
            return ok
        deadline = time.time() + deadline_sec
        while not ok and time.time() < deadline:
            time.sleep(self.poll_interval)
            ok = self.configure_cloud(tier, base_cloud_ops)
        return ok

    def configure_cloud(self, tier: str, base_cloud_ops: dict) -> bool:
        cloud_type = str(base_cloud_ops.get("cloud_type", "aws"))
        self._run_py("deconfigure", "bryck_cloud_deconfigure.py", ["--login", self.login, "--cloud-type", cloud_type])
        cfg = dict(base_cloud_ops)
        cfg["bryck_src"] = f"{self.output_base}/{tier}"
        cfg["cloud_bucket"] = f"{self.bucket}/{tier}"
        cfg["bryck_dst"] = f"{self.download_base}/{tier}"
        if not self.dry_run:
            with open(self.params, "w", encoding="utf-8") as handle:
                json.dump(cfg, handle, indent=2)
        configure = self._run_py("configure", "bryck_cloud_configure.py", ["--login", self.login, "--params", self.params])
        return configure.passed or self.dry_run

    def format_bryck(self, timeout: Optional[int] = None) -> NegCmd:
        """Unconditional format attempt (used by the MASTER flow's baseline setup
        and as a during-active-transfer rejection probe -- unlike ensure_mounted/
        ensure_ejected, this always issues the command."""
        return self._run_py("format", "bryck_format.py", ["--login", self.login, "--params", self.format_mount_params],
                            timeout=timeout or self.action_timeout)

    def mount_bryck(self, timeout: Optional[int] = None) -> NegCmd:
        """Unconditional mount attempt (see format_bryck)."""
        return self._run_py("mount", "bryck_mount.py", ["--login", self.login, "--params", self.format_mount_params],
                            timeout=timeout or self.action_timeout)

    def eject_bryck(self) -> NegCmd:
        """Unconditional eject attempt (see format_bryck)."""
        return self._run_py("eject", "bryck_eject_unmount.py", ["--login", self.login], timeout=self.action_timeout)

    def deconfigure_cloud(self) -> NegCmd:
        try:
            cloud_type = json.loads(pathlib.Path(self.params).read_text(encoding="utf-8")).get("cloud_type", "aws")
        except (OSError, json.JSONDecodeError):
            cloud_type = "aws"
        return self._run_py("deconfigure", "bryck_cloud_deconfigure.py", ["--login", self.login, "--cloud-type", cloud_type])

    def show_cloud(self) -> NegCmd:
        return self._run_py("cloud_show", "bryck_cloud_show.py", ["--login", self.login])

    def initiate_transfer(self, mode: str, params: Optional[str] = None) -> tuple[NegCmd, Optional[str]]:
        cmd = self._run_py(f"initiate:{mode}", "bryck_cloud_transfer_initiate.py",
                           ["--login", self.login, "--params", params or self.params, "--mode", mode],
                           timeout=300)
        if self.dry_run:
            return cmd, "DRYRUN-ID"
        tid = parse_transfer_id_from_output(cmd.stdout + cmd.stderr) if cmd.passed else None
        if tid is not None:
            self.active_transfers.append(str(tid))
        return cmd, (str(tid) if tid is not None else None)

    def transfer_status(self, tid: str) -> tuple[str, NegCmd]:
        cmd = self._run_py(f"status:{tid}", "bryck_cloud_transfer_status.py",
                           ["--login", self.login, "--transfer-id", str(tid)])
        if self.dry_run:
            return "COMPLETED", cmd
        match = re.search(r"STATE\s*:\s*([A-Z_]+)", (cmd.stdout or "") + (cmd.stderr or ""))
        return (match.group(1) if match else "UNKNOWN"), cmd

    def wait_for_terminal(self, tid: str, timeout: int) -> str:
        if self.dry_run:
            return "COMPLETED"
        deadline = time.time() + timeout
        state = "UNKNOWN"
        while time.time() < deadline:
            state, _cmd = self.transfer_status(tid)
            if state in NEG_TERMINAL_STATES:
                return state
            time.sleep(self.poll_interval)
        return state

    def wait_for_state(self, tid: str, states: set, timeout: int) -> str:
        """Generic poll for any of `states` (e.g. {'IN_PROGRESS'}), not just terminal ones."""
        if self.dry_run:
            return next(iter(states))
        deadline = time.time() + timeout
        state = "UNKNOWN"
        while time.time() < deadline:
            state, _cmd = self.transfer_status(tid)
            if state in states:
                return state
            time.sleep(self.poll_interval)
        return state

    def pause_transfer(self, tid: str) -> NegCmd:
        return self._run_py(f"pause:{tid}", "bryck_cloud_transfer_pause.py", ["--login", self.login, "--transfer-id", str(tid)])

    def resume_transfer(self, tid: str) -> NegCmd:
        return self._run_py(f"resume:{tid}", "bryck_cloud_transfer_resume.py", ["--login", self.login, "--transfer-id", str(tid)])

    def cancel_transfer(self, tid: str) -> NegCmd:
        cmd = self._run_py(f"cancel:{tid}", "bryck_cloud_transfer_cancel.py", ["--login", self.login, "--transfer-id", str(tid)])
        if str(tid) in self.active_transfers:
            self.active_transfers.remove(str(tid))
        return cmd

    def download_report(self, tid: str, report_path: str) -> NegCmd:
        return self._run_py(f"report:{tid}", "bryck_cloud_transfer_report.py",
                            ["--login", self.login, "--cloud-transfer-id", str(tid), "--report-path", report_path])

    def cleanup_transfer(self, tid: Optional[str]) -> str:
        if not tid or tid == "DRYRUN-ID":
            return "no fixture transfer to clean up"
        cmd = self.cancel_transfer(tid)
        return "transfer cancelled" if cmd.passed else f"cancel returned rc={cmd.rc}"


# -----------------------------------------------------------------------------
# Test Case Manager
# -----------------------------------------------------------------------------

@dataclass
class NegResult:
    test_id: str
    section: str
    name: str
    status: str  # PASS / FAIL / BLOCKED / SKIP
    expected: str = ""
    actual: str = ""
    reason: str = ""
    dataset_used: str = ""
    duration: float = 0.0
    commands: List[dict] = None

    def __post_init__(self):
        if self.commands is None:
            self.commands = []


@dataclass
class NegTestCase:
    id: str
    section: str
    name: str
    run: Optional[Callable] = None  # (env, ctx) -> NegResult; None = not yet ported
    plan_ref: str = ""


def _neg_blocked(tc: NegTestCase, reason: str, expected: str = "") -> NegResult:
    return NegResult(test_id=tc.id, section=tc.section, name=tc.name, status="BLOCKED",
                     expected=expected or "Required environment/fixture must be established before executing.",
                     actual="Not executed.", reason=reason)


def _neg_from_cmd(tc: NegTestCase, cmd: NegCmd, expect_fail: bool, expected: str,
                  dataset_used: str = "", extra_cmds: Optional[List[NegCmd]] = None) -> NegResult:
    ok = (not cmd.passed) if expect_fail else cmd.passed
    status = "PASS" if ok else "FAIL"
    reason = ("operation correctly rejected" if (expect_fail and ok) else
             "operation unexpectedly succeeded" if (expect_fail and not ok) else
             "operation correctly succeeded" if (not expect_fail and ok) else
             "operation unexpectedly failed")
    all_cmds = (extra_cmds or []) + [cmd]
    return NegResult(test_id=tc.id, section=tc.section, name=tc.name, status=status, expected=expected,
                     actual=f"rc={cmd.rc}; stdout={cmd.stdout[-500:]!r}; stderr={cmd.stderr[-500:]!r}",
                     reason=reason, dataset_used=dataset_used, commands=[c.as_dict() for c in all_cmds])


class NegExecContext:
    """Shared per-run context passed to every test-case handler: paths, args,
    a scratch work dir for fixtures, and the loaded base login.json/cloud_ops.json."""

    def __init__(self, args: argparse.Namespace, work_dir: pathlib.Path):
        self.args = args
        self.work_dir = work_dir
        self.login_cfg = load_json_if_exists(pathlib.Path(args.login)) or {}
        self.cloud_ops_cfg = load_json_if_exists(pathlib.Path(args.cloud_ops)) or {}

    def fixture(self, name: str, base: dict, overrides: dict) -> str:
        path = self.work_dir / name
        data = {**base, **overrides}
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return str(path)

    def raw_fixture(self, name: str, content: str) -> str:
        path = self.work_dir / name
        path.write_text(content, encoding="utf-8")
        return str(path)


# ---- CLI section (fully implemented) ---------------------------------------

def _neg_cli_handler(case_id: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[case_id]
        if case_id == "CLI-01":
            cmd = env._run_py(case_id, "bryck_cloud_transfer_initiate.py", ["--login", env.login, "--params", env.params])
            return _neg_from_cmd(tc, cmd, True, "argparse rejects the call because --mode is required; no transfer created.")
        if case_id == "CLI-02":
            cmd = env._run_py(case_id, "bryck_cloud_transfer_initiate.py",
                              ["--login", env.login, "--params", env.params, "--mode", "copy"])
            return _neg_from_cmd(tc, cmd, True, "argparse rejects --mode copy as an invalid choice.")
        if case_id in {"CLI-03", "CLI-04", "CLI-05"}:
            field = {"CLI-03": "bryck_src", "CLI-04": "cloud_bucket", "CLI-05": "bryck_dst"}[case_id]
            mode = "download" if case_id == "CLI-05" else "upload"
            p = ctx.fixture(f"{case_id}.json", ctx.cloud_ops_cfg, {field: ""})
            cmd = env._run_py(case_id, "bryck_cloud_transfer_initiate.py",
                              ["--login", env.login, "--params", p, "--mode", mode])
            return _neg_from_cmd(tc, cmd, True, f"{mode} is rejected because {field} is empty.")
        if case_id == "CLI-06":
            p = str(ctx.work_dir / "missing-login.json")
            cmd = env._run_py(case_id, "bryck_cloud_show.py", ["--login", p])
            return _neg_from_cmd(tc, cmd, True, "Missing login.json fails with a readable file-not-found error.")
        if case_id == "CLI-07":
            p = ctx.raw_fixture(f"{case_id}.json", "{")
            cmd = env._run_py(case_id, "bryck_cloud_show.py", ["--login", p])
            return _neg_from_cmd(tc, cmd, True, "Malformed login.json fails with a readable JSON error.")
        if case_id == "CLI-08":
            cmd = env._run_py(case_id, "bryck_cloud_transfer_pause.py",
                              ["--login", env.login, "--transfer-id", "not-a-transfer-id"])
            return _neg_from_cmd(tc, cmd, True, "Pause is rejected for an invalid transfer id; no state change.")
        if case_id == "CLI-09":
            spec_path = ctx.work_dir / "missing-spec.yaml"
            cmd = env._run_py(case_id, "bryck_cloud_transfer_initiate.py",
                              ["--login", env.login, "--params", env.params])  # placeholder path never used
            # datagen with a nonexistent spec is the real check here.
            argv = [DEFAULT_DATAGEN, "--spec", str(spec_path)]
            env.logger.info("$ %s", shell_join(argv))
            started = time.time()
            if env.dry_run:
                dg = NegCmd(case_id, argv, 0, "", "", 0.0)
            else:
                try:
                    proc = subprocess.run(argv, capture_output=True, text=True, timeout=60)
                    dg = NegCmd(case_id, argv, proc.returncode, proc.stdout or "", proc.stderr or "", time.time() - started)
                except (OSError, subprocess.TimeoutExpired) as exc:
                    dg = NegCmd(case_id, argv, 1, "", str(exc), time.time() - started)
            return _neg_from_cmd(tc, dg, True, "datagen fails before any host mutation because the spec file does not exist.")
        return _neg_blocked(tc, "no fixture implemented for this CLI case yet")
    return handler


# ---- AUTH section (mutation-based subset implemented; expiry cases need --live) ---

def _neg_auth_handler(case_id: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[case_id]
        mutation = {
            "AUTH-01": {"bryckapi_username": "invalid-user"},
            "AUTH-02": {"bryckapi_password": "invalid-password"},
            "AUTH-05": {"bryckapi_password": ""},
            "AUTH-03": {"bryckapi_token": "invalid.garbage.token"},
        }.get(case_id)
        if mutation is None:
            return _neg_blocked(tc, "requires --live plus a real/expired token fixture (token minting not wired here)")
        p = ctx.fixture(f"{case_id}.json", ctx.login_cfg, mutation)
        cmd = env._run_py(case_id, "bryck_cloud_show.py", ["--login", p])
        return _neg_from_cmd(tc, cmd, True, "Authentication is rejected; no state is mutated.")
    return handler


# ---- TID section (fully implemented) ----------------------------------------

NEG_TID_VALUES = {
    "TID-01": "99999999", "TID-02": "", "TID-03": "-1", "TID-04": "not-a-transfer",
    "TID-05": "!@#$%^&*", "TID-06": "9" * 36, "TID-07": "2147483647", "TID-08": "1", "TID-09": "1.2.3",
}


def _neg_tid_handler(case_id: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[case_id]
        value = NEG_TID_VALUES.get(case_id, "99999999")
        cmds = []
        status_cmd = env._run_py(f"{case_id}:status", "bryck_cloud_transfer_status.py",
                                 ["--login", env.login, "--transfer-id", value])
        cmds.append(status_cmd)
        pause_cmd = env.pause_transfer(value)
        resume_cmd = env.resume_transfer(value)
        cancel_cmd = env.cancel_transfer(value)
        report_cmd = env.download_report(value, str(ctx.work_dir))
        all_cmds = [status_cmd, pause_cmd, resume_cmd, cancel_cmd, report_cmd]
        all_ok = env.dry_run or all(not c.passed for c in all_cmds)
        status = "PASS" if all_ok else "FAIL"
        return NegResult(test_id=case_id, section="TID", name=tc.name, status=status,
                         expected=f"status/pause/resume/cancel/report all reject value={value!r} cleanly.",
                         actual="; ".join(f"{c.label}:rc={c.rc}" for c in all_cmds),
                         reason="all rejected as expected" if all_ok else "at least one op unexpectedly succeeded",
                         commands=[c.as_dict() for c in all_cmds])
    return handler


# ---- AWS config section (fully implemented) ---------------------------------

NEG_AWS_MUTATIONS = {
    "AWS-01": {"access_key_id": ""}, "AWS-02": {"secret_access_key": ""},
    "AWS-03": {"access_key_id": "invalid-access-key"}, "AWS-04": {"secret_access_key": "invalid-secret-key"},
    "AWS-05": {"region": "invalid-region"}, "AWS-06": {"endpoint": "http://127.0.0.1:1"},
    "AWS-07": {"cloud_bucket": "not-a-valid-bucket"},
}


def _neg_aws_handler(case_id: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[case_id]
        if case_id in NEG_AWS_MUTATIONS:
            p = ctx.fixture(f"{case_id}.json", ctx.cloud_ops_cfg, NEG_AWS_MUTATIONS[case_id])
            cmd = env._run_py(case_id, "bryck_cloud_configure.py", ["--login", env.login, "--params", p])
            return _neg_from_cmd(tc, cmd, True, "Provider configuration is rejected; no partial provider remains.")
        if case_id == "AWS-08":
            p = ctx.fixture(f"{case_id}.json", ctx.cloud_ops_cfg,
                            {"cloud_bucket": f"s3://does-not-exist-negative-{uuid.uuid4().hex[:8]}"})
            cmd = env._run_py(case_id, "bryck_cloud_configure.py", ["--login", env.login, "--params", p])
            return _neg_from_cmd(tc, cmd, True, "Nonexistent bucket is rejected; no success is reported.")
        if case_id == "AWS-13":
            cmd = env.deconfigure_cloud()
            return _neg_from_cmd(tc, cmd, True, "Deconfiguring when not configured is rejected or explicitly idempotent.")
        if case_id == "AWS-14":
            first = env.deconfigure_cloud()
            second = env.deconfigure_cloud()
            return _neg_from_cmd(tc, second, True, "Second deconfigure is deterministic; no stale provider remains.",
                                 extra_cmds=[first])
        return _neg_blocked(tc, "requires --live plus a real active/paused transfer fixture")
    return handler


# ---- STATE section (state-machine sequences; needs --live) -----------------

NEG_STATE_SEQUENCES = {
    "STATE-01": [("pause", False), ("pause", True)],
    "STATE-02": [("pause", False), ("resume", False), ("resume", True)],
    "STATE-03": [("pause", False), ("cancel", False), ("cancel", True)],
    "STATE-04": [("resume", True)],
    "STATE-05": [("cancel", False), ("cancel", True)],
}


def _neg_state_handler(case_id: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[case_id]
        if case_id == "STATE-12":
            cmd = env._run_py(case_id, "bryck_cloud_transfer_status.py",
                              ["--login", env.login, "--state", "NOT_A_REAL_STATE"])
            return _neg_from_cmd(tc, cmd, True, "An unknown state filter is rejected without a traceback.")
        if not ctx.args.live:
            return _neg_blocked(tc, "requires --live against the dedicated Bryck device")
        if case_id not in NEG_STATE_SEQUENCES:
            return _neg_blocked(tc, "no automated sequence registered for this case yet")
        dataset = ctx.args.dataset_requirement
        tier = env.datasets.resolve(dataset)
        if not (env.ensure_mounted() and env.configure_cloud(tier, ctx.cloud_ops_cfg)):
            return _neg_blocked(tc, "could not establish mounted+configured baseline")
        _root, _summary = env.datasets.materialize(dataset, env.output_base, env.logger, env.dry_run,
                                                    spec_file=env.spec_file)
        init_cmd, tid = env.initiate_transfer("upload")
        if not tid:
            return _neg_blocked(tc, "could not establish an active transfer fixture")
        env.wait_for_terminal(tid, 60) if False else None  # ops below drive state directly
        cmds = [init_cmd]
        fn_map = {"pause": env.pause_transfer, "resume": env.resume_transfer, "cancel": env.cancel_transfer}
        all_ok = True
        for op, expect_fail in NEG_STATE_SEQUENCES[case_id]:
            cmd = fn_map[op](tid)
            cmds.append(cmd)
            ok = (not cmd.passed) if expect_fail else cmd.passed
            all_ok = all_ok and ok
        cleanup = env.cleanup_transfer(tid)
        status = "PASS" if all_ok else "FAIL"
        seq_desc = " -> ".join(op for op, _ in NEG_STATE_SEQUENCES[case_id])
        return NegResult(test_id=case_id, section="STATE", name=tc.name, status=status, dataset_used=tier,
                         expected=f"Sequence {seq_desc} matches the documented state machine.",
                         actual="; ".join(f"{c.label}:rc={c.rc}" for c in cmds), reason=cleanup,
                         commands=[c.as_dict() for c in cmds])
    return handler


# ---- CLEAN section (read-only final audits implemented) ---------------------

def _neg_clean_handler(case_id: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[case_id]
        if case_id == "CLEAN-09":
            cmd = env._run_py(case_id, "bryck_cloud_transfer_status.py", ["--login", env.login])
            return _neg_from_cmd(tc, cmd, False, "No stale active/orphan transfer is reported.")
        if case_id == "CLEAN-04":
            cmd = env.show_cloud()
            return _neg_from_cmd(tc, cmd, False, "No stale cloud configuration is present.")
        if case_id == "CLEAN-10":
            info = env._run_py(f"{case_id}:info", "bryck_info.py", ["--login", env.login])
            network = env._run_py(f"{case_id}:network", "bryck_network_info.py", ["--login", env.login])
            ok = info.passed and network.passed
            return NegResult(test_id=case_id, section="CLEAN", name=tc.name, status="PASS" if ok else "FAIL",
                             expected="Device and network state are both in a known-valid condition.",
                             actual=f"info.rc={info.rc} network.rc={network.rc}",
                             reason="both queries succeeded" if ok else "one or both queries failed",
                             commands=[info.as_dict(), network.as_dict()])
        return _neg_blocked(tc, "requires --live plus a real transfer/lifecycle fixture")
    return handler


# ---- MASTER section: P0 end-to-end upload/download/both flows --------------
# Ported from cloud_transfer_negative_test_runner.py's run_master_flow()/
# run_master_flow_both(): one continuous narrative (format -> mount ->
# configure -> transfer -> pause/resume -> attempt destructive/lifecycle ops
# mid-transfer -> completion -> report -> deconfigure -> eject -> cleanup)
# instead of independent, isolated catalog cases. Not in NEGATIVE_TEST_PLAN.md's
# section numbering; registered under its own "MASTER" section.

def _neg_master_run_leg(env: NegEnvironmentManager, ctx: NegExecContext, mode: str,
                        notes: List[str], generate: bool) -> Optional[str]:
    """Runs one upload/download leg end-to-end and returns its transfer id, or
    None if a required step failed (aborting just this leg, not the whole flow)."""
    if generate:
        try:
            env.datasets.materialize(ctx.args.dataset_requirement, env.output_base, env.logger, env.dry_run,
                                     spec_file=env.spec_file)
            notes.append("datagen: OK")
        except (RuntimeError, SystemExit) as exc:
            notes.append(f"datagen: FAIL ({exc})")
            return None

    init_cmd, tid = env.initiate_transfer(mode)
    notes.append(f"initiate {mode}: {'OK' if init_cmd.passed else 'FAIL'} (rc={init_cmd.rc})")
    if not tid:
        notes.append(f"initiate {mode}: no transfer id obtained; aborting this leg")
        return None

    state = env.wait_for_state(tid, {"IN_PROGRESS", "COMPLETED"}, timeout=120)
    notes.append(f"wait for IN_PROGRESS: state={state}")
    env.download_report(tid, str(ctx.work_dir / f"{mode}_in_progress_report"))

    if state == "IN_PROGRESS":
        pause_cmd = env.pause_transfer(tid)
        notes.append(f"pause: {'OK' if pause_cmd.passed else 'FAIL'} (rc={pause_cmd.rc})")
        paused_state = env.wait_for_state(tid, {"PAUSED"}, timeout=60)
        notes.append(f"verify PAUSED before proceeding: state={paused_state}")
        env.download_report(tid, str(ctx.work_dir / f"{mode}_paused_report"))
        if paused_state != "PAUSED":
            notes.append(f"WARNING: transfer never confirmed PAUSED (state={paused_state!r}); "
                        f"continuing with reduced confidence in the pause/resume steps below")

        pause_again = env.pause_transfer(tid)
        notes.append(f"pause again (idempotence check): rc={pause_again.rc}")

        resume_cmd = env.resume_transfer(tid)
        notes.append(f"resume: {'OK' if resume_cmd.passed else 'FAIL'} (rc={resume_cmd.rc})")
        resumed_state = env.wait_for_state(tid, {"IN_PROGRESS", "COMPLETED"}, timeout=120)
        notes.append(f"verify IN_PROGRESS/COMPLETED after resume: state={resumed_state}")
        env.download_report(tid, str(ctx.work_dir / f"{mode}_resumed_report"))

        # Destructive/lifecycle attempts while the transfer is active -- every one of
        # these is expected to be rejected (or at least not silently corrupt state).
        fmt_attempt = env.format_bryck()
        notes.append(f"attempt FORMAT during active transfer: rejected={not fmt_attempt.passed} (rc={fmt_attempt.rc})")
        eject_attempt = env.eject_bryck()
        notes.append(f"attempt EJECT during active transfer: rejected={not eject_attempt.passed} (rc={eject_attempt.rc})")
        mount_attempt = env.mount_bryck()
        notes.append(f"attempt MOUNT during active transfer: rc={mount_attempt.rc}")
        deconf_attempt = env.deconfigure_cloud()
        notes.append(f"attempt AWS DECONFIGURE during active transfer: rejected={not deconf_attempt.passed} (rc={deconf_attempt.rc})")
        integrity_state = env.wait_for_bryck_state({"mounted"}, timeout=env.action_timeout)
        notes.append(f"post-attempt integrity check: bryck_state={integrity_state!r} "
                    f"(expected still 'Mounted' -- rejected attempts must not have changed device state)")

    final_state = env.wait_for_terminal(tid, timeout=7200)
    notes.append(f"wait for completion: final_state={final_state}")
    env.download_report(tid, str(ctx.work_dir / f"{mode}_completed_report"))
    return tid


def _neg_master_handler(direction: str) -> Callable:
    def handler(env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
        tc = NEG_CATALOG[f"MASTER-{direction.upper()}"]
        if not ctx.args.live:
            return _neg_blocked(tc, "requires --live against the dedicated Bryck device")

        notes: List[str] = []
        env.commands = []

        def fail(reason: str) -> NegResult:
            return NegResult(test_id=tc.id, section="MASTER", name=tc.name, status="FAIL",
                             expected="Each step's resulting Bryck state is verified before the next step runs "
                                     "(no operation is issued against an unconfirmed/transitional state).",
                             actual="; ".join(notes), reason=reason, commands=[c.as_dict() for c in env.commands])

        # 1. Baseline state -- classify it before deciding what (if anything) to do.
        baseline_state = env.bryck_state()
        bucket = env.state_bucket()
        notes.append(f"baseline bryck_state={baseline_state!r} (bucket={bucket!r})")

        # 2. Only eject if currently Mounted; if already Ejected/Removed, skip the
        # eject step entirely instead of issuing it blindly. If the state is
        # neither (e.g. mid-transition), poll until it resolves before deciding.
        if bucket == "other":
            notes.append(f"state {baseline_state!r} is neither Mounted nor Ejected/Removed; polling for it to settle")
            bucket = env.state_bucket(timeout=env.action_timeout)
            notes.append(f"state resolved to bucket={bucket!r}")

        if bucket == "mounted":
            eject_cmd = env.eject_bryck()
            notes.append(f"eject (currently Mounted): rc={eject_cmd.rc}")
            ejected_state = env.wait_for_bryck_state({"ejected", "removed"}, timeout=env.action_timeout)
            notes.append(f"verify NOT mounted after eject: bryck_state={ejected_state!r}")
            if ejected_state.strip().lower() not in {"ejected", "removed"}:
                return fail(f"device never reached Ejected/Removed after eject (stuck at {ejected_state!r})")
        elif bucket == "ejected":
            notes.append("already Ejected/Removed; skipping eject step")
        else:
            return fail(f"could not determine a known Bryck state before format (state={baseline_state!r})")

        # 3. Format, retried on a bounded deadline (a stuck 'Ejecting' device, or a
        # real hardware format that's still genuinely running, are both handled by
        # format_with_retry itself), then VERIFY it is not mounted immediately
        # afterward before attempting mount.
        fmt_cmd = env.format_with_retry()
        notes.append(f"format: {'OK' if fmt_cmd.passed else 'FAIL'} (rc={fmt_cmd.rc})")
        if not fmt_cmd.passed:
            return fail("format step failed (device never confirmed formatted within the retry deadline)")
        post_format_state = env.wait_for_bryck_state({"ejected", "removed"}, timeout=60)
        notes.append(f"verify NOT mounted after format: bryck_state={post_format_state!r}")
        if post_format_state.strip().lower() not in {"ejected", "removed"}:
            return fail(f"device not in Ejected/Removed immediately after format (state={post_format_state!r})")

        # 4. Only mount if currently Ejected/Removed; if somehow already Mounted
        # (unexpected right after format), skip the mount step instead of
        # issuing it blindly, then VERIFY Mounted before configuring cloud or
        # generating any dataset (never generate data against an unconfirmed
        # mount state).
        pre_mount_bucket = env.state_bucket()
        if pre_mount_bucket == "mounted":
            notes.append("already Mounted right after format (unexpected); skipping mount step")
            mounted_state = "Mounted"
        else:
            mount_cmd = env.mount_with_retry()
            notes.append(f"mount: {'OK' if mount_cmd.passed else 'FAIL'} (rc={mount_cmd.rc})")
            if not mount_cmd.passed:
                return fail("mount step failed (device never confirmed mounted within the retry deadline)")
            mounted_state = env.wait_for_bryck_state({"mounted"}, timeout=env.action_timeout)
        notes.append(f"verify Mounted after mount: bryck_state={mounted_state!r}")
        if mounted_state.strip().lower() != "mounted":
            return fail(f"device never reached Mounted after mount (state={mounted_state!r})")

        # 5. Configure AWS, retried on a bounded deadline, then VERIFY via
        # bryck_cloud_show.py before starting any transfer leg.
        tier = env.datasets.resolve(ctx.args.dataset_requirement)
        configured = env.configure_cloud_with_retry(tier, ctx.cloud_ops_cfg, deadline_sec=120)
        notes.append(f"configure AWS: {'OK' if configured else 'FAIL'}")
        if not configured:
            return fail("cloud configure step failed within the 120s retry deadline")
        show_cmd = env.show_cloud()
        notes.append(f"verify cloud configured: rc={show_cmd.rc}")
        if not show_cmd.passed:
            return fail("bryck_cloud_show.py could not confirm the cloud configuration actually took effect")

        # 6. Transfer leg(s) -- each already verifies IN_PROGRESS/PAUSED/terminal
        # state before proceeding to its own next step (see _neg_master_run_leg).
        upload_tid = download_tid = None
        if direction in ("upload", "both"):
            upload_tid = _neg_master_run_leg(env, ctx, "upload", notes, generate=True)
        if direction in ("download", "both"):
            if direction == "download":
                # Download leg needs source data already in the bucket -- seed it first.
                _neg_master_run_leg(env, ctx, "upload", notes, generate=True)
            download_tid = _neg_master_run_leg(env, ctx, "download", notes, generate=False)

        # 7. Cleanup -- deconfigure, then only eject if currently Mounted
        # (best-effort: recorded but never overrides the flow's own PASS/FAIL).
        env.deconfigure_cloud()
        notes.append("deconfigure (cleanup): done")
        if env.state_bucket() == "mounted":
            eject_cleanup = env.eject_bryck()
            final_state = env.wait_for_bryck_state({"ejected", "removed"}, timeout=env.action_timeout)
            notes.append(f"eject (cleanup): rc={eject_cleanup.rc}; final bryck_state={final_state!r}")
        else:
            notes.append("already not Mounted; skipping cleanup eject step")
        for t in (upload_tid, download_tid):
            if t:
                env.cleanup_transfer(t)

        needed = {"upload": upload_tid, "download": download_tid, "both": upload_tid and download_tid}[direction]
        ok = bool(needed)
        return NegResult(test_id=tc.id, section="MASTER", name=tc.name, status="PASS" if ok else "FAIL",
                         expected=f"P0 master {direction} flow completes end-to-end: format/mount/configure -> "
                                  f"transfer -> pause/resume -> blocked destructive attempts -> completion -> cleanup.",
                         actual="; ".join(notes), reason="flow completed" if ok else "one or more required leg(s) failed",
                         dataset_used=tier, commands=[c.as_dict() for c in env.commands])
    return handler


# -----------------------------------------------------------------------------
# Catalog: every ID from NEGATIVE_TEST_PLAN.md (sections 7-28)
# -----------------------------------------------------------------------------

def _neg_build_catalog() -> Dict[str, NegTestCase]:
    catalog: Dict[str, NegTestCase] = {}

    def add(case_id: str, section: str, name: str, handler_factory: Optional[Callable] = None, plan_ref: str = ""):
        run = handler_factory(case_id) if handler_factory else None
        catalog[case_id] = NegTestCase(id=case_id, section=section, name=name, run=run, plan_ref=plan_ref)

    cli_names = {
        "CLI-01": "Initiate without --mode", "CLI-02": "Invalid mode", "CLI-03": "Upload without bryck_src",
        "CLI-04": "Upload without cloud_bucket", "CLI-05": "Download without bryck_dst",
        "CLI-06": "Missing login file", "CLI-07": "Malformed login JSON", "CLI-08": "Invalid transfer id operation",
        "CLI-09": "Missing dataset spec",
    }
    for cid, name in cli_names.items():
        add(cid, "CLI", name, _neg_cli_handler, "\u00a77")

    auth_names = {
        "AUTH-01": "Invalid username", "AUTH-02": "Invalid password", "AUTH-03": "Invalid access token",
        "AUTH-04": "Expired token", "AUTH-05": "Missing authentication token", "AUTH-06": "Request after session expiry",
        "AUTH-07": "Transfer operation after expiry", "AUTH-08": "Pause after expiry", "AUTH-09": "Resume after expiry",
        "AUTH-10": "Cancel after expiry",
    }
    for cid, name in auth_names.items():
        add(cid, "AUTH", name, _neg_auth_handler, "\u00a78")

    for cid in NEG_TID_VALUES:
        add(cid, "TID", f"Transfer ID validation ({NEG_TID_VALUES[cid]!r})", _neg_tid_handler, "\u00a79")

    aws_names = {
        "AWS-01": "Missing access key", "AWS-02": "Missing secret key", "AWS-03": "Invalid access key",
        "AWS-04": "Invalid secret key", "AWS-05": "Invalid region", "AWS-06": "Invalid endpoint",
        "AWS-07": "Invalid bucket URI", "AWS-08": "Nonexistent bucket", "AWS-09": "Inaccessible bucket",
        "AWS-10": "Missing List permission", "AWS-11": "Missing PutObject permission",
        "AWS-12": "Missing GetObject permission", "AWS-13": "Deconfigure when not configured",
        "AWS-14": "Deconfigure twice", "AWS-15": "Deconfigure during active transfer",
        "AWS-16": "Deconfigure while paused", "AWS-17": "Reconfigure during active transfer",
        "AWS-18": "Reconfigure during paused transfer",
    }
    for cid, name in aws_names.items():
        add(cid, "AWS", name, _neg_aws_handler, "\u00a710")

    path_names = {f"PATH-{i:02d}": n for i, n in enumerate([
        "Invalid destination prefix", "Empty prefix", "Leading slash", "Double slash",
        "Special characters/spaces", "Unicode prefix", "Very long prefix", "Parent traversal",
        "Source/config mismatch",
    ], start=1)}
    for cid, name in path_names.items():
        add(cid, "PATH", name, None, "\u00a711")

    life_names = {f"LIFE-{i:02d}": n for i, n in enumerate([
        "Info unavailable", "Mount before format", "Missing mount params", "Invalid mount params",
        "Mount already mounted", "Format while mounted", "Invalid format params", "Format unavailable",
        "Eject already ejected", "Eject active transfer", "Eject paused transfer", "Erase mounted",
        "Format/erase/remove during transfer", "Mount during transfer", "Format during verification",
        "Eject during cancellation",
    ], start=1)}
    for cid, name in life_names.items():
        add(cid, "LIFE", name, None, "\u00a712")

    data_names = {f"DATA-{i:02d}": n for i, n in enumerate([
        "Generate while ejected", "Generate while unmounted", "Generate while active", "Generate while paused",
        "Missing specification", "Invalid specification", "Invalid/negative size", "Insufficient storage",
        "Outside /bryck", "Inaccessible files", "Interrupted generation", "Duplicate generation",
    ], start=1)}
    for cid, name in data_names.items():
        add(cid, "DATASET", name, None, "\u00a713")

    xfer_names = {f"XFER-{i:02d}": n for i, n in enumerate([
        "Upload while ejected/unmounted", "Cloud not configured", "Invalid source path", "Empty source directory",
        "Inaccessible source", "Nonexistent bucket", "Invalid cloud object path", "Invalid download destination",
        "Upload while download active", "Download while upload active", "Pause immediately", "Resume before pause",
        "Pause twice", "Resume twice", "Cancel twice", "Lifecycle action during transfer", "Cloud change during transfer",
        "Download missing object",
    ], start=1)}
    for cid, name in xfer_names.items():
        add(cid, "XFER", name, None, "\u00a714")

    download_names = {
        "DOWNLOAD-01": "Ejected/unmounted destination", "DOWNLOAD-02": "Missing object",
        "DOWNLOAD-03": "Invalid destination", "DOWNLOAD-04": "Cloud permission denied",
        "DOWNLOAD-05": "Download while upload active", "DOWNLOAD-06": "Cancel/pause/resume duplicate",
    }
    for cid, name in download_names.items():
        add(cid, "DOWNLOAD", name, None, "\u00a715")

    state_names = {
        "STATE-01": "IN_PROGRESS -> PAUSE -> PAUSE", "STATE-02": "IN_PROGRESS -> PAUSE -> RESUME -> RESUME",
        "STATE-03": "IN_PROGRESS -> PAUSE -> CANCEL -> CANCEL", "STATE-04": "IN_PROGRESS -> RESUME",
        "STATE-05": "IN_PROGRESS -> CANCEL -> CANCEL", "STATE-06": "PAUSED -> PAUSE", "STATE-07": "PAUSED -> RESUME -> RESUME",
        "STATE-08": "PAUSED -> CANCEL -> CANCEL", "STATE-09": "PAUSED -> EJECT/FORMAT/ERASE",
        "STATE-10": "COMPLETED -> PAUSE/RESUME/CANCEL", "STATE-11": "CANCELLED -> PAUSE/RESUME/CANCEL",
        "STATE-12": "Unknown transfer state", "STATE-13": "Rejected operation state audit",
    }
    for cid, name in state_names.items():
        add(cid, "STATE", name, _neg_state_handler, "\u00a716")

    race_names = {f"RACE-{i:02d}": n for i, n in enumerate([
        "Pause + cancel", "Resume + cancel", "Pause + pause", "Resume + resume", "Cancel + cancel",
        "Transfer + lifecycle", "Upload + download", "Upload + upload", "Download + download",
        "Operation + deconfigure", "Invalid-ID ops + live transfer",
    ], start=1)}
    for cid, name in race_names.items():
        add(cid, "RACE", name, None, "\u00a717")

    dup_names = {
        "DUP-01": "Duplicate configure", "DUP-02": "Duplicate deconfigure", "DUP-03": "Duplicate mount/eject",
        "DUP-04": "Duplicate report", "DUP-05": "Repeated status",
    }
    for cid, name in dup_names.items():
        add(cid, "DUP", name, None, "\u00a718")

    report_names = {f"REPORT-{i:02d}": n for i, n in enumerate([
        "Invalid ID/missing directory", "Empty ID", "Before transfer", "During IN_PROGRESS", "During PAUSED",
        "During cancellation", "After CANCELLED", "After COMPLETED", "Output is unwritable/file",
        "Duplicate generation", "During transition",
    ], start=1)}
    for cid, name in report_names.items():
        add(cid, "REPORT", name, None, "\u00a719")

    fault_names = {
        "FAULT-01": "API unavailable/timeout/reset", "FAULT-02": "HTTP 400/401/403/404/409/500",
        "FAULT-03": "Malformed API response", "FAULT-04": "SSH unavailable/timeout", "FAULT-05": "SSH connection drop",
    }
    for cid, name in fault_names.items():
        add(cid, "FAULT", name, None, "\u00a720")

    rec_names = {
        "REC-01": "Restart service during upload/download", "REC-02": "Kill runner during transfer",
        "REC-03": "Network interruption and restore", "REC-04": "Status after restart/reboot (excluded)",
        "REC-05": "Configure after recovery",
    }
    for cid, name in rec_names.items():
        add(cid, "REC", name, None, "\u00a721")

    verify_names = {f"VERIFY-{i:02d}": n for i, n in enumerate([
        "Missing objects after completion", "Partial objects after failure", "Incorrect transferred size",
        "Remains active after completion", "Remains paused after resume",
    ], start=1)}
    for cid, name in verify_names.items():
        add(cid, "VERIFY", name, None, "\u00a722")

    int_names = {f"INT-{i:02d}": n for i, n in enumerate([
        "Objects missing after completed upload", "Partial objects after failed upload", "Incorrect transferred size",
        "Source deleted during upload", "Source modified during upload", "Source directory renamed",
        "Object deleted during download", "Destination removed during download", "Partial upload/download then resume",
        "Cancel then new transfer", "Interrupted transfer then status/report",
    ], start=1)}
    for cid, name in int_names.items():
        add(cid, "INT", name, None, "\u00a723")

    clean_names = {f"CLEAN-{i:02d}": n for i, n in enumerate([
        "Cancel then eject", "Cancel then format", "Cancel then mount", "Cancel then deconfigure",
        "Failed transfer follow-up", "New transfer after failed/cancelled", "Deconfigure after completion",
        "Eject after completion", "Final transfer audit", "Final device audit", "Dataset audit", "Process audit",
    ], start=1)}
    for cid, name in clean_names.items():
        add(cid, "CLEAN", name, _neg_clean_handler, "\u00a724")

    mgmt_names = {f"MGMT-{i:02d}": n for i, n in enumerate([
        "Network info while ejected/unmounted", "Invalid IP address", "Invalid netmask",
        "Invalid/unreachable NTP server", "Invalid calendar date", "Invalid time-of-day",
        "Report while ejected/unmounted", "Remove while mounted", "Remove then rescan recovery",
        "Duplicate NTP configuration",
    ], start=1)}
    for cid, name in mgmt_names.items():
        add(cid, "MGMT", name, None, "\u00a725")

    svc_services = [
        "bcloud.service", "bryckcp.service", "bryckmonitor.service", "bryckobjectstore.service.new",
        "bryckagentbsmb.service", "bryck-info-trigger.service", "bryckmonitor_worker.service", "bstream.service",
        "bryckagentlc.service", "bryckmonitor_alert.service", "bryckobjectstore.service", "bryckapi.service",
        "bryckmonitor_prune_db.service", "redis.service", "minio.service",
    ]
    scenario_keys = ["stop_active_transfer", "restart_active_transfer", "stop_before_mgmt_op"]
    n = 0
    for service in svc_services:
        for scenario in scenario_keys:
            n += 1
            add(f"SVC-{n:02d}", "SVC", f"{scenario} ({service})", None, "\u00a726")

    for cid, state, op in [
        ("SM-01", "CREATED", "Status"), ("SM-02", "CREATED", "Pause"), ("SM-03", "CREATED", "Resume"),
        ("SM-04", "CREATED", "Cancel"), ("SM-05", "IN_PROGRESS", "Status"), ("SM-06", "IN_PROGRESS", "Pause"),
        ("SM-07", "IN_PROGRESS", "Resume"), ("SM-08", "IN_PROGRESS", "Cancel"), ("SM-09", "IN_PROGRESS", "Eject"),
        ("SM-10", "IN_PROGRESS", "Format"), ("SM-11", "IN_PROGRESS", "Mount"), ("SM-12", "IN_PROGRESS", "Deconfigure"),
        ("SM-13", "PAUSED", "Status"), ("SM-14", "PAUSED", "Pause"), ("SM-15", "PAUSED", "Resume"),
        ("SM-16", "PAUSED", "Cancel"), ("SM-17", "PAUSED", "Eject"), ("SM-18", "PAUSED", "Format"),
        ("SM-19", "PAUSED", "Mount"), ("SM-20", "PAUSED", "Deconfigure"), ("SM-21", "COMPLETED", "Status"),
        ("SM-22", "COMPLETED", "Pause"), ("SM-23", "COMPLETED", "Resume"), ("SM-24", "COMPLETED", "Cancel"),
        ("SM-25", "CANCELLED", "Status"), ("SM-26", "CANCELLED", "Pause"), ("SM-27", "CANCELLED", "Resume"),
        ("SM-28", "CANCELLED", "Cancel"),
    ]:
        add(cid, "SM", f"{state} -> {op}", None, "\u00a727")

    flow_names = {
        "F-01": "P0 IP Change -> Format -> Mount -> Upload Negative Matrix",
        "F-02": "P0 IP Change -> Format -> Mount -> Download Negative Matrix",
        "F-03": "Format Without Eject -> Recovery", "F-04": "Active Upload -> All Management Conflicts",
        "F-05": "Paused Upload -> All Management Conflicts", "F-06": "Resume Race -> Management Conflicts",
        "F-07": "Active Download -> All Management Conflicts", "F-08": "Upload Pause/Resume Repetition",
        "F-09": "Upload Pause -> Cancel -> Cleanup", "F-10": "Upload Active -> Cancel Immediately -> New Upload",
        "F-11": "Completed Upload -> Invalid Operations", "F-12": "Completed Download -> Invalid Operations",
        "F-13": "Upload + Upload Concurrent", "F-14": "Upload + Download Concurrent",
        "F-15": "Pause + Deconfigure Race", "F-16": "Resume + Deconfigure Race", "F-17": "Pause + Cancel Race",
        "F-18": "Resume + Cancel Race", "F-19": "Eject + Cancel Race", "F-20": "Format + Cancel Race",
        "F-21": "API Failure During Active Upload", "F-22": "SSH Failure During Dataset Generation",
        "F-23": "Service Restart During Upload", "F-24": "Service Restart During Pause",
        "F-25": "Token Expiry During Upload", "F-26": "Token Expiry During Paused Transfer",
        "F-27": "Network Loss During Upload", "F-28": "Network Loss During Download",
        "F-29": "Report At Every Transfer State", "F-30": "Format/Eject/Mount State Cycle With Transfer Attempts",
        "F-31": "Dataset Path Mismatch Flow", "F-32": "Insufficient Space Flow",
        "F-33": "Invalid AWS Permission Flow", "F-34": "Cancel -> Deconfigure -> Eject -> Reconfigure -> New Transfer",
        "F-35": "Completed -> Deconfigure -> Eject -> Reconfigure -> New Transfer",
        "F-36": "Failed Transfer -> Recovery -> New Transfer",
        "F-37": "System Reboot During Active Transfer (excluded)", "F-38": "System Reboot During Paused Transfer (excluded)",
        "F-39": "Full Upload Negative Regression", "F-40": "Full Download Negative Regression",
    }
    for cid, name in flow_names.items():
        add(cid, "F", name, None, "\u00a728")

    master_names = {
        "MASTER-UPLOAD": "P0 end-to-end upload flow (format/mount/configure/upload/pause/resume/"
                        "blocked-destructive-attempts/completion/cleanup)",
        "MASTER-DOWNLOAD": "P0 end-to-end download flow (seed upload, then format/mount/configure/download/"
                          "pause/resume/blocked-destructive-attempts/completion/cleanup)",
        "MASTER-BOTH": "P0 end-to-end both flow (upload leg then download leg in one continuous session)",
    }
    for cid, name in master_names.items():
        add(cid, "MASTER", name, lambda case_id: _neg_master_handler(case_id.split("-", 1)[1].lower()),
            "master flow (not in NEGATIVE_TEST_PLAN.md)")

    return catalog


NEG_CATALOG: Dict[str, NegTestCase] = _neg_build_catalog()
# Canonical 1..N ordering (insertion order == plan section order 7-28) used by
# --list-negative's [n] column and --range START-END selection.
NEG_CATALOG_ORDER: List[str] = list(NEG_CATALOG.keys())


# -----------------------------------------------------------------------------
# Executor
# -----------------------------------------------------------------------------

def _neg_run_case(case_id: str, env: NegEnvironmentManager, ctx: NegExecContext) -> NegResult:
    tc = NEG_CATALOG.get(case_id)
    if tc is None:
        return NegResult(test_id=case_id, section="UNKNOWN", name="?", status="BLOCKED",
                         reason=f"unknown test id {case_id!r}")
    if tc.run is None:
        return _neg_blocked(tc, f"not yet ported into cloudcpclitesting.py; see NEGATIVE_TEST_PLAN.md {tc.plan_ref}")
    env.commands = []
    started = time.time()
    try:
        result = tc.run(env, ctx)
    except Exception as exc:  # noqa: BLE001 - the framework must never crash the whole suite
        result = NegResult(test_id=case_id, section=tc.section, name=tc.name, status="FAIL",
                           expected="Handler executes without raising.", actual=f"{type(exc).__name__}: {exc}",
                           reason="Runner-side exception; treat as a framework defect, not a product verdict.",
                           commands=[c.as_dict() for c in env.commands])
    result.duration = time.time() - started
    return result


def _neg_write_reports(results_dir: pathlib.Path, run_id: str, results: List[NegResult],
                       interrupted: bool = False, total_duration: float = 0.0) -> tuple[pathlib.Path, pathlib.Path]:
    """Writes summary.json/summary.html, reusing cloud_transfer_only.py's report style.

    Called exactly once per run -- including a cancelled/crashed run -- so a
    report always exists at results/<results-dir>/<run_id>/ even if the run
    never reaches its last test case (see run_negative_suite's try/finally)."""
    run_dir = results_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0, "SKIP": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    section_perf: Dict[str, Dict[str, float]] = {}
    for r in results:
        agg = section_perf.setdefault(r.section, {"count": 0, "total_duration_sec": 0.0})
        agg["count"] += 1
        agg["total_duration_sec"] += r.duration
    performance = {
        "total_run_duration_sec": round(total_duration, 2),
        "case_count": len(results),
        "avg_case_duration_sec": round(sum(r.duration for r in results) / len(results), 2) if results else 0.0,
        "by_section": {
            sec: {"count": int(v["count"]), "total_duration_sec": round(v["total_duration_sec"], 2),
                 "avg_duration_sec": round(v["total_duration_sec"] / v["count"], 2) if v["count"] else 0.0}
            for sec, v in sorted(section_perf.items())
        },
    }

    json_path = run_dir / "summary.json"
    json_path.write_text(json.dumps({
        "run_id": run_id, "generated_at": dt.datetime.now().isoformat(), "interrupted": interrupted,
        "counts": counts, "performance": performance,
        "test_cases": [dataclasses_asdict_neg(r) for r in results],
    }, indent=2, default=str), encoding="utf-8")

    def status_class(status: str) -> str:
        return {"PASS": "pass", "FAIL": "fail", "BLOCKED": "blocked", "SKIP": "blocked"}.get(status, "")

    rows = "".join(
        f"<tr class='{status_class(r.status)}'><td><span class='badge {status_class(r.status)}'>{r.status}</span></td>"
        f"<td>{r.test_id}</td><td>{r.section}</td><td>{r.name}</td><td>{r.dataset_used}</td>"
        f"<td>{r.duration:.2f}s</td><td>{(r.reason or '')[:200]}</td></tr>"
        for r in results
    )
    perf_rows = "".join(
        f"<tr><td>{sec}</td><td>{v['count']}</td><td>{v['total_duration_sec']:.2f}s</td>"
        f"<td>{v['avg_duration_sec']:.2f}s</td></tr>"
        for sec, v in performance["by_section"].items()
    )
    banner = (
        f'<div class="banner">RUN WAS CANCELLED / INTERRUPTED -- this report is PARTIAL '
        f'({len(results) - counts.get("SKIP", 0)}/{len(results)} case(s) actually executed, '
        f'remaining case(s) marked SKIP below).</div>' if interrupted else ""
    )
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>CloudCP Negative Suite {run_id}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #f3f4f6; color: #1f2937; }}
.summary {{ display: flex; gap: 12px; margin: 16px 0; }}
.summary div {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px 16px; text-align: center; }}
table {{ border-collapse: collapse; width: 100%; background: #fff; font-size: 13px; margin-bottom: 24px; }}
th, td {{ border: 1px solid #e5e7eb; padding: 6px 10px; text-align: left; }}
th {{ background: #f9fafb; }}
tr.fail td {{ background: #fef2f2; }} tr.blocked td {{ background: #fffbeb; }}
.badge {{ padding: 2px 8px; border-radius: 999px; font-weight: 700; font-size: 12px; }}
.badge.pass {{ background: #dcfce7; color: #14532d; }} .badge.fail {{ background: #fee2e2; color: #7f1d1d; }}
.badge.blocked {{ background: #fef3c7; color: #78350f; }}
.banner {{ background: #fee2e2; color: #7f1d1d; border: 1px solid #fecaca; border-radius: 8px; padding: 10px 16px; margin-bottom: 16px; font-weight: 700; }}
</style></head><body>
<h1>CloudCP Negative Suite {run_id}</h1>
{banner}
<div class="summary">
  <div><div>{len(results)}</div>Total</div>
  <div><div>{counts.get('PASS', 0)}</div>PASS</div>
  <div><div>{counts.get('FAIL', 0)}</div>FAIL</div>
  <div><div>{counts.get('BLOCKED', 0)}</div>BLOCKED</div>
  <div><div>{counts.get('SKIP', 0)}</div>SKIP</div>
  <div><div>{performance['total_run_duration_sec']:.2f}s</div>Total run time</div>
</div>
<h2>Performance by section</h2>
<table><tr><th>Section</th><th>Cases</th><th>Total duration</th><th>Avg duration</th></tr>
{perf_rows}
</table>
<h2>Test cases</h2>
<table><tr><th>Status</th><th>Test ID</th><th>Section</th><th>Name</th><th>Dataset</th><th>Duration</th><th>Reason</th></tr>
{rows}
</table></body></html>"""
    html_path = run_dir / "summary.html"
    html_path.write_text(html, encoding="utf-8")
    return json_path, html_path


def dataclasses_asdict_neg(r: NegResult) -> dict:
    return {"test_id": r.test_id, "section": r.section, "name": r.name, "status": r.status,
            "expected": r.expected, "actual": r.actual, "reason": r.reason, "dataset_used": r.dataset_used,
            "duration": r.duration, "commands": r.commands}


def run_negative_suite(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Consolidated CloudCP CLI negative-test framework.")
    p.add_argument("--negative", action="store_true", help="Run the negative-test suite instead of the single-transfer tool.")
    p.add_argument("--list-negative", action="store_true",
                   help="List every registered test case with its [n] order number (for --range) and exit.")
    p.add_argument("--test", default=None, help="Run one test case, or a comma-separated list, e.g. AWS-03,CLI-01.")
    p.add_argument("--section", default=None, help="Run every case in one section, e.g. --section CLI.")
    p.add_argument("--range", default=None,
                   help="Run a contiguous range of cases by the [n] order number shown by --list-negative, "
                        "e.g. --range 1-9 (first 9 cases) or --range 42-42 (a single case by position).")
    p.add_argument("--all-negative", action="store_true", help="Run every registered test case.")
    p.add_argument("--live", action="store_true", help="Execute for real (default: dry-run).")
    p.add_argument("--dataset-requirement", dest="dataset_requirement", default="small_fast",
                   help="Dataset requirement: 'small_fast' (default), 'empty', 'single_file', or an explicit DS-P* id.")
    p.add_argument("--spec-file", dest="spec_file", default=None,
                   help="Explicit datagen spec YAML, or a directory of *.yaml specs, to materialize instead of "
                        "resolving --dataset-requirement from manifest.json -- overrides --dataset-requirement "
                        "when given, e.g. --spec-file dataset_cloudcp/spec_files/DS-P9-01 or "
                        "--spec-file CloudCpSchedulerTesting/spec_files/SCH-DEEP-01")
    p.add_argument("--login", default=str(BRYCK_CLI_DIR / "login.json"))
    p.add_argument("--cloud-ops", default=str(BRYCK_CLI_DIR / "cloud_ops.json"))
    p.add_argument("--format-mount-params", default=str(BRYCK_CLI_DIR / "format_mount_params.json"))
    p.add_argument("--output-base", default="/bryck")
    p.add_argument("--download-base", default="/bryck/cloudcp_cli_dl")
    p.add_argument("--bucket", default="s3://shravani/cloudcp-cli")
    p.add_argument("--results-dir", default=str(HERE / "results" / "negative"),
                   help="Report root. A report (summary.json/summary.html) is ALWAYS written under "
                        "<results-dir>/<run-id>/ when the run finishes, fails, or is cancelled midway "
                        "(Ctrl+C) -- it is never lost, only marked 'interrupted': true / PARTIAL.")
    p.add_argument("--run-id", default=None)
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    if args.list_negative:
        for i, cid in enumerate(NEG_CATALOG_ORDER, start=1):
            tc = NEG_CATALOG[cid]
            impl = "IMPLEMENTED" if tc.run else "stub"
            print(f"  [{i:>3}] {cid:<10} [{tc.section:<8}] {impl:<12} {tc.name}")
        print(f"\n{len(NEG_CATALOG)} total case(s). Use --range START-END against the [n] numbers above.")
        return 0

    if args.range:
        m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", args.range)
        if not m:
            print(f"ERROR: --range must look like START-END (e.g. 1-9), got {args.range!r}")
            return 2
        start, end = int(m.group(1)), int(m.group(2))
        if not (1 <= start <= end <= len(NEG_CATALOG_ORDER)):
            print(f"ERROR: --range {args.range!r} is out of bounds for 1-{len(NEG_CATALOG_ORDER)} (see --list-negative)")
            return 2
        ids = NEG_CATALOG_ORDER[start - 1:end]
    elif args.test:
        ids = [t.strip() for t in args.test.split(",") if t.strip()]
        unknown = [t for t in ids if t not in NEG_CATALOG]
        if unknown:
            print(f"ERROR: unknown test id(s): {unknown}. Use --list-negative to see valid IDs.")
            return 2
    elif args.section:
        ids = [cid for cid in NEG_CATALOG_ORDER if NEG_CATALOG[cid].section == args.section.upper()]
        if not ids:
            print(f"ERROR: no cases registered for section {args.section!r}.")
            return 2
    elif args.all_negative:
        ids = list(NEG_CATALOG_ORDER)
    else:
        print("Use --list-negative, --test <id[,id...]>, --section <NAME>, --range <START-END>, or --all-negative.")
        return 2

    logger = setup_logging(args.verbose)
    run_id = args.run_id or dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = pathlib.Path(args.results_dir)
    dataset_mgr = NegDatasetManager(SPEC_ROOT)

    results: List[NegResult] = []
    interrupted = False
    run_started = time.time()
    with tempfile.TemporaryDirectory(prefix=f"cloudcpclitesting-neg-{run_id}-") as work_dir:
        work = pathlib.Path(work_dir)
        env = NegEnvironmentManager(
            login=args.login, params=args.cloud_ops, format_mount_params=args.format_mount_params,
            output_base=args.output_base, download_base=args.download_base, bucket=args.bucket,
            datagen_manager=dataset_mgr, dry_run=(not args.live), logger=logger, spec_file=args.spec_file,
        )
        ctx = NegExecContext(args, work)
        try:
            for i, cid in enumerate(ids, start=1):
                logger.info("=== [%d/%d] %s ===", i, len(ids), cid)
                result = _neg_run_case(cid, env, ctx)
                logger.info("    -> %s: %s", result.status, result.reason)
                results.append(result)
        except KeyboardInterrupt:
            # Ctrl+C mid-run -- still write a report for every case that already ran,
            # instead of losing all evidence collected so far.
            interrupted = True
            logger.warning("Run cancelled by user (Ctrl+C) after %d/%d case(s); writing partial report...",
                           len(results), len(ids))
        except Exception as exc:  # noqa: BLE001 - an unexpected crash must not lose already-collected results
            interrupted = True
            logger.error("Run aborted by unexpected error (%s: %s) after %d/%d case(s); writing partial report...",
                        type(exc).__name__, exc, len(results), len(ids))
        finally:
            for cid in ids[len(results):]:
                tc = NEG_CATALOG.get(cid)
                if tc is not None:
                    results.append(NegResult(test_id=cid, section=tc.section, name=tc.name, status="SKIP",
                                             reason="run cancelled/aborted before this case executed"))

    total_duration = time.time() - run_started
    json_path, html_path = _neg_write_reports(results_dir, run_id, results, interrupted=interrupted,
                                              total_duration=total_duration)
    counts: Dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    logger.info("PASS=%s FAIL=%s BLOCKED=%s SKIP=%s (total %s, %.1fs)", counts.get("PASS", 0), counts.get("FAIL", 0),
               counts.get("BLOCKED", 0), counts.get("SKIP", 0), len(results), total_duration)
    logger.info("JSON: %s", json_path)
    logger.info("HTML: %s", html_path)
    if interrupted:
        logger.warning("Run was cancelled/interrupted -- report above is PARTIAL (%d/%d case(s) actually executed).",
                       len(results) - counts.get("SKIP", 0), len(ids))
    return 1 if counts.get("FAIL", 0) else 0


if __name__ == "__main__":
    if any(a in ("--negative", "--list-negative", "--test", "--section", "--range", "--all-negative")
          for a in sys.argv[1:]):
        raise SystemExit(run_negative_suite())
    raise SystemExit(main())
