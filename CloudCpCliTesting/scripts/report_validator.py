#!/usr/bin/env python3
"""scripts/report_validator.py — Transfer report CSV/JSON validation helper.

Validates the structure and content of a cloudcp transfer report.

Usage
-----
    python3 report_validator.py --csv /path/to/transfer_report_42.csv

    # Specify expected row count
    python3 report_validator.py --csv transfer_report_42.csv --expected-count 91320

    # Sample HeadObject checks against S3 (requires boto3 + credentials)
    python3 report_validator.py --csv transfer_report_42.csv \\
        --sample 100 \\
        --endpoint https://10.10.10.103:9000 \\
        --bucket aditya

    # Validate the JSON summary file
    python3 report_validator.py --json /etc/bryck/bryckcloud/transfer_summary_files.json

    # Validate s3_key composition rule for a sample of rows
    python3 report_validator.py --csv transfer_report_42.csv \\
        --check-keys --fs-prefix /bryck/cli_test_data --prefix cli_test

Exit codes
----------
    0 — all checks passed
    1 — one or more checks failed

Report format (from bcloud_final_design.md §16 and config.json VERIFICATION section)
---------------------------------------------------------------------------------------
CSV headers: file_path, size, status, s3_key, transfer_id
Valid statuses: SUCCESS, SKIPPED, FAILED, MISMATCH, PARTIAL
"""

import argparse
import csv
import json
import pathlib
import random
import sys

# Add parent to path so cli_config imports cleanly
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import cli_config as _cfg

# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

class ValidationError(Exception):
    pass


def validate_csv_structure(csv_path: pathlib.Path) -> list[dict]:
    """Load and structurally validate the CSV. Returns rows as dicts."""
    if not csv_path.exists():
        raise ValidationError(f"Report not found: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        headers = reader.fieldnames or []
        missing = [h for h in _cfg.REQUIRED_REPORT_HEADERS if h not in headers]
        if missing:
            raise ValidationError(
                f"Missing required CSV headers: {missing}. "
                f"Found: {headers}"
            )
        rows = list(reader)

    return rows


def validate_status_values(rows: list[dict]) -> list[str]:
    """Return a list of error messages for invalid status values."""
    errors = []
    for i, row in enumerate(rows):
        status = row.get("status", "").strip()
        if status not in _cfg.VALID_REPORT_STATUSES:
            errors.append(
                f"Row {i+2}: invalid status {status!r} "
                f"(file={row.get('file_path','?')})"
            )
    return errors


def validate_row_count(rows: list[dict], expected: int | None) -> list[str]:
    """Check that the actual row count matches the expected count."""
    if expected is None:
        return []
    actual = len(rows)
    if actual != expected:
        return [f"Row count mismatch: expected {expected}, got {actual}."]
    return []


def validate_key_composition(
    rows: list[dict],
    fs_prefix: str,
    prefix: str,
    sample_n: int = 200,
) -> list[str]:
    """Validate s3_key = prefix + strip(fs_prefix, file_path) for a sample."""
    errors = []
    sample = random.sample(rows, min(sample_n, len(rows)))
    for row in sample:
        file_path = row.get("file_path", "")
        s3_key = row.get("s3_key", "")
        if not file_path or not s3_key:
            continue
        # Strip fs_prefix from file_path
        if fs_prefix and file_path.startswith(fs_prefix):
            relative = file_path[len(fs_prefix):]
        else:
            relative = file_path
        # Normalise separators
        relative = relative.lstrip("/")
        expected_key = f"{prefix}/{relative}" if prefix else relative
        if s3_key != expected_key:
            errors.append(
                f"Key mismatch for {file_path!r}: "
                f"expected {expected_key!r}, got {s3_key!r}"
            )
    return errors


def validate_s3_objects(
    rows: list[dict],
    bucket: str,
    endpoint: str,
    sample_n: int = 100,
) -> list[str]:
    """HeadObject check against S3 for a sample of SUCCESS rows.

    Requires boto3. Skips gracefully if not installed.
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        from botocore.exceptions import ClientError
    except ImportError:
        return ["[skip] boto3 not installed; S3 HeadObject checks skipped."]

    success_rows = [r for r in rows if r.get("status", "").strip() == "SUCCESS"]
    sample = random.sample(success_rows, min(sample_n, len(success_rows)))

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        config=BotoConfig(signature_version="s3v4"),
    )

    errors = []
    for row in sample:
        key = row.get("s3_key", "").strip()
        expected_size = int(row.get("size", -1))
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            actual_size = resp.get("ContentLength", -1)
            if expected_size >= 0 and actual_size != expected_size:
                errors.append(
                    f"Size mismatch for key {key!r}: "
                    f"report says {expected_size}, S3 says {actual_size}."
                )
        except ClientError as exc:
            errors.append(f"HeadObject failed for {key!r}: {exc}")

    return errors


def validate_json_summary(json_path: pathlib.Path) -> list[str]:
    """Validate the JSON transfer summary file structure."""
    if not json_path.exists():
        return [f"JSON summary file not found: {json_path}"]

    try:
        with open(json_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:
        return [f"Invalid JSON in summary file: {exc}"]

    errors = []
    required_fields = ["transfer_id", "status", "total", "success", "failed"]
    # Handle list or single-record JSON
    records = data if isinstance(data, list) else [data]
    for i, rec in enumerate(records):
        missing = [f for f in required_fields if f not in rec]
        if missing:
            errors.append(f"Record {i}: missing required fields {missing}")

    return errors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Validate a cloudcp transfer report CSV or JSON summary",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--csv", metavar="PATH", help="Transfer report CSV path")
    p.add_argument("--json", metavar="PATH", help="Transfer summary JSON path")
    p.add_argument(
        "--expected-count", type=int, metavar="N",
        help="Expected number of data rows in the CSV",
    )
    p.add_argument(
        "--sample", type=int, metavar="N", default=0,
        help="Number of rows to sample for S3 HeadObject check (requires boto3)",
    )
    p.add_argument(
        "--bucket", default=_cfg.DEFAULT_BUCKET,
        help="S3 bucket for HeadObject checks",
    )
    p.add_argument(
        "--endpoint", default=_cfg.DEFAULT_ENDPOINT,
        help="S3 endpoint URL for HeadObject checks",
    )
    p.add_argument(
        "--check-keys", action="store_true",
        help="Validate s3_key composition rule for a sample of rows",
    )
    p.add_argument("--fs-prefix", default="", help="fs-prefix used during transfer")
    p.add_argument("--prefix", default="", help="S3 key prefix used during transfer")
    p.add_argument(
        "--key-sample", type=int, default=200, metavar="N",
        help="Number of rows to sample for key composition check",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.csv and not args.json:
        parser.print_help()
        return 1

    all_errors: list[str] = []
    all_warnings: list[str] = []

    # JSON summary validation
    if args.json:
        print(f"[validate] JSON summary: {args.json}")
        errs = validate_json_summary(pathlib.Path(args.json))
        all_errors.extend(errs)
        if not errs:
            print("  [ok] JSON summary structure valid.")
        else:
            for e in errs:
                print(f"  [FAIL] {e}")

    # CSV validation
    if args.csv:
        print(f"[validate] CSV report: {args.csv}")
        try:
            rows = validate_csv_structure(pathlib.Path(args.csv))
        except ValidationError as exc:
            print(f"  [FAIL] {exc}")
            return 1

        print(f"  [ok] CSV structure valid. {len(rows)} data rows.")

        # Row count
        errs = validate_row_count(rows, args.expected_count)
        for e in errs:
            print(f"  [FAIL] {e}")
        all_errors.extend(errs)

        # Status values
        errs = validate_status_values(rows)
        if errs:
            for e in errs[:20]:
                print(f"  [FAIL] {e}")
            if len(errs) > 20:
                print(f"  ... and {len(errs)-20} more status errors.")
        else:
            print("  [ok] All status values are valid enum members.")
        all_errors.extend(errs)

        # Key composition
        if args.check_keys:
            errs = validate_key_composition(
                rows, args.fs_prefix, args.prefix, args.key_sample
            )
            if errs:
                for e in errs[:10]:
                    print(f"  [FAIL] {e}")
                if len(errs) > 10:
                    print(f"  ... and {len(errs)-10} more key errors.")
            else:
                print(f"  [ok] s3_key composition correct for {args.key_sample}-row sample.")
            all_errors.extend(errs)

        # S3 HeadObject
        if args.sample > 0:
            print(f"  [s3] Sampling {args.sample} rows for HeadObject...")
            errs = validate_s3_objects(rows, args.bucket, args.endpoint, args.sample)
            for e in errs:
                if e.startswith("[skip]"):
                    all_warnings.append(e)
                    print(f"  {e}")
                else:
                    all_errors.append(e)
                    print(f"  [FAIL] {e}")
            if not errs:
                print(f"  [ok] HeadObject sample passed for {args.sample} rows.")

    # Summary
    print()
    if all_warnings:
        for w in all_warnings:
            print(f"[warn] {w}")
    if all_errors:
        print(f"[RESULT] FAILED — {len(all_errors)} error(s).")
        return 1
    print("[RESULT] PASSED — all checks OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
