from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple


BYTES_TOLERANCE = 1 * 1024 * 1024


@dataclass
class TierTotals:
    batch_count: int = 0
    file_count: int = 0
    total_bytes: int = 0


def iter_dataset_records(lines: Iterable[str]) -> Iterator[Tuple[int, str]]:
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid data row (missing comma): {line}")
        yield int(parts[0].strip()), parts[1].strip()


def build_tier_thresholds(config_tiers: List[Dict[str, object]]) -> List[Tuple[str, int]]:
    tiers: List[Tuple[str, int]] = []
    for item in config_tiers:
        name = str(item.get("name", "")).strip()
        max_bytes = int(item.get("max_bytes", -1))
        if not name or max_bytes < 0:
            raise ValueError(f"Invalid tier config entry: {item}")
        tiers.append((name, max_bytes))
    tiers.sort(key=lambda x: x[1])
    return tiers


def classify_tier(size_bytes: int, tier_thresholds: List[Tuple[str, int]], overflow_tier: str) -> str:
    for tier_name, limit in tier_thresholds:
        if size_bytes < limit:
            return tier_name
    return overflow_tier


def summarize_dataset_file(
    dataset_path: Path,
    tier_thresholds: List[Tuple[str, int]],
    overflow_tier: str,
) -> Tuple[Dict[str, TierTotals], int]:
    all_tiers = [name for name, _ in tier_thresholds] + [overflow_tier]
    summary = {tier: TierTotals() for tier in all_tiers}
    record_count = 0

    with dataset_path.open("r", encoding="utf-8") as handle:
        for size, _ in iter_dataset_records(handle):
            tier = classify_tier(size, tier_thresholds, overflow_tier)
            summary[tier].file_count += 1
            summary[tier].total_bytes += size
            record_count += 1

    return summary, record_count


def parse_batch_summary_csv(path: Path) -> Tuple[Dict[str, TierTotals], TierTotals]:
    tier_map: Dict[str, TierTotals] = {}
    total_row = TierTotals()

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])

        # Legacy format: tier,batch_count,file_count,total_bytes,total_size_MB
        legacy_required = {"tier", "batch_count", "file_count", "total_bytes", "total_size_MB"}
        if legacy_required.issubset(fieldnames):
            for row in reader:
                tier = (row.get("tier") or "").strip()
                parsed = TierTotals(
                    batch_count=int(row["batch_count"]),
                    file_count=int(row["file_count"]),
                    total_bytes=int(row["total_bytes"]),
                )
                if tier == "TOTAL":
                    total_row = parsed
                else:
                    tier_map[tier] = parsed
            return tier_map, total_row

        # New format observed on destination hosts:
        # batch_id,bucket,file_count,total_size_MB
        new_required = {"batch_id", "bucket", "file_count", "total_size_MB"}
        if new_required.issubset(fieldnames):
            for row in reader:
                tier = (row.get("bucket") or "").strip()
                if not tier:
                    continue

                if tier not in tier_map:
                    tier_map[tier] = TierTotals()

                file_count = int(row.get("file_count") or 0)
                total_size_mb = float(row.get("total_size_MB") or 0.0)
                total_bytes = int(round(total_size_mb * 1024 * 1024))

                tier_map[tier].batch_count += 1
                tier_map[tier].file_count += file_count
                tier_map[tier].total_bytes += total_bytes

            total_row = TierTotals(
                batch_count=sum(v.batch_count for v in tier_map.values()),
                file_count=sum(v.file_count for v in tier_map.values()),
                total_bytes=sum(v.total_bytes for v in tier_map.values()),
            )
            return tier_map, total_row

        raise ValueError(
            "batch summary file missing recognized columns; "
            f"got={sorted(fieldnames)}"
        )

    return tier_map, total_row


def compare_summaries(
    expected: Dict[str, TierTotals],
    actual: Dict[str, TierTotals],
    total_row: TierTotals,
) -> List[str]:
    issues: List[str] = []
    all_tiers = sorted(set(expected.keys()) | set(actual.keys()))

    expected_total_files = 0
    expected_total_bytes = 0
    actual_total_batches = 0

    for tier in all_tiers:
        e = expected.get(tier, TierTotals())
        a = actual.get(tier, TierTotals())

        expected_total_files += e.file_count
        expected_total_bytes += e.total_bytes
        actual_total_batches += a.batch_count

        if e.file_count != a.file_count:
            issues.append(f"Tier {tier}: file_count mismatch expected={e.file_count} actual={a.file_count}")
        if abs(e.total_bytes - a.total_bytes) > BYTES_TOLERANCE:
            issues.append(f"Tier {tier}: total_bytes mismatch expected={e.total_bytes} actual={a.total_bytes}")

    if total_row.file_count != expected_total_files:
        issues.append(f"TOTAL file_count mismatch expected={expected_total_files} actual={total_row.file_count}")
    if abs(total_row.total_bytes - expected_total_bytes) > BYTES_TOLERANCE:
        issues.append(f"TOTAL total_bytes mismatch expected={expected_total_bytes} actual={total_row.total_bytes}")
    if total_row.batch_count != actual_total_batches:
        issues.append(f"TOTAL batch_count mismatch expected={actual_total_batches} actual={total_row.batch_count}")

    return issues


# ---------------------------------------------------------------------------
# Batch parameter constraint validation (requires per-batch CSV format)
# ---------------------------------------------------------------------------

def parse_batch_detail_rows(path: Path) -> List[Dict[str, str]]:
    """
    Return per-batch rows from a batch_summary.csv (new format only).

    The new format has columns: batch_id, bucket, file_count, total_size_MB
    The legacy aggregated format (tier, batch_count, file_count, total_bytes, total_size_MB)
    cannot provide per-batch data; an empty list is returned for that case.
    """
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        required = {"batch_id", "bucket", "file_count", "total_size_MB"}
        if not required.issubset(fieldnames):
            return []
        return [dict(row) for row in reader]


def validate_batch_parameters(
    batch_summary_path: Path,
    batch_params: Dict[str, Dict[str, object]],
) -> List[str]:
    """
    Validate per-batch rows in the summary CSV against the configured BATCH_SIZE and
    TARGET_SIZE_MB limits.

    batch_params maps tier name (any case) to a dict that may contain:
      BATCH_SIZE     – max files allowed per batch  (0 or absent = no limit)
      TARGET_SIZE_MB – max total MB allowed per batch (0 or absent = no limit)

    OPEN_BATCHES is a runtime concurrency setting and cannot be validated from the
    static batch_summary.csv output.

    Returns a list of human-readable violation messages (empty list = all OK).
    If the CSV is in legacy aggregated format the list is empty (validation skipped).
    """
    rows = parse_batch_detail_rows(batch_summary_path)
    if not rows:
        return []

    normalized: Dict[str, Dict[str, object]] = {k.upper(): v for k, v in batch_params.items()}
    violations: List[str] = []

    for row in rows:
        batch_id = (row.get("batch_id") or "").strip()
        tier = (row.get("bucket") or "").strip().upper()
        try:
            file_count = int(row.get("file_count") or 0)
            total_size_mb = float(row.get("total_size_MB") or 0.0)
        except (ValueError, TypeError):
            continue

        params = normalized.get(tier)
        if params is None:
            continue

        batch_size_limit = int(params.get("BATCH_SIZE") or 0)
        target_size_mb = float(params.get("TARGET_SIZE_MB") or 0.0)

        if batch_size_limit > 0 and file_count > batch_size_limit:
            violations.append(
                f"BATCH_SIZE violation  tier={tier} batch={batch_id} "
                f"file_count={file_count} > configured limit={batch_size_limit}"
            )

        if target_size_mb > 0:
            tolerance = target_size_mb * 1.10
            if total_size_mb > tolerance:
                violations.append(
                    f"TARGET_SIZE_MB violation  tier={tier} batch={batch_id} "
                    f"total_size_MB={total_size_mb:.2f} > configured limit={target_size_mb} "
                    f"(with 10%% tolerance={tolerance:.2f})"
                )

    return violations
