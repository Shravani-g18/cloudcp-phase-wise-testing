from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_remote_batchbuilder_validation import (  # noqa: E402
    TierTotals,
    classify_tier,
    compare_summaries,
    expected_tier_summary,
    iter_dataset_records,
    parse_elapsed_seconds,
    parse_batch_summary_csv,
    parse_dataset_lines,
    parse_gnu_time_metrics,
    summarize_dataset_file,
)


def test_parse_dataset_lines_ignores_comments_and_blank_lines():
    raw = [
        "# comment",
        "",
        "0,/zero/path",
        "1048575,/tiny/path",
    ]
    parsed = parse_dataset_lines(raw)
    assert parsed == [(0, "/zero/path"), (1048575, "/tiny/path")]


def test_iter_dataset_records_streams_rows():
    raw = [
        "# comment",
        "",
        "1,/one",
        "2,/two",
    ]
    rows = list(iter_dataset_records(raw))
    assert rows == [(1, "/one"), (2, "/two")]


def test_classify_tier_boundaries():
    assert classify_tier(0) == "zero"
    assert classify_tier(1) == "tiny"
    assert classify_tier(1024 * 1024 - 1) == "tiny"
    assert classify_tier(1024 * 1024) == "small"
    assert classify_tier(64 * 1024 * 1024 - 1) == "small"
    assert classify_tier(64 * 1024 * 1024) == "medium"
    assert classify_tier(1024 * 1024 * 1024 - 1) == "medium"
    assert classify_tier(1024 * 1024 * 1024) == "large"


def test_expected_tier_summary_counts_and_bytes():
    rows = [
        (0, "/z"),
        (1, "/t1"),
        (2, "/t2"),
        (1024 * 1024, "/s"),
        (64 * 1024 * 1024, "/m"),
        (1024 * 1024 * 1024, "/l"),
    ]
    summary = expected_tier_summary(rows)

    assert summary["zero"].file_count == 1
    assert summary["tiny"].file_count == 2
    assert summary["small"].file_count == 1
    assert summary["medium"].file_count == 1
    assert summary["large"].file_count == 1


def test_parse_batch_summary_csv_and_compare_ok(tmp_path: Path):
    csv_file = tmp_path / "batch_summary.csv"
    csv_file.write_text(
        "tier,batch_count,file_count,total_bytes,total_size_MB\n"
        "zero,1,1,0,0.00\n"
        "tiny,1,2,3,0.00\n"
        "small,1,1,1048576,1.00\n"
        "medium,1,1,67108864,64.00\n"
        "large,1,1,1073741824,1024.00\n"
        "TOTAL,5,6,1141899267,1089.00\n",
        encoding="utf-8",
    )

    expected = {
        "zero": TierTotals(file_count=1, total_bytes=0),
        "tiny": TierTotals(file_count=2, total_bytes=3),
        "small": TierTotals(file_count=1, total_bytes=1048576),
        "medium": TierTotals(file_count=1, total_bytes=67108864),
        "large": TierTotals(file_count=1, total_bytes=1073741824),
    }

    actual, total = parse_batch_summary_csv(csv_file)
    issues = compare_summaries(expected, actual, total)
    assert issues == []


def test_compare_summaries_detects_mismatch():
    # total_bytes differences must exceed BYTES_TOLERANCE (1 MB) to be flagged.
    _2MB = 2 * 1024 * 1024
    expected = {
        "zero": TierTotals(file_count=1, total_bytes=0),
        "tiny": TierTotals(file_count=1, total_bytes=0),
        "small": TierTotals(file_count=0, total_bytes=0),
        "medium": TierTotals(file_count=0, total_bytes=0),
        "large": TierTotals(file_count=0, total_bytes=0),
    }
    actual = {
        "zero": TierTotals(batch_count=1, file_count=1, total_bytes=0),
        "tiny": TierTotals(batch_count=1, file_count=2, total_bytes=_2MB),
        "small": TierTotals(batch_count=0, file_count=0, total_bytes=0),
        "medium": TierTotals(batch_count=0, file_count=0, total_bytes=0),
        "large": TierTotals(batch_count=0, file_count=0, total_bytes=0),
    }
    total = TierTotals(batch_count=2, file_count=3, total_bytes=_2MB)

    issues = compare_summaries(expected, actual, total)
    assert any("Tier tiny: file_count mismatch" in msg for msg in issues)
    assert any("Tier tiny: total_bytes mismatch" in msg for msg in issues)


def test_summarize_dataset_file(tmp_path: Path):
    dataset = tmp_path / "dataset.yaml"
    dataset.write_text(
        "# header\n"
        "0,/z\n"
        "1,/t\n"
        f"{1024 * 1024},/s\n"
        f"{64 * 1024 * 1024},/m\n"
        f"{1024 * 1024 * 1024},/l\n",
        encoding="utf-8",
    )

    summary, count = summarize_dataset_file(dataset)
    assert count == 5
    assert summary["zero"].file_count == 1
    assert summary["tiny"].file_count == 1
    assert summary["small"].file_count == 1
    assert summary["medium"].file_count == 1
    assert summary["large"].file_count == 1


def test_parse_elapsed_seconds_variants():
    assert parse_elapsed_seconds("0:03.50") == 3.5
    assert parse_elapsed_seconds("1:02") == 62.0
    assert parse_elapsed_seconds("1:00:00.25") == 3600.25


def test_parse_gnu_time_metrics_extracts_fields():
    stderr_text = (
        "User time (seconds): 11.23\n"
        "System time (seconds): 2.77\n"
        "Percent of CPU this job got: 89%\n"
        "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:15.10\n"
        "Maximum resident set size (kbytes): 204800\n"
    )

    metrics = parse_gnu_time_metrics(stderr_text)
    assert metrics["user_time_sec"] == 11.23
    assert metrics["system_time_sec"] == 2.77
    assert metrics["cpu_percent"] == 89
    assert metrics["elapsed_raw"] == "0:15.10"
    assert metrics["elapsed_sec"] == 15.1
    assert metrics["max_rss_kb"] == 204800
    assert metrics["max_rss_mb"] == 200.0
