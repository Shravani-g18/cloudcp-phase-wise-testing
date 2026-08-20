"""RT-02: Mixed-tier upload (tiny + small + medium)."""
import live_common as lc

CASE_ID = "RT-02"
DESCRIPTION = "Mixed tier upload (tiny/small/medium)"
STEPS = [
    "Cleanup RT-02 source dir and S3 prefix",
    "datagen 250 files across 3 tiers: 80 tiny, 120 small, 50 medium (~8GB)",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report",
    "Assert all RT-01 checks, plus: all 3 tiers present, byte totals reconcile",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 250


def _extra(entries, report_rows, summary, parsed):
    import report_engine as re_
    sizes_reported = [int(r.get("size", 0)) for r in report_rows]
    tiny = sum(1 for s in sizes_reported if s <= 1024 * 1024)
    small = sum(1 for s in sizes_reported if 1024 * 1024 < s <= 100 * 1024 * 1024)
    medium = sum(1 for s in sizes_reported if s > 100 * 1024 * 1024)
    all_tiers_present = tiny > 0 and small > 0 and medium > 0
    byte_total_report = sum(sizes_reported)
    byte_total_summary = re_.summary_int(summary, "Total size", "Bytes transferred (Aprox)")
    bytes_match = byte_total_summary is None or byte_total_summary == byte_total_report
    return (all_tiers_present and bytes_match), {
        "tiny_count": tiny, "small_count": small, "medium_count": medium,
        "all_tiers_present": all_tiers_present,
        "byte_total_report": byte_total_report,
        "byte_total_summary": byte_total_summary,
        "bytes_match": bytes_match,
    }


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, "RT-02_mixed_tiers.yaml", EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
