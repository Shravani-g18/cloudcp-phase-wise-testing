"""RT-04: Nested directory tree (5 levels deep)."""
import live_common as lc

CASE_ID = "RT-04"
DESCRIPTION = "Nested directory tree, 5 levels (upload)"
STEPS = [
    "Cleanup RT-04 source dir and S3 prefix",
    "datagen 100 files spread across 5 levels of subdirectories",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report",
    "Assert full relative path preserved and S3 path mirrors directory structure",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 100


def _extra(entries, report_rows, summary, parsed):
    """entries[i][0] is the relpath from the source root, e.g. 'a/b/c/file.dat' -
    confirm the report's local_path preserves that full nested path, not just
    the basename (i.e. intermediate directories weren't collapsed)."""
    nested_entries = [e for e in entries if "/" in e[0]]
    report_by_name = {r.get("local_path", "").rsplit("/", 1)[-1]: r for r in report_rows}
    mismatches = []
    for relpath, _size in nested_entries:
        name = relpath.rsplit("/", 1)[-1]
        row = report_by_name.get(name)
        if row is None or not row.get("local_path", "").endswith(relpath):
            mismatches.append(relpath)
    return (bool(nested_entries) and not mismatches), {
        "nested_entries_checked": len(nested_entries),
        "path_mismatches": mismatches,
    }


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, "RT-04_nested_dirs.yaml", EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
