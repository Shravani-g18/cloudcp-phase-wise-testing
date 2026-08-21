"""RT-04: Nested directory tree, reusing the real CloudCpFallbackTesting
datagen spec (08_deep_tree.yaml, root: /bryck/cloudcp_fallback/deep_tree)
instead of a duplicate - fanout 2 x depth 12 = 4096 files, one per leaf
directory, 12 levels deep.
"""
import live_common as lc

CASE_ID = "RT-04"
DESCRIPTION = "Nested directory tree, 12 levels (upload)"
SPEC_REF = "../CloudCpFallbackTesting/spec_files/08_deep_tree.yaml"
STEPS = [
    "Cleanup RT-04 source dir and S3 prefix",
    "datagen (08_deep_tree.yaml): 4096 files, one per leaf dir, 12 levels deep",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report",
    "Assert full relative path preserved and S3 path mirrors directory structure",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 4096


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
    return lc.run_upload_case(ctx, CASE_ID, SPEC_REF, EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
