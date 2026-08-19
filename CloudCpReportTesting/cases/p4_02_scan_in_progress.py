"""P4-02: Refuse verify while scan in progress."""
from report_engine import verify, VerificationRefused, src, row

CASE_ID = "P4-02"
DESCRIPTION = "Refuse verify while scan in progress"
STEPS = [
    "Build one trivial source file + matching SUCCESS report row",
    "Call verify() with scan_state=in_progress -> expect VerificationRefused with the exact "
    "message 'scan_state=in_progress, cannot verify'",
    "Call verify() again with scan_state=complete on the same data -> expect success, file OK",
]


def run(out_dir):
    srcs = [src("a/f1.bin", 100)]
    rows = [row("a/f1.bin", "SUCCESS", 100)]
    refused = False
    try:
        verify(srcs, rows, scan_state="in_progress")
    except VerificationRefused as e:
        refused = str(e) == "scan_state=in_progress, cannot verify"
    proceeded = False
    try:
        results = verify(srcs, rows, scan_state="complete")
        proceeded = any(r["Status"] == "OK" for r in results)
    except VerificationRefused:
        proceeded = False
    passed = refused and proceeded
    return passed, {"refused_while_in_progress": refused, "proceeded_when_complete": proceeded}
