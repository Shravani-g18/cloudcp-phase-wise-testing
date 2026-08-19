"""P4-03: Paused transfer does not trigger verify."""
from report_engine import verify, VerificationRefused, src, row

CASE_ID = "P4-03"
DESCRIPTION = "Paused transfer does not trigger verify"
STEPS = [
    "Build one trivial source file + matching SUCCESS report row",
    "Call verify() with pause_requested=True -> expect refusal, message contains 'pause_requested'",
    "Call verify() again with pause_requested=False -> expect success, file classified OK",
]


def run(out_dir):
    srcs = [src("a/f1.bin", 100)]
    rows = [row("a/f1.bin", "SUCCESS", 100)]
    blocked = False
    try:
        verify(srcs, rows, pause_requested=True)
    except VerificationRefused as e:
        blocked = "pause_requested" in str(e)
    resumed = False
    try:
        results = verify(srcs, rows, pause_requested=False)
        resumed = any(r["Status"] == "OK" for r in results)
    except VerificationRefused:
        resumed = False
    passed = blocked and resumed
    return passed, {"blocked_while_paused": blocked, "proceeded_after_resume": resumed}
