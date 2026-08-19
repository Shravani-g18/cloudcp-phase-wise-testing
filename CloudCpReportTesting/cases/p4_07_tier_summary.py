"""P4-07: Per-tier completion summary."""
from report_engine import verify, write_final_report, src, row

CASE_ID = "P4-07"
DESCRIPTION = "Per-tier completion summary"
STEPS = [
    "Build a fixed per-tier file count: zero=20, tiny=40, small=30, medium=15, large=5 "
    "(110 files total), each tagged with its tier and a matching SUCCESS row",
    "Run verify(), write final_report.csv",
    "Re-count classified (non-EXTRA) results grouped by tier",
    "Compare recomputed per-tier counts against the original injected counts - must match",
    "Confirm the grand total across tiers equals 110",
]


def run(out_dir):
    tiers = {"zero": 20, "tiny": 40, "small": 30, "medium": 15, "large": 5}
    srcs, rows = [], []
    for tier, n in tiers.items():
        for i in range(n):
            rp = f"{tier}/file_{i}.bin"
            srcs.append(src(rp, 100, tier=tier))
            rows.append(row(rp, "SUCCESS", 100))
    results = verify(srcs, rows)
    path = write_final_report(results, out_dir)

    per_tier = {t: 0 for t in tiers}
    for r in results:
        if r["Status"] != "EXTRA":
            per_tier[r["tier"]] += 1
    sums_match = per_tier == tiers
    total_match = sum(per_tier.values()) == sum(tiers.values())
    passed = sums_match and total_match
    return passed, {"expected_per_tier": tiers, "actual_per_tier": per_tier,
                     "total_matches": total_match, "final_report": str(path)}


def check_live(source_entries, report_rows, results, out_dir):
    """Generic version: per-tier counts recomputed from the real results must
    match the counts implied by the real source.index, for whatever tiers exist."""
    path = write_final_report(results, out_dir)
    expected = {}
    for e in source_entries:
        t = e.get("tier", "unknown")
        expected[t] = expected.get(t, 0) + 1
    actual = {}
    for r in results:
        if r["Status"] != "EXTRA":
            actual[r["tier"]] = actual.get(r["tier"], 0) + 1
    sums_match = expected == actual
    passed = sums_match
    return passed, {"expected_per_tier": expected, "actual_per_tier": actual,
                     "sums_match": sums_match, "final_report": str(path)}
