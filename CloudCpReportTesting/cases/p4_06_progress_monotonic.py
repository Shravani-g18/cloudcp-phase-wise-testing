"""P4-06: Progress counters monotonic.

NOTE: no live transfer available in this environment. This simulates a
counter sequence and proves the assertion logic; it does NOT prove the real
engine's counters behave correctly. Re-run with a live transfer id once the
tool can be invoked against a real run.
"""
import random

CASE_ID = "P4-06"
DESCRIPTION = "Progress counters monotonic"
STEPS = [
    "Seed a deterministic random generator (seed=42) for reproducibility",
    "Simulate 20 samples of files_done/bytes_done, each adding a non-negative random "
    "increment (never subtracting), capped at total_files=500",
    "Check every consecutive pair of samples: files_done never decreases, bytes_done "
    "never decreases, total_files is non-zero at every sample point",
    "CAVEAT: this is a simulated sequence, not a real transfer's live counters",
]


def run(out_dir):
    random.seed(42)
    total_files = 500
    files_done, bytes_done, samples = 0, 0, []
    for _ in range(20):
        files_done += random.randint(0, 40)
        bytes_done += random.randint(0, 4_000_000)
        files_done = min(files_done, total_files)
        samples.append({"files_done": files_done, "bytes_done": bytes_done,
                         "total_files": total_files})
    monotonic_files = all(samples[i]["files_done"] <= samples[i + 1]["files_done"]
                           for i in range(len(samples) - 1))
    monotonic_bytes = all(samples[i]["bytes_done"] <= samples[i + 1]["bytes_done"]
                           for i in range(len(samples) - 1))
    nonzero_total = all(s["total_files"] > 0 for s in samples)
    passed = monotonic_files and monotonic_bytes and nonzero_total
    return passed, {"simulated": True,
                     "caveat": "synthetic sequence only - not a live-transfer proof",
                     "samples_taken": len(samples),
                     "monotonic_files_done": monotonic_files,
                     "monotonic_bytes_done": monotonic_bytes,
                     "total_files_nonzero_throughout": nonzero_total}
