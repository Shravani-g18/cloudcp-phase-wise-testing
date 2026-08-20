import os
from collections import deque

from bryckcloud.lib.libutils import logger

DEFAULT_OUTPUT_DIR = "output"

# Default batch tuning — overridable via config.json
DEFAULTS = {
    "zero":   {"max_files": 2000, "target_size_mb": 0,      "open_batches": 4, "threshold_mb": 0},
    "tiny":   {"max_files": 511,  "target_size_mb": 256,     "open_batches": 8, "threshold_mb": 1},
    "small":  {"max_files": 317,  "target_size_mb": 2048,    "open_batches": 8, "threshold_mb": 64},
    "medium": {"max_files": 50,   "target_size_mb": 10240,   "open_batches": 8, "threshold_mb": 1024},
    "large":  {"max_files": 5,    "target_size_mb": 51200,   "open_batches": 8, "threshold_mb": 0},
}

# Config keys per bucket (looked up in config.json).
# Each param maps to (flat_key, nested_key).  The flat form (e.g.
# ZEROFILE_BATCH_SIZE) and the nested form BATCH.<TIER>.<KEY> from
# docs/config_reference.md are both honored; flat takes priority.
# Note: when invoked via bcloud_src_enum.py the config has already been run
# through _normalize_config (which promotes nested BATCH.* -> flat), so the
# nested fallback here mainly makes BatchBuilder self-contained for callers
# that pass a raw, un-normalized config dict.
_CONFIG_KEYS = {
    "zero":   {"batch_size": "ZEROFILE_BATCH_SIZE",   "target_size": "ZEROFILE_TARGET_SIZE_MB",   "open_batches": "ZEROFILE_OPEN_BATCHES"},
    "tiny":   {"batch_size": "TINYFILE_BATCH_SIZE",   "target_size": "TINYFILE_TARGET_SIZE_MB",   "open_batches": "TINYFILE_OPEN_BATCHES"},
    "small":  {"batch_size": "SMALLFILE_BATCH_SIZE",  "target_size": "SMALLFILE_TARGET_SIZE_MB",  "open_batches": "SMALLFILE_OPEN_BATCHES"},
    "medium": {"batch_size": "MEDIUMFILE_BATCH_SIZE", "target_size": "MEDIUMFILE_TARGET_SIZE_MB", "open_batches": "MEDIUMFILE_OPEN_BATCHES"},
    "large":  {"batch_size": "LARGEFILE_BATCH_SIZE",  "target_size": "LARGEFILE_TARGET_SIZE_MB",  "open_batches": "LARGEFILE_OPEN_BATCHES"},
}

# bucket name -> nested tier name under the "BATCH" section
_NESTED_TIER = {"zero": "ZERO", "tiny": "TINY", "small": "SMALL", "medium": "MEDIUM", "large": "LARGE"}
# param name -> nested key under BATCH.<TIER>
_NESTED_PARAM = {"batch_size": "BATCH_SIZE", "target_size": "TARGET_SIZE_MB", "open_batches": "OPEN_BATCHES"}


class FileEntry:
    def __init__(self, size, path):
        self.size = size
        self.path = path


class Batch:
    def __init__(self, bucket, batch_id):
        self.id = batch_id
        self.bucket = bucket
        self.files = []
        self.total_size = 0
        self.file_count = 0

    def add(self, file_entry):
        self.files.append(file_entry)
        self.total_size += file_entry.size
        self.file_count += 1


class Bucket:
    def __init__(self, name, target_size, max_files, open_batches, batch_id_start):
        self.name = name
        self.target_size = target_size
        self.max_files = max_files
        self.open_batches = [Batch(name, batch_id_start + i) for i in range(open_batches)]
        self._rr = 0


class BatchBuilder:

    def __init__(self, config=None, start_seq=0):
        """
        Args:
            start_seq: first batch id to allocate. On a resumed scan this is
                seeded from the manifest's ``seq_high_water`` so newly created
                batches never reuse an id already published to a batch-state
                directory (challenge #15). Defaults to 0 for a fresh scan.
            config: dict from config.json (optional). Per-bucket tunables can be
                supplied either as flat keys (below) or as nested keys under a
                "BATCH" section, e.g. BATCH.TINY.BATCH_SIZE. Flat keys win when
                both are present. (When called from bcloud_src_enum.py the
                config is already normalized so both forms resolve to flat.)

                Batch size (max files per batch):
                    ZEROFILE_BATCH_SIZE    (default: 2000)
                    TINYFILE_BATCH_SIZE    (default: 511)
                    SMALLFILE_BATCH_SIZE   (default: 317)
                    MEDIUMFILE_BATCH_SIZE  (default: 50)
                    LARGEFILE_BATCH_SIZE   (default: 5)

                Target batch size in MB:
                    ZEROFILE_TARGET_SIZE_MB    (default: 0)
                    TINYFILE_TARGET_SIZE_MB    (default: 256)
                    SMALLFILE_TARGET_SIZE_MB   (default: 2048)
                    MEDIUMFILE_TARGET_SIZE_MB  (default: 10240)
                    LARGEFILE_TARGET_SIZE_MB   (default: 51200)

                Open batches (round-robin concurrency):
                    ZEROFILE_OPEN_BATCHES    (default: 4)
                    TINYFILE_OPEN_BATCHES    (default: 8)
                    SMALLFILE_OPEN_BATCHES   (default: 8)
                    MEDIUMFILE_OPEN_BATCHES  (default: 8)
                    LARGEFILE_OPEN_BATCHES   (default: 8)
        """
        if config is None:
            config = {}

        self._batch_counter = start_seq
        self.total_bytes = 0

        def _get(bucket_name, param, default_val):
            """Read config with fallback to default.

            Priority: flat key (e.g. TINYFILE_BATCH_SIZE) > nested key
            (BATCH.<TIER>.<KEY>) > default.  Flat wins for backward
            compatibility with existing configs.
            """
            keys = _CONFIG_KEYS.get(bucket_name, {})
            key = keys.get(param)
            if key and key in config:
                return int(config[key])
            # Nested fallback: BATCH.<TIER>.<KEY>
            batch_section = config.get("BATCH")
            tier = _NESTED_TIER.get(bucket_name)
            nested_key = _NESTED_PARAM.get(param)
            if isinstance(batch_section, dict) and tier and nested_key:
                tier_section = batch_section.get(tier)
                if isinstance(tier_section, dict) and nested_key in tier_section:
                    return int(tier_section[nested_key])
            return default_val

        MB = 1024 * 1024

        self.buckets = [
            ("zero", 1),
            ("tiny", 1 * MB),
            ("small", 64 * MB),
            ("medium", 1024 * MB),
            ("large", float("inf"))
        ]

        self.bucket_map = {}
        for name, _ in self.buckets:
            d = DEFAULTS[name]
            max_files = _get(name, "batch_size", d["max_files"])
            target_mb = _get(name, "target_size", d["target_size_mb"])
            open_b = _get(name, "open_batches", d["open_batches"])
            self.bucket_map[name] = Bucket(
                name, target_mb * MB, max_files, open_b, self._alloc_ids(open_b)
            )

        self.ready_batches = deque()

    def _alloc_ids(self, count):
        start = self._batch_counter
        self._batch_counter += count
        return start

    def _new_batch(self, bucket_name):
        batch_id = self._batch_counter
        self._batch_counter += 1
        return Batch(bucket_name, batch_id)

    def classify_bucket(self, size):

        for name, limit in self.buckets:
            if size < limit:
                return name

        raise ValueError("classify_bucket: no bucket matched size {}".format(size))

    def choose_batch(self, bucket):
        idx = bucket._rr % len(bucket.open_batches)
        bucket._rr = idx + 1
        return bucket.open_batches[idx]

    def assign_file(self, file_entry):

        self.total_bytes += file_entry.size

        bucket_name = self.classify_bucket(file_entry.size)
        bucket = self.bucket_map[bucket_name]

        batch = self.choose_batch(bucket)

        if (batch.total_size + file_entry.size > bucket.target_size or
                batch.file_count >= bucket.max_files):

            self.ready_batches.append(batch)

            new_batch = self._new_batch(bucket.name)
            idx = bucket.open_batches.index(batch)
            bucket.open_batches[idx] = new_batch

            batch = new_batch

        batch.add(file_entry)

    def flush(self):

        for bucket in self.bucket_map.values():
            for batch in bucket.open_batches:
                if batch.file_count > 0:
                    self.ready_batches.append(batch)

    def flush_open(self):
        """Flush all non-empty open batches and replace them with fresh ones.

        Unlike :meth:`flush` (a terminal operation), this keeps the builder
        usable for more files. It is used for mid-scan resume checkpoints: a
        published batch must never be mutated afterwards, so each flushed open
        batch is replaced by a brand-new batch with a fresh id.
        """
        for bucket in self.bucket_map.values():
            for i, batch in enumerate(bucket.open_batches):
                if batch.file_count > 0:
                    self.ready_batches.append(batch)
                    bucket.open_batches[i] = self._new_batch(bucket.name)

    def get_ready_batches(self):

        while self.ready_batches:
            yield self.ready_batches.popleft()


# ---------------------------------
# Scanner Simulation
# ---------------------------------

def simulate_scanner(input_file, config=None):

    builder = BatchBuilder(config)

    with open(input_file, "r") as f:

        for line in f:

            line = line.strip()
            if not line:
                continue

            size_str, path = line.split(",", 1)

            size = int(size_str)

            entry = FileEntry(size, path)

            builder.assign_file(entry)

    builder.flush()

    return builder


# ---------------------------------
# Write batch files
# ---------------------------------

def write_batches(builder, output_dir=DEFAULT_OUTPUT_DIR):

    os.makedirs(output_dir, exist_ok=True)

    summary_path = os.path.join(output_dir, "batch_summary.txt")

    with open(summary_path, "w") as summary:

        summary.write("batch_id,bucket,file_count,total_size_MB\n")

        for batch in builder.get_ready_batches():

            batch_file = os.path.join(
                output_dir, f"batch_{batch.id:06d}.txt")

            # cloudcp consumes the batch as a NUL-framed stream of raw path
            # bytes (redesign §4): each record is terminated by a single NUL so
            # paths containing newlines, carriage returns or trailing spaces are
            # preserved verbatim. Write in binary with os.fsencode() so any
            # non-UTF8 (cp1252/surrogate) path bytes round-trip unchanged.
            with open(batch_file, "wb") as bf:

                for f in batch.files:
                    bf.write(os.fsencode(f.path) + b"\0")

            size_mb = batch.total_size / (1024 * 1024)

            summary.write(
                f"{batch.id},{batch.bucket},{batch.file_count},{size_mb:.2f}\n"
            )


# ---------------------------------
# Main
# ---------------------------------

if __name__ == "__main__":

    input_file = "bryck_file_list.csv"

    builder = simulate_scanner(input_file)

    write_batches(builder)

    print("Batch generation complete.")
