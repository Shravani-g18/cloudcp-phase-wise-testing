"""Network profiles — the single resolver for profile-driven tuning (design §8).

The corpus is bimodal (small files bound by requests/sec, large files bound by
bandwidth). The *packaging* of batches is fixed; only *scheduling* — how many
worker slots pull each size tier — is tuned to the link in front of us. A
``NETWORK_PROFILE`` (top-level key) selects one entry from ``NETWORK_PROFILES``
(default ``default_balanced``); the profile tunes worker count, per-tier
scheduling weights + concurrency caps, batch sizing, the rc==1 ProcessPool, and
multipart chunk size. **Static now**; NIC-speed auto-detect is future work.

This module is the one place that resolves the active profile so every consumer
agrees:

  * the broker/scheduler (Phase 2b) → ``max_workers``, ``weight``/``max_concurrent``
  * ``mp_batch_retry`` (§12.1)      → ``rc1_retry``
  * ``BatchBuilder`` (via the enumerator) → per-tier ``batch_size`` /
    ``target_size_mb`` / ``open_batches``, applied through ``overlay_batch_config``

**Precedence (highest first): explicit flat config key > active profile value >
built-in default.** So an operator can still pin a single knob with a flat key
without editing the profile, and a profile that omits a knob falls back to the
BatchBuilder/scheduler defaults (§21.2).
"""

DEFAULT_PROFILE = "default_balanced"

# Tiers the scheduler reasons about (BatchBuilder also emits a ``zero`` tier for
# empty files; profiles rarely mention it, so it falls back to the defaults).
SCHED_TIERS = ("large", "medium", "small", "tiny", "zero")

# lowercase tier -> flat-key stem used by BatchBuilder / config (§6.1, config.py).
_TIER_FLAT = {"zero": "ZERO", "tiny": "TINY", "small": "SMALL",
              "medium": "MEDIUM", "large": "LARGE"}

# Built-in starter profiles (design §8 / §21.2). Indicative numbers — tunable
# via config's NETWORK_PROFILES (a user block deep-merges over the built-in of
# the same name).
BUILTIN_PROFILES = {
    "dt2_100gbe": {
        "max_workers": 32,
        "tiers": {
            "large":  {"weight": 6, "max_concurrent": 16, "batch_size": 5,   "target_size_mb": 51200, "open_batches": 8},
            "medium": {"weight": 4, "max_concurrent": 12, "batch_size": 50,  "target_size_mb": 10240, "open_batches": 8},
            "small":  {"weight": 3, "max_concurrent": 8,  "batch_size": 317, "target_size_mb": 2048,  "open_batches": 8},
            "tiny":   {"weight": 3, "max_concurrent": 8,  "batch_size": 511, "target_size_mb": 256,   "open_batches": 8},
        },
        "rc1_retry": {"processes": 4, "threads_per_process": 16},
        "multipart_chunksize_mb": 64,
    },
    "low_bandwidth": {
        "max_workers": 4,
        "tiers": {
            "large":  {"weight": 3, "max_concurrent": 1},
            "medium": {"weight": 3, "max_concurrent": 1},
            "small":  {"weight": 4, "max_concurrent": 2},
            "tiny":   {"weight": 6, "max_concurrent": 2},
        },
        "rc1_retry": {"processes": 2, "threads_per_process": 8},
        "multipart_chunksize_mb": 16,
    },
    "default_balanced": {
        "max_workers": 16,
        "tiers": {
            "large":  {"weight": 3}, "medium": {"weight": 3},
            "small":  {"weight": 2}, "tiny":   {"weight": 1},
        },
    },
}

# Scheduling defaults per tier when neither a flat key nor the profile sets them.
# ``unknown`` is a legacy flat batch (pre-tier-upgrade); it is scheduled at the
# medium weight per the design §5 reconciliation note.
_SCHED_DEFAULTS = {
    "large":   {"weight": 3, "max_concurrent": 8},
    "medium":  {"weight": 3, "max_concurrent": 8},
    "small":   {"weight": 2, "max_concurrent": 8},
    "tiny":    {"weight": 1, "max_concurrent": 8},
    "zero":    {"weight": 1, "max_concurrent": 4},
    "unknown": {"weight": 3, "max_concurrent": 8},
}

_DEFAULT_MAX_WORKERS = 16
_DEFAULT_RC1_PROCESSES = 2
_DEFAULT_RC1_THREADS = 8
_DEFAULT_MULTIPART_CHUNK_MB = 64


def _deep_merge(base, over):
    """Return ``base`` deep-merged with ``over`` (over wins). Inputs untouched."""
    out = dict(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def active_profile_name(config):
    """Name of the selected profile (``NETWORK_PROFILE`` or the default)."""
    name = (config or {}).get("NETWORK_PROFILE")
    return name if name else DEFAULT_PROFILE


def _int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class NetworkProfile:
    """Resolved view of the active profile with flat-key overrides applied."""

    def __init__(self, config=None):
        self.config = config or {}
        self.name = active_profile_name(self.config)
        # User NETWORK_PROFILES[name] deep-merges over the built-in of that name
        # (so a user can tweak one knob without restating the whole profile).
        builtin = BUILTIN_PROFILES.get(self.name, {})
        user_all = self.config.get("NETWORK_PROFILES") or {}
        user = user_all.get(self.name) if isinstance(user_all, dict) else None
        self._p = _deep_merge(builtin, user) if isinstance(user, dict) else dict(builtin)
        self._tiers = self._p.get("tiers", {}) if isinstance(self._p.get("tiers"), dict) else {}

    # --- scheduler knobs ---------------------------------------------------
    @property
    def max_workers(self):
        """Global cap on concurrent cloudcp/aws_transfer processes.

        flat ``MAX_WORKERS`` > profile ``max_workers`` > legacy ``AWS_THREAD`` /
        ``PARALLEL_WORKERS`` > default 16.
        """
        if "MAX_WORKERS" in self.config:
            return max(1, _int(self.config["MAX_WORKERS"], _DEFAULT_MAX_WORKERS))
        if "max_workers" in self._p:
            return max(1, _int(self._p["max_workers"], _DEFAULT_MAX_WORKERS))
        for legacy in ("AWS_THREAD", "PARALLEL_WORKERS"):
            if legacy in self.config:
                return max(1, _int(self.config[legacy], _DEFAULT_MAX_WORKERS))
        return _DEFAULT_MAX_WORKERS

    def _tier_knob(self, tier, knob, flat_key, default):
        # explicit flat key wins, then profile tier value, then default.
        if flat_key and flat_key in self.config:
            return _int(self.config[flat_key], default)
        t = self._tiers.get(tier)
        if isinstance(t, dict) and knob in t:
            return _int(t[knob], default)
        return default

    def weight(self, tier):
        """Relative worker-slot share for a tier when all tiers have work."""
        d = _SCHED_DEFAULTS.get(tier, {"weight": 1})["weight"]
        return max(0, self._tier_knob(tier, "weight",
                                      "{}FILE_WEIGHT".format(_TIER_FLAT.get(tier, "")), d))

    def max_concurrent(self, tier):
        """Hard cap on inflight batches of a tier (defaults to ``max_workers``)."""
        d = _SCHED_DEFAULTS.get(tier, {}).get("max_concurrent", self.max_workers)
        return max(1, self._tier_knob(tier, "max_concurrent",
                                      "{}FILE_MAX_CONCURRENT".format(_TIER_FLAT.get(tier, "")), d))

    # --- rc==1 ProcessPool + multipart ------------------------------------
    def rc1_retry(self):
        """(processes, threads_per_process) for the rc==1 whole-batch retry."""
        rc1 = self._p.get("rc1_retry") if isinstance(self._p.get("rc1_retry"), dict) else {}
        processes = self.config.get("RC1_RETRY_PROCESSES", rc1.get("processes"))
        threads = self.config.get("RC1_RETRY_THREADS_PER_PROCESS", rc1.get("threads_per_process"))
        return (max(1, _int(processes, _DEFAULT_RC1_PROCESSES)),
                max(1, _int(threads, _DEFAULT_RC1_THREADS)))

    @property
    def multipart_chunksize_mb(self):
        if "MULTIPART_CHUNKSIZE_MB" in self.config:
            return max(1, _int(self.config["MULTIPART_CHUNKSIZE_MB"], _DEFAULT_MULTIPART_CHUNK_MB))
        if "multipart_chunksize_mb" in self._p:
            return max(1, _int(self._p["multipart_chunksize_mb"], _DEFAULT_MULTIPART_CHUNK_MB))
        if "CHUNK_SIZE_MB" in self.config:
            return max(1, _int(self.config["CHUNK_SIZE_MB"], _DEFAULT_MULTIPART_CHUNK_MB))
        return _DEFAULT_MULTIPART_CHUNK_MB

    def scheduling_table(self):
        """Return ``{tier: {'weight', 'max_concurrent'}}`` for the scheduler."""
        return {t: {"weight": self.weight(t), "max_concurrent": self.max_concurrent(t)}
                for t in SCHED_TIERS}


def resolve(config=None):
    """Return the :class:`NetworkProfile` for the given (normalized) config."""
    return NetworkProfile(config)


def overlay_batch_config(config):
    """Return ``config`` augmented with profile-derived flat batch-sizing keys.

    The active profile's ``tiers.<tier>.{batch_size, target_size_mb,
    open_batches}`` are written as the flat keys BatchBuilder already reads
    (``TINYFILE_BATCH_SIZE`` …) **only where the operator has not set that flat
    key explicitly** — so precedence stays *flat key > profile > BatchBuilder
    default* and BatchBuilder itself needs no change. Returns a shallow copy;
    the input is not mutated.
    """
    cfg = dict(config or {})
    prof = NetworkProfile(cfg)
    tiers = prof._tiers
    knob_to_suffix = {"batch_size": "BATCH_SIZE",
                      "target_size_mb": "TARGET_SIZE_MB",
                      "open_batches": "OPEN_BATCHES"}
    for tier, stem in _TIER_FLAT.items():
        t = tiers.get(tier)
        if not isinstance(t, dict):
            continue
        for knob, suffix in knob_to_suffix.items():
            if knob not in t:
                continue
            flat_key = "{}FILE_{}".format(stem, suffix)
            if flat_key not in cfg:            # explicit flat key wins
                cfg[flat_key] = t[knob]
    return cfg
