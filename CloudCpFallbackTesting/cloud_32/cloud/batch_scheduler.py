#!/usr/bin/env python3
"""Broker / scheduler — weighted work-stealing batch dispatch (design §9).

Replaces the ``bcloud_src_enum | parallel aws_transfer.py`` shell pipeline with a
long-lived Python controller. It:

  1. loads the active :mod:`net_profile` (max_workers, per-tier weight +
     max_concurrent);
  2. spawns the **enumerator** (which streams tier-tagged batches into
     ``batches/pending/<tier>/``);
  3. runs a scheduler loop: while a worker slot is free and some tier has pending
     work, pick the next tier by a **deficit-weighted, work-stealing** rule,
     claim one batch (``pending -> inprogress``), and spawn one ``aws_transfer.py``
     subprocess for it;
  4. reaps finished workers, decrements that tier's inflight, and (bounded)
     re-dispatches a batch whose worker *crashed* (non-zero exit);
  5. exits when the enumerator has finished AND no batch is pending AND no worker
     is inflight.

**Runs as its own subprocess** (this file's ``__main__``), so it is a drop-in for
the old pipeline command: ``aws.py`` still spawns the fallback worker, hands this
command to :func:`cloud_transfer.transfer` (which records the pid for pause/cancel
and, on exit, drives verification), then drops ``_fallback_done``. rc==2 batches
left ``inprogress`` are completed by the concurrently-running fallback worker; the
broker does not wait for that drain (``aws.py`` does, after this process exits).

Dispatch is **list-based** (``subprocess`` without a shell), so source paths with
spaces / quotes / odd bytes need no shell escaping — more robust than the old
GNU-parallel string pipeline. GNU parallel remains the default; this broker is
opted into via ``TRANSFER_DISPATCH=broker``.
"""

import argparse
import json
import os
import subprocess
import sys
import time

from bryckcloud.lib.libutils import logger
from bryckcloud.lib.cloud import batch_state
from bryckcloud.lib.cloud import net_profile
from bryckcloud.lib.cloud import upload_report
from bryckcloud.lib.config import CloudConfig


# Max times a batch whose worker crashed (non-zero exit) is re-dispatched before
# it is given up on (its files remain recorded as failures by the report).
MAX_CRASH_RETRIES = 3


class BatchScheduler:
    def __init__(self, transfer_id, transfer_type, src, dst, base_src,
                 transfer_dir, dir_path, endpoint_url=None, config=None,
                 poll_interval=0.5):
        self.transfer_id = transfer_id
        self.transfer_type = transfer_type
        self.src = src
        self.dst = dst
        self.base_src = base_src
        self.transfer_dir = transfer_dir          # <BATCH_FILE_DIR>/transfer_<id>
        self.dir_path = dir_path                   # dir holding the sibling scripts
        self.endpoint_url = endpoint_url or None
        self.config = config or {}
        self.poll_interval = poll_interval

        self.profile = net_profile.resolve(self.config)
        self.max_workers = self.profile.max_workers

        self._py = sys.executable or "python3"
        self._enum_script = os.path.join(dir_path, "bcloud_src_enum.py")
        self._xfer_script = os.path.join(dir_path, "aws_transfer.py")

        self.enum_proc = None
        self.inflight = {}          # tier -> count of running workers
        self.running = {}           # Popen -> (tier, name)
        self.dispatched = set()     # batch names already handed to a worker
        self.crash_counts = {}      # name -> re-dispatch count

    # -- enumerator ---------------------------------------------------------
    def _spawn_enumerator(self):
        cmd = [self._py, self._enum_script, "-i", str(self.transfer_id), self.src]
        logger.info("Broker {}: spawning enumerator".format(self.transfer_id))
        # stdout (the printed batch paths) is unused — we discover batches by
        # listing the pending tier dirs. stderr is kept for diagnostics.
        self.enum_proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def _enum_finished(self):
        """True once the enumerator process has exited (complete or failed)."""
        return self.enum_proc is not None and self.enum_proc.poll() is not None

    def _scan_complete(self):
        try:
            with open(os.path.join(self.transfer_dir, "manifest.json")) as f:
                return json.load(f).get("scan_state") == "complete"
        except (OSError, ValueError):
            return False

    # -- pending discovery + tier selection ---------------------------------
    def _pending_by_tier(self):
        """{tier: [(name, path), ...]} for pending batches not yet dispatched."""
        pend = {}
        for state in (batch_state.PENDING,):
            for tier, name, path in batch_state._iter_state(self.transfer_dir, state):
                if name in self.dispatched:
                    continue
                pend.setdefault(tier, []).append((name, path))
        return pend

    def _select_tier(self, pend):
        """Deficit-weighted work-stealing pick (design §9.1).

        Prefer tiers with work under their ``max_concurrent`` cap, picking the
        largest ``weight / (inflight + 1)``. If all such tiers are capped but a
        worker slot is free, steal it for the best-weighted pending tier so
        workers never idle.
        """
        with_work = [t for t, items in pend.items() if items]
        if not with_work:
            return None
        under_cap = [t for t in with_work
                     if self.inflight.get(t, 0) < self.profile.max_concurrent(t)]
        pool = under_cap or with_work   # steal idle slots when all tiers capped
        return max(pool,
                   key=lambda t: self.profile.weight(t) / (self.inflight.get(t, 0) + 1))

    # -- dispatch + reap ----------------------------------------------------
    def _aws_transfer_argv(self, batch_path):
        # Exact arg layout aws_transfer.py expects (see its main()/batch_transfer):
        #   <id> <batch_path> <dst> <base_src> --expected-size <n> [--endpoint-url <url>]
        argv = [self._py, self._xfer_script, str(self.transfer_id), batch_path,
                self.dst, self.base_src, "--expected-size", "54760833024"]
        if self.endpoint_url:
            argv += ["--endpoint-url", self.endpoint_url]
        return argv

    def _dispatch(self, tier, name):
        """Claim one batch (pending/inprogress) and spawn a worker for it."""
        tier_arg = None if tier == batch_state.UNKNOWN_TIER else tier
        claimed = batch_state.claim(self.transfer_dir, name, tier=tier_arg)
        self.dispatched.add(name)
        if claimed is None:
            return  # already completed in a prior run (resume dedup)
        proc = subprocess.Popen(self._aws_transfer_argv(claimed),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.running[proc] = (tier, name)
        self.inflight[tier] = self.inflight.get(tier, 0) + 1

    def _reap(self):
        """Collect finished workers; re-dispatch (bounded) on a worker crash."""
        for proc in list(self.running.keys()):
            rc = proc.poll()
            if rc is None:
                continue
            tier, name = self.running.pop(proc)
            self.inflight[tier] = max(0, self.inflight.get(tier, 0) - 1)
            if rc != 0:
                # aws_transfer itself crashed (Python error) — the batch is still
                # inprogress. cloudcp exit codes 1/2 are handled *inside*
                # aws_transfer and still exit 0, so a non-zero exit is a genuine
                # worker failure worth retrying a bounded number of times.
                n = self.crash_counts.get(name, 0) + 1
                self.crash_counts[name] = n
                if n <= MAX_CRASH_RETRIES:
                    logger.warning("Broker {}: worker for batch {} exited rc={} — "
                                   "re-dispatching (attempt {})".format(
                                       self.transfer_id, name, rc, n))
                    self.dispatched.discard(name)
                    self._dispatch(tier, name)  # claim re-claims the inprogress batch
                else:
                    logger.error("Broker {}: batch {} failed {} times — giving up "
                                 "(failures recorded in report)".format(
                                     self.transfer_id, name, n))


    # -- resume reconcile ---------------------------------------------------
    def _reset_stale_inprogress(self):
        """On resume, re-home batches stuck in ``inprogress/``.

        If a batch's cloudcp retry ``.lst`` is still on disk, cloudcp finished the
        bulk and the fallback owns the failed subset -> leave it inprogress. If no
        ``.lst`` exists (cloudcp killed/crashed before flushing), move it back to
        pending so the scheduler re-dispatches it (cloudcp's SKIP_EXISTING probe
        skips already-uploaded files).
        """
        # Derive the retry-list path from the batch NAME exactly as aws_transfer
        # does (name -> stem -> retry_list_path), so the "has a pending .lst?"
        # check matches and a .lst-backed batch is never requeued to cloudcp.
        reset = kept = 0
        for tier, name, _path in list(
                batch_state._iter_state(self.transfer_dir, batch_state.INPROGRESS)):
            batch_stem = name[:-4] if name.endswith(".txt") else name
            lst_path = upload_report.retry_list_path(
                self.transfer_id, batch_stem, self.config)
            if os.path.exists(lst_path):
                kept += 1
                continue
            tier_arg = None if tier == batch_state.UNKNOWN_TIER else tier
            batch_state.requeue(self.transfer_dir, name, tier=tier_arg)
            reset += 1

        if reset or kept:
            logger.info("Broker {}: resume reconcile — reset {} stale inprogress "
                        "batch(es) to pending, left {} for the fallback (pending "
                        ".lst)".format(self.transfer_id, reset, kept))


    # -- main loop ----------------------------------------------------------
    def run(self):
        logger.info("Broker {}: profile={} max_workers={}".format(
            self.transfer_id, self.profile.name, self.max_workers))
        # Resume reconcile: recover batches stranded in inprogress/ (no .lst)
        # BEFORE spawning the enumerator or dispatching anything.
        self._reset_stale_inprogress()
        self._spawn_enumerator()

        while True:
            self._reap()
            pend = self._pending_by_tier()
            free = self.max_workers - sum(self.inflight.values())
            while free > 0:
                logger.debug("Pending-{}: {}".format(self.transfer_id, {t: len(items) for t, items in pend.items()}))
                tier = self._select_tier(pend)
                if tier is None:
                    break
                name, _path = pend[tier].pop(0)
                self._dispatch(tier, name)
                free -= 1
                logger.debug(f"Running with workers : {self.inflight}")

            # Terminate only when the enumerator has exited AND nothing is
            # pending AND no worker is inflight. Re-check pending after draining
            # so a batch published in the enumerator's final flush is not missed.
            if self._enum_finished() and not self.running:
                if not any(items for items in self._pending_by_tier().values()):
                    break

            time.sleep(self.poll_interval)

        enum_rc = self.enum_proc.returncode if self.enum_proc else 0
        if enum_rc:
            try:
                err = self.enum_proc.stderr.read().decode("utf-8", "replace")[-2000:]
            except Exception:
                err = ""
            logger.debug("Broker {}: enumerator exited rc={} scan_complete={} err={}".format(
                self.transfer_id, enum_rc, self._scan_complete(), err.strip()))
            return enum_rc

        logger.debug("Broker {}: all batches dispatched and drained, crash_counts={}".format(self.transfer_id, self.crash_counts))
        return len(self.crash_counts)


def _build_parser():
    p = argparse.ArgumentParser(description="Bryckcloud batch broker/scheduler")
    p.add_argument("transfer_id", type=int)
    p.add_argument("transfer_type", choices=["upload", "download"])
    p.add_argument("src")
    p.add_argument("dst")
    p.add_argument("base_src")
    p.add_argument("--transfer-dir", required=True,
                   help="<BATCH_FILE_DIR>/transfer_<id> (batch-state root)")
    p.add_argument("--dir-path", required=True,
                   help="Directory holding bcloud_src_enum.py / aws_transfer.py")
    p.add_argument("--endpoint-url", default=None)
    p.add_argument("--poll-interval", type=float, default=0.5)
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)
    config = CloudConfig().bcloud
    sched = BatchScheduler(
        args.transfer_id, args.transfer_type, args.src, args.dst, args.base_src,
        args.transfer_dir, args.dir_path, endpoint_url=args.endpoint_url,
        config=config, poll_interval=args.poll_interval)
    try:
        return sched.run()
    except Exception as e:
        logger.error("Broker {} crashed: {}".format(args.transfer_id, e))
        return 1


if __name__ == "__main__":
    sys.exit(main())

