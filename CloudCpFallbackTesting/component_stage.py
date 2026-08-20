#!/usr/bin/env python3
"""Target-side staging / driver helper for the component fallback tests.

This file is uploaded to the Bryck and executed with the bcloud venv
interpreter (``/opt/bryck/.venv/bryck/bin/python3``) so it can import the
installed ``bryckcloud`` modules that are under test. It performs the on-disk
staging every component consumes and drives the whole-batch retry directly.

It is intentionally dependency-light and reuses the target's *own* modules so
the NUL framing, the retry-list filename/location, and the S3 key composition
are byte-for-byte identical to what cloudcp / the pipeline produce:

  * ``batch_state``  — batch framing + tier-partitioned state tree
  * ``upload_report``— retry-list path + report reading
  * ``mp_batch_retry``— S3 key composition + the whole-batch retry entrypoint
  * ``config.CloudConfig`` — the live ``config.json`` (``local_aws``)

Subcommands (all print a single JSON object on stdout on success):

  alloc-id     --batchmeta DIR
  stage-batch  --src DIR --transfer-dir DIR --tier TIER --name NAME
  make-lst     --batch PATH --transfer-id N --bucket B --prefix P --fs-prefix FSP
  done-marker  --transfer-dir DIR
  run-mp       --transfer-id N --batch PATH --bucket B --prefix P --fs-prefix FSP
               --endpoint URL --region R [--transfer-type upload]
  verify       --transfer-id N
  batch-state  --transfer-dir DIR --name NAME
"""

import argparse
import json
import os
import sys

from bryckcloud.lib.cloud import batch_state
from bryckcloud.lib.cloud import upload_report
from bryckcloud.lib.cloud import mp_batch_retry
from bryckcloud.lib.config import CloudConfig


def _emit(obj):
    """Print one JSON object (the machine-readable result) on stdout."""
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _iter_files(root):
    """Yield every regular file path under *root* (no symlink follow)."""
    stack = [os.fsencode(root)]
    while stack:
        current = stack.pop()
        try:
            it = os.scandir(os.fsdecode(current))
        except OSError:
            continue
        with it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(os.fsencode(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        yield entry.path
                except OSError:
                    continue


def cmd_alloc_id(args):
    """Next transfer id = max(transfer_<N> under batchmeta) + 1 (>= 1)."""
    max_id = 0
    try:
        for name in os.listdir(args.batchmeta):
            if name.startswith("transfer_"):
                try:
                    max_id = max(max_id, int(name[len("transfer_"):]))
                except ValueError:
                    continue
    except OSError:
        pass
    _emit({"transfer_id": max_id + 1})


def cmd_stage_batch(args):
    """Write a NUL-framed batch into inprogress/<tier>/<name>; print file count."""
    batch_state.ensure_dirs(args.transfer_dir)
    paths = list(_iter_files(args.src))
    dest = batch_state.state_path(args.transfer_dir, batch_state.INPROGRESS,
                                  args.name, args.tier)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    tmp = dest + ".tmp"
    with open(tmp, "wb") as bf:
        for p in paths:
            bf.write(os.fsencode(p) + b"\0")
    os.replace(tmp, dest)
    _emit({"batch_file": dest, "count": len(paths)})


def cmd_make_lst(args):
    """Build the cloudcp retry .lst the fallback worker globs.

    Records are ``local_path \\0 s3path \\0 size \\0 last_error \\0`` with the
    S3 destination composed exactly like cloudcp (mp_batch_retry.compose_s3_key).
    """
    config = CloudConfig().bcloud
    prefix = args.prefix or ""
    records = batch_state.read_batch_records(args.batch)
    stem = os.path.splitext(os.path.basename(args.batch))[0]
    lst_path = upload_report.retry_list_path(args.transfer_id, stem, config)
    os.makedirs(os.path.dirname(lst_path), exist_ok=True)
    count = 0
    tmp = lst_path + ".tmp"
    with open(tmp, "w", newline="", errors="surrogateescape") as f:
        for abspath, size in records:
            if size is None:
                try:
                    size = os.path.getsize(abspath)
                except OSError:
                    size = 0
            key = mp_batch_retry.clean_s3_key(
                mp_batch_retry.compose_s3_key(abspath, args.fs_prefix, prefix))
            s3path = "s3://{}/{}".format(args.bucket, key)
            f.write(abspath + "\0" + s3path + "\0" + str(size) + "\0" + "\0")
            count += 1
    os.replace(tmp, lst_path)
    _emit({"lst_path": lst_path, "count": count})


def cmd_done_marker(args):
    """Write the _fallback_done marker that lets the worker exit once drained."""
    marker = os.path.join(args.transfer_dir, "_fallback_done")
    with open(marker, "w") as f:
        f.write("done\n")
    _emit({"marker": marker})


def cmd_run_mp(args):
    """Call mp_batch_retry.retry_whole_batch and print (ok, failed, ok_bytes)."""
    config = CloudConfig().bcloud
    prefix = None if (args.prefix is None or args.prefix == "") else args.prefix
    ok, failed, ok_bytes = mp_batch_retry.retry_whole_batch(
        args.transfer_id, args.transfer_type, args.batch, args.bucket, prefix,
        args.fs_prefix, args.endpoint, args.region, config, txlog=None)
    _emit({"ok": ok, "failed": failed, "ok_bytes": ok_bytes})


def cmd_verify(args):
    """Tally transfer_report_<id>.csv rows by status."""
    config = CloudConfig().bcloud
    by_status = {}
    total = 0
    for row in upload_report.iter_report_rows(args.transfer_id, config):
        st = row.get("status", "")
        by_status[st] = by_status.get(st, 0) + 1
        total += 1
    _emit({"total_rows": total, "by_status": by_status})


def cmd_batch_state(args):
    """Report state counts + whether <name> is now under completed/."""
    counts = batch_state.counts(args.transfer_dir)
    completed = any(name == args.name
                    for name, _ in batch_state.completed_batches(args.transfer_dir))
    _emit({"counts": counts, "completed": completed})


def build_parser():
    p = argparse.ArgumentParser(description="Component fallback staging/driver helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("alloc-id")
    a.add_argument("--batchmeta", required=True)
    a.set_defaults(func=cmd_alloc_id)

    a = sub.add_parser("stage-batch")
    a.add_argument("--src", required=True)
    a.add_argument("--transfer-dir", required=True)
    a.add_argument("--tier", required=True)
    a.add_argument("--name", required=True)
    a.set_defaults(func=cmd_stage_batch)

    a = sub.add_parser("make-lst")
    a.add_argument("--batch", required=True)
    a.add_argument("--transfer-id", type=int, required=True)
    a.add_argument("--bucket", required=True)
    a.add_argument("--prefix", default="")
    a.add_argument("--fs-prefix", required=True)
    a.set_defaults(func=cmd_make_lst)

    a = sub.add_parser("done-marker")
    a.add_argument("--transfer-dir", required=True)
    a.set_defaults(func=cmd_done_marker)

    a = sub.add_parser("run-mp")
    a.add_argument("--transfer-id", type=int, required=True)
    a.add_argument("--batch", required=True)
    a.add_argument("--bucket", required=True)
    a.add_argument("--prefix", default="")
    a.add_argument("--fs-prefix", required=True)
    a.add_argument("--endpoint", required=True)
    a.add_argument("--region", required=True)
    a.add_argument("--transfer-type", default="upload")
    a.set_defaults(func=cmd_run_mp)

    a = sub.add_parser("verify")
    a.add_argument("--transfer-id", type=int, required=True)
    a.set_defaults(func=cmd_verify)

    a = sub.add_parser("batch-state")
    a.add_argument("--transfer-dir", required=True)
    a.add_argument("--name", required=True)
    a.set_defaults(func=cmd_batch_state)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except Exception as e:  # surface a machine-readable error to the caller
        _emit({"error": "{}: {}".format(type(e).__name__, e)})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
