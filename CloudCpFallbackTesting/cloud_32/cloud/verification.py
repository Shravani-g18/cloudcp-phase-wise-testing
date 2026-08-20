from bryckcloud.lib.libutils import run_cmd, logger, zip_folder, human_readable, load_files, load_json_file
from bryckcloud.lib.bcloud_sql import cloud_db, get_cloudtransfer
from bryckcloud.lib.config import CloudConfig

import csv
import json
import os
import sys
from os import path
from shlex import quote
from pathlib import Path

# Use bryckck_lib directly when available — eliminates subprocess overhead for verification.
# The library is expected to be installed in the same venv (pip install bryckck_lib).
# Override via BRYCKCK_LIB_PATH env var if installed elsewhere.
try:
    from bryckck_lib import Verifier as _Verifier
    _BRYCKCK_LIB = True
except ImportError:
    _bryckck_path = os.environ.get('BRYCKCK_LIB_PATH', '')
    if _bryckck_path and _bryckck_path not in sys.path:
        sys.path.insert(0, _bryckck_path)
        try:
            from bryckck_lib import Verifier as _Verifier
            _BRYCKCK_LIB = True
        except ImportError:
            _BRYCKCK_LIB = False
    else:
        _BRYCKCK_LIB = False


class TransferVerification:
    def __init__(self, transfer_id, src, dst, cloud_type, tr_state):
        self.transfer_id = transfer_id
        self.src = src
        self.dst = dst
        self.cloud_type = cloud_type
        self.tr_state = tr_state
        self.cloud_logdir = self.get_cloud_logs_dir(transfer_id)
        self.src_size = 0
        self.dst_size = 0
        self.transferred_bytes = 0
        self.missing_count = 0
        self.mismatch_count = 0

    @staticmethod
    def get_cloud_logs_dir(transfer_id: str) -> str:
        """
        Get the cloud logs directory for a given transfer ID.

        Args:
            transfer_id (str): The ID of the cloud transfer.
        Returns:
            str: The path to the cloud logs directory for the specified transfer ID.
        """
        cloud_config = CloudConfig().bcloud
        return f"{cloud_config['LOGS_DIR']}/cloud_transfer_{transfer_id}"

    def write_summary(self, starttime, lastupdate, duration, state):
        log_path = Path(self.cloud_logdir)

        # 1. Data Retrieval
        summary = load_json_file(log_path / "summary.json")

        is_upload = self.dst.startswith("s3://")
        transfer_label = "Files transferred" if is_upload else "Objects transferred"
        src_summary_label = "Number of files" if is_upload else "Number of objects"
        dst_summary_label = "Number of objects" if is_upload else "Number of files"

        # 2. Start building report
        lines = [
            "Transfer Summary Report",
            "",
            f"{'Transfer start':<40}: {starttime}",
            f"{'Transfer completion':<40}: {lastupdate}",
            f"{'Transfer status':<40}: {state}",
            f"{'Source':<40}: {self.src}",
            f"{'Destination':<40}: {self.dst}",
            f"{'Transfer ID':<40}: {self.transfer_id}"
        ]

        # 3. Transfer Metrics (Skip if summary is None)
        if summary:
            stats = {
                "bryck": {
                    "name": self.src,
                    "size": summary.get("bryck_size", 0),
                    "count": summary.get("bryck_file_count", 0)
                },
                "bucket": {
                    "name": self.dst,
                    "size": summary.get("bucket_size", 0),
                    "count": summary.get("bucket_obj_count", 0),
                },
            }

            src_stats = stats["bryck" if is_upload else "bucket"]
            self.src_size = src_stats["size"]
            src_gb = self.src_size / (1000 ** 3)

            lines.append("")
            lines.append("Source Summary:")
            lines.append(f"- {'Source':<38}: {self.src}")
            lines.append(f"- {src_summary_label:<38}: {src_stats['count']}")
            lines.append(f"- {'Total size':<38}: {self.src_size:,} bytes ({src_gb:.4f} GB)")
            lines.append("")

            dst_stats = stats["bucket" if is_upload else "bryck"]
            self.dst_size = dst_stats["size"]
            dst_gb = self.dst_size / (1000 ** 3)

            lines.append("Destination Summary:")
            lines.append(f"- {'Destination':<38}: {self.dst}")
            lines.append(f"- {dst_summary_label:<38}: {dst_stats['count']}")
            lines.append(f"- {'Total size':<38}: {self.dst_size:,} bytes ({dst_gb:.4f} GB)")
            lines.append("")

            # 4. Validation Results
            lines.append("============== TOTAL SUMMARY ==============")
            transferred_count = int(summary.get("transferred_count", 0))
            lines.append(f"- {transfer_label + ' (Aprox)':<38}: {transferred_count}")
            self.transferred_bytes = int(summary.get("transferred_bytes", 0))
            tr_gb = self.transferred_bytes / (1000**3)
            lines.append(f"- {'Bytes transferred (Aprox)':<38}: {self.transferred_bytes:,} bytes ({tr_gb:.4f} GB)")

            # Missing/mismatch rendered as counts. Prefer the per-batch
            # verification count (set in verify()); fall back to the verifier's
            # summary.json lists when present.
            missing_raw = summary.get("missing_files", [])
            missing_count = len(missing_raw) if isinstance(missing_raw, (list, tuple)) else (missing_raw or 0)
            if self.missing_count:
                missing_count = self.missing_count
            mismatch_raw = summary.get("mismatch_files", [])
            mismatch_count = len(mismatch_raw) if isinstance(mismatch_raw, (list, tuple)) else (mismatch_raw or 0)
            if self.mismatch_count:
                mismatch_count = self.mismatch_count
            lines.append(f"- {'Number of missing files/objects':<38}: {missing_count}")
            lines.append(f"- {'Number of size mismatched files/objects':<38}: {mismatch_count}")

            count_label = f"Total {'files' if is_upload else 'objects'}"
            lines.append(f"- {count_label:<38}: {src_stats['count']}")
            lines.append(f"- {'Total source size':<38}: {self.src_size:,} bytes ({src_gb:.4f} GB)")

        # 5. Write to file
        output_file = log_path / "transfer_summary.txt"
        with output_file.open("w") as f:
            f.write("\n".join(lines))

    def _update_transfer_bytes(self):
        query = "UPDATE CloudTransfer SET copiedbytes={}, TotalBytes={} WHERE id={}".format(
            str(self.transferred_bytes), self.src_size, str(self.transfer_id)
        )
        rc, msg = cloud_db(query, "update")
        if rc:
            logger.error(msg)

    def _run_verifier(self, output_file):
        """
        Enumerate both transfer sides, compare sizes, and write verification reports.
        Returns (has_missing: bool, has_mismatch: bool, has_error: bool).

        Uses bryckck_lib.Verifier in-process when available; falls back to the
        bryckck CLI subprocess if the library cannot be imported.
        """
        cloud_config = CloudConfig().bcloud
        report_format = cloud_config.get("REPORT_FORMAT", "csv").lower()
        s3_workers = int(cloud_config.get("VERIFY_S3_WORKERS", 16))
        stat_threads = int(cloud_config.get("VERIFY_STAT_THREADS", 32))

        # Ensure boto3 credential env vars are set so bryckck_lib picks up auth
        aws_cfg_path = cloud_config.get("AWS_CONFIG_FILE", "")
        if aws_cfg_path:
            os.environ["AWS_CONFIG_FILE"] = aws_cfg_path
            os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(
                os.path.dirname(aws_cfg_path), "credentials")
            os.environ["AWS_SDK_LOAD_CONFIG"] = "1"
            if _BRYCKCK_LIB:
                try:
                    import bryckck_lib.config as _bck_cfg
                    _bck_cfg.aws_config_file = aws_cfg_path
                except Exception:
                    pass

        # For ARN/role auth, assume the role and inject temporary credentials.
        # This avoids boto3 profile chain resolution issues in bryckck_lib.
        #
        # IMPORTANT: these temp creds are scoped to verification ONLY and are
        # restored in the finally block below. This process is the long-lived
        # daemon that runs many transfers; leaking the (≈1h) temp creds into
        # os.environ would poison subsequent transfers' upload pipelines
        # (aws CLI / cloudcp read AWS_* env vars directly and they take
        # precedence over the config-file role profile), causing auth failures
        # once the token expires mid-transfer.
        from bryckcloud.lib.cloud.aws import get_temporary_credentials
        _cred_env_keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                          "AWS_SESSION_TOKEN", "AWS_PROFILE")
        _saved_cred_env = {k: os.environ.get(k) for k in _cred_env_keys}
        temp_creds = get_temporary_credentials()
        if temp_creds:
            os.environ["AWS_ACCESS_KEY_ID"] = temp_creds["aws_access_key_id"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = temp_creds["aws_secret_access_key"]
            os.environ["AWS_SESSION_TOKEN"] = temp_creds["aws_session_token"]
            # Remove profile-based env vars to avoid conflicts
            os.environ.pop("AWS_PROFILE", None)

        try:
            # Ensure bryckck_lib picks up the correct S3 endpoint (MinIO, etc.)
            local_aws = cloud_config.get("LOCAL_AWS", "")
            if local_aws and _BRYCKCK_LIB:
                try:
                    import bryckck_lib.source as _bck_src
                    _bck_src._endpoint_url_cached = local_aws.strip().strip('"')
                    _bck_src._endpoint_url_loaded = True
                except Exception:
                    pass

            if _BRYCKCK_LIB:
                try:
                    import inspect
                    import time as _time
                    sig = inspect.signature(_Verifier.__init__)
                    kwargs = {'output_dir': self.cloud_logdir}
                    if 's3_workers' in sig.parameters:
                        kwargs['s3_workers'] = s3_workers
                    if 'stat_threads' in sig.parameters:
                        kwargs['stat_threads'] = stat_threads
                    if 'output_format' in sig.parameters:
                        kwargs['output_format'] = report_format
                    logger.info(f"Verifier config: s3_workers={kwargs.get('s3_workers','N/A')} "
                                f"stat_threads={kwargs.get('stat_threads','N/A')} "
                                f"format={kwargs.get('output_format','csv')}")
                    t0 = _time.time()
                    v = _Verifier(self.src, self.dst, output_file, **kwargs)
                    v.run()
                    elapsed = _time.time() - t0
                    logger.info(f"Verification completed in {elapsed:.1f}s: "
                                f"local_files={v.bryck_file_count} s3_objects={v.bucket_object_count} "
                                f"matched={v.transferred_count} missing={len(v.missing_files)} "
                                f"mismatch={len(v.mismatch_files)}")
                    self._create_legacy_files()
                    return bool(v.missing_files), bool(v.mismatch_files), False
                except Exception as e:
                    logger.error(f"bryckck_lib verification error for transfer {self.transfer_id}: {e}")
                    return False, False, True

            # CLI subprocess fallback
            cmd = (
                f"bryckck verify --src {quote(self.src)} --dst {quote(self.dst)} "
                f"--output_file {quote(output_file)} --output_dir {quote(self.cloud_logdir)} "
                f"--s3-workers {s3_workers} --stat-threads {stat_threads} "
                f"--output-format {report_format}"
            )
            logger.debug(cmd)
            rc, out, err = run_cmd(cmd)
            if rc:
                logger.error(f"Transfer verification failed for transfer_id-{self.transfer_id}: {err}")
                return False, False, True
            self._create_legacy_files()
            return (
                path.exists(path.join(self.cloud_logdir, "missing_filename.txt")),
                path.exists(path.join(self.cloud_logdir, "mismatch_filename.txt")),
                False,
            )
        finally:
            # Restore the pre-verification credential env so temp creds don't
            # leak into later transfers run by this same daemon process.
            if temp_creds:
                for _k, _v in _saved_cred_env.items():
                    if _v is None:
                        os.environ.pop(_k, None)
                    else:
                        os.environ[_k] = _v

    def _create_legacy_files(self):
        """Create legacy-named files from the streaming verifier output.

        The new verifier produces: local_list.csv, s3_list.csv, missing.csv,
        mismatch.csv, transfer_report.csv, summary.json.

        When output_format='json', the verifier renames CSVs to .json
        (local_list.json, s3_list.json, etc.). This method checks both
        extensions.

        The test suite and zip packaging expect: bryck_file_list.txt,
        s3_object_list.json, missing_filename.txt, mismatch_filename.txt,
        transfer_report.json, bryck_object_list.txt.
        """
        # Map: (csv_name, json_name) → legacy_name
        mapping = [
            ('local_list.csv', 'local_list.json', 'bryck_file_list.txt'),
            ('s3_list.csv', 's3_list.json', 's3_object_list.json'),
            ('missing.csv', 'missing.json', 'missing_filename.txt'),
            ('mismatch.csv', 'mismatch.json', 'mismatch_filename.txt'),
            ('transfer_report.json', f'transfer_report_{self.transfer_id}.csv', f'transfer_report_{self.transfer_id}.json'),
        ]
        for csv_name, json_name, legacy_name in mapping:
            # Find whichever exists (.csv or .json after conversion)
            src_path = path.join(self.cloud_logdir, csv_name)
            if not path.exists(src_path):
                src_path = path.join(self.cloud_logdir, json_name)
            dst_path = path.join(self.cloud_logdir, legacy_name)
            if path.exists(src_path) and not path.exists(dst_path):
                try:
                    os.link(src_path, dst_path)
                except OSError:
                    import shutil
                    shutil.copy2(src_path, dst_path)

        # bryck_object_list.txt is an alias for the s3 listing
        bryck_obj_list = path.join(self.cloud_logdir, 'bryck_object_list.txt')
        if not path.exists(bryck_obj_list):
            for name in ('s3_list.csv', 's3_list.json'):
                s3_list = path.join(self.cloud_logdir, name)
                if path.exists(s3_list):
                    try:
                        os.link(s3_list, bryck_obj_list)
                    except OSError:
                        import shutil
                        shutil.copy2(s3_list, bryck_obj_list)
                    break

    def _write_transfer_summary(self, state):
        """Write ``transfer_summary.txt`` from the durable upload report.

        ``summary.json`` (produced by the bryckck verifier) is not generated in
        the current flow, so the totals are derived from the sources we do have:
          * transferred files/bytes  → the upload-report shards (terminal-success
            rows: SUCCESS / FALLBACK_OK / SKIPPED / MP_OK);
          * missing count            → per-batch verification (``self.missing_count``);
          * source totals            → the DB ``TotalFiles`` / ``TotalBytes`` the
            enumerator recorded (fall back to transferred + missing).
        """
        from bryckcloud.lib.cloud import upload_report
        cfg = CloudConfig().bcloud
        is_upload = self.dst.startswith("s3://")
        src_label = "Number of files" if is_upload else "Number of objects"
        dst_label = "Number of objects" if is_upload else "Number of files"
        item = "files" if is_upload else "objects"
        transfer_label = ("Files" if is_upload else "Objects") + " transferred (Aprox)"

        # Transferred totals from the upload report (source of truth).
        transferred_count = 0
        transferred_bytes = 0
        try:
            for r in upload_report.iter_report_rows(self.transfer_id, cfg):
                if r.get("status") not in upload_report.COMPLETED_STATUSES:
                    continue
                transferred_count += 1
                try:
                    transferred_bytes += int(r.get("size") or 0)
                except (TypeError, ValueError):
                    pass
        except Exception as e:
            logger.debug("Summary: could not read upload report: {}".format(e))

        missing_count = self.missing_count

        # Source totals from the DB (set by the enumerator); fall back to
        # transferred + missing when the DB has no value yet.
        total_files = transferred_count + missing_count
        total_bytes = transferred_bytes
        rc, row = get_cloudtransfer(self.transfer_id, "totalfiles", "totalbytes")
        if not rc and row:
            db_files, db_bytes = row
            if db_files:
                total_files = int(db_files)
            if db_bytes:
                total_bytes = int(db_bytes)

        self.transferred_bytes = transferred_bytes
        self.src_size = total_bytes

        # Missing = source total − transferred (what didn't make it across).
        missing_count = max(0, total_files - transferred_count)

        # Header timestamps.
        starttime = lastupdate = ""
        rc, ts = get_cloudtransfer(self.transfer_id, "receivedat", "lastupdated")
        if not rc and ts:
            try:
                starttime = human_readable(ts[0])
                lastupdate = human_readable(ts[1])
            except Exception:
                pass

        tr_gb = transferred_bytes / (1000 ** 3)
        total_gb = total_bytes / (1000 ** 3)

        lines = [
            "Transfer Summary Report",
            "",
            f"- {'Transfer start':<38}: {starttime}",
            f"- {'Transfer completion':<38}: {lastupdate}",
            f"- {'Transfer status':<38}: {state}",
            f"- {'Source':<38}: {self.src}",
            f"- {'Destination':<38}: {self.dst}",
            f"- {'Transfer ID':<38}: {self.transfer_id}",
            "",
            "Source Summary:",
            f"- {'Source':<38}: {self.src}",
            f"- {src_label:<38}: {total_files}",
            f"- {'Total size':<38}: {total_bytes:,} bytes ({total_gb:.4f} GB)",
            "",
            "Destination Summary:",
            f"- {'Destination':<38}: {self.dst}",
            f"- {dst_label:<38}: {transferred_count}",
            f"- {'Total size':<38}: {transferred_bytes:,} bytes ({tr_gb:.4f} GB)",
            "",
            "============== TOTAL SUMMARY ==============",
            f"- {transfer_label:<38}: {transferred_count}",
            f"- {'Bytes transferred (Aprox)':<38}: {transferred_bytes:,} bytes ({tr_gb:.4f} GB)",
            f"- {'Number of missing files/objects':<38}: {missing_count}",
            f"- {('Total ' + item):<38}: {total_files}",
            f"- {'Total source size':<38}: {total_bytes:,} bytes ({total_gb:.4f} GB)",
        ]
        out = path.join(self.cloud_logdir, "transfer_summary.txt")
        try:
            with open(out, "w") as f:
                f.write("\n".join(lines) + "\n")
            logger.info("Transfer summary written: {} ({} {} transferred, {} missing)".format(
                out, transferred_count, item, missing_count))
        except OSError as e:
            logger.error("Failed to write transfer summary {}: {}".format(out, e))

    def _generate_final_report(self, report_format="csv", state=""):
        """Generate the final transfer report in the requested format.

        Preferred source (challenge #8): the durable during-upload report shards
        written by aws_transfer/fallback_worker. This avoids a slow full-bucket
        LIST for the report. Falls back to the verifier's transfer_report only
        when no report shards exist (e.g. legacy transfers).

        Columns: AbsoluteFilePath, S3Path, FileSize, ETag.
        """
        # Human-readable totals summary (transfer_summary.txt), computed from the
        # durable upload report — summary.json is not produced in this flow.
        self._write_transfer_summary(state)

        # Fast path: build directly from the upload report shards.
        try:
            from bryckcloud.lib.cloud import upload_report
            cloud_config = CloudConfig().bcloud
            has_report = any(True for _ in upload_report.iter_report_rows(
                self.transfer_id, cloud_config))
            if has_report:
                if report_format == "json":
                    out = path.join(self.cloud_logdir, "final_report.json")
                    rows = []
                    for r in upload_report.iter_report_rows(self.transfer_id, cloud_config):
                        if r.get("status") not in upload_report.COMPLETED_STATUSES:
                            continue
                        rows.append({"AbsoluteFilePath": r["local_path"],
                                     "S3Path": r.get("s3path", ""),
                                     "FileSize": r.get("size", "0"),
                                     "ETag": r.get("etag", "")})
                    with open(out, "w") as f:
                        json.dump(rows, f, indent=2)
                    logger.info("Final report generated from upload report: {} "
                                "({} entries)".format(out, len(rows)))
                else:
                    out = path.join(self.cloud_logdir, "final_report.csv")
                    n = upload_report.write_final_report(self.transfer_id, out, cloud_config)
                    logger.info("Final report generated from upload report: {} "
                                "({} entries)".format(out, n))
                return
        except Exception as e:
            logger.debug("Report-based final report unavailable, falling back to "
                         "verifier listing: {}".format(e))

        # Check both .csv and .json (verifier renames when output_format='json')
        transfer_report_path = path.join(self.cloud_logdir, f"transfer_report_{self.transfer_id}.csv")
        if not path.exists(transfer_report_path):
            transfer_report_path = path.join(self.cloud_logdir, f"transfer_report_{self.transfer_id}.json")
        if not path.exists(transfer_report_path):
            logger.debug(f"transfer_report not found in {self.cloud_logdir} — skipping final report")
            return

        is_upload = self.dst.startswith("s3://")
        s3_url = self.dst if is_upload else self.src
        local_root = (self.src if is_upload else self.dst).rstrip("/")

        # Parse S3 URL: s3://bucket or s3://bucket/prefix
        bucket_and_prefix = s3_url.replace("s3://", "", 1).rstrip("/")
        if "/" in bucket_and_prefix:
            bucket_name, s3_prefix = bucket_and_prefix.split("/", 1)
            s3_prefix = s3_prefix + "/"
        else:
            bucket_name = bucket_and_prefix
            s3_prefix = ""

        # Read transfer_report (CSV or NDJSON depending on verifier output format)
        report_rows = []
        try:
            is_json_file = transfer_report_path.endswith(".json")
            with open(transfer_report_path, "r", newline="" if not is_json_file else None) as f:
                if is_json_file:
                    # NDJSON: one JSON object per line
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        rel_path = row.get("path") or row.get("Path") or row.get("key") or ""
                        size = str(row.get("size") or row.get("Size") or "0")
                        etag = row.get("etag") or row.get("ETag") or ""
                        if not rel_path:
                            continue
                        abs_path = path.join(local_root, rel_path)
                        s3_path = f"s3://{bucket_name}/{s3_prefix}{rel_path}"
                        report_rows.append({
                            "AbsoluteFilePath": abs_path,
                            "S3Path": s3_path,
                            "FileSize": size,
                            "ETag": etag,
                        })
                else:
                    # CSV: skip comment lines
                    lines = []
                    for line in f:
                        if line.startswith("#"):
                            continue
                        lines.append(line)
                    reader = csv.DictReader(lines)
                    for row in reader:
                        rel_path = row.get("path") or row.get("Path") or row.get("key") or ""
                        size = row.get("size") or row.get("Size") or "0"
                        etag = row.get("etag") or row.get("ETag") or ""
                        if not rel_path:
                            continue
                        abs_path = path.join(local_root, rel_path)
                        s3_path = f"s3://{bucket_name}/{s3_prefix}{rel_path}"
                        report_rows.append({
                            "AbsoluteFilePath": abs_path,
                            "S3Path": s3_path,
                            "FileSize": size,
                            "ETag": etag,
                        })
        except Exception as e:
            logger.error(f"Failed to parse transfer report: {e}")
            return

        if not report_rows:
            logger.debug("No entries for final report")
            return

        # Write report in requested format
        if report_format == "json":
            report_file = path.join(self.cloud_logdir, "final_report.json")
            try:
                with open(report_file, "w") as f:
                    json.dump(report_rows, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to write final report (json): {e}")
        else:
            report_file = path.join(self.cloud_logdir, "final_report.csv")
            try:
                with open(report_file, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=["AbsoluteFilePath", "S3Path", "FileSize", "ETag"])
                    writer.writeheader()
                    writer.writerows(report_rows)
            except Exception as e:
                logger.error(f"Failed to write final report (csv): {e}")

        logger.info(f"Final report generated: {report_file} ({len(report_rows)} entries)")

    def _verify_batches(self):
        """Per-batch reconciliation: batch file vs. the durable upload report (§15.1).

        For every ``completed`` batch, its set of local paths ``B`` must be a
        subset of the terminally-recorded paths (``SUCCESS``/``FALLBACK_OK``/
        ``SKIPPED``/``MP_OK``) in the merged report. ``missing = B − recorded``;
        any non-empty batch (or any batch still pending/inprogress) makes the
        transfer **Incomplete**. This needs **no bucket LIST** (challenge #8) —
        completeness is proven from the report we built during upload.

        Writes a human-readable ``batch_verification_failures.txt`` listing the
        offending batches + missing files (only when there are failures).
        Returns ``(ok, incomplete_batches, missing_files)``.
        """
        from bryckcloud.lib.cloud import batch_state, upload_report
        cfg = CloudConfig().bcloud
        batch_base = cfg.get("BATCH_FILE_DIR", "/bryck/bcloud_batchmeta")
        tdir = os.path.join(batch_base, "transfer_{}".format(self.transfer_id))
        if not os.path.isdir(os.path.join(tdir, "batches")):
            # Not a batch-based transfer (legacy / non-parallel) — nothing to do.
            return True, 0, 0

        completed_set = upload_report.load_completed(self.transfer_id, cfg)
        counts = batch_state.counts(tdir)
        outstanding = counts.get("pending", 0) + counts.get("inprogress", 0)

        failures_path = os.path.join(self.cloud_logdir, "batch_verification_failures.txt")
        incomplete_batches = 0
        missing_total = 0
        try:
            with open(failures_path, "w", errors="surrogateescape", newline="") as ff:
                if outstanding:
                    ff.write("# {} batch(es) not terminal (pending/inprogress) at "
                             "verification time\n".format(outstanding))
                for name, bpath in batch_state.completed_batches(tdir):
                    paths = batch_state.read_batch_file(bpath)
                    missing = [p for p in paths if p not in completed_set]
                    if missing:
                        incomplete_batches += 1
                        missing_total += len(missing)
                        ff.write("BATCH {} missing {} file(s):\n".format(name, len(missing)))
                        for p in missing:
                            # single-line-safe for the human log; the batch file
                            # remains the byte-exact record.
                            ff.write("  {}\n".format(p.replace("\n", "\\n").replace("\r", "\\r")))
        except OSError as e:
            logger.error("Per-batch verification: cannot write failures list {}: {}".format(
                failures_path, e))
            return True, 0, 0  # don't fail the transfer on a reporting error

        ok = (incomplete_batches == 0 and outstanding == 0)
        if ok:
            # No failures — drop the empty marker file.
            try:
                os.remove(failures_path)
            except OSError:
                pass
            logger.info("Per-batch verification passed for transfer {}: all completed "
                        "batches fully recorded".format(self.transfer_id))
        else:
            logger.error("Per-batch verification for transfer {}: {} incomplete batch(es), "
                         "{} missing file(s), {} non-terminal batch(es) — see {}".format(
                             self.transfer_id, incomplete_batches, missing_total,
                             outstanding, failures_path))
        return ok, incomplete_batches, missing_total

    def verify(self):
        """
        Verify a transfer, updating DB state, checking mismatch/missing files,
        and zipping logs.
        """
        state, msg = self.tr_state, ""

        # Check if transfer was paused/cancelled before starting verification
        rc, current_state = get_cloudtransfer(self.transfer_id, "transferstate")
        if not rc and current_state in ("PAUSED", "CANCELLED", "STOPPED"):
            logger.info(f"Transfer {self.transfer_id} is {current_state} — skipping verification")
            return current_state, f"Transfer {self.transfer_id} {current_state} before verification"

        # Retrieve timestamps and compute duration
        rc, (starttime, lastupdate) = get_cloudtransfer(
            self.transfer_id, "receivedat", "lastupdated"
        )
        duration = lastupdate - starttime
        starttime = human_readable(starttime)
        lastupdate = human_readable(lastupdate)

        # Update transfer state to VERIFYING only if not already paused/cancelled
        query = (f"UPDATE CloudTransfer SET transferstate='VERIFYING' "
                 f"WHERE id={self.transfer_id} AND transferstate NOT IN ('PAUSED', 'CANCELLED', 'STOPPED')")
        rc, msg = cloud_db(query, "update")
        if rc:
            logger.error(msg)

        # Confirm state actually changed — if pause won the race, bail out
        rc, current_state = get_cloudtransfer(self.transfer_id, "transferstate")
        if not rc and current_state in ("PAUSED", "CANCELLED", "STOPPED"):
            logger.info(f"Transfer {self.transfer_id} was {current_state} — aborting verification")
            return current_state, f"Transfer {self.transfer_id} {current_state} before verification"

        # output_file = path.join(self.cloud_logdir, 'output.txt')
        # has_missing, has_mismatch, has_error = self._run_verifier(output_file)

        # After verifier completes, check if transfer was paused during verification
        rc, current_state = get_cloudtransfer(self.transfer_id, "transferstate")
        if not rc and current_state in ("PAUSED", "CANCELLED", "STOPPED"):
            logger.info(f"Transfer {self.transfer_id} was {current_state} during verification")
            return current_state, f"Transfer {self.transfer_id} {current_state} during verification"

        # If verifier hit a fatal error, mark FAILED but still produce the zip
        # if has_error:
        #     state = "FAILED"
        #     msg = f"Transfer_id {self.transfer_id} - Verification encountered an error"
        #     logger.error(msg)

        # Per-batch reconciliation (§15.1): batch file vs. upload report, no
        # bucket LIST. Runs for AWS uploads when batch metadata exists; a gap
        # marks the transfer Incomplete (PAUSED so it can be resumed).
        cloud_config = CloudConfig().bcloud
        per_batch_enabled = str(cloud_config.get("PER_BATCH_VERIFY", "True")).lower() == "true"
        batch_incomplete = False
        batch_msg = ""
        if per_batch_enabled \
                and self.dst.startswith("s3://") \
                and self.tr_state not in ("CANCELLED", "PAUSED"):
            try:
                bok, inc_batches, miss_files = self._verify_batches()
                self.missing_count = miss_files
                if not bok:
                    batch_incomplete = True
                    batch_msg = ("Transfer_id {} - per-batch verification found {} missing "
                                 "file(s) across {} batch(es)".format(
                                     self.transfer_id, miss_files, inc_batches))
            except Exception as e:
                logger.error("Per-batch verification error for transfer {}: {}".format(
                    self.transfer_id, e))

        # Check for issues and update state
        # if not has_error:
        #     
        #     if self.tr_state not in ("CANCELLED", "PAUSED"):
        #         if has_mismatch:
        #             state = "PAUSED"
        #             issues.append("mismatch files")
        #         if has_missing:
        #             state = "PAUSED"
        #             issues.append("missing files")
        issues = []
        if batch_incomplete:
            state = "PAUSED"
            issues.append("incomplete batches")
        if issues:
            msg = f"Transfer_id {self.transfer_id} - Found {' and '.join(issues)} after verification"
            if batch_incomplete:
                logger.error(batch_msg)

        # Generate final report + transfer summary (CSV or JSON per config).
        # summary.json (from the bryckck verifier) is not produced in this flow,
        # so _generate_final_report computes the summary totals from the durable
        # upload report instead of write_summary/summary.json.
        cloud_config = CloudConfig().bcloud
        report_format = cloud_config.get("REPORT_FORMAT", "csv").lower()
        summary_state = "Completed" if state == "COMPLETED" else "Incomplete"
        self._generate_final_report(report_format, summary_state)

        final_files = load_files(cloud_config["TRANSFER_SUMMARY_FILES"], self.transfer_id)

        # Zip logs
        zip_folder(self.transfer_id, self.cloud_logdir, final_files, self.cloud_logdir + ".zip")

        logger.info(
            f"{self.cloud_type} transfer id:{self.transfer_id} src:{self.src} dst:{self.dst} - {state}"
        )

        return state, msg


# Module-level compatibility functions used by cloud_transfer.py
def get_cloud_logs_dir(transfer_id: str) -> str:
    return TransferVerification.get_cloud_logs_dir(transfer_id)


def verify_transfer(transfer_id, src, dst, cloud_type, tr_state):
    return TransferVerification(transfer_id, src, dst, cloud_type, tr_state).verify()

