# CloudCpCliTesting

This folder is structured to mirror the role of `CloudCpBinaryTesting`, but for
the CloudCP CLI (`bryckclient-cli`) and the `bryckcloud transfer add aws` entry point.

## Layout

```text
CloudCpCliTesting/
  README.md
  cloud_cli_plan.md         # test plan (purpose, scope, test-case catalog, commands)
  cloud_cli_runner.py       # single entry point: --plan then --execute
  cloudcpclitesting.py      # underlying dataset/report helpers, imported by cloud_cli_runner.py
  plan_cp_cli.md
  .gitignore
  bryckclient-cli/          # operator CLI scripts (mount/eject/format/cloud/transfer/...)
  data/
    README.md
  results/                  # generated per-run: results/<RUN_ID>/<TEST_ID>/{report.json,commands.log}
```

`results/` is gitignored and created on demand — nothing under it is checked in.

## Single entry point: `cloud_cli_runner.py`

All test cases (transfers, live intervention/lifecycle tests, service restarts,
edge cases) are run through one script, in two phases. See `cloud_cli_plan.md`
for the full test-case catalog and command reference.

```bash
# Phase 1 — read-only: build + confirm the plan
python3 cloud_cli_runner.py --plan --only CLI-U-ZERO --yes

# Phase 2 — execute the confirmed plan
python3 cloud_cli_runner.py --execute --plan-file results/<RUN_ID>/plan.json
```

Useful flags: `--only <ID> [<ID> ...]` to run specific test cases, `--tiers`/`--modes`
to scope a batch, `--no-lifecycle`/`--no-service`/`--no-edge` to skip a matrix,
`--dry-run` to preview commands without touching a real Bryck, `--keep` to skip
auto-cleanup for debugging.

## `cloudcpclitesting.py` (library + legacy single-dataset runner)

`cloud_cli_runner.py` imports `cloudcpclitesting.py` for dataset generation and
report-validation helpers, so this file must stay in place. It can still be run
directly for the older one-off flow it was originally built for:

1. Selects a dataset from `dataset_cloudcp/spec_files/manifest.json`.
2. Rewrites the dataset's datagen spec roots under your chosen output base.
3. Runs datagen for every spec in that dataset.
4. Validates the generated local file counts against `manifest.json`.
5. Runs:

```bash
/opt/bryck/.venv/bryck/bin/bryckcloud transfer add aws \
  --src <materialized_dataset_root> \
  --dst <s3://bucket/prefix>
```

6. Detects the created transfer id.
7. Waits for transfer artifacts under Bryck's transfer log folders.
8. Validates merged success rows, `final_report.csv`, and leftover retry/failure artifacts.

Naming rule for Bryck-host datasets:

- Local source root is always created as: `<output-base>/<dataset-id>`.
- Destination is normalized to include `<dataset-id>` as the final prefix segment.

Example: if `--dataset DS-P2-01` and `--dst s3://aditya/cloudcp-cli`,
the runner uses `s3://aditya/cloudcp-cli/DS-P2-01` automatically.

It is intended to run on the Linux Bryck host. On Windows, use `--list` or `--dry-run`.

## Quick commands

List datasets:

```bash
python3 CloudCpCliTesting/cloudcpclitesting.py --list
```

Dry-run one dataset:

```bash
python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P2-01 \
  --output-base /bryck/cloudcp_cli_data \
  --dst s3://aditya/cloudcp-cli \
  --dry-run
```

Real run:

```bash
python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P2-01 \
  --output-base /bryck/cloudcp_cli_data \
  --dst s3://aditya/cloudcp-cli \
  --yes
```

Validate an already-submitted transfer:

```bash
python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P2-01 \
  --output-base /bryck/cloudcp_cli_data \
  --skip-transfer \
  --transfer-id 1234 \
  --dst s3://aditya/cloudcp-cli
```

Append extra arguments to the `bryckcloud` command:

```bash
python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P2-01 \
  --output-base /bryck/cloudcp_cli_data \
  --dst s3://aditya/cloudcp-cli \
  --transfer-arg --endpoint-url \
  --transfer-arg https://10.10.10.103:9000 \
  --yes
```

## Expected host inputs

- Datagen binary: `/home/bryck/rperiyas/datagen`
- BryckCloud CLI: `/opt/bryck/.venv/bryck/bin/bryckcloud`
- Batch metadata root: `/opt/bryck/bryckapi/downloads/bcloud_batchmeta`
- Transfer logs root: `/opt/bryck/bryckapi/downloads/cloud_transfer_logs`

Override any of those with the corresponding CLI flags if your host differs.

## Validation performed

- Each rewritten spec generates exactly the file count declared in `manifest.json`.
- The materialized dataset total matches the dataset's `emitted_files` count.
- The merged success rows across `transfer_report_<id>.csv` and `report/upload_report.*.csv` match the expected file count.
- `final_report.csv` exists, has the expected row count, keeps local paths under the generated dataset root, and reports sizes that match the source files.
- No terminal `failed_uploads.*` entries remain.
- No live `cloudcp_retry_<id>_*.lst` files remain.

Per-run JSON reports (`cloudcpclitesting.py` standalone runs) are written under
`CloudCpCliTesting/runs/` if you use that legacy flow directly; the
`cloud_cli_runner.py` two-phase flow writes everything under `results/<RUN_ID>/` instead.

See `plan_cp_cli.md` for the legacy single-dataset test-plan description, and
`cloud_cli_plan.md` for the full CLI test plan / test-case catalog, and
`data/README.md` for dataset guidance.
