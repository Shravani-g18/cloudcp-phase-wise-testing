# CloudCpCliTesting

This folder is structured to mirror the role of `CloudCpBinaryTesting`, but for
the `bryckcloud transfer add aws` entry point.

## Layout

```text
CloudCpCliTesting/
  README.md
  cloudcpclitesting.py
  plan_cp_cli.md
  .gitignore
  data/
    README.md
  scripts/
    run_smoke_cli_test.sh
    run_boundary_cli_test.sh
    validate_existing_transfer.sh
  runs/
    run_<timestamp>_<dataset>/report.json
```

## Main runner

`cloudcpclitesting.py` performs one dataset flow end-to-end:

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

## Phase scripts

The `scripts/` folder contains simple Linux-host entry points for common CLI runs:

- `run_smoke_cli_test.sh` for a small smoke test.
- `run_boundary_cli_test.sh` for a batch-boundary style dataset.
- `validate_existing_transfer.sh` for report-only validation of an existing transfer id.

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

Per-run JSON reports are written under `CloudCpCliTesting/runs/`.

See `plan_cp_cli.md` for the test-plan style description and `data/README.md` for dataset guidance.
