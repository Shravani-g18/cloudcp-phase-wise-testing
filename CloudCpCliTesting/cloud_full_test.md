# `cloud_full_test.py` -- Unified Transfer + Negative-Catalog Test Harness

## 1. What this is

`cloud_full_test.py` is a **copy** of [cloud_transfer_only.py](cloud_transfer_only.py) (that
file is untouched) extended into a single entry point that can run:

1. **Positive transfer cases** -- one case per `dataset x mode`
   (`upload`/`download`/`both`), for every dataset in
   `dataset_cloudcp/spec_files/manifest.json` (54 datasets x 3 modes = 162 cases).
   This is the same real engine as `cloud_transfer_only.py`: datagen -> configure
   cloud -> `bryckcloud transfer add aws` -> pause/resume cycles -> poll to
   terminal -> bryck_state/transfer_status capture at every step -> perf
   capture -> optional cleanup.
2. **The full negative catalog** -- all 308 cases from
   `NEGATIVE_TEST_PLAN.md` (`CLI`, `AUTH`, `TID`, `AWS`, `PATH`, `LIFE`,
   `DATASET`, `XFER`, `DOWNLOAD`, `STATE`, `RACE`, `DUP`, `REPORT`, `FAULT`,
   `REC`, `VERIFY`, `INT`, `CLEAN`, `MGMT`, `SVC`, `SM`, `F`) plus the 3 `MASTER-*`
   end-to-end flows -- 311 negative cases total, exactly matching
   `python3 cloudcpclitesting.py --list-negative`'s ID/name list.

   **These are NOT reimplemented here.** Every negative case is delegated
   in-process to [negative_environment_runner.py](negative_environment_runner.py)'s
   `dispatch()` / `EnvironmentManager` -- **this is the actual environment file
   negative cases run through**, not `cloudcpclitesting.py`. That file's own
   `NEG_CATALOG` is used here **only** as a name/order source (so `--list`/
   `--one`/`--from`/`--to` work without needing the still-missing
   `NEGATIVE_TEST_PLAN.md` that `negative_environment_runner.py`'s own catalog
   loader would otherwise require). `negative_environment_runner.py` has real,
   working handlers (`handle_cli`, `handle_life`, `handle_data`, `handle_xfer`,
   `handle_download`, `handle_state`, `handle_race`, `handle_dup`,
   `handle_report`, `handle_fault`, `handle_rec`, `handle_verify`, `handle_int`,
   `handle_clean`, `handle_mgmt`, `handle_svc`, `handle_statematrix`,
   `handle_combo`) for **every section except `MASTER-*`** -- 308 of the 311
   negative cases are genuinely implemented; only the 3 `MASTER-*` end-to-end
   flows (which live in a separate, not-yet-wired file,
   `cloud_transfer_negative_test_runner.py`) still report `BLOCKED`
   ("no handler registered for this case prefix").

Combined catalog: **473 cases** (162 transfer + 311 negative, of which 308
negative cases are genuinely implemented and only 3 -- the `MASTER-*` flows --
are still unwired).


## 2. Environment preparation before every case

Per request, every single case -- transfer **or** negative -- now runs
`prepare_environment()` first, modeled directly on
[negative_environment_runner.py](negative_environment_runner.py)'s
`EnvironmentManager.snapshot()`/`ensure_mounted()` methods. The returned dict
intentionally matches that file's `snapshot()` shape field-for-field, so
reports read the same way across scripts:

```python
{"bryck_state": ..., "cloud_configured": ..., "info_ok": ..., "cloud_ok": ..., "status_ok": ...}
```

Steps:

1. Query `bryck_info.py` for the current state (`info_ok`).
2. If `Ejected`, mount it (`bryck_mount.py`) before anything else runs --
   never assume a mounted baseline (`bryck_state`).
3. Query `bryck_cloud_show.py` and record whether cloud is already configured
   (`cloud_configured`, `cloud_ok`).
4. Query `bryck_cloud_transfer_status.py` to confirm the status endpoint
   itself is reachable (`status_ok`).
5. Log the full snapshot: `environment snapshot: {'bryck_state': ..., 'cloud_configured': ..., 'info_ok': ..., 'cloud_ok': ..., 'status_ok': ...}`.

This guarantees every case starts from a **known** state instead of an assumed
one -- including the ~180 still-stub negative cases (so if/when they get
implemented, they inherit a sane starting point) and every transfer case
(previously, only `cloud_transfer_only.py`'s single-invocation `main()` did
this baseline mount check; the batch loop here didn't -- now it does, before
dataset generation).

This baseline step does **not** replace fixture-specific setup a negative
case's own handler performs internally (e.g. `STATE-*` handlers already call
`ensure_mounted()`/`configure_cloud()` themselves for their own transfer
fixture) -- it runs *before* that, as a first-line guarantee.

For negative cases, the environment-prep snapshot is written to
`results/full_test/<run-id>/negative/<case-id>/env_prep.json` alongside that
case's own `summary.json`/`summary.html` (written by
`cloudcpclitesting.run_negative_suite()`).

## 3. Important invariant: report retrieval must never trigger pause/resume

For **positive** transfer cases specifically: retrieving/checking a
transfer's report or status -- whether the transfer is `IN_PROGRESS`,
`PAUSED`, or just `resumed` -- is **strictly read-only** and must **never**
cause a pause or resume as a side effect. In this script:

- `capture_transfer_status()` / `poll_until_terminal()` only call
  `bryck_cloud_transfer_status.py` (a query), never pause/resume.
- `find_transfer_report_csv()` only reads a CSV the broker already wrote,
  never touches the transfer's state.
- **Only** `pause_resume_transfer()` (gated behind `--pause-resume`) ever
  issues real `bryck_cloud_transfer_pause.py`/`bryck_cloud_transfer_resume.py`
  commands, and it never reads/downloads a report as part of doing so.

This separation is intentional and documented directly in `run_leg()`'s
docstring: a positive test case downloading/checking a summary report while a
transfer is active or paused must not itself pause or resume anything. That
coupling (report-download-during-various-states) is only deliberately
exercised by the **negative** catalog's own `REPORT-*`/`DOWNLOAD-*` cases,
which are a separate, dedicated set of scenarios -- not something the
positive engine does incidentally.

## 4. CLI reference

Modeled on `CloudCpFallbackTesting/cloudcp_fallback_test.py`'s argument surface.

### Selection
| Flag | Meaning |
|---|---|
| `--all` | Run every case (162 transfer + 311 negative). |
| `--one ID[,ID...]` | Run one case, or a comma-separated list, by `[n]` index or literal case id. |
| `--order ID` | Run exactly one case by `[n]` index or literal case id (same resolution as `--one`). |
| `--from ID --to ID` | Run an inclusive range in catalog order (index or id). |
| `--range FROM-TO` | Run an inclusive range as a single `FROM-TO` spec, e.g. `--range 1-9` or `--range CLI-01-CLI-09`. |
| `--negative` | Run only the negative-catalog cases. |
| `--negative-case ID[,ID...]` | Run one/comma-separated negative case(s) only. |
| `--list` | Print the full catalog with `[n]` index/kind/status/command/name and exit. |

### Execution
| Flag | Meaning |
|---|---|
| `--dry-run` | Print the plan without executing anything. |
| `--manual` | Interactive: prompt `run/skip/quit` before each selected case. |
| `--skip-datagen` | Reuse already-materialized data. |
| `--skip-seed` | For `download` cases, assume the bucket already has objects (skip the seed upload). |
| `--keep-config` | Do not restore the original `cloud_ops.json` after the run. |
| `--skip-cleanup` | Never clean up, even if `--cleanup` is also given. |
| `--seed N` | Random seed for reproducibility (default `1337`). |
| `--poll-interval` / `--poll-timeout` | Status-poll cadence / overall wait timeout. |
| `--verbose` | Debug-level logging. |
| `--direction {upload,download,both}` | Primary bryck<->s3 direction (default `upload`) used to build each negative case's own transfer fixture. Cases whose scenario is inherently direction-specific (`DOWNLOAD-*`, `XFER-01..10/18`, and any `F-*` case whose name says "Upload"/"Download") ignore this and always use their own fixed direction. `--direction both` runs the exact same selection **twice in one command** (an upload pass, then a download pass), each into its own `<run-id>_upload` / `<run-id>_download` folder, instead of you invoking this script twice by hand. |


### Paths / hosts
`--cli-dir`, `--spec-dir`, `--out-dir`, `--login`, `--cloud-ops`, `--datagen`,
`--config`, `--service` (reserved for future `SVC-*` implementations),
`--transfer-logs-dir`, `--endpoint-url`, `--src-base`, `--bucket-base`, `--dl-base`.

### Component tests -- **NOT IMPLEMENTED**
`--component`, `--component-one`, `--component-negative`, `--component-list`,
`--heavy`, `--component-bucket`, `--region`, `--venv-python`,
`--batchmeta-dir`, `--pool-size` are accepted (matching
`cloudcp_fallback_test.py`'s surface) but **refuse to run**, printing a clear
error and returning exit code `2`. The `fallback_worker`/`mp_batch_retry`
internal-mechanism tests they refer to have no source anywhere in this repo
-- only their CLI surface was described -- so this script does not fabricate
fake results for them.

## 5. Examples

```bash
# See the full 473-case catalog
python3 cloud_full_test.py --list

# Dry-run everything first
python3 cloud_full_test.py --all --dry-run

# Run one transfer case
python3 cloud_full_test.py --one TRANSFER-DS-P8-01-UPLOAD

# Run one case by index or id via --order
python3 cloud_full_test.py --order 5

# Run one negative case
python3 cloud_full_test.py --negative-case AWS-03

# Run every negative case
python3 cloud_full_test.py --negative

# Run a contiguous range by catalog position
python3 cloud_full_test.py --from 1 --to 9

# Same range, as a single FROM-TO spec
python3 cloud_full_test.py --range 1-9

# Range by literal (hyphenated) case ids also works
python3 cloud_full_test.py --range CLI-01-CLI-09

# Interactive: confirm each case before running it
python3 cloud_full_test.py --manual --negative
```

## 6. Reports

- Transfer cases: `results/full_test/<run-id>/transfer/<case-id>/{upload,download}/`
  -- same `commands.log`/`summary.json`/`summary.md`/`summary.html` +
  perf HTML/JSON/zip as `cloud_transfer_only.py`.
- Negative cases: `results/full_test/<run-id>/negative/<case-id>/env_prep.json`
  (this script's baseline-environment addition) plus a single combined report
  for **all** negative cases selected in the run:
  `results/full_test/<run-id>/negative/combined_results.json` and
  `combined_report.html` -- written with
  `negative_environment_runner.build_html()` (the same rich, searchable/
  filterable/sortable report that script itself produces, with full
  before/after environment snapshots and command/stdout/stderr detail per case).
- Top-level `results/full_test/<run-id>/report.json` -- one row per selected
  case with its final status.

## 7. Full 473-case catalog (organized by group)

Reorganized version of `python3 cloud_full_test.py --list`'s output, grouped
by test category with a short heading per group instead of one flat table, so
individual cases are easier to find. **`#`** is the catalog order number used
by `--from`/`--to`/`--range` (matches `[n]` from `--list`). Use `--one <id>`
to run a transfer case by itself, `--negative-case <id>` for a negative case.
All 308 non-`MASTER-*` negative cases have a real handler in
`negative_environment_runner.py` and will attempt execution (see section 1);
only the 3 `MASTER-*` cases are still unimplemented stubs.

### 7.1 Transfer cases (# 1-162) -- 54 datasets x upload/download/both

Each dataset occupies 3 consecutive catalog slots: `UPLOAD`, `DOWNLOAD`, `BOTH`
(e.g. `--one TRANSFER-DS-P1-01-UPLOAD`). Datasets are grouped by phase.

| Phase | Datasets | Catalog range |
|---|---|---|
| P1 | DS-P1-01 .. DS-P1-06 (6) | # 1-18 |
| P2 | DS-P2-01 .. DS-P2-07 (7) | # 19-39 |
| P3 | DS-P3-01 .. DS-P3-06 (6) | # 40-57 |
| P4 | DS-P4-01 .. DS-P4-05 (5) | # 58-72 |
| P5 | DS-P5-01 (1) | # 73-75 |
| P6 | DS-P6-01 (1) | # 76-78 |
| P7 | DS-P7-01 .. DS-P7-03 (3) | # 79-87 |
| P8 | DS-P8-01 .. DS-P8-05 (5) | # 88-102 |
| P9 | DS-P9-01 .. DS-P9-07 (7) | # 103-123 |
| P10 | DS-P10-01 .. DS-P10-08 (8) | # 124-147 |
| P11 | DS-P11-01 .. DS-P11-03 (3) | # 148-156 |
| P12 | DS-P12-01 .. DS-P12-02 (2) | # 157-162 |

Per-dataset index formula: for a dataset at position *i* (1-based) within its
phase's start offset, `UPLOAD = start + 3*(i-1)`, `DOWNLOAD = start + 3*(i-1) + 1`,
`BOTH = start + 3*(i-1) + 2`. Example: `DS-P9-03` is the 3rd dataset in P9
(start #103) -> `UPLOAD=#109`, `DOWNLOAD=#110`, `BOTH=#111`.

### 7.2 Negative catalog (# 163-473) -- 311 cases across 23 groups

#### CLI -- CLI protocol/argument validation (# 163-171)
| # | ID | Name |
|---|---|---|
| 163 | CLI-01 | Initiate without --mode |
| 164 | CLI-02 | Invalid mode |
| 165 | CLI-03 | Upload without bryck_src |
| 166 | CLI-04 | Upload without cloud_bucket |
| 167 | CLI-05 | Download without bryck_dst |
| 168 | CLI-06 | Missing login file |
| 169 | CLI-07 | Malformed login JSON |
| 170 | CLI-08 | Invalid transfer id operation |
| 171 | CLI-09 | Missing dataset spec |

#### AUTH -- authentication / session expiry (# 172-181)
| # | ID | Name |
|---|---|---|
| 172 | AUTH-01 | Invalid username |
| 173 | AUTH-02 | Invalid password |
| 174 | AUTH-03 | Invalid access token |
| 175 | AUTH-04 | Expired token |
| 176 | AUTH-05 | Missing authentication token |
| 177 | AUTH-06 | Request after session expiry |
| 178 | AUTH-07 | Transfer operation after expiry |
| 179 | AUTH-08 | Pause after expiry |
| 180 | AUTH-09 | Resume after expiry |
| 181 | AUTH-10 | Cancel after expiry |

#### TID -- transfer-ID validation (# 182-190)
| # | ID | Name |
|---|---|---|
| 182 | TID-01 | Transfer ID validation ('99999999') |
| 183 | TID-02 | Transfer ID validation ('') |
| 184 | TID-03 | Transfer ID validation ('-1') |
| 185 | TID-04 | Transfer ID validation ('not-a-transfer') |
| 186 | TID-05 | Transfer ID validation ('!@#$%^&*') |
| 187 | TID-06 | Transfer ID validation ('999999999999999999999999999999999999') |
| 188 | TID-07 | Transfer ID validation ('2147483647') |
| 189 | TID-08 | Transfer ID validation ('1') |
| 190 | TID-09 | Transfer ID validation ('1.2.3') |

#### AWS -- cloud credentials / bucket / configure-deconfigure (# 191-208)
| # | ID | Name |
|---|---|---|
| 191 | AWS-01 | Missing access key |
| 192 | AWS-02 | Missing secret key |
| 193 | AWS-03 | Invalid access key |
| 194 | AWS-04 | Invalid secret key |
| 195 | AWS-05 | Invalid region |
| 196 | AWS-06 | Invalid endpoint |
| 197 | AWS-07 | Invalid bucket URI |
| 198 | AWS-08 | Nonexistent bucket |
| 199 | AWS-09 | Inaccessible bucket |
| 200 | AWS-10 | Missing List permission |
| 201 | AWS-11 | Missing PutObject permission |
| 202 | AWS-12 | Missing GetObject permission |
| 203 | AWS-13 | Deconfigure when not configured |
| 204 | AWS-14 | Deconfigure twice |
| 205 | AWS-15 | Deconfigure during active transfer |
| 206 | AWS-16 | Deconfigure while paused |
| 207 | AWS-17 | Reconfigure during active transfer |
| 208 | AWS-18 | Reconfigure during paused transfer |

#### PATH -- destination-prefix validation (# 209-217)
| # | ID | Name |
|---|---|---|
| 209 | PATH-01 | Invalid destination prefix |
| 210 | PATH-02 | Empty prefix |
| 211 | PATH-03 | Leading slash |
| 212 | PATH-04 | Double slash |
| 213 | PATH-05 | Special characters/spaces |
| 214 | PATH-06 | Unicode prefix |
| 215 | PATH-07 | Very long prefix |
| 216 | PATH-08 | Parent traversal |
| 217 | PATH-09 | Source/config mismatch |

#### LIFE -- device lifecycle (mount/format/eject) (# 218-233)
| # | ID | Name |
|---|---|---|
| 218 | LIFE-01 | Info unavailable |
| 219 | LIFE-02 | Mount before format |
| 220 | LIFE-03 | Missing mount params |
| 221 | LIFE-04 | Invalid mount params |
| 222 | LIFE-05 | Mount already mounted |
| 223 | LIFE-06 | Format while mounted |
| 224 | LIFE-07 | Invalid format params |
| 225 | LIFE-08 | Format unavailable |
| 226 | LIFE-09 | Eject already ejected |
| 227 | LIFE-10 | Eject active transfer |
| 228 | LIFE-11 | Eject paused transfer |
| 229 | LIFE-12 | Erase mounted |
| 230 | LIFE-13 | Format/erase/remove during transfer |
| 231 | LIFE-14 | Mount during transfer |
| 232 | LIFE-15 | Format during verification |
| 233 | LIFE-16 | Eject during cancellation |

#### DATA -- dataset generation (datagen) (# 234-245)
| # | ID | Name |
|---|---|---|
| 234 | DATA-01 | Generate while ejected |
| 235 | DATA-02 | Generate while unmounted |
| 236 | DATA-03 | Generate while active |
| 237 | DATA-04 | Generate while paused |
| 238 | DATA-05 | Missing specification |
| 239 | DATA-06 | Invalid specification |
| 240 | DATA-07 | Invalid/negative size |
| 241 | DATA-08 | Insufficient storage |
| 242 | DATA-09 | Outside /bryck |
| 243 | DATA-10 | Inaccessible files |
| 244 | DATA-11 | Interrupted generation |
| 245 | DATA-12 | Duplicate generation |

#### XFER -- upload/download transfer + pause/resume/cancel (# 246-263)
| # | ID | Name |
|---|---|---|
| 246 | XFER-01 | Upload while ejected/unmounted |
| 247 | XFER-02 | Cloud not configured |
| 248 | XFER-03 | Invalid source path |
| 249 | XFER-04 | Empty source directory |
| 250 | XFER-05 | Inaccessible source |
| 251 | XFER-06 | Nonexistent bucket |
| 252 | XFER-07 | Invalid cloud object path |
| 253 | XFER-08 | Invalid download destination |
| 254 | XFER-09 | Upload while download active |
| 255 | XFER-10 | Download while upload active |
| 256 | XFER-11 | Pause immediately |
| 257 | XFER-12 | Resume before pause |
| 258 | XFER-13 | Pause twice |
| 259 | XFER-14 | Resume twice |
| 260 | XFER-15 | Cancel twice |
| 261 | XFER-16 | Lifecycle action during transfer |
| 262 | XFER-17 | Cloud change during transfer |
| 263 | XFER-18 | Download missing object |

#### DOWNLOAD -- download-specific edge cases (# 264-269)
| # | ID | Name |
|---|---|---|
| 264 | DOWNLOAD-01 | Ejected/unmounted destination |
| 265 | DOWNLOAD-02 | Missing object |
| 266 | DOWNLOAD-03 | Invalid destination |
| 267 | DOWNLOAD-04 | Cloud permission denied |
| 268 | DOWNLOAD-05 | Download while upload active |
| 269 | DOWNLOAD-06 | Cancel/pause/resume duplicate |

#### STATE -- transfer state-machine transitions (# 270-282)
| # | ID | Name |
|---|---|---|
| 270 | STATE-01 | IN_PROGRESS -> PAUSE -> PAUSE |
| 271 | STATE-02 | IN_PROGRESS -> PAUSE -> RESUME -> RESUME |
| 272 | STATE-03 | IN_PROGRESS -> PAUSE -> CANCEL -> CANCEL |
| 273 | STATE-04 | IN_PROGRESS -> RESUME |
| 274 | STATE-05 | IN_PROGRESS -> CANCEL -> CANCEL |
| 275 | STATE-06 | PAUSED -> PAUSE |
| 276 | STATE-07 | PAUSED -> RESUME -> RESUME |
| 277 | STATE-08 | PAUSED -> CANCEL -> CANCEL |
| 278 | STATE-09 | PAUSED -> EJECT/FORMAT/ERASE |
| 279 | STATE-10 | COMPLETED -> PAUSE/RESUME/CANCEL |
| 280 | STATE-11 | CANCELLED -> PAUSE/RESUME/CANCEL |
| 281 | STATE-12 | Unknown transfer state |
| 282 | STATE-13 | Rejected operation state audit |

#### RACE -- concurrent/racing operations (# 283-293)
| # | ID | Name |
|---|---|---|
| 283 | RACE-01 | Pause + cancel |
| 284 | RACE-02 | Resume + cancel |
| 285 | RACE-03 | Pause + pause |
| 286 | RACE-04 | Resume + resume |
| 287 | RACE-05 | Cancel + cancel |
| 288 | RACE-06 | Transfer + lifecycle |
| 289 | RACE-07 | Upload + download |
| 290 | RACE-08 | Upload + upload |
| 291 | RACE-09 | Download + download |
| 292 | RACE-10 | Operation + deconfigure |
| 293 | RACE-11 | Invalid-ID ops + live transfer |

#### DUP -- duplicate operation calls (# 294-298)
| # | ID | Name |
|---|---|---|
| 294 | DUP-01 | Duplicate configure |
| 295 | DUP-02 | Duplicate deconfigure |
| 296 | DUP-03 | Duplicate mount/eject |
| 297 | DUP-04 | Duplicate report |
| 298 | DUP-05 | Repeated status |

#### REPORT -- report generation across states (# 299-309)
| # | ID | Name |
|---|---|---|
| 299 | REPORT-01 | Invalid ID/missing directory |
| 300 | REPORT-02 | Empty ID |
| 301 | REPORT-03 | Before transfer |
| 302 | REPORT-04 | During IN_PROGRESS |
| 303 | REPORT-05 | During PAUSED |
| 304 | REPORT-06 | During cancellation |
| 305 | REPORT-07 | After CANCELLED |
| 306 | REPORT-08 | After COMPLETED |
| 307 | REPORT-09 | Output is unwritable/file |
| 308 | REPORT-10 | Duplicate generation |
| 309 | REPORT-11 | During transition |

#### FAULT -- API/SSH fault injection (# 310-314)
| # | ID | Name |
|---|---|---|
| 310 | FAULT-01 | API unavailable/timeout/reset |
| 311 | FAULT-02 | HTTP 400/401/403/404/409/500 |
| 312 | FAULT-03 | Malformed API response |
| 313 | FAULT-04 | SSH unavailable/timeout |
| 314 | FAULT-05 | SSH connection drop |

#### REC -- recovery (service restart / kill / network) (# 315-319)
| # | ID | Name |
|---|---|---|
| 315 | REC-01 | Restart service during upload/download |
| 316 | REC-02 | Kill runner during transfer |
| 317 | REC-03 | Network interruption and restore |
| 318 | REC-04 | Status after restart/reboot (excluded) |
| 319 | REC-05 | Configure after recovery |

#### VERIFY -- post-transfer data verification (# 320-324)
| # | ID | Name |
|---|---|---|
| 320 | VERIFY-01 | Missing objects after completion |
| 321 | VERIFY-02 | Partial objects after failure |
| 322 | VERIFY-03 | Incorrect transferred size |
| 323 | VERIFY-04 | Remains active after completion |
| 324 | VERIFY-05 | Remains paused after resume |

#### INT -- integrity during interruption (# 325-335)
| # | ID | Name |
|---|---|---|
| 325 | INT-01 | Objects missing after completed upload |
| 326 | INT-02 | Partial objects after failed upload |
| 327 | INT-03 | Incorrect transferred size |
| 328 | INT-04 | Source deleted during upload |
| 329 | INT-05 | Source modified during upload |
| 330 | INT-06 | Source directory renamed |
| 331 | INT-07 | Object deleted during download |
| 332 | INT-08 | Destination removed during download |
| 333 | INT-09 | Partial upload/download then resume |
| 334 | INT-10 | Cancel then new transfer |
| 335 | INT-11 | Interrupted transfer then status/report |

#### CLEAN -- cleanup / post-cancel-completion audits (# 336-347)
| # | ID | Name |
|---|---|---|
| 336 | CLEAN-01 | Cancel then eject |
| 337 | CLEAN-02 | Cancel then format |
| 338 | CLEAN-03 | Cancel then mount |
| 339 | CLEAN-04 | Cancel then deconfigure |
| 340 | CLEAN-05 | Failed transfer follow-up |
| 341 | CLEAN-06 | New transfer after failed/cancelled |
| 342 | CLEAN-07 | Deconfigure after completion |
| 343 | CLEAN-08 | Eject after completion |
| 344 | CLEAN-09 | Final transfer audit |
| 345 | CLEAN-10 | Final device audit |
| 346 | CLEAN-11 | Dataset audit |
| 347 | CLEAN-12 | Process audit |

#### MGMT -- device management (network/NTP/date/remove) (# 348-357)
| # | ID | Name |
|---|---|---|
| 348 | MGMT-01 | Network info while ejected/unmounted |
| 349 | MGMT-02 | Invalid IP address |
| 350 | MGMT-03 | Invalid netmask |
| 351 | MGMT-04 | Invalid/unreachable NTP server |
| 352 | MGMT-05 | Invalid calendar date |
| 353 | MGMT-06 | Invalid time-of-day |
| 354 | MGMT-07 | Report while ejected/unmounted |
| 355 | MGMT-08 | Remove while mounted |
| 356 | MGMT-09 | Remove then rescan recovery |
| 357 | MGMT-10 | Duplicate NTP configuration |

#### SVC -- systemd service stop/restart during ops, 15 services x 3 ops (# 358-402, needs `--allow-service-faults`)
| # | ID | Name |
|---|---|---|
| 358 | SVC-01 | stop_active_transfer (bcloud.service) |
| 359 | SVC-02 | restart_active_transfer (bcloud.service) |
| 360 | SVC-03 | stop_before_mgmt_op (bcloud.service) |
| 361 | SVC-04 | stop_active_transfer (bryckcp.service) |
| 362 | SVC-05 | restart_active_transfer (bryckcp.service) |
| 363 | SVC-06 | stop_before_mgmt_op (bryckcp.service) |
| 364 | SVC-07 | stop_active_transfer (bryckmonitor.service) |
| 365 | SVC-08 | restart_active_transfer (bryckmonitor.service) |
| 366 | SVC-09 | stop_before_mgmt_op (bryckmonitor.service) |
| 367 | SVC-10 | stop_active_transfer (bryckobjectstore.service.new) |
| 368 | SVC-11 | restart_active_transfer (bryckobjectstore.service.new) |
| 369 | SVC-12 | stop_before_mgmt_op (bryckobjectstore.service.new) |
| 370 | SVC-13 | stop_active_transfer (bryckagentbsmb.service) |
| 371 | SVC-14 | restart_active_transfer (bryckagentbsmb.service) |
| 372 | SVC-15 | stop_before_mgmt_op (bryckagentbsmb.service) |
| 373 | SVC-16 | stop_active_transfer (bryck-info-trigger.service) |
| 374 | SVC-17 | restart_active_transfer (bryck-info-trigger.service) |
| 375 | SVC-18 | stop_before_mgmt_op (bryck-info-trigger.service) |
| 376 | SVC-19 | stop_active_transfer (bryckmonitor_worker.service) |
| 377 | SVC-20 | restart_active_transfer (bryckmonitor_worker.service) |
| 378 | SVC-21 | stop_before_mgmt_op (bryckmonitor_worker.service) |
| 379 | SVC-22 | stop_active_transfer (bstream.service) |
| 380 | SVC-23 | restart_active_transfer (bstream.service) |
| 381 | SVC-24 | stop_before_mgmt_op (bstream.service) |
| 382 | SVC-25 | stop_active_transfer (bryckagentlc.service) |
| 383 | SVC-26 | restart_active_transfer (bryckagentlc.service) |
| 384 | SVC-27 | stop_before_mgmt_op (bryckagentlc.service) |
| 385 | SVC-28 | stop_active_transfer (bryckmonitor_alert.service) |
| 386 | SVC-29 | restart_active_transfer (bryckmonitor_alert.service) |
| 387 | SVC-30 | stop_before_mgmt_op (bryckmonitor_alert.service) |
| 388 | SVC-31 | stop_active_transfer (bryckobjectstore.service) |
| 389 | SVC-32 | restart_active_transfer (bryckobjectstore.service) |
| 390 | SVC-33 | stop_before_mgmt_op (bryckobjectstore.service) |
| 391 | SVC-34 | stop_active_transfer (bryckapi.service) |
| 392 | SVC-35 | restart_active_transfer (bryckapi.service) |
| 393 | SVC-36 | stop_before_mgmt_op (bryckapi.service) |
| 394 | SVC-37 | stop_active_transfer (bryckmonitor_prune_db.service) |
| 395 | SVC-38 | restart_active_transfer (bryckmonitor_prune_db.service) |
| 396 | SVC-39 | stop_before_mgmt_op (bryckmonitor_prune_db.service) |
| 397 | SVC-40 | stop_active_transfer (redis.service) |
| 398 | SVC-41 | restart_active_transfer (redis.service) |
| 399 | SVC-42 | stop_before_mgmt_op (redis.service) |
| 400 | SVC-43 | stop_active_transfer (minio.service) |
| 401 | SVC-44 | restart_active_transfer (minio.service) |
| 402 | SVC-45 | stop_before_mgmt_op (minio.service) |

#### SM -- transfer state matrix, 5 states x every op (# 403-430)
| # | ID | Name |
|---|---|---|
| 403 | SM-01 | CREATED -> Status |
| 404 | SM-02 | CREATED -> Pause |
| 405 | SM-03 | CREATED -> Resume |
| 406 | SM-04 | CREATED -> Cancel |
| 407 | SM-05 | IN_PROGRESS -> Status |
| 408 | SM-06 | IN_PROGRESS -> Pause |
| 409 | SM-07 | IN_PROGRESS -> Resume |
| 410 | SM-08 | IN_PROGRESS -> Cancel |
| 411 | SM-09 | IN_PROGRESS -> Eject |
| 412 | SM-10 | IN_PROGRESS -> Format |
| 413 | SM-11 | IN_PROGRESS -> Mount |
| 414 | SM-12 | IN_PROGRESS -> Deconfigure |
| 415 | SM-13 | PAUSED -> Status |
| 416 | SM-14 | PAUSED -> Pause |
| 417 | SM-15 | PAUSED -> Resume |
| 418 | SM-16 | PAUSED -> Cancel |
| 419 | SM-17 | PAUSED -> Eject |
| 420 | SM-18 | PAUSED -> Format |
| 421 | SM-19 | PAUSED -> Mount |
| 422 | SM-20 | PAUSED -> Deconfigure |
| 423 | SM-21 | COMPLETED -> Status |
| 424 | SM-22 | COMPLETED -> Pause |
| 425 | SM-23 | COMPLETED -> Resume |
| 426 | SM-24 | COMPLETED -> Cancel |
| 427 | SM-25 | CANCELLED -> Status |
| 428 | SM-26 | CANCELLED -> Pause |
| 429 | SM-27 | CANCELLED -> Resume |
| 430 | SM-28 | CANCELLED -> Cancel |

#### F -- combined end-to-end flow scenarios (# 431-470)
| # | ID | Name |
|---|---|---|
| 431 | F-01 | P0 IP Change -> Format -> Mount -> Upload Negative Matrix |
| 432 | F-02 | P0 IP Change -> Format -> Mount -> Download Negative Matrix |
| 433 | F-03 | Format Without Eject -> Recovery |
| 434 | F-04 | Active Upload -> All Management Conflicts |
| 435 | F-05 | Paused Upload -> All Management Conflicts |
| 436 | F-06 | Resume Race -> Management Conflicts |
| 437 | F-07 | Active Download -> All Management Conflicts |
| 438 | F-08 | Upload Pause/Resume Repetition |
| 439 | F-09 | Upload Pause -> Cancel -> Cleanup |
| 440 | F-10 | Upload Active -> Cancel Immediately -> New Upload |
| 441 | F-11 | Completed Upload -> Invalid Operations |
| 442 | F-12 | Completed Download -> Invalid Operations |
| 443 | F-13 | Upload + Upload Concurrent |
| 444 | F-14 | Upload + Download Concurrent |
| 445 | F-15 | Pause + Deconfigure Race |
| 446 | F-16 | Resume + Deconfigure Race |
| 447 | F-17 | Pause + Cancel Race |
| 448 | F-18 | Resume + Cancel Race |
| 449 | F-19 | Eject + Cancel Race |
| 450 | F-20 | Format + Cancel Race |
| 451 | F-21 | API Failure During Active Upload |
| 452 | F-22 | SSH Failure During Dataset Generation |
| 453 | F-23 | Service Restart During Upload |
| 454 | F-24 | Service Restart During Pause |
| 455 | F-25 | Token Expiry During Upload |
| 456 | F-26 | Token Expiry During Paused Transfer |
| 457 | F-27 | Network Loss During Upload |
| 458 | F-28 | Network Loss During Download |
| 459 | F-29 | Report At Every Transfer State |
| 460 | F-30 | Format/Eject/Mount State Cycle With Transfer Attempts |
| 461 | F-31 | Dataset Path Mismatch Flow |
| 462 | F-32 | Insufficient Space Flow |
| 463 | F-33 | Invalid AWS Permission Flow |
| 464 | F-34 | Cancel -> Deconfigure -> Eject -> Reconfigure -> New Transfer |
| 465 | F-35 | Completed -> Deconfigure -> Eject -> Reconfigure -> New Transfer |
| 466 | F-36 | Failed Transfer -> Recovery -> New Transfer |
| 467 | F-37 | System Reboot During Active Transfer (excluded) |
| 468 | F-38 | System Reboot During Paused Transfer (excluded) |
| 469 | F-39 | Full Upload Negative Regression |
| 470 | F-40 | Full Download Negative Regression |

#### MASTER -- end-to-end master flows, still unimplemented stubs (# 471-473)
| # | ID | Name |
|---|---|---|
| 471 | MASTER-UPLOAD | P0 end-to-end upload flow (format/mount/configure/upload/pause/resume/blocked-destructive-attempts/completion/cleanup) |
| 472 | MASTER-DOWNLOAD | P0 end-to-end download flow (seed upload, then format/mount/configure/download/pause/resume/blocked-destructive-attempts/completion/cleanup) |
| 473 | MASTER-BOTH | P0 end-to-end both flow (upload leg then download leg in one continuous session) |

**Total: 473 cases** -- 162 transfer + 311 negative (308 implemented, 3 `MASTER-*` stubs).

Notes:
- `SVC-*` cases additionally need `--allow-service-faults`; destructive `LIFE-*`/`F-*` ops need `--confirm-destructive`; `REC-03` needs `--allow-network-faults`; `REC-04`/`F-37`/`F-38` are permanently excluded (reboot cases).
- `--skip-cancel-ops` makes only the actual cancel-transfer step a no-op across every case above that issues one, without skipping the rest of that case's flow.
- For the exact raw `python3 cloud_full_test.py --list` output (fixed-width, unmodified), run the command directly -- this section is a reorganized, easier-to-scan view of the same data, not a replacement for it.
