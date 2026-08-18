from __future__ import annotations

import html
import json
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional


def status_class(status: str) -> str:
    return "ok" if status == "PASSED" else "bad"


def render_run_html(report: Dict[str, object], full_log: str) -> str:
    status = str(report.get("status", "UNKNOWN"))
    summary_total = report.get("summary_total", {}) if isinstance(report.get("summary_total"), dict) else {}
    perf = report.get("performance", {}) if isinstance(report.get("performance"), dict) else {}
    steps = report.get("steps", []) if isinstance(report.get("steps"), list) else []
    issues = report.get("issues", []) if isinstance(report.get("issues"), list) else []
    records = report.get("records", "-")

    def _float_or_none(value: object) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        return None

    cpu_value = _float_or_none(perf.get("cpu_percent"))
    temp_avg_value = _float_or_none(perf.get("temp_avg_c"))
    cpu_bar_pct = max(0.0, min(100.0, cpu_value if cpu_value is not None else 0.0))
    temp_bar_pct = max(0.0, min(100.0, temp_avg_value if temp_avg_value is not None else 0.0))

    step_rows = "\n".join(
        "<tr><td>{}</td><td>{}</td></tr>".format(
            html.escape(str(s.get("step", ""))), html.escape(str(s.get("details", "")))
        )
        for s in steps
    )
    if not step_rows:
        step_rows = "<tr><td colspan='2'>No steps recorded.</td></tr>"

    issue_rows = "\n".join("<li>{}</li>".format(html.escape(str(i))) for i in issues)
    if not issue_rows:
        issue_rows = "<li>None</li>"

    drive_rows = []
    temperature = report.get("temperature", {}) if isinstance(report.get("temperature"), dict) else {}
    drives = temperature.get("drives", []) if isinstance(temperature.get("drives"), list) else []
    for drive in drives:
        drive_rows.append(
            "<tr>"
            f"<td>{html.escape(str(drive.get('dev', '-')))}</td>"
            f"<td>{html.escape(str(drive.get('sn', '-')))}</td>"
            f"<td>{html.escape(str(drive.get('samples', '-')))}</td>"
            f"<td>{html.escape(str(drive.get('min_c', '-')))}</td>"
            f"<td>{html.escape(str(drive.get('avg_c', '-')))}</td>"
            f"<td>{html.escape(str(drive.get('max_c', '-')))}</td>"
            "</tr>"
        )
    drive_rows_html = "\n".join(drive_rows) if drive_rows else "<tr><td colspan='6'>No per-drive telemetry captured.</td></tr>"

    disk_usage = report.get("disk_usage", {}) if isinstance(report.get("disk_usage"), dict) else {}
    disk_usage_items = [
        f"<li>filesystem: {html.escape(str(disk_usage.get('filesystem', '-')))}</li>",
        f"<li>size_kb: {html.escape(str(disk_usage.get('size_kb', '-')))}</li>",
        f"<li>used_kb: {html.escape(str(disk_usage.get('used_kb', '-')))}</li>",
        f"<li>avail_kb: {html.escape(str(disk_usage.get('avail_kb', '-')))}</li>",
        f"<li>used_pct: {html.escape(str(disk_usage.get('used_pct', '-')))}</li>",
        f"<li>mount: {html.escape(str(disk_usage.get('mount', '-')))}</li>",
    ] if disk_usage else ["<li>No disk usage captured.</li>"]

    template = """<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>BatchBuilder Detailed Run Report</title>
    <style>
        :root { --bg:#f3f8f5; --fg:#111827; --muted:#4b5563; --card:#ffffff; --ok:#0f766e; --bad:#b91c1c; --line:#dbe3ef; --brand:#0f5132; --accent:#f59e0b; }
        body { margin:0; background:radial-gradient(circle at 100% 0,#d1fae5 0,#ecfdf5 35%,#f8fafc 100%); color:var(--fg); font:14px/1.45 Segoe UI, Arial, sans-serif; }
        .wrap { max-width:1320px; margin:24px auto; padding:0 16px; }
        .page { display:grid; grid-template-columns:240px 1fr; gap:14px; align-items:start; }
        .sidebar { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; position:sticky; top:12px; }
        .sidebar h3 { margin:0 0 8px; font-size:13px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }
        .sidebar a { display:block; text-decoration:none; color:var(--fg); border:1px solid var(--line); border-radius:8px; padding:7px 9px; margin-bottom:8px; }
        .sidebar a:hover { background:#f0fdf4; }
        .sidebar ul { margin:8px 0 0; padding-left:18px; background:transparent; border:none; }
        .nav { margin-bottom:12px; display:flex; gap:10px; flex-wrap:wrap; }
        .btn { display:inline-block; text-decoration:none; border:1px solid var(--line); background:var(--card); color:var(--fg); border-radius:999px; padding:7px 12px; font-weight:600; }
        .btn:hover { background:#f0fdf4; }
        .head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; }
        h1 { margin:0; font-size:24px; }
        .sub { color:var(--muted); margin-top:6px; }
        .badge { padding:6px 12px; border-radius:999px; font-weight:600; color:#fff; }
        .badge.ok { background:var(--ok); }
        .badge.bad { background:var(--bad); }
        .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin:16px 0; }
        .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px; }
        .k { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
        .v { margin-top:4px; font-size:16px; font-weight:600; word-break:break-word; }
        h2 { margin:18px 0 10px; font-size:18px; }
        table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }
        th, td { border-bottom:1px solid var(--line); padding:10px; text-align:left; vertical-align:top; }
        th { background:#f3f4f6; font-weight:600; }
        tr:last-child td { border-bottom:none; }
        ul { margin:0; padding-left:20px; background:var(--card); border:1px solid var(--line); border-radius:10px; padding-top:10px; padding-bottom:10px; }
        pre { white-space:pre-wrap; word-break:break-word; background:#0b1220; color:#d1e7ff; border-radius:10px; padding:12px; border:1px solid #1f2937; }
        .foot { color:var(--muted); margin-top:18px; font-size:12px; }
        .perf { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-top:8px; }
        .mini-charts { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; margin-top:12px; }
        .meter { margin-top:8px; }
        .meter .label { display:flex; justify-content:space-between; color:var(--muted); font-size:12px; }
        .track { height:10px; border-radius:999px; background:#e5e7eb; overflow:hidden; margin-top:6px; }
        .fill { height:100%; background:linear-gradient(90deg,var(--brand),var(--accent)); }
        details { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:10px; }
        summary { cursor:pointer; font-weight:600; }
        @media (max-width: 1080px) { .page { grid-template-columns:1fr; } .sidebar { position:static; } }
    </style>
</head>
<body>
    <div class=\"wrap\">
        <div class=\"page\">
        <aside class=\"sidebar\">
            <h3>Run Navigation</h3>
            <a href=\"../index.html\">Back To Runs</a>
            <a href=\"../../shareable_report.html\">Back To Suite</a>
            <a href=\"#perf\">Performance</a>
            <a href=\"#temp\">Temperature</a>
            <a href=\"#drive-temp\">Drive Temperatures</a>
            <a href=\"#disk-usage\">Disk Usage</a>
            <a href=\"#issues\">Issues</a>
            <h3>Color Guide</h3>
            <ul>
                <li>Green badge: passed</li>
                <li>Red badge: failed</li>
                <li>Bar fill: higher value</li>
            </ul>
        </aside>
        <div>
        <div class="nav">
            <a class="btn" href="../index.html">Back To Runs</a>
            <a class="btn" href="../../shareable_report.html">Back To Suite</a>
        </div>

        <div class=\"head\">
            <div>
                <h1>Remote BatchBuilder Validation</h1>
                <div class=\"sub\">YAML transfer, CSV conversion, BatchBuilder execution, and summary cross-validation.</div>
            </div>
            <div class=\"badge __STATUS_CLASS__\">__STATUS__</div>
        </div>

        <div class=\"grid\">
            <div class=\"card\"><div class=\"k\">Dataset</div><div class=\"v\">__DATASET__</div></div>
            <div class=\"card\"><div class=\"k\">Source Host</div><div class=\"v\">__SOURCE_HOST__</div></div>
            <div class=\"card\"><div class=\"k\">Destination Host</div><div class=\"v\">__DEST_HOST__</div></div>
            <div class=\"card\"><div class=\"k\">Records</div><div class=\"v\">__RECORDS__</div></div>
            <div class=\"card\"><div class=\"k\">Batch Count</div><div class=\"v\">__BATCH_COUNT__</div></div>
            <div class=\"card\"><div class=\"k\">File Count</div><div class=\"v\">__FILE_COUNT__</div></div>
            <div class=\"card\"><div class=\"k\">Total Bytes</div><div class=\"v\">__TOTAL_BYTES__</div></div>
            <div class=\"card\"><div class=\"k\">Timestamp</div><div class=\"v\">__TIMESTAMP__</div></div>
        </div>

        <h2 id="perf">Performance</h2>
        <div class=\"perf\">
            <div class=\"card\"><div class=\"k\">Elapsed (sec)</div><div class=\"v\">__ELAPSED_SEC__</div></div>
            <div class=\"card\"><div class=\"k\">CPU Utilization</div><div class=\"v\">__CPU_PERCENT__%</div></div>
            <div class=\"card\"><div class=\"k\">Peak Memory (MB)</div><div class=\"v\">__MAX_RSS_MB__</div></div>
            <div class=\"card\"><div class=\"k\">Rows / Sec</div><div class=\"v\">__ROWS_PER_SEC__</div></div>
            <div class=\"card\"><div class=\"k\">Batches / Sec</div><div class=\"v\">__BATCHES_PER_SEC__</div></div>
            <div class=\"card\"><div class=\"k\">Bytes / Sec</div><div class=\"v\">__BYTES_PER_SEC__</div></div>
            <div class=\"card\"><div class=\"k\">Sec / Batch</div><div class=\"v\">__SECONDS_PER_BATCH__</div></div>
            <div class=\"card\"><div class=\"k\">Bytes / Record</div><div class=\"v\">__BYTES_PER_RECORD__</div></div>
        </div>

        <h2 id="temp">Temperature</h2>
        <div class=\"perf\">
            <div class=\"card\"><div class=\"k\">Telemetry Supported</div><div class=\"v\">__TEMP_SUPPORTED__</div></div>
            <div class=\"card\"><div class=\"k\">Samples</div><div class=\"v\">__TEMP_SAMPLES__</div></div>
            <div class=\"card\"><div class=\"k\">Min Temp C</div><div class=\"v\">__TEMP_MIN_C__</div></div>
            <div class=\"card\"><div class=\"k\">Avg Temp C</div><div class=\"v\">__TEMP_AVG_C__</div></div>
            <div class=\"card\"><div class=\"k\">Max Temp C</div><div class=\"v\">__TEMP_MAX_C__</div></div>
        </div>

        <div class="mini-charts">
            <div class="card meter">
                <div class="k">CPU Usage Snapshot</div>
                <div class="label"><span>CPU %</span><span>__CPU_PERCENT__%</span></div>
                <div class="track"><div class="fill" style="width:__CPU_BAR_PCT__%;"></div></div>
            </div>
            <div class="card meter">
                <div class="k">Temperature Snapshot</div>
                <div class="label"><span>Avg Temp C</span><span>__TEMP_AVG_C__</span></div>
                <div class="track"><div class="fill" style="width:__TEMP_BAR_PCT__%;"></div></div>
            </div>
        </div>

        <h2 id="drive-temp">Per-Drive Temperature</h2>
        <table>
            <thead><tr><th>Device</th><th>Serial</th><th>Samples</th><th>Min C</th><th>Avg C</th><th>Max C</th></tr></thead>
            <tbody>
                __DRIVE_ROWS__
            </tbody>
        </table>

        <h2 id="disk-usage">Disk Usage</h2>
        <ul>
            __DISK_USAGE_ITEMS__
        </ul>

        <h2>Execution Steps</h2>
        <table>
            <thead><tr><th>Step</th><th>Details</th></tr></thead>
            <tbody>
                __STEP_ROWS__
            </tbody>
        </table>

        <h2 id="issues">Issues</h2>
        <ul>
            __ISSUE_ROWS__
        </ul>

        <h2>Execution Log</h2>
        <details open>
            <summary>Detailed command log</summary>
            <pre>__FULL_LOG__</pre>
        </details>

        <div class=\"foot\">Generated automatically at run completion.</div>
        </div>
        </div>
    </div>
</body>
</html>
"""

    replacements = {
        "__STATUS__": html.escape(status),
        "__STATUS_CLASS__": status_class(status),
        "__DATASET__": html.escape(str(report.get("dataset", "-"))),
        "__SOURCE_HOST__": html.escape(str(report.get("source_host", "-"))),
        "__DEST_HOST__": html.escape(str(report.get("dest_host", "-"))),
        "__RECORDS__": html.escape(str(records)),
        "__BATCH_COUNT__": html.escape(str(summary_total.get("batch_count", "-"))),
        "__FILE_COUNT__": html.escape(str(summary_total.get("file_count", "-"))),
        "__TOTAL_BYTES__": html.escape(str(summary_total.get("total_bytes", "-"))),
        "__TIMESTAMP__": html.escape(str(report.get("timestamp", "-"))),
        "__STEP_ROWS__": step_rows,
        "__ISSUE_ROWS__": issue_rows,
        "__FULL_LOG__": html.escape(full_log or "No execution log available."),
        "__ELAPSED_SEC__": html.escape(str(perf.get("elapsed_sec", "-"))),
        "__CPU_PERCENT__": html.escape(str(perf.get("cpu_percent", "-"))),
        "__MAX_RSS_MB__": html.escape(str(perf.get("max_rss_mb", "-"))),
        "__ROWS_PER_SEC__": html.escape(str(perf.get("rows_per_sec", "-"))),
        "__BATCHES_PER_SEC__": html.escape(str(perf.get("batches_per_sec", "-"))),
        "__BYTES_PER_SEC__": html.escape(str(perf.get("bytes_per_sec", "-"))),
        "__SECONDS_PER_BATCH__": html.escape(str(perf.get("seconds_per_batch", "-"))),
        "__BYTES_PER_RECORD__": html.escape(str(perf.get("bytes_per_record", "-"))),
        "__TEMP_SUPPORTED__": html.escape(str(perf.get("temperature_supported", "-"))),
        "__TEMP_SAMPLES__": html.escape(str(perf.get("temp_samples", "-"))),
        "__TEMP_MIN_C__": html.escape(str(perf.get("temp_min_c", "-"))),
        "__TEMP_AVG_C__": html.escape(str(perf.get("temp_avg_c", "-"))),
        "__TEMP_MAX_C__": html.escape(str(perf.get("temp_max_c", "-"))),
        "__DRIVE_ROWS__": drive_rows_html,
        "__DISK_USAGE_ITEMS__": "\n".join(disk_usage_items),
        "__CPU_BAR_PCT__": f"{cpu_bar_pct:.2f}",
        "__TEMP_BAR_PCT__": f"{temp_bar_pct:.2f}",
    }

    for key, value in replacements.items():
        template = template.replace(key, value)

    return template


def build_master_index_html(artifacts_root: Path) -> str:
    rows = []
    for report_path in sorted(artifacts_root.glob("run_*/validation_report.json"), reverse=True):
        run_dir = report_path.parent
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        summary_total = report.get("summary_total", {}) if isinstance(report.get("summary_total"), dict) else {}
        status = str(report.get("status", "UNKNOWN"))
        run_name = run_dir.name

        rows.append(
            "<tr>"
            f"<td>{html.escape(run_name)}</td>"
            f"<td>{html.escape(str(report.get('dataset', '-')))}</td>"
            f"<td><span class='badge {status_class(status)}'>{html.escape(status)}</span></td>"
            f"<td>{html.escape(str(report.get('records', '-')))}</td>"
            f"<td>{html.escape(str(summary_total.get('batch_count', '-')))}</td>"
            f"<td>{html.escape(str(summary_total.get('file_count', '-')))}</td>"
            f"<td>{html.escape(str(report.get('timestamp', '-')))}</td>"
            f"<td><a href='{run_name}/validation_report.html'>HTML</a> | <a href='{run_name}/validation_report.md'>MD</a> | <a href='{run_name}/validation_report.json'>JSON</a></td>"
            "</tr>"
        )

    body_rows = "\n".join(rows) if rows else "<tr><td colspan='8'>No completed reports found.</td></tr>"
    return f"""<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Remote BatchBuilder Validation Dashboard</title>
    <style>
        :root {{ --bg:#f7f8fa; --fg:#111827; --muted:#4b5563; --card:#ffffff; --ok:#0f766e; --bad:#b91c1c; --line:#e5e7eb; }}
        body {{ margin:0; background:var(--bg); color:var(--fg); font:14px/1.45 Segoe UI, Arial, sans-serif; }}
        .wrap {{ max-width:1200px; margin:24px auto; padding:0 16px; }}
        table {{ width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
        th, td {{ border-bottom:1px solid var(--line); padding:10px; text-align:left; }}
        th {{ background:#f3f4f6; }}
        tr:last-child td {{ border-bottom:none; }}
        .badge {{ padding:4px 9px; border-radius:999px; font-weight:600; color:#fff; }}
        .badge.ok {{ background:var(--ok); }}
        .badge.bad {{ background:var(--bad); }}
    </style>
</head>
<body>
    <div class=\"wrap\">
        <h1>Remote BatchBuilder Validation Dashboard</h1>
        <table>
            <thead>
                <tr>
                    <th>Run</th>
                    <th>Dataset</th>
                    <th>Status</th>
                    <th>Records</th>
                    <th>Batch Count</th>
                    <th>File Count</th>
                    <th>Timestamp</th>
                    <th>Artifacts</th>
                </tr>
            </thead>
            <tbody>
                {body_rows}
            </tbody>
        </table>
    </div>
</body>
</html>
"""


def to_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def stat_triplet(values: List[float]) -> Dict[str, object]:
    if not values:
        return {"min": None, "avg": None, "max": None, "count": 0}
    return {
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
        "count": len(values),
    }


def fmt(value: object, precision: int = 2) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.{precision}f}"
    return "-"


def _safe_div(numerator: object, denominator: object) -> Optional[float]:
    if not isinstance(numerator, (int, float)):
        return None
    if not isinstance(denominator, (int, float)):
        return None
    if float(denominator) == 0.0:
        return None
    return float(numerator) / float(denominator)


def extract_run_metrics(run: Dict[str, object]) -> Dict[str, object]:
    report = run.get("report", {}) if isinstance(run.get("report"), dict) else {}
    summary = report.get("summary_total", {}) if isinstance(report.get("summary_total"), dict) else {}
    perf = report.get("performance", {}) if isinstance(report.get("performance"), dict) else {}

    records = report.get("records", "-")
    batch_count = summary.get("batch_count", "-")
    total_bytes = summary.get("total_bytes", "-")
    elapsed_sec = perf.get("elapsed_sec", "-")
    rows_per_sec = perf.get("rows_per_sec", "-")
    batches_per_sec = perf.get("batches_per_sec", "-")
    bytes_per_sec = perf.get("bytes_per_sec", "-")
    seconds_per_batch = perf.get("seconds_per_batch", "-")
    bytes_per_record = perf.get("bytes_per_record", "-")

    records_per_batch = _safe_div(records, batch_count)
    bytes_per_batch = _safe_div(total_bytes, batch_count)
    ms_per_record = _safe_div(elapsed_sec, records)
    if ms_per_record is not None:
        ms_per_record *= 1000.0

    if not isinstance(batches_per_sec, (int, float)):
        batches_per_sec = _safe_div(batch_count, elapsed_sec)
    if not isinstance(bytes_per_sec, (int, float)):
        bytes_per_sec = _safe_div(total_bytes, elapsed_sec)
    if not isinstance(seconds_per_batch, (int, float)):
        seconds_per_batch = _safe_div(elapsed_sec, batch_count)
    if not isinstance(bytes_per_record, (int, float)):
        bytes_per_record = _safe_div(total_bytes, records)
    if not isinstance(rows_per_sec, (int, float)):
        rows_per_sec = _safe_div(records, elapsed_sec)

    return {
        "dataset": run.get("dataset", "-"),
        "run_name": run.get("run_name", "-"),
        "status": run.get("status", "FAILED"),
        "records": records,
        "batch_count": batch_count,
        "file_count": summary.get("file_count", "-"),
        "total_bytes": total_bytes,
        "elapsed_sec": elapsed_sec,
        "rows_per_sec": rows_per_sec,
        "cpu_percent": perf.get("cpu_percent", "-"),
        "max_rss_mb": perf.get("max_rss_mb", "-"),
        "seconds_per_batch": seconds_per_batch,
        "bytes_per_record": bytes_per_record,
        "batches_per_sec": batches_per_sec,
        "bytes_per_sec": bytes_per_sec,
        "records_per_batch": records_per_batch if records_per_batch is not None else "-",
        "bytes_per_batch": bytes_per_batch if bytes_per_batch is not None else "-",
        "ms_per_record": ms_per_record if ms_per_record is not None else "-",
        "perf_warnings": [],
        "temp_supported": perf.get("temperature_supported", False),
        "temp_samples": perf.get("temp_samples", "-"),
        "temp_min_c": perf.get("temp_min_c", "-"),
        "temp_avg_c": perf.get("temp_avg_c", "-"),
        "temp_max_c": perf.get("temp_max_c", "-"),
        "issues": report.get("issues", []),
    }


def compute_variation(metrics_rows: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    numeric_map = {
        "rows_per_sec": [],
        "cpu_percent": [],
        "max_rss_mb": [],
        "seconds_per_batch": [],
        "records_per_batch": [],
        "bytes_per_batch": [],
        "ms_per_record": [],
        "temp_min_c": [],
        "temp_avg_c": [],
        "temp_max_c": [],
    }

    for row in metrics_rows:
        for key in numeric_map:
            value = to_float(row.get(key))
            if value is not None:
                numeric_map[key].append(value)

    return {key: stat_triplet(values) for key, values in numeric_map.items()}


def load_baselines(path: Path) -> Dict[str, Dict[str, List[float]]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    baseline: Dict[str, Dict[str, List[float]]] = {}
    for dataset, metrics in payload.items():
        if not isinstance(dataset, str) or not isinstance(metrics, dict):
            continue
        row: Dict[str, List[float]] = {}
        for key, values in metrics.items():
            if isinstance(key, str) and isinstance(values, list):
                numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                row[key] = numeric_values
        baseline[dataset] = row
    return baseline


def save_baselines(path: Path, baseline: Dict[str, Dict[str, List[float]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(baseline, indent=2), encoding="utf-8")


def annotate_and_update_performance_warnings(
    metrics_rows: List[Dict[str, object]],
    baseline_path: Path,
    history_limit: int = 25,
) -> Dict[str, Dict[str, List[float]]]:
    baseline = load_baselines(baseline_path)
    tracked_metrics = ["rows_per_sec", "seconds_per_batch", "cpu_percent", "records_per_batch", "ms_per_record"]

    for row in metrics_rows:
        row.setdefault("perf_warnings", [])
        if str(row.get("status", "FAILED")) != "PASSED":
            continue

        dataset = str(row.get("dataset", "-"))
        entry = baseline.setdefault(dataset, {})
        for metric in tracked_metrics:
            entry.setdefault(metric, [])

        current_rows_per_sec = to_float(row.get("rows_per_sec"))
        current_seconds_per_batch = to_float(row.get("seconds_per_batch"))
        current_cpu_percent = to_float(row.get("cpu_percent"))
        current_records_per_batch = to_float(row.get("records_per_batch"))
        current_ms_per_record = to_float(row.get("ms_per_record"))

        previous_rows = entry.get("rows_per_sec", [])
        previous_spb = entry.get("seconds_per_batch", [])
        previous_rpb = entry.get("records_per_batch", [])
        previous_mspr = entry.get("ms_per_record", [])

        if current_rows_per_sec is not None and previous_rows:
            baseline_rows = median(previous_rows)
            if baseline_rows > 0 and current_rows_per_sec < baseline_rows * 0.70:
                drop_pct = ((baseline_rows - current_rows_per_sec) / baseline_rows) * 100.0
                row["perf_warnings"].append(
                    f"Rows/Sec dropped {drop_pct:.1f}% vs median baseline ({baseline_rows:.2f})."
                )

        if (
            current_cpu_percent is not None
            and current_cpu_percent < 60.0
            and current_seconds_per_batch is not None
            and previous_spb
        ):
            baseline_spb = median(previous_spb)
            if baseline_spb > 0 and current_seconds_per_batch > baseline_spb * 1.30:
                row["perf_warnings"].append(
                    "Low CPU with higher Sec/Batch suggests wait/overhead bottleneck."
                )

        if current_records_per_batch is not None and previous_rpb:
            baseline_rpb = median(previous_rpb)
            if baseline_rpb > 0 and current_records_per_batch < baseline_rpb * 0.70:
                row["perf_warnings"].append(
                    f"Records/Batch reduced versus baseline median ({baseline_rpb:.2f}); batch granularity increased."
                )

        if current_ms_per_record is not None and previous_mspr:
            baseline_mspr = median(previous_mspr)
            if baseline_mspr > 0 and current_ms_per_record > baseline_mspr * 1.30:
                row["perf_warnings"].append(
                    f"ms/Record increased above baseline median ({baseline_mspr:.4f})."
                )

        updates = {
            "rows_per_sec": current_rows_per_sec,
            "seconds_per_batch": current_seconds_per_batch,
            "cpu_percent": current_cpu_percent,
            "records_per_batch": current_records_per_batch,
            "ms_per_record": current_ms_per_record,
        }
        for metric, value in updates.items():
            if value is None:
                continue
            history = entry.setdefault(metric, [])
            history.append(float(value))
            if len(history) > history_limit:
                del history[:-history_limit]

    save_baselines(baseline_path, baseline)
    return baseline


def render_shareable_dashboard(suite_name: str, generated_at: str, metrics_rows: List[Dict[str, object]], variation: Dict[str, Dict[str, object]]) -> str:
    passed = sum(1 for row in metrics_rows if row["status"] == "PASSED")
    failed = len(metrics_rows) - passed
    pass_rate = (passed / len(metrics_rows) * 100.0) if metrics_rows else 0.0

    cpu_bar_rows: List[str] = []
    temp_values = [to_float(row.get("temp_avg_c")) for row in metrics_rows]
    numeric_temps = [v for v in temp_values if v is not None]
    temp_max = max(numeric_temps) if numeric_temps else 0.0
    for row in metrics_rows:
        dataset = html.escape(str(row.get("dataset", "-")))
        cpu_value = to_float(row.get("cpu_percent"))
        cpu_pct = max(0.0, min(100.0, cpu_value if cpu_value is not None else 0.0))
        temp_value = to_float(row.get("temp_avg_c"))
        if temp_value is not None and temp_max > 0:
            temp_pct = max(0.0, min(100.0, (temp_value / temp_max) * 100.0))
            temp_label = f"{temp_value:.2f}"
        else:
            temp_pct = 0.0
            temp_label = "N/A"

        cpu_bar_rows.append(
            "<div class='bar-row'>"
            f"<div class='bar-label'>{dataset}<span>CPU {cpu_pct:.2f}% | Temp {temp_label}</span></div>"
            f"<div class='bar-track'><div class='bar-fill cpu' style='width:{cpu_pct:.2f}%'></div></div>"
            f"<div class='bar-track'><div class='bar-fill temp' style='width:{temp_pct:.2f}%'></div></div>"
            "</div>"
        )

    def _fmt_num(value: object, precision: int = 2) -> str:
        if isinstance(value, (int, float)):
            return f"{float(value):.{precision}f}"
        return "-"

    rows_html = []
    for idx, row in enumerate(metrics_rows, 1):
        status = str(row["status"])
        perf_warnings = row.get("perf_warnings", []) if isinstance(row.get("perf_warnings"), list) else []

        rows = to_float(row.get("rows_per_sec"))
        spb = to_float(row.get("seconds_per_batch"))
        rpb = to_float(row.get("records_per_batch"))
        mspr = to_float(row.get("ms_per_record"))
        cpu = to_float(row.get("cpu_percent"))
        elapsed = to_float(row.get("elapsed_sec"))
        temp = to_float(row.get("temp_avg_c"))

        speed_tone = "tone-neutral"
        if rows is not None:
            if rows >= 30000:
                speed_tone = "tone-good"
            elif rows >= 10000:
                speed_tone = "tone-mid"
            else:
                speed_tone = "tone-bad"

        ms_tone = "tone-neutral"
        if mspr is not None:
            if mspr <= 0.05:
                ms_tone = "tone-good"
            elif mspr <= 0.12:
                ms_tone = "tone-mid"
            else:
                ms_tone = "tone-bad"

        warn_block = "<span class='ok-note'>No performance warning</span>"
        if perf_warnings:
            warn_items = "".join(f"<li>{html.escape(str(item))}</li>" for item in perf_warnings)
            warn_block = f"<ul class='warn-list'>{warn_items}</ul>"

        dataset_name = html.escape(str(row["dataset"]))
        rows_text = _fmt_num(rows)
        spb_text = _fmt_num(spb, 4)
        rpb_text = _fmt_num(rpb, 2)
        mspr_text = _fmt_num(mspr, 4)
        cpu_text = _fmt_num(cpu, 0)
        elapsed_text = _fmt_num(elapsed, 2)
        temp_text = _fmt_num(temp)

        rows_html.append(
            "<tr>"
            f"<td data-label='#'>{idx}</td>"
            f"<td data-label='Dataset' data-tip='Dataset||This is the input profile used for this run. Example: {dataset_name}.'><span class='dataset-cell'>{dataset_name}</span></td>"
            f"<td data-label='Status'><span class='badge {status_class(status)}'>{html.escape(status)}</span></td>"
            f"<td data-label='Throughput' class='{speed_tone}' data-tip='Throughput||Rows/Sec tells you speed. Higher is faster. Here: {rows_text} rows/sec over {elapsed_text} sec.'><div class='metric-line'><span class='metric-main'>{rows_text} rows/s</span><span class='metric-sub'>Elapsed {elapsed_text}s</span></div></td>"
            f"<td data-label='Batching' data-tip='Batching||Rec/Batch shows batch size. Bigger batches usually reduce overhead. Here: {rpb_text} rec/batch and {spb_text} sec/batch.'><div class='metric-line'><span class='metric-main'>{rpb_text} rec/batch</span><span class='metric-sub'>{spb_text} sec/batch</span></div></td>"
            f"<td data-label='Efficiency' class='{ms_tone}' data-tip='Efficiency||ms/Rec is time per record. Lower is better. Here: {mspr_text} ms/record with CPU at {cpu_text}% and Avg Temp {temp_text}C.'><div class='metric-line'><span class='metric-main'>{mspr_text} ms/rec</span><span class='metric-sub'>CPU {cpu_text}% | Temp {temp_text}C</span></div></td>"
            f"<td data-label='Health' data-tip='Health||Warnings indicate unusual slowdown versus your historical baseline. No warning means behavior is within expected range.'>{warn_block}</td>"
            f"<td data-label='Run'><a class='details-link' href='runs/{row['run_name']}/validation_report.html'>open run</a></td>"
            "</tr>"
        )

    matrix_rows = "\n".join(rows_html) if rows_html else "<tr><td colspan='8'>No runs.</td></tr>"

    template = """<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>BatchBuilder Suite Shareable Report</title>
    <style>
        :root { --bg:#fafaf5; --fg:#0f172a; --muted:#334155; --card:#ffffff; --line:#d4d4d8; --ok:#0f766e; --bad:#b91c1c; --brand:#1d4ed8; --cpu:#2563eb; --temp:#ea580c; }
        body { margin:0; background:radial-gradient(circle at 100% 0,#fef3c7 0,#fffbeb 25%,#f8fafc 60%,#ecfeff 100%); color:var(--fg); font:14px/1.45 Segoe UI, Arial, sans-serif; }
        .shell { max-width:1600px; margin:20px auto; padding:0 16px 60px; display:grid; grid-template-columns:250px 1fr; gap:16px; }
        .sidebar { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; position:sticky; top:12px; height:max-content; }
        .sidebar h3 { margin:0 0 10px; font-size:13px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }
        .snav { display:grid; gap:8px; }
        .snav a { text-decoration:none; color:var(--fg); border:1px solid var(--line); border-radius:8px; padding:8px 10px; background:#fff; font-weight:600; }
        .snav a:hover { background:#fef9c3; }
        .legend { margin-top:12px; border-top:1px dashed var(--line); padding-top:10px; }
        .legend ul { margin:6px 0 0; padding-left:18px; }
        .wrap { max-width:1380px; }
        h1 { margin:0; font-size:28px; }
        h2 { margin:18px 0 10px; font-size:20px; }
        .sub { color:var(--muted); margin-top:8px; }
        .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin:16px 0; }
        .kpi { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; box-shadow:0 8px 18px rgba(15,23,42,.04); }
        .kpi .k { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.03em; }
        .kpi .v { font-size:24px; font-weight:700; margin-top:4px; }
        .badge { display:inline-block; padding:4px 10px; border-radius:999px; font-weight:600; color:#fff; }
        .badge.ok { background:var(--ok); }
        .badge.bad { background:var(--bad); }
        .table-shell { border:1px solid var(--line); border-radius:12px; background:var(--card); overflow:hidden; box-shadow:0 8px 18px rgba(15,23,42,.04); }
        table { width:100%; border-collapse:collapse; background:var(--card); table-layout:fixed; }
        th, td { border-bottom:1px solid var(--line); padding:10px; text-align:left; vertical-align:top; }
        th { background:#e2e8f0; position:sticky; top:0; z-index:2; }
        th .hint-label { border-bottom:1px dashed #475569; cursor:help; }
        td { color:#0f172a; }
        td[data-tip], th[data-help] { cursor:help; }
        .tooltip-card { position:fixed; max-width:420px; background:#0f172a; color:#e2e8f0; border:1px solid #334155; border-radius:10px; padding:10px 12px; font-size:12px; line-height:1.45; box-shadow:0 12px 28px rgba(2,6,23,.45); z-index:9999; pointer-events:none; opacity:0; transform:translateY(4px); transition:opacity .14s ease, transform .14s ease; }
        .tooltip-card.show { opacity:1; transform:translateY(0); }
        tr:last-child td { border-bottom:none; }
        .dataset-cell { font-weight:600; display:block; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
        .metric-line { display:grid; gap:2px; }
        .metric-main { font-weight:700; white-space:nowrap; }
        .metric-sub { font-size:12px; color:var(--muted); white-space:nowrap; }
        .tone-good { background:#ecfdf5; }
        .tone-mid { background:#fffbeb; }
        .tone-bad { background:#fef2f2; }
        .tone-neutral { background:#f8fafc; }
        .ok-note { color:#0f766e; font-weight:600; }
        .warn-list { margin:0; padding-left:18px; color:#991b1b; }
        .details-link { font-weight:700; }
        .info { margin-top:14px; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }
        .links { display:flex; gap:12px; flex-wrap:wrap; margin-top:8px; }
        .pill { display:inline-block; padding:6px 12px; border-radius:999px; border:1px solid var(--line); background:#fff; }
        .viz { margin-top:14px; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }
        .viz-grid { display:grid; grid-template-columns:280px 1fr; gap:16px; align-items:start; }
        .ring-wrap { display:flex; align-items:center; justify-content:center; }
        .ring {
            --p: 0;
            width:180px;
            aspect-ratio:1;
            border-radius:50%;
            background:conic-gradient(var(--ok) calc(var(--p) * 1%), var(--bad) 0);
            display:grid;
            place-items:center;
            position:relative;
        }
        .ring::before { content:""; width:120px; aspect-ratio:1; border-radius:50%; background:var(--card); }
        .ring-label { position:absolute; text-align:center; font-weight:700; }
        .bar-chart { display:grid; gap:10px; }
        .bar-row { display:grid; gap:6px; }
        .bar-label { display:flex; justify-content:space-between; font-size:12px; color:var(--fg); }
        .bar-label.muted { color:var(--muted); }
        .bar-track { height:9px; background:#e5e7eb; border-radius:999px; overflow:hidden; }
        .bar-fill { height:100%; }
        .bar-fill.cpu { background:linear-gradient(90deg,#60a5fa,var(--cpu)); }
        .bar-fill.temp { background:linear-gradient(90deg,#fdba74,var(--temp)); }
        @media (max-width: 1180px) { .shell { grid-template-columns:1fr; } .sidebar { position:static; } }
        @media (max-width: 980px) {
            .viz-grid { grid-template-columns:1fr; }
            .shell { padding:0 10px 60px; }
            .kpi .v { font-size:21px; }
            table, thead, tbody, th, td, tr { display:block; }
            thead { display:none; }
            .table-shell { border:none; box-shadow:none; background:transparent; }
            tbody { display:grid; gap:10px; }
            tr { background:var(--card); border:1px solid var(--line); border-radius:12px; overflow:hidden; }
            td { border-bottom:1px solid var(--line); padding:9px 10px; }
            td:last-child { border-bottom:none; }
            td::before { content:attr(data-label); display:block; font-size:11px; letter-spacing:.03em; text-transform:uppercase; color:var(--muted); margin-bottom:3px; }
            .dataset-cell, .metric-main, .metric-sub { white-space:normal; overflow:visible; text-overflow:clip; }
        }
        a { color:var(--brand); text-decoration:none; }
        a:hover { text-decoration:underline; }
    </style>
</head>
<body>
    <div class=\"shell\">
        <aside class=\"sidebar\">
            <h3>Suite Navigation</h3>
            <div class=\"snav\">
                <a href=\"#overview\">Overview</a>
                <a href=\"#visuals\">Quick Visuals</a>
                <a href=\"#matrix\">Suite Matrix</a>
                <a href=\"#variation\">Variation Insights</a>
                <a href=\"structured_reports/index.html\">Structured Reports</a>
            </div>
            <div class=\"legend\">
                <h3>Color Guide</h3>
                <ul>
                    <li>Green badge/ring: passed datasets</li>
                    <li>Red badge/ring: failed datasets</li>
                    <li>Blue bars: CPU utilization</li>
                    <li>Orange bars: temperature level</li>
                </ul>
            </div>
        </aside>
        <main class=\"wrap\">
        <h1 id=\"overview\">__SUITE_TITLE__</h1>
        <div class=\"sub\">Generated: __GENERATED_AT__</div>
        <div class=\"sub\">Compact dashboard here. Deep-dive reports are separated under structured_reports and per-run detail pages.</div>

        <div class=\"kpis\">
            <div class=\"kpi\"><div class=\"k\">Datasets</div><div class=\"v\">__TOTAL__</div></div>
            <div class=\"kpi\"><div class=\"k\">Passed</div><div class=\"v\">__PASSED__</div></div>
            <div class=\"kpi\"><div class=\"k\">Failed</div><div class=\"v\">__FAILED__</div></div>
            <div class="kpi"><div class="k">Pass Rate</div><div class="v">__PASS_RATE__%</div></div>
            <div class=\"kpi\"><div class=\"k\">Avg Rows/Sec</div><div class=\"v\">__AVG_SPEED__</div></div>
            <div class=\"kpi\"><div class=\"k\">Avg CPU %</div><div class=\"v\">__AVG_CPU__</div></div>
            <div class=\"kpi\"><div class=\"k\">Avg Temp C</div><div class=\"v\">__AVG_TEMP__</div></div>
        </div>

        <div class="viz" id="visuals">
            <h2>Quick Visuals</h2>
            <div class="viz-grid">
                <div class="ring-wrap">
                    <div class="ring" style="--p:__PASS_RATE__">
                        <div class="ring-label">__PASSED__/__TOTAL__<br/>Passed</div>
                    </div>
                </div>
                <div class="bar-chart">__CPU_TEMP_BARS__</div>
            </div>
        </div>

        <div class=\"info\">
            <h2>Structured Reports</h2>
            <div class=\"links\">
                <a class=\"pill\" href=\"structured_reports/index.html\">overview</a>
                <a class=\"pill\" href=\"structured_reports/dataset_matrix.html\">dataset matrix</a>
                <a class=\"pill\" href=\"structured_reports/performance.html\">performance analysis</a>
                <a class=\"pill\" href=\"structured_reports/temperature.html\">temperature analysis</a>
                <a class=\"pill\" href=\"structured_reports/issues.html\">issues</a>
            </div>
        </div>

        <h2 id="matrix">Suite Matrix</h2>
        <div class="table-shell">
        <table>
            <thead>
                <tr>
                    <th data-help="Row Number||Simple index for this suite row. Example: row 4 means the fourth run in this report."><span class="hint-label">#</span></th>
                    <th data-help="Dataset||Input profile used in this run. Think of this as the recipe size and shape."><span class="hint-label">Dataset</span></th>
                    <th data-help="Status||PASSED means logic is correct. FAILED means validation or run issue."><span class="hint-label">Status</span></th>
                    <th data-help="Throughput||How fast the run completed overall. Example: 30000 rows/s is much faster than 4000 rows/s."><span class="hint-label">Throughput</span></th>
                    <th data-help="Batching||How work is split into batches. Example: 30 rec/batch is usually more efficient than 5 rec/batch."><span class="hint-label">Batching</span></th>
                    <th data-help="Efficiency||Time per record and CPU usage. Lower ms/rec is better."><span class="hint-label">Efficiency</span></th>
                    <th data-help="Health||Warnings compared with past baseline. No warning usually means stable behavior."><span class="hint-label">Health</span></th>
                    <th data-help="Run Details||Open full logs and deep metrics for this row."><span class="hint-label">Run</span></th>
                </tr>
            </thead>
            <tbody>
                __ROWS__
            </tbody>
        </table>
        </div>

        <div class=\"info\" id=\"variation\">
            <h2>Variation Insights</h2>
            <ul>
                <li>Rows/Sec: min __SPEED_MIN__, avg __SPEED_AVG__, max __SPEED_MAX__</li>
                <li>CPU %: min __CPU_MIN__, avg __CPU_AVG__, max __CPU_MAX__</li>
                <li>Peak Mem MB: min __MEM_MIN__, avg __MEM_AVG__, max __MEM_MAX__</li>
                <li>Sec/Batch: min __SPB_MIN__, avg __SPB_AVG__, max __SPB_MAX__</li>
                <li>Rec/Batch: min __RPB_MIN__, avg __RPB_AVG__, max __RPB_MAX__</li>
                <li>ms/Rec: min __MSPR_MIN__, avg __MSPR_AVG__, max __MSPR_MAX__</li>
                <li>Avg Temp C: min __TEMP_AVG_MIN__, avg __TEMP_AVG_AVG__, max __TEMP_AVG_MAX__</li>
            </ul>
        </div>
        </main>
    </div>
    <div id="metric-tooltip" class="tooltip-card" role="tooltip" aria-hidden="true"></div>
    <script>
        (() => {
            const tooltip = document.getElementById('metric-tooltip');
            const triggers = Array.from(document.querySelectorAll('[data-help], [data-tip]'));
            if (!tooltip || !triggers.length) return;

            const clamp = (v, min, max) => Math.max(min, Math.min(max, v));

            const place = (el) => {
                const rect = el.getBoundingClientRect();
                const pad = 12;
                const top = rect.bottom + 10;
                const maxLeft = window.innerWidth - tooltip.offsetWidth - pad;
                const centered = rect.left + (rect.width / 2) - (tooltip.offsetWidth / 2);
                const left = clamp(centered, pad, Math.max(pad, maxLeft));
                tooltip.style.left = `${left}px`;
                tooltip.style.top = `${top}px`;
            };

            const show = (el) => {
                const text = el.getAttribute('data-help') || el.getAttribute('data-tip') || '';
                const parts = text.split('||');
                const title = (parts[0] || '').trim();
                const body = (parts.slice(1).join('||') || '').trim();
                tooltip.innerHTML = body
                    ? `<strong style="display:block;margin-bottom:4px;color:#f8fafc;">${title}</strong><span>${body}</span>`
                    : `<span>${title}</span>`;
                tooltip.classList.add('show');
                tooltip.setAttribute('aria-hidden', 'false');
                place(el);
            };

            const hide = () => {
                tooltip.classList.remove('show');
                tooltip.setAttribute('aria-hidden', 'true');
            };

            triggers.forEach((el) => {
                el.addEventListener('mouseenter', () => show(el));
                el.addEventListener('focus', () => show(el));
                el.addEventListener('mouseleave', hide);
                el.addEventListener('blur', hide);
            });

            window.addEventListener('scroll', hide, { passive: true });
            window.addEventListener('resize', hide);
        })();
    </script>
</body>
</html>
"""

    replacements = {
        "__SUITE_TITLE__": html.escape(suite_name),
        "__GENERATED_AT__": html.escape(generated_at),
        "__TOTAL__": str(len(metrics_rows)),
        "__PASSED__": str(passed),
        "__FAILED__": str(failed),
        "__PASS_RATE__": f"{pass_rate:.2f}",
        "__AVG_SPEED__": fmt(variation["rows_per_sec"]["avg"]),
        "__AVG_CPU__": fmt(variation["cpu_percent"]["avg"]),
        "__AVG_TEMP__": fmt(variation["temp_avg_c"]["avg"]),
        "__CPU_TEMP_BARS__": "".join(cpu_bar_rows) if cpu_bar_rows else "<div class='sub'>No CPU or temperature data available.</div>",
        "__ROWS__": matrix_rows,
        "__SPEED_MIN__": fmt(variation["rows_per_sec"]["min"]),
        "__SPEED_AVG__": fmt(variation["rows_per_sec"]["avg"]),
        "__SPEED_MAX__": fmt(variation["rows_per_sec"]["max"]),
        "__CPU_MIN__": fmt(variation["cpu_percent"]["min"]),
        "__CPU_AVG__": fmt(variation["cpu_percent"]["avg"]),
        "__CPU_MAX__": fmt(variation["cpu_percent"]["max"]),
        "__MEM_MIN__": fmt(variation["max_rss_mb"]["min"]),
        "__MEM_AVG__": fmt(variation["max_rss_mb"]["avg"]),
        "__MEM_MAX__": fmt(variation["max_rss_mb"]["max"]),
        "__SPB_MIN__": fmt(variation["seconds_per_batch"]["min"], 4),
        "__SPB_AVG__": fmt(variation["seconds_per_batch"]["avg"], 4),
        "__SPB_MAX__": fmt(variation["seconds_per_batch"]["max"], 4),
        "__RPB_MIN__": fmt(variation["records_per_batch"]["min"], 2),
        "__RPB_AVG__": fmt(variation["records_per_batch"]["avg"], 2),
        "__RPB_MAX__": fmt(variation["records_per_batch"]["max"], 2),
        "__MSPR_MIN__": fmt(variation["ms_per_record"]["min"], 4),
        "__MSPR_AVG__": fmt(variation["ms_per_record"]["avg"], 4),
        "__MSPR_MAX__": fmt(variation["ms_per_record"]["max"], 4),
        "__TEMP_AVG_MIN__": fmt(variation["temp_avg_c"]["min"]),
        "__TEMP_AVG_AVG__": fmt(variation["temp_avg_c"]["avg"]),
        "__TEMP_AVG_MAX__": fmt(variation["temp_avg_c"]["max"]),
    }

    for key, value in replacements.items():
        template = template.replace(key, value)

    return template


def render_structured_table_page(
    title: str,
    subtitle: str,
    headers: List[str],
    rows: List[List[str]],
    header_tooltips: Optional[Dict[str, str]] = None,
) -> str:
    header_tooltips = header_tooltips or {}
    thead_cells = []
    for h in headers:
        tip = header_tooltips.get(h)
        if tip:
            thead_cells.append(f"<th title='{html.escape(tip)}'>{html.escape(h)}</th>")
        else:
            thead_cells.append(f"<th>{html.escape(h)}</th>")
    thead = "".join(thead_cells)

    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    tbody = "\n".join(body_rows) if body_rows else f"<tr><td colspan='{len(headers)}'>No rows</td></tr>"

    return f"""<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>{html.escape(title)}</title>
    <style>
        :root {{ --bg:#f8fafc; --fg:#0f172a; --muted:#475569; --card:#ffffff; --line:#cbd5e1; --brand:#0b5ed7; }}
        body {{ margin:0; background:var(--bg); color:var(--fg); font:14px/1.45 Segoe UI, Arial, sans-serif; }}
        .wrap {{ max-width:1320px; margin:24px auto; padding:0 16px 60px; }}
        h1 {{ margin:0; font-size:26px; }}
        .sub {{ color:var(--muted); margin-top:8px; }}
        .back {{ margin-top:8px; display:inline-block; }}
        table {{ width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; margin-top:12px; }}
        th, td {{ border-bottom:1px solid var(--line); padding:10px; text-align:left; vertical-align:top; }}
        th {{ background:#e2e8f0; }}
        th[title] {{ cursor:help; }}
        tr:last-child td {{ border-bottom:none; }}
        a {{ color:var(--brand); }}
    </style>
</head>
<body>
    <div class=\"wrap\">
        <h1>{html.escape(title)}</h1>
        <div class=\"sub\">{html.escape(subtitle)}</div>
        <a class=\"back\" href=\"../shareable_report.html\">back to suite report</a>
        <table>
            <thead><tr>{thead}</tr></thead>
            <tbody>{tbody}</tbody>
        </table>
    </div>
</body>
</html>
"""


def write_structured_reports(
        suite_dir: Path,
        metrics_rows: List[Dict[str, object]],
        variation: Dict[str, Dict[str, object]],
        suite_summary: Dict[str, object],
) -> None:
    structured_dir = suite_dir / "structured_reports"
    structured_dir.mkdir(parents=True, exist_ok=True)

    (structured_dir / "overview.json").write_text(json.dumps(suite_summary, indent=2), encoding="utf-8")
    (structured_dir / "dataset_metrics.json").write_text(json.dumps(metrics_rows, indent=2), encoding="utf-8")
    (structured_dir / "variation_analysis.json").write_text(json.dumps(variation, indent=2), encoding="utf-8")

    issues_payload: List[Dict[str, object]] = []
    perf_flags_payload: List[Dict[str, object]] = []
    for row in metrics_rows:
        issues_payload.append(
            {
                "dataset": row["dataset"],
                "run_name": row["run_name"],
                "status": row["status"],
                "issues": row["issues"],
            }
        )
        perf_flags_payload.append(
            {
                "dataset": row["dataset"],
                "run_name": row["run_name"],
                "status": row["status"],
                "perf_warnings": row.get("perf_warnings", []),
            }
        )
    (structured_dir / "issues.json").write_text(json.dumps(issues_payload, indent=2), encoding="utf-8")
    (structured_dir / "performance_flags.json").write_text(
        json.dumps(perf_flags_payload, indent=2),
        encoding="utf-8",
    )

    matrix_rows: List[List[str]] = []
    perf_rows: List[List[str]] = []
    temp_rows: List[List[str]] = []
    issue_rows: List[List[str]] = []

    for row in metrics_rows:
        details_link = f"<a href='../runs/{row['run_name']}/validation_report.html'>details</a>"
        matrix_rows.append(
            [
                html.escape(str(row["dataset"])),
                html.escape(str(row["status"])),
                html.escape(str(row["records"])),
                html.escape(str(row["batch_count"])),
                html.escape(str(row["file_count"])),
                html.escape(str(row["total_bytes"])),
                details_link,
            ]
        )

        perf_warnings = row.get("perf_warnings", [])
        if isinstance(perf_warnings, list) and perf_warnings:
            perf_flags_text = html.escape(" | ".join(str(item) for item in perf_warnings))
        else:
            perf_flags_text = "-"

        perf_rows.append(
            [
                html.escape(str(row["dataset"])),
                html.escape(str(row["elapsed_sec"])),
                html.escape(str(row["rows_per_sec"])),
                html.escape(str(row["seconds_per_batch"])),
                html.escape(str(row.get("records_per_batch", "-"))),
                html.escape(str(row.get("bytes_per_batch", "-"))),
                html.escape(str(row.get("ms_per_record", "-"))),
                html.escape(str(row["cpu_percent"])),
                html.escape(str(row["max_rss_mb"])),
                html.escape(str(row["bytes_per_record"])),
                perf_flags_text,
                details_link,
            ]
        )

        temp_rows.append(
            [
                html.escape(str(row["dataset"])),
                html.escape(str(row["temp_supported"])),
                html.escape(str(row["temp_samples"])),
                html.escape(str(row["temp_min_c"])),
                html.escape(str(row["temp_avg_c"])),
                html.escape(str(row["temp_max_c"])),
                details_link,
            ]
        )

        issues = row.get("issues", [])
        if isinstance(issues, list) and issues:
            for issue in issues:
                issue_rows.append(
                    [
                        html.escape(str(row["dataset"])),
                        html.escape(str(row["status"])),
                        html.escape(str(issue)),
                        details_link,
                    ]
                )
        else:
            issue_rows.append(
                [
                    html.escape(str(row["dataset"])),
                    html.escape(str(row["status"])),
                    "None",
                    details_link,
                ]
            )

    (structured_dir / "dataset_matrix.html").write_text(
        render_structured_table_page(
            "Dataset Matrix",
            "Per-dataset high-level status and volume summary.",
            ["Dataset", "Status", "Records", "Batch Count", "File Count", "Total Bytes", "Details"],
            matrix_rows,
        ),
        encoding="utf-8",
    )
    (structured_dir / "performance.html").write_text(
        render_structured_table_page(
            "Performance Analysis",
            "Execution speed and compute resource metrics.",
            [
                "Dataset",
                "Elapsed Sec",
                "Rows/Sec",
                "Sec/Batch",
                "Rec/Batch",
                "Bytes/Batch",
                "ms/Rec",
                "CPU %",
                "Peak Mem MB",
                "Bytes/Record",
                "Perf Flags",
                "Details",
            ],
            perf_rows,
            {
                "Elapsed Sec": "Total time in seconds for one full dataset run.",
                "Rows/Sec": "Records processed per second for the full run.",
                "Sec/Batch": "Average seconds per batch.",
                "Rec/Batch": "Average records per batch.",
                "Bytes/Batch": "Average bytes in each batch.",
                "ms/Rec": "Average milliseconds spent per record.",
                "CPU %": "CPU utilization percentage.",
                "Peak Mem MB": "Peak resident memory in MB.",
                "Bytes/Record": "Average bytes represented by each record.",
                "Perf Flags": "Automated warning text based on baseline comparisons.",
            },
        ),
        encoding="utf-8",
    )
    (structured_dir / "temperature.html").write_text(
        render_structured_table_page(
            "Temperature Analysis",
            "Temperature telemetry collected during BatchBuilder execution.",
            ["Dataset", "Supported", "Samples", "Min C", "Avg C", "Max C", "Details"],
            temp_rows,
        ),
        encoding="utf-8",
    )
    (structured_dir / "issues.html").write_text(
        render_structured_table_page(
            "Issues",
            "Validation and execution issues across all datasets.",
            ["Dataset", "Status", "Issue", "Details"],
            issue_rows,
        ),
        encoding="utf-8",
    )

    index_html = """<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Structured Reports Index</title>
    <style>
        :root { --bg:#f8fafc; --fg:#0f172a; --card:#ffffff; --line:#cbd5e1; --brand:#0b5ed7; }
        body { margin:0; background:var(--bg); color:var(--fg); font:14px/1.45 Segoe UI, Arial, sans-serif; }
        .wrap { max-width:920px; margin:28px auto; padding:0 16px; }
        h1 { margin:0; font-size:26px; }
        .card { margin-top:14px; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }
        a { color:var(--brand); text-decoration:none; }
        a:hover { text-decoration:underline; }
        ul { margin:0; padding-left:18px; }
    </style>
</head>
<body>
    <div class=\"wrap\">
        <h1>Structured Reports</h1>
        <div class=\"card\">
            <ul>
                <li><a href=\"dataset_matrix.html\">dataset_matrix.html</a></li>
                <li><a href=\"performance.html\">performance.html</a></li>
                <li><a href=\"temperature.html\">temperature.html</a></li>
                <li><a href=\"issues.html\">issues.html</a></li>
                <li><a href=\"overview.json\">overview.json</a></li>
                <li><a href=\"dataset_metrics.json\">dataset_metrics.json</a></li>
                <li><a href=\"variation_analysis.json\">variation_analysis.json</a></li>
                <li><a href=\"issues.json\">issues.json</a></li>
                <li><a href=\"performance_flags.json\">performance_flags.json</a></li>
            </ul>
        </div>
        <div class=\"card\"><a href=\"../shareable_report.html\">back to suite dashboard</a></div>
    </div>
</body>
</html>
"""
    (structured_dir / "index.html").write_text(index_html, encoding="utf-8")
