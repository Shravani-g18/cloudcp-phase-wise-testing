from __future__ import annotations

import re
import shlex
from typing import Dict


def parse_elapsed_seconds(elapsed: str) -> float:
    parts = elapsed.strip().split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    if len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    return float(parts[0])


def parse_gnu_time_metrics(stderr_text: str) -> Dict[str, object]:
    metrics: Dict[str, object] = {}
    patterns = {
        "user_time_sec": r"User time \(seconds\):\s*([0-9.]+)",
        "system_time_sec": r"System time \(seconds\):\s*([0-9.]+)",
        "cpu_percent": r"Percent of CPU this job got:\s*([0-9]+)%",
        "elapsed_raw": r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([^\n\r]+)",
        "max_rss_kb": r"Maximum resident set size \(kbytes\):\s*([0-9]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, stderr_text)
        if not match:
            continue
        value = match.group(1).strip()
        if key in {"user_time_sec", "system_time_sec"}:
            metrics[key] = float(value)
        elif key in {"cpu_percent", "max_rss_kb"}:
            metrics[key] = int(value)
        else:
            metrics[key] = value

    elapsed_raw = metrics.get("elapsed_raw")
    if isinstance(elapsed_raw, str):
        try:
            metrics["elapsed_sec"] = parse_elapsed_seconds(elapsed_raw)
        except Exception:
            pass

    if "max_rss_kb" in metrics:
        metrics["max_rss_mb"] = round(float(metrics["max_rss_kb"]) / 1024.0, 2)

    return metrics


def parse_temperature_metrics(text: str) -> Dict[str, object]:
    metrics: Dict[str, object] = {}

    match = re.search(
        r"TEMP_METRICS\s+supported=(\d+)\s+samples=(\d+)\s+min_c=([0-9.]+)\s+avg_c=([0-9.]+)\s+max_c=([0-9.]+)",
        text,
    )
    if match:
        metrics.update(
            {
                "temperature_supported": bool(int(match.group(1))),
                "temp_samples": int(match.group(2)),
                "temp_min_c": float(match.group(3)),
                "temp_avg_c": float(match.group(4)),
                "temp_max_c": float(match.group(5)),
            }
        )
    elif re.search(r"TEMP_METRICS\s+supported=0", text):
        metrics["temperature_supported"] = False

    drive_rows = []
    for drive_match in re.finditer(
        r"DRIVE_TEMP\s+dev=(\S+)\s+sn=(\S+)\s+samples=(\d+)\s+min_c=([0-9.]+)\s+avg_c=([0-9.]+)\s+max_c=([0-9.]+)",
        text,
    ):
        drive_rows.append(
            {
                "dev": drive_match.group(1),
                "sn": drive_match.group(2),
                "samples": int(drive_match.group(3)),
                "min_c": float(drive_match.group(4)),
                "avg_c": float(drive_match.group(5)),
                "max_c": float(drive_match.group(6)),
            }
        )
    if drive_rows:
        metrics["drive_temperatures"] = sorted(drive_rows, key=lambda x: (x["dev"], x["sn"]))

    disk_match = re.search(
        r"DISK_USAGE\s+fs=(\S+)\s+size_kb=(\d+)\s+used_kb=(\d+)\s+avail_kb=(\d+)\s+used_pct=(\S+)\s+mount=(\S+)",
        text,
    )
    if disk_match:
        metrics["disk_usage"] = {
            "filesystem": disk_match.group(1),
            "size_kb": int(disk_match.group(2)),
            "used_kb": int(disk_match.group(3)),
            "avail_kb": int(disk_match.group(4)),
            "used_pct": disk_match.group(5),
            "mount": disk_match.group(6),
        }

    return metrics


def build_monitored_batchbuilder_command(bb_cmd: str, temp_sample_interval: float) -> str:
    safe_bb_cmd = shlex.quote(bb_cmd)
    safe_interval = max(0.2, temp_sample_interval)
    return f"""
TMP_OUT="$(mktemp)"
TMP_ERR="$(mktemp)"
TMP_TEMP="$(mktemp)"
TMP_DRIVE="$(mktemp)"

read_temp_c() {{
  for f in /sys/class/thermal/thermal_zone*/temp; do
    if [ -r "$f" ]; then
      raw="$(cat "$f" 2>/dev/null | tr -d '\\r\\n')"
      if [ -n "$raw" ]; then
        if [ "$raw" -ge 1000 ] 2>/dev/null; then
          awk -v raw="$raw" 'BEGIN {{printf "%.2f\\n", raw/1000}}'
          return 0
        fi
        echo "$raw"
        return 0
      fi
    fi
  done

  if command -v sensors >/dev/null 2>&1; then
    sensors 2>/dev/null | awk '{{
      for (i = 1; i <= NF; i++) {{
        if ($i ~ /C$/) {{
          val = $i
          gsub(/[^0-9.\\-]/, "", val)
          if (val != "") {{
            print val
            exit
          }}
        }}
      }}
    }}'
    return $?
  fi

  return 1
}}

capture_temp() {{
  t="$(read_temp_c)"
  if [ -n "$t" ]; then
    echo "$t" >> "$TMP_TEMP"
  fi
}}

capture_drive_temps() {{
  if ! command -v nvme >/dev/null 2>&1; then
    return 0
  fi

    nvme list 2>/dev/null | awk '/^\\/dev/ {{print $1}}' | while read -r dev; do
    if [ -z "$dev" ]; then
      continue
    fi
    sn="$(nvme id-ctrl "$dev" 2>/dev/null | awk '/^sn / {{print $3; exit}}')"
    temp="$(nvme smart-log "$dev" 2>/dev/null | awk -F':' '/^temperature/ {{gsub(/^[ \t]+/, "", $2); gsub(/[ \t]*C.*/, "", $2); print $2; exit}}')"
    if [ -n "$temp" ]; then
      echo "$dev|${{sn:-unknown}}|$temp" >> "$TMP_DRIVE"
    fi
  done
}}

BB_CMD={safe_bb_cmd}

capture_temp
capture_drive_temps
{{ /usr/bin/time -v sh -c "$BB_CMD"; }} >"$TMP_OUT" 2>"$TMP_ERR" &
BB_PID=$!

while kill -0 "$BB_PID" 2>/dev/null; do
  capture_temp
  capture_drive_temps
  sleep {safe_interval}
done

wait "$BB_PID"
BB_EXIT=$?
capture_temp
capture_drive_temps

if [ -s "$TMP_TEMP" ]; then
  awk 'NR==1 {{min=$1; max=$1; sum=$1; c=1; next}} {{
    if ($1 < min) min=$1;
    if ($1 > max) max=$1;
    sum += $1;
    c += 1;
  }} END {{
    avg = sum / c;
    printf "TEMP_METRICS supported=1 samples=%d min_c=%.2f avg_c=%.2f max_c=%.2f\\n", c, min, avg, max;
  }}' "$TMP_TEMP"
else
  echo "TEMP_METRICS supported=0"
fi

if [ -s "$TMP_DRIVE" ]; then
  awk -F'|' '{{
    key=$1 FS $2;
    t=$3 + 0;
    if (!(key in c)) {{
      c[key]=0;
      s[key]=0;
      mn[key]=t;
      mx[key]=t;
    }}
    c[key] += 1;
    s[key] += t;
    if (t < mn[key]) mn[key] = t;
    if (t > mx[key]) mx[key] = t;
  }} END {{
    for (k in c) {{
      split(k, p, FS);
      avg = s[k] / c[k];
      printf "DRIVE_TEMP dev=%s sn=%s samples=%d min_c=%.2f avg_c=%.2f max_c=%.2f\\n", p[1], p[2], c[k], mn[k], avg, mx[k];
    }}
  }}' "$TMP_DRIVE"
fi

if command -v df >/dev/null 2>&1; then
  df -Pk . | awk 'NR==2 {{printf "DISK_USAGE fs=%s size_kb=%s used_kb=%s avail_kb=%s used_pct=%s mount=%s\\n", $1, $2, $3, $4, $5, $6}}'
fi

cat "$TMP_OUT"
cat "$TMP_ERR" 1>&2

rm -f "$TMP_OUT" "$TMP_ERR" "$TMP_TEMP" "$TMP_DRIVE"
exit "$BB_EXIT"
""".strip()


def add_derived_metrics(perf: Dict[str, object], records: int, batches: int, total_bytes: int) -> Dict[str, object]:
    elapsed = perf.get("elapsed_sec")
    if isinstance(elapsed, (int, float)) and elapsed > 0:
        perf["rows_per_sec"] = round(records / elapsed, 2)
        perf["batches_per_sec"] = round(batches / elapsed, 4)
        perf["bytes_per_sec"] = round(total_bytes / elapsed, 2)
    if isinstance(elapsed, (int, float)) and batches > 0:
        perf["seconds_per_batch"] = round(elapsed / batches, 6)
    if records > 0:
        perf["bytes_per_record"] = round(total_bytes / records, 2)
    return perf
