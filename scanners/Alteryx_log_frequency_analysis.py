import os
import re
from collections import defaultdict
from datetime import datetime
import pandas as pd

print("=== Script started ===")

# ── CONFIG ────────────────────────────────────────────────────────────────────
ROOT_FOLDER = r"\\odie\sys1\DPA-Ops\Alteryx_Prod_Logs"
OUTPUT_FILE = r"C:\Users\npatil\Desktop\Alteryx_Log_Analysis.csv"
OUTPUT_FREQ = r"C:\Users\npatil\Desktop\Alteryx_Frequency_Summary.csv"
# ─────────────────────────────────────────────────────────────────────────────

START_PATTERN = re.compile(r"Started running .+ at (.+)$")
FNAME_PATTERN = re.compile(r"^(\d+)_(\d{8})_(\d{6})_(.+)\.log$", re.IGNORECASE)
DAY_NAMES     = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


def parse_log_timestamp(filepath):
    """Read only the first line — much faster than reading 2000 chars."""
    for enc in ("utf-16", "utf-8", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc, errors="ignore") as f:
                first_line = f.readline()
            m = START_PATTERN.search(first_line)
            if m:
                raw = re.sub(r"\s+", " ", m.group(1).strip())
                try:
                    return datetime.strptime(raw, "%a %b %d %H:%M:%S %Y")
                except ValueError:
                    pass
            break  # first line read OK, no timestamp found
        except Exception:
            continue
    return None


def parse_fname_timestamp(filename):
    m = FNAME_PATTERN.match(filename)
    if m:
        try:
            dt = datetime.strptime(m.group(2) + m.group(3), "%Y%m%d%H%M%S")
            wf = m.group(4).replace("_", " ").replace(".", " ").strip()
            return dt, wf
        except ValueError:
            pass
    return None, ""


def workflow_name_from_path(filepath):
    """Use parent folder name as workflow name — fast, no file read needed."""
    return os.path.basename(os.path.dirname(filepath))


def infer_frequency(timestamps):
    if len(timestamps) < 2:
        return "Insufficient data (only {} log found)".format(len(timestamps))
    ts = sorted(timestamps)
    gaps = [(ts[i+1] - ts[i]).total_seconds() / 3600 for i in range(len(ts)-1)]
    avg_gap_h = sum(gaps) / len(gaps)
    dow_counts = defaultdict(int)
    for t in ts:
        dow_counts[t.weekday()] += 1
    distinct_days = sorted(dow_counts.keys())

    if avg_gap_h < 6:
        return "Multiple times per day (~{}x)".format(round(24 / max(avg_gap_h, 0.25)))
    if avg_gap_h < 20:
        return "Daily"
    if avg_gap_h < 36:
        if len(distinct_days) <= 5 and 5 not in distinct_days and 6 not in distinct_days:
            return "Weekdays (Mon-Fri)"
        return "Daily"
    if 30 <= avg_gap_h <= 100:
        if len(distinct_days) == 1:
            return "Weekly on {}".format(DAY_NAMES[distinct_days[0]])
        elif len(distinct_days) == 2:
            return "Twice a week ({} & {})".format(DAY_NAMES[distinct_days[0]], DAY_NAMES[distinct_days[1]])
        elif len(distinct_days) == 3:
            return "3x per week ({})".format(", ".join(DAY_NAMES[d] for d in distinct_days))
        return "Multiple days per week"
    if avg_gap_h <= 200:
        return "Bi-weekly"
    if avg_gap_h <= 800:
        return "Monthly"
    if avg_gap_h <= 2200:
        return "Quarterly"
    return "Yearly / Ad-hoc"


# ── CHECK FOLDER ──────────────────────────────────────────────────────────────
if not os.path.exists(ROOT_FOLDER):
    print("ERROR: Folder not found: {}".format(ROOT_FOLDER))
    raise SystemExit(1)
print("OK: Folder found.")

# ── SCAN ──────────────────────────────────────────────────────────────────────
workflow_runs  = defaultdict(list)
workflow_files = defaultdict(list)
processed = 0
skipped   = 0

print("Scanning... (progress shown every 5000 files)")

for dirpath, _dirs, files in os.walk(ROOT_FOLDER):
    for fname in files:
        if not fname.lower().endswith(".log"):
            continue

        full_path = os.path.join(dirpath, fname)
        wf = workflow_name_from_path(full_path)

        dt = parse_log_timestamp(full_path)
        if dt is None:
            dt, _ = parse_fname_timestamp(fname)

        if dt:
            workflow_runs[wf].append(dt)
            workflow_files[wf].append(full_path)
            processed += 1
        else:
            skipped += 1

        total = processed + skipped
        if total % 5000 == 0:
            print("  {} files scanned | {} parsed | {} skipped | {} workflows so far".format(
                total, processed, skipped, len(workflow_runs)))

print("Scan complete: {} parsed | {} skipped | {} unique workflows".format(
    processed, skipped, len(workflow_runs)))

if not workflow_runs:
    print("ERROR: No timestamps could be extracted from any log file.")
    raise SystemExit(1)

# ── BUILD DATAFRAME ───────────────────────────────────────────────────────────
print("Building summary...")
rows = []
for wf, times in workflow_runs.items():
    times_sorted = sorted(times)
    rows.append({
        "Workflow Name":      wf,
        "Last Refresh Time":  times_sorted[-1],
        "First Log Date":     times_sorted[0],
        "Total Log Count":    len(times),
        "Inferred Frequency": infer_frequency(times_sorted),
        "Log Folder":         os.path.dirname(workflow_files[wf][0]),
    })

df = pd.DataFrame(rows).sort_values("Workflow Name").reset_index(drop=True)
print(df.to_string(index=False))

# ── SAVE ──────────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

df.to_csv(OUTPUT_FILE, index=False)
print("Saved: {}".format(OUTPUT_FILE))

freq_summary = (
    df.groupby("Inferred Frequency")["Workflow Name"]
    .count().reset_index()
    .rename(columns={"Workflow Name": "Workflow Count"})
    .sort_values("Workflow Count", ascending=False)
)
freq_summary.to_csv(OUTPUT_FREQ, index=False)
print("Saved: {}".format(OUTPUT_FREQ))

print("=== Done ===")
