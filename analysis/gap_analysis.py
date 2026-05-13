#!/usr/bin/env python3
"""Analyze gaps in KNMI non-validated, VIIRS A1, and VIIRS A2 datasets."""
import os, re, sys
from datetime import datetime, timedelta

def analyze_knmi(directory, prefix, label):
    print("=" * 70)
    print(f"{label}")
    print("=" * 70)
    sys.stdout.flush()
    
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".nc")]
    
    datetimes = set()
    for f in files:
        m = re.match(re.escape(prefix) + r"(\d{8})-(\d{2})\.nc", f)
        if m:
            dt = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H")
            datetimes.add(dt)
    
    if not datetimes:
        print("ERROR: No files found")
        return
    
    min_dt = min(datetimes)
    max_dt = max(datetimes)
    print(f"Range: {min_dt} -> {max_dt}")
    print(f"Files: {len(datetimes)}")
    
    expected = set()
    cur = min_dt
    while cur <= max_dt:
        expected.add(cur)
        cur += timedelta(hours=1)
    
    missing = sorted(expected - datetimes)
    print(f"Expected hours: {len(expected)}")
    print(f"Missing hours:  {len(missing)}")
    
    if missing:
        gaps = []
        gap_start = missing[0]
        gap_end = missing[0]
        for dt in missing[1:]:
            if dt == gap_end + timedelta(hours=1):
                gap_end = dt
            else:
                gaps.append((gap_start, gap_end))
                gap_start = dt
                gap_end = dt
        gaps.append((gap_start, gap_end))
        
        print(f"Number of gap ranges: {len(gaps)}")
        for start, end in gaps[:50]:
            hours = int((end - start).total_seconds() / 3600) + 1
            if start == end:
                print(f"  {start.strftime('%Y-%m-%d %H:00')} (1 hour)")
            else:
                print(f"  {start.strftime('%Y-%m-%d %H:00')} -> {end.strftime('%Y-%m-%d %H:00')} ({hours} hours / {hours/24:.1f} days)")
        if len(gaps) > 50:
            print(f"  ... and {len(gaps) - 50} more gap ranges")
    else:
        print("No gaps found!")
    
    print()
    sys.stdout.flush()


def analyze_viirs(directory, prefix, label):
    print("=" * 70)
    print(f"{label}")
    print("=" * 70)
    sys.stdout.flush()
    
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".h5")]
    
    dates = set()
    for f in files:
        m = re.match(re.escape(prefix) + r"\.A(\d{4})(\d{3})\.", f)
        if m:
            year = int(m.group(1))
            doy = int(m.group(2))
            dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
            dates.add(dt.date())
    
    if not dates:
        print("ERROR: No files found")
        return dates
    
    min_d = min(dates)
    max_d = max(dates)
    print(f"Range: {min_d} -> {max_d}")
    print(f"Files (unique dates): {len(dates)}")
    
    expected_dates = set()
    cur = min_d
    while cur <= max_d:
        expected_dates.add(cur)
        cur += timedelta(days=1)
    
    missing_dates = sorted(expected_dates - dates)
    print(f"Expected days: {len(expected_dates)}")
    print(f"Missing days:  {len(missing_dates)}")
    
    if missing_dates:
        gaps = []
        gap_start = missing_dates[0]
        gap_end = missing_dates[0]
        for d in missing_dates[1:]:
            if d == gap_end + timedelta(days=1):
                gap_end = d
            else:
                gaps.append((gap_start, gap_end))
                gap_start = d
                gap_end = d
        gaps.append((gap_start, gap_end))
        
        print(f"Number of gap ranges: {len(gaps)}")
        for start, end in gaps[:50]:
            days = (end - start).days + 1
            if start == end:
                print(f"  {start} (1 day)")
            else:
                print(f"  {start} -> {end} ({days} days)")
        if len(gaps) > 50:
            print(f"  ... and {len(gaps) - 50} more gap ranges")
    else:
        print("No gaps found!")
    
    print()
    sys.stdout.flush()
    return dates


# Run analyses
analyze_knmi("/projects/prjs2061/data/knmi", "hourly-observations-", "KNMI NON-VALIDATED (hourly)")

a1_dates = analyze_viirs("/projects/prjs2061/data/viirs/A1", "VNP46A1", "VIIRS A1 (VNP46A1, daily)")
a2_dates = analyze_viirs("/projects/prjs2061/data/viirs/A2", "VNP46A2", "VIIRS A2 (VNP46A2, daily)")

# Cross-comparison
if a1_dates and a2_dates:
    print("=" * 70)
    print("VIIRS A1 vs A2 ALIGNMENT")
    print("=" * 70)
    in_a1_not_a2 = sorted(a1_dates - a2_dates)
    in_a2_not_a1 = sorted(a2_dates - a1_dates)
    print(f"Days in A1 but not A2: {len(in_a1_not_a2)}")
    if in_a1_not_a2:
        for d in in_a1_not_a2[:20]:
            print(f"  {d}")
        if len(in_a1_not_a2) > 20:
            print(f"  ... and {len(in_a1_not_a2) - 20} more")
    print(f"Days in A2 but not A1: {len(in_a2_not_a1)}")
    if in_a2_not_a1:
        for d in in_a2_not_a1[:20]:
            print(f"  {d}")
        if len(in_a2_not_a1) > 20:
            print(f"  ... and {len(in_a2_not_a1) - 20} more")
    sys.stdout.flush()

print("\nDone.")
