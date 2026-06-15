#!/usr/bin/env python3
"""Generate Dutch public holidays from the ``holidays`` Python library.

Uses the well-maintained ``holidays`` package (rule-based computation
from Dutch legislation) to produce a CSV of all official Netherlands
public holidays from January 2012 through December 2025.

The library correctly handles:
  - Fixed holidays (New Year, Christmas, Second Christmas Day)
  - Easter-dependent holidays (Good Friday, Easter, Ascension, Whitsun)
  - King's Day (27 April; shifted to 26 April when the 27th is a Sunday)
  - Liberation Day (5 May — observed as a national holiday every 5 years:
    2015, 2020, 2025)

Usage
-----
    python src/download/generate_nl_public_holidays.py

    # Custom date range:
    python src/download/generate_nl_public_holidays.py \
        --start-year 2010 --end-year 2026

    # Custom output path:
    python src/download/generate_nl_public_holidays.py \
        --dest /path/to/output.csv

Output columns
--------------
    date, name, day_of_week, year, month, day

Source: ``holidays`` library v0.83+ — https://pypi.org/project/holidays/
"""

import argparse
import csv
import logging
import os
from pathlib import Path

import holidays

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("NL_PublicHolidays")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
START_YEAR = 2012
END_YEAR = 2025

DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

CSV_COLUMNS = [
    "date",
    "name",
    "day_of_week",
    "year",
    "month",
    "day",
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Dutch public holidays from the holidays library.",
    )
    parser.add_argument(
        "--dest",
        type=str,
        default=None,
        help="Destination CSV file path "
             "(default: <project>/data/calendar/nl_public_holidays.csv)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=START_YEAR,
        help=f"First year to generate (default: {START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=END_YEAR,
        help=f"Last year to generate (default: {END_YEAR})",
    )
    args = parser.parse_args()

    # Resolve output path
    if args.dest:
        out_path = Path(args.dest)
    else:
        repo_root = Path(__file__).resolve().parents[2]
        out_path = repo_root.parent / "data" / "calendar" / "nl_public_holidays.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Generating NL public holidays from %d to %d → %s",
        args.start_year, args.end_year, out_path,
    )

    # Generate holidays for all requested years
    year_range = range(args.start_year, args.end_year + 1)
    nl_holidays = holidays.Netherlands(years=year_range)

    rows = []
    for date, name in sorted(nl_holidays.items()):
        rows.append({
            "date": date.isoformat(),
            "name": name,
            "day_of_week": DAY_NAMES[date.weekday()],
            "year": date.year,
            "month": date.month,
            "day": date.day,
        })

    # Write CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(
        "Done — %d public holiday records written to %s",
        len(rows), out_path,
    )

    # Print summary by year
    year_counts: dict[int, int] = {}
    for row in rows:
        y = row["year"]
        year_counts[y] = year_counts.get(y, 0) + 1

    logger.info("Per-year breakdown:")
    for y in sorted(year_counts):
        logger.info("  %d: %d holidays", y, year_counts[y])


if __name__ == "__main__":
    main()
