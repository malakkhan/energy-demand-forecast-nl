#!/usr/bin/env python3
"""Download Dutch public and school holidays from the OpenHolidays API.

Fetches all public holidays and school holidays for the Netherlands
(country code NL) from January 2012 to December 2025 and saves them
into a single CSV file.

Usage
-----
    python src/download/download_nl_holidays.py

    # Custom output path:
    python src/download/download_nl_holidays.py \
        --dest /projects/prjs2061/data/calendar/nl_holidays.csv

API docs:  https://openholidaysapi.org
License:   Open Database License (ODbL)
"""

import argparse
import csv
import logging
import os
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("OpenHolidays_Download")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://openholidaysapi.org"
COUNTRY_ISO = "NL"
LANGUAGE_ISO = "EN"  # English names for holiday labels
START_YEAR = 2012
END_YEAR = 2025

CSV_COLUMNS = [
    "id",
    "type",
    "name_en",
    "name_nl",
    "start_date",
    "end_date",
    "nationwide",
    "regional_scope",
    "temporal_scope",
    "subdivisions",
    "tags",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_name(name_list: list, lang: str) -> str:
    """Extract the localised name from the API's name array."""
    if not name_list:
        return ""
    for entry in name_list:
        if entry.get("language", "").upper() == lang.upper():
            return entry.get("text", "")
    # Fallback: return the first available name
    return name_list[0].get("text", "")


def _extract_subdivisions(subdivisions: list) -> str:
    """Comma-separated subdivision codes (e.g. 'NL-NH,NL-ZH')."""
    if not subdivisions:
        return ""
    return ",".join(s.get("code", "") for s in subdivisions)


def _fetch_holidays(endpoint: str, year: int, session: requests.Session) -> list:
    """Fetch one year of holidays from a given endpoint with retry logic."""
    params = {
        "countryIsoCode": COUNTRY_ISO,
        "validFrom": f"{year}-01-01",
        "validTo": f"{year}-12-31",
        "languageIsoCode": LANGUAGE_ISO,
    }

    for attempt in range(5):
        try:
            resp = session.get(
                f"{BASE_URL}/{endpoint}",
                params=params,
                timeout=30,
            )
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                logger.warning(
                    "Rate limited on %s (year %d). Waiting %ds (attempt %d/5)…",
                    endpoint, year, wait, attempt + 1,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as exc:
            if attempt < 4:
                wait = 5 * (attempt + 1)
                logger.warning(
                    "Request failed for %s (year %d): %s — retrying in %ds…",
                    endpoint, year, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "Giving up on %s (year %d) after 5 attempts: %s",
                    endpoint, year, exc,
                )
                return []

    return []


def _parse_holiday(raw: dict, holiday_type: str) -> dict:
    """Convert a raw API holiday object into a flat dict."""
    return {
        "id": raw.get("id", ""),
        "type": holiday_type,
        "name_en": _extract_name(raw.get("name", []), "EN"),
        "name_nl": _extract_name(raw.get("name", []), "NL"),
        "start_date": raw.get("startDate", ""),
        "end_date": raw.get("endDate", ""),
        "nationwide": raw.get("nationwide", ""),
        "regional_scope": raw.get("regionalScope", ""),
        "temporal_scope": raw.get("temporalScope", ""),
        "subdivisions": _extract_subdivisions(raw.get("subdivisions", [])),
        "tags": raw.get("tags", ""),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Dutch public and school holidays from the OpenHolidays API.",
    )
    parser.add_argument(
        "--dest",
        type=str,
        default=None,
        help="Destination CSV file path "
             "(default: <project>/data/calendar/nl_holidays.csv)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=START_YEAR,
        help=f"First year to fetch (default: {START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=END_YEAR,
        help=f"Last year to fetch (default: {END_YEAR})",
    )
    args = parser.parse_args()

    # Resolve output path
    if args.dest:
        out_path = Path(args.dest)
    else:
        repo_root = Path(__file__).resolve().parents[2]
        out_path = repo_root.parent / "data" / "calendar" / "nl_holidays.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Fetching NL holidays from %d to %d → %s",
        args.start_year, args.end_year, out_path,
    )

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    all_holidays: list[dict] = []
    seen_ids: set[str] = set()

    for year in range(args.start_year, args.end_year + 1):
        # ── Public holidays ──
        logger.info("  Fetching PublicHolidays for %d …", year)
        public = _fetch_holidays("PublicHolidays", year, session)
        for h in public:
            hid = h.get("id", "")
            if hid and hid not in seen_ids:
                seen_ids.add(hid)
                all_holidays.append(_parse_holiday(h, "Public"))
        logger.info("    → %d public holidays", len(public))

        # ── School holidays ──
        logger.info("  Fetching SchoolHolidays for %d …", year)
        school = _fetch_holidays("SchoolHolidays", year, session)
        for h in school:
            hid = h.get("id", "")
            if hid and hid not in seen_ids:
                seen_ids.add(hid)
                all_holidays.append(_parse_holiday(h, "School"))
        logger.info("    → %d school holidays", len(school))

        # Be polite — short pause between years
        time.sleep(0.5)

    # ── Sort by start_date, then type ──
    all_holidays.sort(key=lambda r: (r["start_date"], r["type"], r["name_en"]))

    # ── Write CSV ──
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(all_holidays)

    logger.info(
        "Done — %d unique holiday records written to %s",
        len(all_holidays), out_path,
    )


if __name__ == "__main__":
    main()
