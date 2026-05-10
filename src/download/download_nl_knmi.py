#!/usr/bin/env python3
"""Bulk-download KNMI hourly in-situ meteorological observations via Open Data API v1.

Supports both the non-validated and validated datasets:

  Non-validated (default):
    python download_nl_knmi.py --dest /projects/prjs2061/data/knmi

  Validated:
    python download_nl_knmi.py \
        --dataset hourly-in-situ-meteorological-observations-validated \
        --dest /projects/prjs2061/data/knmi_validated

API keys are read from the environment (or .env file):
  - KNMI_API_KEY             → used for the non-validated dataset (default)
  - KNMI_VALIDATED_API_KEY   → used when --dataset contains 'validated'

You can always override with -k/--key.
"""
import asyncio
import logging
import os
import argparse
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests import Session

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("KNMI_Download")
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


def load_dotenv(path: Path) -> None:
    """Load KEY=VALUE pairs from a .env file into os.environ (no-op if missing)."""
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def download_dataset_file(
    session: Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    filename: str,
    directory: str,
    overwrite: bool,
) -> tuple[bool, str]:
    file_path = Path(directory, filename).resolve()
    if not overwrite and file_path.exists():
        logger.info(f"Already downloaded '{filename}', skipping.")
        return True, filename

    endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    timeout = 60  # seconds

    get_file_response = None
    for attempt in range(5):
        get_file_response = session.get(endpoint, timeout=timeout)
        if get_file_response.status_code == 429:
            wait = 20 * (attempt + 1)
            logger.warning(f"Rate limited on '{filename}'. Waiting {wait}s (attempt {attempt + 1}/5)...")
            time.sleep(wait)
            continue
        break

    if get_file_response is None or get_file_response.status_code != 200:
        status = get_file_response.status_code if get_file_response is not None else "N/A"
        logger.warning(f"Failed to get download URL for '{filename}' (status {status})")
        return False, filename

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    if not download_url:
        logger.warning(f"No temporaryDownloadUrl in response for '{filename}'")
        return False, filename

    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        logger.warning(f"Download failed for '{filename}': {e}")
        return False, filename

    logger.info(f"Downloaded '{filename}'")
    return True, filename


def list_dataset_files(
    session: Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    params: dict[str, str],
) -> tuple[list[str], dict[str, Any]]:
    endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files"

    response = None
    for attempt in range(5):
        response = session.get(endpoint, params=params, timeout=60)
        if response.status_code == 429:
            wait = 20 * (attempt + 1)
            logger.warning(f"Rate limited while listing files. Waiting {wait}s...")
            time.sleep(wait)
            continue
        break

    if response is None or response.status_code != 200:
        status = response.status_code if response is not None else "N/A"
        body_text = response.text if response is not None else ""
        raise RuntimeError(f"Failed to list dataset files (status {status}): {body_text}")

    body = response.json()
    filenames = [f["filename"] for f in body.get("files", [])]
    return filenames, body


def worker_count_for(file_sizes: list[int]) -> int:
    if not file_sizes:
        return 5
    avg = sum(file_sizes) / len(file_sizes)
    return 1 if avg > 10_000_000 else 10


def year_from_filename(filename: str) -> int:
    """Return the 4-digit year embedded in a KNMI filename, or -1 if indeterminate."""
    match = re.search(r"(?:_|-|^)(\d{4})(?:\d{4}|\d{2}|[_\-.])", filename)
    if match:
        year = int(match.group(1))
        if 1900 < year <= datetime.now().year:
            return year
    match = re.search(r"(19|20)\d{2}", filename)
    if match:
        return int(match.group(0))
    return -1


async def main() -> None:
    # Load .env from project root (two levels up from this file: src/download/ -> project root)
    env_file = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_file)

    parser = argparse.ArgumentParser(
        description="Bulk-download KNMI hourly in-situ meteorological observations via Open Data API v1"
    )
    parser.add_argument(
        "-k", "--key",
        help="KNMI bulk-download API key. Falls back to KNMI_API_KEY env var.",
    )
    parser.add_argument(
        "-n", "--dataset",
        default="hourly-in-situ-meteorological-observations",
        help="KNMI dataset name (default: hourly-in-situ-meteorological-observations). "
             "Use 'hourly-in-situ-meteorological-observations-validated' for the validated dataset.",
    )
    parser.add_argument(
        "-V", "--version",
        default="1.0",
        help="Dataset version (default: 1.0)",
    )
    parser.add_argument(
        "-d", "--dest",
        default=None,
        help="Destination directory for downloaded files "
             "(default: /projects/prjs2061/data/knmi for non-validated, "
             "/projects/prjs2061/data/knmi_validated for validated)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2015,
        help="Only download files from this year onwards (default: 2015 — dataset starts 2015-01-31)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Only download files up to and including this year (default: no limit)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download and overwrite files that already exist locally",
    )
    args = parser.parse_args()

    # Select the correct API key based on dataset name
    is_validated = "validated" in args.dataset.lower()
    if args.key:
        api_key = args.key
    elif is_validated:
        api_key = os.environ.get("KNMI_VALIDATED_API_KEY")
    else:
        api_key = os.environ.get("KNMI_API_KEY")

    if not api_key:
        env_var = "KNMI_VALIDATED_API_KEY" if is_validated else "KNMI_API_KEY"
        parser.error(
            f"No API key found. Pass -k/--key or set {env_var} in the environment / .env file."
        )

    # Default destination based on dataset type
    if args.dest is None:
        args.dest = (
            "/projects/prjs2061/data/knmi_validated" if is_validated
            else "/projects/prjs2061/data/knmi"
        )

    base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
    download_dir = args.dest

    os.makedirs(download_dir, exist_ok=True)

    session = requests.Session()
    session.headers.update({"Authorization": api_key})

    logger.info(f"Listing files for dataset '{args.dataset}' v{args.version} (start_year={args.start_year})...")

    filenames: list[str] = []
    file_sizes: list[int] = []
    next_page_token: str | None = None

    page = 0
    while True:
        page += 1
        params: dict[str, str] = {"maxKeys": "500"}
        if next_page_token:
            params["nextPageToken"] = next_page_token

        logger.info(f"  Fetching file list page {page}...")
        batch_names, body = list_dataset_files(session, base_url, args.dataset, args.version, params)
        batch_files = body.get("files", [])

        for meta, name in zip(batch_files, batch_names):
            year = year_from_filename(name)
            if year != -1 and year < args.start_year:
                continue
            if args.end_year is not None and year != -1 and year > args.end_year:
                continue
            filenames.append(name)
            file_sizes.append(meta.get("size", 0))

        next_page_token = body.get("nextPageToken")
        if not next_page_token:
            logger.info("Retrieved all file listings.")
            break

    logger.info(f"Files to download (filtered >= {args.start_year}): {len(filenames)}")
    if not filenames:
        logger.info("Nothing to download. Exiting.")
        return

    n_workers = worker_count_for(file_sizes)
    logger.info(f"Starting download with {n_workers} concurrent thread(s)...")

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=n_workers)

    futures = [
        loop.run_in_executor(
            executor,
            download_dataset_file,
            session,
            base_url,
            args.dataset,
            args.version,
            name,
            download_dir,
            args.overwrite,
        )
        for name in filenames
    ]

    results = await asyncio.gather(*futures)
    executor.shutdown(wait=False)

    failed = [name for ok, name in results if not ok]
    logger.info(f"Download complete. Success: {len(results) - len(failed)}, Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed files ({len(failed)}): {failed[:10]}{'...' if len(failed) > 10 else ''}")


if __name__ == "__main__":
    asyncio.run(main())
