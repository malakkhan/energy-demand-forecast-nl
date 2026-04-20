#!/usr/bin/env python3
import asyncio
import logging
import os
import argparse
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import requests
from requests import Session

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("KNMI_Download")
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

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
        logger.info(f"Dataset file '{filename}' was already downloaded. Skipping.")
        return True, filename

    endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    
    retries = 0
    while retries < 5:
        get_file_response = session.get(endpoint)
        if get_file_response.status_code == 429:
            logger.warning(f"Rate limit exceeded (downloading {filename}). Sleeping 20s...")
            time.sleep(20)
            retries += 1
            continue
        break

    if get_file_response.status_code != 200:
        logger.warning(f"Unable to get file: {filename}")
        return False, filename

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    if not download_url:
        return False, filename

    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        logger.warning(f"Unable to download file {filename}: {e}")
        return False, filename

    logger.info(f"Downloaded dataset file '{filename}'")
    return True, filename

def list_dataset_files(
    session: Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    params: dict[str, str],
) -> tuple[list[str], dict[str, Any]]:
    list_files_endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files"
    
    retries = 0
    while retries < 5:
        list_files_response = session.get(list_files_endpoint, params=params)
        if list_files_response.status_code == 429:
            logger.warning("Rate limit exceeded while listing files. Sleeping 20s...")
            time.sleep(20)
            retries += 1
            continue
        break

    if list_files_response.status_code != 200:
        logger.error(f"Failed to list files. Status: {list_files_response.status_code}, Body: {list_files_response.text}")
        raise Exception("Unable to list dataset files")

    try:
        list_files_response_json = list_files_response.json()
        dataset_files = list_files_response_json.get("files", [])
        dataset_filenames = list(map(lambda x: x.get("filename"), dataset_files))
        return dataset_filenames, list_files_response_json
    except Exception as e:
        logger.exception(e)
        raise Exception(e)

def get_max_worker_count(filesizes):
    if not filesizes:
        return 5
    size_for_threading = 10_000_000  # 10 MB threshold
    average = sum(filesizes) / len(filesizes)
    return 1 if average > size_for_threading else 10

def extract_year_from_filename(filename: str) -> int:
    """
    Attempt to extract a 4-digit year from the filename to filter for 2012+ data.
    Returns -1 if we cannot definitively parse a year, so we can keep it as a fallback.
    """
    # Look for 'YYYY' in common KNMI formats like _20201231, -202012, or just 19xx / 20xx
    match = re.search(r'(?:_|-|^)(\d{4})(?:\d{4}|\d{2}|_|-|\.nc)', filename)
    if match:
        year = int(match.group(1))
        if 1900 < year <= datetime.now().year:
            return year
            
    # Fallback just finding a general year format 19xx/20xx
    match = re.search(r'(19|20)\d{2}', filename)
    if match:
        return int(match.group(0))
        
    return -1

async def main():
    parser = argparse.ArgumentParser(description="Download KNMI hourly meteorological data via Open Data API")
    parser.add_argument('-k', '--key', help="KNMI Open Data API Key. Can also use KNMI_API_KEY environment variable.")
    parser.add_argument('-d', '--dataset', default="uurwaarden", help="KNMI dataset name. Note: Sometimes KNMI uses 'hourly-in-situ-meteorological-observations'.")
    parser.add_argument('-v', '--version', default="1", help="KNMI dataset version.")
    parser.add_argument('--dest', default="/projects/prjs2061/data/knmi", help="Destination folder path")
    parser.add_argument('--start-year', type=int, default=2012, help="Start year to download data for")
    parser.add_argument('--overwrite', action='store_true', help="Overwrite existing files explicitly")
    
    args = parser.parse_args()

    api_key = args.key or os.environ.get("KNMI_API_KEY")
    if not api_key:
        logger.warning("No API key provided! Falling back to the shared anonymous key from documentation.")
        api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImVlNDFjMWI0MjlkODQ2MThiNWI4ZDViZDAyMTM2YTM3IiwiaCI6Im11cm11cjEyOCJ9"
        
    dataset_name = args.dataset
    dataset_version = args.version
    base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
    download_directory = args.dest

    os.makedirs(download_directory, exist_ok=True)

    session = requests.Session()
    session.headers.update({"Authorization": api_key})

    filenames = []
    file_sizes = []
    max_keys = 500
    next_page_token = None
    
    logger.info(f"Listing files for {dataset_name} version {dataset_version}...")
    while True:
        params = {"maxKeys": str(max_keys)}
        if next_page_token:
            params["nextPageToken"] = next_page_token
            
        dataset_filenames, response_json = list_dataset_files(
            session, base_url, dataset_name, dataset_version, params
        )
        
        for i, filename in enumerate(dataset_filenames):
            # Include files that represent data from 2012 onwards
            year = extract_year_from_filename(filename)
            if year == -1 or year >= args.start_year:
                filenames.append(filename)
                filesize = response_json.get("files", [])[i].get("size", 0)
                file_sizes.append(filesize)

        next_page_token = response_json.get("nextPageToken")
        if not next_page_token:
            break

    logger.info(f"Files found (filtered >= {args.start_year}): {len(filenames)}")

    if not filenames:
        logger.info("No files matched the criteria, or dataset is empty. Exiting.")
        return

    worker_count = get_max_worker_count(file_sizes)
    logger.info(f"Starting bulk download with {worker_count} concurrent threads...")
    
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=worker_count)
    futures = []

    for filename in filenames:
        future = loop.run_in_executor(
            executor,
            download_dataset_file,
            session,
            base_url,
            dataset_name,
            dataset_version,
            filename,
            download_directory,
            args.overwrite,
        )
        futures.append(future)

    future_results = await asyncio.gather(*futures)
    logger.info(f"Finished '{dataset_name}' dataset download.")

    failed_downloads = [res[1] for res in future_results if not res[0]]
    if failed_downloads:
        logger.warning(f"Failed to download {len(failed_downloads)} files.")
        logger.warning(f"Examples of failures: {failed_downloads[:5]}")

if __name__ == "__main__":
    asyncio.run(main())
