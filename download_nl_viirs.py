#!/usr/bin/env python3

import os
import sys
import json
import ssl
import urllib.request
from urllib.error import HTTPError, URLError
from datetime import datetime, timedelta
import argparse
import subprocess

# Bypass strict SSL verification (helpful to mimic NASA's fallback strategy)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch_url(url, token, out_path=None):
    """Fetches JSON content or downloads a file from the LAADS DAAC."""
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
            if out_path:
                with open(out_path, 'wb') as f:
                    # Write in chunks for large files
                    while True:
                        chunk = response.read(16384)
                        if not chunk:
                            break
                        f.write(chunk)
                return True
            else:
                return response.read().decode('utf-8')
    except (HTTPError, URLError) as e:
        # 404s are completely normal if data hasn't been uploaded for a specific day yet
        if getattr(e, 'code', None) == 404:
            return None
        return get_curl(url, token, out_path)
    except Exception:
        return get_curl(url, token, out_path)

def get_curl(url, token, out_path=None):
    """Fallback cURL execution if Python's urllib fails."""
    args = ['curl', '--fail', '-sS', '-L', '-b', 'session', '--get', url, '-H', f'Authorization: Bearer {token}']
    try:
        if out_path:
            with open(out_path, 'wb') as f:
                subprocess.run(args, stdout=f, check=True)
            return True
        else:
            result = subprocess.run(args, capture_output=True, text=True, check=True)
            return result.stdout
    except subprocess.CalledProcessError:
        return None

def main():
    parser = argparse.ArgumentParser(description="Download VIIRS VNP46A2 (Standard/NRT) for the Netherlands.")
    parser.add_argument('-d', '--dest', default="/projects/prjs2061/data/viirs", help="Destination folder path")
    args = parser.parse_args()

    if os.environ.get('LAADS_TOKEN'):
        token = os.environ.get('LAADS_TOKEN')
        print("Using LAADS_TOKEN from environment.")
    elif os.environ.get('EDL_TOKEN'):
        token = os.environ.get('EDL_TOKEN')
        print("Using EDL_TOKEN from environment.")
    else:
        print("Error: LAADS_TOKEN or EDL_TOKEN environment variable not set. Please source your .env file or export your token.", file=sys.stderr)
        sys.exit(1)

    # The Netherlands fits perfectly within the h18v03 tile on the 10x10 degree geographic grid
    TARGET_TILE = "h18v03"
    
    # LAADS DAAC endpoints (Collection 5200 is standard for VIIRS VNP46A2 Collection 2)
    STD_URL = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP46A2"
    NRT_URL = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP46A2_NRT"

    os.makedirs(args.dest, exist_ok=True)

    # Note: Actual VIIRS satellite data collection began in Jan 2012
    start_date = datetime(2012, 1, 1).date()
    end_date = datetime.today().date()
    
    print(f"Starting download for tile {TARGET_TILE} (Netherlands) from {start_date} to {end_date}...\n")

    current_date = start_date
    delta = timedelta(days=1)

    while current_date <= end_date:
        year = current_date.strftime("%Y")
        doy = current_date.strftime("%j") # Day of Year (001-366)
        
        file_found = False
        
        # Try the Standard historical archive first. If missing (e.g., recent days), try the NRT archive.
        for base_url in [STD_URL, NRT_URL]:
            dir_url = f"{base_url}/{year}/{doy}"
            json_url = f"{dir_url}.json"
            
            directory_json = fetch_url(json_url, token)
            if directory_json:
                try:
                    directory_data = json.loads(directory_json)
                    for item in directory_data.get('content', []):
                        filename = item.get('name', '')
                        size = int(item.get('size', 0))
                        
                        # Filter strictly for the Netherlands tile and check it's an HDF5 data file
                        if TARGET_TILE in filename and size > 0 and filename.endswith('.h5'):
                            file_url = f"{dir_url}/{filename}"
                            dest_path = os.path.join(args.dest, filename)
                            
                            # Skip if the file already exists locally and the size matches (Resume capability)
                            if os.path.exists(dest_path) and os.path.getsize(dest_path) == size:
                                print(f"[{current_date}] {filename} already exists. Skipping.")
                            else:
                                source_type = "Standard" if base_url == STD_URL else "NRT"
                                print(f"[{current_date}] Downloading {filename} ({source_type})...")
                                fetch_url(file_url, token, dest_path)
                                
                            file_found = True
                            break
                except json.JSONDecodeError:
                    pass
            
            if file_found:
                break
                
        # Periodic heartbeat so the terminal doesn't look frozen during months with missing satellite data
        if not file_found and current_date.day == 1:
            print(f"[{current_date}] Scanning month: {current_date.strftime('%Y-%m')}...")

        current_date += delta

    print("\nData pull complete.")

if __name__ == '__main__':
    main()