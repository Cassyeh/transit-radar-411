"""
seeds/download_data.py

PURPOSE
-------
A reusable file downloader for the Transit Radar 411 pipeline.

Contains two things:
1. download_file() — a reusable function that downloads any file
   from any URL and saves it to a destination folder. Sends email
   notifications on success, failure and completion. Can be imported
   and used by any other script in the project.

2. main() — calls download_file() for all 48 monthly OpenSky
   COVID-19 flight dataset files from Zenodo (January 2019 to
   December 2022). Files are saved to seeds/historical/.

REUSABILITY
-----------
download_file() is designed to be generic. Any script can use it:

    from seeds.download_data import download_file

    download_file(
        url="https://example.com/data.csv.gz",
        destination_folder="seeds/historical",
        file_number=1,
        total_files=1
    )

EMAIL NOTIFICATIONS
-------------------
After each successful download:
    "Download 3/48 complete — flightlist_20200301..."

After each failed download (with retry info):
    "Download ERROR (file 3/48) — attempt 2 of 3"

After the final file:
    "All downloads complete — 48/48 successful, 12.3 minutes"

HOW TO RUN
----------
From your project root (transit-radar-411/):
    python seeds/download_data.py

Files are saved to seeds/historical/ — already in .gitignore.
Expected total download size: ~5-15 GB depending on months selected.
Expected runtime: 30-90 minutes depending on your internet speed.
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone

# Add project root to path so we can import from utils/
# This is needed when running the script directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.emails_utils import (
    send_email,
    send_success_email,
    send_error_email,
    send_completion_email
)
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Base URL for the Zenodo OpenSky COVID-19 flight dataset
# Record ID: 7923702
ZENODO_BASE_URL = "https://zenodo.org/records/7923702/files"

# Where downloaded files are saved
SEEDS_DIR       = os.path.dirname(os.path.abspath(__file__))
HISTORICAL_DIR  = os.path.join(SEEDS_DIR, "historical")

# Download settings
MAX_RETRIES     = 3       # Number of retry attempts per file
RETRY_DELAY_S   = 30      # Seconds to wait between retries
CHUNK_SIZE      = 8192    # Bytes per chunk when streaming download
                          # 8192 bytes = 8KB per chunk
                          # Keeps memory usage flat for large files


def _attempt_download(
    url: str,
    destination_path: str,
    filename: str,
    file_number: int,
    total_files: int
) -> bool:
    """
    Attempts to download a file with up to MAX_RETRIES attempts.
    Returns True on success, False if all attempts fail.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(
                f"  Downloading [{file_number}/{total_files}]: {filename} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )

            start_time = time.time()
            response   = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()

            bytes_downloaded = 0
            with open(destination_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

            file_size_mb = bytes_downloaded / (1024 * 1024)
            duration_s   = time.time() - start_time
            speed_mbps   = file_size_mb / duration_s if duration_s > 0 else 0

            log.info(
                f"  Downloaded: {filename} | "
                f"{file_size_mb:.1f} MB | "
                f"{duration_s:.0f}s | "
                f"{speed_mbps:.1f} MB/s"
            )

            send_success_email(
                filename=filename,
                file_number=file_number,
                total_files=total_files,
                file_size_mb=file_size_mb
            )
            return True

        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error: {e}"
            log.warning(f"  Attempt {attempt}/{MAX_RETRIES} failed: {error_msg}")

        except requests.exceptions.Timeout:
            error_msg = "Connection timed out after 120 seconds"
            log.warning(f"  Attempt {attempt}/{MAX_RETRIES} failed: {error_msg}")

        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {e}"
            log.warning(f"  Attempt {attempt}/{MAX_RETRIES} failed: {error_msg}")

        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            log.error(f"  Attempt {attempt}/{MAX_RETRIES} failed: {error_msg}")

        send_error_email(
            filename=filename,
            file_number=file_number,
            total_files=total_files,
            error=error_msg,
            attempt=attempt,
            max_attempts=MAX_RETRIES
        )

        if os.path.exists(destination_path):
            os.remove(destination_path)

        if attempt < MAX_RETRIES:
            log.info(f"  Waiting {RETRY_DELAY_S}s before retry...")
            time.sleep(RETRY_DELAY_S)

    return False


# ─────────────────────────────────────────────────────────────
# CORE DOWNLOAD FUNCTION — reusable
# ─────────────────────────────────────────────────────────────

def download_file(
    url: str,
    destination_folder: str,
    file_number: int = 1,
    total_files: int = 1,
    filename: str = None,
    url_fallback: str = None,
    filename_fallback: str = None
) -> bool:
    """
    Downloads a single file from a URL and saves it to a folder.

    This function is reusable — it can download any file from
    any URL. The Zenodo-specific logic lives in main() below.
    If all retries fail and a fallback URL is provided, tries the fallback URL before
    giving up entirely.
    Fallback is used for Zenodo files where the last day of
    the month in the filename is sometimes 30 instead of the
    true last day

    Parameters
    ----------
    url                : The full URL of the file to download
    url_fallback       : The full URL of the file to download if the url failed, 404 client error
    destination_folder : Local folder to save the file into
    file_number        : Position in the download sequence (for emails)
    total_files        : Total files in the sequence (for emails)
    filename           : Override filename (defaults to last part of URL)
    filename_fallback  : Override filename (defaults to last part of URL fallback)

    Returns
    -------
    bool
        True if download succeeded, False if all retries failed.

    How streaming download works
    ----------------------------
    We use requests with stream=True which means Python connects
    to the URL and receives the file in small chunks (8KB at a time)
    rather than loading the entire file into memory at once.
    This is essential for large files like the Zenodo CSVs which
    can be several hundred MB each.

    For each chunk received, we write it to disk immediately.
    This keeps memory usage flat regardless of file size.
    """
    # Determine filename from URL if not provided
    if not filename:
        filename = url.split("/")[-1]

    destination_path = os.path.join(destination_folder, filename)

    # Skip if file already exists and is not empty
    # This allows safe re-running of the script if interrupted
    if os.path.exists(destination_path) and os.path.getsize(destination_path) > 0:
        size_mb = os.path.getsize(destination_path) / (1024 * 1024)
        log.info(f"  Already exists: {filename} ({size_mb:.1f} MB) — skipping.")
        return True

    # Create destination folder if it does not exist
    os.makedirs(destination_folder, exist_ok=True)

    # Try primary URL first
    success = _attempt_download(
        url=url,
        destination_path=destination_path,
        filename=filename,
        file_number=file_number,
        total_files=total_files
    )

    if success:
        return True

    # Primary failed — try fallback URL if provided
    if url_fallback and filename_fallback:
        log.info(f"  Primary URL failed. Trying fallback: {filename_fallback}")

        destination_path_fallback = os.path.join(
            destination_folder, filename_fallback
        )

        # Check if fallback already exists
        if os.path.exists(destination_path_fallback) and \
           os.path.getsize(destination_path_fallback) > 0:
            size_mb = os.path.getsize(destination_path_fallback) / (1024 * 1024)
            log.info(f"  Fallback already exists: {filename_fallback} ({size_mb:.1f} MB) — skipping.")
            return True

        success = _attempt_download(
            url=url_fallback,
            destination_path=destination_path_fallback,
            filename=filename_fallback,
            file_number=file_number,
            total_files=total_files
        )

        if success:
            return True

    return False

    
# ─────────────────────────────────────────────────────────────
# BUILD LIST OF ALL 48 ZENODO FILE URLS
# ─────────────────────────────────────────────────────────────

def build_zenodo_urls() -> list[dict]:
    """
    Builds the list of all 48 monthly file URLs from the
    OpenSky COVID-19 flight dataset on Zenodo.

    Coverage: January 2019 to December 2022 (48 months).

    Each entry in the returned list is a dict with:
        url      — the full download URL
        filename — the expected filename
        year     — the year (for logging)
        month    — the month (for logging)

    File naming pattern on Zenodo:
        flightlist_YYYYMMDD_YYYYMMDD.csv.gz
    Where the first date is the first day of the month and
    the second date is the last day of the month.

    For each month we try the correct last day first.
    If that URL returns 404, we fall back to the 30th.
    This handles edge cases where Zenodo filenames use
    the 30th instead of the true last day of the month.
    """
    import calendar

    urls = []

    for year in range(2019, 2023):      # 2019, 2020, 2021, 2022
        for month in range(1, 13):      # 1 through 12

            # Get the last day of this month
            last_day = calendar.monthrange(year, month)[1]

            # Build the filename
            start_date = f"{year}{month:02d}01"
            end_date   = f"{year}{month:02d}{last_day:02d}"
            end_date_30    = f"{year}{month:02d}30"
            filename   = f"flightlist_{start_date}_{end_date}.csv.gz"
            filename_30    = f"flightlist_{start_date}_{end_date_30}.csv.gz"

            # Build the full Zenodo URL
            url = f"{ZENODO_BASE_URL}/{filename}?download=1"

            # Fallback URL — 30th of month
            url_30   = f"{ZENODO_BASE_URL}/{filename_30}?download=1"

            urls.append({
                "url":      url,
                "url_fallback": url_30,
                "filename": filename,
                "filename_fallback": filename_30,
                "year":     year,
                "month":    month
            })

    return urls


# ─────────────────────────────────────────────────────────────
# MAIN — downloads all 48 files
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  Transit Radar 411 — Historical Data Downloader")
    log.info("=" * 60)
    log.info(f"  Destination: {HISTORICAL_DIR}")
    log.info(f"  Max retries per file: {MAX_RETRIES}")

    # Build list of all 48 files to download
    files = build_zenodo_urls()
    total = len(files)
    log.info(f"  Total files to download: {total}")

    # Send a start notification
    send_email(
        subject="Transit Radar 411 — Download started",
        body=(
            f"Starting download of {total} monthly flight dataset files.\n\n"
            f"  Coverage : January 2019 to December 2022\n"
            f"  Source   : Zenodo record 7923702 (OpenSky COVID-19 dataset)\n"
            f"  Saving to: seeds/historical/\n\n"
            f"You will receive an email after each file completes."
        )
    )

    # Track results
    successful     = 0
    failed         = 0
    overall_start  = time.time()

    # Download each file
    for i, file_info in enumerate(files, start=1):
        log.info(
            f"\n[{i}/{total}] {file_info['year']}-{file_info['month']:02d} — "
            f"{file_info['filename']}"
        )

        success = download_file(
            url=file_info["url"],
            url_fallback=file_info.get("url_fallback"),
            destination_folder=HISTORICAL_DIR,
            file_number=i,
            total_files=total,
            filename=file_info["filename"],
            filename_fallback=file_info.get("filename_fallback")
        )

        if success:
            successful += 1
        else:
            failed += 1

    # Calculate total duration
    duration_minutes = (time.time() - overall_start) / 60

    # Log final summary
    log.info("\n" + "=" * 60)
    log.info("  Download Summary")
    log.info("=" * 60)
    log.info(f"  Total files    : {total}")
    log.info(f"  Successful     : {successful}")
    log.info(f"  Failed         : {failed}")
    log.info(f"  Duration       : {duration_minutes:.1f} minutes")
    log.info(f"  Files saved to : {HISTORICAL_DIR}")

    # Send final completion email
    send_completion_email(
        total_files=total,
        successful=successful,
        failed=failed,
        duration_minutes=duration_minutes
    )

    if failed > 0:
        log.warning(
            f"  {failed} file(s) failed. Check emails for details. "
            f"Re-run this script to retry — existing files are skipped automatically."
        )
    else:
        log.info("  All files downloaded successfully.")
        log.info("  Next step: run seeds/load_historical_states.py")


if __name__ == "__main__":
    main()