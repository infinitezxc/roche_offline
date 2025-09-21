#!/usr/bin/env python3
"""
Script to OCR all PDF files in the ori directory and save results to Redis.
Uses the existing OCR functionality from ocr.py with proper imports.
Runs continuously, checking for new files every 30 minutes.
"""

import os
import sys
import asyncio
import logging
import time
import hashlib
import json
from pathlib import Path
from datetime import datetime
import PyPDF2

from utils.config import config
from utils.redis_client import redis_client
from utils.ocr import get_ocr

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


max_page_count = 100

def get_pdf_page_count(pdf_path: str) -> int:
    """Get the number of pages in a PDF file."""
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            return len(pdf_reader.pages)
    except Exception as e:
        logger.warning(f"Could not get page count for {pdf_path}: {e}")
        return 0


def find_pdf_files(directory: str) -> list[str]:
    """Find all PDF files in the given directory recursively."""
    pdf_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files


def get_file_md5(file_path: str) -> str:
    """Calculate MD5 hash of a file."""
    try:
        with open(file_path, "rb") as f:
            file_data = f.read()
            return hashlib.md5(file_data).hexdigest()
    except Exception as e:
        logger.error(f"Error calculating MD5 for {file_path}: {e}")
        return None


def get_redis_processed_files() -> set:
    """Get all MD5 hashes of files processed and stored in Redis."""
    try:
        keys = redis_client.client.keys("ocr_results:ocr_*")
        # Extract MD5 hashes from Redis keys
        md5_hashes = set()
        for key in keys:
            # Handle both string and bytes key formats
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            # Extract MD5 from key format: "ocr_results:ocr_<md5>"
            if key.startswith("ocr_results:ocr_"):
                md5_hash = key[16:]  # Remove "ocr_results:ocr_" prefix
                md5_hashes.add(md5_hash)
        logger.info(f"Found {len(md5_hashes)} processed files in Redis")
        return md5_hashes
    except Exception as e:
        logger.error(f"Error getting Redis processed files: {e}")
        return set()


def get_file_md5_mapping(pdf_files: list[str]) -> dict[str, str]:
    """Create mapping of file paths to their MD5 hashes."""
    md5_mapping = {}
    failed_files = []

    logger.info(f"Calculating MD5 hashes for {len(pdf_files)} files...")

    for i, file_path in enumerate(pdf_files):
        if (i + 1) % 500 == 0:  # Progress logging every 500 files
            logger.info(f"Progress: {i + 1}/{len(pdf_files)} files processed")

        md5_hash = get_file_md5(file_path)
        if md5_hash:
            md5_mapping[file_path] = md5_hash
        else:
            failed_files.append(file_path)

    logger.info(f"MD5 calculation complete: {len(md5_mapping)} successful, {len(failed_files)} failed")

    if failed_files:
        logger.warning(f"Files that failed MD5 calculation:")
        for failed_file in failed_files[:10]:  # Show first 10 failed files
            logger.warning(f"  {failed_file}")
        if len(failed_files) > 10:
            logger.warning(f"  ... and {len(failed_files) - 10} more files")

    return md5_mapping


def find_unprocessed_files(pdf_files: list[str]) -> tuple[list[str], dict[str, str]]:
    """Find files that haven't been processed (not in Redis)."""
    redis_processed_md5s = get_redis_processed_files()
    file_md5_mapping = get_file_md5_mapping(pdf_files)

    unprocessed_files = []
    unprocessed_md5_mapping = {}
    processed_files_count = 0

    logger.info(f"Comparing {len(file_md5_mapping)} files with {len(redis_processed_md5s)} Redis entries...")

    for file_path, md5_hash in file_md5_mapping.items():
        if md5_hash not in redis_processed_md5s:
            unprocessed_files.append(file_path)
            unprocessed_md5_mapping[file_path] = md5_hash
        else:
            processed_files_count += 1

    # Calculate the accounting
    total_files = len(pdf_files)
    files_with_md5 = len(file_md5_mapping)
    files_failed_md5 = total_files - files_with_md5
    files_processed = processed_files_count
    files_unprocessed = len(unprocessed_files)

    logger.info("=== FILE PROCESSING SUMMARY ===")
    logger.info(f"Total PDF files found: {total_files}")
    logger.info(f"Files with successful MD5: {files_with_md5}")
    logger.info(f"Files that failed MD5 calculation: {files_failed_md5}")
    logger.info(f"Files already processed (in Redis): {files_processed}")
    logger.info(f"Files to be processed: {files_unprocessed}")
    logger.info(f"Verification: {files_with_md5} = {files_processed} + {files_unprocessed} ✓")

    if files_unprocessed > 0:
        logger.info(f"Found {len(unprocessed_files)} unprocessed files:")
        for i, file_path in enumerate(unprocessed_files[:5]):  # Show first 5
            logger.info(f"  {i+1}. {file_path}")
        if len(unprocessed_files) > 5:
            logger.info(f"  ... and {len(unprocessed_files) - 5} more files")

    return unprocessed_files, unprocessed_md5_mapping



async def process_pdf_file(pdf_path: str, semaphore: asyncio.Semaphore) -> dict:
    """Process a single PDF file using the OCR service with concurrency control."""
    async with semaphore:
        try:
            # Get page count before processing
            page_count = get_pdf_page_count(pdf_path)
            file_name = os.path.basename(pdf_path)

            # Skip files with more than max_page_count pages
            if page_count > max_page_count:
                logger.info(f"Skipping large file: {file_name} ({page_count} pages > {max_page_count} page limit)")
                return {"file": pdf_path, "status": "skipped", "reason": f"Too many pages ({page_count} > {max_page_count})"}

            logger.info(f"Processing: data/policy/{file_name} page_count: {page_count}")

            # Call the OCR function with appropriate parameters
            result = await get_ocr(
                input_file=pdf_path,
                file_category="mixed",
                file_suffix="pdf",
                prompt="",
                max_pages=None,
                override=False
            )

            logger.info(f"Successfully processed: {pdf_path}")
            return {"file": pdf_path, "status": "success", "result": result}

        except Exception as e:
            logger.error(f"Error processing {pdf_path}: {str(e)}")
            return {"file": pdf_path, "status": "error", "error": str(e)}


async def process_new_files(directory_path: str):
    """Process only new or modified PDF files in the specified directory."""
    ori_directory = directory_path
    max_concurrency = 32

    if not os.path.exists(ori_directory):
        logger.error(f"Directory {ori_directory} does not exist")
        return

    # Find all PDF files
    all_pdf_files = find_pdf_files(ori_directory)
    logger.info(f"Found {len(all_pdf_files)} total PDF files")

    # Check if Redis is available
    try:
        redis_client.client.ping()
        logger.info("Redis connection successful")
        redis_available = True
    except Exception as e:
        logger.warning(f"Redis connection failed: {str(e)}")
        logger.warning("Continuing without Redis caching...")
        redis_available = False

    if not redis_available:
        logger.error("Redis is required for accurate processed files tracking")
        return

    # Find unprocessed files by comparing MD5s with Redis keys
    new_files, file_md5_mapping = find_unprocessed_files(all_pdf_files)

    if not new_files:
        logger.info("No new or unprocessed PDF files found")
        # Display current Redis statistics
        redis_processed_md5s = get_redis_processed_files()
        logger.info(f"Total files processed and stored in Redis: {len(redis_processed_md5s)}")
        return

    logger.info(f"Found {len(new_files)} new or unprocessed PDF files to process")

    # Check if OCR URL is configured
    if not config.ocr_url:
        logger.warning("OCR_URL is not configured in environment variables")
        logger.warning("Set OCR_URL=http://your-ocr-service:port to enable OCR processing")
        return

    logger.info(f"Using OCR service at: {config.ocr_url}")
    logger.info(f"Processing with max concurrency: {max_concurrency}")

    # Create semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(max_concurrency)

    # Process files concurrently
    tasks = []
    for pdf_file in new_files:
        task = process_pdf_file(pdf_file, semaphore)
        tasks.append(task)

    # Execute all tasks concurrently and collect results
    logger.info(f"Starting concurrent processing of {len(tasks)} files...")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    successful = 0
    failed = 0
    skipped = 0

    for i, result in enumerate(results):
        file_path = new_files[i]
        file_md5 = file_md5_mapping.get(file_path)

        if isinstance(result, Exception):
            failed += 1
            logger.error(f"✗ Exception processing {file_path}: {str(result)}")
        elif result["status"] == "success":
            successful += 1
            logger.info(f"✓ Processed {result['file']} (MD5: {file_md5})")
        elif result["status"] == "skipped":
            skipped += 1
            logger.info(f"↷ Skipped {result['file']}: {result.get('reason', 'Unknown reason')}")
        else:
            failed += 1
            logger.error(f"✗ Failed to process {result['file']}: {result.get('error', 'Unknown error')}")

    logger.info(f"Processing complete. Successful: {successful}, Failed: {failed}, Skipped: {skipped}")

    # Display updated Redis statistics
    if redis_available:
        try:
            keys = redis_client.client.keys("ocr_results:*")
            logger.info(f"Total Redis keys with 'ocr_results:' prefix: {len(keys)}")

            # Show sample keys
            sample_keys = []
            for key in keys[:10]:
                if isinstance(key, bytes):
                    sample_keys.append(key.decode('utf-8'))
                else:
                    sample_keys.append(key)

            for key in sample_keys:
                logger.info(f"  {key}")
            if len(keys) > 10:
                logger.info(f"  ... and {len(keys) - 10} more keys")

            # Display accurate count of processed files
            redis_processed_md5s = get_redis_processed_files()
            logger.info(f"Accurate count of processed files in Redis: {len(redis_processed_md5s)}")

        except Exception as e:
            logger.error(f"Error listing Redis keys: {str(e)}")
    else:
        logger.info("Redis not available - results not cached")


async def run_continuous_monitoring(directory_path: str = "data/policy"):
    """Run continuous monitoring, checking for new files every 30 minutes."""
    logger.info(f"Starting continuous file monitoring for {directory_path} (checking every 30 minutes)")
    logger.info("Press Ctrl+C to stop monitoring")

    while True:
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{current_time}] Checking for new files...")

            await process_new_files(directory_path)

            logger.info("Waiting 1 minute before next check...")
            await asyncio.sleep(60)  # 1 minute = 60 seconds

        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
            break
        except Exception as e:
            logger.error(f"Error during monitoring cycle: {str(e)}")
            logger.info("Continuing monitoring in 5 minutes...")
            await asyncio.sleep(5 * 60)  # Wait 5 minutes before retrying



def main():
    """Main function with command line argument support."""
    if len(sys.argv) < 2:
        print("Usage: python ocr_all_pdfs.py <directory_path>")
        print("  directory_path: Path to the directory containing PDF files (required)")
        print("\nExample:")
        print("  python ocr_all_pdfs.py /path/to/pdfs")
        sys.exit(1)

    directory_path = sys.argv[1]
    logger.info(f"Using directory: {directory_path}")

    # Always run once mode
    asyncio.run(process_new_files(directory_path))


if __name__ == "__main__":
    main()