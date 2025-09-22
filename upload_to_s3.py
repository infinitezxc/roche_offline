#!/usr/bin/env python3
"""
Script to upload files from mapping_filtered.json to S3 with MD5 names.
File structure: data/{kb}/{folder}/{file_name}
S3 key: {md5_hash}
"""

import json
import os
from pathlib import Path

from tqdm import tqdm
from utils.s3_client import fs
from utils.config import config


def upload_file_to_s3(local_path, s3_key, bucket_name):
    """Upload a file to S3."""
    s3_path = f"{bucket_name}/{s3_key}"

    try:
        with open(local_path, 'rb') as local_file:
            with fs.open(s3_path, 'wb') as s3_file:
                s3_file.write(local_file.read())
        return True
    except Exception as e:
        print(f"Error uploading {local_path} to {s3_path}: {e}")
        return False


def main():
    """Main function to process mapping and upload files."""
    # Load mapping data
    mapping_file = "data/mapping_filtered.json"
    if not os.path.exists(mapping_file):
        print(f"Error: {mapping_file} not found")
        return

    with open(mapping_file, 'r', encoding='utf-8') as f:
        mapping_data = json.load(f)

    bucket_name = config.s3_bucket_name
    print(f"Using S3 bucket: {bucket_name}")

    total_files = len(mapping_data)
    uploaded_count = 0
    skipped_count = 0

    for md5_hash, file_info in tqdm(mapping_data.items(), desc="Uploading files"):
        # Construct local file path
        local_path = Path("data") / file_info["kb"] / file_info["folder"] / file_info["file_name"]

        if not local_path.exists():
            print(f"File not found: {local_path}")
            skipped_count += 1
            continue

        # Upload to S3 with MD5 as key
        print(f"Uploading {local_path} -> {md5_hash}")
        if upload_file_to_s3(local_path, md5_hash, bucket_name):
            uploaded_count += 1
            print(f"✓ Uploaded {file_info['file_name']}")
        else:
            skipped_count += 1

    print(f"\nUpload complete:")
    print(f"Total files: {total_files}")
    print(f"Uploaded: {uploaded_count}")
    print(f"Skipped: {skipped_count}")


if __name__ == "__main__":
    main()