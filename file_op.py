"""
File operations script for inserting files from mapping data into PostgreSQL.
Handles user_id and kb_id mappings and creates folder/file records.
"""

import json
import logging
import argparse
from typing import Dict, Any

from models.folder_file_creator import delete_all_files_and_folders_in_kb, create_folders_batch, create_files_batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Single user ID for all operations
USER_ID = "usr_01K2MCN3YEMTR5EKHTGN08ENHT"

# KB name to ID mappings
KB_MAPPINGS = {
    "sop": "kb_01K5RZ9WJV0607KBSQ545B9VDJ",
    "policy": "kb_01K5RZA4KS04Y7VRGVSYN5EHDE",
}

# Batch processing configuration
FOLDER_BATCH_SIZE = 100  # Process folders in batches of 100
FILE_BATCH_SIZE = 2000    # Process files in batches of 500

def chunk_list(lst, chunk_size):
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def load_mapping_data(file_path: str = "data/mapping_filtered.json") -> Dict[str, Any]:
    """Load the mapping data from JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Mapping file not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file: {e}")
        raise


def insert_files_to_postgresql():
    """Main function to insert all files from mapping data into PostgreSQL."""
    logger.info("Starting optimized file insertion process...")

    # Load mapping data
    mapping_data = load_mapping_data()
    logger.info(f"Loaded {len(mapping_data)} files from mapping data")

    # Pre-process data to organize by KB and collect unique folders
    kb_data = {}
    for file_hash, file_info in mapping_data.items():
        kb_type = file_info["kb"]
        if kb_type not in KB_MAPPINGS:
            logger.warning(f"Unknown KB type: {kb_type} for file: {file_info['file_name']}")
            continue

        kb_id = KB_MAPPINGS[kb_type]
        folder_name = file_info["folder"]

        if kb_id not in kb_data:
            kb_data[kb_id] = {
                "folders": set(),
                "files": []
            }

        kb_data[kb_id]["folders"].add(folder_name)
        kb_data[kb_id]["files"].append({
            "file_hash": file_hash,
            "file_info": file_info,
            "folder_name": folder_name
        })

    # Process each KB
    total_success = 0
    total_errors = 0

    for kb_id, data in kb_data.items():
        try:
            logger.info(f"Processing KB {kb_id}: {len(data['folders'])} folders, {len(data['files'])} files")

            # Step 1: Create all folders for this KB in batches
            folder_names = list(data["folders"])
            folder_mapping = {}

            for folder_batch in chunk_list(folder_names, FOLDER_BATCH_SIZE):
                batch_mapping = create_folders_batch(USER_ID, kb_id, folder_batch)
                folder_mapping.update(batch_mapping)
                logger.info(f"Created {len(batch_mapping)} folders in batch for KB {kb_id}")

            logger.info(f"Created total {len(folder_mapping)} folders for KB {kb_id}")

            # Step 2: Prepare file data for batch insertion
            file_batch_data = []
            file_errors = 0

            for file_entry in data["files"]:
                try:
                    file_info = file_entry["file_info"]
                    folder_name = file_entry["folder_name"]

                    if folder_name not in folder_mapping:
                        logger.error(f"Folder {folder_name} not found in mapping")
                        file_errors += 1
                        continue

                    file_batch_data.append({
                        "folder_id": folder_mapping[folder_name],
                        "file_name": file_info["file_name"],
                        "file_hash": file_entry["file_hash"],
                        "file_size": file_info["file_size"],
                        "status": "completed"
                    })
                except Exception as e:
                    logger.error(f"Error preparing file data: {e}")
                    file_errors += 1

            # Step 3: Create all files for this KB in batches
            files_created = 0
            for file_batch in chunk_list(file_batch_data, FILE_BATCH_SIZE):
                batch_file_ids = create_files_batch(USER_ID, kb_id, file_batch)
                files_created += len(batch_file_ids)
                logger.info(f"Created {len(batch_file_ids)} files in batch for KB {kb_id} (total: {files_created}/{len(file_batch_data)})")

            total_success += files_created
            total_errors += file_errors

            logger.info(f"Completed KB {kb_id}: {files_created} files created, {file_errors} errors")

        except Exception as e:
            logger.error(f"Error processing KB {kb_id}: {e}")
            total_errors += len(data["files"])

    logger.info(f"Optimized file insertion completed. Success: {total_success}, Errors: {total_errors}")

    # Log folder creation summary
    total_folders = sum(len(data["folders"]) for data in kb_data.values())
    logger.info(f"Created {total_folders} unique folders across all KBs")


def clear_all_kbs():
    """Clear all files and folders from both knowledge bases."""
    logger.info("Starting clear operation for all knowledge bases...")

    total_files_deleted = 0
    total_folders_deleted = 0

    for kb_name, kb_id in KB_MAPPINGS.items():
        try:
            logger.info(f"Clearing KB: {kb_name} (ID: {kb_id})")
            result = delete_all_files_and_folders_in_kb(kb_id)

            files_deleted = result["files_deleted"]
            folders_deleted = result["folders_deleted"]

            total_files_deleted += files_deleted
            total_folders_deleted += folders_deleted

            logger.info(f"KB {kb_name}: Deleted {files_deleted} files and {folders_deleted} folders")

        except Exception as e:
            logger.error(f"Error clearing KB {kb_name}: {e}")
            continue

    logger.info(f"Clear operation completed. Total deleted: {total_files_deleted} files, {total_folders_deleted} folders")


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(description="File operations for knowledge base management")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear all files and folders from both knowledge bases"
    )

    args = parser.parse_args()

    if args.clear:
        clear_all_kbs()
    else:
        insert_files_to_postgresql()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise