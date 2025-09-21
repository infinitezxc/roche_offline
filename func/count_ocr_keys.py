#!/usr/bin/env python3

import os
import sys
import json
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.redis_client import redis_client

def count_ocr_pages():
    """Count total pages across all Redis keys starting with 'ocr_results'"""
    try:
        # Get all keys from Redis
        all_keys = redis_client.list()

        if all_keys is None:
            print("Error: Could not retrieve keys from Redis")
            return -1

        # Filter keys that start with 'ocr_results'
        ocr_keys = [key for key in all_keys if key.startswith('ocr_results')]

        if not ocr_keys:
            print("No OCR keys found")
            return 0

        total_pages = 0
        processed_keys = 0
        deleted_keys = 0

        for key in ocr_keys:
            try:
                # Get the value from Redis
                value = redis_client.get(key)
                if value is None:
                    print(f"Warning: Could not retrieve value for key: {key}")
                    continue

                # Parse JSON content
                data = json.loads(value)

                # Count pages using len(r["content"])
                if "text" in data and isinstance(data["text"], list):
                    page_count = len(data["text"])
                    total_pages += page_count
                    processed_keys += 1
                else:
                    print(f"Warning: Key {key} does not have valid 'text' array - deleting key")
                    print(data)
                    try:
                        redis_client.delete(key)
                        deleted_keys += 1
                        print(f"Deleted key: {key}")
                    except Exception as delete_error:
                        print(f"Error deleting key {key}: {delete_error}")

            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse JSON for key {key}: {e} - deleting key")
                try:
                    redis_client.delete(key)
                    deleted_keys += 1
                    print(f"Deleted key with invalid JSON: {key}")
                except Exception as delete_error:
                    print(f"Error deleting key {key}: {delete_error}")
            except Exception as e:
                print(f"Error processing key {key}: {e}")

        print(f"Total pages across {processed_keys} OCR keys: {total_pages}")
        if deleted_keys > 0:
            print(f"Deleted {deleted_keys} invalid keys")
        return total_pages

    except Exception as e:
        print(f"Error: {e}")
        return -1

def count_ocr_keys():
    """Count Redis keys that start with 'ocr_results'"""
    try:
        # Get all keys from Redis
        all_keys = redis_client.list()

        if all_keys is None:
            print("Error: Could not retrieve keys from Redis")
            return -1

        # Filter keys that start with 'ocr_results'
        ocr_keys = [key for key in all_keys if key.startswith('ocr_results')]

        print(f"Number of Redis keys starting with 'ocr_results': {len(ocr_keys)}")

        return len(ocr_keys)

    except Exception as e:
        print(f"Error: {e}")
        return -1

def clear_empty_text_keys():
    """Delete all Redis keys where the text list contains only empty strings"""
    try:
        # Get all OCR result keys
        all_keys = redis_client.list()

        if all_keys is None:
            print("Error: Could not retrieve keys from Redis")
            return -1

        # Filter keys that start with 'ocr_results'
        ocr_keys = [key for key in all_keys if key.startswith('ocr_results')]

        if not ocr_keys:
            print("No OCR keys found")
            return 0

        print(f"Found {len(ocr_keys)} total OCR result keys")

        empty_text_keys = []
        deleted_keys = 0

        for key in ocr_keys:
            try:
                value = redis_client.get(key)
                if value:
                    data = json.loads(value)
                    if 'text' in data and isinstance(data['text'], list):
                        # Check if all text entries are empty strings
                        if all(text == "" for text in data['text']):
                            empty_text_keys.append(key)
                            print(f"Found empty text key: {key}")
            except Exception as e:
                print(f"Error processing key {key}: {e}")

        print(f"\nFound {len(empty_text_keys)} keys with all empty text")

        if empty_text_keys:
            confirm = input(f"Are you sure you want to delete {len(empty_text_keys)} keys? (y/N): ")
            if confirm.lower() == 'y':
                for key in empty_text_keys:
                    try:
                        if redis_client.delete(key):
                            deleted_keys += 1
                            print(f"Deleted key: {key}")
                        else:
                            print(f"Failed to delete key: {key}")
                    except Exception as e:
                        print(f"Error deleting key {key}: {e}")

                print(f"\nSuccessfully deleted {deleted_keys} keys with empty text")
            else:
                print("Operation cancelled")
        else:
            print("No keys with empty text found")

        return deleted_keys

    except Exception as e:
        print(f"Error: {e}")
        return -1

def backup_ocr_keys(output_file=None):
    """Backup all Redis keys starting with 'ocr_results' to a JSON file"""
    try:
        # Get all OCR result keys
        all_keys = redis_client.list()

        if all_keys is None:
            print("Error: Could not retrieve keys from Redis")
            return -1

        # Filter keys that start with 'ocr_results'
        ocr_keys = [key for key in all_keys if key.startswith('ocr_results')]

        if not ocr_keys:
            print("No OCR keys found")
            return 0

        print(f"Found {len(ocr_keys)} OCR result keys")

        # Generate output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"ocr_backup_{timestamp}.json"

        backup_data = {}
        processed_keys = 0
        failed_keys = 0

        print("Starting backup...")

        for i, key in enumerate(ocr_keys, 1):
            try:
                value = redis_client.get(key)
                if value:
                    # Parse JSON to validate it
                    data = json.loads(value)
                    backup_data[key] = data
                    processed_keys += 1

                    # Progress indicator
                    if i % 100 == 0:
                        print(f"Processed {i}/{len(ocr_keys)} keys...")
                else:
                    print(f"Warning: Could not retrieve value for key: {key}")
                    failed_keys += 1
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON for key {key}: {e}")
                failed_keys += 1
            except Exception as e:
                print(f"Error processing key {key}: {e}")
                failed_keys += 1

        # Write backup to file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)

            print(f"\nBackup completed successfully!")
            print(f"File: {output_file}")
            print(f"Keys backed up: {processed_keys}")
            if failed_keys > 0:
                print(f"Failed keys: {failed_keys}")

            # Show file size
            file_size = os.path.getsize(output_file)
            if file_size > 1024 * 1024:
                print(f"File size: {file_size / (1024 * 1024):.2f} MB")
            else:
                print(f"File size: {file_size / 1024:.2f} KB")

            return processed_keys

        except Exception as e:
            print(f"Error writing backup file: {e}")
            return -1

    except Exception as e:
        print(f"Error: {e}")
        return -1

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "pages":
            count_ocr_pages()
        elif sys.argv[1] == "clear":
            clear_empty_text_keys()
        elif sys.argv[1] == "backup":
            # Check if a custom filename is provided
            output_file = sys.argv[2] if len(sys.argv) > 2 else None
            backup_ocr_keys(output_file)
        else:
            print("Usage: python count_ocr_keys.py [pages|clear|backup] [filename]")
            print("  (no args): Count OCR keys")
            print("  pages: Count total pages across all OCR keys")
            print("  clear: Delete all keys with empty text arrays")
            print("  backup [filename]: Backup all OCR keys to JSON file")
            print("    If no filename provided, uses timestamp: ocr_backup_YYYYMMDD_HHMMSS.json")
    else:
        count_ocr_keys()