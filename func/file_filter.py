#!/usr/bin/env python3
"""
Filter keys from data/mapping.json that don't exist in JSON files in data/index.
Reads all index files and extracts all keys, then keeps only the keys that exist in the index files.
"""

import json
import os
from pathlib import Path
from typing import Set, Dict, Any


def extract_hash_from_key(key: str) -> str:
    """
    Extract hash from index file keys.
    Index keys have format: "ocr_results:ocr_<hash>"
    Returns the hash part.
    """
    if key.startswith("ocr_results:ocr_"):
        return key[len("ocr_results:ocr_"):]
    return key


def collect_all_keys_from_index(index_dir: str) -> Set[str]:
    """
    Collect all keys (hashes) from all JSON files in the index directory.
    """
    index_path = Path(index_dir)
    all_keys = set()

    if not index_path.exists():
        print(f"Index directory {index_dir} does not exist")
        return all_keys

    json_files = list(index_path.glob("*.json"))
    print(f"Found {len(json_files)} JSON files in {index_dir}")

    for json_file in json_files:
        print(f"Processing {json_file.name}...")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract keys and convert them to hashes
            for key in data.keys():
                hash_key = extract_hash_from_key(key)
                all_keys.add(hash_key)

        except Exception as e:
            print(f"Error processing {json_file}: {e}")
            continue

    print(f"Total unique keys found in index files: {len(all_keys)}")
    return all_keys


def filter_mapping_file(mapping_file: str, index_keys: Set[str], output_file: str = None) -> Dict[str, Any]:
    """
    Filter mapping.json to keep only keys that exist in the index files.
    """
    if not os.path.exists(mapping_file):
        print(f"Mapping file {mapping_file} does not exist")
        return {}

    print(f"Loading mapping file: {mapping_file}")
    with open(mapping_file, 'r', encoding='utf-8') as f:
        mapping_data = json.load(f)

    original_count = len(mapping_data)
    print(f"Original mapping file has {original_count} keys")

    # Filter keys that exist in index files
    filtered_data = {}
    for key, value in mapping_data.items():
        if key in index_keys:
            filtered_data[key] = value

    filtered_count = len(filtered_data)
    removed_count = original_count - filtered_count

    print(f"Filtered mapping file has {filtered_count} keys")
    print(f"Removed {removed_count} keys that don't exist in index files")

    # Save filtered data if output file is specified
    if output_file:
        print(f"Saving filtered data to: {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
        print(f"Filtered mapping saved to {output_file}")

    return filtered_data


def main():
    """
    Main function to filter mapping.json based on keys in index files.
    """
    # Define paths
    base_dir = Path(__file__).parent.parent
    mapping_file = base_dir / "data" / "mapping.json"
    index_dir = base_dir / "data" / "index"
    output_file = base_dir / "data" / "mapping_filtered.json"

    print("Starting file filtering process...")
    print(f"Base directory: {base_dir}")
    print(f"Mapping file: {mapping_file}")
    print(f"Index directory: {index_dir}")
    print(f"Output file: {output_file}")

    # Collect all keys from index files
    index_keys = collect_all_keys_from_index(str(index_dir))

    if not index_keys:
        print("No keys found in index files. Exiting.")
        return

    # Filter mapping file
    filtered_data = filter_mapping_file(str(mapping_file), index_keys, str(output_file))

    if filtered_data:
        print("\nFiltering completed successfully!")
        print(f"Original keys: {len(index_keys) if index_keys else 0}")
        print(f"Filtered keys: {len(filtered_data)}")
    else:
        print("No data was filtered or an error occurred.")


if __name__ == "__main__":
    main()