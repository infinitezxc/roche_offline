#!/usr/bin/env python3
import json
import sys
import os

def find_new_keys(file1_path, file2_path, output_path):
    """
    Compare two JSON files and find new key-value pairs in file2 that don't exist in file1.

    Args:
        file1_path: Path to the reference JSON file (0921_processed.json)
        file2_path: Path to the new JSON file (0922.json)
        output_path: Path to output the differences (0922_diff.json)
    """
    try:
        # Load the reference file (0921_processed.json)
        with open(file1_path, 'r', encoding='utf-8') as f:
            reference_data = json.load(f)

        # Load the new file (0922.json)
        with open(file2_path, 'r', encoding='utf-8') as f:
            new_data = json.load(f)

        # Find new key-value pairs
        new_kvs = {}
        for key, value in new_data.items():
            if key not in reference_data:
                new_kvs[key] = value

        # Save the differences
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(new_kvs, f, indent=2, ensure_ascii=False)

        print(f"Found {len(new_kvs)} new key-value pairs")
        print(f"Differences saved to {output_path}")

        return len(new_kvs)

    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        return -1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}")
        return -1
    except Exception as e:
        print(f"Error: {e}")
        return -1

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python index_diff.py <reference_file> <new_file> <output_file>")
        print("Example: python index_diff.py data/0921_processed.json data/0922.json data/0922_diff.json")
        sys.exit(1)

    reference_file = sys.argv[1]
    new_file = sys.argv[2]
    output_file = sys.argv[3]

    result = find_new_keys(reference_file, new_file, output_file)
    sys.exit(0 if result >= 0 else 1)