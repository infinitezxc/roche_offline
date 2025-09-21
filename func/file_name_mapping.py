import hashlib
import json
from pathlib import Path
from typing import Dict


def calculate_md5(file_path: str) -> str:
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def traverse_pdfs_to_json(directory: str, output_file: str = None) -> Dict[str, str]:
    """
    Traverse all PDFs in the given directory and create a mapping of MD5 hash to filename.

    Args:
        directory: Directory path to traverse
        output_file: Optional output JSON file path

    Returns:
        Dictionary with MD5 hash as key and filename as value
    """
    pdf_mapping = {}
    directory_path = Path(directory)

    if not directory_path.exists():
        raise ValueError(f"Directory {directory} does not exist")

    # Find all PDF files recursively
    for pdf_file in directory_path.rglob("*.pdf"):
        if pdf_file.is_file():
            try:
                md5_hash = calculate_md5(str(pdf_file))
                pdf_mapping[md5_hash] = pdf_file.name
            except Exception as e:
                print(f"Error processing {pdf_file}: {e}")

    # Also check for .PDF extension (case insensitive)
    for pdf_file in directory_path.rglob("*.PDF"):
        if pdf_file.is_file():
            try:
                md5_hash = calculate_md5(str(pdf_file))
                pdf_mapping[md5_hash] = pdf_file.name
            except Exception as e:
                print(f"Error processing {pdf_file}: {e}")

    # Output to JSON file if specified
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(pdf_mapping, f, indent=2, ensure_ascii=False)
        print(f"PDF mapping saved to {output_file}")

    return pdf_mapping


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python file_name_mapping.py <directory> [output_file.json]")
        sys.exit(1)

    directory = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "pdf_mapping.json"

    try:
        result = traverse_pdfs_to_json(directory, output_file)
        print(f"Found {len(result)} PDF files")
        print("Sample mapping:")
        for i, (md5_hash, filename) in enumerate(result.items()):
            if i >= 5:  # Show only first 5 entries
                break
            print(f"  {md5_hash}: {filename}")
        if len(result) > 5:
            print(f"  ... and {len(result) - 5} more")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)