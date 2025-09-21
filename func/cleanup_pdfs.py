#!/usr/bin/env python3
import os
import hashlib
import PyPDF2
from pathlib import Path

def get_file_md5(file_path):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Error calculating MD5 for {file_path}: {e}")
        return None

def cleanup_duplicate_files_keep_level1(directory_path):
    """Remove duplicate files based on MD5, keeping only files in '国家医疗保障局' directory."""
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    # Find all files recursively
    all_files = list(Path(directory_path).rglob("*"))
    # Filter only regular files (not directories)
    all_files = [f for f in all_files if f.is_file()]

    if not all_files:
        print(f"No files found in {directory_path}")
        return

    print(f"Found {len(all_files)} files to check for duplicates...")

    # Group files by MD5 hash
    md5_to_files = {}
    error_count = 0

    for file_path in all_files:
        md5_hash = get_file_md5(file_path)
        if md5_hash is None:
            error_count += 1
            continue

        if md5_hash not in md5_to_files:
            md5_to_files[md5_hash] = []
        md5_to_files[md5_hash].append(file_path)

    # Process duplicates - keep files in 'level1 国家医疗保障局' directory
    removed_count = 0
    duplicate_groups = {md5: files for md5, files in md5_to_files.items() if len(files) > 1}

    print(f"Found {len(duplicate_groups)} groups of duplicate files")

    for md5_hash, duplicate_files in duplicate_groups.items():
        print(f"\nDuplicate group (MD5: {md5_hash[:8]}...):")

        # Find files in '国家医疗保障局' directory
        level1_files = [f for f in duplicate_files if f.parent.name == "国家医疗保障局"]

        if level1_files:
            # Keep the first file from level1 directory
            file_to_keep = level1_files[0]
            files_to_remove = [f for f in duplicate_files if f != file_to_keep]

            print(f"  Keeping: {file_to_keep}")

            for file_to_remove in files_to_remove:
                print(f"  Removing: {file_to_remove}")
                try:
                    os.remove(file_to_remove)
                    removed_count += 1
                except Exception as e:
                    print(f"  Error removing {file_to_remove}: {e}")
                    error_count += 1
        else:
            # No files in level1 directory, keep the first file found
            file_to_keep = duplicate_files[0]
            files_to_remove = duplicate_files[1:]

            print(f"  No 国家医疗保障局 files found, keeping: {file_to_keep}")

            for file_to_remove in files_to_remove:
                print(f"  Removing: {file_to_remove}")
                try:
                    os.remove(file_to_remove)
                    removed_count += 1
                except Exception as e:
                    print(f"  Error removing {file_to_remove}: {e}")
                    error_count += 1

    print(f"\nDuplicate cleanup summary:")
    print(f"- Removed: {removed_count} duplicate files")
    print(f"- Errors: {error_count} files")
    print(f"- Duplicate groups processed: {len(duplicate_groups)}")

def count_pdf_pages(pdf_path):
    """Count the number of pages in a PDF file."""
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            return len(pdf_reader.pages)
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return -1

def cleanup_zero_page_pdfs(directory_path):
    """Remove PDF files with 0 pages from the specified directory."""
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    pdf_files = list(Path(directory_path).rglob("*.pdf"))

    if not pdf_files:
        print(f"No PDF files found in {directory_path}")
        return

    print(f"Found {len(pdf_files)} PDF files to check...")

    removed_count = 0
    error_count = 0

    for pdf_file in pdf_files:
        page_count = count_pdf_pages(pdf_file)

        if page_count == 0:
            print(f"Removing {pdf_file} (0 pages)")
            try:
                os.remove(pdf_file)
                removed_count += 1
            except Exception as e:
                print(f"Error removing {pdf_file}: {e}")
                error_count += 1
        elif page_count == -1:
            error_count += 1
        else:
            print(f"Keeping {pdf_file} ({page_count} pages)")

    print(f"\nSummary:")
    print(f"- Removed: {removed_count} files")
    print(f"- Errors: {error_count} files")
    print(f"- Total checked: {len(pdf_files)} files")

if __name__ == "__main__":
    ori_directory = "ori"

    # Cleanup zero page PDFs
    print("=== Cleaning up zero-page PDFs ===")
    # cleanup_zero_page_pdfs(ori_directory)

    # Cleanup duplicate files (keeping files in 国家医疗保障局 folder)
    print("\n=== Cleaning up duplicate files ===")
    cleanup_duplicate_files_keep_level1(ori_directory)