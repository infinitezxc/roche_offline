#!/usr/bin/env python3
"""
Offline OCR Processing Script

This script processes OCR JSON data to generate context and embeddings using the same logic
as in index/utils/index_utils.py. It takes an OCR JSON file as input and outputs processed
results with context summaries and embeddings.

Usage:
    python offline_ocr_processor.py <ocr_json_file> [output_file]

Dependencies:
    - json_repair
    - numpy
    - portkey_ai
    - aiohttp
    - tenacity
    - dotenv
"""

import asyncio
import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Any
from tqdm.asyncio import tqdm
import traceback
import multiprocessing as mp
from functools import partial

# Import required modules from the existing codebase
from json_repair import repair_json
from utils.embedding import clean_html_tags, get_embedding, get_sparse_embedding
from utils.open_api import qwen_vl_predict
from utils.lang_detect import detect_language


async def get_file_metadata(content: List[str], file_name: str) -> Dict[str, str]:
    """
    Generate file metadata using Qwen model.

    Args:
        file_name: Name of the file to analyze

    Returns:
        Dictionary containing entity and time metadata
    """
    sys_prompt = f"""Generate the file metadata in json format.
    {{
        "entity": str, province name in file name in Chinese, return "" if not mentioned
        "time": in YYYY format, include every year in the range split by comma if it is a time range, return "" if not mentioned
    }}
    """
    file_summary = await qwen_vl_predict(
        sys_prompt=sys_prompt, user_prompt=f"file_name: {file_name}\nfirst page:\n{content[0]}"
    )
    return eval(repair_json(file_summary))


async def process_content(content: List[str], file_name: str) -> List[str]:
    """
    Process OCR content to generate context summaries and embeddings.

    Args:
        content: List of OCR text content from each page
        file_name: Name of the source file

    Returns:
        List of processed content with context summaries
    """
    # Clean HTML tags and remove repetitive text patterns
    content = [clean_html_tags(re.sub(r"(.+?)\1{20,}", lambda m: m.group(1), text)) if text and text.strip() else "" for text in content]

    # Detect language based on first 5 pages
    lang = detect_language("\n".join(content[:5]))

    # Generate file summary using Qwen model
    file_summary = await qwen_vl_predict(
        user_prompt=f"summary the file {file_name} in one sentence in {lang}:\nFirst two pages:\n{'\n'.join(content[:2])}"
    )

    # Prepare prompts for context generation
    prompts = []
    for i, text in enumerate(content[1:]):
        sys_prompt = f"""Generate a concise contextual summary for the current page to enhance search retrieval. The summary should:
1. Provide essential background information, key concepts and fitting conditions
2. Highlight relationships with previous content
3. Make the current page self-contained and understandable
4. use {lang}

Requirements:
- Length: Maximum 100 words
- Style: Clear, factual, and objective
- Focus: Emphasize unique identifiers, technical terms, and critical details
"""
        user_prompt = f"""
File Summary: {file_summary}

============Previous Content============
{"\n".join(content[max(0, i - 2) : i + 1])}

============Current Page============
{text}

Please provide the context below:
"""
        prompts.append((sys_prompt, user_prompt))

    # Process prompts with concurrency control
    semaphore = asyncio.Semaphore(36)

    async def process_with_semaphore(sys_prompt, user_prompt):
        async with semaphore:
            return await qwen_vl_predict(sys_prompt=sys_prompt, user_prompt=user_prompt)

    # Generate context summaries for all pages
    context_list = await asyncio.gather(
        *[
            process_with_semaphore(sys_prompt, user_prompt)
            for sys_prompt, user_prompt in prompts
        ]
    )

    # Combine file summary, context, and content
    results = [f"file_summary: {file_summary}\n{content[0]}"]
    for i, context in enumerate(context_list):
        results.append(
            f"file_summary: {file_summary}\ncontext: {context}\npage_content:\n{content[i + 1]}"
        )

    return results


async def generate_embeddings(processed_content: List[str]) -> List[List[float]]:
    """
    Generate embeddings for processed content.

    Args:
        processed_content: List of processed content strings

    Returns:
        List of normalized embedding vectors
    """
    embeddings = await get_embedding(processed_content)
    sparse_embeddings = await get_sparse_embedding(processed_content)
    return embeddings, sparse_embeddings


def load_ocr_json(file_path: str) -> Dict[str, Any]:
    """
    Load OCR JSON data from file.

    Args:
        file_path: Path to the OCR JSON file

    Returns:
        Dictionary containing OCR data
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def process_single_file_sync(args):
    """
    Synchronous wrapper for processing a single file in a separate process.

    Args:
        args: Tuple of (key, file_name, content, timeout)

    Returns:
        Tuple of (key, result) or (key, None) if failed
    """
    key, file_name, content, timeout = args
    loop = None

    # Try processing with one retry on timeout
    for attempt in range(2):  # 0 = first attempt, 1 = retry
        try:
            # Check if there's already an event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = None
            except RuntimeError:
                loop = None

            # Create a new event loop if needed
            if loop is None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Run the async processing
            result = loop.run_until_complete(
                asyncio.wait_for(
                    process_single_file_async(key, file_name, content),
                    timeout=timeout
                )
            )

            return key, result

        except asyncio.TimeoutError:
            if attempt < 1:
                continue
            else:
                print(f"Process timeout for {file_name} ({len(content)} pages) after {timeout}s on retry, giving up")
                return key, None
        except Exception as e:
            print(f"Process error for {file_name} ({len(content)} pages): {str(e)}")
            import traceback
            traceback.print_exc()
            return key, None


async def process_single_file_async(key: str, file_name: str, content: List[str]):
    """
    Async processing logic for a single file (no semaphore needed in separate process).
    """
    try:
        # Get file metadata
        metadata = await get_file_metadata(content, file_name)

        # Process content
        processed_content = await process_content(content, file_name)

        # Generate embeddings
        embeddings, sparse_embeddings = await generate_embeddings(processed_content)

        result = {
            "file_name": file_name,
            "metadata": metadata,
            "processed_content": processed_content,
            "embeddings": embeddings,
            "sparse_embeddings": sparse_embeddings
        }

        return result
    except Exception as e:
        print(f"Error in async processing for {file_name}: {str(e)}")
        raise


def main():
    """Main processing function using multiprocessing."""
    if len(sys.argv) < 2:
        print("Usage: python offline_ocr_processor.py <ocr_json_file> [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]

    # Create data/index directory if it doesn't exist
    output_dir = Path("data/index")
    output_dir.mkdir(parents=True, exist_ok=True)

    default_output = output_dir / f"{Path(input_file).stem}_processed.json"
    output_file = sys.argv[2] if len(sys.argv) > 2 else str(default_output)

    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found")
        sys.exit(1)

    print(f"Loading OCR data from: {input_file}")

    mapping_file = Path(input_file).parent / "mapping.json"
    if not mapping_file.exists():
        print(f"Error: Mapping file '{mapping_file}' not found")
        sys.exit(1)
    with open(mapping_file, 'r', encoding='utf-8') as f:
        mapping = json.load(f)

    try:
        # Load OCR JSON data
        ocr_data = load_ocr_json(input_file)

        # Prepare tasks for multiprocessing
        timeout_per_file = 300  # 5 minutes per file
        max_workers = min(mp.cpu_count(), 32)  # Limit to avoid overwhelming the system

        tasks = []
        all_keys = sorted(list(ocr_data.keys()))
        for key in all_keys:
            file_name = mapping[key.removeprefix("ocr_results:ocr_")]["file_name"]
            content = ocr_data[key]['text']
            tasks.append((key, file_name, content, timeout_per_file))

        print(f"Processing {len(tasks)} files with {max_workers} workers...")

        # Process items in batches with multiprocessing
        batch_size = 2000
        part_number = 1
        all_results = {}

        total_batches = (len(tasks) + batch_size - 1) // batch_size

        for i in range(0, len(tasks), batch_size):
            # Check if part file already exists
            part_file = output_dir / f"{Path(output_file).stem}_part_{part_number:03d}.json"

            if part_file.exists():
                print(f"Part file {part_file} already exists, skipping batch {part_number}/{total_batches}")
                # Load existing data to include in final result
                with open(part_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    all_results.update(existing_data)
                part_number += 1
                continue

            batch_tasks = tasks[i:i + batch_size]
            print(f"Processing batch {part_number}/{total_batches} with {len(batch_tasks)} files...")

            # Process batch using multiprocessing
            batch_data = {}

            # Use multiprocessing Pool to process files in parallel
            with mp.Pool(processes=max_workers) as pool:
                # Use tqdm for progress tracking
                with tqdm(total=len(batch_tasks), desc=f"Batch {part_number}/{total_batches}") as pbar:
                    results = []
                    for result in pool.imap_unordered(process_single_file_sync, batch_tasks):
                        results.append(result)
                        pbar.update(1)

                    for key, file_result in results:
                        if file_result is not None:
                            batch_data[key] = file_result
                            all_results[key] = file_result
                        else:
                            print(f"Skipping failed file: {key}")

            print(f"Batch {part_number} completed: {len(batch_data)} files processed successfully")

            # Save part file
            print(f"Saving batch {part_number} to: {part_file}")
            with open(part_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False, indent=2)

            part_number += 1

        print(f"Processing completed. Part files saved in: {output_dir}")
        print(f"Total files processed: {len(all_results)}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    # Set multiprocessing start method to avoid issues with event loops
    try:
        mp.set_start_method('spawn')
    except RuntimeError:
        # Start method can only be set once
        pass

    main()