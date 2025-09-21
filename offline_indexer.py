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

# Import required modules from the existing codebase
from json_repair import repair_json
from utils.embedding import clean_html_tags, get_embedding, get_sparse_embedding
from utils.open_api import qwen_vl_predict
from utils.lang_detect import detect_language


async def get_file_metadata(file_name: str) -> Dict[str, str]:
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
        sys_prompt=sys_prompt, user_prompt=f"file_name: {file_name}"
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
    content = [clean_html_tags(re.sub(r"(.+?)\1{20,}", lambda m: m.group(1), text)) for text in content]

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
    semaphore = asyncio.Semaphore(24)

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


async def main():
    """Main processing function."""
    if len(sys.argv) < 2:
        print("Usage: python offline_ocr_processor.py <ocr_json_file> [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else f"{Path(input_file).stem}_processed.json"

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
        output_data = {}

        for key in ocr_data:
            file_name = mapping[key.removeprefix("ocr_results:ocr_")]
            content = ocr_data[key]['text']
            metadata = await get_file_metadata(file_name)
            processed_content = await process_content(content, file_name)
            embeddings, sparse_embeddings = await generate_embeddings(processed_content)
            output_data[key] = {
                "file_name": file_name,
                "metadata": metadata,
                "processed_content": processed_content,
                "embeddings": embeddings,
                "sparse_embeddings": sparse_embeddings
            }
            print(f"Processed {file_name}")

        # Save results
        print(f"Saving results to: {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())