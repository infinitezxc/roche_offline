import base64
import hashlib
import json
import logging
import sys
from pathlib import Path

import aiohttp

from utils.config import config
from utils.redis_client import redis_client


logger = logging.getLogger(__name__)


async def get_ocr(
    input_file: str = None,
    file_category: str = "text",
    file_suffix: str = None,
    prompt: str = "",
    max_pages: int = None,
    override: bool = False,
) -> dict:
    pdf_md5 = None
    file_content = None
    
    if file_suffix == "pdf":
        with open(input_file, "rb") as f:
            file_data = f.read()
            pdf_md5 = hashlib.md5(file_data).hexdigest()
            file_content = base64.b64encode(file_data).decode("utf-8")
        if not override and pdf_md5:
            cached_result = redis_client.get(f"ocr_results:ocr_{pdf_md5}")
            if cached_result:
                logger.info(f"cache hit for {pdf_md5}")
                return json.loads(cached_result)

    timeout = aiohttp.ClientTimeout(total=3600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(
            f"{config.ocr_url}/ocr",
            json={
                "file_content": file_content,
                "file_category": file_category,
                "file_suffix": file_suffix,
                "prompt": prompt,
                "max_pages": max_pages,
            },
        ) as response:
            r = await response.json()

    if "error" in r:
        raise Exception(r["error"])
    elif pdf_md5:
        # Set key without expiration using direct Redis client
        redis_client.client.set(
            f"ocr_results:ocr_{pdf_md5}", json.dumps(r, ensure_ascii=False)
        )
    return r
