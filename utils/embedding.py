from typing import List
import re
import asyncio

import aiohttp
import numpy as np
from openai import AsyncOpenAI

from utils.config import config


def normalize_embedding(embedding):
    embedding_array = np.array(embedding)
    norm = np.linalg.norm(embedding_array)
    if norm == 0:
        return embedding
    normalized_embedding = embedding_array / norm
    return normalized_embedding.tolist()


def clean_html_tags(text: str) -> str:
    table_pattern = re.compile(r'</?(?:table|tr|td|th|thead|tbody|tfoot)\b[^>]*>', re.IGNORECASE)
    return table_pattern.sub(' ', text)


async def get_embedding(text: List[str]):
    if not text:
        return []
    cleaned_text = [clean_html_tags(t) for t in text]
    client = AsyncOpenAI(base_url=config.embedding_url, api_key=config.embedding_key)
    response = await client.embeddings.create(
        model=config.embedding_model, input=cleaned_text, encoding_format="float",
        extra_body={
            "truncate_prompt_tokens": -1
        }
    )
    return [normalize_embedding(x.embedding) for x in response.data]


async def get_sparse_embedding(text: List[str], max_retries: int = 3, timeout: int = 3):
    cleaned_text = [clean_html_tags(t) for t in text]

    for attempt in range(max_retries):
        try:
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.post(
                    f"{config.embedding_url_sparse}/sparse_embed", json={"text": cleaned_text}
                ) as response:
                    r = await response.json()
                    return r
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == max_retries - 1:
                raise
            wait_time = 2 ** attempt
            await asyncio.sleep(wait_time)
