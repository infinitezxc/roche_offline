from typing import List
import re

import aiohttp
import numpy as np
from portkey_ai import AsyncPortkey

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
    client = AsyncPortkey(base_url=config.embedding_url, api_key=config.embedding_key)
    response = await client.embeddings.create(model=config.embedding_model, input=cleaned_text, encoding_format="float")
    return [normalize_embedding(x.embedding) for x in response.data]


async def get_sparse_embedding(text: List[str]):
    cleaned_text = [clean_html_tags(t) for t in text]
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{config.embedding_url_sparse}/sparse_embed", json={"text": cleaned_text}
        ) as response:
            r = await response.json()
            return r
