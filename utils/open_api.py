import asyncio
import datetime
import os
import random
import re
from abc import ABC

from openai import AsyncOpenAI
import httpx


class Model(ABC):
    """
    A base class for model req.
    """

    def __init__(self, api_key: str, url: str, model: str) -> None:
        self.model = model
        self.api_key = api_key
        self.base_url = url
        self.timeout = httpx.Timeout(480)

    async def __call__(
        self,
        sys_prompt: str = "",
        user_prompt: str = "",
        history_messages: list = [],
        json_mode: bool = False,
        temperature: float = 0.0,
        top_p: float = 0.5,
        **kwargs,
    ) -> str:
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        if "base64_image" in kwargs:
            new_message = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{kwargs.pop('base64_image')}",
                            },
                        },
                        {"type": "text", "text": user_prompt},
                    ],
                }
            ]
        else:
            new_message = [{"role": "user", "content": user_prompt}]
        messages = (
            [] if len(sys_prompt) == 0 else [{"role": "system", "content": sys_prompt}]
        )
        messages += history_messages + new_message

        # Create a new client for each request to ensure proper cleanup
        async with httpx.AsyncClient(timeout=self.timeout) as http_client:
            client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                http_client=http_client
            )
            try:
                response = await client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=temperature,
                    top_p=top_p,
                    stream=True,
                    stream_options= {
                        "include_usage": True
                    },
                    max_tokens=18432,
                    **kwargs,
                )
                response_text = ""
                i = 0
                usage = {}

                async def process_stream():
                    nonlocal response_text, i, usage
                    async for chunk in response:
                        if chunk.choices and len(chunk.choices) > 0:
                            chunk_content = chunk.choices[0].delta.content
                            if chunk_content:
                                response_text += chunk_content
                        if chunk.usage:
                            usage = chunk.usage

                await asyncio.wait_for(process_stream(), timeout=480)
                self.log_result(sys_prompt, user_prompt, response_text, usage)
                return response_text
            except (httpx.TimeoutException, asyncio.TimeoutError) as e:
                self.log_result(
                    sys_prompt,
                    user_prompt,
                    f"===Timeout===: {e}",
                    usage={},
                )
                return ""
            except Exception as e:
                self.log_result(
                    sys_prompt,
                    user_prompt,
                    f"===Error===: {e}",
                    usage={},
                )
                raise e

    def log_result(
        self, sys_prompt: str, user_prompt: str, response_text: str, usage: dict
    ):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("logs/open_api.log", "a", encoding="utf-8") as log_file:
            log_file.write(f"[{current_time}] {self.model}\n")
            log_file.write(f"[{current_time}] system prompt:\n{sys_prompt}\n")
            log_file.write(f"[{current_time}] user prompt:\n{user_prompt}\n")
            log_file.write(
                f"[{current_time}] response:\n{response_text}\n{usage}\n"
            )
            log_file.write("=" * 64 + "\n")


class QwenVLlocal_7B_0(Model):
    def __init__(self, api_key: str = "EMPTY") -> None:
        super().__init__(
            api_key=api_key, url="http://192.168.1.3:9889/v1", model="Qwen-VL-7B-GPU4"
        )


class QwenVLlocal_7B_1(Model):
    def __init__(self, api_key: str = "EMPTY") -> None:
        super().__init__(
            api_key=api_key, url="http://192.168.1.3:9890/v1", model="Qwen-VL-7B-GPU5"
        )


class QwenVLlocal_7B_2(Model):
    def __init__(self, api_key: str = "EMPTY") -> None:
        super().__init__(
            api_key=api_key, url="http://192.168.1.2:9888/v1", model="Qwen-VL-local"
        )


qwen_vl_local_list = [
    QwenVLlocal_7B_0,
    QwenVLlocal_7B_1,
    QwenVLlocal_7B_2,
]


async def qwen_vl_predict(**kwargs) -> str:
    available_models = qwen_vl_local_list.copy()
    while available_models:
        try:
            await asyncio.sleep(random.random())
            model = random.choice(available_models)
            qwen_vl = model()
            return await qwen_vl(**kwargs)
        except Exception as e:
            available_models.remove(model)
            if not available_models:
                raise e
