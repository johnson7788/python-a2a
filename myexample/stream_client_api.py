#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date  : 2025/4/29 09:01
# @File  : stream_api.py
# @Author: johnson
# @Contact : github: johnson7788
# @Desc  : A2A的Stream Client的API

import asyncio
from python_a2a import StreamingClient, Message, TextContent, MessageRole
from python_a2a import Task, TaskStatus, TaskState


async def main():
    # Create a streaming client
    client = StreamingClient("http://localhost:6001")

    # Stream a simple message
    message = Message(
        content=TextContent(text="what's today weather in Tokoy"),
        role=MessageRole.USER
    )

    print("Streaming response:")
    print("-" * 50)

    # Define a callback function to process chunks
    def print_chunk(chunk):
        print(chunk, end="", flush=True)

    # Stream the response with the callback
    async for chunk in client.stream_response(message, chunk_callback=print_chunk):
        print(chunk)  # Chunks are handled by the callback

    print("\n" + "-" * 50)

    # Alternatively, create and stream a task
    task = await client.create_task("Explain quantum computing in simple terms")

    print("\nStreaming task response:")
    print("-" * 50)

    # Stream the task execution
    async for chunk in client.stream_task(task, chunk_callback=lambda c: print(c.get("text", ""), end="", flush=True)):
        print(chunk)


if __name__ == "__main__":
    asyncio.run(main())
