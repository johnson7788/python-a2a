#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date  : 2025/4/30
# @File  : fastapi_stream_api.py
# @Author: johnson
# @Contact : github: johnson7788
# @Desc  : FastAPI接口，用于以SSE流的形式返回后端Agent server的结果

import asyncio
import json
from typing import AsyncGenerator

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from python_a2a import (
    A2AClient, Message, TextContent, MessageRole
)

# Import necessary components - using A2AClient instead of abstract StreamingClient
try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    print("Warning: aiohttp not installed. Streaming visualization will be simulated.")
    print("Install aiohttp with: pip install aiohttp")


class EnhancedClient(A2AClient):
    """
    Enhanced client with streaming capabilities.

    This extends the standard A2AClient with basic streaming support
    without requiring the abstract StreamingClient.
    """

    def __init__(self, endpoint_url: str, headers: dict = None):
        super().__init__(endpoint_url=endpoint_url, headers=headers)
        self.url = self.endpoint_url  # Ensure URL is accessible for compatibility

    async def check_streaming_support(self):
        """Check if the agent supports streaming capabilities."""
        # If aiohttp is not available, we can't do true streaming
        if not HAS_AIOHTTP:
            return False

        try:
            async with aiohttp.ClientSession() as session:
                headers = dict(self.headers)
                headers["Accept"] = "application/json"  # Prefer JSON response

                async def fetch_agent_card(url: str):
                    try:
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                content_type = response.headers.get("Content-Type", "").lower()
                                if "json" in content_type:
                                    return await response.json()
                                else:
                                    try:
                                        text = await response.text()
                                        import re
                                        json_match = re.search(r'({[\s\S]*"capabilities"[\s\S]*})', text)
                                        if json_match:
                                            return json.loads(json_match.group(1))
                                    except:
                                        return {}
                    except aiohttp.ClientError as e:
                        print(f"Error fetching {url}: {e}")
                    return {}

                data = await fetch_agent_card(f"{self.url}/agent.json")
                if (isinstance(data, dict) and
                        isinstance(data.get("capabilities"), dict) and
                        data.get("capabilities", {}).get("streaming", False)):
                    return True

                data = await fetch_agent_card(f"{self.url}/a2a/agent.json")
                if (isinstance(data, dict) and
                        isinstance(data.get("capabilities"), dict) and
                        data.get("capabilities", {}).get("streaming", False)):
                    return True

            return False
        except Exception as e:
            print(f"Error checking streaming support: {e}")
            return False

    async def stream_response(self, message: Message) -> AsyncGenerator[str, None]:
        """
        Stream a response from the agent.

        Args:
            message: Message to send

        Yields:
            Response chunks as Server-Sent Events.
        """
        if not HAS_AIOHTTP:
            async for chunk in self._simulate_streaming(message):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            return

        supports_streaming = await self.check_streaming_support()
        if not supports_streaming:
            async for chunk in self._simulate_streaming(message):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            return

        try:
            headers = dict(self.headers)
            headers["Accept"] = "text/event-stream"

            async with aiohttp.ClientSession() as session:
                async with session.post(
                        f"{self.url}/stream",
                        json={"message": message.to_dict()},
                        headers=headers
                ) as response:
                    if response.status >= 400:
                        raise HTTPException(status_code=response.status, detail=f"HTTP error {response.status}")

                    async for chunk in response.content.iter_chunked(1024):
                        if not chunk:
                            continue
                        chunk_text = chunk.decode('utf-8')
                        yield chunk_text
        except aiohttp.ClientError as e:
            print(f"Streaming error: {e}")
            # Fallback to simulated streaming on error
            async for chunk in self._simulate_streaming(message):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
        except HTTPException as e:
            raise e
        except Exception as e:
            print(f"Unexpected error during streaming: {e}")
            # Fallback to simulated streaming on unexpected error
            async for chunk in self._simulate_streaming(message):
                yield f"data: {json.dumps({'text': chunk})}\n\n"

    async def _simulate_streaming(self, message: Message) -> AsyncGenerator[str, None]:
        """Simulate streaming using the full response."""
        response = await self.send_message(message)
        full_text = ""
        if response and hasattr(response, "content"):
            if hasattr(response.content, "text"):
                full_text = response.content.text
            elif hasattr(response.content, "message"):
                full_text = response.content.message
            else:
                full_text = str(response.content)

        if not full_text:
            full_text = "No response received from the agent."

        words = full_text.split()
        current_chunk = []
        for word in words:
            current_chunk.append(word)
            if len(current_chunk) >= 5:
                yield " ".join(current_chunk)
                current_chunk = []
                await asyncio.sleep(0.1)
        if current_chunk:
            yield " ".join(current_chunk)
            await asyncio.sleep(0.1)


class AgentRequest(BaseModel):
    query: str
    agent_url: str
    headers: dict = None


app = FastAPI()


async def get_enhanced_client(request_body: AgentRequest) -> EnhancedClient:
    return EnhancedClient(endpoint_url=request_body.agent_url, headers=request_body.headers)


@app.post("/stream")
async def stream_agent_response(request_body: AgentRequest, client: EnhancedClient = Depends(get_enhanced_client)):
    """
    请求后端Agent server并以SSE流的形式返回结果。
    """
    message = Message(
        content=TextContent(text=request_body.query),
        role=MessageRole.USER
    )

    async def event_generator():
        async for chunk in client.stream_response(message):
            yield chunk

    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7000)