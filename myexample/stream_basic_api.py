#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date  : 2025/4/29 09:01
# @File  : stream_api.py
# @Author: johnson
# @Contact : github: johnson7788
# @Desc  : A2A的Stream Client的API, 可以被前端调用

import sys
import argparse
import asyncio
import threading
import time
import socket
import random
from flask import Flask, request, jsonify, Response
import json

from python_a2a import (
    A2AServer, A2AClient, AgentCard, AgentSkill,
    Message, TextContent, MessageRole,
    Task, TaskStatus, TaskState
)

# Import necessary components - using A2AClient instead of abstract StreamingClient
try:
    # Try to use aiohttp if available (needed for true streaming)
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = self.endpoint_url  # Ensure URL is accessible for compatibility

    async def check_streaming_support(self):
        """Check if the agent supports streaming capabilities."""
        # If aiohttp is not available, we can't do true streaming
        if not HAS_AIOHTTP:
            return False

        try:
            # Try to fetch the agent card to check capabilities
            async with aiohttp.ClientSession() as session:
                headers = dict(self.headers)
                headers["Accept"] = "application/json"  # Prefer JSON response

                # Try the primary endpoint
                async with session.get(f"{self.url}/agent.json", headers=headers) as response:
                    if response.status == 200:
                        # Check content type
                        content_type = response.headers.get("Content-Type", "").lower()

                        if "json" in content_type:
                            # Parse as JSON directly
                            data = await response.json()
                        else:
                            # Try to extract JSON from HTML or text
                            try:
                                text = await response.text()
                                # Simple extraction of JSON (could be enhanced)
                                import re
                                json_match = re.search(r'({[\s\S]*"capabilities"[\s\S]*})', text)
                                if json_match:
                                    import json
                                    data = json.loads(json_match.group(1))
                                else:
                                    data = {}
                            except:
                                data = {}

                        # Check capabilities
                        return (
                                isinstance(data, dict) and
                                isinstance(data.get("capabilities"), dict) and
                                data.get("capabilities", {}).get("streaming", False)
                        )

                # Try the alternate endpoint
                async with session.get(f"{self.url}/a2a/agent.json", headers=headers) as response:
                    if response.status == 200:
                        # Check content type
                        content_type = response.headers.get("Content-Type", "").lower()

                        if "json" in content_type:
                            # Parse as JSON directly
                            data = await response.json()
                        else:
                            # Try to extract JSON from HTML or text
                            try:
                                text = await response.text()
                                # Simple extraction of JSON (could be enhanced)
                                import re
                                json_match = re.search(r'({[\s\S]*"capabilities"[\s\S]*})', text)
                                if json_match:
                                    import json
                                    data = json.loads(json_match.group(1))
                                else:
                                    data = {}
                            except:
                                data = {}

                        # Check capabilities
                        return (
                                isinstance(data, dict) and
                                isinstance(data.get("capabilities"), dict) and
                                data.get("capabilities", {}).get("streaming", False)
                        )

            return False
        except Exception as e:
            print(f"Error checking streaming support: {e}")
            return False

    async def stream_response(self, message, chunk_callback=None):
        """
        Stream a response from the agent.

        Args:
            message: Message to send
            chunk_callback: Function to call for each chunk

        Yields:
            Response chunks as they arrive
        """
        if not HAS_AIOHTTP:
            # If aiohttp is not available, simulate streaming with the full response
            async for chunk in self._simulate_streaming(message, chunk_callback):
                yield chunk
            return

        # Check if streaming is supported
        supports_streaming = await self.check_streaming_support()

        if not supports_streaming:
            # Fall back to non-streaming for non-streaming agents
            async for chunk in self._simulate_streaming(message, chunk_callback):
                yield chunk
            return

        try:
            # Set up headers for streaming
            headers = dict(self.headers)
            headers["Accept"] = "text/event-stream"

            # Create a session and send the request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        f"{self.url}/stream",
                        json={"message": message.to_dict()},
                        headers=headers
                ) as response:
                    # Handle errors
                    if response.status >= 400:
                        raise Exception(f"HTTP error {response.status}")

                    # Process the streaming response
                    buffer = ""
                    async for chunk in response.content.iter_chunked(1024):
                        if not chunk:
                            continue

                        # Decode chunk and add to buffer
                        chunk_text = chunk.decode('utf-8')
                        buffer += chunk_text

                        # Process complete events
                        while "\n\n" in buffer:
                            event, buffer = buffer.split("\n\n", 1)

                            # Extract data field
                            for line in event.split("\n"):
                                if line.startswith("data:"):
                                    data = line[5:].strip()

                                    try:
                                        # Try to parse as JSON
                                        data_obj = json.loads(data)
                                        if "text" in data_obj:
                                            # Extract text and process
                                            text = data_obj["text"]
                                            if chunk_callback:
                                                chunk_callback(text)
                                            yield text
                                    except json.JSONDecodeError:
                                        # Not JSON, yield raw data
                                        if chunk_callback:
                                            chunk_callback(data)
                                        yield data

        except Exception as e:
            # Fall back to simulated streaming on error
            print(f"Streaming error (falling back to non-streaming): {str(e)}")
            async for chunk in self._simulate_streaming(message, chunk_callback):
                yield chunk

    async def _simulate_streaming(self, message, chunk_callback=None):
        """Simulate streaming using the full response."""
        # Get the full response
        response = self.send_message(message)

        # Extract text
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

        # Break into words to simulate chunks
        words = full_text.split()

        # Create chunks of 5-10 words
        current_chunk = []
        for word in words:
            current_chunk.append(word)
            if len(current_chunk) >= random.randint(5, 10):
                # Join words into a chunk
                chunk_text = " ".join(current_chunk)

                # Process the chunk
                if chunk_callback:
                    chunk_callback(chunk_text)

                # Add a small delay to simulate network latency
                await asyncio.sleep(random.uniform(0.1, 0.3))

                # Yield the chunk
                yield chunk_text

                # Reset for next chunk
                current_chunk = []

        # Process any remaining words
        if current_chunk:
            chunk_text = " ".join(current_chunk)
            if chunk_callback:
                chunk_callback(chunk_text)
            yield chunk_text


# stream_with_progress：打印的每个 chunk 连在一起输出，模拟自然语言流式生成体验。
async def stream_with_progress(client, message):
    """Stream a response with a progress visualization."""
    total_chars = 0
    chunk_count = 0
    start_time = time.time()
    full_response = ""

    print("\nStreaming response:\n" + "-" * 60)

    # Function to handle each chunk
    def handle_chunk(chunk):
        nonlocal total_chars, chunk_count, full_response

        # Update counters
        total_chars += len(chunk)
        chunk_count += 1
        full_response += chunk

        # Calculate metrics
        elapsed = time.time() - start_time
        chars_per_sec = total_chars / elapsed if elapsed > 0 else 0

        # Print the new chunk
        print(chunk, end="", flush=True)

        # We don't know the total in advance, so we just show the current progress
        elapsed_str = f"{elapsed:.1f}s"
        rate_str = f"{chars_per_sec:.1f} chars/sec"

        # Clear the current line and print status on the second line
        # We store cursor position after content but before status line

    # Process the streaming response
    try:
        async for _ in client.stream_response(message, chunk_callback=handle_chunk):
            # Just process with the callback
            pass

        # Calculate final stats
        elapsed = time.time() - start_time
        chars_per_sec = total_chars / elapsed if elapsed > 0 else 0

        # Print final stats
        print("\n" + "-" * 60)
        print(f"Streaming complete:")
        print(f"- Total characters: {total_chars}")
        print(f"- Chunks received: {chunk_count}")
        print(f"- Time elapsed: {elapsed:.2f} seconds")
        print(f"- Average speed: {chars_per_sec:.1f} characters/second")

        return full_response

    except Exception as e:
        print(f"\nError during streaming: {e}")
        return None

def main():
    """Run the streaming example."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Basic Streaming Example")
    parser.add_argument("--query", type=str, default="Tell me about streaming in the A2A protocol.",
                        help="Query to send to the streaming agent")
    parser.add_argument("--agent", type=str, default="http://127.0.0.1:6002",help="Agent url")
    parser.add_argument("--debug", action="store_true", help="Show debug information")
    args = parser.parse_args()
    print("=== Basic Streaming Example ===\n")
    print(f"连接Agent并进行请求")

    print(f"✓ Streaming agent ready at {args.agent}")

    # Create enhanced client with streaming capabilities instead of StreamingClient
    client = EnhancedClient(args.agent)

    # Create message with query
    message = Message(
        content=TextContent(text=args.query),
        role=MessageRole.USER
    )

    print(f"\nSending query: '{args.query}'")

    # Run the streaming request using asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        # No event loop in thread, create one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        # Check if streaming is supported
        if args.debug:
            print("Checking streaming support...")

        supports_streaming = loop.run_until_complete(client.check_streaming_support())
        assert supports_streaming, "Agent必须支持流的形式的输出"
        print("✓ Agent supports streaming responses")
        loop.run_until_complete(stream_with_progress(client, message))

    except KeyboardInterrupt:
        print("\nStreaming interrupted by user")
    except Exception as e:
        print(f"\nError during streaming: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    main()
