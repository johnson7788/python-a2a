#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date  : 2025/4/29 09:01
# @File  : stream_api.py
# @Author: johnson
# @Contact : github: johnson7788
# @Desc  : A2A的 Client的API

import asyncio
from python_a2a import StreamingClient, Message, TextContent, MessageRole
from python_a2a import Task, TaskStatus, TaskState
from python_a2a import A2AClient

async def main():
    # Create a streaming client
    endpoint_url = "http://localhost:6001"
    client = A2AClient(endpoint_url)
    print("\n=== Agent Information ===")
    print(f"Name: {client.agent_card.name}")
    print(f"Description: {client.agent_card.description}")
    print(f"Version: {client.agent_card.version}")

    if client.agent_card.skills:
        print("\nAvailable Skills:")
        for skill in client.agent_card.skills:
            print(f"- {skill.name}: {skill.description}")
            if skill.examples:
                print(f"  Examples: {', '.join(skill.examples)}")
    response = client.ask("what's today weather in Tokoy")

    # Print the response
    print("\nAgent response:")
    print(f"{response}")


if __name__ == "__main__":
    asyncio.run(main())
