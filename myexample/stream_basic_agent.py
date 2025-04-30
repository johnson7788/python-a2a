#!/usr/bin/env python
"""
Basic Streaming Example

This example demonstrates how to use streaming capabilities to receive
real-time responses from A2A agents that support streaming.

The example:
- Sets up a streaming-capable agent on a local server
- Uses a streaming client to process streamed chunks as they arrive
- Displays real-time progress with a visual indicator

To run:
    python basic_streaming.py [--query QUESTION]

Requirements:
    pip install "python-a2a[all]"
"""

import sys
import argparse
import asyncio
import threading
import time
import socket
import random
from flask import Flask, request, jsonify, Response
import json
import logging

from python_a2a import (
    A2AServer, A2AClient, AgentCard, AgentSkill,
    Message, TextContent, MessageRole,
    Task, TaskStatus, TaskState
)
from mcp_client.client import SSEMCPClient,MCPClient,generate_text,process_tool_call
from mcp_client.utils import load_mcp_config_from_file
from dotenv import load_dotenv
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


load_dotenv()

# Import necessary components - using A2AClient instead of abstract StreamingClient
try:
    # Try to use aiohttp if available (needed for true streaming)
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    print("Warning: aiohttp not installed. Streaming visualization will be simulated.")
    print("Install aiohttp with: pip install aiohttp")


class StreamingAgent(A2AServer):
    """
    A simple agent that supports streaming responses.
    
    This agent simulates a streaming response by sending text
    chunks with deliberate delays to create a realistic streaming effect.
    """
    
    def __init__(self, config_path="mcp_config.json", model_name="deepseek"):
        """Initialize the streaming agent with appropriate capabilities."""
        agent_card = AgentCard(
            name="Streaming Agent",
            description="An agent that supports streaming responses",
            url="http://localhost:0",  # Will be updated later
            version="1.0.0",
            capabilities={"streaming": True},  # Mark as streaming-capable
            skills=[
                AgentSkill(
                    name="Streaming Response",
                    description="Generate responses with streaming output",
                    tags=["streaming"]
                )
            ]
        )
        super().__init__(agent_card=agent_card)
        self.config_path = config_path
        self.model_name = model_name
        self.config = load_mcp_config_from_file(config_path)
        self.servers_cfg = self.config.get("mcpServers", {})
        self.servers = {}
        self.all_functions = []
        self.conversation = [] # Initial conversation might be built later in run() or here
        self.tool_ready = False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logging.info("Creating a new event loop in a sub-thread.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self.setup_tools())
        logging.info(f"初始化Agent和MCP工具成功")

    async def setup_tools(self):
        """
        启动所有的MCP工具
        """
        print("Starting MCP servers...")
        successful_servers = {}
        all_functions = []
        # 初始化MCP的server
        for server_name, conf in self.servers_cfg.items():
            client = None
            if "url" in conf:  # SSE server
                client = SSEMCPClient(server_name, conf["url"])
            elif "command" in conf:  # Local process-based server
                 client = MCPClient(
                     server_name=server_name,
                     command=conf.get("command"),
                     args=conf.get("args", []),
                     env=conf.get("env", {})
                 )
            else:
                 if not self.quiet_mode:
                     print(f"[WARN] Skipping server {server_name}: No 'url' or 'command' specified.")
                 continue

            try:
                ok = await client.start() # <-- AWAIT is valid here (inside async def)
                if not ok:
                    if not self.quiet_mode:
                        print(f"[WARN] Could not start server {server_name}")
                    # Ensure client is stopped even if start failed
                    if client: await client.stop()
                    continue
                else:
                    print(f"[MCP Tool OK] {server_name}")
                    successful_servers[server_name] = client

                    # gather tools
                    try:
                         tools = await client.list_tools() # <-- AWAIT is valid here
                         for t in tools:
                             input_schema = t.get("inputSchema") or {"type": "object", "properties": {}}
                             fn_def = {
                                 "name": f"{server_name}_{t['name']}",
                                 "description": t.get("description", ""),
                                 "parameters": input_schema
                             }
                             all_functions.append(fn_def)
                    except Exception as e:
                        if not self.quiet_mode:
                            print(f"[WARN] Error listing tools for {server_name}: {e}")
                        # Consider if failing to list tools should stop processing for this server


            except Exception as e: # Catch potential errors during client creation or start
                if not self.quiet_mode:
                    print(f"[WARN] Exception starting server {server_name}: {e}")
                # Ensure client is stopped if created before exception
                if 'client' in locals() and client: await client.stop()


        self.servers = successful_servers
        self.all_functions = all_functions

        if not self.servers:
            error_msg = "No MCP servers could be started."
            print(f"[ERROR] {error_msg}")
            self.tool_ready = False # Cannot run without servers
            return False

        print(f"Found {len(self.all_functions)} tools.它们是: {self.all_functions}")
        self.tool_ready = True # Setup was successful
        return True
    def handle_message(self, message):
        """Handle a message request with a complete response."""
        # Extract the query from the message
        query = ""
        if hasattr(message.content, "text"):
            query = message.content.text
        
        # Generate a complete response
        response_text = self._generate_response(query)
        
        # Create response message
        response = Message(
            content=TextContent(text=response_text),
            role=MessageRole.AGENT,
            message_id=f"response-{time.time()}",
            parent_message_id=message.message_id,
            conversation_id=message.conversation_id
        )
        
        return response
    
    def handle_task(self, task):
        """Handle a task by providing a non-streaming response."""
        # Extract query from task
        query = self._extract_query(task)
        
        # Generate complete response
        response_text = self._generate_response(query)
        
        # Create response
        task.artifacts = [{
            "parts": [{"type": "text", "text": response_text}]
        }]
        task.status = TaskStatus(state=TaskState.COMPLETED)
        return task
    
    def _extract_query(self, task):
        """Extract the query text from a task."""
        if task.message:
            if isinstance(task.message, dict):
                content = task.message.get("content", {})
                if isinstance(content, dict):
                    return content.get("text", "")
        return ""
    
    def _generate_response(self, query):
        """Generate a complete response to the query."""
        query = query.lower()
        
        if "hello" in query or "hi" in query:
            return "Hello! I'm a streaming-capable agent. I can respond to your questions in real-time, sending each part of my response as soon as it's ready."
            
        elif "weather" in query:
            return "The weather today is sunny with some scattered clouds. Temperature is around 22°C (72°F) with a light breeze from the west. There's a small chance of rain in the evening, but overall it should be a pleasant day."
            
        elif "help" in query:
            return "I'm a streaming-capable agent that demonstrates how the A2A streaming protocol works. You can ask me various questions and I'll respond in a streaming fashion, sending my response chunk by chunk. Try asking about the weather, streaming capabilities, or any general knowledge question!"
            
        elif "stream" in query or "streaming" in query:
            return "Streaming in the A2A protocol allows agents to send responses incrementally as they're generated, rather than waiting for the complete response to be ready. This provides a more interactive experience, especially for longer responses or when the response generation takes time. The streaming capabilities in Python A2A make it easy to consume these streaming responses."
            
        else:
            # Default response for any other query
            return (
                "I received your query and I'm responding with a streaming response. "
                "This means my answer is being sent to you piece by piece, as soon as each fragment is ready. "
                "Streaming is particularly useful for long-form content, allowing you to start reading "
                "the beginning of the response while I'm still generating the rest. "
                "This creates a more interactive and responsive experience. "
                "The Python A2A library handles all the complexity of managing the streaming connection, "
                "so developers can focus on creating great agent experiences. "
                "In this example, you'll see chunks arriving with deliberate delays to simulate "
                "the streaming experience. In a real application with actual language models, "
                "the chunks would arrive as the model generates them."
            )

# 初始化Agent 信息
port = 6002
app = Flask(__name__)
agent = StreamingAgent()
# Update the agent's URL to include the actual port
agent.agent_card.url = f"http://localhost:{port}"

@app.route('/agent.json', methods=['GET'])
def get_agent_card():
    """Return the agent card information."""
    return jsonify(agent.agent_card.to_dict())

@app.route('/a2a/agent.json', methods=['GET'])
def get_a2a_agent_card():
    """Return the agent card at the alternate endpoint."""
    return jsonify(agent.agent_card.to_dict())

@app.route('/', methods=['POST'])
def handle_message():
    """Handle standard message requests."""
    try:
        # Extract the request data
        data = request.json

        # Check what type of request this is
        if isinstance(data, dict) and "message" in data:
            # This is a message request
            message = Message.from_dict(data["message"])

            # Process the message
            response = agent.handle_message(message)

            # Return the response
            return jsonify(response.to_dict())

        elif isinstance(data, dict) and "id" in data:
            # This is a Task request
            task = Task.from_dict(data)

            # Process the task
            result = agent.handle_task(task)

            # Return the result
            return jsonify(result.to_dict())

        else:
            # Create a message from the raw data
            if isinstance(data, dict):
                content = data.get("content", {})
                if isinstance(content, dict):
                    text = content.get("text", "")
                else:
                    text = str(content)
            else:
                text = str(data)

            message = Message(
                content=TextContent(text=text),
                role=MessageRole.USER
            )

            # Process the message
            response = agent.handle_message(message)

            # Return the response
            return jsonify(response.to_dict())

    except Exception as e:
        # If there's an error, return it
        return jsonify({"error": str(e)}), 400

# Add task endpoints to avoid 404 errors
@app.route('/tasks/send', methods=['POST'])
@app.route('/a2a/tasks/send', methods=['POST'])
def handle_task_send():
    """Handle task send requests."""
    try:
        # Extract the request data
        data = request.json

        # This is a JSON-RPC request for tasks
        if "params" in data:
            # Extract the task from params
            task_data = data.get("params", {})
            task = Task.from_dict(task_data)

            # Process the task
            result = agent.handle_task(task)

            # Return JSON-RPC response
            return jsonify({
                "jsonrpc": "2.0",
                "id": data.get("id", 1),
                "result": result.to_dict()
            })
        else:
            # Direct task submission
            task = Task.from_dict(data)
            result = agent.handle_task(task)
            return jsonify(result.to_dict())

    except Exception as e:
        # If there's an error, return it as a JSON-RPC error
        return jsonify({
            "jsonrpc": "2.0",
            "id": data.get("id", 1) if 'data' in locals() else 1,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }), 500

@app.route('/stream', methods=['POST'])
def handle_streaming():
    """Handle streaming requests."""
    try:
        # Extract the request data
        data = request.json

        # Get the message
        if "message" in data:
            if isinstance(data["message"], dict):
                message = Message.from_dict(data["message"])
            else:
                message = Message(
                    content=TextContent(text=str(data["message"])),
                    role=MessageRole.USER
                )
        else:
            # Create a default message
            message = Message(
                content=TextContent(text="Default query"),
                role=MessageRole.USER
            )

        # Get the query text
        query = ""
        if hasattr(message.content, "text"):
            query = message.content.text

        # Generate a full response
        full_response = agent._generate_response(query)

        # Simulate streaming by breaking it into chunks with delays
        def generate():
            # Break the response into words
            words = full_response.split()
            current_chunk = []

            # Send words in small chunks with random delays
            for word in words:
                current_chunk.append(word)

                # Every 3-7 words (randomized for realism), send a chunk
                if len(current_chunk) >= random.randint(3, 7):
                    chunk_text = " ".join(current_chunk)

                    # Format as server-sent event
                    data_obj = {"text": chunk_text}
                    yield f"data: {json.dumps(data_obj)}\n\n"

                    # Reset for next chunk
                    current_chunk = []

                    # Add a small delay to simulate typing or thinking
                    time.sleep(random.uniform(0.2, 0.5))

            # Send any remaining words
            if current_chunk:
                chunk_text = " ".join(current_chunk)
                data_obj = {"text": chunk_text}
                yield f"data: {json.dumps(data_obj)}\n\n"

        # Return streaming response
        return Response(generate(), content_type='text/event-stream')

    except Exception as e:
        # If there's an error, return it
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    # Start the Agent server
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
