import aiohttp
import asyncio
import os
import unittest


class A2ABaseTestCase(unittest.IsolatedAsyncioTestCase):
    """
    测试 A2A FastAPI 接口
    """
    host = '127.0.0.1'
    port = 7000
    env_host = os.environ.get('host')
    if env_host:
        host = env_host
    base_url = f"http://{host}:{port}"
    async def test_agent_stream_client(self):
        url = f"{self.base_url}/stream"  # 修改为你的 FastAPI 服务地址
        payload = {
            "query": "你好，Agent！",
            "agent_url": "http://127.0.0.1:6002",  # 修改为你的后端 Agent server 地址
            "headers": {}  # 如果有鉴权信息填这里
        }

        headers = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    print(f"请求失败，状态码: {resp.status}")
                    return

                print("连接成功，开始接收流数据：\n")
                async for line in resp.content:
                    decoded_line = line.decode("utf-8").strip()
                    print(f"收到数据：{decoded_line}")

if __name__ == '__main__':
    unittest.main()
