model_config =  [
    {
      "title": "llama3.2",
      "provider": "lmstudio",
      "model": "llama-3.2-3b-instruct",
      "systemMessage": "You are a helpful assistant that uses tools when appropriate.",
      "default": True
    },
    {
      "model": "deepseek-chat",
      "title": "deepseek-chat",
      "systemMessage": "你是一个关于天然气的大模型，可以使用普通工具和知识检索工具回答各种问题。",
      "provider": "deepseek"
    },
    {
      "title": "llama",
      "provider": "ollama",
      "model": "llama3.1"
    },
    {
      "model": "my-model",
      "title": "custom-url-openai-compatible",
      "apiBase": "http://whatever:8080/v1",
      "provider": "openai"
    },
    {
      "model": "claude-3-7-sonnet-latest",
      "provider": "anthropic",
      "apiKey": "****",
      "title": "claude",
      "temperature": 0.7,
      "top_k": 256,
      "top_p": 0.9,
      "max_tokens": 2048
    },
    {
      "model": "gpt-4o",
      "title": "gpt-4o",
      "systemMessage": "You are an expert software developer. You give helpful and concise responses.",
      "provider": "openai"
    },
    {
      "model": "o3-mini",
      "title": "o3-mini",
      "systemMessage": "You are an expert software developer. You give helpful and concise responses.",
      "contextLength": 128000,
      "maxCompletionTokens": 65536,
      "apiKey": "****",
      "provider": "openai",
      "temperature": 0.2,
      "top_p": 0.8
    }
  ]