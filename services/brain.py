import os
import requests
import yaml
import logging
from pathlib import Path
from typing import List, Tuple

# We will load config dynamically to support hot-reloading behavior
def load_config():
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

def chat(messages: List[Tuple[str, str]]) -> str:
    config = load_config()
    
    # Defaults
    ollama_url = config.get("ollama", {}).get("base_url", "http://localhost:11434")
    model = config.get("ollama", {}).get("model", "llama3.1:8b")
    system_prompt = config.get("system_prompt", "You are a helpful assistant.")
    
    # RAG Logic
    rag_enabled = config.get("rag", {}).get("enabled", False)
    rag_context = ""
    
    if rag_enabled:
        try:
            # Lazy import to avoid circular dependency issues if any
            from services.rag import rag_engine
            
            # Extract query from messages (last user message)
            last_message = next((m for m in reversed(messages) if m[0] == "user"), None)
            if last_message:
                query_text = last_message[1]
                top_k = config.get("rag", {}).get("top_k", 3)
                
                # Check for fuzzy matches (optional/future)
                # ...
                
                results = rag_engine.query(query_text, n_results=top_k)
                
                if results:
                    rag_context = "\n\nRelevant Context from Local Knowledge Base:\n"
                    for res in results:
                        rag_context += f"- [{res['source']}] {res['content']}\n"
                    
                    logger = logging.getLogger(__name__)
                    logger.info(f"RAG retrieved {len(results)} chunks.")

        except Exception as e:
            print(f"RAG Error: {e}") # basic logging

    # Construct Payload
    system_content = system_prompt
    if rag_context:
        system_content += rag_context

    payload = {
        "model": model,
        "stream": False,
        "messages": [{"role": "system", "content": system_content}]
        + [{"role": role, "content": content} for role, content in messages],
    }
    
    # Add optional params
    if "ollama" in config:
        if "temperature" in config["ollama"]:
            payload["temperature"] = config["ollama"]["temperature"]
        if "max_tokens" in config["ollama"]:
             payload["num_predict"] = config["ollama"]["max_tokens"]

    try:
        r = requests.post(f"{ollama_url}/api/chat", json=payload, timeout=180)
        r.raise_for_status()
        data = r.json()
        return (data.get("message") or {}).get("content", "").strip() or "(No response.)"
    except Exception as e:
        return f"Error connecting to Brain: {e}"