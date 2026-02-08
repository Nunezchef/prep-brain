import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _truncate(text: str, limit: int = 220) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit]}..."


def _source_snapshot(rag_engine, limit: int = 5) -> str:
    sources = rag_engine.get_sources() or []
    if not sources:
        return "No sources are currently registered in the local knowledge base."

    sources.sort(key=lambda source: source.get("date_ingested", ""), reverse=True)
    lines = []
    for idx, source in enumerate(sources[:limit], start=1):
        title = source.get("title") or source.get("source_name") or "unknown"
        status = source.get("status", "unknown")
        chunks = source.get("chunk_count", 0)
        date_ingested = source.get("date_ingested", "unknown")
        summary = _truncate(source.get("summary", ""), limit=160)
        lines.append(
            f"{idx}. title={title} | status={status} | chunks={chunks} | date_ingested={date_ingested} | summary={summary}"
        )

    return "\n".join(lines)


def _build_rag_reference_context(query_text: str, config: Dict[str, Any]) -> str:
    top_k = int(config.get("rag", {}).get("top_k", 3))

    from services.rag import rag_engine

    logger.info("RAG query string: %s", query_text)

    results = rag_engine.search(query_text, n_results=top_k)
    retrieved_count = len(results)

    top_preview = _truncate(results[0]["content"], limit=220) if results else "NONE"

    logger.info("RAG retrieved chunk count: %s", retrieved_count)
    logger.info("RAG top chunk preview: %s", top_preview)

    source_snapshot = _source_snapshot(rag_engine, limit=5)

    context_lines: List[str] = [
        "REFERENCE CONTEXT (RAG)",
        f"QUERY: {query_text}",
        f"RETRIEVED_CHUNK_COUNT: {retrieved_count}",
        f"TOP_CHUNK_PREVIEW: {top_preview}",
        "",
        "ACTIVE SOURCE SNAPSHOT (MOST RECENT FIRST):",
        source_snapshot,
        "",
        "INSTRUCTIONS:",
        "- Use RETRIEVED CHUNKS as the highest-priority grounding context.",
        "- If RETRIEVED_CHUNK_COUNT is 0, explicitly say no direct RAG chunks matched this query.",
        "- If user asks about the last ingested document, use ACTIVE SOURCE SNAPSHOT metadata.",
    ]

    if results:
        context_lines.extend(["", "RETRIEVED CHUNKS:"])
        for idx, chunk in enumerate(results, start=1):
            source = chunk.get("source", "unknown")
            heading = chunk.get("heading", "")
            distance = chunk.get("distance", 0)
            content = _truncate(chunk.get("content", ""), limit=1200)
            context_lines.append(
                f"[{idx}] source={source} | heading={heading} | distance={distance}\n{content}"
            )
    else:
        logger.warning("RAG retrieval returned 0 chunks for query: %s", query_text)
        context_lines.extend(
            [
                "",
                "RETRIEVED CHUNKS: NONE",
                "NO_MATCHING_CHUNKS_FOUND_FOR_THIS_QUERY=true",
            ]
        )

    return "\n".join(context_lines)


def chat(messages: List[Tuple[str, str]]) -> str:
    config = load_config()

    ollama_url = config.get("ollama", {}).get("base_url", "http://localhost:11434")
    model = config.get("ollama", {}).get("model", "llama3.1:8b")
    system_prompt = config.get("system_prompt", "You are a helpful assistant.")

    rag_enabled = bool(config.get("rag", {}).get("enabled", False))

    last_user_message: Optional[Tuple[str, str]] = next((m for m in reversed(messages) if m[0] == "user"), None)
    query_text = (last_user_message[1].strip() if last_user_message else "") if messages else ""

    payload_messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]

    if rag_enabled:
        if query_text:
            try:
                rag_reference = _build_rag_reference_context(query_text=query_text, config=config)
                payload_messages.append({"role": "system", "content": rag_reference})
            except Exception as exc:
                logger.exception("RAG integration error")
                payload_messages.append(
                    {
                        "role": "system",
                        "content": (
                            "REFERENCE CONTEXT (RAG)\n"
                            f"QUERY: {query_text}\n"
                            "RETRIEVED_CHUNK_COUNT: 0\n"
                            f"RAG_ERROR: {exc}\n"
                            "No RAG chunks were injected due to an integration error."
                        ),
                    }
                )
        else:
            logger.warning("RAG is enabled but no user query was found in message history.")
            payload_messages.append(
                {
                    "role": "system",
                    "content": (
                        "REFERENCE CONTEXT (RAG)\n"
                        "QUERY: NONE\n"
                        "RETRIEVED_CHUNK_COUNT: 0\n"
                        "No user query was available for retrieval."
                    ),
                }
            )

    payload_messages.extend([{"role": role, "content": content} for role, content in messages])

    payload: Dict[str, Any] = {
        "model": model,
        "stream": False,
        "messages": payload_messages,
    }

    ollama_cfg = config.get("ollama", {})
    if "temperature" in ollama_cfg:
        payload["temperature"] = ollama_cfg["temperature"]
    if "max_tokens" in ollama_cfg:
        payload["num_predict"] = ollama_cfg["max_tokens"]

    try:
        response = requests.post(f"{ollama_url}/api/chat", json=payload, timeout=180)
        response.raise_for_status()
        data = response.json()
        return (data.get("message") or {}).get("content", "").strip() or "(No response.)"
    except Exception as exc:
        logger.error("Error connecting to Brain: %s", exc)
        return f"Error connecting to Brain: {exc}"
