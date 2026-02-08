import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

logger = logging.getLogger(__name__)

CHEF_CARD_STYLE_APPENDIX = """Assistant Response Style

You are a kitchen and restaurant operations assistant.

You must format all answers for Telegram delivery using HTML and a concise "chef reference card" style:
- Start with a bold title.
- Follow with a single clear definition or answer.
- Use a short bullet list for key points (3-5 bullets max).
- Include a brief "In the kitchen" section focused on real-world application.
- Keep answers practical, grounded, and calm.

Avoid:
- Long paragraphs
- Academic or textbook language
- Overly verbose explanations
- Generic chatbot phrasing

Speak like a senior chef explaining something during prep or service: precise, minimal, and useful.
"""

GROUNDING_RULE_APPENDIX = """Grounding Rule (MANDATORY)

When reference context from the local knowledge base is provided:
- Base your answer primarily on that context.
- Prefer specific claims, mechanisms, or observations stated in the source.
- Avoid generic textbook explanations when a cited source is available.
- If the source does not explicitly answer the question, say so clearly.
- Do not use filler phrases without completing the idea.

Attribution Rule:
- When mentioning an author or book, only attribute claims that are supported by retrieved context.
"""

ANTI_REPETITION_RULE_APPENDIX = """Anti-Repetition Rule (MANDATORY)

- Do not restate the same sentence or idea in multiple sections of the same answer.
- Each section must add new information.
"""

SOURCE_GROUNDED_AUTHOR_RULE_APPENDIX = """Source-Grounded Answering Rule (MANDATORY)

When reference context from the local knowledge base (RAG) is provided and the user asks about a specific author, book, or document:
- Base your answer primarily and explicitly on the retrieved reference context.
- Prefer specific mechanisms, distinctions, or constraints stated in the source.
- Do not substitute generic background knowledge under an author's name.
- Avoid vague filler phrases.
- If the retrieved context does not clearly support a claim, say exactly:
  "The retrieved material does not explicitly address this."
- Do not repeat introductory sentences or restate the same idea in multiple sections.

Your role is to interpret and explain the source, not to summarize general knowledge.

RAG Context Handling:
- Treat retrieved RAG context as authoritative evidence.
- When a source is cited (for example, an author or book), only attribute claims supported by retrieved context.
- If multiple chunks are retrieved, synthesize them carefully rather than generalizing.
"""

RECIPE_AUTHORITY_RULE_APPENDIX = """Recipe Authority Rule

When answering questions about a specific dish or recipe:
- Use restaurant recipe sources as the sole authority when available.
- Do not supplement or replace missing recipe information with reference material.
- If a recipe does not specify something, say so explicitly.
- Reference sources may only be used to explain why a step exists, never what the recipe is.
"""

COMPONENT_FIRST_RULE_APPENDIX = """Component-First Answering Rule (MANDATORY)

When the user asks for a recipe component (for example: vinaigrette, glaze, custard, sauce):
1) Respond with the base recipe first (ingredients and quantities).
2) Do not include plating ratios or dish assembly unless explicitly asked.
3) If relevant, include a single-line "In service" note.
4) Do not suggest quantity adjustments.
5) Do not ask follow-up questions.
6) Avoid filler, hedging, and conversational padding.

Style (Kitchen Mode):
- Be concise.
- Be directive.
- Avoid duplicated phrases.
- Sound like a senior sous chef reading from the prep book.
"""

RECIPE_QUERY_PATTERNS = [
    r"\brecipe\b",
    r"\bdish\b",
    r"\bhow (?:to|do i|do we)\s+(?:make|cook|prepare)\b",
    r"\bwhat(?:'s| is)\s+(?:the|our)\s+recipe\b",
]

RECIPE_COMPONENT_TERMS = [
    "sauce",
    "vinaigrette",
    "glaze",
    "custard",
    "stock",
    "broth",
    "dressing",
    "marinade",
    "aioli",
    "syrup",
    "ganache",
    "puree",
]

COMPONENT_QUERY_STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "from",
    "how",
    "in",
    "is",
    "make",
    "me",
    "of",
    "our",
    "please",
    "recipe",
    "show",
    "the",
    "to",
    "what",
}

COMPONENT_ASSEMBLY_EXCLUSION_TERMS = [
    "assembly",
    "garnish",
    "plating",
    "plating ratio",
    "smoked prawn",
    "smoked prawns",
    "prawn",
    "prawns",
    "shallot",
]


def _is_component_query(query_text: str) -> bool:
    lowered = (query_text or "").strip().lower()
    if not lowered:
        return False
    return any(term in lowered for term in RECIPE_COMPONENT_TERMS)


def _component_focus_terms(query_text: str) -> List[str]:
    tokens = re.findall(r"[a-zA-Z0-9%]+", (query_text or "").lower())
    terms: List[str] = []
    for token in tokens:
        if token in COMPONENT_QUERY_STOPWORDS:
            continue
        if len(token) < 3:
            continue
        terms.append(token)
    return list(dict.fromkeys(terms))


def _chunk_component_match_score(chunk: Dict[str, Any], focus_terms: List[str]) -> int:
    heading = (chunk.get("heading") or "").lower()
    nearby = (chunk.get("content") or "")[:260].lower()
    haystack = f"{heading} {nearby}"
    score = 0
    for term in focus_terms:
        if term in heading:
            score += 3
        elif term in haystack:
            score += 1
    return score


def _is_component_assembly_chunk(chunk: Dict[str, Any]) -> bool:
    haystack = f"{chunk.get('heading', '')} {chunk.get('content', '')}".lower()
    return any(term in haystack for term in COMPONENT_ASSEMBLY_EXCLUSION_TERMS)


def _filter_component_results(query_text: str, results: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not _is_component_query(query_text) or not results:
        return results, {
            "component_query": False,
            "focus_terms": [],
            "before_count": len(results),
            "after_count": len(results),
            "excluded_assembly": 0,
            "heading_matched": 0,
        }

    focus_terms = _component_focus_terms(query_text=query_text)
    scored: List[Tuple[int, Dict[str, Any]]] = []
    excluded_assembly = 0
    heading_matched = 0

    for chunk in results:
        if _is_component_assembly_chunk(chunk):
            excluded_assembly += 1
            continue

        score = _chunk_component_match_score(chunk=chunk, focus_terms=focus_terms)
        if score > 0:
            heading_matched += 1
        scored.append((score, chunk))

    scored.sort(key=lambda item: item[0], reverse=True)

    preferred = [chunk for score, chunk in scored if score > 0]
    ordered = preferred if preferred else [chunk for _, chunk in scored]

    debug = {
        "component_query": True,
        "focus_terms": focus_terms,
        "before_count": len(results),
        "after_count": len(ordered),
        "excluded_assembly": excluded_assembly,
        "heading_matched": heading_matched,
    }
    return ordered, debug


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


def _is_recipe_query(query_text: str) -> bool:
    text = (query_text or "").strip().lower()
    if not text:
        return False

    for pattern in RECIPE_QUERY_PATTERNS:
        if re.search(pattern, text):
            return True

    return any(f" {term} " in f" {text} " for term in RECIPE_COMPONENT_TERMS)


def _retrieve_rag_results(query_text: str, top_k: int):
    from services.rag import TIER_1_RECIPE_OPS, TIER_3_REFERENCE_THEORY, rag_engine

    recipe_query = _is_recipe_query(query_text)
    component_query = _is_component_query(query_text)

    if recipe_query:
        tier1_results_raw = rag_engine.search(
            query_text=query_text,
            n_results=top_k,
            source_tiers=[TIER_1_RECIPE_OPS],
        )
        tier1_results, component_filter = _filter_component_results(
            query_text=query_text,
            results=tier1_results_raw,
        )
        logger.info(
            "RAG component filter: query=%s component_query=%s before=%s after=%s excluded_assembly=%s heading_matched=%s focus_terms=%s",
            query_text,
            component_filter["component_query"],
            component_filter["before_count"],
            component_filter["after_count"],
            component_filter["excluded_assembly"],
            component_filter["heading_matched"],
            component_filter["focus_terms"],
        )

        if tier1_results:
            return tier1_results, {
                "recipe_query": True,
                "component_query": component_filter["component_query"],
                "routing_mode": "tier1_recipe_only",
                "tier1_hits": len(tier1_results),
                "tier3_hits": 0,
                "tier1_raw_hits": len(tier1_results_raw),
                "component_focus_terms": component_filter["focus_terms"],
                "component_excluded_assembly": component_filter["excluded_assembly"],
                "component_heading_matched": component_filter["heading_matched"],
            }

        if component_query and tier1_results_raw:
            return [], {
                "recipe_query": True,
                "component_query": True,
                "routing_mode": "tier1_component_filtered_zero",
                "tier1_hits": 0,
                "tier3_hits": 0,
                "tier1_raw_hits": len(tier1_results_raw),
                "component_focus_terms": component_filter["focus_terms"],
                "component_excluded_assembly": component_filter["excluded_assembly"],
                "component_heading_matched": component_filter["heading_matched"],
            }

        tier3_results = rag_engine.search(
            query_text=query_text,
            n_results=top_k,
            source_tiers=[TIER_3_REFERENCE_THEORY],
        )
        return tier3_results, {
            "recipe_query": True,
            "component_query": component_query,
            "routing_mode": "tier3_reference_fallback",
            "tier1_hits": 0,
            "tier3_hits": len(tier3_results),
            "tier1_raw_hits": len(tier1_results_raw),
            "component_focus_terms": component_filter["focus_terms"],
            "component_excluded_assembly": component_filter["excluded_assembly"],
            "component_heading_matched": component_filter["heading_matched"],
        }

    all_results = rag_engine.search(query_text=query_text, n_results=top_k)
    return all_results, {
        "recipe_query": False,
        "component_query": component_query,
        "routing_mode": "default_all_tiers",
        "tier1_hits": 0,
        "tier3_hits": 0,
        "tier1_raw_hits": 0,
        "component_focus_terms": [],
        "component_excluded_assembly": 0,
        "component_heading_matched": 0,
    }


def _build_rag_reference_context(query_text: str, config: Dict[str, Any]) -> str:
    top_k = int(config.get("rag", {}).get("top_k", 3))

    from services.rag import rag_engine

    logger.info("RAG query string: %s", query_text)

    results, routing = _retrieve_rag_results(query_text=query_text, top_k=top_k)
    retrieved_count = len(results)

    top_preview = _truncate(results[0]["content"], limit=220) if results else "NONE"

    logger.info("RAG retrieved chunk count: %s", retrieved_count)
    logger.info("RAG top chunk preview: %s", top_preview)
    logger.info(
        "RAG routing: recipe_query=%s component_query=%s mode=%s tier1_hits=%s tier3_hits=%s tier1_raw_hits=%s",
        routing["recipe_query"],
        routing["component_query"],
        routing["routing_mode"],
        routing["tier1_hits"],
        routing["tier3_hits"],
        routing["tier1_raw_hits"],
    )
    logger.info(
        "RAG component routing details: focus_terms=%s excluded_assembly=%s heading_matched=%s",
        routing["component_focus_terms"],
        routing["component_excluded_assembly"],
        routing["component_heading_matched"],
    )

    source_snapshot = _source_snapshot(rag_engine, limit=5)

    context_lines: List[str] = [
        "REFERENCE CONTEXT (RAG)",
        f"QUERY: {query_text}",
        f"RETRIEVED_CHUNK_COUNT: {retrieved_count}",
        f"TOP_CHUNK_PREVIEW: {top_preview}",
        f"RECIPE_QUERY_DETECTED: {str(routing['recipe_query']).lower()}",
        f"COMPONENT_QUERY_DETECTED: {str(routing['component_query']).lower()}",
        f"RAG_ROUTING_MODE: {routing['routing_mode']}",
        f"TIER1_RECIPE_HITS: {routing['tier1_hits']}",
        f"TIER1_RECIPE_RAW_HITS: {routing['tier1_raw_hits']}",
        f"TIER3_REFERENCE_HITS: {routing['tier3_hits']}",
        f"COMPONENT_FOCUS_TERMS: {', '.join(routing['component_focus_terms']) if routing['component_focus_terms'] else 'NONE'}",
        f"COMPONENT_ASSEMBLY_EXCLUDED: {routing['component_excluded_assembly']}",
        f"COMPONENT_HEADING_MATCHED: {routing['component_heading_matched']}",
        "",
        "ACTIVE SOURCE SNAPSHOT (MOST RECENT FIRST):",
        source_snapshot,
        "",
        "INSTRUCTIONS:",
        "- Use RETRIEVED CHUNKS as the highest-priority grounding context.",
        "- If RETRIEVED_CHUNK_COUNT is 0, explicitly say no direct RAG chunks matched this query.",
        "- If user asks about the last ingested document, use ACTIVE SOURCE SNAPSHOT metadata.",
        "- Prefer specific claims/mechanisms from retrieved chunks over generic explanations.",
        "- If chunks do not explicitly answer the question, say that clearly.",
        "- Attribute claims to books/authors only when supported by retrieved chunks.",
        "- For author/book/document questions, interpret retrieved source context instead of generic prior knowledge.",
        '- If support is missing, state exactly: "The retrieved material does not explicitly address this."',
        "- Synthesize across chunks carefully; do not generalize beyond retrieved evidence.",
        "- Avoid vague filler phrasing and avoid repeating the same idea across sections.",
    ]

    if routing["recipe_query"] and routing["tier1_hits"] > 0:
        context_lines.extend(
            [
                "- For dish/recipe questions, Tier 1 recipe sources are the sole recipe authority.",
                "- Do not inject or infer recipe steps, quantities, or substitutions from reference material.",
                "- Output order for component/recipe requests: base recipe first (ingredients + quantities).",
                "- For component requests, include all listed ingredients from retrieved chunks (do not omit xanthan/Xgum when present).",
                "- Exclude plating ratios and dish assembly unless user explicitly asks for them.",
                "- Prefer chunks whose heading or nearby text matches the requested component name.",
                "- Exclude chunks that read like dish assembly or non-base garnish notes.",
                '- If relevant, include at most one line labeled "In service".',
                "- Do not suggest quantity adjustments, do not ask follow-up questions, and do not add filler.",
                '- Template for component answers:\n  "<TITLE>\nBase recipe\n• ingredients...\nMethod\n• Blend, then shear in xanthan."',
            ]
        )
    elif routing["recipe_query"] and routing["tier1_hits"] == 0:
        context_lines.extend(
            [
                "- Tier 1 recipe sources returned no matching chunks for this query.",
                '- For missing recipe specifics, say exactly: "The retrieved material does not explicitly address this."',
                "- Reference chunks may explain principles only; they are not recipe instructions.",
            ]
        )

    if results:
        context_lines.extend(["", "RETRIEVED CHUNKS:"])
        for idx, chunk in enumerate(results, start=1):
            source = chunk.get("source", "unknown")
            heading = chunk.get("heading", "")
            distance = chunk.get("distance", 0)
            tier = chunk.get("knowledge_tier", "unknown")
            content = _truncate(chunk.get("content", ""), limit=1200)
            context_lines.append(
                f"[{idx}] source={source} | tier={tier} | heading={heading} | distance={distance}\n{content}"
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
    base_system_prompt = config.get("system_prompt", "You are a helpful assistant.")
    system_prompt = (
        f"{base_system_prompt.rstrip()}\n\n"
        f"{CHEF_CARD_STYLE_APPENDIX}\n\n"
        f"{GROUNDING_RULE_APPENDIX}\n\n"
        f"{ANTI_REPETITION_RULE_APPENDIX}\n\n"
        f"{SOURCE_GROUNDED_AUTHOR_RULE_APPENDIX}\n\n"
        f"{RECIPE_AUTHORITY_RULE_APPENDIX}\n\n"
        f"{COMPONENT_FIRST_RULE_APPENDIX}"
    ).strip()

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
