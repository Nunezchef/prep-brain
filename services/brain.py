import html
import logging
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import time

from prep_brain.config import load_config
from prep_brain.logging import correlation_context, get_correlation_id
from services.retry import retry_with_backoff
from services import metrics

logger = logging.getLogger(__name__)

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

CITATION_QUERY_RE = re.compile(r"\b(cite|citation|quote|quoted|verbatim|source line)\b", re.I)
CHUNK_REF_RE = re.compile(r"\(C(\d+)\)")

STYLE_APPENDIX_BY_NAME: Dict[str, str] = {
    "concise": (
        "Response style (concise):\n"
        "- Keep answers to 3-6 lines.\n"
        "- No repeated headers.\n"
        "- No filler, no marketing language.\n"
        "- Give direct kitchen-usable output."
    ),
    "chef_card": (
        "Response style (chef_card):\n"
        "- Keep answers compact and practical.\n"
        "- Use at most one short header only when it adds clarity.\n"
        "- Use short bullets for procedures or lists.\n"
        "- Avoid repeated sections and avoid template phrasing."
    ),
    "explain": (
        "Response style (explain):\n"
        "- Provide slightly more detail while staying concise.\n"
        "- Keep structure simple and avoid repetitive sections.\n"
        "- Focus on operational implications."
    ),
}

GROUNDING_APPENDIX = (
    "Grounding and citation rules:\n"
    "- Use only the provided CONTEXT blocks as evidence when present.\n"
    "- Cite context using chunk IDs like (C1).\n"
    "- Never invent citations or source names.\n"
    "- If context is missing, say exactly: Not in my sources yet.\n"
    "- For house recipes, never summarize away ingredient lines."
)


def _truncate(text: str, limit: int = 220) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit]}..."


def _query_log_preview(query_text: str, limit: int = 240) -> str:
    preview = _truncate(query_text or "", limit=limit)
    length = len(str(query_text or ""))
    return f"{preview} [len={length}]"


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


def _filter_component_results(
    query_text: str, results: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
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

    return ordered, {
        "component_query": True,
        "focus_terms": focus_terms,
        "before_count": len(results),
        "after_count": len(ordered),
        "excluded_assembly": excluded_assembly,
        "heading_matched": heading_matched,
    }


def _is_recipe_query(query_text: str) -> bool:
    text = (query_text or "").strip().lower()
    if not text:
        return False

    for pattern in RECIPE_QUERY_PATTERNS:
        if re.search(pattern, text):
            return True

    return any(f" {term} " in f" {text} " for term in RECIPE_COMPONENT_TERMS)


def _is_citation_request(query_text: str) -> bool:
    return bool(CITATION_QUERY_RE.search((query_text or "").strip()))


def _resolve_response_style(
    config: Dict[str, Any],
    *,
    response_style: Optional[str],
    mode: Optional[str],
) -> str:
    style = (response_style or str(config.get("response_style", "concise"))).strip().lower()
    mode_value = (mode or "").strip().lower()
    if mode_value == "service":
        style = "concise"
    elif mode_value == "admin" and style == "concise":
        style = "chef_card"
    if style not in STYLE_APPENDIX_BY_NAME:
        style = "concise"
    return style


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


def _format_context_entries(results: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for idx, chunk in enumerate(results, start=1):
        source_name = str(chunk.get("source_title") or chunk.get("source") or "unknown").strip()
        page = chunk.get("page") or chunk.get("page_number") or chunk.get("page_index")
        page_label = str(page).strip() if page not in (None, "", 0, "0") else ""
        raw_text = str(chunk.get("content") or "").strip()
        compact_text = " ".join(raw_text.split())
        compact_text = compact_text.replace('"', '\\"')
        compact_text = _truncate(compact_text, limit=1200)
        entries.append(
            {
                "cid": f"C{idx}",
                "source": source_name,
                "page": page_label,
                "text": compact_text,
                "raw_text": raw_text,
                "chunk_id": chunk.get("chunk_id", idx - 1),
            }
        )
    return entries


def _build_rag_reference_context(
    query_text: str, config: Dict[str, Any]
) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
    top_k = int(config.get("rag", {}).get("top_k", 3))

    logger.info("RAG query string: %s", _query_log_preview(query_text))
    results, routing = _retrieve_rag_results(query_text=query_text, top_k=top_k)
    context_entries = _format_context_entries(results)

    logger.info(
        "RAG routing: recipe_query=%s component_query=%s mode=%s hits=%s",
        routing["recipe_query"],
        routing["component_query"],
        routing["routing_mode"],
        len(context_entries),
    )

    lines: List[str] = [
        "CONTEXT (RAG)",
        f"QUERY: {query_text}",
        f"RETRIEVED_CHUNK_COUNT: {len(context_entries)}",
        "If you use context evidence, cite chunk IDs as (C#).",
        "Do not cite anything outside these chunks.",
        "",
    ]
    if context_entries:
        for entry in context_entries:
            page_part = f", page={entry['page']}" if entry["page"] else ""
            lines.append(
                f"[{entry['cid']}] source={entry['source']}{page_part}, "
                f"chunk_id={entry['chunk_id']}, text=\"{entry['text']}\""
            )
    else:
        lines.append("NO_CONTEXT=true")

    return "\n".join(lines), context_entries, routing


def _normalize_quote_candidates(raw_text: str) -> List[str]:
    compact = " ".join((raw_text or "").split()).strip()
    if not compact:
        return []

    candidates: List[str] = []
    for sentence in re.split(r"(?<=[.!?])\s+", compact):
        value = sentence.strip(" \"'")
        if not value:
            continue
        words = value.split()
        if 4 <= len(words) <= 25:
            candidates.append(value)

    if not candidates:
        snippet = " ".join(compact.split()[:25]).strip()
        if snippet:
            candidates.append(snippet)

    unique: List[str] = []
    seen = set()
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _citation_refusal_response() -> str:
    return (
        "No relevant passage found in current sources for that request.\n"
        "Not in my sources yet.\n"
        "Try broadening the query or reingest/OCR the source."
    )


def _build_grounded_quote_response(query_text: str, context_entries: Sequence[Dict[str, Any]]) -> str:
    _ = query_text
    if not context_entries:
        return _citation_refusal_response()

    lines: List[str] = []
    quote_count = 0
    for entry in context_entries:
        if quote_count >= 2:
            break
        candidates = _normalize_quote_candidates(str(entry.get("raw_text") or entry.get("text") or ""))
        if not candidates:
            continue
        quote = candidates[0]
        source_label = str(entry.get("source") or "unknown")
        page = str(entry.get("page") or "").strip()
        source_meta = f"{source_label}, p. {page}" if page else source_label
        lines.append(f'<i>"{html.escape(quote)}"</i>')
        lines.append(f"{html.escape(source_meta)} ({entry.get('cid', 'C?')})")
        quote_count += 1

    if quote_count == 0:
        return _citation_refusal_response()

    lines.append("")
    lines.append("So what: Use this as reference context, then execute against house specs and station needs.")
    return "\n".join(lines).strip()


def _strip_invalid_chunk_refs(answer: str, allowed_chunk_ids: Sequence[str]) -> str:
    allowed = {value.upper() for value in allowed_chunk_ids}

    def _replace(match: re.Match[str]) -> str:
        value = f"C{match.group(1)}".upper()
        return f"({value})" if value in allowed else ""

    cleaned = CHUNK_REF_RE.sub(_replace, answer or "")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = re.sub(r"\s+\n", "\n", cleaned)
    return cleaned.strip()


def chat(
    messages: List[Tuple[str, str]],
    *,
    response_style: Optional[str] = None,
    mode: Optional[str] = None,
) -> str:
    config = load_config()

    ollama_url = os.environ.get("OLLAMA_URL") or config.get("ollama", {}).get(
        "base_url", "http://localhost:11434"
    )
    model = config.get("ollama", {}).get("model", "gpt-oss:20b")
    rag_enabled = bool(config.get("rag", {}).get("enabled", False))
    base_system_prompt = config.get("system_prompt", "You are a helpful assistant.")
    style_name = _resolve_response_style(config, response_style=response_style, mode=mode)
    style_block = STYLE_APPENDIX_BY_NAME.get(style_name, STYLE_APPENDIX_BY_NAME["concise"])
    system_prompt = f"{base_system_prompt.rstrip()}\n\n{style_block}\n\n{GROUNDING_APPENDIX}".strip()

    last_user_message: Optional[Tuple[str, str]] = next(
        (m for m in reversed(messages) if m[0] == "user"), None
    )
    query_text = (last_user_message[1].strip() if last_user_message else "") if messages else ""

    if rag_enabled and query_text and _is_recipe_query(query_text):
        recipe_cfg = (
            config.get("rag", {}).get("recipes", {})
            if isinstance(config.get("rag", {}), dict)
            else {}
        )
        recipe_conf_threshold = float(recipe_cfg.get("completeness_confidence_threshold", 0.75))
        try:
            from services.rag import rag_engine

            assembled = rag_engine.assemble_house_recipe(
                query_text=query_text,
                n_results=max(8, int(config.get("rag", {}).get("top_k", 3)) * 3),
                confidence_threshold=recipe_conf_threshold,
            )
            status = str(assembled.get("status") or "").lower()
            logger.info(
                "House recipe assembly: status=%s query=%s source=%s recipe=%s chunks_used=%s sections=%s missing=%s confidence=%.3f",
                status,
                query_text,
                assembled.get("source_name", ""),
                assembled.get("matched_recipe_name", ""),
                assembled.get("chunks_used", 0),
                assembled.get("sections_detected", []),
                assembled.get("missing_sections", []),
                float(assembled.get("confidence", 0.0)),
            )
            if status == "ok" and assembled.get("html"):
                return str(assembled["html"])
            if status == "incomplete":
                return "Recipe found but incomplete in source. Use /recipe <id> to review."
        except Exception:
            logger.exception("House recipe assembly failed for query=%s", query_text)

    payload_messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    context_entries: List[Dict[str, Any]] = []

    if rag_enabled and query_text:
        try:
            rag_reference, context_entries, _ = _build_rag_reference_context(
                query_text=query_text,
                config=config,
            )
            payload_messages.append({"role": "system", "content": rag_reference})
        except Exception as exc:
            logger.exception("RAG integration error")
            payload_messages.append(
                {
                    "role": "system",
                    "content": (
                        "CONTEXT (RAG)\n"
                        f"QUERY: {query_text}\n"
                        "RETRIEVED_CHUNK_COUNT: 0\n"
                        f"RAG_ERROR: {exc}\n"
                        "NO_CONTEXT=true"
                    ),
                }
            )
            context_entries = []

    if _is_citation_request(query_text):
        return _build_grounded_quote_response(query_text=query_text, context_entries=context_entries)

    if rag_enabled and query_text and not context_entries:
        payload_messages.append(
            {
                "role": "system",
                "content": "If no relevant context exists, answer with: Not in my sources yet.",
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

    @retry_with_backoff(
        max_retries=3,
        base_delay=2.0,
        max_delay=30.0,
        retry_on=(requests.exceptions.ConnectionError, requests.exceptions.Timeout, OSError),
    )
    def _call_ollama() -> str:
        cid = get_correlation_id()
        logger.debug(f"[{cid}] Calling Ollama model={model}")
        response = requests.post(f"{ollama_url}/api/chat", json=payload, timeout=180)
        response.raise_for_status()
        data = response.json()
        return (data.get("message") or {}).get("content", "").strip() or "(No response.)"

    start_ts = time.time()
    success = False
    try:
        answer = _call_ollama()
        success = True
        if context_entries:
            allowed = [str(item.get("cid") or "").upper() for item in context_entries]
            answer = _strip_invalid_chunk_refs(answer, allowed)
        return answer
    except Exception as exc:
        logger.error("Error connecting to Brain after retries: %s", exc)
        return f"Error connecting to Brain: {exc}"
    finally:
        duration_ms = (time.time() - start_ts) * 1000
        metrics.record_llm_call(model=model, duration_ms=duration_ms, success=success)
