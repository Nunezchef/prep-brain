import re

from services import brain


def _routing_stub() -> dict:
    return {
        "recipe_query": False,
        "component_query": False,
        "routing_mode": "default_all_tiers",
        "tier1_hits": 0,
        "tier3_hits": 0,
        "tier1_raw_hits": 0,
        "component_focus_terms": [],
        "component_excluded_assembly": 0,
        "component_heading_matched": 0,
    }


def test_citation_request_without_context_refuses_quotes(monkeypatch):
    monkeypatch.setattr(
        brain,
        "load_config",
        lambda: {
            "rag": {"enabled": True, "top_k": 3},
            "ollama": {"base_url": "http://localhost:11434", "model": "gpt-oss:20b"},
            "system_prompt": "test",
            "response_style": "concise",
        },
    )
    monkeypatch.setattr(brain, "_retrieve_rag_results", lambda query_text, top_k: ([], _routing_stub()))
    monkeypatch.setattr(
        brain.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("LLM should not be called for citation-only responses")
        ),
    )

    answer = brain.chat([("user", "cite something from it")])
    assert "No relevant passage found in current sources for that request." in answer
    assert "Not in my sources yet." in answer


def test_citation_request_with_context_quotes_from_chunks(monkeypatch):
    chunk_text = (
        "Salt helps proteins retain moisture during cooking. "
        "Early seasoning improves diffusion and more even flavor."
    )
    results = [
        {
            "content": chunk_text,
            "source": "On Food and Cooking.pdf",
            "source_title": "On Food and Cooking",
            "chunk_id": 12,
        }
    ]

    monkeypatch.setattr(
        brain,
        "load_config",
        lambda: {
            "rag": {"enabled": True, "top_k": 3},
            "ollama": {"base_url": "http://localhost:11434", "model": "gpt-oss:20b"},
            "system_prompt": "test",
            "response_style": "concise",
        },
    )
    monkeypatch.setattr(
        brain,
        "_retrieve_rag_results",
        lambda query_text, top_k: (results, _routing_stub()),
    )
    monkeypatch.setattr(
        brain.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("LLM should not be called for citation-only responses")
        ),
    )

    answer = brain.chat([("user", "cite something from On Food and Cooking")])
    quotes = re.findall(r'"([^"]+)"', answer)
    assert 1 <= len(quotes) <= 2

    normalized_chunk = " ".join(chunk_text.split())
    for quote in quotes:
        assert quote in normalized_chunk
        assert len(quote.split()) <= 25
    assert "On Food and Cooking" in answer
