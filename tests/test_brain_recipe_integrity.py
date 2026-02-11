import sys
import types

from services import brain


def _install_stub_rag(monkeypatch, result_payload):
    rag_stub = types.ModuleType("services.rag")
    rag_stub.TIER_1_RECIPE_OPS = "tier1_recipe_ops"
    rag_stub.TIER_3_REFERENCE_THEORY = "tier3_reference_theory"

    class _Engine:
        def assemble_house_recipe(self, **kwargs):
            return dict(result_payload)

        def search(self, **kwargs):
            return []

        def get_sources(self):
            return []

    rag_stub.rag_engine = _Engine()
    monkeypatch.setitem(sys.modules, "services.rag", rag_stub)


def test_chat_uses_full_house_recipe_assembly_without_llm(monkeypatch):
    _install_stub_rag(
        monkeypatch,
        {
            "status": "ok",
            "html": (
                "<b>Cucumber Spice</b>\n"
                "<i>Source: Fire Recipes 2024</i>\n\n"
                "<b>Base</b>\n"
                "• 10 g Black pepper\n\n"
                "<b>Grind and add</b>\n"
                "• 5 g Ground green cardamom\n\n"
                "<b>Method</b>\n"
                "Grind and add."
            ),
            "source_name": "FIRE recipes 2024.docx",
            "matched_recipe_name": "Cucumber Spice",
            "chunks_used": 2,
            "sections_detected": ["Base", "Grind and add", "Method"],
            "missing_sections": [],
            "confidence": 0.92,
        },
    )
    monkeypatch.setattr(
        brain,
        "load_config",
        lambda: {
            "rag": {"enabled": True, "top_k": 3},
            "ollama": {"base_url": "http://localhost:11434", "model": "gpt-oss:20b"},
            "system_prompt": "test",
        },
    )

    def _unexpected_post(*args, **kwargs):
        raise AssertionError("LLM should not be called for complete house recipe assembly")

    monkeypatch.setattr(brain.requests, "post", _unexpected_post)

    answer = brain.chat([("user", "What is the recipe for cucumber spice?")])
    assert "<b>Cucumber Spice</b>" in answer
    assert "<b>Grind and add</b>" in answer
    assert "<b>Method</b>" in answer


def test_chat_refuses_partial_house_recipe_without_llm(monkeypatch):
    _install_stub_rag(
        monkeypatch,
        {
            "status": "incomplete",
            "source_name": "FIRE recipes 2024.docx",
            "matched_recipe_name": "Cucumber Spice",
            "chunks_used": 1,
            "sections_detected": ["Base"],
            "missing_sections": ["method"],
            "confidence": 0.67,
        },
    )
    monkeypatch.setattr(
        brain,
        "load_config",
        lambda: {
            "rag": {"enabled": True, "top_k": 3},
            "ollama": {"base_url": "http://localhost:11434", "model": "gpt-oss:20b"},
            "system_prompt": "test",
        },
    )

    def _unexpected_post(*args, **kwargs):
        raise AssertionError("LLM should not be called for incomplete house recipe assembly")

    monkeypatch.setattr(brain.requests, "post", _unexpected_post)

    answer = brain.chat([("user", "What is the recipe for cucumber spice?")])
    assert answer == "Recipe found but incomplete in source. Use /recipe <id> to review."
