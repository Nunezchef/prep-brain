from services.web_research import WebResearchClient


def test_extract_price_range_conservative():
    client = WebResearchClient(
        enabled=True,
        mode="research_only",
        rate_limit_rps=5,
        max_pages_per_task=2,
    )
    sources = [
        {"url": "https://example.com/a", "snippet": "Current market: $2.10 to $2.90", "text": "", "title": "A"},
        {"url": "https://example.com/b", "snippet": "", "text": "Wholesale around $2.40 and retail $3.20", "title": "B"},
    ]

    estimate = client.extract_price_range_conservative(
        item_name="Tomatoes",
        unit="lb",
        sources=sources,
    )

    assert estimate is not None
    assert estimate["low_price"] <= estimate["high_price"]
    assert estimate["knowledge_tier"] == "general_knowledge_web"
    assert estimate["unit"] == "lb"


def test_research_price_estimate_uses_research_only_mode(monkeypatch):
    client = WebResearchClient(
        enabled=True,
        mode="research_only",
        rate_limit_rps=5,
        max_pages_per_task=2,
    )

    monkeypatch.setattr(
        client,
        "research",
        lambda query, max_results: [
            {
                "url": "https://example.com/price",
                "title": "Price page",
                "snippet": "$4.50 per lb",
                "text": "Historical average $5.00",
                "domain": "example.com",
            }
        ],
    )

    estimate = client.research_price_estimate(item_name="Onion", unit="lb")
    assert estimate is not None
    assert estimate["low_price"] > 0
    assert "source_urls_json" in estimate
