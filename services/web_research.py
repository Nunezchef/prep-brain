import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; PrepBrainResearchBot/1.0; +https://example.invalid/prep-brain)"
)


def _normalize_url(raw_url: str) -> str:
    candidate = (raw_url or "").strip()
    if not candidate:
        return ""

    parsed = urlparse(candidate)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        query = parse_qs(parsed.query)
        target = query.get("uddg", [""])[0]
        if target:
            return unquote(target)
    return candidate


def _domain_allowed(url: str, allowed_domains: List[str]) -> bool:
    if not allowed_domains:
        return True

    hostname = (urlparse(url).hostname or "").lower()
    if not hostname:
        return False

    for domain in allowed_domains:
        d = str(domain or "").strip().lower()
        if not d:
            continue
        if hostname == d or hostname.endswith(f".{d}"):
            return True
    return False


class WebResearchClient:
    def __init__(
        self,
        *,
        enabled: bool,
        mode: str,
        rate_limit_rps: float,
        max_pages_per_task: int,
        allowed_domains: Optional[List[str]] = None,
        timeout_seconds: int = 12,
    ):
        self.enabled = bool(enabled)
        self.mode = str(mode or "research_only").strip().lower()
        self.rate_limit_rps = max(float(rate_limit_rps or 0.5), 0.05)
        self.max_pages_per_task = max(int(max_pages_per_task or 3), 1)
        self.allowed_domains = [str(d).strip().lower() for d in (allowed_domains or []) if str(d).strip()]
        self.timeout_seconds = int(timeout_seconds)
        self._last_request_ts = 0.0
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    def _wait_for_rate_limit(self) -> None:
        min_interval = 1.0 / self.rate_limit_rps
        elapsed = time.time() - self._last_request_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_ts = time.time()

    def search_duckduckgo(self, query: str, max_results: int = 8) -> List[Dict[str, str]]:
        if not self.enabled or self.mode != "research_only":
            return []

        self._wait_for_rate_limit()
        response = self._session.get(
            "https://duckduckgo.com/html/",
            params={"q": query},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        results: List[Dict[str, str]] = []

        for anchor in soup.select("a.result__a"):
            href = _normalize_url(anchor.get("href", ""))
            if not href or not href.startswith(("http://", "https://")):
                continue
            if not _domain_allowed(href, self.allowed_domains):
                continue

            parent = anchor.find_parent("div", class_="result")
            snippet = ""
            if parent:
                snippet_tag = parent.select_one(".result__snippet")
                if snippet_tag:
                    snippet = " ".join(snippet_tag.get_text(" ", strip=True).split())

            results.append(
                {
                    "title": " ".join(anchor.get_text(" ", strip=True).split()),
                    "url": href,
                    "snippet": snippet[:600],
                }
            )
            if len(results) >= max_results:
                break

        return results

    def fetch_page_text(self, url: str, max_chars: int = 7000) -> str:
        self._wait_for_rate_limit()
        response = self._session.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type.lower():
            return ""

        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        text = " ".join(soup.get_text(" ", strip=True).split())
        return text[:max_chars]

    def research(self, query: str, max_results: int = 8) -> List[Dict[str, str]]:
        search_results = self.search_duckduckgo(query=query, max_results=max_results)
        if not search_results:
            return []

        sources: List[Dict[str, str]] = []
        for entry in search_results[: self.max_pages_per_task]:
            url = entry["url"]
            try:
                page_text = self.fetch_page_text(url)
            except Exception as exc:
                logger.debug("WebResearch fetch failed for %s: %s", url, exc)
                page_text = ""

            sources.append(
                {
                    "title": entry["title"],
                    "url": url,
                    "snippet": entry.get("snippet", ""),
                    "text": page_text,
                    "domain": (urlparse(url).hostname or "").lower(),
                }
            )
        return sources

    def extract_price_range_conservative(
        self,
        *,
        item_name: str,
        unit: Optional[str],
        sources: List[Dict[str, str]],
    ) -> Optional[Dict[str, Any]]:
        if not sources:
            return None

        price_re = re.compile(r"\$\s?(\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?)")
        values: List[float] = []

        for source in sources:
            haystack = " ".join(
                [
                    source.get("snippet", ""),
                    source.get("text", "")[:2000],
                ]
            )
            for match in price_re.findall(haystack):
                try:
                    value = float(match.replace(",", ""))
                except ValueError:
                    continue
                if 0.05 <= value <= 10000:
                    values.append(value)

        if not values:
            return None

        values.sort()
        if len(values) == 1:
            low = max(values[0] * 0.85, 0.01)
            high = values[0] * 1.15
        else:
            low_idx = max(int((len(values) - 1) * 0.25), 0)
            high_idx = min(int((len(values) - 1) * 0.75), len(values) - 1)
            low = values[low_idx]
            high = values[high_idx]
            if high < low:
                low, high = high, low

        urls = [source.get("url", "") for source in sources if source.get("url")]
        return {
            "item_name": item_name,
            "low_price": round(float(low), 2),
            "high_price": round(float(high), 2),
            "unit": (unit or "unit").strip() or "unit",
            "source_urls": urls,
            "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "knowledge_tier": "general_knowledge_web",
        }

    def research_price_estimate(
        self,
        *,
        item_name: str,
        unit: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        query = f"{item_name} price per {unit or 'unit'}"
        sources = self.research(query=query, max_results=max(self.max_pages_per_task * 2, 6))
        estimate = self.extract_price_range_conservative(item_name=item_name, unit=unit, sources=sources)
        if not estimate:
            return None
        estimate["source_urls_json"] = json.dumps(estimate["source_urls"])
        return estimate
