import base64
import datetime
import html
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import uuid
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import chromadb
import fitz  # pymupdf
import requests
from chromadb.utils import embedding_functions
from sentence_transformers import SentenceTransformer

from services import memory
from services.command_runner import CommandRunner
from services.doc_extract import detect_images_in_docx, extract_docx_images, extract_text
from prep_brain.config import load_config, resolve_path

# Set up logging
logger = logging.getLogger(__name__)

# Constants
PERSIST_DIRECTORY = str(resolve_path("data/chroma_db"))
COLLECTION_NAME = "prep_brain_knowledge"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
SOURCES_FILE = str(resolve_path("data/sources.json"))

TIER_1_RECIPE_OPS = "tier1_recipe_ops"
TIER_2_NOTES_SOPS = "tier2_notes_sops"
TIER_3_REFERENCE_THEORY = "tier3_reference_theory"

TIER_ALIASES = {
    "tier1": TIER_1_RECIPE_OPS,
    "tier1_recipe_ops": TIER_1_RECIPE_OPS,
    "recipe": TIER_1_RECIPE_OPS,
    "recipes": TIER_1_RECIPE_OPS,
    "recipe_ops": TIER_1_RECIPE_OPS,
    "restaurant_recipe": TIER_1_RECIPE_OPS,
    "ops": TIER_1_RECIPE_OPS,
    "operations": TIER_1_RECIPE_OPS,
    "tier2": TIER_2_NOTES_SOPS,
    "tier2_notes_sops": TIER_2_NOTES_SOPS,
    "notes": TIER_2_NOTES_SOPS,
    "note": TIER_2_NOTES_SOPS,
    "sop": TIER_2_NOTES_SOPS,
    "sops": TIER_2_NOTES_SOPS,
    "tier3": TIER_3_REFERENCE_THEORY,
    "tier3_reference_theory": TIER_3_REFERENCE_THEORY,
    "reference": TIER_3_REFERENCE_THEORY,
    "references": TIER_3_REFERENCE_THEORY,
    "book": TIER_3_REFERENCE_THEORY,
    "theory": TIER_3_REFERENCE_THEORY,
    "science": TIER_3_REFERENCE_THEORY,
}

REFERENCE_KEYWORDS = [
    "mcgee",
    "on food and cooking",
    "flavor bible",
    "reference",
    "textbook",
    "food science",
    "theory",
    "chemistry",
]

NOTES_SOP_KEYWORDS = [
    "note",
    "notes",
    "shift",
    "post-service",
    "service notes",
    "debrief",
    "sop",
    "standard operating",
]

RECIPE_OPS_KEYWORDS = [
    "recipe",
    "prep",
    "station",
    "menu",
    "dish",
    "line build",
    "plating",
    "sauce",
    "vinaigrette",
    "custard",
    "glaze",
    "ops",
    "operations",
]

HOUSE_RECIPE_DOC_KEYWORDS = [
    "recipe book",
    "house recipe",
    "fire recipe",
    "prep recipe",
    "line recipe",
    "dish book",
]

VENDOR_DOC_KEYWORDS = [
    "vendor",
    "vendors",
    "price list",
    "catalog",
    "invoice",
    "order guide",
    "supplier",
]

SOP_DOC_KEYWORDS = [
    "sop",
    "standard operating procedure",
    "standard operating",
    "policy",
    "procedure",
]

PREP_NOTE_KEYWORDS = [
    "prep notes",
    "prep list",
    "prep sheet",
    "production notes",
    "mise",
]

DEFAULT_RAG_SETTINGS: Dict[str, Any] = {
    "ocr": {
        "enabled": True,
        "tool": "ocrmypdf",
        "image_page_ratio_threshold": 0.6,
        "min_text_chars_per_page": 300,
        "low_text_char_threshold": 500,
    },
    "image_processing": {
        "extract_images": False,
        "max_images": 30,
    },
    "vision": {
        "enabled": False,
        "model": "",
        "max_images": 8,
        "prompt": (
            "Describe the culinary or operationally relevant information in this image. "
            "Focus on ingredients, measurements, steps, labels, tables, and constraints."
        ),
    },
    "chunking": {
        "chunk_size_chars": 3500,
        "chunk_overlap_chars": 400,
        "minimum_chunk_chars": 400,
        "dedupe_enabled": True,
    },
    "docx": {
        "image_only_text_threshold": 5000,
    },
}

RECIPE_QUERY_PREFIX_RE = re.compile(
    r"(?i)\b(?:what(?:'s| is)?|show|give|send|need|recipe|for|the|our|please|how|to|make|of)\b"
)
NON_RECIPE_TITLE_KEYS = {
    "recipes book",
    "recipe book",
    "ratio",
    "method",
    "ingredients",
    "base",
    "general",
}
SECTION_NAME_ALIASES = {
    "ingredient": "Ingredients",
    "ingredients": "Ingredients",
    "base": "Base",
    "base recipe": "Base",
    "method": "Method",
    "instructions": "Method",
    "grind and add": "Grind and add",
    "finish": "Finish",
    "for service": "For service",
    "part 1": "Part 1",
    "part 2": "Part 2",
    "part 3": "Part 3",
}
METHOD_SECTION_KEYS = {"method", "instructions"}
ACTION_SECTION_KEYS = {"grind and add", "finish", "for service"}
INGREDIENT_LINE_RE = re.compile(r"^\s*(?:[-•]\s*)?(?:\d+(?:\.\d+)?)\s*(?:[a-zA-Z#%]{1,12})\b")
STEP_LINE_RE = re.compile(r"^\s*\d+[\.)]\s+")


def _normalize_key(text: str) -> str:
    lowered = str(text or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return " ".join(cleaned.split()).strip()


def _normalize_recipe_query_target(query_text: str) -> str:
    cleaned = RECIPE_QUERY_PREFIX_RE.sub(" ", str(query_text or ""))
    cleaned = re.sub(r"[\"'`]+", " ", cleaned)
    cleaned = re.sub(r"[^a-zA-Z0-9\s-]+", " ", cleaned)
    return " ".join(cleaned.split()).strip()


def _split_table_row_cells(line: str) -> List[str]:
    text = str(line or "").strip()
    if not text.startswith("|") or "|" not in text:
        return [text]
    parts = [part.strip() for part in text.strip("|").split("|")]
    return [part for part in parts if part]


def _is_ingredient_line(line: str) -> bool:
    text = str(line or "").strip()
    if not text:
        return False
    if INGREDIENT_LINE_RE.match(text):
        return True
    compact = text.replace(" ", "")
    return bool(re.match(r"^\d+(?:\.\d+)?#", compact))


def _detect_section_name(line: str) -> Optional[str]:
    text = str(line or "").strip()
    if not text:
        return None
    base = text.rstrip(":").strip()
    norm = _normalize_key(base)
    if not norm:
        return None
    mapped = SECTION_NAME_ALIASES.get(norm)
    if mapped:
        return mapped
    if text.endswith(":") and len(norm.split()) <= 6 and not _is_ingredient_line(text):
        return base
    return None


def _looks_like_recipe_title(line: str) -> bool:
    text = str(line or "").strip()
    if not text:
        return False
    if text.startswith("#") or text.endswith(":") or STEP_LINE_RE.match(text):
        return False
    if _is_ingredient_line(text):
        return False
    if any(ch.isdigit() for ch in text):
        return False
    words = text.split()
    if len(words) == 0 or len(words) > 8:
        return False
    if len(text) < 3 or len(text) > 80:
        return False
    norm = _normalize_key(text)
    if norm in NON_RECIPE_TITLE_KEYS:
        return False
    if not any(ch.isalpha() for ch in text):
        return False
    return True


def _parse_recipe_entries_from_chunk(text: str, chunk_id: int) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    current_recipe: Optional[str] = None
    current_section = "Base"
    local_order = 0

    raw_lines = [line for line in str(text or "").splitlines() if line.strip()]
    for raw in raw_lines:
        expanded = _split_table_row_cells(raw.strip())
        for cell in expanded:
            line = " ".join(cell.split()).strip()
            if not line:
                continue
            if line.startswith("## "):
                continue

            if _looks_like_recipe_title(line):
                current_recipe = line
                current_section = "Base"
                continue

            section = _detect_section_name(line)
            if section and current_recipe:
                current_section = section
                continue

            if not current_recipe:
                continue

            local_order += 1
            kind = "ingredient" if _is_ingredient_line(line) else "method"
            entries.append(
                {
                    "recipe_name": current_recipe,
                    "section_name": current_section,
                    "line": line,
                    "kind": kind,
                    "chunk_id": int(chunk_id),
                    "order_index": local_order,
                }
            )

    return entries


def _title_match_score(target: str, candidate: str) -> float:
    left = _normalize_key(target)
    right = _normalize_key(candidate)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    score = SequenceMatcher(None, left, right).ratio()
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if left_tokens and right_tokens:
        overlap = len(left_tokens & right_tokens) / max(len(left_tokens), 1)
        score = max(score, overlap)
    return float(score)


def _format_house_recipe_html(
    *,
    recipe_name: str,
    source_title: str,
    ingredient_sections: List[Tuple[str, List[str]]],
    method_lines: List[str],
) -> str:
    out: List[str] = [
        f"<b>{html.escape(recipe_name)}</b>",
        f"<i>Source: {html.escape(source_title)}</i>",
        "",
    ]
    for section_name, lines in ingredient_sections:
        out.append(f"<b>{html.escape(section_name)}</b>")
        for line in lines:
            out.append(f"• {html.escape(line)}")
        out.append("")
    out.append("<b>Method</b>")
    out.append(html.escape(" ".join(method_lines).strip()))
    return "\n".join(out).strip()


def normalize_knowledge_tier(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return TIER_ALIASES.get(str(value).strip().lower())


def infer_knowledge_tier(
    source_type: str = "",
    title: str = "",
    source_name: str = "",
    summary: str = "",
) -> str:
    normalized_source_type = normalize_knowledge_tier(source_type)
    if normalized_source_type:
        return normalized_source_type

    haystack = " ".join([source_type, title, source_name, summary]).lower()

    if any(keyword in haystack for keyword in REFERENCE_KEYWORDS):
        return TIER_3_REFERENCE_THEORY
    if any(keyword in haystack for keyword in NOTES_SOP_KEYWORDS):
        return TIER_2_NOTES_SOPS
    if any(keyword in haystack for keyword in RECIPE_OPS_KEYWORDS):
        return TIER_1_RECIPE_OPS

    # Safety default: ambiguous documents are treated as reference.
    return TIER_3_REFERENCE_THEORY


def classify_document_type(
    *,
    title: str = "",
    source_name: str = "",
    summary: str = "",
) -> Tuple[str, str]:
    """Classify uploaded documents for safe ingestion routing."""
    haystack = " ".join([title, source_name, summary]).lower()

    if any(keyword in haystack for keyword in REFERENCE_KEYWORDS):
        return "reference_book", TIER_3_REFERENCE_THEORY
    if any(keyword in haystack for keyword in VENDOR_DOC_KEYWORDS):
        return "vendor_list", TIER_1_RECIPE_OPS
    if any(keyword in haystack for keyword in SOP_DOC_KEYWORDS):
        return "sop", TIER_2_NOTES_SOPS
    if any(keyword in haystack for keyword in PREP_NOTE_KEYWORDS):
        return "prep_notes", TIER_1_RECIPE_OPS
    if any(keyword in haystack for keyword in HOUSE_RECIPE_DOC_KEYWORDS):
        return "house_recipe_book", TIER_1_RECIPE_OPS
    if any(keyword in haystack for keyword in RECIPE_OPS_KEYWORDS):
        return "house_recipe_book", TIER_1_RECIPE_OPS

    # Ambiguous defaults to reference-only handling.
    return "unknown", TIER_3_REFERENCE_THEORY


def load_runtime_config() -> Dict[str, Any]:
    return load_config()


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _map_doc_source_type(source_type: str, knowledge_tier: str) -> str:
    lowered = str(source_type or "").strip().lower()
    if knowledge_tier == TIER_3_REFERENCE_THEORY:
        return "general_knowledge"
    if lowered in {"general_knowledge_web"}:
        return "general_knowledge_web"
    if lowered in {
        "house_recipe_book",
        "house_recipe_document",
        "house_recipe",
        "prep_notes",
        "vendor_list",
    }:
        return "restaurant_recipes"
    if knowledge_tier in {TIER_1_RECIPE_OPS, TIER_2_NOTES_SOPS}:
        return "restaurant_recipes"
    if knowledge_tier == TIER_3_REFERENCE_THEORY:
        return "general_knowledge"
    return "unknown"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iso_now() -> str:
    return datetime.datetime.now().isoformat()


class SmartChunker:
    """
    Intelligent chunking strategy for cooking/reference texts.
    Detects headings (UPPERCASE or Bold) and groups content by section.
    """

    def __init__(self, target_size: int = 1000, overlap: int = 100):
        self.target_size = target_size
        self.overlap = overlap

    def chunk_pdf(self, path: Path) -> List[Dict[str, str]]:
        doc = fitz.open(path)
        chunks: List[Dict[str, str]] = []
        current_chunk: List[str] = []
        current_length = 0
        current_heading = "General"

        try:
            for page in doc:
                blocks = page.get_text("dict").get("blocks", [])
                for block in blocks:
                    if block.get("type") != 0:
                        continue
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            text = (span.get("text") or "").strip()
                            if not text:
                                continue

                            is_heading = (span.get("size", 0) > 14) or (
                                text.isupper() and len(text) < 60
                            )

                            if is_heading:
                                if current_chunk and current_length > 100:
                                    chunks.append(
                                        {
                                            "text": "\n".join(current_chunk),
                                            "heading": current_heading,
                                        }
                                    )
                                    current_chunk = []
                                    current_length = 0

                                current_heading = text
                                current_chunk.append(f"## {text}")
                                current_length += len(text)
                            else:
                                current_chunk.append(text)
                                current_length += len(text)

                                if current_length > self.target_size:
                                    chunks.append(
                                        {
                                            "text": "\n".join(current_chunk),
                                            "heading": current_heading,
                                        }
                                    )
                                    keep_lines = (
                                        current_chunk[-3:] if len(current_chunk) > 3 else []
                                    )
                                    current_chunk = keep_lines
                                    current_length = sum(len(line) for line in keep_lines)

            if current_chunk:
                chunks.append(
                    {
                        "text": "\n".join(current_chunk),
                        "heading": current_heading,
                    }
                )

            return chunks
        finally:
            doc.close()


class RAGEngine:
    def __init__(self):
        Path(PERSIST_DIRECTORY).parent.mkdir(parents=True, exist_ok=True)
        self.ingest_reports_dir = resolve_path("data/ingest_reports")
        self.ingest_reports_dir.mkdir(parents=True, exist_ok=True)
        self.docx_ocr_runner = CommandRunner(allowed_commands={"tesseract"})

        self.chroma_client = chromadb.PersistentClient(
            path=PERSIST_DIRECTORY,
            settings=chromadb.Settings(anonymized_telemetry=False),
        )

        # Keep this initialized so model download failures happen on startup.
        self.embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        self.embedding_func = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=EMBEDDING_MODEL_NAME
        )

        self.collection = self.chroma_client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=self.embedding_func,
        )

        self.sources_file = Path(SOURCES_FILE)
        if not self.sources_file.exists():
            self._save_sources([])

        logger.info("RAG Engine initialized. Collection size: %s", self.collection.count())

    def _persist_doc_source(
        self,
        *,
        ingest_id: str,
        filename: str,
        source_type: str,
        restaurant_tag: Optional[str],
        file_sha256: str,
        file_size: int,
        extracted_text_chars: int,
        chunk_count: int,
        chunks_added: int,
        status: str,
    ) -> None:
        con = memory.get_conn()
        try:
            con.execute(
                """
                INSERT INTO doc_sources (
                    ingest_id, filename, source_type, restaurant_tag, file_sha256, file_size,
                    extracted_text_chars, chunk_count, chunks_added, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ingest_id) DO UPDATE SET
                    filename = excluded.filename,
                    source_type = excluded.source_type,
                    restaurant_tag = excluded.restaurant_tag,
                    file_sha256 = excluded.file_sha256,
                    file_size = excluded.file_size,
                    extracted_text_chars = excluded.extracted_text_chars,
                    chunk_count = excluded.chunk_count,
                    chunks_added = excluded.chunks_added,
                    status = excluded.status,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    ingest_id,
                    filename,
                    source_type,
                    restaurant_tag,
                    file_sha256,
                    int(file_size),
                    int(extracted_text_chars),
                    int(chunk_count),
                    int(chunks_added),
                    status,
                    _iso_now(),
                ),
            )
            con.commit()
        finally:
            con.close()

    def _write_ingest_report(self, ingest_id: str, payload: Dict[str, Any]) -> None:
        report_path = self.ingest_reports_dir / f"{ingest_id}.json"
        report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def list_ingest_reports(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.ingest_reports_dir.exists():
            return []
        files = sorted(
            self.ingest_reports_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        output: List[Dict[str, Any]] = []
        for file in files[: max(1, min(limit, 100))]:
            try:
                payload = json.loads(file.read_text(encoding="utf-8"))
            except Exception:
                continue
            output.append(
                {
                    "ingest_id": payload.get("ingest_id") or file.stem,
                    "filename": payload.get("raw_document", {}).get("filename") or "",
                    "status": payload.get("status") or "unknown",
                    "created_at": payload.get("created_at") or "",
                }
            )
        return output

    def load_ingest_report(self, ingest_id_or_prefix: str) -> Optional[Dict[str, Any]]:
        needle = str(ingest_id_or_prefix or "").strip()
        if not needle:
            return None
        exact = self.ingest_reports_dir / f"{needle}.json"
        if exact.exists():
            try:
                return json.loads(exact.read_text(encoding="utf-8"))
            except Exception:
                return None

        matches = sorted(
            self.ingest_reports_dir.glob(f"{needle}*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not matches:
            return None
        try:
            return json.loads(matches[0].read_text(encoding="utf-8"))
        except Exception:
            return None

    def _load_sources(self) -> List[Dict[str, Any]]:
        try:
            with open(self.sources_file, "r") as f:
                sources = json.load(f)

            normalized = False
            for source in sources:
                if "collection_name" not in source:
                    source["collection_name"] = COLLECTION_NAME
                    normalized = True

                tier = normalize_knowledge_tier(source.get("knowledge_tier"))
                if not tier:
                    tier = infer_knowledge_tier(
                        source_type=source.get("type", ""),
                        title=source.get("title", ""),
                        source_name=source.get("source_name", ""),
                        summary=source.get("summary", ""),
                    )
                if source.get("knowledge_tier") != tier:
                    source["knowledge_tier"] = tier
                    normalized = True

            if normalized:
                self._save_sources(sources)

            return sources
        except Exception:
            return []

    def _save_sources(self, sources: List[Dict[str, Any]]) -> None:
        with open(self.sources_file, "w") as f:
            json.dump(sources, f, indent=2)

    def _get_settings(self) -> Dict[str, Any]:
        config = load_runtime_config()
        rag_config = config.get("rag", {}) if isinstance(config, dict) else {}
        return deep_merge(DEFAULT_RAG_SETTINGS, rag_config)

    def _profile_pdf(self, path: Path) -> Dict[str, Any]:
        doc = fitz.open(path)
        page_count = len(doc)
        image_count = 0
        pages_with_images = 0
        text_chars = 0
        pages_with_text = 0

        try:
            for page in doc:
                page_text = (page.get_text("text") or "").strip()
                if page_text:
                    pages_with_text += 1
                    text_chars += len(page_text)

                page_images = page.get_images(full=True)
                if page_images:
                    pages_with_images += 1
                    image_count += len(page_images)
        finally:
            doc.close()

        image_page_ratio = (pages_with_images / page_count) if page_count else 0.0
        text_chars_per_page = (text_chars / page_count) if page_count else 0.0

        return {
            "page_count": page_count,
            "image_count": image_count,
            "pages_with_images": pages_with_images,
            "image_page_ratio": round(image_page_ratio, 3),
            "pages_with_text": pages_with_text,
            "text_chars": text_chars,
            "text_chars_per_page": round(text_chars_per_page, 1),
        }

    def _is_image_rich(self, profile: Dict[str, Any], settings: Dict[str, Any]) -> bool:
        ratio_threshold = float(settings["ocr"].get("image_page_ratio_threshold", 0.6))
        return profile["image_count"] > 0 and profile["image_page_ratio"] >= ratio_threshold

    def _should_apply_ocr(self, profile: Dict[str, Any], settings: Dict[str, Any]) -> bool:
        if profile["image_count"] == 0:
            return False

        min_chars_per_page = float(settings["ocr"].get("min_text_chars_per_page", 300))
        low_text_threshold = int(settings["ocr"].get("low_text_char_threshold", 500))

        low_text = (
            profile["text_chars_per_page"] < min_chars_per_page
            or profile["text_chars"] < low_text_threshold
        )
        image_rich = self._is_image_rich(profile, settings)

        # OCR is mandatory for scanned or mixed PDFs where image pages dominate
        # or when text density is very low in image-bearing pages.
        return image_rich or (low_text and profile["image_page_ratio"] >= 0.25)

    def _run_ocr(self, input_pdf: Path, output_pdf: Path, tool_name: str) -> Tuple[bool, str]:
        tool_path = shutil.which(tool_name)
        if not tool_path:
            return (
                False,
                (
                    f"OCR required but '{tool_name}' is not installed. "
                    "Install ocrmypdf and retry, or preprocess manually with: "
                    f"ocrmypdf --skip-text '{input_pdf}' '{input_pdf.stem}_ocr.pdf'"
                ),
            )

        output_pdf.parent.mkdir(parents=True, exist_ok=True)
        cmd = [tool_path, "--skip-text", "--optimize", "0", str(input_pdf), str(output_pdf)]

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            return True, ""
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            if len(stderr) > 600:
                stderr = f"{stderr[:600]}..."
            return (
                False,
                (
                    "OCR failed with ocrmypdf. "
                    f"Details: {stderr or 'unknown error'}. "
                    f"Try manual preprocessing: ocrmypdf --skip-text '{input_pdf}' '{input_pdf.stem}_ocr.pdf'"
                ),
            )

    def _extract_pdf_images(
        self, path: Path, source_id: str, max_images: int
    ) -> List[Dict[str, Any]]:
        images_dir = resolve_path("data/extracted_images") / source_id
        images_dir.mkdir(parents=True, exist_ok=True)

        doc = fitz.open(path)
        seen_xrefs = set()
        records: List[Dict[str, Any]] = []

        try:
            for page_index, page in enumerate(doc, start=1):
                for image_index, image in enumerate(page.get_images(full=True), start=1):
                    xref = image[0]
                    if xref in seen_xrefs:
                        continue
                    seen_xrefs.add(xref)

                    try:
                        base_image = doc.extract_image(xref)
                    except Exception:
                        continue

                    image_bytes = base_image.get("image")
                    if not image_bytes:
                        continue

                    ext = base_image.get("ext", "png")
                    image_path = images_dir / f"page_{page_index:04d}_img_{image_index:03d}.{ext}"
                    image_path.write_bytes(image_bytes)

                    records.append(
                        {
                            "page": page_index,
                            "path": str(image_path),
                        }
                    )

                    if len(records) >= max_images:
                        return records
        finally:
            doc.close()

        return records

    def _vision_descriptions(
        self,
        image_records: List[Dict[str, Any]],
        settings: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        warnings: List[str] = []
        vision_chunks: List[Dict[str, Any]] = []

        if not image_records:
            warnings.append(
                "Vision descriptions requested, but no images were available to describe."
            )
            return vision_chunks, warnings

        config = load_runtime_config()
        ollama_cfg = config.get("ollama", {}) if isinstance(config, dict) else {}
        vision_cfg = settings.get("vision", {})

        model = vision_cfg.get("model") or ollama_cfg.get("vision_model") or ""
        if not model:
            warnings.append(
                "Vision descriptions requested, but no vision model is configured. "
                "Set rag.vision.model (or ollama.vision_model) to enable this step."
            )
            return vision_chunks, warnings

        base_url = os.environ.get("OLLAMA_URL") or ollama_cfg.get(
            "base_url", "http://localhost:11434"
        )
        prompt = vision_cfg.get(
            "prompt",
            "Describe the key operational and culinary information in this image.",
        )
        max_images = int(vision_cfg.get("max_images", 8))

        for image in image_records[:max_images]:
            image_path = Path(image["path"])
            if not image_path.exists():
                warnings.append(f"Image missing during vision step: {image_path}")
                continue

            try:
                encoded = base64.b64encode(image_path.read_bytes()).decode("utf-8")
                payload = {
                    "model": model,
                    "stream": False,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt,
                            "images": [encoded],
                        }
                    ],
                }
                response = requests.post(f"{base_url}/api/chat", json=payload, timeout=120)
                response.raise_for_status()
                content = ((response.json().get("message") or {}).get("content") or "").strip()

                if not content:
                    warnings.append(f"Vision model returned no text for {image_path.name}.")
                    continue

                vision_chunks.append(
                    {
                        "text": f"Image context from page {image['page']}:\n{content}",
                        "heading": f"Image Description (Page {image['page']})",
                        "kind": "vision",
                    }
                )
            except Exception as exc:
                warnings.append(f"Vision step failed for {image_path.name}: {exc}")

        return vision_chunks, warnings

    def _ocr_docx_images(self, path: Path, ingest_id: str) -> Tuple[str, int, List[str]]:
        warnings: List[str] = []
        images_dir = resolve_path("data/tmp/images") / ingest_id
        image_paths = extract_docx_images(str(path), str(images_dir))
        ocr_parts: List[str] = []
        ocr_ok = 0
        for image_path in image_paths:
            try:
                result = self.docx_ocr_runner.run(
                    ["tesseract", image_path, "stdout"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=90,
                )
                text = (result.stdout or "").strip()
                if text:
                    ocr_parts.append(text)
                    ocr_ok += 1
            except Exception as exc:
                warnings.append(f"OCR failed for {Path(image_path).name}: {exc}")
        return "\n\n".join(ocr_parts).strip(), ocr_ok, warnings

    def _is_heading_line(self, line: str) -> bool:
        text = str(line or "").strip()
        if not text:
            return False
        if text.startswith("#"):
            return True
        if len(text) <= 90 and text.upper() == text and any(ch.isalpha() for ch in text):
            return True
        return False

    def _has_unit_hint(self, text: str) -> bool:
        return bool(
            re.search(
                r"(?i)\b\d+(?:\.\d+)?\s*(?:g|kg|mg|ml|l|lb|lbs|#|oz|fl oz|qt|pt|gal|cup|cups|tbsp|tsp|cs|case|cases|ea|each|pcs)\b",
                str(text or ""),
            )
        ) or bool(re.match(r"^\d+(?:\.\d+)?#", str(text or "").replace(" ", "")))

    def _looks_like_recipe_boundary(self, line: str, next_lines: List[str]) -> bool:
        text = str(line or "").strip()
        if not text:
            return False
        if self._is_heading_line(text):
            return False
        if _is_ingredient_line(text):
            return False
        if text.endswith(":"):
            return False
        if len(text) < 3 or len(text) > 90:
            return False
        if sum(1 for ch in text if ch.isdigit()) > 0:
            return False
        if len(text.split()) > 8:
            return False
        if not any(ch.isalpha() for ch in text):
            return False

        probe = [str(item or "").strip() for item in next_lines[:5] if str(item or "").strip()]
        if not probe:
            return False
        if any(self._has_unit_hint(item) for item in probe):
            return True
        section_hits = {"ingredients", "base", "method", "grind and add", "finish", "for service"}
        if any(_normalize_key(item.rstrip(":")) in section_hits for item in probe):
            return True
        return False

    def _split_restaurant_recipe_blocks(
        self, text: str, default_heading: str
    ) -> List[Tuple[str, List[str]]]:
        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
        if not lines:
            return []

        blocks: List[Tuple[str, List[str]]] = []
        current_heading = default_heading
        current_lines: List[str] = []

        for idx, line in enumerate(lines):
            if self._looks_like_recipe_boundary(line, lines[idx + 1 :]):
                if current_lines:
                    blocks.append((current_heading, current_lines))
                    current_lines = []
                current_heading = line
                current_lines = [line]
                continue
            current_lines.append(line)

        if current_lines:
            blocks.append((current_heading, current_lines))

        # Merge heading-only fragments into the next block so chunks carry actual body content.
        merged: List[Tuple[str, List[str]]] = []
        idx = 0
        section_markers = {
            "ingredients",
            "base",
            "method",
            "grind and add",
            "finish",
            "for service",
            "ratio",
        }
        while idx < len(blocks):
            heading, body_lines = blocks[idx]
            payload = False
            for candidate in body_lines[1:]:
                norm = _normalize_key(str(candidate).rstrip(":"))
                if (
                    self._has_unit_hint(candidate)
                    or norm in section_markers
                    or STEP_LINE_RE.match(str(candidate))
                ):
                    payload = True
                    break
            if not payload and idx + 1 < len(blocks):
                next_heading, next_body = blocks[idx + 1]
                blocks[idx + 1] = (next_heading, [*body_lines, *next_body])
                idx += 1
                continue
            merged.append((heading, body_lines))
            idx += 1

        if len(merged) <= 1:
            return []
        return merged

    def _derive_text_profile_label(
        self,
        *,
        extracted_text_chars: int,
        extracted_from_tables_chars: int,
        extracted_from_paragraphs_chars: int,
        image_rich: bool,
    ) -> str:
        if image_rich:
            return "IMAGE-RICH"
        if int(extracted_text_chars) >= 20000:
            return "TEXT-RICH"
        if (
            int(extracted_from_tables_chars) > int(extracted_from_paragraphs_chars)
            and int(extracted_from_tables_chars) > 0
        ):
            return "TABLES-ONLY"
        return "LOW TEXT"

    def _chunk_text_blocks(
        self,
        text: str,
        heading: str = "General",
        chunk_size_chars: int = 3500,
        chunk_overlap_chars: int = 400,
        minimum_chunk_chars: int = 400,
        merge_short_chunks: bool = True,
        pre_sections: Optional[List[Tuple[str, List[str]]]] = None,
    ) -> List[Dict[str, str]]:
        sections: List[Tuple[str, List[str]]] = []
        if pre_sections:
            sections = [
                (
                    str(h or heading),
                    [str(line or "").strip() for line in body if str(line or "").strip()],
                )
                for h, body in pre_sections
            ]
        else:
            lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
            if not lines:
                return []

            current_heading = heading
            current_lines: List[str] = []
            for line in lines:
                if self._is_heading_line(line):
                    if current_lines:
                        sections.append((current_heading, current_lines))
                        current_lines = []
                    current_heading = line.lstrip("#").strip() or heading
                    continue
                current_lines.append(line)
            if current_lines:
                sections.append((current_heading, current_lines))
            if not sections:
                sections = [(heading, lines)]

        chunks: List[Dict[str, str]] = []
        stride = max(200, int(chunk_size_chars) - int(chunk_overlap_chars))
        for sec_heading, body_lines in sections:
            body = "\n".join(body_lines).strip()
            if not body:
                continue
            prefix = f"## {sec_heading}\n"
            if len(body) + len(prefix) <= chunk_size_chars:
                chunks.append({"text": f"{prefix}{body}".strip(), "heading": sec_heading})
                continue

            start = 0
            while start < len(body):
                end = min(len(body), start + int(chunk_size_chars) - len(prefix))
                slice_text = body[start:end].strip()
                if not slice_text:
                    break
                chunks.append({"text": f"{prefix}{slice_text}".strip(), "heading": sec_heading})
                if end >= len(body):
                    break
                start += stride

        if not chunks:
            return []

        if not merge_short_chunks:
            return [
                {
                    "text": str(chunk.get("text") or "").strip(),
                    "heading": str(chunk.get("heading") or heading),
                }
                for chunk in chunks
            ]

        merged: List[Dict[str, str]] = []
        for chunk in chunks:
            text_value = str(chunk.get("text") or "").strip()
            if not text_value:
                continue
            if merged and len(text_value) < int(minimum_chunk_chars):
                merged[-1]["text"] = f"{merged[-1]['text']}\n{text_value}".strip()
                continue
            merged.append({"text": text_value, "heading": str(chunk.get("heading") or heading)})

        return merged

    def _apply_chunk_dedupe(
        self,
        chunks_data: List[Dict[str, Any]],
        *,
        dedupe_enabled: bool,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
        warnings: List[str] = []
        repeated: Dict[str, int] = {}
        pre_count = len(chunks_data)
        if not dedupe_enabled or pre_count <= 1:
            return (
                chunks_data,
                {
                    "dedupe_enabled": bool(dedupe_enabled),
                    "pre_dedupe_count": pre_count,
                    "post_dedupe_count": pre_count,
                    "dedupe_key_strategy": "sha256(normalized_chunk_text)",
                    "top_repeated_hashes": [],
                },
                warnings,
            )

        deduped: List[Dict[str, Any]] = []
        seen: Dict[str, int] = {}
        for item in chunks_data:
            raw_text = str(item.get("text") or "")
            normalized = " ".join(raw_text.split())
            if not normalized:
                continue
            dedupe_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
            if dedupe_hash in seen:
                repeated[dedupe_hash] = repeated.get(dedupe_hash, 1) + 1
                continue
            seen[dedupe_hash] = 1
            deduped.append(item)

        post_count = len(deduped)
        if pre_count > 0 and post_count < (0.3 * pre_count):
            warnings.append(
                f"dedupe_collapse: pre_dedupe_count={pre_count} post_dedupe_count={post_count} strategy=sha256(normalized_chunk_text)"
            )

        top_repeated = sorted(repeated.items(), key=lambda pair: pair[1], reverse=True)[:5]
        return (
            deduped,
            {
                "dedupe_enabled": True,
                "pre_dedupe_count": pre_count,
                "post_dedupe_count": post_count,
                "dedupe_key_strategy": "sha256(normalized_chunk_text)",
                "top_repeated_hashes": [
                    {"hash": key, "count": count} for key, count in top_repeated
                ],
            },
            warnings,
        )

    def _infer_chunk_recipe_metadata(self, text: str, chunk_index: int) -> Dict[str, Any]:
        entries = _parse_recipe_entries_from_chunk(text=text, chunk_id=chunk_index)
        if not entries:
            return {"recipe_name": "", "section_name": "", "order_index": int(chunk_index)}
        first = entries[0]
        return {
            "recipe_name": str(first.get("recipe_name") or ""),
            "section_name": str(first.get("section_name") or ""),
            "order_index": int(chunk_index),
        }

    def _fetch_source_chunks(self, source_name: str) -> List[Dict[str, Any]]:
        try:
            payload = self.collection.get(
                where={"source": source_name},
                include=["documents", "metadatas"],
            )
        except Exception:
            return []

        rows: List[Dict[str, Any]] = []
        ids = payload.get("ids") or []
        docs = payload.get("documents") or []
        metas = payload.get("metadatas") or []
        for idx in range(len(ids)):
            meta = metas[idx] if idx < len(metas) else {}
            try:
                chunk_id = int((meta or {}).get("chunk_id", idx))
            except Exception:
                chunk_id = idx
            rows.append(
                {
                    "chunk_id": chunk_id,
                    "content": str(docs[idx] if idx < len(docs) else ""),
                    "metadata": meta or {},
                }
            )

        rows.sort(key=lambda item: int(item.get("chunk_id", 0)))
        return rows

    def _assemble_house_recipe_from_source(
        self,
        *,
        query_text: str,
        source_name: str,
        source_title: str,
        confidence_threshold: float,
    ) -> Dict[str, Any]:
        chunks = self._fetch_source_chunks(source_name=source_name)
        if not chunks:
            return {
                "status": "not_found",
                "query": query_text,
                "source_name": source_name,
                "source_title": source_title,
                "reason": "source_chunks_empty",
            }

        entries: List[Dict[str, Any]] = []
        for chunk in chunks:
            entries.extend(
                _parse_recipe_entries_from_chunk(
                    text=str(chunk.get("content") or ""),
                    chunk_id=int(chunk.get("chunk_id", 0)),
                )
            )

        if not entries:
            return {
                "status": "incomplete",
                "query": query_text,
                "source_name": source_name,
                "source_title": source_title,
                "reason": "extraction_empty",
                "chunks_used": 0,
                "sections_detected": [],
                "missing_sections": ["ingredients", "method"],
                "confidence": 0.0,
            }

        target = _normalize_recipe_query_target(query_text) or str(query_text or "").strip()
        candidate_names = sorted(
            {
                str(entry.get("recipe_name") or "").strip()
                for entry in entries
                if entry.get("recipe_name")
            }
        )
        if not candidate_names:
            return {
                "status": "incomplete",
                "query": query_text,
                "source_name": source_name,
                "source_title": source_title,
                "reason": "no_recipe_titles",
                "chunks_used": 0,
                "sections_detected": [],
                "missing_sections": ["ingredients", "method"],
                "confidence": 0.0,
            }

        best_name = ""
        best_score = 0.0
        for candidate in candidate_names:
            score = _title_match_score(target=target, candidate=candidate)
            if score > best_score:
                best_name = candidate
                best_score = score

        if best_score < 0.55:
            return {
                "status": "not_found",
                "query": query_text,
                "source_name": source_name,
                "source_title": source_title,
                "reason": "low_title_match",
                "confidence": float(best_score),
            }

        selected = [
            entry
            for entry in entries
            if _normalize_key(str(entry.get("recipe_name") or "")) == _normalize_key(best_name)
        ]
        selected.sort(
            key=lambda item: (int(item.get("chunk_id", 0)), int(item.get("order_index", 0)))
        )

        section_order: List[str] = []
        section_lines: Dict[str, List[Tuple[str, str]]] = {}
        for entry in selected:
            section = str(entry.get("section_name") or "Base").strip() or "Base"
            if section not in section_lines:
                section_lines[section] = []
                section_order.append(section)
            line = str(entry.get("line") or "").strip()
            kind = str(entry.get("kind") or "ingredient")
            if not line:
                continue
            if section_lines[section] and section_lines[section][-1][0].lower() == line.lower():
                continue
            section_lines[section].append((line, kind))

        ingredient_sections: List[Tuple[str, List[str]]] = []
        method_lines: List[str] = []
        for section in section_order:
            values = section_lines.get(section, [])
            if not values:
                continue
            section_key = _normalize_key(section)
            if section_key in METHOD_SECTION_KEYS:
                for line, _ in values:
                    if line.lower() not in {item.lower() for item in method_lines}:
                        method_lines.append(line)
                continue

            ingredients = [line for line, kind in values if kind == "ingredient"]
            if ingredients:
                ingredient_sections.append((section, ingredients))

            narrative = [line for line, kind in values if kind != "ingredient"]
            for line in narrative:
                if line.lower() not in {item.lower() for item in method_lines}:
                    method_lines.append(line)

        if not method_lines:
            for section, _ in ingredient_sections:
                if _normalize_key(section) in ACTION_SECTION_KEYS:
                    method_lines.append(section)
                    break

        missing_sections: List[str] = []
        if not ingredient_sections:
            missing_sections.append("ingredients")
        if not method_lines:
            missing_sections.append("method")

        chunks_used = len({int(entry.get("chunk_id", 0)) for entry in selected})
        sections_detected = [section for section, lines in ingredient_sections if lines]
        if "Method" not in sections_detected and method_lines:
            sections_detected.append("Method")

        confidence = float(best_score)
        if missing_sections or confidence < float(confidence_threshold):
            return {
                "status": "incomplete",
                "query": query_text,
                "source_name": source_name,
                "source_title": source_title,
                "matched_recipe_name": best_name,
                "reason": "validation_failed" if missing_sections else "low_confidence",
                "chunks_used": int(chunks_used),
                "sections_detected": sections_detected,
                "missing_sections": missing_sections or ["confidence"],
                "confidence": confidence,
            }

        return {
            "status": "ok",
            "query": query_text,
            "source_name": source_name,
            "source_title": source_title,
            "matched_recipe_name": best_name,
            "chunks_used": int(chunks_used),
            "sections_detected": sections_detected,
            "missing_sections": [],
            "confidence": confidence,
            "html": _format_house_recipe_html(
                recipe_name=best_name,
                source_title=source_title or source_name,
                ingredient_sections=ingredient_sections,
                method_lines=method_lines,
            ),
        }

    def assemble_house_recipe(
        self,
        *,
        query_text: str,
        n_results: int = 10,
        confidence_threshold: float = 0.75,
    ) -> Dict[str, Any]:
        hits = self.search(
            query_text=query_text,
            n_results=max(5, int(n_results)),
            source_tiers=[TIER_1_RECIPE_OPS],
        )
        if not hits:
            return {
                "status": "not_found",
                "query": query_text,
                "reason": "no_tier1_hits",
            }

        sources_meta = self._load_sources()
        source_title_by_name = {
            str(source.get("source_name") or ""): str(
                source.get("title") or source.get("source_name") or ""
            )
            for source in sources_meta
        }

        source_names: List[str] = []
        for hit in hits:
            source_name = str(hit.get("source") or "")
            if source_name and source_name not in source_names:
                source_names.append(source_name)

        best_incomplete: Optional[Dict[str, Any]] = None
        for source_name in source_names:
            source_title = source_title_by_name.get(source_name, source_name)
            assembled = self._assemble_house_recipe_from_source(
                query_text=query_text,
                source_name=source_name,
                source_title=source_title,
                confidence_threshold=confidence_threshold,
            )
            if assembled.get("status") == "ok":
                return assembled
            if assembled.get("status") == "incomplete":
                if best_incomplete is None:
                    best_incomplete = assembled
                else:
                    score_now = float(assembled.get("confidence", 0.0)) + float(
                        assembled.get("chunks_used", 0)
                    )
                    score_best = float(best_incomplete.get("confidence", 0.0)) + float(
                        best_incomplete.get("chunks_used", 0)
                    )
                    if score_now > score_best:
                        best_incomplete = assembled

        if best_incomplete is not None:
            return best_incomplete

        return {
            "status": "not_found",
            "query": query_text,
            "reason": "no_source_match",
        }

    def debug_house_recipe(
        self,
        *,
        query_text: str,
        n_results: int = 10,
        confidence_threshold: float = 0.75,
    ) -> Dict[str, Any]:
        return self.assemble_house_recipe(
            query_text=query_text,
            n_results=n_results,
            confidence_threshold=confidence_threshold,
        )

    def get_sources(self) -> List[Dict[str, Any]]:
        return self._load_sources()

    def toggle_source(self, source_id: str, active: bool) -> bool:
        sources = self._load_sources()
        for source in sources:
            if source["id"] == source_id:
                source["status"] = "active" if active else "disabled"
                self._save_sources(sources)
                return True
        return False

    def delete_source(self, source_id: str) -> bool:
        sources = self._load_sources()
        source_name: Optional[str] = None
        extracted_dir: Optional[str] = None
        new_sources: List[Dict[str, Any]] = []

        for source in sources:
            if source["id"] == source_id:
                source_name = source["source_name"]
                extracted_dir = source.get("extracted_image_dir")
            else:
                new_sources.append(source)

        if not source_name:
            return False

        try:
            self.collection.delete(where={"source": source_name})
        except Exception as exc:
            logger.error("Error deleting chunks for %s: %s", source_name, exc)

        if extracted_dir:
            extracted_path = Path(extracted_dir)
            if extracted_path.exists() and extracted_path.is_dir():
                shutil.rmtree(extracted_path, ignore_errors=True)

        self._save_sources(new_sources)
        return True

    def ingest_file(
        self,
        file_path: str,
        extra_metadata: Optional[Dict[str, Any]] = None,
        ingestion_options: Optional[Dict[str, Any]] = None,
        ingest_id: Optional[str] = None,
    ) -> Tuple[bool, Any]:
        """
        Ingestion pipeline:
        1) text extraction
        2) OCR (if needed; mandatory for image-heavy scanned/mixed PDFs)
        3) chunking + embeddings
        4) optional image extraction
        5) optional vision-to-text descriptions
        """
        if extra_metadata is None:
            extra_metadata = {}

        path = Path(file_path)
        if not path.exists():
            return False, "File not found."

        ingest_id = str(ingest_id or "").strip() or uuid.uuid4().hex
        source_id = str(uuid.uuid4())
        date_ingested = _iso_now()
        file_sha256 = _sha256_file(path)
        file_size = int(path.stat().st_size)

        settings = self._get_settings()
        runtime_cfg = load_runtime_config()
        debug_cfg = runtime_cfg.get("debug", {}) if isinstance(runtime_cfg, dict) else {}
        image_cfg = settings.get("image_processing", {})
        vision_cfg = settings.get("vision", {})
        chunk_cfg = settings.get("chunking", {})
        docx_cfg = settings.get("docx", {})

        chunk_size_chars = int(chunk_cfg.get("chunk_size_chars", 3500))
        chunk_overlap_chars = int(chunk_cfg.get("chunk_overlap_chars", 400))
        minimum_chunk_chars = int(chunk_cfg.get("minimum_chunk_chars", 400))
        dedupe_enabled = bool(chunk_cfg.get("dedupe_enabled", True))
        image_only_text_threshold = int(docx_cfg.get("image_only_text_threshold", 5000))

        knowledge_tier = normalize_knowledge_tier(
            extra_metadata.get("knowledge_tier")
        ) or infer_knowledge_tier(
            source_type=extra_metadata.get("source_type", "document"),
            title=extra_metadata.get("source_title", path.stem),
            source_name=path.name,
            summary=extra_metadata.get("summary", ""),
        )
        source_type_raw = str(extra_metadata.get("source_type", "unknown"))
        source_type = _map_doc_source_type(source_type_raw, knowledge_tier)
        restaurant_tag = extra_metadata.get("restaurant_tag")

        options = {
            "extract_images": bool(image_cfg.get("extract_images", False)),
            "vision_descriptions": bool(vision_cfg.get("enabled", False)),
        }
        if ingestion_options:
            options.update({k: bool(v) for k, v in ingestion_options.items() if k in options})
        if options["vision_descriptions"]:
            options["extract_images"] = True

        is_pdf = path.suffix.lower() == ".pdf"
        is_docx = path.suffix.lower() == ".docx"
        chunker = SmartChunker(target_size=chunk_size_chars, overlap=chunk_overlap_chars)

        warnings: List[str] = []
        pipeline = {
            "text_extraction": "started",
            "ocr": "not_applicable",
            "chunking_embeddings": "pending",
            "image_extraction": "skipped",
            "vision_descriptions": "skipped",
        }

        profile_before: Dict[str, Any] = {}
        profile_after: Dict[str, Any] = {}
        image_rich = False
        ocr_required = False
        ocr_applied = False
        extracted_images: List[Dict[str, Any]] = []
        extracted_text_chars = 0

        report: Dict[str, Any] = {
            "ingest_id": ingest_id,
            "created_at": date_ingested,
            "raw_document": {
                "filename": path.name,
                "file_extension": path.suffix.lower(),
                "file_size_bytes": file_size,
                "file_sha256": file_sha256,
                "guessed_source_type": source_type,
                "restaurant_tag": restaurant_tag,
                "source_type_raw": source_type_raw,
            },
            "extraction_metrics": {
                "extracted_text_chars": 0,
                "extracted_text_lines": 0,
                "docx_paragraph_count": 0,
                "docx_table_count": 0,
                "docx_table_cell_count": 0,
                "extracted_from_tables_chars": 0,
                "extracted_from_paragraphs_chars": 0,
                "embedded_image_count": 0,
                "warning_flags": {
                    "text_too_short": False,
                    "tables_present_but_empty_extraction": False,
                    "likely_image_only": False,
                },
            },
            "chunking_metrics": {
                "chunk_size_config": chunk_size_chars,
                "chunk_overlap_config": chunk_overlap_chars,
                "minimum_chunk_chars": minimum_chunk_chars,
                "produced_chunk_count": 0,
                "avg_chunk_chars": 0.0,
                "min_chunk_chars": 0,
                "max_chunk_chars": 0,
                "pre_dedupe_chunk_samples": [],
            },
            "dedupe_metrics": {
                "dedupe_enabled": dedupe_enabled,
                "pre_dedupe_count": 0,
                "post_dedupe_count": 0,
                "dedupe_key_strategy": "sha256(normalized_chunk_text)",
                "top_repeated_hashes": [],
            },
            "vector_store_metrics": {
                "attempted_add_count": 0,
                "successfully_added_count": 0,
                "collection_name": COLLECTION_NAME,
                "metadata_fields_stored": [],
                "error_count": 0,
            },
            "chunk_samples": [],
            "warnings": warnings,
            "status": "failed",
        }

        # Persist canonical source row immediately so ingest is visible while in progress.
        self._persist_doc_source(
            ingest_id=ingest_id,
            filename=path.name,
            source_type=source_type,
            restaurant_tag=restaurant_tag,
            file_sha256=file_sha256,
            file_size=file_size,
            extracted_text_chars=0,
            chunk_count=0,
            chunks_added=0,
            status="queued",
        )

        ocr_working_pdf = path
        temp_ocr_path: Optional[Path] = None
        chunks_added = 0
        chunks_final_count = 0
        error_text: Optional[str] = None

        try:
            if is_pdf:
                profile_before = self._profile_pdf(path)
                image_rich = self._is_image_rich(profile_before, settings)
                ocr_required = self._should_apply_ocr(profile_before, settings)

                if ocr_required:
                    pipeline["ocr"] = "required"
                    if not bool(settings.get("ocr", {}).get("enabled", True)):
                        raise RuntimeError(
                            "Ingestion blocked: this PDF appears image-heavy and requires OCR before indexing."
                        )

                    temp_ocr_path = (
                        resolve_path("data/tmp/ocr") / f"{path.stem}_{source_id[:8]}.pdf"
                    )
                    ok, error_msg = self._run_ocr(
                        path, temp_ocr_path, settings["ocr"].get("tool", "ocrmypdf")
                    )
                    if not ok:
                        raise RuntimeError(error_msg)

                    ocr_working_pdf = temp_ocr_path
                    ocr_applied = True
                    pipeline["ocr"] = "applied"
                    profile_after = self._profile_pdf(ocr_working_pdf)
                else:
                    pipeline["ocr"] = "not_needed"
                    profile_after = profile_before

            pipeline["text_extraction"] = "completed"

            chunks_data: List[Dict[str, Any]] = []
            if is_pdf:
                chunks = chunker.chunk_pdf(ocr_working_pdf)
                chunks_data = [
                    {"text": chunk["text"], "heading": chunk["heading"], "kind": "text"}
                    for chunk in chunks
                ]
                extracted_text_chars = int(sum(len(chunk["text"]) for chunk in chunks_data))
                report["extraction_metrics"]["extracted_text_chars"] = extracted_text_chars
                report["extraction_metrics"]["extracted_text_lines"] = int(
                    sum(chunk["text"].count("\n") + 1 for chunk in chunks_data)
                )
            elif is_docx:
                extracted_text, metrics = extract_text(str(path))
                extracted_text_chars = len(extracted_text)
                report["extraction_metrics"].update(metrics)
                report["extraction_metrics"]["extracted_text_chars"] = extracted_text_chars
                report["extraction_metrics"]["extracted_text_lines"] = len(
                    [line for line in extracted_text.splitlines() if line.strip()]
                )

                embedded_image_count = int(
                    metrics.get("embedded_image_count", detect_images_in_docx(str(path)))
                )
                tables_present_but_empty = (
                    int(metrics.get("docx_table_count", 0)) > 0
                    and int(metrics.get("extracted_from_tables_chars", 0)) == 0
                )
                likely_image_only = (
                    embedded_image_count > 0 and extracted_text_chars < image_only_text_threshold
                )
                text_too_short = extracted_text_chars < image_only_text_threshold
                report["extraction_metrics"]["warning_flags"] = {
                    "text_too_short": bool(text_too_short),
                    "tables_present_but_empty_extraction": bool(tables_present_but_empty),
                    "likely_image_only": bool(likely_image_only),
                }
                if tables_present_but_empty:
                    warnings.append(
                        "tables_present_but_empty_extraction: docx_table_count>0 but extracted_from_tables_chars=0"
                    )
                if text_too_short:
                    warnings.append(
                        f"text_too_short: extracted_text_chars={extracted_text_chars} threshold={image_only_text_threshold}"
                    )
                if likely_image_only:
                    warnings.append(
                        f"likely_image_only: embedded_image_count={embedded_image_count} extracted_text_chars={extracted_text_chars}"
                    )

                if source_type == "restaurant_recipes" and (
                    likely_image_only or (text_too_short and embedded_image_count > 0)
                ):
                    ocr_text, ocr_images, ocr_warnings = self._ocr_docx_images(path, ingest_id)
                    warnings.extend(ocr_warnings)
                    if ocr_text:
                        ocr_applied = True
                        pipeline["ocr"] = "applied_docx_images"
                        extracted_text = f"{extracted_text}\n\n# OCR Fallback\n{ocr_text}".strip()
                        extracted_text_chars = len(extracted_text)
                        report["extraction_metrics"]["embedded_image_count"] = embedded_image_count
                        report["extraction_metrics"]["extracted_text_chars"] = extracted_text_chars
                        report["extraction_metrics"]["extracted_text_lines"] = len(
                            [line for line in extracted_text.splitlines() if line.strip()]
                        )
                        report["extraction_metrics"]["ocr_images_processed"] = int(ocr_images)
                        report["extraction_metrics"]["ocr_fallback_chars"] = len(ocr_text)
                    else:
                        warnings.append("docx_ocr_fallback_no_text: OCR produced no usable text")

                recipe_sections = (
                    self._split_restaurant_recipe_blocks(extracted_text, "DOCX Content")
                    if source_type == "restaurant_recipes"
                    else []
                )
                text_chunks = self._chunk_text_blocks(
                    extracted_text,
                    heading="DOCX Content",
                    chunk_size_chars=chunk_size_chars,
                    chunk_overlap_chars=chunk_overlap_chars,
                    minimum_chunk_chars=(
                        120 if source_type == "restaurant_recipes" else minimum_chunk_chars
                    ),
                    merge_short_chunks=False if source_type == "restaurant_recipes" else True,
                    pre_sections=recipe_sections if recipe_sections else None,
                )
                chunks_data = [
                    {"text": chunk["text"], "heading": chunk["heading"], "kind": "text"}
                    for chunk in text_chunks
                ]
            else:
                text = path.read_text(encoding="utf-8", errors="replace")
                extracted_text_chars = len(text)
                report["extraction_metrics"]["extracted_text_chars"] = extracted_text_chars
                report["extraction_metrics"]["extracted_text_lines"] = len(
                    [line for line in text.splitlines() if line.strip()]
                )
                report["extraction_metrics"][
                    "extracted_from_paragraphs_chars"
                ] = extracted_text_chars
                text_chunks = self._chunk_text_blocks(
                    text,
                    heading="General",
                    chunk_size_chars=chunk_size_chars,
                    chunk_overlap_chars=chunk_overlap_chars,
                    minimum_chunk_chars=minimum_chunk_chars,
                )
                chunks_data = [
                    {"text": chunk["text"], "heading": chunk["heading"], "kind": "text"}
                    for chunk in text_chunks
                ]

            if not chunks_data:
                raise RuntimeError("Extraction yielded no text chunks.")

            if is_pdf and options["extract_images"]:
                pipeline["image_extraction"] = "applied"
                max_images = int(image_cfg.get("max_images", 30))
                extracted_images = self._extract_pdf_images(
                    ocr_working_pdf, source_id, max_images=max_images
                )
                if not extracted_images:
                    warnings.append("image_extraction_enabled_but_none_found")

            if is_pdf and options["vision_descriptions"]:
                pipeline["vision_descriptions"] = "applied"
                vision_chunks, vision_warnings = self._vision_descriptions(
                    extracted_images, settings
                )
                chunks_data.extend(vision_chunks)
                warnings.extend(vision_warnings)

            pre_lengths = [
                len(str(item.get("text") or ""))
                for item in chunks_data
                if str(item.get("text") or "").strip()
            ]
            produced_chunk_count = len(pre_lengths)
            report["chunking_metrics"].update(
                {
                    "produced_chunk_count": produced_chunk_count,
                    "avg_chunk_chars": (
                        round(sum(pre_lengths) / produced_chunk_count, 2)
                        if produced_chunk_count
                        else 0.0
                    ),
                    "min_chunk_chars": min(pre_lengths) if pre_lengths else 0,
                    "max_chunk_chars": max(pre_lengths) if pre_lengths else 0,
                    "pre_dedupe_chunk_samples": [
                        {
                            "chunk_id": idx,
                            "heading": str(item.get("heading") or "General"),
                            "text_preview": str(item.get("text") or "")[:300],
                        }
                        for idx, item in enumerate(chunks_data[:3])
                    ],
                }
            )
            if extracted_text_chars < 5000:
                warnings.append(f"low_text_extracted: extracted_text_chars={extracted_text_chars}")
            elif extracted_text_chars >= 20000 and produced_chunk_count < 20:
                if chunk_size_chars >= 10000:
                    warnings.append(
                        f"low_chunk_count: chunk_size too large ({chunk_size_chars} chars) for extracted_text_chars={extracted_text_chars}"
                    )
                else:
                    warnings.append(
                        f"low_chunk_count: extracted_text_chars={extracted_text_chars} produced_chunk_count={produced_chunk_count}"
                    )

            chunks_data, dedupe_metrics, dedupe_warnings = self._apply_chunk_dedupe(
                chunks_data,
                dedupe_enabled=dedupe_enabled,
            )
            warnings.extend(dedupe_warnings)
            report["dedupe_metrics"] = dedupe_metrics
            chunks_final_count = len(chunks_data)

            total_chars = int(sum(len(str(chunk.get("text") or "")) for chunk in chunks_data))
            if total_chars < 100:
                if is_pdf and (image_rich or ocr_required):
                    if not ocr_applied:
                        raise RuntimeError(
                            "Ingestion blocked: most content appears image-based and OCR was not applied."
                        )
                    raise RuntimeError("OCR ran but extracted too little text to index safely.")
                raise RuntimeError(f"Extracted only {total_chars} characters. Ingestion aborted.")

            ids: List[str] = []
            metadatas: List[Dict[str, Any]] = []
            documents: List[str] = []
            for i, item in enumerate(chunks_data):
                chunk_id = f"{path.name}_{i}_{source_id[:8]}"
                recipe_meta = self._infer_chunk_recipe_metadata(str(item.get("text") or ""), i)
                ids.append(chunk_id)
                documents.append(str(item.get("text") or ""))
                metadatas.append(
                    {
                        "source": path.name,
                        "chunk_id": i,
                        "date_ingested": date_ingested,
                        "source_type": source_type_raw,
                        "source_title": extra_metadata.get("source_title", path.stem),
                        "heading": item.get("heading", "General"),
                        "chunk_kind": item.get("kind", "text"),
                        "ocr_applied": bool(ocr_applied),
                        "knowledge_tier": knowledge_tier,
                        "doc_source_type": source_type,
                        "ingest_id": ingest_id,
                        "recipe_name": recipe_meta.get("recipe_name", ""),
                        "section_name": recipe_meta.get("section_name", ""),
                        "order_index": int(recipe_meta.get("order_index", i)),
                    }
                )

            report["vector_store_metrics"]["attempted_add_count"] = len(documents)
            report["vector_store_metrics"]["metadata_fields_stored"] = (
                list(metadatas[0].keys()) if metadatas else []
            )

            try:
                self.collection.delete(where={"source": path.name})
            except Exception:
                pass

            self.collection.add(documents=documents, metadatas=metadatas, ids=ids)
            chunks_added = len(documents)
            report["vector_store_metrics"]["successfully_added_count"] = chunks_added
            pipeline["chunking_embeddings"] = "applied"

            sources = self._load_sources()
            sources = [source for source in sources if source["source_name"] != path.name]

            extracted_image_dir = (
                str((resolve_path("data/extracted_images") / source_id).resolve())
                if extracted_images
                else ""
            )
            extracted_from_tables_chars = int(
                report["extraction_metrics"].get("extracted_from_tables_chars", 0)
            )
            extracted_from_paragraphs_chars = int(
                report["extraction_metrics"].get("extracted_from_paragraphs_chars", 0)
            )
            text_profile = self._derive_text_profile_label(
                extracted_text_chars=int(extracted_text_chars),
                extracted_from_tables_chars=extracted_from_tables_chars,
                extracted_from_paragraphs_chars=extracted_from_paragraphs_chars,
                image_rich=bool(image_rich),
            )
            new_source = {
                "id": source_id,
                "ingest_id": ingest_id,
                "source_name": path.name,
                "collection_name": COLLECTION_NAME,
                "title": extra_metadata.get("source_title", path.stem),
                "type": source_type_raw,
                "knowledge_tier": knowledge_tier,
                "doc_source_type": source_type,
                "date_ingested": date_ingested,
                "chunk_count": len(documents),
                "status": "active",
                "summary": extra_metadata.get("summary", "No summary provided."),
                "ocr_required": bool(ocr_required),
                "ocr_applied": bool(ocr_applied),
                "ocr_tool": settings["ocr"].get("tool", "ocrmypdf") if is_pdf else "tesseract",
                "image_rich": bool(image_rich),
                "image_count": int(profile_before.get("image_count", 0)),
                "page_count": int(profile_before.get("page_count", 0)),
                "image_page_ratio": float(profile_before.get("image_page_ratio", 0.0)),
                "text_chars_before_ocr": int(profile_before.get("text_chars", 0)),
                "text_chars_after_ocr": int((profile_after or profile_before).get("text_chars", 0)),
                "images_extracted": len(extracted_images),
                "extracted_image_dir": extracted_image_dir,
                "vision_descriptions_enabled": bool(options["vision_descriptions"]),
                "vision_descriptions_count": len(
                    [chunk for chunk in chunks_data if chunk.get("kind") == "vision"]
                ),
                "extracted_text_chars": int(extracted_text_chars),
                "extracted_text_lines": int(
                    report["extraction_metrics"].get("extracted_text_lines", 0)
                ),
                "docx_paragraph_count": int(
                    report["extraction_metrics"].get("docx_paragraph_count", 0)
                ),
                "docx_table_count": int(report["extraction_metrics"].get("docx_table_count", 0)),
                "docx_table_cell_count": int(
                    report["extraction_metrics"].get("docx_table_cell_count", 0)
                ),
                "extracted_from_tables_chars": extracted_from_tables_chars,
                "extracted_from_paragraphs_chars": extracted_from_paragraphs_chars,
                "embedded_image_count": int(
                    report["extraction_metrics"].get("embedded_image_count", 0)
                ),
                "text_profile_label": text_profile,
                "ingestion_pipeline": pipeline,
                "warnings": warnings,
                "ingestion_complete": True,
            }
            sources.append(new_source)
            self._save_sources(sources)

            sample_limit = 10 if bool(debug_cfg.get("ingest_report", False)) else 3
            report["chunk_samples"] = [
                {
                    "chunk_id": idx,
                    "heading": str(item.get("heading") or "General"),
                    "text_preview": str(item.get("text") or "")[:320],
                }
                for idx, item in enumerate(chunks_data[:sample_limit])
            ]

            status = "warn" if warnings else "ok"
            report["status"] = status
            self._persist_doc_source(
                ingest_id=ingest_id,
                filename=path.name,
                source_type=source_type,
                restaurant_tag=restaurant_tag,
                file_sha256=file_sha256,
                file_size=file_size,
                extracted_text_chars=extracted_text_chars,
                chunk_count=chunks_final_count,
                chunks_added=chunks_added,
                status=status,
            )
            self._write_ingest_report(ingest_id, report)

            return True, {
                "num_chunks": len(documents),
                "source_title": new_source["title"],
                "date": date_ingested,
                "ingest_id": ingest_id,
                "source_id": source_id,
                "ocr_required": new_source["ocr_required"],
                "ocr_applied": new_source["ocr_applied"],
                "image_rich": new_source["image_rich"],
                "images_extracted": new_source["images_extracted"],
                "vision_descriptions_count": new_source["vision_descriptions_count"],
                "knowledge_tier": new_source["knowledge_tier"],
                "warnings": warnings,
            }

        except Exception as exc:
            error_text = str(exc)
            logger.exception("Error ingesting %s", file_path)
            report["status"] = "failed"
            report["error"] = error_text
            report["vector_store_metrics"]["error_count"] = (
                int(report["vector_store_metrics"].get("error_count", 0)) + 1
            )
            self._persist_doc_source(
                ingest_id=ingest_id,
                filename=path.name,
                source_type=source_type,
                restaurant_tag=restaurant_tag,
                file_sha256=file_sha256,
                file_size=file_size,
                extracted_text_chars=extracted_text_chars,
                chunk_count=chunks_final_count,
                chunks_added=chunks_added,
                status="failed",
            )
            self._write_ingest_report(ingest_id, report)
            return False, error_text

        finally:
            if temp_ocr_path and temp_ocr_path.exists():
                try:
                    temp_ocr_path.unlink()
                except Exception:
                    pass

    def search(
        self,
        query_text: str,
        n_results: int = 5,
        source_tiers: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Searches active sources from the configured collection for relevant context."""
        try:
            sources = self._load_sources()
            active_entries = [
                source
                for source in sources
                if source.get("status") == "active"
                and source.get("collection_name", COLLECTION_NAME) == COLLECTION_NAME
            ]
            requested_tiers = [
                tier
                for tier in (normalize_knowledge_tier(value) for value in (source_tiers or []))
                if tier
            ]
            if requested_tiers:
                requested_set = set(requested_tiers)
                active_entries = [
                    source
                    for source in active_entries
                    if source.get("knowledge_tier", TIER_1_RECIPE_OPS) in requested_set
                ]
            active_sources = [source["source_name"] for source in active_entries]
            source_tier_by_name = {
                source["source_name"]: source.get("knowledge_tier", TIER_1_RECIPE_OPS)
                for source in active_entries
            }
            collection_doc_count = self.collection.count()
            query_preview = " ".join((query_text or "").split())[:180]
            runtime_cfg = load_runtime_config()
            rag_cfg = runtime_cfg.get("rag", {}) if isinstance(runtime_cfg, dict) else {}
            similarity_threshold = rag_cfg.get("similarity_threshold", None)
            enforce_similarity_threshold = bool(rag_cfg.get("enforce_similarity_threshold", False))

            logger.info(
                "RAG search: collection=%s collection_docs=%s active_sources=%s n_results=%s similarity_threshold=%s enforce_similarity_threshold=%s source_tiers=%s query=%s",
                COLLECTION_NAME,
                collection_doc_count,
                len(active_sources),
                n_results,
                similarity_threshold,
                enforce_similarity_threshold,
                requested_tiers if requested_tiers else "all",
                query_preview,
            )
            logger.debug("RAG active source names: %s", active_sources[:20])

            mismatched_sources = [
                source.get("source_name", "unknown")
                for source in sources
                if source.get("status") == "active"
                and source.get("collection_name")
                and source.get("collection_name") != COLLECTION_NAME
            ]
            if mismatched_sources:
                logger.warning(
                    "RAG source/collection mismatch detected. current_collection=%s mismatched_sources=%s",
                    COLLECTION_NAME,
                    mismatched_sources[:20],
                )

            if not active_sources:
                logger.warning("RAG search skipped: no active sources.")
                return []

            results = self.collection.query(
                query_texts=[query_text],
                n_results=n_results,
                where={"source": {"$in": active_sources}},
            )

            output: List[Dict[str, Any]] = []
            if results.get("documents"):
                for i, document in enumerate(results["documents"][0]):
                    metadata = results["metadatas"][0][i]
                    distance = results["distances"][0][i] if "distances" in results else None
                    if (
                        enforce_similarity_threshold
                        and similarity_threshold is not None
                        and distance is not None
                    ):
                        try:
                            if float(distance) > float(similarity_threshold):
                                continue
                        except Exception:
                            pass
                    output.append(
                        {
                            "content": document,
                            "source": metadata.get("source", "unknown"),
                            "heading": metadata.get("heading", ""),
                            "distance": distance if distance is not None else 0,
                            "chunk_id": metadata.get("chunk_id", i),
                            "ingest_id": metadata.get("ingest_id", ""),
                            "source_title": metadata.get("source_title", ""),
                            "doc_source_type": metadata.get("doc_source_type", ""),
                            "recipe_name": metadata.get("recipe_name", ""),
                            "section_name": metadata.get("section_name", ""),
                            "order_index": metadata.get("order_index", metadata.get("chunk_id", i)),
                            "knowledge_tier": metadata.get(
                                "knowledge_tier",
                                source_tier_by_name.get(
                                    metadata.get("source", ""), TIER_1_RECIPE_OPS
                                ),
                            ),
                        }
                    )

            logger.info("RAG search returned %s chunks.", len(output))

            if not output:
                try:
                    unfiltered = self.collection.query(
                        query_texts=[query_text],
                        n_results=1,
                    )
                    unfiltered_hits = len((unfiltered.get("documents") or [[]])[0])
                    if unfiltered_hits > 0:
                        top_meta = (unfiltered.get("metadatas") or [[{}]])[0][0] or {}
                        logger.warning(
                            "RAG retrieval returned 0 with active-source filter; unfiltered hits exist. "
                            "top_unfiltered_source=%s active_sources=%s",
                            top_meta.get("source", "unknown"),
                            active_sources[:20],
                        )
                except Exception as diagnostic_error:
                    logger.warning("RAG retrieval diagnostic failed: %s", diagnostic_error)
            return output

        except Exception as exc:
            logger.error("RAG search error: %s", exc)
            return []

    def query(self, query_text: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """Backward-compatible alias for search()."""
        return self.search(query_text=query_text, n_results=n_results)

    def clear_database(self) -> None:
        self.chroma_client.delete_collection(COLLECTION_NAME)
        self.collection = self.chroma_client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=self.embedding_func,
        )


# Singleton instance for import
rag_engine = RAGEngine()
