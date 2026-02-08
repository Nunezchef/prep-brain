import base64
import json
import logging
import shutil
import subprocess
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zipfile import ZipFile

import chromadb
import fitz  # pymupdf
import requests
import yaml
from chromadb.utils import embedding_functions
from sentence_transformers import SentenceTransformer

# Set up logging
logger = logging.getLogger(__name__)

# Constants
PERSIST_DIRECTORY = "data/chroma_db"
COLLECTION_NAME = "prep_brain_knowledge"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
SOURCES_FILE = "data/sources.json"
DOCX_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

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
}


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

    # Default to operational authority so recipe queries don't accidentally route to reference books.
    return TIER_1_RECIPE_OPS


def load_runtime_config() -> Dict[str, Any]:
    config_path = Path("config.yaml")
    if not config_path.exists():
        return {}
    try:
        return yaml.safe_load(config_path.read_text()) or {}
    except Exception:
        return {}


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


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

                            is_heading = (span.get("size", 0) > 14) or (text.isupper() and len(text) < 60)

                            if is_heading:
                                if current_chunk and current_length > 100:
                                    chunks.append({
                                        "text": "\n".join(current_chunk),
                                        "heading": current_heading,
                                    })
                                    current_chunk = []
                                    current_length = 0

                                current_heading = text
                                current_chunk.append(f"## {text}")
                                current_length += len(text)
                            else:
                                current_chunk.append(text)
                                current_length += len(text)

                                if current_length > self.target_size:
                                    chunks.append({
                                        "text": "\n".join(current_chunk),
                                        "heading": current_heading,
                                    })
                                    keep_lines = current_chunk[-3:] if len(current_chunk) > 3 else []
                                    current_chunk = keep_lines
                                    current_length = sum(len(line) for line in keep_lines)

            if current_chunk:
                chunks.append({
                    "text": "\n".join(current_chunk),
                    "heading": current_heading,
                })

            return chunks
        finally:
            doc.close()


class RAGEngine:
    def __init__(self):
        Path(PERSIST_DIRECTORY).parent.mkdir(parents=True, exist_ok=True)

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

    def _extract_pdf_images(self, path: Path, source_id: str, max_images: int) -> List[Dict[str, Any]]:
        images_dir = Path("data/extracted_images") / source_id
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
            warnings.append("Vision descriptions requested, but no images were available to describe.")
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

        base_url = ollama_cfg.get("base_url", "http://localhost:11434")
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

    def _docx_paragraph_text(self, paragraph_element: ET.Element) -> str:
        texts = [node.text for node in paragraph_element.findall(".//w:t", DOCX_NS) if node.text]
        return "".join(texts).strip()

    def _extract_docx_text(self, path: Path) -> Tuple[str, int, int]:
        lines: List[str] = []
        paragraph_count = 0
        table_row_count = 0

        with ZipFile(path, "r") as archive:
            if "word/document.xml" not in archive.namelist():
                return "", 0, 0
            xml_bytes = archive.read("word/document.xml")

        root = ET.fromstring(xml_bytes)
        body = root.find("w:body", DOCX_NS)
        if body is None:
            return "", 0, 0

        paragraph_tag = f"{{{DOCX_NS['w']}}}p"
        table_tag = f"{{{DOCX_NS['w']}}}tbl"

        for child in list(body):
            if child.tag == paragraph_tag:
                paragraph_text = self._docx_paragraph_text(child)
                if paragraph_text:
                    lines.append(paragraph_text)
                    paragraph_count += 1
                continue

            if child.tag != table_tag:
                continue

            for row_index, row in enumerate(child.findall("w:tr", DOCX_NS), start=1):
                row_cells: List[str] = []
                for cell in row.findall("w:tc", DOCX_NS):
                    cell_paragraphs: List[str] = []
                    for paragraph in cell.findall(".//w:p", DOCX_NS):
                        paragraph_text = self._docx_paragraph_text(paragraph)
                        if paragraph_text:
                            cell_paragraphs.append(paragraph_text)
                    row_cells.append(" ".join(cell_paragraphs).strip())

                populated_cells = [cell for cell in row_cells if cell]
                if populated_cells:
                    row_text = " | ".join(populated_cells)
                    lines.append(f"TABLE ROW {row_index}: {row_text}")
                    table_row_count += 1

        normalized_lines = [" ".join(line.split()) for line in lines if " ".join(line.split())]
        return "\n".join(normalized_lines), paragraph_count, table_row_count

    def _chunk_text_blocks(
        self,
        text: str,
        heading: str = "General",
        target_size: int = 900,
        overlap_lines: int = 2,
    ) -> List[Dict[str, str]]:
        lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
        if not lines:
            return []

        chunks: List[Dict[str, str]] = []
        current_lines: List[str] = []
        current_length = 0

        for line in lines:
            line_length = len(line) + 1
            if current_lines and current_length + line_length > target_size:
                chunks.append({"text": "\n".join(current_lines), "heading": heading})
                overlap = current_lines[-overlap_lines:] if overlap_lines > 0 else []
                current_lines = list(overlap)
                current_length = sum(len(value) + 1 for value in current_lines)

            current_lines.append(line)
            current_length += line_length

        if current_lines:
            chunks.append({"text": "\n".join(current_lines), "heading": heading})

        # Safety split to avoid giant single-chunk ingestion for non-tiny documents.
        if len(chunks) == 1:
            compact_text = " ".join(lines)
            if len(compact_text) > target_size * 2:
                words = compact_text.split()
                midpoint = len(words) // 2
                first = " ".join(words[:midpoint]).strip()
                second = " ".join(words[midpoint:]).strip()
                if first and second:
                    chunks = [
                        {"text": first, "heading": heading},
                        {"text": second, "heading": heading},
                    ]

        return chunks

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

        settings = self._get_settings()
        image_cfg = settings.get("image_processing", {})
        vision_cfg = settings.get("vision", {})
        knowledge_tier = (
            normalize_knowledge_tier(extra_metadata.get("knowledge_tier"))
            or infer_knowledge_tier(
                source_type=extra_metadata.get("source_type", "document"),
                title=extra_metadata.get("source_title", path.stem),
                source_name=path.name,
                summary=extra_metadata.get("summary", ""),
            )
        )

        options = {
            "extract_images": bool(image_cfg.get("extract_images", False)),
            "vision_descriptions": bool(vision_cfg.get("enabled", False)),
        }
        if ingestion_options:
            options.update({k: bool(v) for k, v in ingestion_options.items() if k in options})

        # Vision descriptions require images to be extracted first.
        if options["vision_descriptions"]:
            options["extract_images"] = True

        is_pdf = path.suffix.lower() == ".pdf"
        is_docx = path.suffix.lower() == ".docx"
        chunker = SmartChunker()

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

        source_id = str(uuid.uuid4())
        date_ingested = __import__("datetime").datetime.now().isoformat()

        ocr_working_pdf = path
        temp_ocr_path: Optional[Path] = None

        try:
            if is_pdf:
                profile_before = self._profile_pdf(path)
                image_rich = self._is_image_rich(profile_before, settings)
                ocr_required = self._should_apply_ocr(profile_before, settings)

                if ocr_required:
                    pipeline["ocr"] = "required"
                    if not bool(settings.get("ocr", {}).get("enabled", True)):
                        return (
                            False,
                            (
                                "Ingestion blocked: this PDF appears image-heavy and requires OCR before indexing. "
                                f"Recommended preprocessing: ocrmypdf --skip-text '{path}' '{path.stem}_ocr.pdf'"
                            ),
                        )

                    temp_ocr_path = Path("data/tmp/ocr") / f"{path.stem}_{source_id[:8]}.pdf"
                    ok, error_msg = self._run_ocr(path, temp_ocr_path, settings["ocr"].get("tool", "ocrmypdf"))
                    if not ok:
                        return False, error_msg

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
                    {
                        "text": chunk["text"],
                        "heading": chunk["heading"],
                        "kind": "text",
                    }
                    for chunk in chunks
                ]
            elif is_docx:
                extracted_text, paragraph_count, table_row_count = self._extract_docx_text(path)
                logger.info(
                    "DOCX extraction: file=%s raw_text_chars=%s paragraphs=%s table_rows=%s",
                    path.name,
                    len(extracted_text),
                    paragraph_count,
                    table_row_count,
                )
                text_chunks = self._chunk_text_blocks(
                    extracted_text,
                    heading="DOCX Content",
                    target_size=900,
                    overlap_lines=2,
                )
                logger.info("DOCX chunking: file=%s chunks_created=%s", path.name, len(text_chunks))
                chunks_data = [
                    {
                        "text": chunk["text"],
                        "heading": chunk["heading"],
                        "kind": "text",
                    }
                    for chunk in text_chunks
                ]
            else:
                text = path.read_text(errors="replace")
                text_chunks = self._chunk_text_blocks(
                    text,
                    heading="General",
                    target_size=1000,
                    overlap_lines=2,
                )
                chunks_data = [
                    {
                        "text": chunk["text"],
                        "heading": chunk["heading"],
                        "kind": "text",
                    }
                    for chunk in text_chunks
                ]

            if not chunks_data:
                return (
                    False,
                    (
                        "Extraction yielded no text. For scanned/image-heavy PDFs, OCR is mandatory. "
                        f"Recommended preprocessing: ocrmypdf --skip-text '{path}' '{path.stem}_ocr.pdf'"
                    ),
                )

            total_chars = sum(len(chunk["text"]) for chunk in chunks_data)
            if total_chars < 100:
                if is_pdf and (image_rich or ocr_required):
                    if not ocr_applied:
                        return (
                            False,
                            (
                                "Ingestion blocked: most content appears image-based and OCR was not applied. "
                                f"Run: ocrmypdf --skip-text '{path}' '{path.stem}_ocr.pdf'"
                            ),
                        )

                    return (
                        False,
                        (
                            "OCR ran but extracted too little text to index safely. "
                            "Please verify OCR output quality before ingesting."
                        ),
                    )
                return False, f"Extracted only {total_chars} characters. Ingestion aborted."

            if is_pdf and options["extract_images"]:
                pipeline["image_extraction"] = "applied"
                max_images = int(image_cfg.get("max_images", 30))
                extracted_images = self._extract_pdf_images(ocr_working_pdf, source_id, max_images=max_images)
                if not extracted_images:
                    warnings.append("Image extraction enabled, but no extractable images were found.")

            if is_pdf and options["vision_descriptions"]:
                pipeline["vision_descriptions"] = "applied"
                vision_chunks, vision_warnings = self._vision_descriptions(extracted_images, settings)
                chunks_data.extend(vision_chunks)
                warnings.extend(vision_warnings)

            ids: List[str] = []
            metadatas: List[Dict[str, Any]] = []
            documents: List[str] = []

            for i, item in enumerate(chunks_data):
                chunk_id = f"{path.name}_{i}_{source_id[:8]}"
                ids.append(chunk_id)
                documents.append(item["text"])

                metadatas.append(
                    {
                        "source": path.name,
                        "chunk_id": i,
                        "date_ingested": date_ingested,
                        "source_type": extra_metadata.get("source_type", "document"),
                        "source_title": extra_metadata.get("source_title", path.stem),
                        "heading": item.get("heading", "General"),
                        "chunk_kind": item.get("kind", "text"),
                        "ocr_applied": bool(ocr_applied),
                        "knowledge_tier": knowledge_tier,
                    }
                )

            pipeline["chunking_embeddings"] = "applied"

            try:
                self.collection.delete(where={"source": path.name})
            except Exception:
                pass

            self.collection.add(documents=documents, metadatas=metadatas, ids=ids)

            sources = self._load_sources()
            sources = [source for source in sources if source["source_name"] != path.name]

            extracted_image_dir = (
                str((Path("data/extracted_images") / source_id).resolve()) if extracted_images else ""
            )

            new_source = {
                "id": source_id,
                "source_name": path.name,
                "collection_name": COLLECTION_NAME,
                "title": extra_metadata.get("source_title", path.stem),
                "type": extra_metadata.get("source_type", "document"),
                "knowledge_tier": knowledge_tier,
                "date_ingested": date_ingested,
                "chunk_count": len(documents),
                "status": "active",
                "summary": extra_metadata.get("summary", "No summary provided."),
                "ocr_required": bool(ocr_required),
                "ocr_applied": bool(ocr_applied),
                "ocr_tool": settings["ocr"].get("tool", "ocrmypdf") if is_pdf else "",
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
                "ingestion_pipeline": pipeline,
                "warnings": warnings,
                "ingestion_complete": True,
            }
            sources.append(new_source)
            self._save_sources(sources)

            result = {
                "num_chunks": len(documents),
                "source_title": new_source["title"],
                "date": date_ingested,
                "ocr_required": new_source["ocr_required"],
                "ocr_applied": new_source["ocr_applied"],
                "image_rich": new_source["image_rich"],
                "images_extracted": new_source["images_extracted"],
                "vision_descriptions_count": new_source["vision_descriptions_count"],
                "knowledge_tier": new_source["knowledge_tier"],
                "warnings": warnings,
            }
            return True, result

        except Exception as exc:
            logger.exception("Error ingesting %s", file_path)
            return False, str(exc)

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
                tier for tier in (normalize_knowledge_tier(value) for value in (source_tiers or [])) if tier
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
                            "knowledge_tier": metadata.get(
                                "knowledge_tier",
                                source_tier_by_name.get(metadata.get("source", ""), TIER_1_RECIPE_OPS),
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
