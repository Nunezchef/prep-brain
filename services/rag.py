import os
import chromadb
import uuid
import json
import time
import logging
import fitz  # pymupdf
from pathlib import Path
from chromadb.utils import embedding_functions
from sentence_transformers import SentenceTransformer

# Set up logging
logger = logging.getLogger(__name__)

# Constants
PERSIST_DIRECTORY = "data/chroma_db"
COLLECTION_NAME = "prep_brain_knowledge"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
SOURCES_FILE = "data/sources.json"

class SmartChunker:
    """
    Intelligent chunking strategy for cooking/reference texts.
    Detects headings (UPPERCASE or Bold) and groups content by section.
    """
    def __init__(self, target_size=1000, overlap=100):
        self.target_size = target_size
        self.overlap = overlap

    def chunk_pdf(self, path: Path):
        doc = fitz.open(path)
        chunks = []
        current_chunk = []
        current_length = 0
        current_heading = "General"
        
        for page in doc:
            # Extract blocks to get structure
            blocks = page.get_text("dict")["blocks"]
            for b in blocks:
                if b['type'] == 0:  # Text block
                    for line in b["lines"]:
                        for span in line["spans"]:
                            text = span["text"].strip()
                            if not text:
                                continue
                                
                            # Heuristic for Heading:
                            # 1. Font size > 12 (approx, depends on doc) 
                            # 2. OR All Caps and length < 50
                            # 3. OR Bold font style (if detected)
                            is_heading = (span["size"] > 14) or (text.isupper() and len(text) < 60)
                            
                            if is_heading:
                                # If we have a substantial chunk built up, save it
                                if current_chunk and current_length > 100:
                                    full_text = "\\n".join(current_chunk)
                                    chunks.append({
                                        "text": full_text,
                                        "heading": current_heading
                                    })
                                    current_chunk = []
                                    current_length = 0
                                
                                current_heading = text
                                # Headings are also part of the context for the next chunk
                                current_chunk.append(f"## {text}") 
                                current_length += len(text)
                            else:
                                current_chunk.append(text)
                                current_length += len(text)
                                
                                # Chunk splitting if too large
                                if current_length > self.target_size:
                                    full_text = "\\n".join(current_chunk)
                                    chunks.append({
                                        "text": full_text,
                                        "heading": current_heading
                                    })
                                    # Keep last few lines for overlap
                                    keep_lines = current_chunk[-3:] if len(current_chunk) > 3 else []
                                    current_chunk = keep_lines
                                    current_length = sum(len(l) for l in keep_lines)

        # Flush remainder
        if current_chunk:
             chunks.append({
                "text": "\\n".join(current_chunk),
                "heading": current_heading
            })
            
        doc.close()
        return chunks

class RAGEngine:
    def __init__(self):
        # Initialize paths
        Path(PERSIST_DIRECTORY).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize Vector DB
        self.chroma_client = chromadb.PersistentClient(
            path=PERSIST_DIRECTORY,
            settings=chromadb.Settings(anonymized_telemetry=False)
        )
        
        # Initialize Embedding Function
        self.embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        self.embedding_func = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=EMBEDDING_MODEL_NAME)

        # Get or create collection
        self.collection = self.chroma_client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=self.embedding_func
        )
        
        # Initialize Sources
        self.sources_file = Path(SOURCES_FILE)
        if not self.sources_file.exists():
            self._save_sources([])
            
        logger.info(f"RAG Engine initialized. Collection size: {self.collection.count()}")

    def _load_sources(self):
        try:
            with open(self.sources_file, "r") as f:
                return json.load(f)
        except Exception:
            return []

    def _save_sources(self, sources):
        with open(self.sources_file, "w") as f:
            json.dump(sources, f, indent=2)

    def get_sources(self):
        return self._load_sources()

    def toggle_source(self, source_id: str, active: bool):
        sources = self._load_sources()
        for s in sources:
            if s["id"] == source_id:
                s["status"] = "active" if active else "disabled"
                self._save_sources(sources)
                return True
        return False

    def delete_source(self, source_id: str):
        sources = self._load_sources()
        source_name = None
        new_sources = []
        for s in sources:
            if s["id"] == source_id:
                source_name = s["source_name"] # Using filename as key for chroma deletion
            else:
                new_sources.append(s)
        
        if source_name:
            # Delete from Chroma
            try:
                self.collection.delete(where={"source": source_name})
            except Exception as e:
                logger.error(f"Error deleting chunks for {source_name}: {e}")
            
            # Save new list
            self._save_sources(new_sources)
            return True
        return False

    def ingest_file(self, file_path: str, extra_metadata: dict = None):
        """Reads a file, intelligently chunks it using PyMuPDF, and adds to Vector DB."""
        if extra_metadata is None:
            extra_metadata = {}

        path = Path(file_path)
        if not path.exists():
            return False, "File not found."
        
        try:
            chunker = SmartChunker()
            chunks_data = []
            
            if path.suffix.lower() == ".pdf":
                chunks_data = chunker.chunk_pdf(path)
            else:
                # Text fallback
                text = path.read_text(errors='replace')
                chunks_data = [{"text": text, "heading": "General"}]
            
            if not chunks_data:
                return False, "Extraction yielded no content. Is this a scanned PDF?"

            total_chars = sum(len(c["text"]) for c in chunks_data)
            if total_chars < 100:
                 return False, f"Extracted only {total_chars} characters. Likely logic/OCR failure."

            # --- METADATA ENRICHMENT ---
            import datetime
            date_ingested = datetime.datetime.now().isoformat()
            source_id = str(uuid.uuid4())
            
            ids = []
            metadatas = []
            documents = []
            
            for i, item in enumerate(chunks_data):
                chunk_id = f"{path.name}_{i}_{source_id[:8]}"
                ids.append(chunk_id)
                documents.append(item["text"])
                
                meta = {
                    "source": path.name,
                    "chunk_id": i,
                    "date_ingested": date_ingested,
                    "source_type": extra_metadata.get("source_type", "document"),
                    "source_title": extra_metadata.get("source_title", path.stem),
                    "heading": item["heading"]
                }
                metadatas.append(meta)
            
            # Remove old if exists (re-ingestion)
            try:
                self.collection.delete(where={"source": path.name})
            except:
                pass

            # Add to collection
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            
            # --- SAVE SOURCE RECORD ---
            sources = self._load_sources()
            # Remove old entry if exists
            sources = [s for s in sources if s["source_name"] != path.name]
            
            new_source = {
                "id": source_id,
                "source_name": path.name, 
                "title": extra_metadata.get("source_title", path.stem),
                "type": extra_metadata.get("source_type", "document"),
                "date_ingested": date_ingested,
                "chunk_count": len(documents),
                "status": "active",
                "summary": extra_metadata.get("summary", "No summary provided.")
            }
            sources.append(new_source)
            self._save_sources(sources)

            result = {
                "num_chunks": len(documents),
                "source_title": new_source["title"],
                "date": date_ingested
            }
            return True, result

        except Exception as e:
            logger.exception(f"Error ingesting {file_path}")
            return False, str(e)

    def query(self, query_text: str, n_results: int = 5):
        """Searches the vector DB for relevant context."""
        try:
            # 1. Get active sources
            sources = self._load_sources()
            active_sources = [s["source_name"] for s in sources if s.get("status") == "active"]
            
            if not active_sources:
                return []

            # 2. Filter query
            where_clause = {"source": {"$in": active_sources}}

            results = self.collection.query(
                query_texts=[query_text],
                n_results=n_results,
                where=where_clause
            )
            
            output = []
            if results["documents"]:
                for i, doc in enumerate(results["documents"][0]):
                    meta = results["metadatas"][0][i]
                    output.append({
                        "content": doc,
                        "source": meta.get("source", "unknown"),
                        "heading": meta.get("heading", ""),
                        "distance": results["distances"][0][i] if "distances" in results else 0
                    })
            return output

        except Exception as e:
            logger.error(f"RAG Query error: {e}")
            return []

    def clear_database(self):
        self.chroma_client.delete_collection(COLLECTION_NAME)
        self.collection = self.chroma_client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=self.embedding_func
        )

# Singleton instance for import
rag_engine = RAGEngine()
