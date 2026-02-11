from __future__ import annotations

from typing import Dict, Any

from prep_brain.config import load_config


def chunking_defaults() -> Dict[str, Any]:
    cfg = load_config()
    rag_cfg = cfg.get("rag", {}) if isinstance(cfg.get("rag", {}), dict) else {}
    return {
        "chunk_size_chars": int(rag_cfg.get("chunk_size_chars", 3500)),
        "chunk_overlap_chars": int(rag_cfg.get("chunk_overlap_chars", 400)),
        "minimum_chunk_chars": int(rag_cfg.get("minimum_chunk_chars", 400)),
    }
