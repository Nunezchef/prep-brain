import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from services import brain, costing, memory, notifier, prep_list, rag, recipes
from services.web_research import WebResearchClient
from prep_brain.config import load_config, resolve_path

logger = logging.getLogger(__name__)

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover - non-posix fallback
    fcntl = None  # type: ignore

ALLERGEN_ALIASES = {
    "milk": "Milk",
    "egg": "Eggs",
    "eggs": "Eggs",
    "fish": "Fish",
    "shellfish": "Shellfish",
    "tree nut": "Tree Nuts",
    "tree nuts": "Tree Nuts",
    "nut": "Tree Nuts",
    "nuts": "Tree Nuts",
    "peanut": "Peanuts",
    "peanuts": "Peanuts",
    "wheat": "Wheat",
    "soy": "Soybeans",
    "soybean": "Soybeans",
    "soybeans": "Soybeans",
    "sesame": "Sesame",
}

LOG_SECRET_PATTERNS = [
    re.compile(r"(?i)\b(token|api[_-]?key|password|secret|authorization)\b\s*[:=]\s*([^\s,;]+)"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._\-]+\b"),
]

ACTIVE_INGEST_STATUSES = {
    "queued",
    "extracting",
    "chunking",
    "indexing",
    "extracting_recipes",
    "enriching",
    "promoting",
}


def _sanitize_error_text(raw: Any, limit: int = 400) -> str:
    cleaned = str(raw or "")
    for pattern in LOG_SECRET_PATTERNS:
        if "Bearer" in pattern.pattern:
            cleaned = pattern.sub("Bearer [REDACTED]", cleaned)
        else:
            cleaned = pattern.sub(r"\1=[REDACTED]", cleaned)
    return cleaned[:limit]


def normalize_job_source_type(source_type: str, knowledge_tier: Optional[str] = None) -> str:
    st = str(source_type or "").strip().lower()
    tier = rag.normalize_knowledge_tier(knowledge_tier)
    if st in {
        "restaurant_recipes",
        "house_recipe_book",
        "house_recipe_document",
        "house_recipe",
        "prep_notes",
    }:
        return "restaurant_recipes"
    if st in {"general_knowledge", "reference_book"}:
        return "general_knowledge"
    if tier == rag.TIER_1_RECIPE_OPS:
        return "restaurant_recipes"
    if tier == rag.TIER_3_REFERENCE_THEORY:
        return "general_knowledge"
    return "unknown"


def queue_ingest_job(
    *,
    source_filename: str,
    source_type: str,
    restaurant_tag: Optional[str] = None,
) -> Dict[str, Any]:
    ingest_id = uuid.uuid4().hex
    normalized_type = normalize_job_source_type(source_type)
    file_path = resolve_path("data/documents") / str(source_filename or "").strip()
    file_size = int(file_path.stat().st_size) if file_path.exists() else 0
    file_sha256 = ""
    if file_path.exists():
        try:
            import hashlib

            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                for block in iter(lambda: f.read(1024 * 1024), b""):
                    hasher.update(block)
            file_sha256 = hasher.hexdigest()
        except Exception:
            file_sha256 = ""
    con = memory.get_conn()
    try:
        con.execute(
            """
            INSERT INTO doc_sources (
                ingest_id, filename, source_type, restaurant_tag, file_sha256, file_size,
                extracted_text_chars, chunk_count, chunks_added, status, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 'queued', CURRENT_TIMESTAMP)
            ON CONFLICT(ingest_id) DO UPDATE SET
                filename = excluded.filename,
                source_type = excluded.source_type,
                restaurant_tag = excluded.restaurant_tag,
                file_sha256 = excluded.file_sha256,
                file_size = excluded.file_size,
                status = 'queued',
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                ingest_id,
                str(source_filename or "").strip(),
                normalized_type,
                str(restaurant_tag or "").strip() or None,
                file_sha256,
                file_size,
            ),
        )
        cur = con.execute(
            """
            INSERT INTO ingest_jobs (
                ingest_id, source_filename, source_type, restaurant_tag, status,
                progress_current, progress_total, updated_at
            )
            VALUES (?, ?, ?, ?, 'queued', 0, 0, CURRENT_TIMESTAMP)
            """,
            (
                ingest_id,
                str(source_filename or "").strip(),
                normalized_type,
                str(restaurant_tag or "").strip() or None,
            ),
        )
        con.commit()
        return {
            "ok": True,
            "job_id": int(cur.lastrowid),
            "ingest_id": ingest_id,
            "source_type": normalized_type,
        }
    finally:
        con.close()


def list_ingest_jobs(limit: int = 5) -> List[Dict[str, Any]]:
    con = memory.get_conn()
    try:
        rows = con.execute(
            """
            SELECT *
            FROM ingest_jobs
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(int(limit), 50)),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def get_ingest_job(job_id: int) -> Optional[Dict[str, Any]]:
    con = memory.get_conn()
    try:
        row = con.execute("SELECT * FROM ingest_jobs WHERE id = ?", (int(job_id),)).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def get_autonomy_status_snapshot() -> Dict[str, Any]:
    con = memory.get_conn()
    try:
        row = con.execute("SELECT * FROM autonomy_status WHERE id = 1").fetchone()
        if not row:
            return {
                "is_running": 0,
                "last_tick_at": None,
                "queue_pending_drafts": 0,
                "queue_pending_ingests": 0,
            }
        return dict(row)
    finally:
        con.close()


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class AutonomyWorker:
    def __init__(self):
        self.config = load_config()
        autonomy_cfg = self.config.get("autonomy", {})

        self.mode = str(autonomy_cfg.get("mode", "balanced")).strip().lower()
        default_promote_threshold = 0.75 if self.mode == "balanced" else 0.85
        default_enrich_min = 0.60 if self.mode == "balanced" else 0.65

        self.enabled = bool(autonomy_cfg.get("enabled", True))
        self.interval = int(
            autonomy_cfg.get("cycle_interval_seconds", 2700 if self.mode == "balanced" else 1800)
        )
        self.poll_interval_seconds = int(autonomy_cfg.get("poll_interval_seconds", 300))
        self.telegram_alerts = bool(autonomy_cfg.get("telegram_alerts", False))
        self.ingest_completion_message = bool(autonomy_cfg.get("ingest_completion_message", False))
        self.alert_cooldown_minutes = int(autonomy_cfg.get("alert_cooldown_minutes", 180))
        self.auto_promote_threshold = float(
            autonomy_cfg.get("auto_promote_threshold", default_promote_threshold)
        )
        self.enrich_min_confidence = float(
            autonomy_cfg.get("enrich_min_confidence", default_enrich_min)
        )
        self.enrich_attempt_band_max = float(
            autonomy_cfg.get("enrich_attempt_band_max", self.auto_promote_threshold - 0.01)
        )
        self.draft_scan_limit = int(autonomy_cfg.get("draft_scan_limit", 10))
        self.max_source_chunks_per_draft = int(autonomy_cfg.get("max_source_chunks_per_draft", 12))
        self.min_source_chars_for_draft = int(autonomy_cfg.get("min_source_chars_for_draft", 300))
        web_cfg = (
            autonomy_cfg.get("web", {}) if isinstance(autonomy_cfg.get("web", {}), dict) else {}
        )
        self.web_enabled = bool(web_cfg.get("enabled", False))
        self.web_mode = str(web_cfg.get("mode", "research_only")).strip().lower()
        self.web_rate_limit_rps = float(web_cfg.get("rate_limit_rps", 0.5))
        self.web_max_pages_per_task = int(web_cfg.get("max_pages_per_task", 3))
        self.web_allowed_domains = [
            str(d).strip() for d in web_cfg.get("allowed_domains", []) if str(d).strip()
        ]

        self.running = False
        self._stop_event = asyncio.Event()
        self.last_run: Optional[datetime] = None
        self._last_maintenance_cycle_at: float = 0.0
        self._is_tick_running = False
        self.web_client: Optional[WebResearchClient] = None
        self._lock_handle = None
        self._lock_path = resolve_path("run/autonomy.singleton.lock")
        self._singleton_owner = False

        # Ensure required tables exist even when worker runs standalone.
        memory.init_db()
        if self.web_enabled and self.web_mode == "research_only":
            self.web_client = WebResearchClient(
                enabled=True,
                mode=self.web_mode,
                rate_limit_rps=self.web_rate_limit_rps,
                max_pages_per_task=self.web_max_pages_per_task,
                allowed_domains=self.web_allowed_domains,
            )

    def _redact_sensitive(self, text: Optional[str]) -> str:
        cleaned = str(text or "")
        for pattern in LOG_SECRET_PATTERNS:
            if "Bearer" in pattern.pattern:
                cleaned = pattern.sub("Bearer [REDACTED]", cleaned)
            else:
                cleaned = pattern.sub(r"\1=[REDACTED]", cleaned)
        return cleaned

    def _acquire_singleton(self) -> bool:
        if self._singleton_owner:
            return True
        if fcntl is None:
            # Non-posix fallback: best effort (no hard lock).
            self._singleton_owner = True
            return True
        try:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_handle = open(self._lock_path, "a+")
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._lock_handle.seek(0)
            self._lock_handle.truncate(0)
            self._lock_handle.write(str(os.getpid()))
            self._lock_handle.flush()
            self._singleton_owner = True
            return True
        except Exception:
            self._singleton_owner = False
            return False

    def _release_singleton(self) -> None:
        if not self._lock_handle:
            self._singleton_owner = False
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._lock_handle.close()
        except Exception:
            pass
        self._lock_handle = None
        self._singleton_owner = False

    def _queue_depths(self, con) -> Tuple[int, int]:
        pending_drafts = int(
            con.execute(
                "SELECT COUNT(*) FROM recipe_drafts WHERE status IN ('pending','enriched')"
            ).fetchone()[0]
        )
        pending_ingests = int(
            con.execute(
                f"""
                SELECT COUNT(*) FROM ingest_jobs
                WHERE status IN ({",".join("?" for _ in ACTIVE_INGEST_STATUSES)})
                """,
                tuple(ACTIVE_INGEST_STATUSES),
            ).fetchone()[0]
        )
        return pending_drafts, pending_ingests

    def _set_status(self, **fields: Any) -> None:
        allowed = {
            "is_running",
            "last_tick_at",
            "last_cycle_started_at",
            "last_cycle_finished_at",
            "last_action",
            "last_error",
            "last_error_at",
            "queue_pending_drafts",
            "queue_pending_ingests",
            "last_promoted_recipe_id",
            "last_promoted_recipe_name",
            "last_promoted_at",
        }
        updates: Dict[str, Any] = {}
        for key, value in fields.items():
            if key not in allowed:
                continue
            if key == "last_error":
                updates[key] = _sanitize_error_text(value)
            else:
                updates[key] = value
        if not updates:
            return
        sql = ", ".join(f"{key} = ?" for key in updates.keys())
        params = list(updates.values()) + [1]
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            con = memory.get_conn()
            try:
                con.execute("INSERT OR IGNORE INTO autonomy_status (id, is_running) VALUES (1, 0)")
                con.execute(f"UPDATE autonomy_status SET {sql} WHERE id = ?", params)
                con.commit()
                return
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower() and attempt < max_attempts:
                    time.sleep(0.05 * attempt)
                    continue
                raise
            finally:
                con.close()

    def _refresh_status_queues(self, *, action: Optional[str] = None) -> None:
        con = memory.get_conn()
        try:
            pending_drafts, pending_ingests = self._queue_depths(con)
        finally:
            con.close()
        payload: Dict[str, Any] = {
            "last_tick_at": datetime.now().isoformat(),
            "queue_pending_drafts": pending_drafts,
            "queue_pending_ingests": pending_ingests,
        }
        if action:
            payload["last_action"] = action
        self._set_status(**payload)

    def _update_ingest_job(
        self,
        job_id: int,
        *,
        status: Optional[str] = None,
        progress_current: Optional[int] = None,
        progress_total: Optional[int] = None,
        error: Optional[str] = None,
        promoted_count: Optional[int] = None,
        needs_review_count: Optional[int] = None,
        started: bool = False,
        finished: bool = False,
    ) -> None:
        updates: List[str] = ["updated_at = CURRENT_TIMESTAMP"]
        params: List[Any] = []
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if progress_current is not None:
            updates.append("progress_current = ?")
            params.append(int(progress_current))
        if progress_total is not None:
            updates.append("progress_total = ?")
            params.append(int(progress_total))
        if error is not None:
            updates.append("error = ?")
            params.append(_sanitize_error_text(error))
        if promoted_count is not None:
            updates.append("promoted_count = ?")
            params.append(int(promoted_count))
        if needs_review_count is not None:
            updates.append("needs_review_count = ?")
            params.append(int(needs_review_count))
        if started:
            updates.append("started_at = COALESCE(started_at, CURRENT_TIMESTAMP)")
        if finished:
            updates.append("finished_at = CURRENT_TIMESTAMP")
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            con = memory.get_conn()
            try:
                con.execute(
                    f"UPDATE ingest_jobs SET {', '.join(updates)} WHERE id = ?",
                    tuple(params + [int(job_id)]),
                )
                con.commit()
                return
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower() and attempt < max_attempts:
                    time.sleep(0.05 * attempt)
                    continue
                raise
            finally:
                con.close()

    def _classify_needs_review_reason(
        self, *, report: Optional[Dict[str, Any]], source_type: str
    ) -> str:
        if source_type != "restaurant_recipes":
            return "classified_as_general_knowledge"
        if not report:
            return "validation_failed"
        extraction = report.get("extraction_metrics", {}) if isinstance(report, dict) else {}
        warnings = [str(item) for item in (report.get("warnings") or [])]
        if int(extraction.get("extracted_text_chars", 0)) == 0:
            return "extraction_empty"
        if any(item.startswith("tables_present_but_empty_extraction") for item in warnings):
            return "table_parse_missing"
        if any(item.startswith("dedupe_collapse") for item in warnings) or any(
            item.startswith("low_chunk_count") for item in warnings
        ):
            return "chunk_collapse"
        return "validation_failed"

    def log_action(
        self,
        action: str,
        target_type: Optional[str] = None,
        target_id: Optional[str] = None,
        detail: Optional[str] = None,
        conf_before: float = 0.0,
        conf_after: float = 0.0,
    ) -> None:
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            con = memory.get_conn()
            try:
                con.execute(
                    """
                    INSERT INTO autonomy_log (action, target_type, target_id, detail, confidence_before, confidence_after)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        action,
                        target_type,
                        str(target_id) if target_id is not None else None,
                        self._redact_sensitive(detail)[:4000],
                        conf_before,
                        conf_after,
                    ),
                )
                con.commit()
                return
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower() and attempt < max_attempts:
                    time.sleep(0.05 * attempt)
                    continue
                logger.error("Failed to log autonomy action: %s", exc)
                return
            except Exception as exc:
                logger.error("Failed to log autonomy action: %s", exc)
                return
            finally:
                con.close()

    def _has_recent_log(
        self,
        action: str,
        target_type: Optional[str],
        target_id: Optional[str],
        minutes: int,
    ) -> bool:
        con = memory.get_conn()
        try:
            row = con.execute(
                """
                SELECT 1
                FROM autonomy_log
                WHERE action = ?
                  AND (? IS NULL OR target_type = ?)
                  AND (? IS NULL OR target_id = ?)
                  AND created_at >= datetime('now', ?)
                LIMIT 1
                """,
                (
                    action,
                    target_type,
                    target_type,
                    str(target_id) if target_id is not None else None,
                    str(target_id) if target_id is not None else None,
                    f"-{int(minutes)} minutes",
                ),
            ).fetchone()
            return row is not None
        except Exception:
            return False
        finally:
            con.close()

    def _alert_if_required(
        self,
        alert_key: str,
        message: str,
        throttle_minutes: Optional[int] = None,
    ) -> None:
        if not self.telegram_alerts:
            return

        cooldown = int(throttle_minutes or self.alert_cooldown_minutes)
        if self._has_recent_log(
            action="alert_sent",
            target_type="autonomy_alert",
            target_id=alert_key,
            minutes=cooldown,
        ):
            return

        sent = bool(notifier.send_telegram_notification(message))
        self.log_action(
            "alert_sent" if sent else "alert_failed",
            target_type="autonomy_alert",
            target_id=alert_key,
            detail=message,
        )

    def _build_source_index(self) -> Dict[str, Dict[str, Any]]:
        index: Dict[str, Dict[str, Any]] = {}
        try:
            for source in rag.rag_engine.get_sources():
                source_id = source.get("id")
                if source_id:
                    index[source_id] = source
        except Exception as exc:
            logger.warning("Could not build source index for autonomy: %s", exc)
        return index

    def _resolve_draft_tier(
        self,
        draft: Dict[str, Any],
        source_index: Dict[str, Dict[str, Any]],
    ) -> str:
        stored = rag.normalize_knowledge_tier(draft.get("knowledge_tier"))
        if stored:
            return stored

        source = source_index.get(draft.get("source_id"))
        if source:
            source_tier = rag.normalize_knowledge_tier(
                source.get("knowledge_tier")
            ) or rag.infer_knowledge_tier(
                source_type=source.get("type", ""),
                title=source.get("title", ""),
                source_name=source.get("source_name", ""),
                summary=source.get("summary", ""),
            )
            if source_tier:
                return source_tier

        return rag.infer_knowledge_tier(
            source_type="recipe_draft",
            title=draft.get("name", ""),
            source_name=draft.get("source_id", ""),
            summary=draft.get("raw_text", "")[:300],
        )

    def _extract_json_object(self, text: str) -> Dict[str, Any]:
        content = (text or "").strip()
        if not content:
            raise ValueError("Empty model response.")

        if "```json" in content:
            content = content.split("```json", 1)[1].split("```", 1)[0].strip()
        elif "```" in content:
            content = content.split("```", 1)[1].split("```", 1)[0].strip()

        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            parsed = json.loads(content[start : end + 1])
            if isinstance(parsed, dict):
                return parsed

        raise ValueError("Could not parse JSON object from model response.")

    def _sanitize_ingredients(self, ingredients_raw: Any) -> List[Dict[str, Any]]:
        if not isinstance(ingredients_raw, list):
            return []

        cleaned: List[Dict[str, Any]] = []
        for ingredient in ingredients_raw:
            if not isinstance(ingredient, dict):
                continue

            item_name = " ".join(str(ingredient.get("item_name_text", "")).split()).strip()
            if not item_name:
                continue

            cleaned.append(
                {
                    "inventory_item_id": None,
                    "item_name_text": item_name[:200],
                    "quantity": _safe_float(ingredient.get("quantity")),
                    "unit": " ".join(str(ingredient.get("unit", "")).split())[:50] or None,
                    "notes": " ".join(str(ingredient.get("notes", "")).split())[:500] or None,
                }
            )
        return cleaned

    def _sanitize_allergens(self, allergens_raw: Any) -> List[str]:
        if not isinstance(allergens_raw, list):
            return []

        normalized: List[str] = []
        seen: Set[str] = set()
        for value in allergens_raw:
            key = " ".join(str(value).strip().lower().split())
            if not key:
                continue
            resolved = ALLERGEN_ALIASES.get(key, value)
            resolved_text = " ".join(str(resolved).split()).strip()
            if not resolved_text:
                continue
            dedupe_key = resolved_text.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            normalized.append(resolved_text)
        return normalized

    def _estimate_draft_confidence(self, text: str) -> float:
        base = 0.35
        lowered = (text or "").lower()

        if any(token in lowered for token in ("ingredient", "ingredients", "mise", "prep")):
            base += 0.2
        if any(token in lowered for token in ("method", "procedure", "steps", "instructions")):
            base += 0.2
        if re.search(r"\b\d+(?:\.\d+)?\s?(g|kg|oz|lb|ml|l|cup|cups|tbsp|tsp|qt|gal)\b", lowered):
            base += 0.15
        if len(text or "") >= 1200:
            base += 0.1

        return max(0.05, min(base, 0.9))

    def _structured_confidence(
        self,
        before: float,
        ingredient_count: int,
        has_method: bool,
        has_yield: bool,
    ) -> float:
        score = max(before, 0.45)
        if ingredient_count >= 2:
            score += 0.2
        if ingredient_count >= 5:
            score += 0.1
        if has_method:
            score += 0.15
        if has_yield:
            score += 0.1
        return max(0.05, min(score, 0.95))

    def _missing_required_fields(
        self,
        name_value: Optional[str],
        method_value: Optional[str],
        ingredients: List[Dict[str, Any]],
    ) -> List[str]:
        missing: List[str] = []
        if not str(name_value or "").strip():
            missing.append("name")
        if not str(method_value or "").strip():
            missing.append("method")
        if len(ingredients) < 1:
            missing.append("ingredients")
        return missing

    def _normalize_item_key(self, value: str) -> str:
        lowered = (value or "").lower()
        cleaned = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
        tokens = [token for token in cleaned.split() if token]
        singularized: List[str] = []
        for token in tokens:
            if len(token) > 4 and token.endswith("es"):
                singularized.append(token[:-2])
            elif len(token) > 3 and token.endswith("s"):
                singularized.append(token[:-1])
            else:
                singularized.append(token)
        return " ".join(singularized)

    def _best_inventory_match(
        self,
        ingredient_name: str,
        exact_map: Dict[str, int],
        normalized_map: Dict[str, List[int]],
    ) -> Optional[int]:
        lowered = " ".join((ingredient_name or "").strip().lower().split())
        if not lowered:
            return None

        if lowered in exact_map:
            return exact_map[lowered]

        normalized = self._normalize_item_key(lowered)
        if normalized in normalized_map and len(normalized_map[normalized]) == 1:
            return normalized_map[normalized][0]

        if normalized:
            candidates: Set[int] = set()
            for key, item_ids in normalized_map.items():
                if key == normalized:
                    candidates.update(item_ids)
                    continue
                if normalized in key or key in normalized:
                    candidates.update(item_ids)
            if len(candidates) == 1:
                return list(candidates)[0]

        return None

    def _is_recipe_candidate_source(self, source: Dict[str, Any]) -> bool:
        source_type = str(source.get("type", "") or "").strip().lower()
        title = str(source.get("title", "") or "").strip().lower()
        summary = str(source.get("summary", "") or "").strip().lower()
        source_name = str(source.get("source_name", "") or "").strip().lower()
        haystack = " ".join([source_type, title, summary, source_name])

        if source_type in {"reference_book", "unknown", "sop", "vendor_list"}:
            return False

        if source_type in {"house_recipe_book", "house_recipe_document", "house_recipe"}:
            return True

        if any(
            token in haystack
            for token in ("vendor", "invoice", "price list", "catalog", "order guide")
        ):
            return False
        if any(token in haystack for token in ("reference", "theory", "mcgee", "flavor bible")):
            return False

        return any(
            token in haystack
            for token in ("recipe", "dish", "prep recipe", "line recipe", "yield", "method")
        )

    def _has_recent_price_estimate(self, con, item_name: str, hours: int = 24) -> bool:
        row = con.execute(
            """
            SELECT 1
            FROM price_estimates
            WHERE LOWER(item_name) = LOWER(?)
              AND datetime(retrieved_at) >= datetime('now', ?)
            LIMIT 1
            """,
            (item_name, f"-{int(hours)} hours"),
        ).fetchone()
        return row is not None

    def _has_authoritative_cost(self, con, item_name: str, inventory_cost: Optional[float]) -> bool:
        if inventory_cost is not None and float(inventory_cost) > 0:
            return True

        inv = con.execute(
            """
            SELECT cost
            FROM inventory_items
            WHERE LOWER(name) = LOWER(?)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (item_name,),
        ).fetchone()
        if inv and inv["cost"] is not None and float(inv["cost"]) > 0:
            return True

        vendor = con.execute(
            """
            SELECT price
            FROM order_guide_items
            WHERE LOWER(item_name) = LOWER(?) AND is_active = 1 AND price IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (item_name,),
        ).fetchone()
        if vendor and vendor["price"] is not None and float(vendor["price"]) > 0:
            return True

        return False

    def _save_price_estimate(
        self,
        con,
        *,
        item_name: str,
        low_price: float,
        high_price: float,
        unit: str,
        source_urls: List[str],
        retrieved_at: Optional[str] = None,
    ) -> None:
        con.execute(
            """
            INSERT INTO price_estimates (
                item_name, low_price, high_price, unit, source_urls, knowledge_tier, retrieved_at
            )
            VALUES (?, ?, ?, ?, ?, 'general_knowledge_web', COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (
                item_name,
                float(low_price),
                float(high_price),
                unit,
                json.dumps(source_urls),
                retrieved_at,
            ),
        )

    def _web_estimate_missing_costs(self, con) -> int:
        if not self.web_client or not self.web_enabled or self.web_mode != "research_only":
            return 0

        estimated = 0
        seen: Set[str] = set()
        rows = con.execute("""
            SELECT
                ri.id AS ingredient_id,
                ri.item_name_text,
                ri.unit AS ingredient_unit,
                ii.name AS inventory_name,
                ii.cost AS inventory_cost
            FROM recipe_ingredients ri
            LEFT JOIN inventory_items ii ON ri.inventory_item_id = ii.id
            """).fetchall()

        for row in rows:
            ingredient_name = str(row["inventory_name"] or row["item_name_text"] or "").strip()
            if not ingredient_name:
                continue

            key = ingredient_name.lower()
            if key in seen:
                continue
            seen.add(key)

            if self._has_authoritative_cost(
                con, ingredient_name, _safe_float(row["inventory_cost"])
            ):
                continue
            if self._has_recent_price_estimate(con, ingredient_name, hours=24):
                continue

            try:
                estimate = self.web_client.research_price_estimate(
                    item_name=ingredient_name,
                    unit=str(row["ingredient_unit"] or "unit"),
                )
            except Exception as exc:
                self.log_action(
                    "web_price_estimate_error",
                    target_type="ingredient_name",
                    target_id=ingredient_name,
                    detail=f"Web research error for '{ingredient_name}': {exc}",
                )
                continue
            if not estimate:
                self.log_action(
                    "web_price_estimate_skipped",
                    target_type="ingredient_name",
                    target_id=ingredient_name,
                    detail=f"No conservative price estimate found for '{ingredient_name}'.",
                )
                continue

            self._save_price_estimate(
                con,
                item_name=ingredient_name,
                low_price=float(estimate["low_price"]),
                high_price=float(estimate["high_price"]),
                unit=str(estimate.get("unit", "unit")),
                source_urls=[str(url) for url in estimate.get("source_urls", []) if str(url)],
                retrieved_at=str(estimate.get("retrieved_at", "")) or None,
            )
            estimated += 1
            self.log_action(
                "web_price_estimate_created",
                target_type="ingredient_name",
                target_id=ingredient_name,
                detail=(
                    f"Saved web estimate for '{ingredient_name}': "
                    f"{estimate['low_price']}-{estimate['high_price']} per {estimate.get('unit', 'unit')}"
                ),
            )

        return estimated

    async def run_cycle(self):
        logger.info("Starting autonomy cycle...")
        started = time.time()
        started_at = datetime.now().isoformat()
        self._set_status(
            is_running=1,
            last_cycle_started_at=started_at,
            last_action="cycle_start",
            last_error=None,
        )
        self._refresh_status_queues(action="cycle_start")

        try:
            # 1) Evaluate active Tier 1 sources and create pending drafts.
            self._set_status(last_action="scan_drafts")
            await self.evaluate_documents()
            # 2) Enrich pending drafts.
            self._set_status(last_action="enrich")
            await self.enrich_drafts()
            # 3) Promote high-confidence enriched drafts.
            self._set_status(last_action="promote")
            await self.promote_drafts()
            # 4) Reconcile ingredient links and costs.
            self._set_status(last_action="reconcile_inventory")
            await self.reconcile_inventory()
            # 5) Audit for missing critical recipe data.
            self._set_status(last_action="audit")
            await self.audit_system()
            # 6) Maintain tier metadata hygiene.
            self._set_status(last_action="maintain_rag")
            await self.maintain_rag()

            self.last_run = datetime.now()
            self._last_maintenance_cycle_at = time.time()
            self._set_status(
                is_running=1,
                last_cycle_finished_at=datetime.now().isoformat(),
                last_action="cycle_complete",
            )
            self._refresh_status_queues(action="cycle_complete")
            logger.info("Autonomy cycle completed in %.2fs", time.time() - started)
        except Exception as exc:
            logger.exception("Error in autonomy cycle")
            self._set_status(
                is_running=1,
                last_action="cycle_error",
                last_error=str(exc),
                last_error_at=datetime.now().isoformat(),
            )
            self.log_action(
                action="autonomy_cycle_error",
                target_type="worker",
                target_id="cycle",
                detail=str(exc),
            )

    async def evaluate_documents(self):
        """Create draft records from active Tier 1 sources when no draft exists yet."""
        con = memory.get_conn()
        created = 0

        try:
            sources = rag.rag_engine.get_sources()
            for source in sources:
                if source.get("status") != "active":
                    continue

                source_id = source.get("id")
                source_name = source.get("source_name")
                if not source_id or not source_name:
                    continue

                tier = rag.normalize_knowledge_tier(
                    source.get("knowledge_tier")
                ) or rag.infer_knowledge_tier(
                    source_type=source.get("type", ""),
                    title=source.get("title", ""),
                    source_name=source_name,
                    summary=source.get("summary", ""),
                )
                if tier != rag.TIER_1_RECIPE_OPS:
                    continue
                if not self._is_recipe_candidate_source(source):
                    continue

                existing = con.execute(
                    """
                    SELECT id
                    FROM recipe_drafts
                    WHERE source_id = ?
                    LIMIT 1
                    """,
                    (source_id,),
                ).fetchone()
                if existing:
                    continue

                try:
                    chunk_payload = rag.rag_engine.collection.get(
                        where={"source": source_name},
                        include=["documents", "metadatas"],
                    )
                except Exception as exc:
                    logger.warning("Could not read source chunks for %s: %s", source_name, exc)
                    continue

                documents = chunk_payload.get("documents") or []
                metadatas = chunk_payload.get("metadatas") or []
                if not documents:
                    continue

                ordered_chunks: List[Tuple[int, str]] = []
                for idx, doc_text in enumerate(documents):
                    text = str(doc_text or "").strip()
                    if not text:
                        continue

                    metadata = (
                        metadatas[idx]
                        if idx < len(metadatas) and isinstance(metadatas[idx], dict)
                        else {}
                    )
                    try:
                        chunk_id = int(metadata.get("chunk_id", idx))
                    except Exception:
                        chunk_id = idx

                    heading = str(metadata.get("heading", "")).strip()
                    payload_text = f"{heading}\n{text}" if heading else text
                    ordered_chunks.append((chunk_id, payload_text))

                ordered_chunks.sort(key=lambda item: item[0])
                selected_chunks = ordered_chunks[: self.max_source_chunks_per_draft]
                raw_text = "\n\n".join(chunk_text for _, chunk_text in selected_chunks).strip()

                if len(raw_text) < self.min_source_chars_for_draft:
                    continue

                draft_name = " ".join(
                    str(source.get("title") or Path(source_name).stem or "Untitled Draft").split()
                ).strip()
                confidence = min(
                    self._estimate_draft_confidence(raw_text), self.enrich_attempt_band_max
                )

                con.execute(
                    """
                    INSERT INTO recipe_drafts (
                        source_id, name, raw_text, confidence, status, knowledge_tier, updated_at
                    )
                    VALUES (?, ?, ?, ?, 'pending', ?, CURRENT_TIMESTAMP)
                    """,
                    (source_id, draft_name, raw_text, confidence, tier),
                )
                created += 1

                self.log_action(
                    "evaluate_document",
                    target_type="source",
                    target_id=source_id,
                    detail=f"Created draft '{draft_name}' from source '{source_name}'",
                    conf_before=0.0,
                    conf_after=confidence,
                )

            con.commit()
            if created:
                logger.info("Autonomy created %s new draft(s) from Tier 1 sources.", created)
        except Exception as exc:
            logger.error("Document evaluation failed: %s", exc)
        finally:
            con.close()

    async def enrich_drafts(self):
        """Enrich pending drafts into structured recipe payloads (Tier 1 only)."""
        con = memory.get_conn()
        source_index = self._build_source_index()
        enriched_count = 0
        rejected_count = 0

        try:
            drafts = con.execute(
                "SELECT * FROM recipe_drafts WHERE status = 'pending' ORDER BY id ASC LIMIT ?",
                (self.draft_scan_limit,),
            ).fetchall()

            for draft_row in drafts:
                draft = dict(draft_row)
                draft_id = draft["id"]
                tier = self._resolve_draft_tier(draft=draft, source_index=source_index)
                confidence_before = float(draft["confidence"] or 0.0)
                if confidence_before > self.enrich_attempt_band_max:
                    confidence_before = self.enrich_attempt_band_max
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET confidence = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (confidence_before, draft_id),
                    )

                if tier != rag.TIER_1_RECIPE_OPS:
                    reason = (
                        "Knowledge boundary enforced: only Tier 1 restaurant recipe sources "
                        "are auto-enriched by autonomy."
                    )
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason, tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "enrich_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=float(draft["confidence"] or 0.0),
                        conf_after=float(draft["confidence"] or 0.0),
                    )
                    continue

                if confidence_before < self.enrich_min_confidence:
                    reason = (
                        f"Draft confidence {confidence_before:.2f} is below balanced minimum "
                        f"{self.enrich_min_confidence:.2f}; human intervention required."
                    )
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason, tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "enrich_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=confidence_before,
                        conf_after=confidence_before,
                    )
                    continue

                raw_text = draft["raw_text"] or ""
                if not raw_text.strip():
                    reason = "Draft has no raw text payload to enrich."
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason, tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "enrich_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                    )
                    continue

                prompt = (
                    "Extract structured recipe data from the source text.\n\n"
                    "Rules:\n"
                    "- Return JSON only (no markdown).\n"
                    "- Do not invent missing values; use null or [] when unknown.\n"
                    "- Keep ingredient names exactly as written when possible.\n"
                    "- Never invent quantities or method steps that are not explicit in the text.\n\n"
                    f"SOURCE TEXT:\n{raw_text}\n\n"
                    "JSON schema:\n"
                    "{\n"
                    '  "name": "string",\n'
                    '  "yield_amount": "number|null",\n'
                    '  "yield_unit": "string|null",\n'
                    '  "station": "string|null",\n'
                    '  "category": "string|null",\n'
                    '  "method": "string|null",\n'
                    '  "ingredients": [{"item_name_text":"string","quantity":"number|null","unit":"string|null","notes":"string|null"}],\n'
                    '  "allergens": ["string"]\n'
                    "}"
                )

                response_text = brain.chat([("user", prompt)])

                try:
                    parsed = self._extract_json_object(response_text)
                    ingredients = self._sanitize_ingredients(parsed.get("ingredients"))
                    allergens_list = self._sanitize_allergens(parsed.get("allergens"))
                    name_value = (
                        " ".join(str(parsed.get("name") or draft["name"]).split()).strip() or None
                    )
                    method_value = str(parsed.get("method") or "").strip() or None
                    missing_required = self._missing_required_fields(
                        name_value=name_value,
                        method_value=method_value,
                        ingredients=ingredients,
                    )
                    if missing_required:
                        raise ValueError(
                            f"Missing required field(s): {', '.join(missing_required)}"
                        )

                    confidence_after = self._structured_confidence(
                        before=confidence_before,
                        ingredient_count=len(ingredients),
                        has_method=bool(method_value),
                        has_yield=parsed.get("yield_amount") not in (None, 0, 0.0, "0"),
                    )
                    if confidence_after < self.enrich_min_confidence:
                        raise ValueError(
                            f"Post-enrichment confidence {confidence_after:.2f} below minimum "
                            f"{self.enrich_min_confidence:.2f}"
                        )

                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET name = ?, yield_amount = ?, yield_unit = ?, station = ?,
                            category = ?, method = ?, ingredients_json = ?, allergens_json = ?,
                            confidence = ?, knowledge_tier = ?, status = 'enriched',
                            rejection_reason = NULL, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            name_value,
                            _safe_float(parsed.get("yield_amount")),
                            " ".join(str(parsed.get("yield_unit") or "").split())[:50] or None,
                            " ".join(str(parsed.get("station") or "").split())[:100] or None,
                            " ".join(str(parsed.get("category") or "").split())[:100] or None,
                            method_value,
                            json.dumps(ingredients),
                            json.dumps(allergens_list),
                            confidence_after,
                            tier,
                            draft_id,
                        ),
                    )
                    enriched_count += 1

                    self.log_action(
                        "enrich_draft",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=f"Enriched draft '{draft['name']}'",
                        conf_before=confidence_before,
                        conf_after=confidence_after,
                    )
                except Exception as exc:
                    reason = f"Autonomy enrichment failed: {exc}"
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason[:800], tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "enrich_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=confidence_before,
                        conf_after=confidence_before,
                    )

            con.commit()
            logger.info(
                "Autonomy enrichment complete. enriched=%s rejected=%s",
                enriched_count,
                rejected_count,
            )
        except Exception as exc:
            logger.error("Draft enrichment failed: %s", exc)
        finally:
            con.close()

    def _set_recipe_allergens(self, con, recipe_id: int, allergens_json: str) -> None:
        if not allergens_json:
            return

        try:
            candidate_names = self._sanitize_allergens(json.loads(allergens_json))
        except Exception:
            return

        if not candidate_names:
            return

        db_rows = con.execute("SELECT id, name FROM allergens").fetchall()
        id_by_name = {str(row["name"]).strip().lower(): int(row["id"]) for row in db_rows}
        allergen_ids = [
            id_by_name[name.lower()] for name in candidate_names if name.lower() in id_by_name
        ]
        if not allergen_ids:
            return

        from services import allergens

        allergens.set_recipe_allergens(recipe_id, allergen_ids)

    async def promote_drafts(self):
        """Auto-promote enriched Tier 1 drafts above confidence threshold."""
        con = memory.get_conn()
        source_index = self._build_source_index()
        promoted_count = 0
        rejected_count = 0

        try:
            drafts = con.execute(
                """
                SELECT *
                FROM recipe_drafts
                WHERE status = 'enriched' AND confidence >= ?
                ORDER BY confidence DESC, id ASC
                LIMIT ?
                """,
                (self.auto_promote_threshold, self.draft_scan_limit),
            ).fetchall()

            for draft_row in drafts:
                draft = dict(draft_row)
                draft_id = draft["id"]
                tier = self._resolve_draft_tier(draft=draft, source_index=source_index)
                if tier != rag.TIER_1_RECIPE_OPS:
                    reason = (
                        "Knowledge boundary enforced: non-Tier-1 draft cannot be auto-promoted."
                    )
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason, tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "promote_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=float(draft["confidence"] or 0.0),
                        conf_after=float(draft["confidence"] or 0.0),
                    )
                    continue

                try:
                    ingredients = json.loads(draft["ingredients_json"] or "[]")
                    if not isinstance(ingredients, list) or not ingredients:
                        raise ValueError("No ingredients payload available.")
                except Exception as exc:
                    reason = f"Promotion blocked: invalid ingredient payload ({exc})"
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason[:800], tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "promote_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=float(draft["confidence"] or 0.0),
                        conf_after=float(draft["confidence"] or 0.0),
                    )
                    self._alert_if_required(
                        alert_key=f"critical_promotion_payload_{draft_id}",
                        message=(
                            "🔎 *Autonomy Review Needed*\n"
                            f"Draft *{draft.get('name', 'Unnamed')}* has invalid ingredient payload.\n"
                            f"Reason: {str(exc)[:220]}"
                        ),
                        throttle_minutes=24 * 60,
                    )
                    continue

                missing_required = self._missing_required_fields(
                    name_value=draft.get("name"),
                    method_value=draft.get("method"),
                    ingredients=ingredients,
                )
                if missing_required:
                    reason = f"Promotion blocked: missing required field(s): {', '.join(missing_required)}"
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason, tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "promote_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=float(draft["confidence"] or 0.0),
                        conf_after=float(draft["confidence"] or 0.0),
                    )
                    self._alert_if_required(
                        alert_key=f"critical_missing_fields_{draft_id}",
                        message=(
                            "🔎 *Autonomy Review Needed*\n"
                            f"Draft *{draft.get('name', 'Unnamed')}* cannot be promoted.\n"
                            f"Missing: {', '.join(missing_required)}"
                        ),
                        throttle_minutes=24 * 60,
                    )
                    continue

                recipe_data = {
                    "name": draft["name"],
                    "yield_amount": draft["yield_amount"],
                    "yield_unit": draft["yield_unit"],
                    "station": draft["station"],
                    "category": draft["category"],
                    "method": draft["method"],
                }

                result = recipes.create_recipe(recipe_data, ingredients)
                if "successfully" not in result.lower():
                    reason = f"Auto-promotion failed: {result}"
                    con.execute(
                        """
                        UPDATE recipe_drafts
                        SET status = 'rejected', rejection_reason = ?, knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (reason[:800], tier, draft_id),
                    )
                    rejected_count += 1
                    self.log_action(
                        "promote_draft_rejected",
                        target_type="recipe_draft",
                        target_id=draft_id,
                        detail=reason,
                        conf_before=float(draft["confidence"] or 0.0),
                        conf_after=float(draft["confidence"] or 0.0),
                    )
                    continue

                new_recipe_row = con.execute(
                    "SELECT id FROM recipes WHERE name = ? ORDER BY id DESC LIMIT 1",
                    (draft["name"],),
                ).fetchone()
                if new_recipe_row:
                    self._set_recipe_allergens(
                        con=con,
                        recipe_id=int(new_recipe_row["id"]),
                        allergens_json=draft["allergens_json"] or "[]",
                    )
                    self._set_status(
                        last_action="promote",
                        last_promoted_recipe_id=int(new_recipe_row["id"]),
                        last_promoted_recipe_name=str(draft.get("name") or "").strip() or None,
                        last_promoted_at=datetime.now().isoformat(),
                    )

                con.execute(
                    """
                    UPDATE recipe_drafts
                    SET status = 'promoted', knowledge_tier = ?, rejection_reason = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (tier, draft_id),
                )
                promoted_count += 1
                self.log_action(
                    "promote_draft",
                    target_type="recipe_draft",
                    target_id=draft_id,
                    detail=f"Auto-promoted draft '{draft['name']}'",
                    conf_before=float(draft["confidence"] or 0.0),
                    conf_after=float(draft["confidence"] or 0.0),
                )

            con.commit()
            logger.info(
                "Autonomy promotion complete. promoted=%s rejected=%s threshold=%.2f",
                promoted_count,
                rejected_count,
                self.auto_promote_threshold,
            )
        except Exception as exc:
            logger.error("Draft promotion failed: %s", exc)
        finally:
            con.close()

    async def reconcile_inventory(self):
        """Link ingredient lines to inventory items and refresh recipe costs."""
        con = memory.get_conn()
        linked = 0
        estimated_prices = 0
        active_recipe_ids: List[int] = []

        try:
            inventory_rows = con.execute("SELECT id, name FROM inventory_items").fetchall()
            exact_map: Dict[str, int] = {}
            normalized_map: Dict[str, List[int]] = {}

            for row in inventory_rows:
                item_id = int(row["id"])
                lowered = " ".join(str(row["name"] or "").strip().lower().split())
                if not lowered:
                    continue
                exact_map[lowered] = item_id

                norm_key = self._normalize_item_key(lowered)
                if norm_key:
                    normalized_map.setdefault(norm_key, []).append(item_id)

            unlinked_rows = con.execute("""
                SELECT id, item_name_text, recipe_id
                FROM recipe_ingredients
                WHERE inventory_item_id IS NULL AND item_name_text IS NOT NULL
                """).fetchall()

            touched_recipes: Set[int] = set()
            for row in unlinked_rows:
                ingredient_id = int(row["id"])
                ingredient_name = str(row["item_name_text"] or "")
                recipe_id = int(row["recipe_id"])

                match_id = self._best_inventory_match(
                    ingredient_name=ingredient_name,
                    exact_map=exact_map,
                    normalized_map=normalized_map,
                )
                if match_id is None:
                    continue

                con.execute(
                    "UPDATE recipe_ingredients SET inventory_item_id = ? WHERE id = ?",
                    (match_id, ingredient_id),
                )
                touched_recipes.add(recipe_id)
                linked += 1
                self.log_action(
                    "reconcile_inventory_link",
                    target_type="recipe_ingredient",
                    target_id=ingredient_id,
                    detail=f"Linked '{ingredient_name}' -> inventory item {match_id}",
                )

            active_recipe_ids = [
                int(row["id"])
                for row in con.execute("SELECT id FROM recipes WHERE is_active = 1").fetchall()
            ]

            # Web research is optional and always non-authoritative:
            # estimates are stored in price_estimates and never overwrite inventory/vendor costs.
            estimated_prices = self._web_estimate_missing_costs(con)
            con.commit()

            # Run costing refresh after commit to avoid nested write-lock contention.
            for recipe_id in active_recipe_ids:
                try:
                    costing.update_ingredient_costs(recipe_id)
                except Exception as exc:
                    logger.warning("Cost refresh failed for recipe %s: %s", recipe_id, exc)

            self.log_action(
                "reconcile_inventory_summary",
                detail=(
                    f"Linked {linked} ingredient(s); refreshed costs for {len(active_recipe_ids)} active recipe(s); "
                    f"saved {estimated_prices} web price estimate(s)."
                ),
            )
        except Exception as exc:
            logger.error("Inventory reconciliation failed: %s", exc)
        finally:
            con.close()

    async def audit_system(self):
        """Detect critical recipe data gaps and alert only when intervention is required."""
        con = memory.get_conn()
        try:
            empty_recipes = con.execute("""
                SELECT r.id, r.name
                FROM recipes r
                LEFT JOIN recipe_ingredients ri ON r.id = ri.recipe_id
                WHERE ri.id IS NULL AND r.is_active = 1
                """).fetchall()

            no_yield_recipes = con.execute("""
                SELECT id, name
                FROM recipes
                WHERE (yield_amount IS NULL OR yield_amount = 0) AND is_active = 1
                """).fetchall()
            no_method_recipes = con.execute("""
                SELECT id, name
                FROM recipes
                WHERE (method IS NULL OR TRIM(method) = '') AND is_active = 1
                """).fetchall()

            for row in empty_recipes:
                recipe_id = str(row["id"])
                if not self._has_recent_log(
                    action="safety_audit",
                    target_type="recipe",
                    target_id=recipe_id,
                    minutes=360,
                ):
                    self.log_action(
                        "safety_audit",
                        target_type="recipe",
                        target_id=recipe_id,
                        detail=f"Recipe '{row['name']}' has no ingredients.",
                    )

            for row in no_yield_recipes:
                recipe_id = str(row["id"])
                if not self._has_recent_log(
                    action="safety_audit",
                    target_type="recipe",
                    target_id=recipe_id,
                    minutes=360,
                ):
                    self.log_action(
                        "safety_audit",
                        target_type="recipe",
                        target_id=recipe_id,
                        detail=f"Recipe '{row['name']}' is missing yield data.",
                    )

            for row in no_method_recipes:
                recipe_id = str(row["id"])
                if not self._has_recent_log(
                    action="safety_audit",
                    target_type="recipe",
                    target_id=recipe_id,
                    minutes=360,
                ):
                    self.log_action(
                        "safety_audit",
                        target_type="recipe",
                        target_id=recipe_id,
                        detail=f"Recipe '{row['name']}' is missing method data.",
                    )

            _ = len(empty_recipes) + len(no_yield_recipes) + len(no_method_recipes)
        except Exception as exc:
            logger.error("Safety audit failed: %s", exc)
        finally:
            con.close()

    async def maintain_rag(self):
        """Normalize draft tiers against source metadata and log hygiene status."""
        con = memory.get_conn()
        updated = 0
        try:
            source_index = self._build_source_index()
            drafts = con.execute("""
                SELECT id, source_id, name, raw_text, knowledge_tier
                FROM recipe_drafts
                WHERE status IN ('pending','enriched') OR knowledge_tier IS NULL OR TRIM(knowledge_tier) = ''
                """).fetchall()

            for draft_row in drafts:
                draft = dict(draft_row)
                target_tier = self._resolve_draft_tier(draft=draft, source_index=source_index)
                current_tier = rag.normalize_knowledge_tier(draft["knowledge_tier"])
                if current_tier == target_tier:
                    continue

                con.execute(
                    """
                    UPDATE recipe_drafts
                    SET knowledge_tier = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (target_tier, int(draft["id"])),
                )
                updated += 1

            con.commit()
            self.log_action(
                "rag_hygiene",
                detail=f"RAG hygiene completed. Draft tier updates={updated}.",
            )
        except Exception as exc:
            logger.error("RAG hygiene failed: %s", exc)
        finally:
            con.close()

    async def _process_ingest_job(self, job: Dict[str, Any]) -> None:
        job_id = int(job["id"])
        source_filename = str(job.get("source_filename") or "").strip()
        source_type = str(job.get("source_type") or "unknown").strip().lower()
        restaurant_tag = str(job.get("restaurant_tag") or "").strip() or None

        file_path = resolve_path("data/documents") / source_filename
        if not file_path.exists():
            self._update_ingest_job(
                job_id,
                status="failed",
                error=f"file_missing: {source_filename}",
                finished=True,
            )
            if self.ingest_completion_message:
                self._alert_if_required(
                    alert_key=f"ingest_failed_{job_id}",
                    message=f"❌ Ingest failed: {source_filename}. Use /job {job_id}",
                    throttle_minutes=30,
                )
            self._set_status(
                last_action="ingest_failed",
                last_error=f"ingest job #{job_id} missing file",
                last_error_at=datetime.now().isoformat(),
            )
            return

        self._update_ingest_job(
            job_id, status="extracting", progress_current=1, progress_total=6, started=True
        )
        self._set_status(last_action="extracting")

        source_title = file_path.stem.replace("_", " ").title()
        metadata_type = (
            "house_recipe_book" if source_type == "restaurant_recipes" else "reference_book"
        )
        knowledge_tier = (
            rag.TIER_1_RECIPE_OPS
            if source_type == "restaurant_recipes"
            else rag.TIER_3_REFERENCE_THEORY
        )

        self._update_ingest_job(job_id, status="chunking", progress_current=2, progress_total=6)
        self._set_status(last_action="chunking")
        self._update_ingest_job(job_id, status="indexing", progress_current=3, progress_total=6)
        self._set_status(last_action="indexing")

        ok, result = rag.rag_engine.ingest_file(
            str(file_path),
            extra_metadata={
                "source_title": source_title,
                "source_type": metadata_type,
                "knowledge_tier": knowledge_tier,
                "restaurant_tag": restaurant_tag,
                "summary": f"Queued ingest job #{job_id}: {source_filename}",
            },
            ingest_id=str(job.get("ingest_id") or "").strip() or None,
        )
        if not ok:
            self._update_ingest_job(
                job_id,
                status="failed",
                error=f"ingest_failed: {_sanitize_error_text(result)}",
                finished=True,
            )
            if self.ingest_completion_message:
                self._alert_if_required(
                    alert_key=f"ingest_failed_{job_id}",
                    message=f"❌ Ingest failed: {source_filename}. Use /job {job_id}",
                    throttle_minutes=30,
                )
            self._set_status(
                last_action="ingest_failed",
                last_error=str(result),
                last_error_at=datetime.now().isoformat(),
            )
            return

        source_id = str((result or {}).get("source_id") or "").strip()
        report = None
        ingest_id = str((result or {}).get("ingest_id") or "").strip()
        if ingest_id:
            report = rag.rag_engine.load_ingest_report(ingest_id)

        if source_type != "restaurant_recipes":
            self._update_ingest_job(
                job_id, status="done", progress_current=6, progress_total=6, finished=True
            )
            self._set_status(last_action="ingest_done_reference")
            return

        before_promoted = 0
        con = memory.get_conn()
        try:
            if source_id:
                before_promoted = int(
                    con.execute(
                        "SELECT COUNT(*) FROM recipe_drafts WHERE source_id = ? AND status = 'promoted'",
                        (source_id,),
                    ).fetchone()[0]
                )
        finally:
            con.close()

        self._update_ingest_job(
            job_id, status="extracting_recipes", progress_current=4, progress_total=6
        )
        self._set_status(last_action="extracting_recipes")
        await self.evaluate_documents()

        self._update_ingest_job(job_id, status="enriching", progress_current=5, progress_total=6)
        self._set_status(last_action="enriching")
        await self.enrich_drafts()

        self._update_ingest_job(job_id, status="promoting", progress_current=6, progress_total=6)
        self._set_status(last_action="promoting")
        await self.promote_drafts()

        con = memory.get_conn()
        try:
            promoted_after = before_promoted
            total_for_source = 0
            rejected_for_source = 0
            if source_id:
                promoted_after = int(
                    con.execute(
                        "SELECT COUNT(*) FROM recipe_drafts WHERE source_id = ? AND status = 'promoted'",
                        (source_id,),
                    ).fetchone()[0]
                )
                total_for_source = int(
                    con.execute(
                        "SELECT COUNT(*) FROM recipe_drafts WHERE source_id = ?",
                        (source_id,),
                    ).fetchone()[0]
                )
                rejected_for_source = int(
                    con.execute(
                        "SELECT COUNT(*) FROM recipe_drafts WHERE source_id = ? AND status = 'rejected'",
                        (source_id,),
                    ).fetchone()[0]
                )
            promoted_count = max(0, promoted_after - before_promoted)
            needs_review_count = max(0, total_for_source - promoted_after) if source_id else 0

            if promoted_count == 0:
                reason_code = self._classify_needs_review_reason(
                    report=report, source_type=source_type
                )
                self._update_ingest_job(
                    job_id,
                    status="needs_review",
                    progress_current=6,
                    progress_total=6,
                    error=reason_code,
                    promoted_count=promoted_count,
                    needs_review_count=max(needs_review_count, rejected_for_source),
                    finished=True,
                )
                self._alert_if_required(
                    alert_key=f"ingest_no_promotions_{job_id}",
                    message=f"⚠️ No recipes promoted: {source_filename} — reason: {reason_code}. Use /job {job_id}",
                    throttle_minutes=60,
                )
            else:
                self._update_ingest_job(
                    job_id,
                    status="done",
                    progress_current=6,
                    progress_total=6,
                    promoted_count=promoted_count,
                    needs_review_count=max(0, needs_review_count),
                    finished=True,
                )
                if self.ingest_completion_message:
                    if promoted_count > 0 or needs_review_count > 0:
                        self._alert_if_required(
                            alert_key=f"ingest_done_{job_id}",
                            message=(
                                f"✅ Ingest done: {source_filename} — +{promoted_count} recipes, "
                                f"{needs_review_count} need review."
                            ),
                            throttle_minutes=30,
                        )
        finally:
            con.close()

    async def process_ingest_jobs(self, limit: int = 1) -> int:
        con = memory.get_conn()
        try:
            rows = con.execute(
                """
                SELECT *
                FROM ingest_jobs
                WHERE status IN ('queued','extracting','chunking','indexing','extracting_recipes','enriching','promoting')
                ORDER BY id ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            jobs = [dict(row) for row in rows]
        finally:
            con.close()

        processed = 0
        for job in jobs:
            processed += 1
            try:
                await self._process_ingest_job(job)
            except Exception as exc:
                job_id = int(job["id"])
                self._update_ingest_job(
                    job_id,
                    status="failed",
                    error=f"job_exception: {_sanitize_error_text(exc)}",
                    finished=True,
                )
                if self.ingest_completion_message:
                    self._alert_if_required(
                        alert_key=f"ingest_failed_{job_id}",
                        message=f"❌ Ingest failed: {job.get('source_filename', 'document')}. Use /job {job_id}",
                        throttle_minutes=30,
                    )
                self._set_status(
                    last_action="ingest_failed",
                    last_error=str(exc),
                    last_error_at=datetime.now().isoformat(),
                )
        return processed

    async def run_background_tick(self) -> None:
        if self._is_tick_running:
            return
        if not self._acquire_singleton():
            return

        self._is_tick_running = True
        try:
            self._set_status(
                is_running=1, last_tick_at=datetime.now().isoformat(), last_action="tick"
            )
            self._refresh_status_queues(action="tick")

            jobs_processed = await self.process_ingest_jobs(limit=2)
            now_ts = time.time()

            if jobs_processed == 0:
                # Fast pass for drafts every polling interval.
                con = memory.get_conn()
                try:
                    pending_drafts = int(
                        con.execute(
                            "SELECT COUNT(*) FROM recipe_drafts WHERE status IN ('pending','enriched')"
                        ).fetchone()[0]
                    )
                finally:
                    con.close()
                if pending_drafts > 0:
                    self._set_status(last_action="scan_drafts")
                    await self.evaluate_documents()
                    self._set_status(last_action="enrich")
                    await self.enrich_drafts()
                    self._set_status(last_action="promote")
                    await self.promote_drafts()

            # Keep Prep-List alive without noisy chatter.
            self._set_status(last_action="prep_list_refresh")
            gen = prep_list.auto_generate_if_empty()
            snapshot = prep_list.behind_service_snapshot()
            if int(snapshot.get("open_items", 0)) > 0:
                self._alert_if_required(
                    alert_key="prep_list_behind_service",
                    message=(
                        f"⚠️ Prep-List behind: {int(snapshot.get('open_items', 0))} open items "
                        f"across {int(snapshot.get('stations', 0))} stations."
                    ),
                    throttle_minutes=90,
                )
            if int(gen.get("generated", 0)) > 0:
                self.log_action(
                    action="prep_list_autogen",
                    target_type="prep_list",
                    target_id="daily",
                    detail=f"Auto-generated {int(gen.get('generated', 0))} prep item(s).",
                )

            # Full maintenance cycle on configured interval.
            if (now_ts - self._last_maintenance_cycle_at) >= max(60, int(self.interval)):
                await self.run_cycle()
        except Exception as exc:
            self._set_status(
                is_running=1,
                last_action="tick_error",
                last_error=str(exc),
                last_error_at=datetime.now().isoformat(),
            )
            self.log_action(
                action="autonomy_tick_error",
                target_type="worker",
                target_id="tick",
                detail=str(exc),
            )
        finally:
            self._refresh_status_queues(action="tick_done")
            self._is_tick_running = False

    async def start(self):
        if not self.enabled:
            logger.info("Autonomy is disabled in config. Running silent.")
            self._set_status(
                is_running=0, last_action="disabled", last_tick_at=datetime.now().isoformat()
            )
            return

        if not self._acquire_singleton():
            logger.info("Autonomy worker standby: singleton lock held by another process.")
            return

        self.running = True
        self._set_status(
            is_running=1, last_action="started", last_tick_at=datetime.now().isoformat()
        )
        logger.info(
            "Autonomy worker started. Poll interval: %ss (maintenance interval: %ss)",
            self.poll_interval_seconds,
            self.interval,
        )

        try:
            while not self._stop_event.is_set():
                await self.run_background_tick()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=max(30, self.poll_interval_seconds)
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            self.running = False
            self._set_status(
                is_running=0, last_action="stopped", last_tick_at=datetime.now().isoformat()
            )
            self._release_singleton()

    def stop(self):
        self.running = False
        self._stop_event.set()
        self._set_status(
            is_running=0, last_action="stopping", last_tick_at=datetime.now().isoformat()
        )
        logger.info("Autonomy worker stopping...")


async def main():
    worker = AutonomyWorker()
    await worker.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
