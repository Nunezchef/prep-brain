import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from services import memory


def _normalize(text: str) -> str:
    value = str(text or "").strip().lower()
    value = re.sub(r"[^a-z0-9\s]+", " ", value)
    return " ".join(value.split())


def _score_name(query: str, candidate: str) -> float:
    q = _normalize(query)
    c = _normalize(candidate)
    if not q or not c:
        return 0.0
    if q == c:
        return 1.0
    if q in c:
        return 0.92
    ratio = SequenceMatcher(None, q, c).ratio()
    q_tokens = set(q.split())
    c_tokens = set(c.split())
    overlap = (len(q_tokens & c_tokens) / max(len(q_tokens), 1)) if q_tokens else 0.0
    return max(float(ratio), float(overlap))


def resolve_recipe_by_name(
    query: str,
    *,
    max_results: int = 5,
    min_confidence: float = 0.62,
    ambiguity_delta: float = 0.08,
) -> Dict[str, Any]:
    con = memory.get_conn()
    try:
        rows = con.execute(
            """
            SELECT id, name
            FROM recipes
            WHERE COALESCE(is_active, 1) = 1
            ORDER BY name ASC
            """
        ).fetchall()
    finally:
        con.close()

    scored: List[Dict[str, Any]] = []
    for row in rows:
        score = _score_name(query, str(row["name"] or ""))
        if score <= 0.0:
            continue
        scored.append(
            {
                "entity_type": "recipe",
                "id": int(row["id"]),
                "name": str(row["name"] or ""),
                "score": float(score),
            }
        )

    scored.sort(key=lambda item: item["score"], reverse=True)
    top = scored[: max(1, int(max_results))]
    if not top:
        return {
            "query": query,
            "matches": [],
            "best": None,
            "status": "no_match",
            "ambiguous": False,
        }

    best = top[0]
    second = top[1] if len(top) > 1 else None
    best_score = float(best["score"])
    ambiguous = False
    if best_score < float(min_confidence):
        ambiguous = True
    elif second is not None and (best_score - float(second["score"])) < float(ambiguity_delta):
        ambiguous = True

    if ambiguous and best_score < float(min_confidence):
        status = "no_match"
    elif ambiguous:
        status = "ambiguous"
    else:
        status = "resolved"

    return {
        "query": query,
        "matches": top,
        "best": best if status == "resolved" else None,
        "status": status,
        "ambiguous": bool(ambiguous),
    }


def get_recipe_by_id(recipe_id: int) -> Optional[Dict[str, Any]]:
    con = memory.get_conn()
    try:
        row = con.execute(
            "SELECT id, name FROM recipes WHERE id = ? LIMIT 1",
            (int(recipe_id),),
        ).fetchone()
        if not row:
            return None
        return {"id": int(row["id"]), "name": str(row["name"] or "")}
    finally:
        con.close()
