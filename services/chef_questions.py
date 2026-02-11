import sqlite3
import logging
from typing import List, Dict, Any, Optional
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def create_question(question: str, expected_answer: str, category: str) -> str:
    """Add a chef question to the test suite."""
    con = _get_conn()
    try:
        con.execute(
            "INSERT INTO chef_questions (question, expected_answer, category) VALUES (?, ?, ?)",
            (question, expected_answer, category)
        )
        con.commit()
        return f"Question added: {question[:50]}..."
    except Exception as e:
        logger.error(f"Error adding question: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_all_questions(category: str = None) -> List[dict]:
    """Get all test questions, optionally filtered by category."""
    con = _get_conn()
    try:
        if category:
            cur = con.execute(
                "SELECT * FROM chef_questions WHERE category = ? ORDER BY category, id",
                (category,)
            )
        else:
            cur = con.execute("SELECT * FROM chef_questions ORDER BY category, id")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def update_test_result(question_id: int, result: str) -> str:
    """Update the last test result for a question."""
    con = _get_conn()
    try:
        con.execute(
            "UPDATE chef_questions SET last_result = ?, last_tested_at = CURRENT_TIMESTAMP WHERE id = ?",
            (result, question_id)
        )
        con.commit()
        return "Result updated."
    finally:
        con.close()

def get_test_summary() -> Dict[str, Any]:
    """Get pass/fail counts."""
    con = _get_conn()
    try:
        cur = con.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN last_result = 'Pass' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN last_result = 'Fail' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN last_result = 'Partial' THEN 1 ELSE 0 END) as partial,
                SUM(CASE WHEN last_result IS NULL THEN 1 ELSE 0 END) as untested
            FROM chef_questions
        """)
        row = cur.fetchone()
        return dict(row) if row else {"total": 0, "passed": 0, "failed": 0, "partial": 0, "untested": 0}
    finally:
        con.close()

def delete_question(question_id: int) -> str:
    """Delete a question."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM chef_questions WHERE id = ?", (question_id,))
        con.commit()
        return "Question deleted."
    finally:
        con.close()
