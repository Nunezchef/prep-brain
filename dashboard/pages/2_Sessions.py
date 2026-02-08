import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from dashboard.utils import load_config

st.set_page_config(page_title="Sessions", page_icon="🗂️", layout="wide")
st.title("🗂️ Session Memory")

config = load_config()
db_path = config["memory"]["db_path"]

# Connection helper
def get_connection():
    return sqlite3.connect(db_path)

# Sidebar: Session List
st.sidebar.header("Sessions")

try:
    conn = get_connection()
    sessions = pd.read_sql_query("""
        SELECT 
            s.id, 
            s.title, 
            u.display_name, 
            s.created_at,
            (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id) as msg_count
        FROM sessions s
        LEFT JOIN users u ON s.telegram_user_id = u.telegram_user_id
        ORDER BY s.created_at DESC
    """, conn)
    conn.close()
except Exception as e:
    st.error(f"Error loading sessions: {e}")
    sessions = pd.DataFrame()

if sessions.empty:
    st.info("No sessions found.")
    st.stop()

# Selection
selected_session_id = st.sidebar.selectbox(
    "Select Session",
    sessions["id"].tolist(),
    format_func=lambda x: f"ID {x}: {sessions[sessions['id']==x].iloc[0]['display_name']} ({sessions[sessions['id']==x].iloc[0]['created_at']})"
)

if selected_session_id:
    st.markdown(f"### Session #{selected_session_id}")
    
    # Actions
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("🗑️ Clear History"):
             # In a real app, adding verify step is good.
             conn = get_connection()
             conn.execute("DELETE FROM messages WHERE session_id = ?", (selected_session_id,))
             conn.commit()
             conn.close()
             st.success("History cleared!")
             st.rerun()

    # Load messages
    conn = get_connection()
    messages = pd.read_sql_query(
        "SELECT role, content, created_at FROM messages WHERE session_id = ? ORDER BY id ASC",
        conn,
        params=(selected_session_id,)
    )
    conn.close()

    for _, msg in messages.iterrows():
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            st.caption(msg["created_at"])
