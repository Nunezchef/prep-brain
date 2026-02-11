import streamlit as st
import pandas as pd
from datetime import date
from services import service_notes

st.set_page_config(page_title="Service Notes", page_icon="📝", layout="wide")

st.title("📝 Service Notes & Post-Mortem")

tab_write, tab_read = st.tabs(["✏️ Write Note", "📚 Past Notes"])

# --- WRITE TAB ---
with tab_write:
    with st.form("service_note_form"):
        c1, c2, c3 = st.columns(3)
        sdate = c1.date_input("Service Date", value=date.today())
        shift = c2.selectbox("Shift", ["Lunch", "Dinner", "Brunch", "All Day"])
        covers = c3.number_input("Covers", min_value=0, step=1)
        
        weather = st.text_input("Weather / Event Notes")
        
        st.markdown("---")
        notes = st.text_area("General Notes", height=100)
        highlights = st.text_area("✅ What Went Well?", height=80)
        issues = st.text_area("❌ Issues / Problems", height=80)
        action_items = st.text_area("📋 Action Items for Tomorrow", height=80)
        
        logged_by = st.text_input("Logged By")
        
        if st.form_submit_button("Save Service Note"):
            data = {
                "service_date": sdate.strftime("%Y-%m-%d"),
                "shift": shift,
                "covers": covers,
                "weather": weather,
                "notes": notes,
                "highlights": highlights,
                "issues": issues,
                "action_items": action_items,
                "logged_by": logged_by
            }
            msg = service_notes.create_service_note(data)
            st.success(msg)

# --- READ TAB ---
with tab_read:
    notes_list = service_notes.get_service_notes()
    
    if not notes_list:
        st.info("No service notes yet.")
    else:
        for note in notes_list:
            with st.expander(f"**{note['service_date']}** — {note['shift']} ({note['covers']} covers)"):
                if note.get("weather"):
                    st.caption(f"🌤️ {note['weather']}")
                if note.get("notes"):
                    st.write(note["notes"])
                
                c1, c2 = st.columns(2)
                if note.get("highlights"):
                    c1.markdown(f"**✅ Highlights**\n\n{note['highlights']}")
                if note.get("issues"):
                    c2.markdown(f"**❌ Issues**\n\n{note['issues']}")
                if note.get("action_items"):
                    st.markdown(f"**📋 Action Items**\n\n{note['action_items']}")
                
                st.caption(f"Logged by: {note.get('logged_by', 'N/A')}")
