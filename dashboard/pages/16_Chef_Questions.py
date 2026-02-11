import streamlit as st
import pandas as pd
from services import chef_questions

st.set_page_config(page_title="Chef Questions", page_icon="🧪", layout="wide")

st.title("🧪 Chef Questions Test Suite")

tab_run, tab_manage = st.tabs(["▶️ Run Tests", "📝 Manage Questions"])

# --- RUN TAB ---
with tab_run:
    summary = chef_questions.get_test_summary()
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", summary["total"])
    c2.metric("✅ Passed", summary["passed"])
    c3.metric("❌ Failed", summary["failed"])
    c4.metric("⏳ Untested", summary["untested"])
    
    if summary["total"] > 0:
        pass_rate = (summary["passed"] / summary["total"]) * 100 if summary["total"] else 0
        st.progress(pass_rate / 100, text=f"Pass Rate: {pass_rate:.0f}%")
    
    st.divider()
    
    questions = chef_questions.get_all_questions()
    
    if not questions:
        st.info("No questions yet. Add some in the 'Manage Questions' tab.")
    else:
        categories = list(set(q.get("category", "General") for q in questions))
        cat_filter = st.selectbox("Filter Category", ["All"] + sorted(categories))
        
        filtered = questions if cat_filter == "All" else [q for q in questions if q.get("category") == cat_filter]
        
        for q in filtered:
            result_emoji = {"Pass": "✅", "Fail": "❌", "Partial": "🟡"}.get(q.get("last_result"), "⏳")
            
            with st.expander(f"{result_emoji} {q['question'][:80]}"):
                st.markdown(f"**Expected:** {q['expected_answer']}")
                st.caption(f"Category: {q['category']} | Last tested: {q.get('last_tested_at', 'Never')}")
                
                c1, c2, c3 = st.columns(3)
                if c1.button("✅ Pass", key=f"pass_{q['id']}"):
                    chef_questions.update_test_result(q["id"], "Pass")
                    st.rerun()
                if c2.button("❌ Fail", key=f"fail_{q['id']}"):
                    chef_questions.update_test_result(q["id"], "Fail")
                    st.rerun()
                if c3.button("🟡 Partial", key=f"partial_{q['id']}"):
                    chef_questions.update_test_result(q["id"], "Partial")
                    st.rerun()

# --- MANAGE TAB ---  
with tab_manage:
    st.subheader("Add New Question")
    
    with st.form("add_question_form"):
        question = st.text_area("Question")
        expected = st.text_area("Expected Answer")
        category = st.selectbox("Category", ["Inventory", "Recipes", "Operations", "Vendors", "Food Safety", "General"])
        
        if st.form_submit_button("Add Question"):
            if question and expected:
                msg = chef_questions.create_question(question, expected, category)
                st.success(msg)
                st.rerun()
            else:
                st.error("Fill out both fields.")
    
    st.divider()
    st.subheader("Existing Questions")
    questions = chef_questions.get_all_questions()
    if questions:
        df = pd.DataFrame(questions)[["id", "category", "question", "last_result"]]
        st.dataframe(df, use_container_width=True)
