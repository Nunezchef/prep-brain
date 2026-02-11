import streamlit as st
import pandas as pd
from datetime import date
from services import schedule, prep

st.set_page_config(page_title="Production Schedule", page_icon="📅", layout="wide")

st.title("📅 Production Schedule")

tab_assign, tab_staff = st.tabs(["📋 Assign Work", "👨‍🍳 Manage Staff"])

# --- ASSIGN WORK TAB ---
with tab_assign:
    # Top Control Bar
    c1, c2 = st.columns([1, 3])
    selected_date = c1.date_input("Schedule Date", value=date.today())
    
    st.divider()
    
    col_unassigned, col_schedule = st.columns([1, 2])
    
    with col_unassigned:
        st.subheader("Unassigned Prep")
        # Get Todo items
        # Filter out ones already assigned today? Or simply show all todos not in schedule.
        # Ideally, we query 'Get unassigned todo items'.
        # For v1, we get all todos and filter in python.
        
        all_todos = prep.get_prep_list('todo')
        current_schedule = schedule.get_schedule(selected_date.strftime("%Y-%m-%d"))
        assigned_ids = [s["prep_list_item_id"] for s in current_schedule]
        
        unassigned = [item for item in all_todos if item["id"] not in assigned_ids]
        
        if not unassigned:
            st.info("No unassigned prep tasks.")
        else:
            # Assignment Form
            active_staff = schedule.get_active_staff()
            staff_opts = {s["id"]: s["name"] for s in active_staff}
            
            with st.form("assign_form"):
                task_id = st.selectbox(
                    "Select Task", 
                    options=[t["id"] for t in unassigned],
                    format_func=lambda x: [t["recipe_name"] for t in unassigned if t["id"]==x][0] + f" ({[t['need_quantity'] for t in unassigned if t['id']==x][0]})"
                )
                
                assignee = st.selectbox(
                    "Assign To",
                    options=list(staff_opts.keys()),
                    format_func=lambda x: staff_opts[x]
                )
                
                shift = st.selectbox("Shift", ["AM", "PM", "All Day"])
                
                if st.form_submit_button("Assign Task"):
                    msg = schedule.assign_prep_item(task_id, assignee, selected_date.strftime("%Y-%m-%d"), shift)
                    st.success(msg)
                    st.rerun()
            
            st.markdown("---")
            st.caption("Available Tasks:")
            for item in unassigned:
                st.text(f"• {item['recipe_name']} ({item['need_quantity']} {item['unit']})")

    with col_schedule:
        st.subheader(f"Schedule for {selected_date}")
        
        if not current_schedule:
            st.info("Nothing scheduled yet.")
        else:
            # Group by Staff
            df = pd.DataFrame(current_schedule)
            staff_members = df["staff_name"].unique()
            
            for staff_name in staff_members:
                with st.container():
                    st.markdown(f"#### 👨‍🍳 {staff_name}")
                    staff_items = df[df["staff_name"] == staff_name]
                    
                    for _, row in staff_items.iterrows():
                        cc1, cc2, cc3 = st.columns([3, 1, 1])
                        cc1.markdown(f"**{row['recipe_name']}** ({row['need_quantity']} {row['unit']})")
                        cc2.caption(f"{row['shift']}")
                        if cc3.button(f"Unassign", key=f"un_{row['id']}"):
                            schedule.unassign_item(row['id'])
                            st.rerun()
                    st.divider()

# --- STAFF TAB ---
with tab_staff:
    st.subheader("Kitchen Staff")
    
    with st.form("new_staff"):
        name = st.text_input("Name")
        role = st.selectbox("Role", ["Chef", "Sous Chef", "Line Cook", "Prep Cook", "Dishwasher"])
        if st.form_submit_button("Add Staff"):
            if name:
                msg = schedule.create_staff(name, role)
                st.success(msg)
                st.rerun()
            else:
                st.error("Name required.")
    
    st.markdown("### Team Roster")
    staff = schedule.get_active_staff()
    if staff:
        st.dataframe(pd.DataFrame(staff)[["name", "role"]], use_container_width=True)
