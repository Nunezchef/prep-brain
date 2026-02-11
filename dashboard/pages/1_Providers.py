import streamlit as st
import pandas as pd
from services import providers

st.set_page_config(page_title="Providers", page_icon="🚚", layout="wide")

st.title("🚚 Provider Directory")

# Tabs for different modes
tab_view, tab_add = st.tabs(["View Providers", "Add New Vendor"])

# --- VIEW TAB ---
with tab_view:
    vendors = providers.get_all_vendors()
    
    if not vendors:
        st.info("No vendors found. Add one in the 'Add New Vendor' tab.")
    else:
        # Convert to DataFrame for easier display
        df = pd.DataFrame(vendors)
        
        # Display as a dataframe with some column config
        st.dataframe(
            df,
            column_config={
                "website": st.column_config.LinkColumn("Website"),
                "email": st.column_config.LinkColumn("Email"),
            },
            use_container_width=True,
            hide_index=True
        )

        # Edit/Delete Section
        st.divider()
        st.subheader("Manage Vendor")
        
        col1, col2 = st.columns(2)
        
        with col1:
            vendor_options = {v['id']: v['name'] for v in vendors}
            selected_vendor_id = st.selectbox("Select Vendor to Edit/Delete", options=list(vendor_options.keys()), format_func=lambda x: vendor_options[x])
        
        if selected_vendor_id:
            vendor = providers.get_vendor(selected_vendor_id)
            
            with st.expander("Edit Vendor Details", expanded=True):
                with st.form("edit_vendor_form"):
                    new_name = st.text_input("Vendor Name", value=vendor['name'])
                    
                    c1, c2 = st.columns(2)
                    new_contact = c1.text_input("Contact Name (Rep)", value=vendor['contact_name'])
                    new_phone = c2.text_input("Phone", value=vendor['phone'])
                    
                    c3, c4 = st.columns(2)
                    new_email = c3.text_input("Email", value=vendor['email'])
                    new_website = c4.text_input("Website", value=vendor['website'])
                    
                    c5, c6, c7 = st.columns(3)
                    new_method = c5.selectbox("Ordering Method", ["Email", "Text", "Portal", "Call"], index=["Email", "Text", "Portal", "Call"].index(vendor['ordering_method']) if vendor['ordering_method'] in ["Email", "Text", "Portal", "Call"] else 0)
                    new_cutoff = c6.text_input("Cut-off Time", value=vendor['cutoff_time'])
                    new_lead = c7.number_input("Lead Time (Days)", min_value=0, value=vendor['lead_time_days'])
                    
                    new_notes = st.text_area("Notes", value=vendor['notes'])
                    
                    submitted = st.form_submit_button("Update Vendor")
                    
                    if submitted:
                        update_data = {
                            "name": new_name,
                            "contact_name": new_contact,
                            "phone": new_phone,
                            "email": new_email,
                            "website": new_website,
                            "ordering_method": new_method,
                            "cutoff_time": new_cutoff,
                            "lead_time_days": new_lead,
                            "notes": new_notes
                        }
                        result = providers.update_vendor(selected_vendor_id, update_data)
                        if "successfully" in result:
                            st.success(result)
                            st.rerun()
                        else:
                            st.error(result)

            if st.button("Delete Vendor", type="primary"):
                result = providers.delete_vendor(selected_vendor_id)
                st.success(result)
                st.rerun()

# --- ADD TAB ---
with tab_add:
    st.subheader("Add New Vendor")
    with st.form("add_vendor_form"):
        name = st.text_input("Vendor Name (Required)")
        
        c1, c2 = st.columns(2)
        contact = c1.text_input("Contact Name (Rep)")
        phone = c2.text_input("Phone")
        
        c3, c4 = st.columns(2)
        email = c3.text_input("Email")
        website = c4.text_input("Website")
        
        c5, c6, c7 = st.columns(3)
        method = c5.selectbox("Ordering Method", ["Email", "Text", "Portal", "Call"])
        cutoff = c6.text_input("Cut-off Time", placeholder="e.g. 10pm")
        lead = c7.number_input("Lead Time (Days)", min_value=0, value=1)
        
        notes = st.text_area("Notes")
        
        submitted = st.form_submit_button("Create Vendor")
        
        if submitted:
            if not name:
                st.error("Vendor Name is required.")
            else:
                data = {
                    "name": name,
                    "contact_name": contact,
                    "phone": phone,
                    "email": email,
                    "website": website,
                    "ordering_method": method,
                    "cutoff_time": cutoff,
                    "lead_time_days": lead,
                    "notes": notes
                }
                result = providers.create_vendor(data)
                
                if "successfully" in result:
                    st.success(result)
                else:
                    st.error(result)
