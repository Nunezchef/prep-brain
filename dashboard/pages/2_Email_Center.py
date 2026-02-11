import streamlit as st
from services import providers, mailer
import yaml

# Helper to check config status
def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

st.set_page_config(page_title="Email Center", page_icon="📧", layout="wide")

st.title("📧 Email Composer")

# Check SMTP Status
smtp_enabled = CONFIG.get("smtp", {}).get("enabled", False)
if smtp_enabled:
    st.success("✅ SMTP Service is ENABLED. You can send emails directly.")
else:
    st.warning("⚠️ SMTP Service is DISABLED. Emails will be generated for Copy/Paste.")

# Recipient Selection
vendors = providers.get_all_vendors()
recipient_options = {"custom": "Custom Recipient"}
for v in vendors:
    label = f"{v['name']} ({v['contact_name'] or 'No Contact'})"
    recipient_options[v['id']] = label

recipient_mode = st.radio("Recipient Type", ["Vendor", "Custom Email"], horizontal=True)

to_email = ""
if recipient_mode == "Vendor":
    # Filter out vendors without email if you want, or just show all
    # For now show all, and warn if no email
    vendor_id = st.selectbox("Select Vendor", options=[v['id'] for v in vendors], format_func=lambda x: recipient_options.get(x, "Unknown"))
    if vendor_id:
        vendor = providers.get_vendor(vendor_id)
        if vendor and vendor.get('email'):
            to_email = vendor['email']
            st.info(f"Sending to: {to_email}")
        else:
            st.error("This vendor has no email address saved!")
else:
    to_email = st.text_input("To (Email Address)")

subject = st.text_input("Subject", placeholder="Order for [Date] or Inquiry about [Item]")
body = st.text_area("Message Body", height=300, placeholder="Hi [Name],\n\nI'd like to place an order for...")

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("Generate Preview (Copy/Paste)", type="primary"):
        if not to_email:
            st.error("Recipient email required.")
        else:
            preview = mailer.get_email_preview(to_email, subject, body)
            st.code(preview, language="text")
            st.toast("Preview generated!")

with col2:
    if smtp_enabled:
        if st.button("Send Email 🚀"):
            if not to_email:
                st.error("Recipient email required.")
            else:
                with st.spinner("Sending..."):
                    result = mailer.send_email(to_email, subject, body)
                    if result["success"]:
                        st.success(result["message"])
                    else:
                        st.error(result["message"])
    else:
        st.button("Send Email 🚀", disabled=True, help="Enable SMTP in config.yaml to use this feature.")
