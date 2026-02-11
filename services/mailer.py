import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yaml
from pathlib import Path
from typing import Dict, Any

# Reuse configuration loading
def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

logger = logging.getLogger(__name__)

def _get_smtp_credentials():
    """Retrieve SMTP credentials from environment variables."""
    email_var = CONFIG["smtp"]["sender_email_env_var"]
    pass_var = CONFIG["smtp"]["password_env_var"]
    
    email = os.environ.get(email_var)
    password = os.environ.get(pass_var)
    
    return email, password

def get_email_preview(to_email: str, subject: str, body: str) -> str:
    """Generate a text preview of the email for copy/pasting."""
    return f"""TO: {to_email}
SUBJECT: {subject}
--------------------------------------------------
{body}
--------------------------------------------------
"""

def send_email(to_email: str, subject: str, body: str) -> Dict[str, Any]:
    """Send an email using the configured SMTP server."""
    if not CONFIG["smtp"].get("enabled", False):
        return {"success": False, "message": "SMTP is disabled in config.yaml."}
    
    sender_email, password = _get_smtp_credentials()
    
    if not sender_email or not password:
        return {"success": False, "message": "SMTP credentials not found in .env."}

    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        host = CONFIG["smtp"]["host"]
        port = CONFIG["smtp"]["port"]

        server = smtplib.SMTP(host, port)
        server.starttls()
        server.login(sender_email, password)
        text = msg.as_string()
        server.sendmail(sender_email, to_email, text)
        server.quit()
        
        logger.info(f"Email sent to {to_email}")
        return {"success": True, "message": f"Email sent successfully to {to_email}"}
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return {"success": False, "message": f"Failed to send email: {str(e)}"}
