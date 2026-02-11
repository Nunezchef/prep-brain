import pytest
import os
from unittest.mock import MagicMock, patch
from services import mailer

def test_get_email_preview():
    to = "chef@example.com"
    subject = "Order #123"
    body = "Please send 50lbs of onions."
    
    preview = mailer.get_email_preview(to, subject, body)
    
    assert f"TO: {to}" in preview
    assert f"SUBJECT: {subject}" in preview
    assert body in preview

@patch("smtplib.SMTP")
def test_send_email_success(mock_smtp):
    # Mock config to enable SMTP
    mailer.CONFIG["smtp"]["enabled"] = True
    
    # Mock environment variables
    with patch.dict(os.environ, {"SMTP_EMAIL": "me@kitchen.com", "SMTP_PASSWORD": "secret"}):
        # Mock SMTP server instance
        server_instance = mock_smtp.return_value
        
        result = mailer.send_email("vendor@supply.com", "Subject", "Body")
        
        assert result["success"] is True
        assert "sent successfully" in result["message"]
        
        # Verify SMTP interactions
        mock_smtp.assert_called_with("smtp.gmail.com", 587)
        server_instance.starttls.assert_called_once()
        server_instance.login.assert_called_with("me@kitchen.com", "secret")
        server_instance.sendmail.assert_called_once()

def test_send_email_disabled():
    # Mock config to disable SMTP
    mailer.CONFIG["smtp"]["enabled"] = False
    
    result = mailer.send_email("vendor@supply.com", "Subject", "Body")
    
    assert result["success"] is False
    assert "disabled" in result["message"]

def test_send_email_missing_creds():
    mailer.CONFIG["smtp"]["enabled"] = True
    
    with patch.dict(os.environ, {}, clear=True):
        result = mailer.send_email("vendor@supply.com", "Subject", "Body")
        
        assert result["success"] is False
        assert "credentials not found" in result["message"]
