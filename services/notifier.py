import logging
import os

import requests
from prep_brain.config import load_config

logger = logging.getLogger(__name__)
_WARNED_MISSING_CONFIG = False


def send_telegram_notification(message: str):
    """
    Sends a message securely via the Telegram Bot API using requests (synchronous).
    Best for dashboard use where no async loop is running.
    """
    config = load_config()
    telegram_cfg = config.get("telegram", {}) if isinstance(config, dict) else {}
    token = str(telegram_cfg.get("token") or "").strip()
    if not token:
        env_var = str(telegram_cfg.get("bot_token_env_var") or "TELEGRAM_BOT_TOKEN").strip()
        token = os.getenv(env_var, "").strip()
    # We need a chat_id. Since the bot might talk to multiple people,
    # we ideally want to notify the *admin* or the specific user.
    # For this MVP, we will try to find a 'default_admin_id' or 'allowed_users' in config.
    # If not found, we can't send.

    # Prefer current config key, but keep legacy fallback.
    allowed_users = telegram_cfg.get("allowed_user_ids", []) or telegram_cfg.get(
        "allowed_users", []
    )
    global _WARNED_MISSING_CONFIG
    if not token or not allowed_users:
        if not _WARNED_MISSING_CONFIG:
            logger.info("Notifier disabled: missing token or allowed users.")
            _WARNED_MISSING_CONFIG = True
        return False

    # Send to all allowed users? Or just the first?
    # Let's send to all allowed users for visibility.
    success_count = 0
    for user_id in allowed_users:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": user_id, "text": message, "parse_mode": "Markdown"}
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                success_count += 1
            else:
                logger.warning("Notifier send failed: status=%s", resp.status_code)
        except Exception as e:
            logger.warning("Notifier exception: %s", e)

    return success_count > 0
